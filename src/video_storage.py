"""전체 영상 다운로드 + Apify KV Store 저장.

- <= 9MB: 단일 KV Store 레코드 (영구 URL, 브라우저 직접 접근 가능)
- > 9MB: TikTok CDN URL만 반환 (다운로드 스킵, 브라우저에서 Referer 필요할 수 있음)

과거 9~30MB 구간은 청크 분할 저장했으나 MP4를 바이트 단위로 자르면 moov atom
누락으로 앞부분만 재생되는 버그가 있어 제거. KV Store 단일 레코드 9MB 한계
때문에 초과분은 CDN URL 반환 경로로 통일.
"""
from __future__ import annotations

import asyncio
import os
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

from apify import Actor

from constants import MAX_KV_RECORD_BYTES, DOWNLOAD_TIMEOUT_SEC, _FIXED_UA


@dataclass
class DownloadResult:
    success: bool
    video_id: str
    file_size_bytes: int = 0
    storage_type: str = ""        # "kv_store" | "cdn_url_only" | "error"
    download_url: str | None = None       # KV URL(<=9MB) 또는 TikTok CDN URL(>9MB)
    download_urls: list[str] = field(default_factory=list)  # 동일 URL 단일 원소
    chunk_count: int = 0
    cdn_url: str | None = None
    error: str | None = None


def _kv_public_url(store_id: str, key: str) -> str:
    """Apify KV Store 공개 URL."""
    return (
        f"https://api.apify.com/v2/key-value-stores/"
        f"{store_id}/records/{urllib.parse.quote(key, safe='')}"
    )


async def download_full_video(
    actor: Actor,
    client: Any,
    cdn_url: str,
    video_id: str,
    cookie_str: str | None,
    max_size_bytes: int,
) -> DownloadResult:
    """CDN에서 전체 영상 다운로드 → KV Store 저장 → 공개 URL 반환."""
    if not cdn_url or not video_id:
        return DownloadResult(
            success=False, video_id=video_id,
            storage_type="error", error="cdn_url 또는 video_id 누락",
        )

    ua = _FIXED_UA
    headers = {
        "User-Agent": ua,
        "Referer": "https://www.tiktok.com/",
        "Accept": "*/*",
    }
    if cookie_str:
        headers["Cookie"] = cookie_str

    # 1단계: HEAD 요청으로 파일 크기 확인. 타임아웃 짧게(5s) — 실패 시 GET으로 바로 폴백.
    content_length = 0
    try:
        head_resp = await client.head(
            cdn_url, headers=headers, timeout=5.0,
            allow_redirects=True,
        )
        cl = head_resp.headers.get("content-length")
        if cl:
            content_length = int(cl)
            actor.log.info(
                f"[download] id={video_id} content-length={content_length} "
                f"({content_length / 1024 / 1024:.1f}MB)"
            )
    except Exception as e:
        actor.log.warning(
            f"[download] HEAD 실패 id={video_id}: {type(e).__name__}: {e} "
            f"— GET으로 진행"
        )

    # 크기 제한 초과 체크. 두 케이스 모두 CDN URL만 반환 — 다운로드 자체를 스킵.
    # 1) max_size_bytes(유저 상한) 초과 → 대역폭 낭비 방지
    # 2) MAX_KV_RECORD_BYTES(Apify KV 9MB 한계) 초과 → 쪼개면 MP4가 깨져 재생 불가
    skip_threshold = min(max_size_bytes, MAX_KV_RECORD_BYTES)
    if content_length > skip_threshold:
        reason = (
            "user_max_size"
            if content_length > max_size_bytes
            else "kv_record_limit"
        )
        actor.log.info(
            f"[download] CDN URL만 반환 id={video_id} reason={reason} "
            f"size={content_length / 1024 / 1024:.1f}MB "
            f"kv_limit={MAX_KV_RECORD_BYTES / 1024 / 1024:.0f}MB "
            f"user_limit={max_size_bytes / 1024 / 1024:.0f}MB"
        )
        return DownloadResult(
            success=True, video_id=video_id,
            file_size_bytes=content_length,
            storage_type="cdn_url_only",
            download_url=cdn_url,
            download_urls=[cdn_url],
            cdn_url=cdn_url,
        )

    # 2단계: 전체 다운로드 — 예외 타입별로 레벨·태그 차등
    try:
        resp = await client.get(
            cdn_url, headers=headers, timeout=DOWNLOAD_TIMEOUT_SEC,
            allow_redirects=True,
        )
        if resp.status_code not in (200, 206):
            actor.log.error(
                f"[download] http_err id={video_id} status={resp.status_code} "
                f"url={cdn_url}"
            )
            return DownloadResult(
                success=False, video_id=video_id,
                storage_type="error",
                error=f"HTTP {resp.status_code}",
                cdn_url=cdn_url,
            )
        content = resp.content
        if not content:
            actor.log.error(f"[download] empty_body id={video_id} url={cdn_url}")
            return DownloadResult(
                success=False, video_id=video_id,
                storage_type="error", error="빈 응답",
                cdn_url=cdn_url,
            )
    except asyncio.TimeoutError:
        actor.log.error(
            f"[download] timeout id={video_id} {DOWNLOAD_TIMEOUT_SEC}s url={cdn_url}"
        )
        return DownloadResult(
            success=False, video_id=video_id,
            storage_type="error",
            error=f"timeout {DOWNLOAD_TIMEOUT_SEC}s",
            cdn_url=cdn_url,
        )
    except Exception as e:
        actor.log.error(
            f"[download] network_err id={video_id} "
            f"{type(e).__name__}: {e} url={cdn_url}"
        )
        return DownloadResult(
            success=False, video_id=video_id,
            storage_type="error",
            error=f"{type(e).__name__}: {e}",
            cdn_url=cdn_url,
        )

    file_size = len(content)
    actor.log.info(f"[download] id={video_id} 다운로드 완료 {file_size} bytes")

    # 다운로드 후 크기 재확인 — HEAD에서 content-length 누락된 경우의 안전망.
    post_threshold = min(max_size_bytes, MAX_KV_RECORD_BYTES)
    if file_size > post_threshold:
        actor.log.info(
            f"[download] 다운로드 후 크기 초과 — CDN URL만 반환 id={video_id} "
            f"size={file_size / 1024 / 1024:.1f}MB kv_limit=9MB"
        )
        return DownloadResult(
            success=True, video_id=video_id,
            file_size_bytes=file_size,
            storage_type="cdn_url_only",
            download_url=cdn_url,
            download_urls=[cdn_url],
            cdn_url=cdn_url,
        )

    # 3단계: KV Store 저장
    store_id = (os.environ.get("APIFY_DEFAULT_KEY_VALUE_STORE_ID") or "").strip()
    if not store_id:
        # Named KV Store 사용
        try:
            store = await actor.open_key_value_store(name="tiktok-downloads")
            # store ID를 환경에서 가져올 수 없으면 store 객체에서 추출
            store_id = getattr(store, 'id', '') or ''
        except Exception:
            store = await actor.open_key_value_store()
    else:
        store = await actor.open_key_value_store()

    # 단일 KV 레코드 저장. 9MB 초과는 위 post_threshold에서 이미 CDN URL로 빠짐.
    try:
        key = f"video_{video_id}.mp4"
        await store.set_value(key, content, content_type="video/mp4")
        url = _kv_public_url(store_id, key) if store_id else None
        actor.log.info(f"[download] KV 저장 완료 id={video_id} key={key}")
        return DownloadResult(
            success=True, video_id=video_id,
            file_size_bytes=file_size,
            storage_type="kv_store",
            download_url=url,
            download_urls=[url] if url else [],
            chunk_count=1,
            cdn_url=cdn_url,
        )
    except Exception as e:
        actor.log.warning(f"[download] KV 저장 실패 id={video_id}: {type(e).__name__}: {e}")
        # KV 저장 실패해도 CDN URL이 있으니 성공으로 처리 — 유저는 CDN URL로 다운로드.
        return DownloadResult(
            success=True, video_id=video_id,
            file_size_bytes=file_size,
            storage_type="cdn_url_only",
            download_url=cdn_url,
            download_urls=[cdn_url],
            cdn_url=cdn_url,
            error=f"KV 저장 실패: {type(e).__name__}: {e}",
        )
