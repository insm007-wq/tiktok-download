"""전체 영상 다운로드 + Apify KV Store 저장.

- <= 9MB: 단일 KV Store 레코드
- 9MB ~ 30MB: 청크 분할 저장 (각 청크 최대 9MB)
- > 설정 상한: CDN URL만 반환
"""
from __future__ import annotations

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
    storage_type: str = ""        # "kv_store" | "kv_store_chunked" | "cdn_url_only" | "error"
    download_url: str | None = None       # 단일 KV URL (단일 저장 시)
    download_urls: list[str] = field(default_factory=list)  # 청크 URL 목록
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

    # 1단계: HEAD 요청으로 파일 크기 확인
    content_length = 0
    try:
        head_resp = await client.head(
            cdn_url, headers=headers, timeout=15.0,
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

    # 크기 제한 초과 체크
    if content_length > max_size_bytes:
        actor.log.info(
            f"[download] id={video_id} 크기 초과 "
            f"({content_length / 1024 / 1024:.1f}MB > {max_size_bytes / 1024 / 1024:.0f}MB) "
            f"— CDN URL만 반환"
        )
        return DownloadResult(
            success=True, video_id=video_id,
            file_size_bytes=content_length,
            storage_type="cdn_url_only",
            cdn_url=cdn_url,
        )

    # 2단계: 전체 다운로드
    try:
        resp = await client.get(
            cdn_url, headers=headers, timeout=DOWNLOAD_TIMEOUT_SEC,
            allow_redirects=True,
        )
        if resp.status_code not in (200, 206):
            return DownloadResult(
                success=False, video_id=video_id,
                storage_type="error",
                error=f"HTTP {resp.status_code}",
                cdn_url=cdn_url,
            )
        content = resp.content
        if not content:
            return DownloadResult(
                success=False, video_id=video_id,
                storage_type="error", error="빈 응답",
                cdn_url=cdn_url,
            )
    except Exception as e:
        return DownloadResult(
            success=False, video_id=video_id,
            storage_type="error",
            error=f"{type(e).__name__}: {e}",
            cdn_url=cdn_url,
        )

    file_size = len(content)
    actor.log.info(f"[download] id={video_id} 다운로드 완료 {file_size} bytes")

    # 다운로드 후 크기 재확인
    if file_size > max_size_bytes:
        return DownloadResult(
            success=True, video_id=video_id,
            file_size_bytes=file_size,
            storage_type="cdn_url_only",
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

    try:
        if file_size <= MAX_KV_RECORD_BYTES:
            # 단일 레코드 저장
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
        else:
            # 청크 분할 저장
            chunk_size = MAX_KV_RECORD_BYTES
            chunks = []
            for i in range(0, file_size, chunk_size):
                chunk_data = content[i:i + chunk_size]
                key = f"video_{video_id}_part{len(chunks)}.mp4"
                await store.set_value(key, chunk_data, content_type="video/mp4")
                url = _kv_public_url(store_id, key) if store_id else None
                chunks.append(url)
                actor.log.info(
                    f"[download] 청크 저장 id={video_id} part={len(chunks) - 1} "
                    f"bytes={len(chunk_data)}"
                )

            actor.log.info(
                f"[download] 청크 저장 완료 id={video_id} chunks={len(chunks)}"
            )
            return DownloadResult(
                success=True, video_id=video_id,
                file_size_bytes=file_size,
                storage_type="kv_store_chunked",
                download_url=chunks[0] if chunks else None,
                download_urls=[u for u in chunks if u],
                chunk_count=len(chunks),
                cdn_url=cdn_url,
            )

    except Exception as e:
        actor.log.warning(f"[download] KV 저장 실패 id={video_id}: {type(e).__name__}: {e}")
        return DownloadResult(
            success=False, video_id=video_id,
            file_size_bytes=file_size,
            storage_type="error",
            error=f"KV 저장 실패: {type(e).__name__}: {e}",
            cdn_url=cdn_url,
        )
