"""TikTok CDN URL 검증 + 반환 (영상 바이트 다운로드 안 함).

유저 사용 패턴: 엑터 실행 후 dataset에서 URL 받아 즉시 다운로드. 24시간 뒤에
쓰는 케이스 거의 없음 → CDN URL(12~24h 유효) 만 반환해도 충분.

과거 구현은 영상을 먼저 다운로드해서 Apify KV Store에 저장했으나:
- 9MB 초과는 쪼개면 MP4가 깨져 재생 불가 (moov atom 누락, 앞부분만 재생)
- 프록시 대역폭 + KV 저장 비용이 크게 들어감
- 유저는 즉시 받으니 영구 저장 이득 없음

결론: HEAD로 파일 크기만 확인하고 CDN URL을 바로 반환. run 시간·원가 모두 절감.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from apify import Actor

from constants import _FIXED_UA


@dataclass
class DownloadResult:
    success: bool
    video_id: str
    file_size_bytes: int = 0
    storage_type: str = ""        # "cdn_url_only" | "error"
    download_url: str | None = None       # TikTok CDN URL (유저가 직접 다운로드)
    download_urls: list[str] = field(default_factory=list)  # 동일 URL 단일 원소
    chunk_count: int = 0
    cdn_url: str | None = None
    error: str | None = None


async def download_full_video(
    actor: Actor,
    client: Any,
    cdn_url: str,
    video_id: str,
    cookie_str: str | None,
    max_size_bytes: int,
) -> DownloadResult:
    """CDN URL을 HEAD로 검증하고 바로 반환. 실제 바이트 다운로드는 하지 않음.

    `cookie_str`·`max_size_bytes` 는 과거 다운로드 파이프라인 호환용으로만 유지.
    """
    if not cdn_url or not video_id:
        return DownloadResult(
            success=False, video_id=video_id,
            storage_type="error", error="cdn_url 또는 video_id 누락",
        )

    headers = {
        "User-Agent": _FIXED_UA,
        "Referer": "https://www.tiktok.com/",
        "Accept": "*/*",
    }
    if cookie_str:
        headers["Cookie"] = cookie_str

    # HEAD 요청으로 파일 크기·접근성만 확인. 실패해도 URL은 반환 — 유저가
    # 직접 시도할 수 있도록.
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
        # 4xx/5xx면 CDN URL 자체가 깨졌을 가능성 — 경고만 남기고 URL은 반환.
        status = getattr(head_resp, "status_code", 0)
        if status and status >= 400:
            actor.log.warning(
                f"[download] HEAD status={status} id={video_id} "
                f"— URL 유효성 의심, 유저 브라우저에서 실패할 수 있음"
            )
    except Exception as e:
        actor.log.warning(
            f"[download] HEAD 실패 id={video_id}: {type(e).__name__}: {e} "
            f"— CDN URL 그대로 반환"
        )

    # 대역폭 안전장치 기록용 — max_size_bytes 초과 시 경고만. 어차피 바이트 안 받음.
    if content_length and content_length > max_size_bytes:
        actor.log.info(
            f"[download] 크기 {content_length / 1024 / 1024:.1f}MB > "
            f"limit {max_size_bytes / 1024 / 1024:.0f}MB (안내용)"
        )

    actor.log.info(f"[download] CDN URL 반환 id={video_id}")
    return DownloadResult(
        success=True,
        video_id=video_id,
        file_size_bytes=content_length,
        storage_type="cdn_url_only",
        download_url=cdn_url,
        download_urls=[cdn_url],
        chunk_count=1,
        cdn_url=cdn_url,
    )
