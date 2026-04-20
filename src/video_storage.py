"""영상 다운로드 URL 결정 — URL Resolver 모델.

엑터는 더 이상 바이트를 다운로드하지 않는다. TikTok API로 CDN URL을 찾고,
Railway 영상 프록시(`/v/:videoId`) 서명 URL 을 만들어 고객에게 반환.

실제 바이트 전송은 고객이 브라우저로 프록시 URL을 열면 Railway 가 TikTok CDN 에
올바른 Referer·UA를 붙여 대리 스트리밍. Apify 런 시간이 60% 이상 단축되고
컴퓨팅 부담이 Railway로 이동.

프록시 환경 변수 미설정 시(`TIKTOK_VIDEO_PROXY_BASE`/`_SECRET`) 원본 CDN URL 을
그대로 반환 — backward compatible 폴백. 브라우저에서 403 가능성 있으니 운영 시
반드시 프록시 설정 권장.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from apify import Actor

from proxy_url import build_proxy_url


@dataclass
class DownloadResult:
    success: bool
    video_id: str
    file_size_bytes: int = 0
    storage_type: str = ""        # "proxy_url" | "cdn_url_fallback" | "error"
    download_url: str | None = None
    download_urls: list[str] = field(default_factory=list)
    chunk_count: int = 0
    cdn_url: str | None = None
    error: str | None = None


def resolve_download_url(
    actor: Actor,
    video_id: str,
    cdn_url: str,
    file_size_bytes: int,
    max_size_bytes: int,
) -> DownloadResult:
    """CDN URL + 메타 크기 → 서명된 프록시 URL(혹은 CDN URL 폴백) 포장.

    실제 바이트 다운로드 없음. 순수 URL 생성만.

    - 메타 크기가 유저 상한(`max_size_bytes`) 초과 시 에러 반환 (대역폭 낭비 방지).
    - 프록시 env 설정돼 있으면 `storage_type=proxy_url`, 아니면 `cdn_url_fallback`.
    """
    if not cdn_url or not video_id:
        return DownloadResult(
            success=False, video_id=video_id,
            storage_type="error", error="cdn_url 또는 video_id 누락",
        )

    if max_size_bytes and file_size_bytes > max_size_bytes:
        actor.log.warning(
            f"[resolve] 크기 상한 초과 id={video_id} "
            f"size={file_size_bytes / 1024 / 1024:.1f}MB "
            f"max={max_size_bytes / 1024 / 1024:.0f}MB"
        )
        return DownloadResult(
            success=False, video_id=video_id,
            file_size_bytes=file_size_bytes,
            storage_type="error",
            error=f"영상 크기({file_size_bytes // 1024 // 1024}MB)가 "
                  f"상한({max_size_bytes // 1024 // 1024}MB)을 초과",
            cdn_url=cdn_url,
        )

    proxy_url = build_proxy_url(video_id, cdn_url)
    if proxy_url:
        actor.log.info(
            f"[resolve] 프록시 URL 발행 id={video_id} "
            f"size={file_size_bytes / 1024 / 1024:.1f}MB"
        )
        return DownloadResult(
            success=True, video_id=video_id,
            file_size_bytes=file_size_bytes,
            storage_type="proxy_url",
            download_url=proxy_url,
            download_urls=[proxy_url],
            chunk_count=1,
            cdn_url=cdn_url,
        )

    actor.log.warning(
        f"[resolve] 프록시 미설정 → CDN URL 그대로 반환 id={video_id} "
        f"(TIKTOK_VIDEO_PROXY_BASE/SECRET 설정 권장)"
    )
    return DownloadResult(
        success=True, video_id=video_id,
        file_size_bytes=file_size_bytes,
        storage_type="cdn_url_fallback",
        download_url=cdn_url,
        download_urls=[cdn_url],
        chunk_count=1,
        cdn_url=cdn_url,
    )
