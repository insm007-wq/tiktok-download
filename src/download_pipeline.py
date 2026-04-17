"""다운로드 파이프라인 — URL 파싱 → 영상 조회 → 다운로드 → dataset push."""
from __future__ import annotations

from typing import Any

from apify import Actor

from url_parser import parse_video_input
from video_detail import fetch_video_detail, fetch_video_detail_html
from video_storage import download_full_video
from play_url import _play_url_candidates, _best_preview_play_url, _merged_video_block
from aweme_fields import (
    _aweme_unique_id,
    _hashtags_from_aweme,
    _statistics_merged,
    _stat_int,
    _uploaded_at_seconds,
)
from session import cookie_dict as _cookie_dict


async def process_video(
    actor: Actor,
    client: Any,
    raw_url: str,
    max_size_bytes: int,
    ms_token_override: str | None = None,
) -> dict | None:
    """단일 영상을 처리하고 dataset 아이템을 반환."""

    # 1. URL 파싱
    video_input = await parse_video_input(raw_url, client, actor)
    if not video_input:
        actor.log.error(f"[pipeline] URL 파싱 실패: {raw_url!r}")
        return {
            "inputUrl": raw_url,
            "downloadStatus": "error",
            "error": "URL에서 video ID를 추출할 수 없습니다.",
        }

    video_id = video_input.video_id
    actor.log.info(f"[pipeline] 처리 시작 id={video_id} url={video_input.original}")

    # 2. 영상 상세 데이터 조회 (API 우선, HTML 폴백)
    aweme = await fetch_video_detail(
        client, video_id, actor, ms_token_override=ms_token_override,
    )
    if not aweme:
        actor.log.info(f"[pipeline] API 실패 → HTML 폴백 id={video_id}")
        aweme = await fetch_video_detail_html(
            client, video_id, video_input.username, actor,
        )

    if not aweme:
        actor.log.error(f"[pipeline] 영상 데이터 조회 실패 id={video_id}")
        return {
            "id": video_id,
            "inputUrl": raw_url,
            "downloadStatus": "error",
            "error": "영상 데이터를 조회할 수 없습니다. URL이 유효한지 확인해주세요.",
        }

    # 3. 영상 URL 추출
    video_block = _merged_video_block(aweme, aweme)
    play_urls = _play_url_candidates(video_block)
    primary_url, hls_url, candidates = _best_preview_play_url(play_urls)

    if not primary_url and not candidates:
        actor.log.error(f"[pipeline] 재생 URL 없음 id={video_id}")
        return _build_result(
            aweme, video_id, raw_url,
            download_status="error",
            error="영상 재생 URL을 찾을 수 없습니다.",
        )

    cdn_url = primary_url or (candidates[0] if candidates else None)

    # 4. 전체 다운로드
    cookies = _cookie_dict(client)
    cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())

    dl_result = await download_full_video(
        actor, client, cdn_url, video_id, cookie_str, max_size_bytes,
    )

    # 5. 결과 조합
    result = _build_result(
        aweme, video_id, raw_url,
        download_status="success" if dl_result.success else "error",
        download_url=dl_result.download_url,
        download_urls=dl_result.download_urls,
        file_size_bytes=dl_result.file_size_bytes,
        chunk_count=dl_result.chunk_count,
        storage_type=dl_result.storage_type,
        cdn_url=dl_result.cdn_url or cdn_url,
        play_url_candidates=candidates,
        error=dl_result.error,
    )

    return result


def _build_result(
    aweme: dict,
    video_id: str,
    input_url: str,
    *,
    download_status: str = "success",
    download_url: str | None = None,
    download_urls: list[str] | None = None,
    file_size_bytes: int = 0,
    chunk_count: int = 0,
    storage_type: str = "",
    cdn_url: str | None = None,
    play_url_candidates: list[str] | None = None,
    error: str | None = None,
) -> dict:
    """aweme 데이터 + 다운로드 결과를 dataset 아이템으로 변환."""
    author = aweme.get("author") or {}
    stats = _statistics_merged(aweme, aweme)

    username = (
        author.get("unique_id") or author.get("uniqueId") or ""
    )
    tiktok_url = None
    if username and video_id:
        tiktok_url = f"https://www.tiktok.com/@{username}/video/{video_id}"

    video_block = aweme.get("video") or {}
    duration = 0
    for dk in ("duration", "Duration"):
        d = video_block.get(dk)
        if d is not None:
            try:
                duration = float(d)
                break
            except (TypeError, ValueError):
                pass

    result: dict[str, Any] = {
        "id": video_id,
        "inputUrl": input_url,
        "url": tiktok_url,
        "description": aweme.get("desc") or "",
        "author": {
            "username": username,
            "nickname": author.get("nickname") or "",
            "url": f"https://www.tiktok.com/@{username}" if username else None,
        },
        "statistics": {
            "views": _stat_int(stats, "playCount", "play_count", "v2_play_api_play_count"),
            "likes": _stat_int(stats, "diggCount", "digg_count"),
            "comments": _stat_int(stats, "commentCount", "comment_count"),
            "shares": _stat_int(stats, "shareCount", "share_count"),
        },
        "duration": duration,
        "hashtags": _hashtags_from_aweme(aweme),
        "uploadedAt": _uploaded_at_seconds(aweme) or None,
        # 다운로드 결과
        "downloadStatus": download_status,
        "downloadUrl": download_url,
        "downloadUrls": download_urls or [],
        "fileSizeBytes": file_size_bytes,
        "chunkCount": chunk_count,
        "storageType": storage_type,
        "cdnUrl": cdn_url,
        "playUrlCandidates": play_url_candidates or [],
    }
    if error:
        result["error"] = error

    return result
