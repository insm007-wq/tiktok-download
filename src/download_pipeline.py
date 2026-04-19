"""다운로드 파이프라인 — URL 파싱 → 영상 조회 → 다운로드 → dataset push."""
from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlparse

from apify import Actor

from url_parser import parse_video_input
from video_detail import fetch_video_detail, fetch_video_detail_html
from video_storage import download_full_video
from play_url import (
    _play_url_candidates,
    _best_preview_play_url,
    _merged_video_block,
    _codec_summary,
    _classify_url,
    _first_safe_h264_url,
    _h264_url_from_bitrate,
)
from tikwm_api import fetch_tikwm
from aweme_fields import (
    _hashtags_from_aweme,
    _statistics_merged,
    _stat_int,
    _uploaded_at_seconds,
)
from session import cookie_dict as _cookie_dict


def _aweme_from_tikwm(d: dict, video_id: str) -> dict:
    """TikWM data dict → _build_result이 기대하는 aweme 스키마.

    웹 API·HTML 모두 실패했을 때 최소 메타데이터를 채우기 위한 폴백.
    `aweme_fields.py`가 camelCase/snake_case 병행 처리하므로 TikWM의 snake_case
    필드명을 그대로 둬도 추출됨. duration은 초(TikWM) → ms(기존 스키마) 변환.
    """
    return {
        "aweme_id": d.get("id") or video_id,
        "desc": d.get("title") or "",
        "author": d.get("author") or {},
        "statistics": {
            "play_count": d.get("play_count", 0),
            "digg_count": d.get("digg_count", 0),
            "comment_count": d.get("comment_count", 0),
            "share_count": d.get("share_count", 0),
        },
        "create_time": d.get("create_time", 0),
        "video": {"duration": (d.get("duration") or 0) * 1000},
    }


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

    # 2. 영상 상세 데이터 조회 — TikTok 웹 API와 TikWM 병렬 호출.
    # TikWM(공개 API)는 워터마크 없는 CDN URL(hdplay/play) 확보용.
    # return_exceptions=True — 한쪽 태스크가 예외를 던져도 다른 쪽 결과를 살리고
    # 나머지 폴백 체인(HTML·TikWM 재구성)으로 자연스럽게 복구.
    aweme, tikwm_data = await asyncio.gather(
        fetch_video_detail(client, video_id, actor, ms_token_override=ms_token_override),
        fetch_tikwm(client, raw_url, actor),
        return_exceptions=True,
    )
    if isinstance(aweme, BaseException):
        actor.log.warning(
            f"[pipeline] 웹 API 예외 id={video_id}: "
            f"{type(aweme).__name__}: {aweme}"
        )
        aweme = None
    if isinstance(tikwm_data, BaseException):
        actor.log.warning(
            f"[pipeline] TikWM 예외 id={video_id}: "
            f"{type(tikwm_data).__name__}: {tikwm_data}"
        )
        tikwm_data = None

    if not aweme:
        actor.log.info(f"[pipeline] 웹 API 실패 → HTML 폴백 id={video_id}")
        try:
            aweme = await fetch_video_detail_html(
                client, video_id, video_input.username, actor,
            )
        except Exception as e:
            actor.log.warning(
                f"[pipeline] HTML 폴백 예외 id={video_id}: "
                f"{type(e).__name__}: {e}"
            )
            aweme = None
    if not aweme and tikwm_data:
        actor.log.info(f"[pipeline] 웹·HTML 실패 → TikWM 메타로 재구성 id={video_id}")
        aweme = _aweme_from_tikwm(tikwm_data, video_id)

    if not aweme:
        actor.log.error(f"[pipeline] 영상 데이터 조회 실패 id={video_id}")
        return {
            "id": video_id,
            "inputUrl": raw_url,
            "downloadStatus": "error",
            "error": "영상 데이터를 조회할 수 없습니다. URL이 유효한지 확인해주세요.",
        }

    # 3. 영상 URL 결정 — 다층 방어 fallback (yt-dlp 방식 참고).
    # 우선순위:
    #   1. bit_rate 엔트리의 codec_type=h264 의 play_addr URL (dict 레벨 메타 신뢰)
    #   2. URL 경로에 `_h264_` 박힌 play_addr URL (URL 패턴 신뢰)
    #   3. TikWM hdplay/play (워터마크는 없지만 코덱 보증 없음 — bytevc2일 수 있음)
    #   4. bit_rate 정렬 1순위 (URL 코덱·워터마크 기반 정렬됨)
    #   5. 최후 fallback
    video_block = _merged_video_block(aweme, aweme)
    play_urls = _play_url_candidates(video_block)
    primary_url, hls_url, candidates = _best_preview_play_url(play_urls)
    fallback_cdn = primary_url or (candidates[0] if candidates else None)

    # 진단: bit_rate 항목의 코덱 목록. bytevc2가 1순위로 정렬되면 호환성 문제 있음.
    codec_list = _codec_summary(video_block)
    if codec_list:
        codecs_str = ", ".join(f"{c}@{b}" for c, b in codec_list[:5])
        actor.log.info(f"[pipeline] codec_candidates id={video_id} [{codecs_str}]")

    # TikWM URL 확보 (코덱 보증 없음)
    tikwm_url = None
    if tikwm_data:
        tikwm_url = tikwm_data.get("hdplay") or tikwm_data.get("play")

    # 1순위: bit_rate 엔트리의 codec_type=h264 (메타 신뢰)
    h264_from_bitrate = _h264_url_from_bitrate(video_block)
    # 2순위: URL 경로에 `_h264_` 박힌 것 (패턴 신뢰)
    safe_h264 = _first_safe_h264_url(play_urls)

    cdn_url: str | None = None
    url_source = "unknown"
    if h264_from_bitrate:
        cdn_url = h264_from_bitrate
        url_source = "bitrate_h264"
    elif safe_h264:
        cdn_url = safe_h264
        url_source = "play_addr_h264_pattern"
    elif tikwm_url:
        cdn_url = tikwm_url
        url_source = "tikwm"
    elif fallback_cdn:
        cdn_url = fallback_cdn
        url_source = "tiktok_cdn_sorted"

    if not cdn_url:
        actor.log.error(f"[pipeline] 재생 URL 없음 id={video_id}")
        return _build_result(
            aweme, video_id, raw_url,
            download_status="error",
            error="영상 재생 URL을 찾을 수 없습니다.",
        )

    # 진단: 선택된 URL의 코덱·워터마크 분류.
    classified = _classify_url(cdn_url)
    host = urlparse(cdn_url).netloc
    picked_codec = classified["codec"]
    picked_wm = classified["watermark"]

    # bytevc2 URL이 실제로 선택되면 심각한 문제 — 정렬·필터 로직 회귀 의심.
    if picked_codec == "bytevc2":
        actor.log.warning(
            f"[pipeline] ⚠ bytevc2 URL 선택됨 — 재생 실패 예상 id={video_id} "
            f"source={url_source} host={host}"
        )
    elif url_source in ("bitrate_h264", "play_addr_h264_pattern"):
        actor.log.info(
            f"[pipeline] ✅ safe h264 path id={video_id} source={url_source} "
            f"host={host} wm={picked_wm}"
        )
    elif url_source == "tikwm":
        actor.log.info(
            f"[pipeline] ✅ no-watermark path id={video_id} source=tikwm(hdplay/play) "
            f"host={host} codec={picked_codec} wm={picked_wm}"
        )
    else:
        fallback_reason = "tikwm_none" if not tikwm_data else "tikwm_url_missing"
        actor.log.warning(
            f"[pipeline] ⚠ fallback path id={video_id} source={url_source} "
            f"host={host} codec={picked_codec} wm={picked_wm} "
            f"reason={fallback_reason}"
        )

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
