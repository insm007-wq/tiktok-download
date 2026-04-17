"""TikTok URL 파싱 — 다양한 URL 형식에서 video_id 추출.

지원 형식:
  - https://www.tiktok.com/@user/video/1234567890
  - https://vm.tiktok.com/ZMxxxxx/
  - https://www.tiktok.com/t/ZMxxxxx/
  - https://m.tiktok.com/v/1234567890
  - 순수 숫자 ID (예: "1234567890")
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from apify import Actor

from session import req_kw


@dataclass
class VideoInput:
    original: str       # 사용자 입력 원본
    video_id: str       # 숫자 video ID
    username: str       # URL에서 추출한 username (없으면 빈 문자열)


# /video/<digits> 또는 /v/<digits> 패턴
_RE_VIDEO_ID = re.compile(r"/(?:video|v)/(\d{15,25})")
# 순수 숫자 ID
_RE_PURE_ID = re.compile(r"^\d{15,25}$")
# @username 추출
_RE_USERNAME = re.compile(r"/@([^/?\s]+)")


def _extract_from_url(url: str) -> tuple[str, str]:
    """URL에서 (video_id, username) 추출. 못 찾으면 ("", "")."""
    m = _RE_VIDEO_ID.search(url)
    vid = m.group(1) if m else ""
    m2 = _RE_USERNAME.search(url)
    uname = m2.group(1) if m2 else ""
    return vid, uname


async def parse_video_input(
    raw: str,
    client: Any,
    actor: Actor,
) -> VideoInput | None:
    """사용자 입력을 VideoInput으로 변환. 실패 시 None."""
    raw = raw.strip()
    if not raw:
        return None

    # 순수 숫자 ID
    if _RE_PURE_ID.match(raw):
        return VideoInput(original=raw, video_id=raw, username="")

    # 일반 TikTok URL에서 직접 추출 시도
    vid, uname = _extract_from_url(raw)
    if vid:
        return VideoInput(original=raw, video_id=vid, username=uname)

    # 단축 URL → 리다이렉트 따라가서 canonical URL 얻기
    if "tiktok.com" in raw.lower():
        try:
            resp = await client.get(
                raw,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                },
                **req_kw(client, timeout=15.0),
            )
            final_url = str(resp.url)
            vid, uname = _extract_from_url(final_url)
            if vid:
                return VideoInput(original=raw, video_id=vid, username=uname)
        except Exception as e:
            actor.log.warning(f"[url_parser] 단축 URL 리다이렉트 실패: {raw} → {type(e).__name__}: {e}")

    actor.log.warning(f"[url_parser] video ID 추출 실패: {raw!r}")
    return None
