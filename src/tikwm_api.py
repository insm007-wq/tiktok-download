"""TikWM 공개 API 클라이언트.

워터마크 없는 CDN URL(`hdplay`/`play`) 확보용 병렬 호출 경로. TikTok 웹 API의
`play_addr`가 간헐적으로 워터마크 박힌 URL로 떨어지는 케이스를 커버.

실패는 조용히 None 반환 — 호출자는 기존 CDN URL로 자연스럽게 폴백.
"""
from __future__ import annotations

import urllib.parse
from typing import Any

from apify import Actor

from constants import TIKWM_API_URL, TIKWM_TIMEOUT_SEC
from session import req_kw as _req_kw


async def fetch_tikwm(
    client: Any,
    raw_url: str,
    actor: Actor,
) -> dict | None:
    """TikWM `/api/?url=<tiktok_url>&hd=1` 호출.

    성공 시 응답의 `data` dict 반환, 실패/타임아웃/code≠0 시 None.
    """
    if not raw_url:
        return None

    qs = urllib.parse.urlencode(
        {"url": raw_url, "hd": "1"}, quote_via=urllib.parse.quote
    )
    full_url = f"{TIKWM_API_URL}?{qs}"

    try:
        resp = await client.get(
            full_url,
            headers={"Accept": "application/json"},
            **_req_kw(client, timeout=TIKWM_TIMEOUT_SEC),
        )
        data = resp.json()
    except Exception as e:
        actor.log.warning(f"[tikwm] 요청 실패: {type(e).__name__}: {e}")
        return None

    code = data.get("code")
    if code != 0:
        actor.log.warning(
            f"[tikwm] code={code} msg={data.get('msg')!r}"
        )
        return None

    inner = data.get("data")
    if not isinstance(inner, dict):
        actor.log.warning(f"[tikwm] data 필드 없음 keys={list(data.keys())}")
        return None

    actor.log.info(
        f"[tikwm] 응답 OK id={inner.get('id')!r} "
        f"hdplay={bool(inner.get('hdplay'))} play={bool(inner.get('play'))}"
    )
    return inner
