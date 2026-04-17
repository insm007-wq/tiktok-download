"""TikWM 공개 API 클라이언트.

워터마크 없는 CDN URL(`hdplay`/`play`) 확보용 병렬 호출 경로. TikTok 웹 API의
`play_addr`가 간헐적으로 워터마크 박힌 URL로 떨어지는 케이스를 커버.

실패는 조용히 None 반환 — 호출자는 기존 CDN URL로 자연스럽게 폴백.
실패 원인(타임아웃·네트워크·JSON·code≠0)은 로그에서 구분 가능하도록 태그.
"""
from __future__ import annotations

import asyncio
import time
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
    t0 = time.monotonic()

    try:
        resp = await client.get(
            full_url,
            headers={"Accept": "application/json"},
            **_req_kw(client, timeout=TIKWM_TIMEOUT_SEC),
        )
    except asyncio.TimeoutError:
        actor.log.warning(f"[tikwm] timeout {TIKWM_TIMEOUT_SEC}s url={raw_url}")
        return None
    except Exception as e:
        # 네트워크·SSL·DNS 등 전송 단계 실패
        actor.log.warning(
            f"[tikwm] network_err {type(e).__name__}: {e} "
            f"elapsed={time.monotonic() - t0:.1f}s"
        )
        return None

    # 응답 본문 파싱 — JSON 실패 시 body preview를 로그에 남겨서 디버그 가능
    try:
        data = resp.json()
    except Exception as e:
        body_preview = (getattr(resp, "text", "") or "")[:200]
        actor.log.warning(
            f"[tikwm] json_err {type(e).__name__} "
            f"status={getattr(resp, 'status_code', None)} "
            f"body={body_preview!r}"
        )
        return None

    code = data.get("code")
    if code != 0:
        actor.log.warning(
            f"[tikwm] code={code} msg={data.get('msg')!r} url={raw_url}"
        )
        return None

    inner = data.get("data")
    if not isinstance(inner, dict):
        actor.log.warning(f"[tikwm] no_data_field keys={list(data.keys())}")
        return None

    actor.log.info(
        f"[tikwm] ok id={inner.get('id')!r} "
        f"hdplay={bool(inner.get('hdplay'))} play={bool(inner.get('play'))} "
        f"elapsed={time.monotonic() - t0:.2f}s"
    )
    return inner
