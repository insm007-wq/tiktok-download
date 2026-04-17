"""개별 TikTok 영상의 aweme(상세) 데이터 조회.

Primary: /api/item/detail/ API
Fallback: HTML 페이지 __UNIVERSAL_DATA_FOR_REHYDRATION__ 파싱
"""
from __future__ import annotations

import json
import random
import time
import urllib.parse
from typing import Any

from apify import Actor

from xbogus import get_x_bogus
from generators import generate_device_id, generate_verify_fp
from session import (
    cookie_dict as _cookie_dict,
    req_kw as _req_kw,
    ensure_ttwid as _ensure_ttwid,
)
from mstoken import resolve_ms_token
from constants import (
    VIDEO_DETAIL_API_URL,
    MOBILE_AWEME_DETAIL_URL,
    VERBOSE_DIAG,
    _FIXED_UA,
    _MOBILE_UA,
)


# chrome131을 선두로 시도 — warmup fallback 순서와 일치시켜 성공률↑.
# 실측 결과 세 번째 시도(chrome124)는 거의 항상 빈 응답이라 제거.
_IMPERSONATE_ORDER = ("chrome131", "chrome120")


async def fetch_video_detail(
    client: Any,
    video_id: str,
    actor: Actor,
    ms_token_override: str | None = None,
) -> dict | None:
    """API로 영상 상세 데이터 조회. 실패 시 None."""
    ua = _FIXED_UA
    if not getattr(client, "_tt_device_id", None):
        client._tt_device_id = generate_device_id()
    device_id = client._tt_device_id

    for attempt, imp in enumerate(_IMPERSONATE_ORDER):
        if attempt == 0:
            await _ensure_ttwid(client, ua, actor, impersonate=imp)

        cookies = _cookie_dict(client)
        _mt = resolve_ms_token(client, override=ms_token_override, cookie_dict=cookies)
        ms_token = _mt.value

        params = {
            "aid": "1988",
            "app_language": "en",
            "app_name": "tiktok_web",
            "browser_language": "en-US",
            "browser_name": "Mozilla",
            "browser_online": "true",
            "browser_platform": "Win32",
            "browser_version": "5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "channel": "tiktok_web",
            "cookie_enabled": "true",
            "device_id": device_id,
            "device_platform": "web_pc",
            "itemId": video_id,
            "language": "en",
            "msToken": ms_token,
            "os": "windows",
            "region": "US",
            "screen_height": "1080",
            "screen_width": "1920",
            "tz_name": "America/New_York",
            "verifyFp": generate_verify_fp(),
            "WebIdLastTime": str(int(time.time()) - random.randint(100, 1000)),
        }

        qs = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        x_bogus = get_x_bogus(qs, ua)
        full_url = f"{VIDEO_DETAIL_API_URL}?{qs}&X-Bogus={urllib.parse.quote(x_bogus, safe='')}"

        cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
        headers = {
            "User-Agent": ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cookie": cookie_str,
            "Referer": f"https://www.tiktok.com/",
            "Origin": "https://www.tiktok.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        }

        try:
            resp = await client.get(
                full_url,
                headers=headers,
                **_req_kw(client, timeout=5.0, impersonate=imp),
            )
            if not resp.content:
                # 빈 응답은 임펄세네이트 로테이션으로 복구되지 않는 경향이 큼
                # (X-Bogus/msToken 단계에서 이미 걸러진 상태). 즉시 HTML 폴백으로 전환.
                actor.log.warning(
                    f"[video_detail] API 빈 응답 attempt={attempt + 1} imp={imp} "
                    f"→ 남은 재시도 스킵, HTML 폴백으로 전환"
                )
                return None

            data = resp.json()
            status_code = data.get("statusCode") or data.get("status_code", 0)
            if status_code != 0:
                actor.log.warning(
                    f"[video_detail] API status={status_code} "
                    f"msg={data.get('statusMsg') or data.get('status_msg')!r}"
                )
                continue

            # itemInfo.itemStruct 또는 itemInfo 직접
            item_info = data.get("itemInfo", {})
            aweme = item_info.get("itemStruct")
            if aweme and isinstance(aweme, dict):
                actor.log.info(f"[video_detail] API 조회 성공 id={video_id} imp={imp}")
                return aweme

            actor.log.warning(f"[video_detail] itemStruct 없음 keys={list(data.keys())}")

        except Exception as e:
            actor.log.warning(
                f"[video_detail] API 요청 실패 attempt={attempt + 1}: "
                f"{type(e).__name__}: {e}"
            )

    # Fallback: None 반환 (호출자가 HTML fallback 시도)
    return None


async def fetch_video_detail_html(
    client: Any,
    video_id: str,
    username: str,
    actor: Actor,
) -> dict | None:
    """HTML 페이지에서 __UNIVERSAL_DATA_FOR_REHYDRATION__ 파싱으로 aweme 데이터 추출."""
    ua = _FIXED_UA
    if username:
        url = f"https://www.tiktok.com/@{username}/video/{video_id}"
    else:
        # username 없으면 oembed API로 canonical URL 획득 시도
        url = f"https://www.tiktok.com/oembed?url=https://www.tiktok.com/video/{video_id}"
        try:
            resp = await client.get(url, **_req_kw(client, timeout=10.0))
            oembed = resp.json()
            author_url = oembed.get("author_url", "")
            if author_url:
                url = f"{author_url}/video/{video_id}"
            else:
                actor.log.warning("[video_detail_html] oembed에서 author_url 없음")
                return None
        except Exception as e:
            actor.log.warning(f"[video_detail_html] oembed 실패: {type(e).__name__}: {e}")
            return None

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    }

    try:
        resp = await client.get(
            url,
            headers=headers,
            **_req_kw(client, timeout=12.0),
        )
        html = resp.text or ""
        if not html:
            actor.log.warning("[video_detail_html] 빈 HTML 응답")
            return None

        # __UNIVERSAL_DATA_FOR_REHYDRATION__ 추출
        import re
        pattern = r'<script\s+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>'
        m = re.search(pattern, html, re.DOTALL)
        if not m:
            actor.log.warning("[video_detail_html] __UNIVERSAL_DATA_FOR_REHYDRATION__ 없음")
            return None

        universal_data = json.loads(m.group(1))
        # 다양한 경로 시도
        default_scope = universal_data.get("__DEFAULT_SCOPE__", {})
        video_detail = default_scope.get("webapp.video-detail", {})
        item_info = video_detail.get("itemInfo", {})
        aweme = item_info.get("itemStruct")

        if aweme and isinstance(aweme, dict):
            actor.log.info(f"[video_detail_html] HTML 파싱 성공 id={video_id}")
            return aweme

        # 대안 경로
        for key in default_scope:
            if "video" in key.lower() or "detail" in key.lower():
                sub = default_scope[key]
                if isinstance(sub, dict):
                    ii = sub.get("itemInfo", {})
                    a = ii.get("itemStruct")
                    if a and isinstance(a, dict):
                        actor.log.info(
                            f"[video_detail_html] 대안 경로로 파싱 성공 key={key}"
                        )
                        return a

        actor.log.warning(
            f"[video_detail_html] aweme 데이터 추출 실패 "
            f"scope_keys={list(default_scope.keys())}"
        )
        return None

    except json.JSONDecodeError as e:
        actor.log.warning(f"[video_detail_html] JSON 파싱 실패: {e}")
        return None
    except Exception as e:
        actor.log.warning(f"[video_detail_html] 요청 실패: {type(e).__name__}: {e}")
        return None


async def fetch_video_detail_mobile(
    client: Any,
    video_id: str,
    actor: Actor,
) -> dict | None:
    """TikTok 모바일 aweme detail API로 aweme 데이터 조회.

    웹 API가 `play_addr`(워터마크 없음) 대신 `download_addr`(워터마크)만
    반환하는 영상에 대한 폴백. 모바일 앱 파라미터를 시뮬레이션해 TikTok이
    정식 앱에 내주는 메타데이터를 받아옴 — 경쟁 엑터 수준의 parity 목적.
    실패 시 None.
    """
    if not getattr(client, "_tt_device_id", None):
        client._tt_device_id = generate_device_id()
    device_id = client._tt_device_id

    params = {
        "aweme_ids": json.dumps([video_id], separators=(",", ":")),
        "aid": "1233",
        "app_name": "musical_ly",
        "channel": "googleplay",
        "device_platform": "android",
        "os": "android",
        "os_version": "13",
        "device_type": "SM-G998B",
        "device_id": device_id,
        "iid": device_id,
        "version_code": "300904",
        "version_name": "30.9.4",
        "build_number": "30.9.4",
        "manifest_version_code": "2023009040",
        "update_version_code": "2023009040",
        "resolution": "1080*2400",
        "dpi": "420",
        "language": "en",
        "region": "US",
    }

    qs = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    full_url = f"{MOBILE_AWEME_DETAIL_URL}?{qs}"

    headers = {
        "User-Agent": _MOBILE_UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
    }

    for attempt, imp in enumerate(_IMPERSONATE_ORDER):
        try:
            resp = await client.get(
                full_url,
                headers=headers,
                **_req_kw(client, timeout=6.0, impersonate=imp),
            )
            if not resp.content:
                actor.log.warning(
                    f"[video_detail_mobile] 빈 응답 attempt={attempt + 1} imp={imp}"
                )
                continue

            data = resp.json()
            status_code = data.get("status_code") or data.get("statusCode", 0)
            if status_code != 0:
                actor.log.warning(
                    f"[video_detail_mobile] status={status_code} "
                    f"msg={data.get('status_msg') or data.get('statusMsg')!r}"
                )
                continue

            details = data.get("aweme_details") or []
            if not isinstance(details, list) or not details:
                actor.log.warning(
                    f"[video_detail_mobile] aweme_details 비어있음 keys={list(data.keys())}"
                )
                continue

            aweme = details[0]
            if isinstance(aweme, dict):
                actor.log.info(
                    f"[video_detail_mobile] 조회 성공 id={video_id} imp={imp}"
                )
                return aweme

        except Exception as e:
            actor.log.warning(
                f"[video_detail_mobile] 요청 실패 attempt={attempt + 1}: "
                f"{type(e).__name__}: {e}"
            )

    return None
