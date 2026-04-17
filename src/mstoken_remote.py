"""Railway `proxy_apify /mstoken` 엔드포인트 호출 래퍼.

Actor 기동 시 1회 호출해 자동으로 유효한 msToken을 획득한다.
Railway 쪽에서 Puppeteer+stealth + (옵션) IPRoyal 로 실제 브라우저를 띄워
toktoken을 발급받아 캐싱해 두고 있으므로, 일반적으로 ~50ms 안에 응답.

환경 변수:
    TIKTOK_MSTOKEN_URL      — 예: https://proxyapify-production-d4c5.up.railway.app/mstoken
                              (미설정 시 PROXY_BASE 에서 자동 유추)
    TIKTOK_MSTOKEN_API_KEY  — Railway 측 MSTOKEN_API_KEY 와 동일. 미설정 시 생략
    TIKTOK_MSTOKEN_FORCE    — "1" 이면 캐시 무시 강제 재생성 (디버그)

실패 시 조용히 빈 결과 반환 → 기존 resolve_ms_token() fallback 사슬이 동작.
"""
from __future__ import annotations

import asyncio
import os
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any

from apify import Actor


_DEFAULT_TIMEOUT = 15.0
_MIN_ACCEPT_LEN = 140


@dataclass
class RemoteMsTokenResult:
    value: str
    length: int
    mode: str           # "direct" | "iproyal" | "" (실패)
    cache_hit: bool
    elapsed_ms: int
    source: str         # "remote" | "error"
    error: str | None = None

    def __bool__(self) -> bool:
        return bool(self.value) and self.length >= _MIN_ACCEPT_LEN


# 환경변수가 전파되지 않아도 동작하도록 최후 fallback URL 하드코딩.
# 실제 운영 시에는 TIKTOK_MSTOKEN_URL 환경변수로 덮어쓰는 것이 권장됨.
_DEFAULT_MSTOKEN_URL = "https://proxyapify-production-d4c5.up.railway.app/mstoken"


def _derive_url() -> str:
    """env 지정 → PROXY_BASE 에서 유추 → 하드코딩 기본값 순."""
    direct = (os.environ.get("TIKTOK_MSTOKEN_URL") or "").strip()
    if direct:
        return direct
    base = (os.environ.get("TIKTOK_PREVIEW_PROXY_BASE") or "").strip()
    if base:
        q = base.find("?")
        origin = (base[:q] if q >= 0 else base).rstrip("/")
        if origin:
            return f"{origin}/mstoken"
    return _DEFAULT_MSTOKEN_URL


async def fetch_remote_ms_token(
    client: Any,
    actor: Actor,
    *,
    force: bool = False,
) -> RemoteMsTokenResult:
    """Railway /mstoken 엔드포인트 호출 → client 캐시에 주입.

    client 는 curl_cffi AsyncSession. 성공 시:
        client._tt_ms_token = value
        client._tt_ms_token_source = "remote"
    """
    url = _derive_url()
    if not url:
        actor.log.warning(
            "[STAGE:MSTOKEN] remote URL 미설정 — TIKTOK_MSTOKEN_URL "
            "또는 TIKTOK_PREVIEW_PROXY_BASE 를 확인하세요."
        )
        return RemoteMsTokenResult(
            value="", length=0, mode="", cache_hit=False, elapsed_ms=0,
            source="error", error="no_url_configured",
        )

    # ?force=1 지원 (env 또는 인자)
    env_force = os.environ.get("TIKTOK_MSTOKEN_FORCE", "").strip() in ("1", "true", "yes")
    if force or env_force:
        url = f"{url}{'&' if '?' in url else '?'}force=1"

    api_key = (os.environ.get("TIKTOK_MSTOKEN_API_KEY") or "").strip()
    headers: dict[str, str] = {}
    if api_key:
        headers["X-API-Key"] = api_key

    started = time.monotonic()
    actor.log.info(f"[STAGE:MSTOKEN] remote 호출 url={url.split('?')[0]}")

    try:
        # curl_cffi AsyncSession — Railway egress 는 프록시 없이 직접 연결 (빠름).
        r = await client.get(
            url,
            headers=headers,
            timeout=_DEFAULT_TIMEOUT,
            impersonate="chrome120",
            # 이 호출은 Apify residential proxy 를 태우지 않음 (Railway 는 공개 HTTPS)
            proxies=None,
        )
    except Exception as e:
        elapsed = int((time.monotonic() - started) * 1000)
        actor.log.warning(
            f"[STAGE:MSTOKEN] 네트워크 예외 elapsed={elapsed}ms "
            f"{type(e).__name__}: {e}"
        )
        return RemoteMsTokenResult(
            value="", length=0, mode="", cache_hit=False, elapsed_ms=elapsed,
            source="error", error=f"{type(e).__name__}: {e}",
        )

    elapsed = int((time.monotonic() - started) * 1000)

    if r.status_code != 200:
        preview = (r.text or "")[:160]
        actor.log.warning(
            f"[STAGE:MSTOKEN] http={r.status_code} elapsed={elapsed}ms body={preview!r}"
        )
        return RemoteMsTokenResult(
            value="", length=0, mode="", cache_hit=False, elapsed_ms=elapsed,
            source="error", error=f"http_{r.status_code}",
        )

    try:
        data = r.json()
    except Exception as e:
        actor.log.warning(f"[STAGE:MSTOKEN] JSON 파싱 실패: {e}")
        return RemoteMsTokenResult(
            value="", length=0, mode="", cache_hit=False, elapsed_ms=elapsed,
            source="error", error="bad_json",
        )

    token = (data.get("msToken") or "").strip()
    length = int(data.get("length") or len(token))
    mode = str(data.get("mode") or "")
    cache_hit = bool(data.get("cacheHit"))

    if not token or length < _MIN_ACCEPT_LEN:
        actor.log.warning(
            f"[STAGE:MSTOKEN] 반환 토큰 짧음 len={length} mode={mode} "
            f"→ fallback 사슬로 넘김"
        )
        return RemoteMsTokenResult(
            value=token, length=length, mode=mode, cache_hit=cache_hit,
            elapsed_ms=elapsed, source="error", error="stub_token",
        )

    # client 캐시에 주입 — resolve_ms_token() 가 cache 소스로 집어감
    client._tt_ms_token = token
    client._tt_ms_token_source = "remote"

    actor.log.info(
        f"[STAGE:MSTOKEN] acquired len={length} mode={mode} "
        f"cache_hit={cache_hit} elapsed={elapsed}ms"
    )
    return RemoteMsTokenResult(
        value=token, length=length, mode=mode, cache_hit=cache_hit,
        elapsed_ms=elapsed, source="remote",
    )
