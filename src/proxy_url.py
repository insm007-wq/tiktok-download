"""TikTok CDN URL을 Railway 스트리밍 프록시 URL로 서명·포장.

Apify KV Store 9MB 한계를 넘는 영상은 CDN URL 그대로 반환하면 브라우저에서
`Referer` 누락으로 403이 난다. 이 모듈은 Railway 프록시(`/v/:videoId`)에 HMAC
서명된 URL을 만들어 고객이 헤더·쿠키 없이도 바로 다운로드할 수 있게 한다.

환경 변수:
  TIKTOK_VIDEO_PROXY_BASE    — 예: https://proxyapify-production-d4c5.up.railway.app
  TIKTOK_VIDEO_PROXY_SECRET  — Railway 쪽 VIDEO_PROXY_SECRET 과 동일한 랜덤 문자열
  TIKTOK_VIDEO_PROXY_TTL_SEC — 선택, 기본 86400 (24시간)

둘 중 하나라도 없으면 `build_proxy_url()`이 None 반환 → 호출자가 원본 CDN URL로 폴백.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import time
import urllib.parse

_DEFAULT_TTL_SEC = 86400  # 24h — TikTok CDN 서명도 대체로 24h 안이라 더 길어봐야 업스트림 만료


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _ttl_sec() -> int:
    raw = _env("TIKTOK_VIDEO_PROXY_TTL_SEC")
    if not raw:
        return _DEFAULT_TTL_SEC
    try:
        v = int(raw)
        if v <= 0:
            return _DEFAULT_TTL_SEC
        return v
    except ValueError:
        return _DEFAULT_TTL_SEC


def build_proxy_url(video_id: str, cdn_url: str) -> str | None:
    """video_id + cdn_url → 서명된 Railway 프록시 URL.

    실패 조건:
      - video_id 또는 cdn_url 비어 있음
      - 환경 변수 누락 → 호출자는 CDN URL 그대로 돌려주는 폴백 경로를 타야 함.

    반환 URL 예:
      https://<base>/v/<video_id>?u=<urlenc_cdn>&e=<exp>&s=<hmac_hex>
    """
    if not video_id or not cdn_url:
        return None

    base = _env("TIKTOK_VIDEO_PROXY_BASE").rstrip("/")
    secret = _env("TIKTOK_VIDEO_PROXY_SECRET")
    if not base or not secret:
        return None

    exp = int(time.time()) + _ttl_sec()
    payload = f"{video_id}\n{cdn_url}\n{exp}"
    sig = hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    qs = urllib.parse.urlencode(
        {"u": cdn_url, "e": str(exp), "s": sig},
        quote_via=urllib.parse.quote,
    )
    video_id_enc = urllib.parse.quote(video_id, safe="")
    return f"{base}/v/{video_id_enc}?{qs}"
