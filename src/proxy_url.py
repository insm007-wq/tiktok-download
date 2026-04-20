"""TikTok CDN URL을 Railway 스트리밍 프록시 URL로 서명·포장.

TikTok CDN URL 은 `Referer` 누락 시 브라우저에서 403. 이 모듈은 Railway 프록시
(`/v/:videoId`)에 HMAC 서명된 URL을 만들어 고객이 헤더·쿠키 없이도 바로
다운로드할 수 있게 한다.

## 우선순위

1. `TIKTOK_VIDEO_PROXY_BASE` 환경 변수 (배포·테스트 오버라이드용)
2. `_DEFAULT_PROXY_BASE` 하드코딩 상수 (엑터 사용 고객이 env 설정 없이도 동작)

시크릿(`TIKTOK_VIDEO_PROXY_SECRET`)은 **반드시 env 로만 주입** — 코드에
하드코딩하지 않는다. Apify 콘솔에서 Actor 단위 환경변수(secret 타입)로 한 번
등록하면 모든 런에 자동 주입되며 고객에게는 노출되지 않음.

## 환경 변수

  TIKTOK_VIDEO_PROXY_BASE    — 선택. 비워두면 `_DEFAULT_PROXY_BASE` 사용
  TIKTOK_VIDEO_PROXY_SECRET  — 필수. Railway `VIDEO_PROXY_SECRET` 과 동일한 값
  TIKTOK_VIDEO_PROXY_TTL_SEC — 선택, 기본 86400 (24시간)

시크릿이 없으면 `build_proxy_url()`이 None 반환 → 호출자가 원본 CDN URL 폴백.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import time
import urllib.parse

# Railway 에 배포된 proxy_apify 공용 허브의 공개 URL.
# 같은 서비스가 /mstoken (검색 엑터용) 과 /v/:videoId (다운로드 엑터용) 을 같이 서빙.
# 이 URL 은 tiktok-search 엑터의 _DEFAULT_MSTOKEN_URL 과 동일한 base.
_DEFAULT_PROXY_BASE = "https://proxyapify-production-d4c5.up.railway.app"

_DEFAULT_TTL_SEC = 86400  # 24h — TikTok CDN 서명도 대체로 24h 안이라 더 길어봐야 업스트림 만료


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _proxy_base() -> str:
    """env 우선, 없으면 하드코딩 기본값."""
    override = _env("TIKTOK_VIDEO_PROXY_BASE")
    return (override or _DEFAULT_PROXY_BASE).rstrip("/")


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
      - base URL 비어 있음 (env·하드코딩 모두 없을 때 — 실질적으로 안 발생)
      - `TIKTOK_VIDEO_PROXY_SECRET` 미설정 → 호출자는 CDN URL 그대로 돌려주는 폴백 경로를 타야 함

    반환 URL 예:
      https://<base>/v/<video_id>?u=<urlenc_cdn>&e=<exp>&s=<hmac_hex>
    """
    if not video_id or not cdn_url:
        return None

    base = _proxy_base()
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
