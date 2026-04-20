"""msToken 소싱 전략 집중화.

TikTok의 msToken은 본래 `webmssdk.js`가 브라우저에서 런타임에 생성해
`document.cookie`에 주입한다. curl-cffi는 JS를 실행하지 않으므로
HTTP 응답만으로는 정상적으로 얻을 수 없음이 로그로 확인되었다.

이 모듈은 실제로 동작하는 **fallback 사슬**을 한 곳에 모아 관리한다.
검색 API 호출 시 우선순위:

    1) override       — 사용자가 input으로 직접 붙여넣은 값 (가장 신뢰)
    2) cookie jar     — 서버가 Set-Cookie로 심은 값 (실전에선 거의 안 옴)
    3) cache          — 이전 응답/HTML 파싱에서 캐시된 값
    4) html 파싱      — /search HTML 내부 JSON에 박힌 값 (형식이 있을 때만)
    5) generated      — 랜덤 107자 fallback (`generators.generate_random_ms_token`)

반환 객체는 값과 소스 태그를 함께 들고 있어 로그·진단에 활용한다.

※ webmssdk.js 자체를 Node로 실행하는 방법은 reverse-engineered 포팅이
  필요하고 TikTok이 주기적으로 교체하므로 이 모듈에선 다루지 않는다.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from generators import generate_random_ms_token


@dataclass
class MsTokenResult:
    value: str
    source: str       # "override" | "cookie" | "cache" | "html" | "generated"

    def __bool__(self) -> bool:
        return bool(self.value)


def resolve_ms_token(
    client: Any,
    *,
    override: str | None = None,
    cookie_dict: dict[str, str] | None = None,
) -> MsTokenResult:
    """우선순위 사슬로 msToken을 결정.

    Args:
        client: curl_cffi AsyncSession (이전 서버 응답에서 `_tt_ms_token` 캐시 가능).
        override: 사용자 input으로 들어온 msToken (브라우저에서 복사한 값).
        cookie_dict: 현재 세션 쿠키 jar. None이면 client.cookies에서 추출.

    Returns:
        MsTokenResult(value, source). 최종 fallback은 항상 generated.
    """
    ov = (override or "").strip()
    if ov:
        return MsTokenResult(ov, "override")

    if cookie_dict is None:
        try:
            cookie_dict = (
                client.cookies.get_dict()
                if hasattr(client.cookies, "get_dict")
                else dict(client.cookies.items())
            )
        except Exception:
            cookie_dict = {}

    ck_ms = (cookie_dict.get("msToken") or "").strip()
    if ck_ms:
        return MsTokenResult(ck_ms, "cookie")

    cached = (getattr(client, "_tt_ms_token", "") or "").strip()
    if cached:
        # client._tt_ms_token 은 세션 웜업/검색 응답에서 채워짐 — html 또는 server echo
        src = getattr(client, "_tt_ms_token_source", "cache")
        return MsTokenResult(cached, src)

    return MsTokenResult(generate_random_ms_token(), "generated")


def record_html_ms_token(client: Any, value: str | None) -> None:
    """세션 웜업 단계에서 HTML 파싱으로 찾은 msToken을 캐시에 반영."""
    v = (value or "").strip()
    if not v or len(v) < 80:
        return
    client._tt_ms_token = v
    client._tt_ms_token_source = "html"
