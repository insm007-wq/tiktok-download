"""TikTok 요청에 필요한 식별자/토큰을 생성하는 순수 함수 모음.

모든 함수는 무상태(stateless)이며 외부 I/O가 없다.
세션 초기화(session.py)와 검색 API(search_api.py)에서 호출한다.
"""
from __future__ import annotations

import random
import string


# msToken 문자 집합 — 실제 TikTok 토큰의 base64-like 문자 + `=`, `_`
# (정식 webmssdk.js 생성값 대체용 fallback. 진짜 토큰은 mstoken.py에서 생성)
_MS_TOKEN_CHARS = string.ascii_letters + string.digits + "=_"

# verifyFp 는 소문자 영숫자 16자 + `verify_` prefix — TikTok 웹 패턴
_VERIFY_FP_CHARS = string.ascii_lowercase + string.digits


def generate_random_ms_token(length: int = 107) -> str:
    """가짜 msToken 생성 (길이 기본 107).

    TikTok이 자체 JS(`webmssdk.js`)로 동적 생성하는 진짜 msToken은
    curl-cffi로 얻을 수 없어 fallback 용도로 사용. 일부 요청에서는
    서버가 랜덤 토큰도 수용하므로 None보다는 이쪽이 낫다.
    """
    return "".join(random.choices(_MS_TOKEN_CHARS, k=length))


def generate_verify_fp() -> str:
    """verifyFp 브라우저 핑거프린트 토큰 생성 (형식: verify_<16자>).

    검색 HTML 파싱 실패 시 백업용. 같은 키워드에 대해선 세션 내 캐시해
    페이지네이션마다 바뀌지 않도록 search_api.py에서 관리한다.
    """
    part = "".join(random.choices(_VERIFY_FP_CHARS, k=16))
    return f"verify_{part}"


def generate_device_id() -> str:
    """deviceId 생성 — 19자리 숫자 문자열 (TikTok 웹 ID 규격).

    세션 수명 동안 고정되어야 하며(`client._tt_device_id`에 캐시),
    X-Bogus 서명과 msToken 생성의 입력으로 사용된다.
    """
    return str(random.randint(10**18, 10**19 - 1))
