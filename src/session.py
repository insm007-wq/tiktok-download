"""TikTok 세션/쿠키 획득 레이어.

이 모듈의 책임:
- curl_cffi AsyncSession 위에 프록시/impersonate 같은 공통 요청 kwargs를 붙이기
- `/` → `/explore` → `/search` 3-step 웜업으로 ttwid·tt_chain_token 쿠키 획득
- 키워드별 `/search` HTML 로드 + verifyFp/webId 파싱 캐시
- msToken 업데이트는 `mstoken.record_html_ms_token()`에 위임

공유 상태 (AsyncSession 속성 — 관례):
  client._tt_proxy                  : 현재 아웃바운드 프록시 URL
  client._tt_ms_token               : 가장 최근에 획득/수신한 msToken
  client._tt_ms_token_source        : 위 값의 소스 태그 (html/server/cache)
  client._tt_token_cache            : {keyword: tokens_dict} HTML 파싱 캐시
  client._tt_verify_fp_by_kw        : {keyword: verifyFp} 키워드별 fp 캐시
  client._tt_search_imp_by_kw       : {keyword: impersonate} 직전 성공 프로파일
  client._tt_device_id              : 19자리 deviceId (세션 수명 고정)
  client._tt_history_len_by_kw      : {keyword: history_len} 키워드별 history_len
  client._tt_warmup_lock            : asyncio.Lock — 병렬 키워드 동시 웜업 방지
"""
from __future__ import annotations

import asyncio
import random
import re
import time
import urllib.parse
from typing import Any

from apify import Actor
from tiktok_tokens import extract_tokens_from_search_html
from mstoken import record_html_ms_token


# ── 공통 설정 ────────────────────────────────────────────────────────
# TLS/HTTP2 지문 — 기본 프로파일
CURL_IMPERSONATE = "chrome120"

# 빈 본문(200·0바이트) 시 TLS 지문·세션 교체 재시도용 fallback 순서
# 프록시 IP가 매번 교체되므로 같은 profile 반복도 의미 있음
SEARCH_IMPERSONATE_FALLBACKS = ("chrome120", "chrome131")

# 세션 웜업 실패 시 로테이션하는 profile 순서 (최신 우선)
_SESSION_INIT_IMPERSONATE_ORDER = ("chrome131", "chrome124", "chrome120", "safari17_0")

# 검증 규칙: msToken은 JS(webmssdk)가 런타임에 쿠키로 설정 → HTTP만으로는
# 보장되지 않으므로 optional로 둔다. ttwid / tt_chain_token만 필수.
_SESSION_INIT_REQUIRED = {
    "ttwid": {"min_len": 10, "max_len": None},
    "tt_chain_token": {"min_len": 5, "max_len": None},
}
_SESSION_INIT_OPTIONAL = {
    "msToken": {"min_len": 80, "max_len": None},
}


# ── 저수준 유틸 ──────────────────────────────────────────────────────
def cookie_dict(client: Any) -> dict[str, str]:
    """AsyncSession의 cookie jar를 name→value 딕셔너리로 변환."""
    jar = client.cookies
    if hasattr(jar, "get_dict"):
        return jar.get_dict()
    try:
        return dict(jar.items())
    except Exception:
        return {}


def req_kw(
    client: Any,
    timeout: float = 25.0,
    *,
    impersonate: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """curl_cffi 요청에 공통으로 주입할 kwargs 사전.

    - impersonate 지문 (기본 chrome120)
    - timeout
    - allow_redirects=True
    - client._tt_proxy가 있으면 proxies 설정
    """
    kw: dict[str, Any] = {
        "impersonate": impersonate or CURL_IMPERSONATE,
        "timeout": timeout,
        "allow_redirects": True,
    }
    proxy = getattr(client, "_tt_proxy", None)
    if proxy:
        kw["proxies"] = {"http": proxy, "https": proxy}
    kw.update(extra)
    return kw


def impersonate_try_order(client: Any, keyword: str) -> tuple[str, ...]:
    """직전에 성공한 profile을 앞세운 fallback 순서.

    페이지네이션마다 chrome120부터 다시 시도하는 낭비를 방지한다.
    """
    cache = getattr(client, "_tt_search_imp_by_kw", None)
    preferred: str | None = None
    if isinstance(cache, dict):
        preferred = cache.get(keyword)
    if preferred:
        rest = tuple(x for x in SEARCH_IMPERSONATE_FALLBACKS if x != preferred)
        return (preferred,) + rest
    return SEARCH_IMPERSONATE_FALLBACKS


# ── 3-step warm-up ──────────────────────────────────────────────────
async def ensure_ttwid(
    client: Any,
    ua: str,
    actor: Actor,
    impersonate: str | None = None,
    keyword: str | None = None,
) -> dict:
    """3-step warm-up (`/` → `/explore` → `/search?q=<kw>`).

    쿠키·토큰이 이미 유효하면 즉시 short-circuit.
    누락 시 최대 3회까지 impersonate 프로파일을 로테이션하며 재시도.
    병렬 키워드가 동시에 웜업을 돌리지 않도록 `client._tt_warmup_lock`으로 직렬화.

    Returns:
        {cookies, msToken, ttwid, tt_chain_token, impersonate, attempt}
    Raises:
        RuntimeError — 3회 모두 실패 시.
    """
    # 병렬 키워드 동시 웜업 방지 (lock 없는 client에는 생성)
    lock: asyncio.Lock = getattr(client, "_tt_warmup_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        client._tt_warmup_lock = lock

    async with lock:
        return await _do_warmup(client, ua, actor, impersonate, keyword)


async def _do_warmup(
    client: Any,
    ua: str,
    actor: Actor,
    impersonate: str | None,
    keyword: str | None,
) -> dict:
    def _get_token(name: str, ck: dict) -> str:
        # msToken은 쿠키 jar가 아니라 client._tt_ms_token 캐시를 우선 참조.
        if name == "msToken":
            cached = (getattr(client, "_tt_ms_token", "") or "").strip()
            if cached:
                return cached
        return (ck.get(name) or "").strip()

    def _check() -> tuple[dict, list[str]]:
        ck = cookie_dict(client)
        missing: list[str] = []
        for name, rule in _SESSION_INIT_REQUIRED.items():
            val = _get_token(name, ck)
            if not val:
                missing.append(name)
                continue
            n = len(val)
            if n < rule["min_len"] or (rule["max_len"] is not None and n > rule["max_len"]):
                missing.append(name)
        return ck, missing

    # 이미 유효하면 즉시 반환 (병렬 진입 시 두 번째 호출자가 빠르게 통과)
    existing, missing_now = _check()
    if not missing_now:
        ms = _get_token("msToken", existing)
        return {
            "cookies": existing,
            "msToken": ms,
            "ttwid": existing.get("ttwid", ""),
            "tt_chain_token": existing.get("tt_chain_token", ""),
            "impersonate": impersonate or "",
            "attempt": 0,
        }

    # 브라우저 네비게이션 요청과 동일한 보안/콘텐츠 협상 헤더
    base_headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    search_kw = (keyword or "tiktok").strip() or "tiktok"
    search_url = f"https://www.tiktok.com/search?q={urllib.parse.quote(search_kw)}"
    # 속도 개선: warmup step 사이 sleep 대폭 축소 (원본 평균 5.5s → 0.4s).
    # TikTok 은 warmup 수준에서 rate-limit 걸지 않음. 마지막 step 후 sleep 은 완전 제거.
    steps = (
        ("https://www.tiktok.com/", (0.05, 0.1)),
        ("https://www.tiktok.com/explore", (0.05, 0.1)),
        (search_url, (0.0, 0.0)),
    )

    last_error: Exception | None = None
    for attempt in range(1, 4):
        imp = _SESSION_INIT_IMPERSONATE_ORDER[
            min(attempt - 1, len(_SESSION_INIT_IMPERSONATE_ORDER) - 1)
        ]
        actor.log.info(f"[SESSION] warm-up attempt={attempt}/3 impersonate={imp}")
        try:
            for step_idx, (url, (lo, hi)) in enumerate(steps, 1):
                t0 = time.monotonic()
                r = await client.get(
                    url,
                    headers=base_headers,
                    **req_kw(client, timeout=20.0, impersonate=imp),
                )
                elapsed = time.monotonic() - t0
                status = getattr(r, "status_code", None)
                ctype = r.headers.get("content-type", "")
                clen = r.headers.get("content-length")
                body_len = 0
                try:
                    body_len = len(r.content or b"")
                except Exception:
                    pass
                set_cookie = r.headers.get("set-cookie", "") or ""
                sc_names = sorted({
                    p.split("=", 1)[0].strip()
                    for p in set_cookie.split(",")
                    if "=" in p
                })
                # HTML 내부 msToken 추출 시도 → mstoken 모듈에 위임
                html_ms_src = ""
                html_body = r.text or ""
                try:
                    tok = extract_tokens_from_search_html(html_body)
                    html_ms = (tok.get("msToken") or "").strip() if tok else ""
                    if html_ms and len(html_ms) >= 80:
                        record_html_ms_token(client, html_ms)
                        html_ms_src = f"html({len(html_ms)})"
                except Exception as pe:
                    html_ms_src = f"parse_err:{type(pe).__name__}"
                # regex가 못 잡아도 HTML 내 'msToken' 문자열 위치 진단
                if not html_ms_src and html_body:
                    hits = [m.start() for m in re.finditer(r"msToken", html_body)][:3]
                    if hits:
                        samples = [
                            repr(html_body[max(0, p - 30): p + 60]) for p in hits
                        ]
                        actor.log.info(
                            f"[SESSION:step{step_idx}:diag_ms] occurrences={len(hits)} "
                            f"samples={samples}"
                        )
                    else:
                        actor.log.info(
                            f"[SESSION:step{step_idx}:diag_ms] no 'msToken' substring in HTML"
                        )
                ck_now = cookie_dict(client)
                ms_jar = (ck_now.get("msToken") or "").strip()
                ms_cache = (getattr(client, "_tt_ms_token", "") or "").strip()
                tw_now = (ck_now.get("ttwid") or "").strip()
                tc_now = (ck_now.get("tt_chain_token") or "").strip()
                actor.log.info(
                    f"[SESSION:step{step_idx}] url={url.split('?')[0]} "
                    f"status={status} body={body_len}B clen={clen!r} "
                    f"ctype={ctype[:40]!r} elapsed={elapsed:.2f}s "
                    f"set-cookie_names={sc_names} ms_from={html_ms_src or 'none'} "
                    f"jar: msToken_cookie={len(ms_jar)} msToken_cache={len(ms_cache)} "
                    f"ttwid={len(tw_now)} tt_chain_token={len(tc_now)}"
                )
                await asyncio.sleep(random.uniform(lo, hi))
            last_error = None
        except Exception as e:
            last_error = e
            actor.log.warning(
                f"[SESSION] warm-up 요청 실패 attempt={attempt} imp={imp}: "
                f"{type(e).__name__}: {e}"
            )

        cookies, missing = _check()
        actor.log.info(
            f"[SESSION] attempt={attempt} cookie_jar_keys={sorted(cookies.keys())}"
        )
        for name, rule in {**_SESSION_INIT_REQUIRED, **_SESSION_INIT_OPTIONAL}.items():
            val = _get_token(name, cookies)
            optional = name in _SESSION_INIT_OPTIONAL
            tag = "OPTIONAL" if optional else "REQUIRED"
            if not val:
                actor.log.warning(f"[SESSION] MISSING({tag}) field={name}")
                continue
            n = len(val)
            if n < rule["min_len"] or (rule["max_len"] is not None and n > rule["max_len"]):
                actor.log.warning(f"[SESSION] INVALID({tag}) field={name} len={n}")
            else:
                actor.log.info(f"[SESSION] OK({tag}) field={name} len={n}")

        if not missing:
            ms = _get_token("msToken", cookies)
            actor.log.info(
                f"[SESSION] acquired msToken len={len(ms)} via {imp} attempt={attempt}"
            )
            return {
                "cookies": cookies,
                "msToken": ms,
                "ttwid": cookies.get("ttwid", ""),
                "tt_chain_token": cookies.get("tt_chain_token", ""),
                "impersonate": imp,
                "attempt": attempt,
            }

        if attempt < 3:
            # 속도 개선: 재시도 사이 백오프 5~10s → 1~2s. 일시적 jitter 케이스 대부분 커버.
            delay = random.uniform(1.0, 2.0)
            actor.log.warning(
                f"[SESSION] missing={missing} — {delay:.1f}s 후 재시도"
            )
            await asyncio.sleep(delay)

    raise RuntimeError(
        f"Session init failed after 3 attempts (last_error={last_error!r})"
    )


# ── 키워드별 HTML 토큰 캐시 ───────────────────────────────────────
async def load_keyword_html_tokens(
    client: Any,
    keyword: str,
    ua: str,
    actor: Actor,
    impersonate: str | None = None,
    verbose: bool = False,
) -> dict:
    """키워드당 1회 `/search?q=<kw>` HTML을 받아 verifyFp·webId·msToken 파싱·캐시.

    같은 키워드로 페이지네이션할 때 HTML을 다시 받지 않도록
    `client._tt_token_cache[keyword]`에 결과를 저장한다.
    """
    if not hasattr(client, "_tt_token_cache"):
        client._tt_token_cache = {}
    if keyword in client._tt_token_cache:
        return client._tt_token_cache[keyword]

    await ensure_ttwid(client, ua, actor, impersonate=impersonate, keyword=keyword)

    warmup_headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    url = f"https://www.tiktok.com/search?q={urllib.parse.quote(keyword)}"
    try:
        r = await client.get(
            url,
            headers=warmup_headers,
            **req_kw(client, timeout=6.0, impersonate=impersonate),
        )
        tok = extract_tokens_from_search_html(r.text)
        html_len = len(r.text or "")
    except Exception as e:
        actor.log.warning(f"검색 HTML 로드 실패: {type(e).__name__}: {e}")
        tok = {}
        html_len = 0

    # HTML에서 msToken 추출되면 캐시 갱신
    ms = (tok.get("msToken") or "").strip() if tok else ""
    if ms:
        record_html_ms_token(client, ms)

    client._tt_token_cache[keyword] = tok
    if verbose:
        actor.log.info(
            f"[diag:html_token] verifyFp={'ok' if tok.get('verifyFp') else 'miss'} "
            f"webId={'ok' if tok.get('webId') else 'miss'} "
            f"msToken={'html' if ms else 'miss'} html_bytes={html_len}"
        )
    return tok
