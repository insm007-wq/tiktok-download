# src/tiktok_tokens.py — TikTok 검색 페이지 HTML에서 verifyFp 등 추출 (틱톡 번들 변경 시 패턴 추가)
from __future__ import annotations

import json
import re
import urllib.parse
from collections import Counter
from typing import Any


def _walk_find_verify_fp(obj: Any) -> str | None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "verifyFp" and isinstance(v, str) and v.startswith("verify_"):
                return v
            r = _walk_find_verify_fp(v)
            if r:
                return r
    elif isinstance(obj, list):
        for it in obj:
            r = _walk_find_verify_fp(it)
            if r:
                return r
    return None


def _walk_find_web_id(obj: Any) -> str | None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("webId", "user_unique_id", "odinId") and isinstance(v, (str, int)):
                s = str(v).strip()
                if s.isdigit() and len(s) >= 10:
                    return s
            r = _walk_find_web_id(v)
            if r:
                return r
    elif isinstance(obj, list):
        for it in obj:
            r = _walk_find_web_id(it)
            if r:
                return r
    return None


def _verify_fp_from_json_scripts(html: str) -> str | None:
    """type=application/json 스크립트 및 인라인 JSON 덩어리에서 verifyFp 탐색."""
    for m in re.finditer(
        r'<script[^>]*type=["\']application/json["\'][^>]*>([\s\S]*?)</script>',
        html,
        re.IGNORECASE,
    ):
        raw = m.group(1).strip()
        if len(raw) < 10 or len(raw) > 2_000_000:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        fp = _walk_find_verify_fp(data)
        if fp:
            return fp
    return None


def _verify_fp_unicode_escaped(html: str) -> str | None:
    """\\u0022 로 이스케이프된 JSON 속성에서 추출."""
    for pat in (
        r'\\u0022verifyFp\\u0022\s*:\s*\\u0022(verify_[a-z0-9]+)\\u0022',
        r'\\"verifyFp\\"\s*:\s*\\"(verify_[a-z0-9]+)\\"',
    ):
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def _verify_fp_heuristic(html: str) -> str | None:
    """페이지에 등장하는 verify_* 토큰 빈도 — 가장 많이 등장한 값을 사용합니다."""
    found = re.findall(r"\b(verify_[a-z0-9]{16,48})\b", html, re.IGNORECASE)
    if not found:
        return None
    uniq = len(set(found))
    best, count = Counter(found).most_common(1)[0]
    if uniq > 40 and count == 1:
        return None
    return best


def _extract_ms_token_from_html(html: str) -> str | None:
    """HTML 또는 스크립트 블록에서 msToken 추출 (HTTP Set-Cookie 없을 때 보조 수단)."""
    # 1) 직접 JSON 패턴: "msToken":"<value>"
    m = re.search(r'"msToken"\s*:\s*"([A-Za-z0-9_=+/\-]{80,120})"', html)
    if m:
        return m.group(1)
    # 2) URL 파라미터 패턴: ?msToken= or &msToken=
    m = re.search(r'[?&]msToken=([A-Za-z0-9_%\-]{80,160})', html)
    if m:
        try:
            return urllib.parse.unquote(m.group(1))
        except Exception:
            return m.group(1)
    return None


def extract_tokens_from_search_html(html: str) -> dict[str, str | None]:
    """
    검색 결과 HTML에서 verifyFp / webId / msToken 후보를 추출합니다.
    실패 시 각 필드는 None일 수 있습니다.
    """
    out: dict[str, str | None] = {"verifyFp": None, "webId": None, "msToken": None}

    # 직접 문자열 패턴 (가장 흔함)
    m = re.search(r'"verifyFp"\s*:\s*"(verify_[a-z0-9]+)"', html, re.IGNORECASE)
    if m:
        out["verifyFp"] = m.group(1)
    if not out["verifyFp"]:
        m = re.search(r"verifyFp=([^&\s\"']+)", html)
        if m:
            out["verifyFp"] = urllib.parse.unquote(m.group(1))
    if not out["verifyFp"]:
        m = re.search(r"(verify_[a-z0-9]{16,})", html)
        if m:
            out["verifyFp"] = m.group(1)

    if not out["verifyFp"]:
        out["verifyFp"] = _verify_fp_unicode_escaped(html)

    if not out["verifyFp"]:
        out["verifyFp"] = _verify_fp_from_json_scripts(html)

    # SIGI_STATE / __UNIVERSAL_DATA__ 스크립트 블록
    for block_pat in (
        r'<script[^>]*id=["\']SIGI_STATE["\'][^>]*>([\s\S]*?)</script>',
        r'<script[^>]*id=["\']__UNIVERSAL_DATA_FOR_REHYDRATION__["\'][^>]*>([\s\S]*?)</script>',
    ):
        bm = re.search(block_pat, html, re.IGNORECASE)
        if not bm:
            continue
        raw = bm.group(1).strip()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not out["verifyFp"]:
            out["verifyFp"] = _walk_find_verify_fp(data)
        if not out["webId"]:
            out["webId"] = _walk_find_web_id(data)
        if out["verifyFp"]:
            break

    # 전체 HTML JSON 워크 (무거우나 마지막 수단)
    if not out["verifyFp"]:
        try:
            # 일부 페이지는 JSON이 여러 블록으로 나뉨 — 큰 덩어리만 시도
            for big in re.findall(r"\{[^{}]*\"verifyFp\"[^{}]*\}", html):
                try:
                    j = json.loads(big)
                except json.JSONDecodeError:
                    continue
                fp = _walk_find_verify_fp(j)
                if fp:
                    out["verifyFp"] = fp
                    break
        except Exception:
            pass

    if not out["verifyFp"]:
        out["verifyFp"] = _verify_fp_heuristic(html)

    # msToken: HTML 내 JSON 또는 URL 파라미터에서 추출
    out["msToken"] = _extract_ms_token_from_html(html)

    return out
