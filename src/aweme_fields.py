"""aweme/raw item 파싱 유틸 — id·해시태그·통계·업로드 시각 등 순수 변환.

main.py 에서 추출 (Phase 1 리팩터). 로직 불변, 위치만 이동.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any

from url_sorting import _addr_block_sort_key
from play_url import _extract_urls_from_media_value


def _is_flex_char(ch: str) -> bool:
    """문자 단위 유연 매칭 대상: CJK 한자 + 한글 음절.
    각 글자가 독립적 의미를 가지므로 어순 무관 AND 매칭에 적합."""
    cp = ord(ch)
    return (0xAC00 <= cp <= 0xD7AF       # Hangul Syllables (가–힣)
            or 0x4E00 <= cp <= 0x9FFF     # CJK Unified
            or 0x3400 <= cp <= 0x4DBF     # CJK Extension A
            or 0x20000 <= cp <= 0x2A6DF   # CJK Extension B
            or 0xF900 <= cp <= 0xFAFF)    # CJK Compatibility


def _keyword_match(aweme: dict, keyword: str) -> list[str]:
    """키워드가 실제 등장한 필드 이름 리스트. 빈 리스트면 매치 실패 → 필터 탈락.

    - NFKC 정규화 + casefold 로 한글/영문/특수 호환 문자 처리
    - 1차: 공백 구분 다중 키워드는 모두(AND) 한 필드 안에 등장해야 매치 (엄격)
    - 2차: 1차 실패 시 공백 제거한 키워드가 공백 제거한 필드의 substring이면 매치
      (예: "삼성라이온즈" 입력 ↔ "삼성 라이온즈" 필드, "롯데 자이언츠" 입력 ↔ "#롯데자이언츠")
      — 띄어쓰기/연결 표기 차이만 관용. 오타는 여전히 걸러냄.
    - 3차: 한글/CJK 문자 단위 AND 매칭 (어순·띄어쓰기 무관)
      (예: "꿀템추천" ↔ "추천 꿀템 모음", "美食推荐" ↔ "推荐美食给大家")
    - TikTok fuzzy match 로 딸려 들어온 무관 영상 제거 목적
    """
    if not keyword:
        return ["*"]

    def _norm(s: Any) -> str:
        s = unicodedata.normalize("NFKC", str(s or "")).casefold()
        # 악센트/발음부호 제거: é→e, ü→u, ñ→n, ё→е (유럽어·러시아어 대응)
        return ''.join(ch for ch in unicodedata.normalize("NFD", s)
                       if unicodedata.category(ch) != 'Mn')

    nkw = _norm(keyword)
    tokens = [t for t in nkw.split() if t]
    if not tokens:
        return ["*"]

    # 2차 매칭용 — 공백 제거 키워드 (한 글자 이하면 2차 매칭 스킵: 너무 광범위)
    concat_kw = nkw.replace(" ", "")
    relaxed_enabled = len(concat_kw) >= 2

    # 3차 매칭용 — 한글/CJK 개별 문자 토큰 (어순 무관 AND)
    flex_chars = list({ch for ch in nkw if _is_flex_char(ch)})
    flex_enabled = len(flex_chars) >= 2

    author = aweme.get("author") or {}
    fields = {
        "desc": aweme.get("desc") or "",
        "author_nickname": author.get("nickname") or "",
        "author_unique_id": author.get("unique_id") or author.get("uniqueId") or "",
        "hashtags": " ".join(_hashtags_from_aweme(aweme)),
    }

    matched: list[str] = []
    for name, val in fields.items():
        nval = _norm(val)
        # 1차: 기존 엄격 토큰 AND 매칭
        if all(tok in nval for tok in tokens):
            matched.append(name)
            continue
        # 2차: 공백 무시 substring 매칭 (엄격 매칭 실패 시에만)
        if relaxed_enabled and concat_kw in nval.replace(" ", ""):
            matched.append(name)
            continue
        # 3차: 한글/CJK 문자 단위 AND 매칭 (어순 무관)
        if flex_enabled and all(ch in nval for ch in flex_chars):
            matched.append(name)
    return matched


def _aweme_unique_id(raw: dict, aweme: dict, fallback: str) -> str:
    """item_list 원소는 루트에 id만 있고 aweme_id가 없는 경우가 많음 — str(None) 중복 방지."""
    vid = aweme.get("aweme_id") or raw.get("id") or aweme.get("id")
    if vid is not None and str(vid).strip() and str(vid) != "None":
        return str(vid)
    return fallback


def _hashtags_from_aweme(aweme: dict) -> list[str]:
    """틱톡킬라 tiktok.ts — item.hashtags 배열."""
    tags: list[str] = []
    for t in aweme.get("text_extra") or []:
        if isinstance(t, dict):
            name = t.get("hashtag_name")
            if name:
                tags.append(str(name))
    if tags:
        return tags
    desc = aweme.get("desc") or ""
    for m in re.finditer(r"#([^#\s]+)", desc):
        tags.append(m.group(1))
    return tags


def _safe_int(val: Any) -> int:
    try:
        if val is None or val == "":
            return 0
        return int(float(val))
    except (TypeError, ValueError):
        return 0


def _statistics_merged(aweme: dict, raw: dict) -> dict[str, Any]:
    """틱톡킬라 tiktok.ts — item.views / likes / comments / shares (statistics·stats 변종 병합)."""
    merged: dict[str, Any] = {}
    for src in (
        aweme.get("statistics"),
        raw.get("statistics"),
        aweme.get("stats"),
        raw.get("stats"),
    ):
        if isinstance(src, dict):
            merged.update(src)
    return merged


def _stat_int(st: dict[str, Any], *keys: str) -> int:
    for k in keys:
        if k in st and st[k] is not None:
            return _safe_int(st[k])
    return 0


def _first_url_from_named_keys(obj: Any, keys: tuple[str, ...]) -> str | None:
    if not isinstance(obj, dict):
        return None
    for k in keys:
        if k not in obj:
            continue
        urls = _extract_urls_from_media_value(obj.get(k))
        if urls:
            return sorted(urls, key=_addr_block_sort_key)[0]
    return None


def _uploaded_at_seconds(aweme: dict, raw: dict | None = None) -> int:
    """tiktok.ts: createTime = parseInt(uploadedAt) * 1000 → uploadedAt는 Unix 초.
    TikTok API 스키마 변동 대비 — aweme / raw / 중첩 dict 전부 훑어 create_time 류 필드 탐색."""

    CANDIDATE_KEYS = (
        "create_time", "createTime", "createdAt", "created_at",
        "publish_time", "publishTime", "uploaded_at", "uploadedAt",
        "timestamp",
    )

    def from_dict(d: Any) -> int:
        if not isinstance(d, dict):
            return 0
        for k in CANDIDATE_KEYS:
            v = d.get(k)
            if v is None or v == "":
                continue
            try:
                n = int(v)
            except (TypeError, ValueError):
                continue
            if n <= 0:
                continue
            return n // 1000 if n > 10_000_000_000 else n
        return 0

    # 1순위: aweme 본체
    n = from_dict(aweme)
    if n:
        return n
    # 2순위: raw 본체 (aweme_info 파생 케이스에 raw가 상위일 때)
    n = from_dict(raw) if raw is not None else 0
    if n:
        return n
    # 3순위: aweme 하위 중첩 dict들 얕게 탐색
    if isinstance(aweme, dict):
        for v in aweme.values():
            if isinstance(v, dict):
                n = from_dict(v)
                if n:
                    return n
    return 0
