"""aweme/raw item 파싱 유틸 — 해시태그·통계·업로드 시각 등 순수 변환."""
from __future__ import annotations

import re
from typing import Any


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
