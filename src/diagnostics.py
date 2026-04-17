"""단계별 진단/로그 헬퍼.

[STAGE:<name>] 태그 규격으로 모든 로그를 통일해 실패 지점을 즉시 식별 가능하게 한다.
기존 코드는 점진적으로 log_stage()로 전환한다.
"""
from __future__ import annotations

import time
from typing import Any


# Stage 이름 상수 — 오타 방지 + 자동완성
class Stage:
    INPUT = "INPUT"
    PROXY = "PROXY"
    SESSION = "SESSION"
    HTML_TOKEN = "HTML_TOKEN"
    SIGN = "SIGN"
    MSTOKEN = "MSTOKEN"
    FETCH = "FETCH"
    PARSE = "PARSE"
    FILTER = "FILTER"
    URL_SELECT = "URL_SELECT"
    PROBE = "PROBE"
    MIRROR = "MIRROR"
    PUSH = "PUSH"


def log_stage(actor: Any, stage: str, level: str, msg: str, **ctx: Any) -> None:
    """actor.log.<level>(f"[STAGE:{stage}] {msg} k=v ...")"""
    parts = [f"[STAGE:{stage}] {msg}"]
    if ctx:
        parts.append(" ".join(f"{k}={v!r}" for k, v in ctx.items()))
    getattr(actor.log, level)(" ".join(parts))


class StageError(Exception):
    """단계 태그를 보존하는 예외 — 상위에서 e.stage로 실패 지점 식별."""

    def __init__(self, stage: str, cause: BaseException | None = None, **ctx: Any):
        self.stage = stage
        self.cause = cause
        self.ctx = ctx
        ctx_str = " ".join(f"{k}={v!r}" for k, v in ctx.items())
        super().__init__(
            f"[STAGE:{stage}] {type(cause).__name__ if cause else 'error'}: "
            f"{cause!s} {ctx_str}".rstrip()
        )


class StageCounter:
    """키워드별 단계 집계 — 종료 시 [SUMMARY:kw] 한 줄 출력용."""

    def __init__(self, keyword: str):
        self.keyword = keyword
        self.started_at = time.monotonic()
        self.counters: dict[str, dict[str, int]] = {}
        self.stage_ms: dict[str, float] = {}

    def incr(self, stage: str, outcome: str = "ok", n: int = 1) -> None:
        self.counters.setdefault(stage, {}).setdefault(outcome, 0)
        self.counters[stage][outcome] += n

    def add_ms(self, stage: str, ms: float) -> None:
        self.stage_ms[stage] = self.stage_ms.get(stage, 0.0) + ms

    def summary(self, *, items: int = 0) -> str:
        elapsed = time.monotonic() - self.started_at
        parts = []
        for stage, buckets in self.counters.items():
            bits = ",".join(f"{k}={v}" for k, v in buckets.items())
            ms = self.stage_ms.get(stage)
            if ms:
                bits = f"{bits} {ms:.0f}ms"
            parts.append(f"{stage}=({bits})")
        return (
            f"[SUMMARY:{self.keyword}] "
            + " ".join(parts)
            + f" items={items} total={elapsed:.1f}s"
        )


class RunCounter:
    """run-level 집계 — Actor 종료 직전 [RUN_SUMMARY]."""

    def __init__(self):
        self.started_at = time.monotonic()
        self.keywords = 0
        self.ok = 0
        self.empty = 0
        self.stages_failed: dict[str, int] = {}

    def record_keyword(self, *, ok: bool, empty: bool = False) -> None:
        self.keywords += 1
        if ok:
            self.ok += 1
        if empty:
            self.empty += 1

    def record_stage_fail(self, stage: str) -> None:
        self.stages_failed[stage] = self.stages_failed.get(stage, 0) + 1

    def summary(self) -> str:
        elapsed = time.monotonic() - self.started_at
        sf = (
            "{"
            + ", ".join(f"{k}:{v}" for k, v in self.stages_failed.items())
            + "}"
            if self.stages_failed
            else "{}"
        )
        return (
            f"[RUN_SUMMARY] keywords={self.keywords} ok={self.ok} "
            f"empty={self.empty} stages_failed={sf} total={elapsed:.1f}s"
        )


def body_preview(content: bytes | None, max_chars: int = 200) -> str:
    """HTTP body의 안전한 미리보기 문자열."""
    if not content:
        return "(empty)"
    chunk = content[: max_chars + 4]
    try:
        s = chunk.decode("utf-8")
    except UnicodeDecodeError:
        return f"<binary {min(48, len(content))}B hex>{content[:48].hex()}"
    s = s[:max_chars].replace("\r\n", "\n").replace("\n", "\\n")
    if len(s) > max_chars:
        s = s[:max_chars] + "…"
    return repr(s)


def response_body_len(r: Any) -> int:
    try:
        return len(r.content or b"")
    except Exception:
        return 0
