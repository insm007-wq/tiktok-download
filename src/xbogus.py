"""X-Bogus 서명 생성 — Node subprocess 상주 방식.

`xbogus.js --serve` 모드로 Node 프로세스를 한 번 띄워 stdin 루프로 서명 요청을 보낸다.
기존 방식(매 요청마다 `subprocess.run`)에서 Node JIT 기동 비용(~200ms)이 사라진다.

사용:
    sig = get_x_bogus(query_string, user_agent)   # 기존 호환 API
    shutdown_signer()                              # Actor 종료 시 정리 (옵션)
"""
from __future__ import annotations

import os
import subprocess
import threading
from typing import Optional

_JS_PATH = os.path.join(os.path.dirname(__file__), "xbogus.js")

# 모듈 전역으로 하나의 Node 프로세스를 공유. 스레드 락으로 stdin/stdout 직렬화.
_proc: Optional[subprocess.Popen] = None
_lock = threading.Lock()


def _ensure_proc() -> subprocess.Popen:
    """Node 프로세스가 죽었거나 없으면 새로 띄운다. 호출은 _lock 안에서."""
    global _proc
    if _proc is not None and _proc.poll() is None:
        return _proc
    _proc = subprocess.Popen(
        ["node", _JS_PATH, "--serve"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,          # line-buffered
        text=True,
        encoding="utf-8",
    )
    return _proc


def get_x_bogus(query_string: str, user_agent: str) -> str:
    """X-Bogus 서명 문자열을 반환.

    상주 Node 프로세스에 "qs\\tua\\n" 한 줄을 쓰고 응답 한 줄을 읽는다.
    프로세스가 예기치 않게 죽으면 1회 재기동 후 재시도.
    실패가 지속되면 `subprocess.run` fallback으로 호환성 유지.
    """
    # 입력 안전성: TAB·개행 혼입 시 프로토콜이 깨지므로 UA에서만 치환 (qs는 URL-encoded라 안전)
    ua_safe = (user_agent or "").replace("\t", " ").replace("\n", " ")
    qs_safe = (query_string or "").replace("\n", "")

    for attempt in (1, 2):
        with _lock:
            try:
                proc = _ensure_proc()
                assert proc.stdin is not None and proc.stdout is not None
                proc.stdin.write(f"{qs_safe}\t{ua_safe}\n")
                proc.stdin.flush()
                line = proc.stdout.readline()
            except (BrokenPipeError, OSError):
                # 프로세스 파이프 손상 → 재기동
                _kill_proc_locked()
                line = ""

        if not line:
            if attempt == 1:
                continue  # 한 번 더 시도 (새 프로세스로)
            # 최후 fallback: 일회성 subprocess.run
            return _fallback_run(qs_safe, ua_safe)

        line = line.rstrip("\n")
        if line.startswith("ERR "):
            if attempt == 1:
                _kill_proc_locked()
                continue
            raise RuntimeError(f"xbogus --serve 오류: {line[4:]}")
        return line

    # 이 지점 도달 불가
    return _fallback_run(qs_safe, ua_safe)


def _fallback_run(qs: str, ua: str) -> str:
    """상주 프로세스가 계속 실패할 때 마지막 수단."""
    r = subprocess.run(
        ["node", _JS_PATH, qs, ua],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if r.returncode != 0:
        raise RuntimeError(f"xbogus.js fallback 오류: {r.stderr.strip()}")
    return r.stdout.strip()


def _kill_proc_locked() -> None:
    """_lock 안에서만 호출. 좀비 프로세스 정리."""
    global _proc
    if _proc is None:
        return
    try:
        if _proc.stdin:
            _proc.stdin.close()
    except Exception:
        pass
    try:
        _proc.kill()
    except Exception:
        pass
    _proc = None


def shutdown_signer() -> None:
    """Actor 종료 시 호출 — Node 프로세스 정리."""
    with _lock:
        _kill_proc_locked()
