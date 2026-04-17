# src/constants.py
# tiktok-download 전용 상수 + 검색 액터 공유 상수.
import os


def _bool_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


# 검색 액터 공유 상수
SEARCH_API_URL = "https://www.tiktok.com/api/search/item/full/"

# 다운로드 액터 전용
VIDEO_DETAIL_API_URL = "https://www.tiktok.com/api/item/detail/"
ACTOR_DOWNLOAD_REVISION = "20260417_v1"

# KV Store 레코드 최대 크기 (9MB)
MAX_KV_RECORD_BYTES = 9 * 1024 * 1024

# 다운로드 타임아웃 (초)
DOWNLOAD_TIMEOUT_SEC = 120

# 프리뷰용 릴레이 베이스
_DEFAULT_TIKTOK_PREVIEW_PROXY_BASE = "https://proxyapify-production-d4c5.up.railway.app/?url="
PROXY_BASE = (
    os.environ.get("TIKTOK_PREVIEW_PROXY_BASE") or _DEFAULT_TIKTOK_PREVIEW_PROXY_BASE
).strip()

VERBOSE_DIAG = _bool_env("TIKTOK_VERBOSE_DIAG")
_PLAY_URL_DIAG_ENV = _bool_env("TIKTOK_PLAY_URL_DIAG")

# KV Store 세션 캐싱
KV_SESSION_KEY = "tiktok_session_cache"
KV_SESSION_TTL_SEC = 12 * 3600

_TT_TRACE_HEADER_KEYS = (
    "x-tt-logid",
    "x-tt-trace-id",
    "x-tt-pba-trace-id",
    "x-tt-trace-tag",
    "x-tt-request-tag",
    "x-bd-auth",
    "x-ss-dp",
    "x-tt-zhihu",
)

_FIXED_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
