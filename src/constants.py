# src/constants.py — tiktok-download 전용 상수.

# TikTok 웹 영상 상세 API (primary)
VIDEO_DETAIL_API_URL = "https://www.tiktok.com/api/item/detail/"
ACTOR_DOWNLOAD_REVISION = "20260420_v2"

# KV Store 세션 캐싱 (msToken·ttwid 등 웜업 결과)
KV_SESSION_KEY = "tiktok_session_cache"
KV_SESSION_TTL_SEC = 12 * 3600

_FIXED_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
