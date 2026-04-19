# src/constants.py — tiktok-download 전용 상수.

# TikTok 웹 영상 상세 API (primary)
VIDEO_DETAIL_API_URL = "https://www.tiktok.com/api/item/detail/"
ACTOR_DOWNLOAD_REVISION = "20260417_v1"

# TikWM 공개 API — 워터마크 없는 CDN URL(hdplay/play) 확보용. TikTok 웹 API와
# 병렬 호출해서 영상 URL만 TikWM 걸 우선 사용. 실패 시 기존 CDN URL 폴백.
TIKWM_API_URL = "https://tikwm.com/api/"
TIKWM_TIMEOUT_SEC = 5.0

# KV Store 세션 캐싱
KV_SESSION_KEY = "tiktok_session_cache"
KV_SESSION_TTL_SEC = 12 * 3600

_FIXED_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
