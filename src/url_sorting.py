"""URL 정렬·호스트·도메인 티어 유틸 — play_addr 후보 순위 계산.

main.py 에서 추출 (Phase 1 리팩터). 로직 불변, 위치만 이동.
"""
from __future__ import annotations


def _hostname_lower(url: str) -> str:
    """정렬·티어용 호스트만 추출. 미디어 URL 출력에는 사용하지 않음(urlparse 미사용 — 쿼리 손상 경로 차단)."""
    u = url.strip()
    low = u.lower()
    if not (low.startswith("http://") or low.startswith("https://")):
        return ""
    try:
        rest = u.split("://", 1)[1]
        authority = rest.split("/", 1)[0].split("?", 1)[0]
        if "@" in authority:
            authority = authority.rsplit("@", 1)[-1]
        host = authority
        if host.startswith("["):
            end = host.find("]")
            if end != -1:
                host = host[1:end]
        elif ":" in host:
            host = host.rsplit(":", 1)[0]
        return host.lower()
    except Exception:
        return ""


def _query_tail_len(url: str) -> int:
    """? 이후 쿼리·인증 파라미터 길이 (길수록 토큰·서명이 풍부한 편)."""
    u = url.strip()
    i = u.find("?")
    if i < 0:
        return 0
    tail = u[i + 1 :]
    if "#" in tail:
        tail = tail.split("#", 1)[0]
    return len(tail)


def _query_param_count(url: str) -> int:
    """쿼리 스트링에 붙은 파라미터 개수 (&로 구분). 상세·인증 필드가 많을수록 큼."""
    u = url.strip()
    i = u.find("?")
    if i < 0:
        return 0
    q = u[i + 1 :]
    if "#" in q:
        q = q.split("#", 1)[0]
    if not q:
        return 0
    return q.count("&") + 1


def _domain_tier_and_prime_rank(h: str) -> tuple[int, int]:
    """hostname 문자열을 받아 (tier, prime_rank)를 한 번에 반환 — _addr_block_sort_key 전용.
    v16-webapp-prime 은 IPRoyal 프록시 경유 시 IP 불일치로 403이 많아 후순위. tiktokcdn-us/eu 우선."""
    if not h:
        return 30, 2
    if h in ("v16m.tiktokcdn-us.com", "v45.tiktokcdn-eu.com"):
        return 0, 2
    if h.endswith(".tiktokcdn-us.com") or h.endswith(".tiktokcdn-eu.com"):
        return 0, 2
    if "webapp-prime" in h and h.endswith(".tiktok.com"):
        prime = 0 if "v16-webapp-prime" in h else 1
        return 1, prime
    if "webapp" in h and h.endswith(".tiktok.com"):
        return 2, 2
    if "tiktokcdn.com" in h or "bytefcdn" in h or "tiktok.com" in h:
        return 3, 2
    return 4, 2


def _tiktok_auth_param_score(u: str) -> int:
    """인증 파라미터 밀도(점수만). URL 원문 u는 변경하지 않음."""
    ul = u.lower()
    n = 0
    if "btag=" in ul:
        n += 1
    if "bti=" in ul:
        n += 1
    if "&rc=" in ul or "?rc=" in ul:
        n += 1
    if "mstoken=" in ul:
        n += 1
    if "&a=" in ul:
        n += 1
    elif "?" in u:
        q = u.split("?", 1)[1]
        if "#" in q:
            q = q.split("#", 1)[0]
        if q.lower().startswith("a="):
            n += 1
    return n


def _url_codec_rank(url_lower: str) -> int:
    """URL 경로의 `_h264_`, `_bytevc1_`, `_bytevc2_` 패턴에서 코덱 추출.

    낮을수록 범용 재생. TikTok CDN URL 경로에 `v[서명]_[코덱]_[해상도]p_[비트레이트]`
    형태로 코덱이 박혀있어 API 필드 없이도 식별 가능 (yt-dlp 방식).
    """
    if "_h264_" in url_lower or "/h264/" in url_lower:
        return 0
    if "_avc1_" in url_lower:
        return 0
    if "_bytevc1_" in url_lower:
        return 1  # H.265 ByteDance 변종, 최신 기기 재생 가능
    if "_h265_" in url_lower or "_hevc_" in url_lower:
        return 1
    if "_bytevc2_" in url_lower:
        return 3  # H.266 ByteDance 변종 — Windows 기본 플레이어 미지원
    return 2  # 코덱 정보 없음 — 중립


def _url_watermark_rank(url_lower: str) -> int:
    """URL 쿼리·경로에서 워터마크 유무 판별. 0=확실히 없음, 1=모름, 2=있을 가능성."""
    if "unwatermarked" in url_lower or "watermark=0" in url_lower:
        return 0
    if "watermark=1" in url_lower or "wmlogo" in url_lower or "logo_name=tiktok" in url_lower:
        return 2
    return 1


def _addr_block_sort_key(u: str) -> tuple[int, int, int, int, int, int, int, int, int, int, int]:
    """코덱 호환성 → 워터마크 → 티어 → webapp-prime → btag → URL·쿼리·인증 밀도 → 포맷.

    코덱·워터마크를 최상위 정렬키로 둬서 bytevc2·워터마크 URL이 어떤 경우에도 후보
    상위로 올라오지 않게 함. 그 아래는 기존 도메인/품질 우선순위 유지.
    """
    ul = u.lower()
    codec_rank = _url_codec_rank(ul)
    wm_rank = _url_watermark_rank(ul)
    h = _hostname_lower(u)  # 한 번만 파싱 — tier·prime_rank 모두 재사용
    tier, prime_var = _domain_tier_and_prime_rank(h)
    btag_rank = 0 if "btag=" in ul else 1
    ln = len(u)
    q = _query_tail_len(u)
    auth = _tiktok_auth_param_score(u)
    npar = _query_param_count(u)
    m3u8 = 1 if ".m3u8" in ul else 0
    mp4ish = 0 if (".mp4" in ul or "/video/tos/" in ul) else 1
    return (codec_rank, wm_rank, tier, prime_var, btag_rank, -ln, -q, -auth, -npar, m3u8, mp4ish)
