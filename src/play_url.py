"""play_addr / download_addr / bitrate / 썸네일 URL 후보 추출·정렬.

main.py 에서 추출 (Phase 1 리팩터). 로직 불변, 위치만 이동.
url_sorting 에만 의존.
"""
from __future__ import annotations

from typing import Any

from url_sorting import _addr_block_sort_key, _url_codec_rank, _url_watermark_rank

# 데이터셋에 넣는 재생 URL 후보 개수(ORB·403 시 소비자가 다음 항목 시도).
PLAY_URL_CANDIDATES_MAX = 12


def _is_http_url(text: str) -> bool:
    t = text.strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _coerce_media_url(value: Any) -> str | None:
    """play_addr `url_list` 원문 그대로 — urllib.parse·재인코딩·슬라이스·정규식 가공 없음.

    btag·rc·bti 등 쿼리 바이트가 바뀌면 403. str은 API가 준 문자열을 그대로 통과시키고,
    bytes만 UTF-8로 디코드(그 외 변형 없음).
    """
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            s = value.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            s = value.decode("utf-8", errors="replace")
    elif isinstance(value, str):
        s = value
    elif isinstance(value, (int, float)):
        s = str(value)
    else:
        # dict/list 등은 str()로 직렬화하면 쿼리가 사라지므로 무시 (url_list 항목은 별도 언랩)
        return None
    if not s or not _is_http_url(s):
        return None
    return s


def _urls_from_url_list_item(item: Any) -> list[str]:
    """url_list 원소가 str·dict·list일 때 play_addr와 동일 규칙으로 URL 수집 (인증 쿼리 유지)."""
    acc: list[str] = []
    if item is None:
        return acc
    if isinstance(item, str):
        u = _coerce_media_url(item)
        if u:
            acc.append(u)
        return acc
    if isinstance(item, (list, tuple)):
        for sub in item:
            acc.extend(_urls_from_url_list_item(sub))
        return acc
    if isinstance(item, dict):
        for k in ("url_list", "urlList", "UrlList"):
            v = item.get(k)
            if isinstance(v, list):
                for sub in v:
                    acc.extend(_urls_from_url_list_item(sub))
                break
        for k in ("url", "Url", "src", "uri", "URI"):
            v = item.get(k)
            if isinstance(v, str):
                u = _coerce_media_url(v)
                if u:
                    acc.append(u)
        for nk in (
            "play_addr",
            "playAddr",
            "PlayAddr",
            "download_addr",
            "downloadAddr",
            "DownloadAddr",
        ):
            sub = item.get(nk)
            if sub is not None:
                acc.extend(_urls_from_addr_block(sub))
        return acc
    return acc


def _url_list_from_block(block: Any) -> list[str]:
    """dict 블록에서 url_list·urlList·UrlList만 추출. 쿼리 전량 보존, 긴 파라미터·prime 도메인 우선 정렬."""
    if not isinstance(block, dict):
        return []
    ul = None
    for k in ("url_list", "urlList", "UrlList"):
        v = block.get(k)
        if isinstance(v, list):
            ul = v
            break
    if ul is None:
        return []
    out: list[str] = []
    for u in ul:
        out.extend(_urls_from_url_list_item(u))
    if not out:
        return []
    return sorted(out, key=_addr_block_sort_key)


def _collect_urls_from_addr_dict(d: dict) -> list[str]:
    """addr 블록 dict에서 url_list + 단일 url 계열 (dict형 url_list 항목 언랩)."""
    acc: list[str] = []
    for k in ("url_list", "urlList", "UrlList"):
        v = d.get(k)
        if isinstance(v, list):
            for item in v:
                acc.extend(_urls_from_url_list_item(item))
            break
    for k in ("url", "Url", "src", "uri", "URI"):
        v = d.get(k)
        if isinstance(v, str):
            cu = _coerce_media_url(v)
            if cu:
                acc.append(cu)
    return acc


def _urls_from_addr_block(block: Any) -> list[str]:
    """play_addr·playAddr·download_addr 블록 전용. ? 뒤 인증 파라미터 전량 보존.

    url_list에 여러 개면 쿼리가 가장 길고 상세한 주소를 최우선(str/dict/list 방어).
    """
    acc: list[str] = []

    if block is None:
        return []
    if isinstance(block, str):
        u = _coerce_media_url(block)
        return [u] if u else []
    if isinstance(block, dict):
        acc.extend(_collect_urls_from_addr_dict(block))
    elif isinstance(block, list):
        for item in block:
            if isinstance(item, dict):
                acc.extend(_collect_urls_from_addr_dict(item))
            else:
                acc.extend(_urls_from_url_list_item(item))

    seen: set[str] = set()
    uniq: list[str] = []
    for s in acc:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    if not uniq:
        return []
    return sorted(uniq, key=_addr_block_sort_key)


_NESTED_ADDR_KEYS = (
    "play_addr",
    "playAddr",
    "PlayAddr",
    "download_addr",
    "downloadAddr",
    "DownloadAddr",
    "play_url",
    "playUrl",
    "download_url",
    "downloadUrl",
    "video_addr",
    "videoAddr",
)


def _dict_media_urls(d: dict) -> list[str]:
    """dict 한 겹에서 url_list·단일 url·중첩 playAddr/downloadAddr 등 (얕은 한 단계)."""
    out: list[str] = []
    out.extend(_url_list_from_block(d))
    for k in (
        "url",
        "Url",
        "src",
        "uri",
        "URI",
        "download_addr",
        "downloadAddr",
        "play_addr",
        "playAddr",
        "dynamic_cover",
        "dynamicCover",
        "origin_cover",
        "originCover",
        "cover",
        "Cover",
        "thumb_url",
        "thumbUrl",
        "thumbnail_url",
        "thumbnailUrl",
        "poster",
        "poster_url",
        "posterUrl",
    ):
        u = d.get(k)
        if isinstance(u, str):
            cu = _coerce_media_url(u)
            if cu:
                out.append(cu)
    for nk in _NESTED_ADDR_KEYS:
        sub = d.get(nk)
        if isinstance(sub, dict):
            out.extend(_url_list_from_block(sub))
            for kk in (
                "url",
                "Url",
                "src",
                "uri",
                "URI",
                "download_addr",
                "downloadAddr",
            ):
                u = sub.get(kk)
                if isinstance(u, str):
                    cu = _coerce_media_url(u)
                    if cu:
                        out.append(cu)
        elif isinstance(sub, list):
            for item in sub:
                if isinstance(item, dict):
                    out.extend(_dict_media_urls(item))
                else:
                    out.extend(_urls_from_url_list_item(item))
        elif isinstance(sub, str):
            cu = _coerce_media_url(sub)
            if cu:
                out.append(cu)
    # 순서 유지 중복 제거
    seen: set[str] = set()
    deduped: list[str] = []
    for s in out:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


def _extract_urls_from_media_value(val: Any) -> list[str]:
    """문자열·dict·list에서 http(s) URL. 쿼리·서명 전량 보존 — play_addr와 동일 언랩 규칙."""
    out: list[str] = []
    if val is None:
        return out
    if isinstance(val, str):
        u = _coerce_media_url(val)
        return [u] if u else out
    if isinstance(val, dict):
        return _dict_media_urls(val)
    if isinstance(val, (list, tuple)):
        for item in val:
            if isinstance(item, dict):
                out.extend(_dict_media_urls(item))
            else:
                out.extend(_urls_from_url_list_item(item))
        return out
    return out


def _bit_rate_entries(video: dict) -> list[Any]:
    """TikTok video.bit_rate / bitRate / bitrateInfo 등 변종."""
    for k in ("bit_rate", "bitRate", "bitrateInfo", "bitrate_info"):
        br = video.get(k)
        if isinstance(br, list) and br:
            return br
    return []


# 코덱 호환성 랭크 — 낮을수록 범용 재생 가능성 높음.
# bytevc2(ByteVC2)는 Windows 기본 플레이어·대부분 브라우저 미지원 → 영상 안 나오고
# 오디오만 재생되는 증상의 주원인이라 마지막 순위로 밀어둠.
_CODEC_RANK = {
    "h264": 0,
    "avc1": 0,
    "h265": 1,
    "hevc": 1,
    "bytevc1": 2,  # ByteDance 변종 H.265
    "bytevc2": 3,  # ByteDance 변종 H.266 — 호환성 최악
}


def _codec_type(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    for k in ("codec_type", "codecType", "CodecType"):
        v = item.get(k)
        if isinstance(v, str):
            return v.strip().lower()
    return ""


def _bitrate_sort_key(item: Any) -> tuple[int, int]:
    """정렬: (코덱 호환성 랭크, 비트레이트) — h264 우선, 그 안에서 낮은 비트레이트 우선.

    bytevc2를 밀어내지 않으면 Windows 기본 플레이어에서 "음성만 나오는 영상" 이슈 발생.
    """
    if not isinstance(item, dict):
        return (99, 0)
    codec = _codec_type(item)
    # 모르는 코덱은 mid-risk(2)로 취급 — bytevc2(3)보다는 앞, h264/h265(0~1)보다는 뒤
    codec_rank = _CODEC_RANK.get(codec, 2)
    bitrate = 0
    for k in ("BitRate", "bit_rate", "bitrate"):
        v = item.get(k)
        if v is not None:
            try:
                bitrate = int(v)
                break
            except (TypeError, ValueError):
                pass
    return (codec_rank, bitrate)


def _play_url_candidates(video: dict) -> list[str]:
    """재생 URL 후보. play_addr 우선; 정렬은 _addr_block_sort_key
    (v16m US·webapp-prime·btag=·긴 URL·인증 밀도). MIRROR_MEDIA=False — 텍스트 URL만."""
    if not isinstance(video, dict):
        return []
    seen: set[str] = set()
    ordered: list[str] = []

    def extend_tier(raw: list[str]) -> None:
        for u in sorted(raw, key=_addr_block_sort_key):
            if u not in seen:
                seen.add(u)
                ordered.append(u)

    br = _bit_rate_entries(video)
    br_sorted = sorted(br, key=_bitrate_sort_key) if br else []

    # Tier A-0: play_addr_h264 — 앱 API가 명시적으로 h264 변종을 따로 제공할 때.
    # yt-dlp도 이 필드를 최우선으로 사용. 현재 웹 API엔 대개 없지만 있으면 무조건 안전.
    tier_a0: list[str] = []
    for pk in ("play_addr_h264", "playAddrH264", "PlayAddrH264"):
        if pk in video:
            tier_a0.extend(_urls_from_addr_block(video.get(pk)))
    for item in br_sorted:
        if not isinstance(item, dict):
            continue
        for pk in ("play_addr_h264", "playAddrH264", "PlayAddrH264"):
            if pk in item:
                tier_a0.extend(_urls_from_addr_block(item.get(pk)))
    extend_tier(tier_a0)

    # Tier A: 실제 스트리밍 블록 play_addr (download_addr보다 우선).
    # _addr_block_sort_key가 URL 경로의 _bytevc2_ 패턴을 penalty 처리하므로 안전.
    tier_a: list[str] = []
    for item in br_sorted:
        if not isinstance(item, dict):
            continue
        for pk in ("play_addr", "playAddr", "PlayAddr"):
            if pk in item:
                tier_a.extend(_urls_from_addr_block(item.get(pk)))
    for pk in ("play_addr", "playAddr", "PlayAddr"):
        if pk in video:
            tier_a.extend(_urls_from_addr_block(video.get(pk)))
    extend_tier(tier_a)

    # Tier B: 워터마크·저비트·플랫 play_* (백업보다 앞)
    tier_b_keys = (
        "play_url",
        "playUrl",
        "play_wm_addr",
        "playWmAddr",
        "play_addr_lowbr",
        "playAddrLowbr",
        "playApi",
        "play_api",
    )
    tier_b: list[str] = []
    for k in tier_b_keys:
        if k in video:
            tier_b.extend(_extract_urls_from_media_value(video.get(k)))
    extend_tier(tier_b)

    # Tier C: download_addr 계열 (백업)
    tier_c: list[str] = []
    for item in br_sorted:
        if not isinstance(item, dict):
            continue
        for dk in ("download_addr", "downloadAddr", "DownloadAddr"):
            if dk in item:
                tier_c.extend(_urls_from_addr_block(item.get(dk)))
    for dk in (
        "download_addr",
        "downloadAddr",
        "DownloadAddr",
        "download_url",
        "downloadUrl",
    ):
        if dk in video:
            tier_c.extend(_urls_from_addr_block(video.get(dk)))
    extend_tier(tier_c)

    # Tier D: 기타 비디오 URL 필드
    tier_d: list[str] = []
    for k in ("video_url", "videoUrl", "video_uri", "videoUri"):
        if k in video:
            tier_d.extend(_extract_urls_from_media_value(video.get(k)))
    extend_tier(tier_d)

    # Tier E: bitrate 행 전체 — 표준 키 밖 중첩·변종만 잡기(중복은 제거)
    tier_e: list[str] = []
    for item in br_sorted:
        if isinstance(item, dict):
            tier_e.extend(_extract_urls_from_media_value(item))
    extend_tier(tier_e)

    return ordered


def _best_preview_play_url(
    play_urls: list[str],
) -> tuple[str | None, str | None, list[str]]:
    """프리뷰·videoUrl용 URL: HLS가 아닌 직링크 우선(MP4·tos 등), 없으면 정렬 첫 항.

    반환: (primary_for_video_tag, any_m3u8_for_hls_field, candidates_slice)
    """
    if not play_urls:
        return None, None, []
    ul = [u for u in play_urls if u]
    if not ul:
        return None, None, []
    m3u8 = next((u for u in ul if ".m3u8" in u.lower()), None)
    # 한 번만 정렬 후 HLS 여부로 분기 — 중복 sorted() 호출 제거
    sorted_ul = sorted(ul, key=_addr_block_sort_key)
    primary = next((u for u in sorted_ul if ".m3u8" not in u.lower()), sorted_ul[0])
    candidates = sorted_ul[:PLAY_URL_CANDIDATES_MAX]
    return primary, m3u8, candidates


def _merged_video_block(raw: dict, aweme: dict) -> dict:
    """item.video와 aweme.video를 합쳐 검색 API 필드 분리 대응."""
    av = aweme.get("video") if isinstance(aweme.get("video"), dict) else {}
    rv = raw.get("video") if isinstance(raw.get("video"), dict) else {}
    if not av and not rv:
        return {}
    # aweme 쪽을 우선(덮어쓰기), 없는 키는 raw.video로 보강
    return {**rv, **av}


def _classify_url(url: str) -> dict:
    """URL 한 개를 코덱·워터마크 관점에서 분류.

    반환: {"codec": "h264|bytevc1|bytevc2|unknown", "watermark": "no|maybe|yes"}
    download_pipeline에서 선택된 CDN URL의 진단 로그에 사용.
    """
    if not url:
        return {"codec": "unknown", "watermark": "maybe"}
    ul = url.lower()
    codec_rank = _url_codec_rank(ul)
    wm_rank = _url_watermark_rank(ul)
    codec_map = {0: "h264", 1: "bytevc1", 2: "unknown", 3: "bytevc2"}
    wm_map = {0: "no", 1: "maybe", 2: "yes"}
    return {
        "codec": codec_map.get(codec_rank, "unknown"),
        "watermark": wm_map.get(wm_rank, "maybe"),
    }


def _first_safe_h264_url(play_urls: list[str]) -> str | None:
    """후보 URL 중 코덱이 h264이고 워터마크가 "예(yes)"가 아닌 첫 번째 URL.

    URL 경로에 `_h264_` 가 없으면 반환 안 함(코덱 확신 없는 URL 금지).
    """
    for u in play_urls:
        c = _classify_url(u)
        if c["codec"] == "h264" and c["watermark"] != "yes":
            return u
    return None


def _h264_url_from_bitrate(video: dict) -> str | None:
    """bit_rate 엔트리 중 codec_type 필드가 h264인 것의 play_addr URL.

    `_first_safe_h264_url` 은 URL 경로에 `_h264_` 가 박힌 경우만 신뢰하지만,
    TikTok CDN URL 중에는 코덱 힌트가 경로에 없는 케이스가 많음. bit_rate의 dict
    레벨 `codec_type` 필드는 신뢰도 높으니 이걸로 h264 엔트리를 찾고, 그 엔트리의
    play_addr URL 목록에서 워터마크 "yes" 아닌 것을 고름.

    URL 경로 기반 `_first_safe_h264_url` 과 상호 보완.
    """
    if not isinstance(video, dict):
        return None
    br = _bit_rate_entries(video)
    if not br:
        return None
    sorted_br = sorted(br, key=_bitrate_sort_key)
    for item in sorted_br:
        if not isinstance(item, dict):
            continue
        codec = _codec_type(item)
        if codec not in ("h264", "avc1"):
            continue
        urls: list[str] = []
        for pk in ("play_addr_h264", "playAddrH264", "play_addr", "playAddr", "PlayAddr"):
            if pk in item:
                urls.extend(_urls_from_addr_block(item.get(pk)))
        for u in sorted(urls, key=_addr_block_sort_key):
            if _classify_url(u)["watermark"] != "yes":
                return u
    return None


def _codec_summary(video: dict) -> list[tuple[str, int]]:
    """진단용: bit_rate 항목의 (codec, bitrate) 목록을 코덱 호환성 순으로 반환.

    Apify 로그에서 "어떤 코덱 후보가 있었고 어떤 게 골라졌는지" 확인용.
    """
    if not isinstance(video, dict):
        return []
    br = _bit_rate_entries(video)
    if not br:
        return []
    sorted_br = sorted(br, key=_bitrate_sort_key)
    out: list[tuple[str, int]] = []
    for item in sorted_br:
        if not isinstance(item, dict):
            continue
        codec = _codec_type(item) or "?"
        bitrate = 0
        for k in ("BitRate", "bit_rate", "bitrate"):
            v = item.get(k)
            if v is not None:
                try:
                    bitrate = int(v)
                    break
                except (TypeError, ValueError):
                    pass
        out.append((codec, bitrate))
    return out
