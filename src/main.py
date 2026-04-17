# src/main.py — TikTok Video Download Actor 진입점
import asyncio
import os
import time
from typing import Any

from curl_cffi import AsyncSession
from apify import Actor

from session import (
    CURL_IMPERSONATE,
    cookie_dict as _cookie_dict,
    ensure_ttwid,
)
from constants import (
    ACTOR_DOWNLOAD_REVISION,
    KV_SESSION_KEY,
    KV_SESSION_TTL_SEC,
    VERBOSE_DIAG,
    _FIXED_UA,
)
from download_pipeline import process_video


async def _kv_load_session(actor: Actor) -> dict:
    """KV Store에서 세션 캐시(ttwid·msToken·device_id)를 불러옵니다."""
    try:
        store = await actor.open_key_value_store(name="tiktok-session-shared")
        data = await store.get_value(KV_SESSION_KEY)
        if not isinstance(data, dict):
            return {}
        saved_at = data.get("saved_at", 0)
        if time.time() - saved_at > KV_SESSION_TTL_SEC:
            actor.log.info("[kv_cache] 세션 캐시 만료 → 새로 수집합니다.")
            return {}
        if not (data.get("ttwid") or "").strip() or not (data.get("tt_chain_token") or "").strip():
            actor.log.info("[kv_cache] ttwid/tt_chain_token 누락 → 불완전 세션")
            return {}
        actor.log.info(
            f"[kv_cache] 세션 캐시 히트 "
            f"(age={(int(time.time()) - saved_at) // 60}분)"
        )
        return data
    except Exception as e:
        actor.log.warning(f"[kv_cache] 로드 실패: {e}")
        return {}


async def _kv_save_session(actor: Actor, client: Any) -> None:
    """현재 세션 쿠키를 KV Store에 저장."""
    try:
        if getattr(client, "_tt_auth_failed", False):
            actor.log.info("[kv_cache] 인증 실패 → 세션 저장 스킵")
            try:
                store = await actor.open_key_value_store(name="tiktok-session-shared")
                await store.set_value(KV_SESSION_KEY, None)
            except Exception:
                pass
            return
        ck = _cookie_dict(client)
        ttwid = ck.get("ttwid", "").strip()
        tt_chain_token = ck.get("tt_chain_token", "").strip()
        ms_token = (ck.get("msToken") or getattr(client, "_tt_ms_token", "") or "").strip()
        device_id = (getattr(client, "_tt_device_id", "") or "").strip()
        if not ttwid or not tt_chain_token:
            return
        payload = {
            "ttwid": ttwid,
            "tt_chain_token": tt_chain_token,
            "msToken": ms_token,
            "device_id": device_id,
            "saved_at": int(time.time()),
        }
        store = await actor.open_key_value_store(name="tiktok-session-shared")
        await store.set_value(KV_SESSION_KEY, payload)
        actor.log.info("[kv_cache] 세션 저장 완료")
    except Exception as e:
        actor.log.warning(f"[kv_cache] 저장 실패: {e}")


async def main():
    _t_boot = time.monotonic()
    async with Actor() as actor:
        actor.log.info(f"TikTok Video Download Actor revision={ACTOR_DOWNLOAD_REVISION}")
        input_data = await actor.get_input() or {}

        video_urls = input_data.get("videoUrls") or []
        if not video_urls:
            actor.log.error("다운로드할 영상 URL을 입력해주세요.")
            return

        max_video_size_mb = int(input_data.get("maxVideoSizeMb", 30) or 30)
        max_video_size_mb = max(1, min(max_video_size_mb, 30))
        max_size_bytes = max_video_size_mb * 1024 * 1024

        max_concurrent = max(1, min(int(input_data.get("maxConcurrentDownloads", 3) or 3), 10))
        ms_token_input = (input_data.get("msToken") or "").strip() or None
        use_proxy = bool(input_data.get("useProxy", True))

        # 프록시 설정
        if use_proxy:
            user_proxy_cfg = input_data.get("proxyConfiguration") or {}
            _pg = user_proxy_cfg.get("apifyProxyGroups") or ["RESIDENTIAL"]
            _proxy_kwargs: dict[str, Any] = {"groups": _pg}
            proxy_config = await actor.create_proxy_configuration(**_proxy_kwargs)
            proxy = await proxy_config.new_url() if proxy_config else None
            actor.log.info(f"[proxy] groups={_pg} url_ok={bool(proxy)}")
        else:
            proxy_config = None
            proxy = None
            actor.log.info("[proxy] disabled")

        # KV 세션 캐시 로드
        kv_session = await _kv_load_session(actor)

        actor.log.info(
            f"영상 {len(video_urls)}개 다운로드 시작 | "
            f"max_size={max_video_size_mb}MB | concurrent={max_concurrent} | "
            f"proxy={'on' if proxy else 'off'} | "
            f"kv_cache={'hit' if kv_session.get('ttwid') else 'miss'}"
        )

        async with AsyncSession() as client:
            client._tt_proxy = proxy

            # KV 캐시 세션 복원
            if kv_session.get("ttwid"):
                try:
                    client.cookies.set("ttwid", kv_session["ttwid"], domain=".tiktok.com")
                except Exception:
                    pass
            if kv_session.get("tt_chain_token"):
                try:
                    client.cookies.set("tt_chain_token", kv_session["tt_chain_token"], domain=".tiktok.com")
                except Exception:
                    pass
            if kv_session.get("msToken"):
                client._tt_ms_token = kv_session["msToken"]
                try:
                    client.cookies.set("msToken", kv_session["msToken"], domain=".tiktok.com")
                except Exception:
                    pass
            if kv_session.get("device_id"):
                client._tt_device_id = kv_session["device_id"]

            # 병렬 prefetch: Railway msToken + TikTok 세션 웜업을 동시에 진행.
            # 두 호출 모두 client 에 쓰는 키가 겹치지 않고(msToken vs ttwid/tt_chain_token)
            # 파이프라인 진입 전에 모두 준비되므로, 순차 대비 ~3초 절감.
            async def _prefetch_ms_token() -> None:
                existing_ms = (getattr(client, "_tt_ms_token", "") or "").strip()
                if len(existing_ms) >= 140 or ms_token_input:
                    return
                try:
                    from mstoken_remote import fetch_remote_ms_token
                    _rmt = await fetch_remote_ms_token(client, actor)
                    if _rmt.value and len(_rmt.value) >= 140:
                        try:
                            client.cookies.set("msToken", _rmt.value, domain=".tiktok.com")
                        except Exception:
                            pass
                except Exception as e:
                    actor.log.warning(f"[mstoken] 원격 획득 실패: {type(e).__name__}: {e}")

            async def _prewarm_session() -> None:
                try:
                    await ensure_ttwid(client, _FIXED_UA, actor)
                except Exception as e:
                    actor.log.warning(f"[session] 사전 웜업 실패: {type(e).__name__}: {e}")

            await asyncio.gather(_prefetch_ms_token(), _prewarm_session())

            # 배치 다운로드
            sem = asyncio.Semaphore(max_concurrent)
            results = []

            async def _process_one(url: str) -> None:
                async with sem:
                    result = await process_video(
                        actor, client, url, max_size_bytes,
                        ms_token_override=ms_token_input,
                    )
                    if result:
                        results.append(result)

            # 5분 타임아웃
            _DEADLINE_SEC = 300
            try:
                await asyncio.wait_for(
                    asyncio.gather(*[_process_one(url) for url in video_urls]),
                    timeout=_DEADLINE_SEC,
                )
            except asyncio.TimeoutError:
                actor.log.warning(
                    f"[DEADLINE] {_DEADLINE_SEC}s 도달 — 완료된 결과만 반환"
                )

            # Dataset에 결과 push — 과금 공정성: 성공 건만 push.
            # 실패 건은 각 파이프라인 경로에서 이미 error/warning 으로 로그됨.
            if results:
                successful = [r for r in results if r.get("downloadStatus") == "success"]
                failed = [r for r in results if r.get("downloadStatus") != "success"]

                if successful:
                    await actor.push_data(successful)

                if failed:
                    sample = [
                        {"url": r.get("inputUrl") or r.get("id"), "error": r.get("error")}
                        for r in failed[:5]
                    ]
                    actor.log.warning(
                        f"[pricing] 실패 {len(failed)}건 dataset 제외(과금 없음) sample={sample}"
                    )

                actor.log.info(
                    f"완료: {len(successful)}/{len(results)} 성공 "
                    f"(전체 {len(video_urls)}개 요청, dataset push={len(successful)})"
                )
            else:
                actor.log.warning("다운로드 결과 없음")

            await _kv_save_session(actor, client)

        # X-Bogus signer 정리
        try:
            from xbogus import shutdown_signer
            shutdown_signer()
        except Exception:
            pass

        elapsed = time.monotonic() - _t_boot
        actor.log.info(f"전체 소요시간: {elapsed:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
