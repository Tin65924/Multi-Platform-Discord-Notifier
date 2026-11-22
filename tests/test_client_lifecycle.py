"""Bounded resource use, real TikTokLive routes, and deterministic shutdown.

All HTTP goes through MockTransport; these tests never contact TikTok,
Discord, or Neon. No headless browser is launched.
"""
import asyncio
import gc
import tracemalloc
from unittest.mock import AsyncMock
from types import SimpleNamespace

import httpx
import pytest

from app.infrastructure.checkers import tiktok as tt


@pytest.fixture
def clients(monkeypatch):
    created = []
    real_client = tt.TikTokWebClient

    def install(handler):
        def factory(**kwargs):
            kwargs["httpx_kwargs"]["transport"] = httpx.MockTransport(handler)
            client = real_client(**kwargs)
            created.append(client)
            return client
        monkeypatch.setattr(tt, "TikTokWebClient", factory)
        return created

    monkeypatch.setattr(tt._settings, "TIKTOK_SESSION_ID", "")
    monkeypatch.setattr(tt._settings, "TT_COOKIE_PROVIDER", "off")
    return install


def response(request, *, live=False, room="123", missing=False):
    data = {"liveRoom": {"status": 2 if live else 4}}
    if not missing:
        data["user"] = {"roomId": room}
    return httpx.Response(200, json={"message": "", "data": data})


@pytest.mark.parametrize("size", [0, -1, 5])
def test_pool_size_is_bounded(size):
    with pytest.raises(ValueError):
        tt.TikTokChecker(size)


def test_actual_library_routes_preserve_status_and_room_identity(clients):
    def handler(request):
        name = request.url.params["uniqueId"].lstrip("@")
        assert request.url.params["referer"] == f"https://www.tiktok.com/@{name}/live"
        if name == "missing":
            return httpx.Response(200, json={"message": "user_not_found"})
        if name == "blocked":
            return httpx.Response(403, text="blocked")
        if name == "timeout":
            raise httpx.ReadTimeout("test timeout")
        return response(request, live=name != "offline", room="456", missing=name == "fallback")
    created = clients(handler)

    async def run():
        checker = tt.TikTokChecker()
        try:
            results = await asyncio.gather(*(checker.is_live(name) for name in
                                            ("@LIVE", "offline", "missing", "blocked", "timeout", "fallback")))
            assert [(r.username, r.is_live, r.error, r.room_id) for r in results] == [
                ("live", True, None, "456"), ("offline", False, None, None),
                ("missing", False, "not_found", None), ("blocked", False, "check_failed", None),
                ("timeout", False, "check_failed", None), ("fallback", True, None, "live-fallback"),
            ]
            assert len(created) == 2
        finally:
            await checker.aclose()
        assert all(c.httpx_client.is_closed for c in created)
    asyncio.run(run())


def test_creator_churn_has_constant_clients_and_bounded_retained_heap(clients):
    created = clients(response)

    async def run():
        checker = tt.TikTokChecker()
        tracemalloc.start()
        try:
            for i in range(100):
                assert (await checker.is_live(f"warm{i}")).error is None
                await asyncio.sleep(0)
            gc.collect()
            before = tracemalloc.get_traced_memory()[0]
            for i in range(1500):
                assert (await checker.is_live(f"creator{i}")).error is None
                checker.drop(f"creator{i}")
                checker.evict_missing(set())
                await asyncio.sleep(0)  # emulate I/O; allow cancelled timer cleanup
            gc.collect()
            retained = tracemalloc.get_traced_memory()[0] - before
            assert len(created) == 2
            assert retained < 1024 * 1024, f"Retained Python heap grew by {retained} bytes"
            assert checker.pool_status()["in_use"] == 0
        finally:
            tracemalloc.stop()
            await checker.aclose()
        assert checker.pool_status()["allocated"] == 0
        assert all(c.httpx_client.is_closed for c in created)
    asyncio.run(run())


def test_waiters_do_not_allocate_and_cancellation_returns_lease(clients):
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def handler(request):
            entered.set()
            await release.wait()
            return response(request)

        created = clients(handler)
        checker = tt.TikTokChecker(pool_size=1)
        active = asyncio.create_task(checker.is_live("active"))
        await entered.wait()
        waiters = [asyncio.create_task(checker.is_live(f"waiting{i}")) for i in range(50)]
        await asyncio.sleep(0)
        assert len(created) == 1
        checker.drop("active")
        assert not created[0].httpx_client.is_closed
        for task in waiters:
            task.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        active.cancel()
        with pytest.raises(asyncio.CancelledError):
            await active
        release.set()
        assert (await checker.is_live("after_cancel")).error is None
        await checker.aclose()
        assert created[0].httpx_client.is_closed
    asyncio.run(run())


def test_shutdown_waits_for_active_check_and_rejects_queued_work(clients):
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()
        requests = 0

        async def handler(request):
            nonlocal requests
            requests += 1
            entered.set()
            await release.wait()
            return response(request)

        created = clients(handler)
        checker = tt.TikTokChecker(pool_size=1)
        active = asyncio.create_task(checker.is_live("active"))
        await entered.wait()
        queued = asyncio.create_task(checker.is_live("queued"))
        closing = asyncio.create_task(checker.aclose())
        await asyncio.sleep(0)
        assert not closing.done()
        assert not created[0].httpx_client.is_closed
        release.set()
        await active
        assert (await queued).error == "check_failed"
        await closing
        await checker.aclose()  # idempotent
        assert requests == 1
        assert (await checker.is_live("closed")).error == "check_failed"
        checker.start()
        assert (await checker.is_live("restarted")).error is None
        await checker.aclose()
        assert len(created) == 2
        assert all(c.httpx_client.is_closed for c in created)
    asyncio.run(run())


def test_auth_uses_supported_session_api_and_scoped_cookies(clients, monkeypatch):
    from app.infrastructure.checkers import cookies
    monkeypatch.setattr(tt._settings, "TIKTOK_SESSION_ID", "test-session")
    monkeypatch.setattr(cookies, "get_cookies", AsyncMock(return_value={}))
    created = clients(response)
    # clients() only replaces the constructor; fixture setup cleared credentials earlier.
    async def run():
        checker = tt.TikTokChecker(pool_size=1)
        try:
            await checker.is_live("example")
            assert created[0].cookies.get("sessionid", domain=".tiktok.com") == "test-session"
            monkeypatch.setattr(cookies, "get_cookies", AsyncMock(return_value={"msToken": "test-token"}))
            await checker.is_live("example")
            assert created[0].cookies.get("msToken", domain=".tiktok.com") == "test-token"
        finally:
            await checker.aclose()
    asyncio.run(run())


def test_shutdown_attempts_other_slots_when_one_close_fails(clients, monkeypatch):
    created = clients(response)
    async def run():
        checker = tt.TikTokChecker()
        await checker.is_live("one")
        await checker.is_live("two")
        real_close = created[0].close
        monkeypatch.setattr(created[0], "close", AsyncMock(side_effect=RuntimeError("close failed")))
        with pytest.raises(RuntimeError, match="close failed"):
            await checker.aclose()
        assert created[1].httpx_client.is_closed
        monkeypatch.setattr(created[0], "close", real_close)
        await checker.aclose()
        assert all(c.httpx_client.is_closed for c in created)
    asyncio.run(run())


def test_lifespan_closes_all_resources_even_on_error(monkeypatch):
    import app.main as main
    from app.infrastructure.checkers import youtube

    monkeypatch.setattr(main, "init_db", AsyncMock())
    monkeypatch.setattr(main.settings, "TT_COOKIE_PROVIDER", "off")
    entered = None
    async def poll():
        entered.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(main, "poll_loop", poll)
    fake_tiktok = tt.TikTokChecker()
    monkeypatch.setattr(main, "tiktok_checker", fake_tiktok)
    monkeypatch.setattr(main, "engine", SimpleNamespace(dispose=AsyncMock()))

    async def run():
        nonlocal entered
        entered = asyncio.Event()
        with pytest.raises(RuntimeError, match="lifespan error"):
            async with main.lifespan(main.app):
                await entered.wait()
                profile_client = tt._profile_http()
                yt_client = youtube.checker._shared()
                raise RuntimeError("lifespan error")
        assert profile_client.is_closed and yt_client.is_closed
        assert tt._profile_client is None and youtube.checker._client is None
        assert main.poll_task is None
        assert fake_tiktok.pool_status()["closing"]
        main.engine.dispose.assert_awaited_once()
    asyncio.run(run())


def test_poll_loop_can_restart_after_cancellation_during_sleep(monkeypatch):
    from app.infrastructure.scheduler import loop
    async def cycle():
        done.set()
        return ({"checked": 0, "notified": 0},) * 3
    monkeypatch.setattr(loop, "poll_cycle", cycle)
    monkeypatch.setattr(loop, "maintenance", AsyncMock())
    monkeypatch.setattr(loop, "should_recycle", lambda *args: False)
    monkeypatch.setattr(loop.settings, "CHECK_INTERVAL_SECONDS", 3600)
    monkeypatch.setattr(loop, "_is_running", False)
    async def run():
        nonlocal done
        for _ in range(2):
            done = asyncio.Event()
            task = asyncio.create_task(loop.poll_loop())
            await asyncio.wait_for(done.wait(), timeout=2)
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            assert loop._is_running is False
            assert loop.state.sweep_in_progress is False
            assert loop.state.next_sweep_at is None
    done = None
    asyncio.run(run())
