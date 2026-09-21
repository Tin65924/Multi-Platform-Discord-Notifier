"""Phase 2 integration: real poll_once (adapters + generic sweep) vs sqlite.

Network is stubbed at the seams (checker.is_live, fetch_tiktok_profile,
send_webhook); everything else — adapters, sweep, sessions, bulk log flush —
runs for real against a throwaway sqlite file.
"""
import asyncio
from datetime import datetime, timezone

import pytest

UTC = timezone.utc


@pytest.fixture()
def db():
    from app.db import async_session, init_db
    from app.models import GlobalSettings, Subscription, SweepLog
    from sqlalchemy import delete, select

    async def _setup():
        await init_db()
        import app.poller as poller
        poller._last_room_cache.clear()
        async with async_session() as s:
            await s.execute(delete(SweepLog))
            await s.execute(delete(Subscription))
            gs = await s.get(GlobalSettings, 1)
            if gs is None:
                gs = GlobalSettings(id=1, webhook_url="https://discord.com/api/webhooks/1/abc")
                s.add(gs)
            else:
                gs.webhook_url = "https://discord.com/api/webhooks/1/abc"
            await s.commit()
    asyncio.run(_setup())
    yield
    async def _teardown():
        async with async_session() as s:
            await s.execute(delete(SweepLog))
            await s.execute(delete(Subscription))
            await s.commit()
    asyncio.run(_teardown())


def _add_sub(handle):
    from app.db import async_session
    from app.models import Subscription

    async def _go():
        async with async_session() as s:
            s.add(Subscription(platform="tiktok", tiktok_username=handle))
            await s.commit()
    asyncio.run(_go())


def _run_poll(monkeypatch, is_live, notified_ok=True):
    import app.poller as poller
    from app.tiktok import LiveInfo

    async def fake_is_live(username):
        clean = username.strip().lstrip("@").lower()
        return is_live.get(clean, LiveInfo(is_live=False, username=clean))

    async def fake_profile(username):
        return None

    sent = []

    async def fake_send(url, payload, timeout=10):
        sent.append(payload)
        return notified_ok

    monkeypatch.setattr(poller.checker, "is_live", fake_is_live)
    monkeypatch.setattr(poller, "fetch_tiktok_profile", fake_profile)
    monkeypatch.setattr(poller, "send_webhook", fake_send)
    out = asyncio.run(poller.poll_once())
    return out, sent


def _sub(handle):
    from app.db import async_session
    from app.models import Subscription
    from sqlalchemy import select

    async def _go():
        async with async_session() as s:
            r = await s.execute(select(Subscription).where(Subscription.tiktok_username == handle))
            sub = r.scalar_one()
            return dict(is_live=sub.is_live, room=sub.last_room_id,
                        notified_at=sub.last_notified_at,
                        first_nf=sub.first_not_found_at, checked=sub.last_checked_at)
    return asyncio.run(_go())


def _logs(handle):
    from app.db import async_session
    from app.models import SweepLog
    from sqlalchemy import select

    async def _go():
        async with async_session() as s:
            r = await s.execute(select(SweepLog).where(SweepLog.handle == handle)
                                .order_by(SweepLog.id))
            return [(x.is_live, x.error, x.notified, x.detail) for x in r.scalars().all()]
    return asyncio.run(_go())


def test_offline_sweep_end_to_end(db, monkeypatch):
    _add_sub("offline_anna")
    out, sent = _run_poll(monkeypatch, {})
    assert (out["checked"], out["notified"]) == (1, 0)
    assert sent == []
    assert _sub("offline_anna")["checked"] is not None
    assert _logs("offline_anna") == [(False, None, False, "offline")]


def test_live_notify_then_dedup(db, monkeypatch):
    from app.tiktok import LiveInfo
    _add_sub("live_bob")
    out, sent = _run_poll(monkeypatch, {"live_bob": LiveInfo(True, "room7", "live_bob")})
    assert (out["checked"], out["notified"]) == (1, 1)
    assert len(sent) == 1
    assert _sub("live_bob")["room"] == "room7"
    # second sweep, same room -> dedup, no second send
    out, sent = _run_poll(monkeypatch, {"live_bob": LiveInfo(True, "room7", "live_bob")})
    assert (out["checked"], out["notified"]) == (1, 0)
    assert sent == []
    rows = _logs("live_bob")
    assert rows[0][2] is True and rows[1][3] == "dedup skip — same room"


def test_not_found_tracked_end_to_end(db, monkeypatch):
    from app.tiktok import LiveInfo
    _add_sub("ghost_renamed")
    out, sent = _run_poll(
        monkeypatch, {"ghost_renamed": LiveInfo(False, None, "ghost_renamed", "not_found")})
    assert (out["checked"], out["notified"]) == (1, 0)
    assert _sub("ghost_renamed")["first_nf"] is not None
    assert _logs("ghost_renamed")[0][1] == "not_found"
