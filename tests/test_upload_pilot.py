import asyncio
import json
from collections import Counter
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app import uploads as u

HANDLE = "pilot"
OLD = "7000000000000000001"
NEW = "7000000000000000002"
PIN = "7000000000000000003"
HOOK = "https://discord.com/api/webhooks/22/test-token"


def profile(ids, owner="123"):
    return {"source": {"data": {"/embed/@pilot": {
        "isReady": True, "isError": False,
        "userInfo": {"privateAccount": False, "uniqueId": HANDLE, "id": owner},
        "videoList": [{"id": v, "authorUniqueId": HANDLE, "privateItem": False} for v in ids],
    }}}}


def video(vid, at):
    return {"__DEFAULT_SCOPE__": {"webapp.video-detail": {"itemInfo": {"itemStruct": {
        "id": vid, "author": {"uniqueId": HANDLE, "id": "123"}, "createTime": at,
    }}}}}


def html(script, state):
    return httpx.Response(200, text=f'<script id="{script}" type="application/json">{json.dumps(state)}</script>')


@pytest.fixture
def pilot(tmp_path, monkeypatch):
    from app.infrastructure.persistence.database import Base
    engine = create_async_engine("sqlite+aiosqlite:///" + (tmp_path / "pilot.db").as_posix())
    factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(u, "async_session", factory)
    monkeypatch.setattr(u.settings, "TIKTOK_UPLOAD_WEBHOOK_URL", HOOK)
    monkeypatch.setattr(u.settings, "DASHBOARD_WEBHOOK_URL", "https://discord.com/api/webhooks/11/live")
    monkeypatch.setattr(u.settings, "RELAY_URL", "")
    monkeypatch.setattr(u.settings, "RELAY_SECRET", "")
    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with factory.begin() as db:
            db.add(u.Subscription(id=1, platform="tiktok", tiktok_username=HANDLE))
        await engine.dispose()
    asyncio.run(setup())
    yield factory
    asyncio.run(engine.dispose())


@pytest.fixture(params=["direct", "relay"])
def upload_payload(request, pilot, monkeypatch):
    relay = request.param == "relay"
    url = "https://relay.example.test" if relay else HOOK
    if relay:
        monkeypatch.setattr(u.settings, "RELAY_URL", url)
        monkeypatch.setattr(u.settings, "RELAY_SECRET", "fixture-secret")
        monkeypatch.setattr(u.settings, "TIKTOK_UPLOAD_WEBHOOK_URL", "")
    def unpack(req):
        assert str(req.url).split("?")[0].rstrip("/") == url
        body = json.loads(req.content)
        if relay:
            assert body["kind"] == "upload" and "secret" not in body
            assert req.headers["authorization"] == "Bearer fixture-secret"
            return body["payload"]
        return body
    return unpack


@pytest.mark.parametrize("outcome", ["ok", "timeout", "429"])
def test_full_flow(pilot, monkeypatch, outcome, upload_payload):
    now = [1000]
    monkeypatch.setattr(u, "time", SimpleNamespace(time=lambda: now[0]))
    ids = [OLD]
    sends = []
    dates = {PIN: 100, NEW: 1050}
    def transport(req):
        if req.method == "POST":
            assert upload_payload(req)["allowed_mentions"] == {"parse": []}
            sends.append(req)
            if outcome == "timeout":
                raise httpx.ReadTimeout("lost response")
            return httpx.Response(429 if outcome == "429" else 200)
        if "/embed/" in req.url.path:
            return html("__FRONTITY_CONNECT_STATE__", profile(ids))
        vid = req.url.path.split("/")[-1]
        return html("__UNIVERSAL_DATA_FOR_REHYDRATION__", video(vid, dates[vid]))
    async def run():
        counts = Counter()
        # New HTTP clients simulate worker restarts; the database persists.
        for delta, visible in [(0,[OLD]), (100,[PIN,NEW,NEW,OLD]), (200,[OLD]), (300,[NEW,OLD,PIN])]:
            ids[:] = visible
            now[0] = 1000 + delta
            async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as c:
                await u.check(c, 1, HANDLE, "deliver", counts)
        assert len(sends) == 1
        async with pilot() as db:
            rows = (await db.scalars(select(u.UploadNotice))).all()
            assert [(r.video_id,r.status) for r in rows] == [(NEW, {"ok":"sent","429":"rejected","timeout":"uncertain"}[outcome])]
    asyncio.run(run())


def test_observation_and_parallel_claim(pilot):
    async def run():
        calls = []
        def accept(req):
            calls.append(req)
            return httpx.Response(200)
        async with httpx.AsyncClient(transport=httpx.MockTransport(accept)) as c:
            now = int(u.time.time())
            assert await u.notice(c, 1, HANDLE, OLD, now, "observe") == "observed"
            assert await u.notice(c, 1, HANDLE, OLD, now, "deliver") == "duplicate"
            outcomes = await asyncio.gather(*(u.notice(c, 1, HANDLE, NEW, now, "deliver") for _ in range(25)))
            assert Counter(outcomes) == {"sent":1,"duplicate":24}
            assert len(calls) == 1
    asyncio.run(run())


def test_rebaseline_and_monotonic_checkpoint(pilot):
    async def run():
        change = u.advance
        assert await change(1,HANDLE,"a",[OLD],1000,"observe") is None
        assert await change(1,HANDLE,"a",[NEW],900,"observe") is None
        assert await change(1,HANDLE,"a",[NEW],1100,"observe") == (1000,[NEW])
        assert await change(1,HANDLE,"b",[NEW],1200,"observe") is None
        assert await change(1,HANDLE,"b",[OLD],1300,"deliver") is None
        assert await change(1,HANDLE,"b",[PIN],2400,"deliver") is None
    asyncio.run(run())


def test_destination_isolation(pilot, monkeypatch):
    gs = SimpleNamespace(notifications_enabled=True, webhook_url="https://discordapp.com/api/webhooks/22/other")
    assert u.destination(gs) is None
    gs.webhook_url = ""
    gs.notifications_enabled = False
    assert u.destination(gs) is None
    gs.notifications_enabled = True
    monkeypatch.setattr(u.settings, "RELAY_URL", "https://example.test/live")
    assert u.destination(gs) is None


def test_unusable_pages_preserve_checkpoint(pilot):
    async def run():
        counts = Counter()
        bad = profile([OLD])
        bad["source"]["data"]["/embed/@pilot"]["videoList"][0]["authorUniqueId"] = "other"
        for reply in [httpx.Response(403), httpx.Response(200,text="challenge"),
                      html("__FRONTITY_CONNECT_STATE__",bad), httpx.Response(200,content=b"x"*(u.MAX_BYTES+1))]:
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda req: reply)) as c:
                await u.check(c, 1, HANDLE, "deliver", counts)
        assert counts == {"feed_unverified":4}
        async with pilot() as db:
            assert await db.scalar(select(u.Subscription.upload_checkpoint)) is None
    asyncio.run(run())



def test_accelerated_upload_soak(pilot, monkeypatch):
    """Override UPLOAD_SOAK_CHECKS for a longer offline run using the real checkpoint."""
    import gc
    import os
    from tools.memory_soak import current_rss_mb

    runs = int(os.getenv("UPLOAD_SOAK_CHECKS", "50"))
    assert 1 <= runs <= 100000
    clock = [2000000000]
    monkeypatch.setattr(u, "time", SimpleNamespace(time=lambda: clock[0]))
    current = [OLD]
    posted = {}
    sends = []

    def serve(request):
        if request.method == "POST":
            sends.append(json.loads(request.content)["content"].split("/")[-1])
            return httpx.Response(204)
        if "/embed/" in request.url.path:
            return html("__FRONTITY_CONNECT_STATE__", profile(current))
        vid = request.url.path.split("/")[-1]
        return html("__UNIVERSAL_DATA_FOR_REHYDRATION__", video(vid, posted.get(vid, 100)))

    async def run():
        counts = Counter()
        samples = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as client:
            for i in range(runs):
                clock[0] += 300
                if i and i % 10 == 0:
                    vid = str(int(NEW) + i)
                    posted[vid] = clock[0] - 10
                current[:] = [OLD] + list(posted)[-5:]
                if i % 2:
                    current.reverse()
                    current.append(PIN)
                await u.check(client, 1, HANDLE, "deliver", counts)
                if i == 49 or i == runs - 1:
                    gc.collect()
                    samples.append(current_rss_mb())
        assert sends == list(posted)
        async with pilot() as db:
            persisted = (await db.scalars(select(u.UploadNotice.video_id))).all()
            assert set(persisted) == set(sends)
        print("upload soak:", runs, "checks;", len(sends), "distinct posts; RSS MiB:", samples)
    asyncio.run(run())


def test_missing_date_or_changed_author_never_sends(pilot):
    async def run():
        await u.advance(1, HANDLE, "123", [OLD], int(u.time.time()) - 20, "deliver")
        def serve(req):
            assert req.method == "GET"
            if "/embed/" in req.url.path:
                return html("__FRONTITY_CONNECT_STATE__", profile([NEW, PIN]))
            if req.url.path.endswith(NEW):
                return httpx.Response(200, text="no metadata")
            data = video(PIN, int(u.time.time()) - 1)
            data["__DEFAULT_SCOPE__"]["webapp.video-detail"]["itemInfo"]["itemStruct"]["author"]["id"] = "999"
            return html("__UNIVERSAL_DATA_FOR_REHYDRATION__", data)
        counts = Counter()
        async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as client:
            await u.check(client, 1, HANDLE, "deliver", counts)
        assert counts["date_unverified"] == 2
    asyncio.run(run())

def test_anchored_creator_cannot_be_replaced(pilot):
    async def run():
        async with pilot.begin() as db:
            sub = await db.get(u.Subscription, 1)
            sub.tiktok_user_id = "original"
        await u.advance(1, HANDLE, "someone-else", [NEW], 1100, "deliver")
        async with pilot() as db:
            assert await db.scalar(select(u.Subscription.upload_checkpoint)) is None
    asyncio.run(run())


def test_creator_tracking_preferences(pilot, monkeypatch):
    from datetime import datetime, timezone
    from unittest.mock import AsyncMock
    from app.infrastructure.persistence import repos
    from app.presentation.api import subscriptions as routes
    from app.schemas import SubscriptionStyleIn

    monkeypatch.setattr(repos, "async_session", pilot)
    monkeypatch.setattr(routes, "log_audit", AsyncMock())

    async def run():
        repo = repos.SubscriptionRepo()
        now = datetime.now(timezone.utc)
        actor = {"username": "operator"}
        await u.advance(1, HANDLE, "123", [OLD], 1000, "deliver")
        async with pilot() as db:
            await routes.update_style(1, SubscriptionStyleIn(track_live=False), db, actor)
        assert await repo.snapshot("tiktok") == []
        assert await repo.check_in(1, now) is None
        assert await u.advance(1, HANDLE, "123", [NEW], 1100, "deliver") == (1000, [NEW])
        async with pilot() as db:
            await routes.update_style(1, SubscriptionStyleIn(track_live=True, track_uploads=False), db, actor)
        assert [c.id for c in await repo.snapshot("tiktok")] == [1]
        assert await u.advance(1, HANDLE, "123", [PIN], 1200, "deliver") is None
        assert await u.notice(None, 1, HANDLE, PIN, 1200, "deliver") == "removed"
        async with pilot() as db:
            await routes.update_style(1, SubscriptionStyleIn(track_uploads=True), db, actor)
        assert await u.advance(1, HANDLE, "123", [NEW, PIN], 1300, "deliver") is None
    asyncio.run(run())


@pytest.mark.parametrize("case", ["ok", "empty", "blocked", "timeout"])
def test_manual_upload_uses_separate_destination_and_preserves_history(pilot, monkeypatch, case, upload_payload):
    from fastapi import HTTPException
    from unittest.mock import AsyncMock
    from app.presentation.api import subscriptions as routes

    sends = []
    def serve(request):
        if request.method == "GET":
            if case == "blocked":
                return httpx.Response(403)
            # Older pinned video first. Duplicate IDs must not duplicate the send.
            ids = [] if case == "empty" else [OLD, NEW, NEW]
            return html("__FRONTITY_CONNECT_STATE__", profile(ids))
        assert request.url.params["wait"] == "true"
        sends.append(upload_payload(request))
        if case == "timeout":
            raise httpx.ReadTimeout("lost response")
        return httpx.Response(200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(serve))
    monkeypatch.setattr(routes.httpx, "AsyncClient", lambda **kwargs: client)
    monkeypatch.setattr(routes, "log_audit", AsyncMock())

    async def run():
        async with pilot() as db:
            call = routes.send_test_upload(1, db, {"username": "tester"})
            if case == "ok":
                assert (await call)["video_url"].endswith("/" + NEW)
            else:
                with pytest.raises(HTTPException) as exc:
                    await call
                assert exc.value.status_code == (404 if case == "empty" else 502)
        assert 1 not in routes._upload_tests_in_progress
        async with pilot() as db:
            assert await db.scalar(select(u.Subscription.upload_checkpoint)) is None
            assert await db.scalar(select(u.UploadNotice.video_id)) is None
    asyncio.run(run())
    expected = {"content": f"Testing upload\n{u.ROOT}/@{HANDLE}/video/{NEW}", "allowed_mentions": {"parse": []}}
    assert sends == ([expected] if case in ("ok", "timeout") else [])
