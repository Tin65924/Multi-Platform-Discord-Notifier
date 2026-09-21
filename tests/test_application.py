"""Phase 2: notify-policy verdicts + generic sweep behavior (fakes, frozen clock).

These pin the legacy poll_once state machine BEFORE poller.py is rewired —
the same tests guard the migration.
"""
import asyncio
from datetime import datetime, timedelta, timezone

UTC = timezone.utc
NOW = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)


# --- decide_live -------------------------------------------------------------

def test_dedup_on_same_stable_room():
    from app.application.notify_policy import decide_live
    v = decide_live(room_id="room9", fallback_room="live-u", cached_room="room9",
                    last_room_id="room9", last_notified_at=NOW - timedelta(seconds=60),
                    now=NOW)
    assert v.action == "dedup" and v.touch_live and not v.cache_room
    assert v.detail == "dedup skip — same room"


def test_fallback_room_never_dedups():
    from app.application.notify_policy import decide_live
    # Same fallback string cached AND outside cooldown -> must still send
    # (fallback rooms are not stable ids; cooldown still applies to them).
    v = decide_live(room_id="live-u", fallback_room="live-u", cached_room="live-u",
                    last_room_id="live-u", last_notified_at=NOW - timedelta(seconds=901),
                    now=NOW)
    assert v.action == "send"
    # ...while a STABLE room in the same situation dedups.
    v = decide_live(room_id="room9", fallback_room="live-u", cached_room="room9",
                    last_room_id="room9", last_notified_at=NOW - timedelta(seconds=901),
                    now=NOW)
    assert v.action == "dedup"


def test_cooldown_inside_window_skips():
    from app.application.notify_policy import decide_live
    v = decide_live(room_id="r", fallback_room="live-u", cached_room=None,
                    last_room_id="r", last_notified_at=NOW - timedelta(seconds=899),
                    now=NOW)
    assert v.action == "cooldown" and v.cache_room and v.touch_live


def test_cooldown_expired_sends():
    from app.application.notify_policy import decide_live
    v = decide_live(room_id="r", fallback_room="live-u", cached_room=None,
                    last_room_id="r", last_notified_at=NOW - timedelta(seconds=901),
                    now=NOW)
    assert v.action == "send"


def test_new_room_sends():
    from app.application.notify_policy import decide_live
    v = decide_live(room_id="new", fallback_room="live-u", cached_room="old",
                    last_room_id="old", last_notified_at=NOW - timedelta(seconds=10),
                    now=NOW)
    assert v.action == "send"


# --- run_platform_sweep with fakes --------------------------------------------

from app.domain.entities import Creator, CreatorCard, WebhookTarget
from app.domain.result import CheckOutcome, CheckResult
from app.application.sweep import SweepConfig, run_platform_sweep


class FrozenClock:
    def __init__(self, at=NOW):
        self.at = at

    def now(self):
        return self.at


class FakeChecker:
    platform = "tiktok"
    fallback_room_prefix = "live-"

    def __init__(self, results):
        self.results = dict(results)  # handle -> CheckResult
        self.drops = []

    async def check(self, handle):
        return self.results.get(handle, CheckResult(CheckOutcome.OFFLINE, handle))

    def drop(self, handle):
        self.drops.append(handle)


class FakeRepo:
    """In-memory SubscriptionRepo. Records calls; applies legacy-equivalent state."""

    def __init__(self, cards):
        self.cards = {c.handle: c for c in cards}
        self.by_id = {c.id: c for c in cards}
        self.calls = []

    def _mut(self, sub_id, **kw):
        c = self.by_id[sub_id]
        self.by_id[sub_id] = c.__class__(**{**c.__dict__, **kw})
        self.cards[self.by_id[sub_id].handle] = self.by_id[sub_id]

    async def snapshot(self, platform):
        return [Creator(id=c.id, handle=c.handle, platform=c.platform, is_live=c.is_live,
                        last_live_at=c.last_live_at, last_room_id=c.last_room_id,
                        last_notified_at=c.last_notified_at,
                        first_not_found_at=c.first_not_found_at)
                for c in self.cards.values() if c.platform == platform]

    async def check_in(self, sub_id, at):
        self.calls.append(("check_in", sub_id))
        return self.by_id.get(sub_id)

    async def mark_not_found(self, sub_id, at):
        self.calls.append(("mark_not_found", sub_id))
        c = self.by_id[sub_id]
        self._mut(sub_id, is_live=False, first_not_found_at=c.first_not_found_at or at)

    async def clear_not_found(self, sub_id):
        self.calls.append(("clear_not_found", sub_id))
        self._mut(sub_id, first_not_found_at=None)

    async def mark_offline(self, sub_id, at):
        self.calls.append(("mark_offline", sub_id))
        self._mut(sub_id, is_live=False)

    async def mark_seen_live(self, sub_id, platform, handle, room_id, at):
        self.calls.append(("mark_seen_live", sub_id))
        self._mut(sub_id, is_live=True)

    async def record_live(self, sub_id, platform, handle, room_id, at, did_notify):
        self.calls.append(("record_live", sub_id, did_notify))
        kw = dict(is_live=True, last_room_id=room_id, last_live_at=at)
        if did_notify:
            kw["last_notified_at"] = at
        self._mut(sub_id, **kw)

    async def update_avatar(self, sub_id, avatar_url, at):
        self.calls.append(("update_avatar", sub_id))
        self._mut(sub_id, avatar_url=avatar_url)


class FakeSessions:
    def __init__(self, notified_rooms=()):
        self.notified_rooms = set(notified_rooms)
        self.opened = []
        self.closed = []

    async def ensure_open(self, sub_id, platform, handle, room_id, at):
        from app.domain.entities import SessionState
        self.opened.append(room_id)
        return SessionState(notified=room_id in self.notified_rooms)

    async def close_open(self, sub_id, at):
        self.closed.append(sub_id)


class FakeSink:
    def __init__(self):
        self.rows = None

    async def flush(self, rows):
        self.rows = list(rows)


class FakeNotifier:
    def __init__(self, ok=True):
        self.ok = ok
        self.sent = []

    async def send(self, payload):
        self.sent.append(payload)
        return self.ok


class FakeProfiles:
    def __init__(self, avatar="http://av/x.png"):
        self.avatar = avatar
        self.fetched = []

    async def fetch(self, handle):
        self.fetched.append(handle)
        return {"avatar_url": self.avatar} if self.avatar else None


def _card(i, handle, **kw):
    base = dict(id=i, handle=handle, platform="tiktok", is_live=False,
                last_live_at=None, last_room_id=None, last_notified_at=None,
                first_not_found_at=None)
    base.update(kw)
    return CreatorCard(**base)


def _run(repo, checker, sessions=None, notifier=None, profiles=None,
         cache=None, notify_on=True):
    sessions = sessions or FakeSessions()
    notifier = notifier if notifier is not None else FakeNotifier()
    sink = FakeSink()
    target = WebhookTarget(url="http://wh", message="{discord} is LIVE!")
    out = asyncio.run(run_platform_sweep(
        checker=checker, repo=repo, sessions=sessions, sink=sink,
        notifier=notifier, profiles=profiles,
        build_payload=lambda card, tgt: {"content": card.handle},
        target=target, notify_on=notify_on, room_cache={} if cache is None else cache,
        clock=FrozenClock(), config=SweepConfig(per_check_sleep=0)))
    return out, repo, sink, notifier, sessions


def test_offline_flips_card_and_logs():
    repo = FakeRepo([_card(1, "a", is_live=True)])
    out, repo, sink, notifier, sessions = _run(
        repo, FakeChecker({"a": CheckResult(CheckOutcome.OFFLINE, "a")}))
    assert (out["checked"], out["notified"]) == (1, 0)
    assert repo.by_id[1].is_live is False
    assert notifier.sent == []
    assert [(r.handle, r.is_live, r.notified) for r in sink.rows] == [("a", False, False)]
    assert sessions.closed == [1]


def test_fresh_live_notifies_and_caches():
    repo = FakeRepo([_card(1, "a")])
    cache = {}
    out, repo, sink, notifier, sessions = _run(
        repo, FakeChecker({"a": CheckResult(CheckOutcome.LIVE, "a", "room1")}),
        profiles=FakeProfiles(), cache=cache)
    assert (out["checked"], out["notified"]) == (1, 1)
    assert repo.by_id[1].last_room_id == "room1"
    assert repo.by_id[1].last_notified_at == NOW
    assert repo.by_id[1].avatar_url == "http://av/x.png"
    assert cache == {"tt:a": "room1"}
    assert len(notifier.sent) == 1
    assert sink.rows[0].notified is True


def test_dedup_skips_send_but_touches_live():
    repo = FakeRepo([_card(1, "a", is_live=True, last_room_id="room1")])
    out, repo, sink, notifier, sessions = _run(
        repo, FakeChecker({"a": CheckResult(CheckOutcome.LIVE, "a", "room1")}),
        cache={"tt:a": "room1"})
    assert (out["checked"], out["notified"]) == (1, 0)
    assert notifier.sent == []
    assert ("mark_seen_live", 1) in repo.calls
    assert sink.rows[0].detail == "dedup skip — same room"


def test_cooldown_skips_send():
    repo = FakeRepo([_card(1, "a", last_room_id="r", last_notified_at=NOW - timedelta(seconds=60))])
    cache = {}
    out, repo, sink, notifier, sessions = _run(
        repo, FakeChecker({"a": CheckResult(CheckOutcome.LIVE, "a", "r")}), cache=cache)
    assert (out["checked"], out["notified"]) == (1, 0)
    assert cache == {"tt:a": "r"}
    assert sink.rows[0].detail == "cooldown skip — 15m"


def test_already_notified_session_skips_send():
    repo = FakeRepo([_card(1, "a")])
    out, repo, sink, notifier, sessions = _run(
        repo, FakeChecker({"a": CheckResult(CheckOutcome.LIVE, "a", "roomX")}),
        sessions=FakeSessions(notified_rooms={"roomX"}))
    assert (out["checked"], out["notified"]) == (1, 0)
    assert notifier.sent == []
    assert sink.rows[0].detail == "already notified this room"


def test_not_found_tracks_and_clears_on_clean_read():
    repo = FakeRepo([_card(1, "a")])
    checker = FakeChecker({"a": CheckResult(CheckOutcome.NOT_FOUND, "a")})
    out, repo, sink, notifier, sessions = _run(repo, checker)
    assert repo.by_id[1].first_not_found_at == NOW
    assert repo.by_id[1].is_live is False
    assert sink.rows[0].error == "not_found"
    # next sweep: clean offline read clears the streak
    checker.results["a"] = CheckResult(CheckOutcome.OFFLINE, "a")
    out, repo, sink, notifier, sessions = _run(repo, checker)
    assert repo.by_id[1].first_not_found_at is None
    assert ("clear_not_found", 1) in repo.calls


def test_inconclusive_keeps_state():
    repo = FakeRepo([_card(1, "a", is_live=True)])
    out, repo, sink, notifier, sessions = _run(
        repo, FakeChecker({"a": CheckResult(CheckOutcome.INCONCLUSIVE, "a",
                                            reason="check_failed")}))
    assert repo.by_id[1].is_live is True  # untouched
    assert notifier.sent == []
    assert sink.rows[0].is_live is None and sink.rows[0].error == "check_failed"


def test_stuck_live_heals_client():
    repo = FakeRepo([_card(1, "a", is_live=True,
                           last_live_at=NOW - timedelta(hours=4))])
    checker = FakeChecker({"a": CheckResult(CheckOutcome.OFFLINE, "a")})
    _run(repo, checker)
    assert checker.drops == ["a"]


def test_removed_mid_sweep_skipped():
    repo = FakeRepo([_card(1, "a")])
    del repo.by_id[1]  # gone between snapshot and check-in
    out, repo, sink, notifier, sessions = _run(
        repo, FakeChecker({"a": CheckResult(CheckOutcome.OFFLINE, "a")}))
    assert out["checked"] == 1 and sink.rows == []


def test_sort_recent_live_first_and_order_returned():
    repo = FakeRepo([
        _card(1, "never"),
        _card(2, "recent", last_live_at=NOW - timedelta(hours=1)),
        _card(3, "older", last_live_at=NOW - timedelta(days=5)),
    ])
    out, *_ = _run(repo, FakeChecker({}))
    assert out["order"] == ["recent", "older", "never"]


def test_paused_notifications_mark_live_without_send():
    repo = FakeRepo([_card(1, "a")])
    out, repo, sink, notifier, sessions = _run(
        repo, FakeChecker({"a": CheckResult(CheckOutcome.LIVE, "a", "room1")}),
        notify_on=False)
    assert (out["checked"], out["notified"]) == (1, 0)
    assert repo.by_id[1].is_live is True  # card still flips live
    assert notifier.sent == []
    assert sink.rows[0].detail == "live but notifications paused"
