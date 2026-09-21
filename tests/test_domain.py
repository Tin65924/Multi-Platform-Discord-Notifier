"""Phase 1: domain taxonomy + ports. Pure — no DB, no network."""
from datetime import datetime, timezone

UTC = timezone.utc


def test_from_legacy_exact_mapping():
    from app.domain.result import CheckOutcome, from_legacy
    # live, clean
    r = from_legacy(True, None, "u", "room1")
    assert (r.outcome, r.room_id, r.reason) == (CheckOutcome.LIVE, "room1", None)
    # offline, clean
    r = from_legacy(False, None, "u")
    assert r.outcome == CheckOutcome.OFFLINE and r.reason is None
    # renamed/deleted handle — distinct from offline
    r = from_legacy(False, "not_found", "u")
    assert (r.outcome, r.reason) == (CheckOutcome.NOT_FOUND, "not_found")
    # every other error string is inconclusive with reason preserved
    for err in ("check_failed", "blocked", "fetch-failed", "TimeoutError", "ConnectError"):
        r = from_legacy(False, err, "u")
        assert r.outcome == CheckOutcome.INCONCLUSIVE and r.reason == err
    # live flag with an error still maps by error (never happens, but total)
    r = from_legacy(True, "check_failed", "u")
    assert r.outcome == CheckOutcome.INCONCLUSIVE


def test_check_result_immutable():
    from app.domain.result import CheckOutcome, CheckResult
    import dataclasses
    r = CheckResult(CheckOutcome.LIVE, "u")
    try:
        r.outcome = CheckOutcome.OFFLINE  # type: ignore[misc]
        assert False, "frozen dataclass must reject mutation"
    except dataclasses.FrozenInstanceError:
        pass


def test_entities_carry_sweep_fields():
    from app.domain.entities import Creator, SweepRecord
    now = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    c = Creator(id=1, handle="u")
    assert (c.platform, c.is_live, c.last_live_at) == ("tiktok", False, None)
    row = SweepRecord("tiktok", "u", 1, True, None, True, "r", "notified", 5, now)
    assert (row.notified, row.created_at) == (True, now)


def test_ports_structural_conformance():
    from app.domain.ports import LiveChecker, Notifier

    class FakeChecker:
        platform = "tiktok"
        fallback_room_prefix = "live-"

        async def check(self, handle: str):
            raise NotImplementedError

    class FakeNotifier:
        async def send(self, payload: dict) -> bool:
            return True

    assert isinstance(FakeChecker(), LiveChecker)
    assert isinstance(FakeNotifier(), Notifier)


def test_domain_has_no_third_party_imports():
    import pathlib
    banned = ("sqlalchemy", "httpx", "fastapi", "pydantic", "TikTokLive", "PIL")
    for mod in ("result.py", "entities.py", "ports.py", "__init__.py"):
        src = (pathlib.Path("app/domain") / mod).read_text(encoding="utf-8")
        for dep in banned:
            assert dep not in src, f"{mod} must stay dependency-free, found {dep}"
