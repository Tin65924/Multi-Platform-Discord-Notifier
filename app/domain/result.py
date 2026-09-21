"""Check outcome taxonomy — the single source of truth for what a live check found.

Replaces stringly-typed `error="not_found" / "check_failed" / None` matching
scattered across the poller and checkers. Every platform adapter maps its raw
result to exactly one of these; the application layer branches on the enum.
"""
from dataclasses import dataclass
from enum import Enum


class CheckOutcome(Enum):
    LIVE = "live"              # definitively live right now (room_id may be a fallback)
    OFFLINE = "offline"        # definitively not live
    NOT_FOUND = "not_found"    # handle looks renamed/deleted (rename-follow tracks it)
    INCONCLUSIVE = "inconclusive"  # the check itself failed — keep last known state


@dataclass(frozen=True)
class CheckResult:
    """One platform check, normalized. `reason` preserves the raw detail
    (e.g. "check_failed", "blocked") for Logs without affecting branching."""
    outcome: CheckOutcome
    username: str = ""
    room_id: str | None = None
    reason: str | None = None


def from_legacy(is_live: bool, error: str | None, username: str = "",
                room_id: str | None = None) -> CheckResult:
    """Bridge from the legacy (is_live, error-string) shape to CheckResult.

    Used at checker boundaries during the migration (Phase 2 wires it in).
    Mapping is exact — no behavior change:
      error == "not_found"            -> NOT_FOUND (even if is_live, never happens)
      error set (anything else)       -> INCONCLUSIVE, reason preserved
      no error + live                 -> LIVE
      no error + not live             -> OFFLINE
    """
    if error == "not_found":
        return CheckResult(CheckOutcome.NOT_FOUND, username, room_id, error)
    if error:
        return CheckResult(CheckOutcome.INCONCLUSIVE, username, room_id, error)
    if is_live:
        return CheckResult(CheckOutcome.LIVE, username, room_id, None)
    return CheckResult(CheckOutcome.OFFLINE, username, room_id, None)
