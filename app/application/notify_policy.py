"""Notify policy — pure pre-send verdicts for a detected live.

Verdict order (dedup → cooldown → send) is the canonical sweep behavior:
  1. same-room dedup (never on fallback rooms — they are not stable ids)
  2. 15-minute notify cooldown
  3. otherwise: proceed to send (session-notified + webhook outcome handled
     by the caller, in the same order the legacy code did them)

No I/O, no clock reads — `now` is passed in. Fully unit-tested.
"""
from datetime import datetime, timedelta, timezone

from ..domain.entities import NotifyDecision


def _aware(dt: datetime | None) -> datetime | None:
    """SQLite returns naive datetimes — treat them as UTC (same as legacy _aware)."""
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def decide_live(*, room_id: str, fallback_room: str, cached_room: str | None,
                last_room_id: str | None, last_notified_at: datetime | None,
                now: datetime, cooldown_seconds: int = 900) -> NotifyDecision:
    """Verdict for a LIVE check before any session/webhook work."""
    # Fallback rooms (fetch failed) never dedup — avoids missing a new live
    # that reuses the same "live-{handle}" string after an offline gap.
    if room_id != fallback_room and cached_room == room_id:
        return NotifyDecision(action="dedup", room_id=room_id, touch_live=True,
                              detail="dedup skip — same room")
    notified_at = _aware(last_notified_at)
    if last_room_id == room_id and notified_at is not None:
        if (now - notified_at) < timedelta(seconds=cooldown_seconds):
            return NotifyDecision(action="cooldown", room_id=room_id,
                                  cache_room=True, touch_live=True,
                                  detail="cooldown skip — 15m")
    return NotifyDecision(action="send", room_id=room_id)
