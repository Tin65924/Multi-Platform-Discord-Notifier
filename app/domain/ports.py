"""Ports — interfaces the application layer depends on.

Infrastructure implements these; application code only ever sees the Protocol.
All protocols are structural (no inheritance needed) and runtime-checkable
where a test wants to assert an adapter conforms.
"""
from datetime import datetime
from typing import Protocol, runtime_checkable

from .entities import Creator, CreatorCard, SweepRecord
from .result import CheckResult


@runtime_checkable
class LiveChecker(Protocol):
    """One platform's live check. Must never raise — inconclusive checks
    return INCONCLUSIVE instead."""
    platform: str  # "tiktok" | "youtube" | "kick"
    fallback_room_prefix: str  # e.g. "live-", "live-yt-", "live-kk-"

    async def check(self, handle: str) -> CheckResult:
        ...

    def drop(self, handle: str) -> None:
        """Evict a cached client (stuck-heal only). Must close cleanly."""
        ...


@runtime_checkable
class Notifier(Protocol):
    """Delivers a pre-built Discord payload. True = accepted."""
    async def send(self, payload: dict) -> bool:
        ...


class Clock(Protocol):
    """Injectable time source (tests freeze it; prod uses UTC now)."""
    def now(self) -> datetime:
        ...


class SubscriptionRepo(Protocol):
    """Creator reads/writes for one sweep. No sessions leak out.

    Session economy mirrors the legacy poller: one short session per method.
    Methods that also touch LiveSession rows (note_not_found, mark_offline,
    mark_seen_live, record_live) do so in the SAME session, exactly like the
    legacy per-creator session did — so per-creator session counts are unchanged.
    """
    async def snapshot(self, platform: str) -> list[Creator]:
        """All enabled creators on a platform (one short read)."""
        ...

    async def check_in(self, sub_id: int, at: datetime) -> CreatorCard | None:
        """Stamp last_checked_at and return the full card. None = removed mid-sweep."""
        ...

    async def mark_not_found(self, sub_id: int, at: datetime) -> None:
        """is_live=False (+ stamp first_not_found_at if unset) and close open sessions."""
        ...

    async def clear_not_found(self, sub_id: int) -> None:
        ...

    async def mark_offline(self, sub_id: int, at: datetime) -> None:
        """is_live=False and close open sessions."""
        ...

    async def mark_seen_live(self, sub_id: int, platform: str, handle: str,
                             room_id: str, at: datetime) -> None:
        """is_live=True and ensure the open session row (dedup/cooldown paths)."""
        ...

    async def record_live(self, sub_id: int, platform: str, handle: str,
                          room_id: str, at: datetime, did_notify: bool) -> None:
        """last_room_id/last_live_at/is_live (+last_notified_at when did_notify)
        and ensure the open session row (stamped notified when did_notify)."""
        ...

    async def update_avatar(self, sub_id: int, avatar_url: str, at: datetime) -> None:
        ...


class SweepLogSink(Protocol):
    """Bulk log writer — one flush per sweep, own transaction."""
    async def flush(self, rows: list[SweepRecord]) -> None:
        ...


class ProfileLookup(Protocol):
    """Best-effort profile fetch (avatar + id anchor). None on block/fail."""
    async def fetch(self, handle: str) -> dict | None:
        ...
