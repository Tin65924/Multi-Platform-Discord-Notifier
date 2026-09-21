"""Ports — interfaces the application layer depends on.

Infrastructure implements these; application code only ever sees the Protocol.
All protocols are structural (no inheritance needed) and runtime-checkable
where a test wants to assert an adapter conforms.
"""
from datetime import datetime
from typing import Protocol, runtime_checkable

from .entities import Creator, SweepRecord
from .result import CheckResult


@runtime_checkable
class LiveChecker(Protocol):
    """One platform's live check. Must never raise — inconclusive checks
    return INCONCLUSIVE instead."""
    platform: str  # "tiktok" | "youtube" | "kick"
    fallback_room_prefix: str  # e.g. "live-", "live-yt-", "live-kk-"

    async def check(self, handle: str) -> CheckResult:
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
    """Creator reads/writes for one sweep. No sessions leak out."""
    async def snapshot(self, platform: str) -> list[Creator]:
        """All enabled creators on a platform (one short read)."""
        ...

    async def touch_checked(self, sub_id: int, at: datetime) -> None:
        ...

    async def mark_not_found(self, sub_id: int, at: datetime) -> Creator:
        """Set is_live=False, stamp first_not_found_at if unset. Returns fresh row."""
        ...

    async def clear_not_found(self, sub_id: int) -> None:
        ...

    async def mark_offline(self, sub_id: int, at: datetime) -> None:
        ...

    async def mark_live(self, sub_id: int, room_id: str, at: datetime,
                        notified: bool) -> None:
        ...


class SweepLogSink(Protocol):
    """Bulk log writer — one flush per sweep, own transaction."""
    async def flush(self, rows: list[SweepRecord]) -> None:
        ...


class ProfileLookup(Protocol):
    """Best-effort profile fetch (avatar + id anchor). None on block/fail."""
    async def fetch(self, handle: str) -> dict | None:
        ...
