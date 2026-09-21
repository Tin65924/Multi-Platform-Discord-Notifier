"""Scheduler state — timing + order snapshot for /api/schedule.

Deliberately dependency-light (config only): both poller and the loop read
and write here without import cycles.
"""
import asyncio
from datetime import datetime

from ...config import get_settings

settings = get_settings()

# One cycle at a time, process-wide: the loop and external cron triggers
# share this. A trigger that finds a running cycle skips (busy) instead of
# overlapping it (overlap caused double notifications + connection pileup).
sweep_lock = asyncio.Lock()

next_sweep_at: datetime | None = None
last_sweep_at: datetime | None = None
sweep_in_progress: bool = False
last_sweep_order: list[str] = []  # usernames in last sort order (for per-creator ETA)


def set_last_sweep_order(order: list[str]) -> None:
    global last_sweep_order
    last_sweep_order = list(order)


def get_schedule_state() -> dict:
    """Snapshot for /api/schedule — cheap, no DB."""
    return {
        "next_sweep_at": next_sweep_at.isoformat() if next_sweep_at else None,
        "last_sweep_at": last_sweep_at.isoformat() if last_sweep_at else None,
        "in_progress": sweep_in_progress or sweep_lock.locked(),
        "interval": int(settings.CHECK_INTERVAL_SECONDS),
        "jitter": int(settings.CHECK_JITTER_SECONDS),
        "per_check_sleep": float(settings.PER_CHECK_SLEEP_SECONDS),
    }
