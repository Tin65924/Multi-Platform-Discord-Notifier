"""Poll loop + memory guard — moved verbatim from poller.py (Phase 5).

Owns the sweep cadence, per-sweep logging and the 512MB recycle. Imports the
sweep machinery from poller (one direction only: loop -> poller -> repos).
"""
import asyncio
import logging
import random
from datetime import datetime, timezone, timedelta

from ...config import get_settings
from ...poller import _last_room_cache, maintenance, poll_cycle
from . import state

logger = logging.getLogger(__name__)
settings = get_settings()

_is_running = False


def rss_mb() -> float | None:
    """Process peak RSS in MB (Linux). None where unsupported (local Windows).

    NOTE: peak never decreases within a process — a flat line means "no new
    highs", not "safe". Use rss_current_mb() for the safety margin.
    """
    try:
        import resource

        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    except Exception:
        return None


def rss_current_mb() -> float | None:
    """Current RSS in MB via /proc (Linux only). The real safety margin."""
    try:
        import os

        with open("/proc/self/statm") as f:
            resident_pages = int(f.read().split()[1])
        return round(resident_pages * os.sysconf("SC_PAGE_SIZE") / 1e6, 1)
    except Exception:
        return None


_last_rss: float | None = None


def should_recycle(peak: float | None, cur: float | None = None) -> bool:
    """Recycle only while still climbing past the ceiling.

    A stable-high peak (equilibrium) is left alone — restarting it would
    loop forever. Strictly-greater means still growing toward the OOM line.
    `cur` (/proc current) runs ~20MB above peak on Render and is the real
    OOM line — recycle early on it even if peak hasn't caught up yet.
    """
    global _last_rss
    if cur is not None and cur > 460:
        return True
    if peak is None:
        return False
    climbing = _last_rss is not None and peak > _last_rss
    _last_rss = peak
    return climbing and peak > 450


async def poll_loop():
    global _is_running
    if _is_running:
        return
    _is_running = True
    logger.info(f"poller started interval={settings.CHECK_INTERVAL_SECONDS}s local comfort")
    # first next estimate so schedule page has data before first sweep finishes
    state.next_sweep_at = datetime.now(timezone.utc) + timedelta(seconds=settings.CHECK_INTERVAL_SECONDS)
    while True:
        try:
            # Bound the dedup cache: keys are per-creator, but never evicted.
            if len(_last_room_cache) > 500:
                _last_room_cache.clear()
            state.sweep_in_progress = True
            state.last_sweep_at = datetime.now(timezone.utc)
            result, yt, kk = await poll_cycle()
            await maintenance()  # rename attempts + avatar refreshes (bounded, best-effort)
            peak = rss_mb()
            cur = rss_current_mb()
            logger.info(
                f"poll sweep checked={result['checked']}+{yt['checked']}+{kk['checked']} "
                f"notified={result['notified']}+{yt['notified']}+{kk['notified']} "
                f"rss={peak}MB cur={cur}MB"
            )
            if should_recycle(peak, cur):
                logger.warning(f"memory ceiling hit (peak {peak}MB cur {cur}MB) — recycling process")
                import os as _os

                _os._exit(0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"poll_loop error {type(e).__name__}")
        finally:
            state.sweep_in_progress = False
        # schedule next sweep (jitter chosen now so API can show accurate ETA)
        state.next_sweep_at = datetime.now(timezone.utc) + timedelta(
            seconds=settings.CHECK_INTERVAL_SECONDS + random.uniform(0, settings.CHECK_JITTER_SECONDS)
        )
        await asyncio.sleep((state.next_sweep_at - datetime.now(timezone.utc)).total_seconds())
