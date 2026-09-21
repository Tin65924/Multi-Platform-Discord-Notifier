"""Cron trigger + memory debug routes (Phase 5: optional CRON_SECRET)."""
import secrets as _secrets

from fastapi import APIRouter, Depends, HTTPException

from ...infrastructure.scheduler.loop import rss_current_mb, rss_mb
from ...poller import poll_cycle_try
from ...security import require_superadmin
from .common import logger, settings

router = APIRouter()


def _cron_authorized(provided: str | None) -> bool:
    """Open when CRON_SECRET is unset (legacy); constant-time compare when set."""
    want = (settings.CRON_SECRET or "").strip()
    if not want:
        return True
    return _secrets.compare_digest((provided or "").strip(), want)


def _require_cron(secret: str | None) -> None:
    if not _cron_authorized(secret):
        raise HTTPException(403, "Invalid cron secret")


@router.get("/debug/memory")
async def debug_memory(trace: str = "", user=Depends(require_superadmin)):
    """Memory telemetry (superadmin): RSS, GC stats, live tasks, optional tracemalloc.

    ?trace=on starts allocation tracing (~5-10% overhead — short windows
    only), ?trace=off stops it. Compare two snapshots' tops to find a leak.
    """
    import asyncio
    import gc
    import tracemalloc

    out: dict = {
        "rss_mb": rss_mb(),
        "rss_current_mb": rss_current_mb(),
        "gc_counts": gc.get_count(),
        "gc_garbage": len(gc.garbage),
    }
    try:
        out["asyncio_tasks"] = len(asyncio.all_tasks())
    except Exception:
        out["asyncio_tasks"] = None
    if trace == "on":
        if not tracemalloc.is_tracing():
            tracemalloc.start(10)
            logger.warning("tracemalloc started via debug endpoint")
        out["tracing"] = True
    elif trace == "off":
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        out["tracing"] = False
    else:
        out["tracing"] = tracemalloc.is_tracing()
    if tracemalloc.is_tracing():
        snap = tracemalloc.take_snapshot()
        out["top"] = [str(s) for s in snap.statistics("lineno")[:25]]
    return out


@router.post("/cron/poll")
async def cron_poll(secret: str | None = None):
    """Sweep trigger: runs unless one is already running.

    Pass ?secret= when CRON_SECRET is set (Render env). Unset = open (legacy).
    """
    _require_cron(secret)
    return await poll_cycle_try()


@router.get("/cron/poll")
async def cron_poll_get(secret: str | None = None):
    """Sweep trigger: runs unless one is already running (see POST variant)."""
    _require_cron(secret)
    return await poll_cycle_try()
