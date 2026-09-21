"""Cron trigger + memory debug routes — moved verbatim from app/api/routes.py (Phase 3)."""
from fastapi import APIRouter, Depends

from ...poller import poll_cycle_try, rss_current_mb, rss_mb
from ...security import require_superadmin
from .common import logger

router = APIRouter()


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
async def cron_poll():
    """Open trigger (no secret): runs a sweep unless one is already running."""
    return await poll_cycle_try()


@router.get("/cron/poll")
async def cron_poll_get():
    """Open trigger (no secret): runs a sweep unless one is already running."""
    return await poll_cycle_try()
