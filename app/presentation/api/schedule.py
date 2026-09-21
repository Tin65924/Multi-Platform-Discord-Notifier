"""Schedule route — moved verbatim from app/api/routes.py (Phase 3)."""
from datetime import datetime, timezone
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...infrastructure.persistence.database import get_session
from ...infrastructure.persistence.models import Subscription
from ...security import require_admin
from .common import logger

router = APIRouter()


@router.get("/schedule")
async def get_schedule(session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """When the next sweep fires and when each creator is expected to be checked.

    Global sweep: poller checks every CHECK_INTERVAL + jitter, all creators each sweep.
    Per-creator ETA = next_sweep_at + position * PER_CHECK_SLEEP (1.0s), ordered
    by last_live_at desc (active first — same order the poller uses).
    """
    from datetime import timedelta as _td

    from ...poller import get_schedule_state

    state = get_schedule_state()
    # Rebuild the poller's sort order so ETA matches reality.
    result = await session.execute(
        select(Subscription).where(Subscription.enabled == True).order_by(Subscription.id)  # noqa
    )
    subs = result.scalars().all()
    # Same sort as poll_once: active (recent last_live_at) first
    def _key(s):
        v = s.last_live_at
        if v is not None and v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v or datetime.min.replace(tzinfo=timezone.utc)
    subs_sorted = sorted(subs, key=_key, reverse=True)
    next_at = None
    try:
        if state["next_sweep_at"]:
            next_at = datetime.fromisoformat(state["next_sweep_at"].replace("Z", "+00:00"))
            if next_at.tzinfo is None:
                next_at = next_at.replace(tzinfo=timezone.utc)
    except Exception:
        next_at = None
    per = float(state.get("per_check_sleep", 1.0) or 1.0)
    per_creator = []
    for idx, s in enumerate(subs_sorted):
        eta = None
        if next_at is not None:
            eta = (next_at + _td(seconds=idx * per)).isoformat()
        per_creator.append({
            "id": s.id,
            "platform": s.platform or "tiktok",
            "handle": s.tiktok_username,
            "label": s.label,
            "author_name": s.author_name,
            "is_live": s.is_live,
            "last_live_at": s.last_live_at.isoformat() if s.last_live_at else None,
            "last_checked_at": s.last_checked_at.isoformat() if s.last_checked_at else None,
            "position": idx + 1,
            "next_check_at": eta,
        })
    return {
        **state,
        "total_creators": len(subs_sorted),
        "estimated_sweep_seconds": round(len(subs_sorted) * per, 1) if subs_sorted else 0,
        "per_creator": per_creator,
    }
