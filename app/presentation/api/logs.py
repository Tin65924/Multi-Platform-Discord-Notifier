"""Sweep-log routes (admin + superadmin)."""
from datetime import datetime, timezone
from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ...infrastructure.persistence.database import get_session
from ...security import require_admin

router = APIRouter()

@router.get("/logs")
async def list_logs(
    frm: str | None = None, to: str | None = None,
    platform: str | None = None, handle: str | None = None,
    status: str | None = None, error: str | None = None,
    notified: str | None = None,  # "true"/"false"/"all"
    q: str | None = None,
    limit: int = 200, offset: int = 0,
    session: AsyncSession = Depends(get_session), user=Depends(require_admin),
):
    """Sweep logs: per-creator check trace.

    Query params: ?from=ISO&to=ISO&platform=tiktok&handle=foo&status=live|offline|error
                 &error=not_found&notified=true&q=free-text&limit=200&offset=0
    All filters are optional. `status` maps to is_live/error. Available to admins
    and superadmins so teams can report what the poller actually did.
    """
    from sqlalchemy import or_

    from ...infrastructure.persistence.models import SweepLog

    limit = max(1, min(limit, 500))
    offset = max(0, offset)

    def _parse_dt(s: str | None):
        if not s:
            return None
        s = s.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    conds = []
    f_dt = _parse_dt(frm)
    t_dt = _parse_dt(to)
    if f_dt is not None:
        conds.append(SweepLog.created_at >= f_dt)
    if t_dt is not None:
        conds.append(SweepLog.created_at <= t_dt)
    if platform and platform.strip() and platform.strip() != "__all__":
        conds.append(SweepLog.platform == platform.strip().lower())
    if handle and handle.strip():
        conds.append(SweepLog.handle.ilike(f"%{handle.strip()}%"))
    if error and error.strip() and error.strip() != "__all__":
        if error.strip() == "__none__":
            conds.append(SweepLog.error.is_(None))
        else:
            conds.append(SweepLog.error == error.strip())
    if status and status.strip() and status.strip() != "__all__":
        s = status.strip()
        if s == "live":
            conds.append(SweepLog.is_live == True)  # noqa
        elif s == "offline":
            conds.append(SweepLog.is_live == False)  # noqa
        elif s == "error":
            conds.append(SweepLog.error.is_not(None))
        elif s == "notified":
            conds.append(SweepLog.notified == True)  # noqa
    if notified and notified.strip() and notified.strip() != "__all__":
        if notified.strip().lower() in ("true", "1", "yes"):
            conds.append(SweepLog.notified == True)  # noqa
        elif notified.strip().lower() in ("false", "0", "no"):
            conds.append(SweepLog.notified == False)  # noqa
    if q and q.strip():
        qq = f"%{q.strip()}%"
        conds.append(or_(SweepLog.detail.ilike(qq), SweepLog.handle.ilike(qq),
                         SweepLog.platform.ilike(qq), SweepLog.error.ilike(qq),
                         SweepLog.room_id.ilike(qq)))

    base = select(SweepLog)
    if conds:
        base = base.where(*conds)

    total = await session.scalar(select(func.count()).select_from(base.subquery())) or 0
    result = await session.execute(base.order_by(SweepLog.id.desc()).limit(limit).offset(offset))
    rows = result.scalars().all()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "rows": [
            {"id": r.id, "created_at": r.created_at.isoformat() if r.created_at else None,
             "platform": r.platform, "handle": r.handle, "subscription_id": r.subscription_id,
             "is_live": r.is_live, "error": r.error, "notified": r.notified,
             "room_id": r.room_id, "duration_ms": r.duration_ms, "detail": r.detail}
            for r in rows
        ],
    }

@router.get("/logs/meta")
async def logs_meta(session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """Distinct platforms/handles/errors for populating sweep-log filters."""
    from ...infrastructure.persistence.models import SweepLog
    plats_q = await session.execute(select(SweepLog.platform).distinct().order_by(SweepLog.platform).limit(20))
    handles_q = await session.execute(select(SweepLog.handle).distinct().order_by(SweepLog.handle).limit(100))
    errors_q = await session.execute(select(SweepLog.error).distinct().order_by(SweepLog.error).limit(50))
    return {
        "platforms": [r[0] for r in plats_q.all() if r[0]],
        "handles": [r[0] for r in handles_q.all() if r[0]],
        "errors": [r[0] for r in errors_q.all() if r[0] is not None],
    }
