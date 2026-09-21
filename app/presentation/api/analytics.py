"""Analytics routes — moved verbatim from app/api/routes.py (Phase 3)."""
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...db import get_session
from ...models import Subscription, LiveSession
from ...security import require_admin
from .common import _analytics_range, logger

router = APIRouter()


@router.get("/analytics/overview")
async def analytics_overview(days: int = 30, platform: str = "",
                             session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """Ranked warn/remove shortlist: least active creators on top."""
    from ...analytics import fetch_window, per_creator

    f, t, now = _analytics_range(None, None, days)
    subs, rows = await fetch_window(session, f, t)
    return {"days": max(1, min(days, 180)),
            "rows": per_creator(subs, rows, f, t, now, (platform or "").lower().strip())}


@router.get("/analytics/kpis")
async def analytics_kpis(frm: str | None = None, to: str | None = None, days: int = 30, platform: str = "",
                         session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """Headline KPIs + trends vs previous equal period + daily series + ranked rows."""
    from ...analytics import fetch_window, per_creator, headline, daily_series, overlap_minutes, pct_change

    f, t, now = _analytics_range(frm, to, days)
    plat = (platform or "").lower().strip()
    subs, rows = await fetch_window(session, f, t)
    scope = {s.id for s in subs if not plat or (s.platform or "tiktok").lower() == plat}
    rows = [r for r in rows if r.subscription_id in scope]
    subs = [s for s in subs if s.id in scope]

    def active_of(rs, ff, tt):
        return {r.subscription_id for r in rs
                if overlap_minutes(r.started_at, r.ended_at or now, ff, tt) > 0}

    h = headline(rows, active_of(rows, f, t), len(scope), f, t, now)
    delta = t - f
    _, prev_rows = await fetch_window(session, f - delta, f)
    prev_rows = [r for r in prev_rows if r.subscription_id in scope]
    hp = headline(prev_rows, active_of(prev_rows, f - delta, f), len(scope), f - delta, f, now)
    trends = {k: pct_change(h[k], hp[k]) for k in
              ("live_minutes", "sessions", "active_creators", "avg_session_minutes")}
    return {"from": f.isoformat(), "to": t.isoformat(),
            "headline": h, "trends": trends,
            "daily": daily_series(rows, f, t, now),
            "rows": per_creator(subs, rows, f, t, now, plat)}


@router.get("/analytics/creator/{sub_id}")
async def analytics_creator(sub_id: int, days: int = 30,
                            session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """Session history for one creator (newest first)."""
    from ...analytics import aware_utc

    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    now = datetime.now(timezone.utc)
    f = now - timedelta(days=max(1, min(days, 180)))
    rows = (await session.execute(
        select(LiveSession)
        .where(LiveSession.subscription_id == sub_id,
               (LiveSession.started_at >= f) | (LiveSession.ended_at.is_(None)))
        .order_by(LiveSession.started_at.desc()).limit(200)
    )).scalars().all()
    out = []
    for r in rows:
        end = aware_utc(r.ended_at) or now
        out.append({"started_at": aware_utc(r.started_at).isoformat(),
                    "ended_at": end.isoformat() if r.ended_at else None,
                    "minutes": int(round(max(0.0, (end - aware_utc(r.started_at)).total_seconds() / 60.0))),
                    "notified": bool(r.notified), "room_id": r.room_id})
    return {"id": sub.id, "handle": sub.tiktok_username,
            "platform": (sub.platform or "tiktok").lower(),
            "creator_name": sub.author_name, "sessions": out}
