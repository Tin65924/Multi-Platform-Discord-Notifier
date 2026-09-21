"""Creator activity analytics — pure computation over live_sessions.

Timezone: Asia/Manila has no DST, so a fixed UTC+8 offset is exact
year-round (no tzdata dependency). All day boundaries use it; instants
stay UTC in the DB.
"""
from datetime import datetime, timedelta, timezone
from sqlalchemy import select

MANILA = timezone(timedelta(hours=8), "Asia/Manila")

SOFT_DAYS = 3  # soft notice
HARD_DAYS = 7  # hard notice
TERM_DAYS = 10  # terminate


def status_for(days_idle: int) -> str:
    if days_idle >= TERM_DAYS:
        return "terminate"
    if days_idle >= HARD_DAYS:
        return "hard"
    if days_idle >= SOFT_DAYS:
        return "soft"
    return "active"


def aware_utc(dt):
    """SQLite returns naive datetimes — treat them as UTC for safe comparison."""
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def last_live_of(sub, sessions, now):
    """Most recent live evidence: sessions first, then legacy columns."""
    starts = [s.started_at for s in sessions]
    if starts:
        return max(aware_utc(x) for x in starts)
    for attr in ("last_live_at", "last_notified_at", "created_at"):
        v = aware_utc(getattr(sub, attr, None))
        if v and v <= now:
            return v
    return None


def days_idle_since(last_live, tracking_since, now) -> tuple[int, bool]:
    """(idle Manila calendar days, has_history). Never-live counts from tracking start."""
    ref = last_live or tracking_since or now
    ref = aware_utc(ref) or now
    days = (now.astimezone(MANILA).date() - ref.astimezone(MANILA).date()).days
    return max(0, days), last_live is not None


def overlap_minutes(start, end, f, t) -> float:
    """Session minutes clipped to [f, t). Open sessions end at t."""
    s = max(aware_utc(start), f)
    e = min(aware_utc(end) if end else t, t)
    return max(0.0, (e - s).total_seconds() / 60.0)


async def fetch_window(session, f, t):
    """All sessions overlapping [f, t), plus the enabled roster."""
    from .infrastructure.persistence.models import LiveSession, Subscription

    subs = (
        await session.execute(
            select(Subscription).where(Subscription.enabled == True)  # noqa
        )
    ).scalars().all()
    rows = (
        await session.execute(
            select(LiveSession).where(
                LiveSession.started_at < t,
                (LiveSession.ended_at.is_(None)) | (LiveSession.ended_at > f),
            )
        )
    ).scalars().all()
    return subs, rows


def headline(sessions, active_ids, roster_n, f, t, now):
    """Headline KPIs for exactly the sessions in scope."""
    minutes = sum(overlap_minutes(s.started_at, s.ended_at or now, f, t) for s in sessions)
    n = len(sessions)
    active = len(active_ids)
    by_day: dict[str, float] = {}
    for s in sessions:
        m = overlap_minutes(s.started_at, s.ended_at or now, f, t)
        if m <= 0:
            continue
        day = aware_utc(s.started_at).astimezone(MANILA).date().isoformat()
        by_day[day] = by_day.get(day, 0.0) + m
    days_total = max(1, (t.date() - f.date()).days + 1)
    best = max(by_day.items(), key=lambda kv: kv[1], default=(None, 0.0))
    return {
        "live_minutes": int(round(minutes)),
        "sessions": n,
        "active_creators": active,
        "active_pct": round(100.0 * active / roster_n, 1) if roster_n else 0.0,
        "avg_session_minutes": round(minutes / n, 1) if n else 0.0,
        "streams_per_creator": round(n / active, 2) if active else 0.0,
        "best_day": best[0],
        "best_day_minutes": int(round(best[1])),
        "coverage_days": len(by_day),
        "silent_days": max(0, days_total - len(by_day)),
        "notifications_sent": sum(1 for s in sessions if s.notified),
    }


def pct_change(cur: float, prev: float):
    if not prev:
        return None
    return round(100.0 * (cur - prev) / prev, 1)


def daily_series(sessions, f, t, now):
    """[{date, minutes}] for every Manila date in [f, t]."""
    days = []
    d = f.astimezone(MANILA).date()
    end = t.astimezone(MANILA).date()
    while d <= end:
        days.append(d.isoformat())
        d += timedelta(days=1)
    mins = {d: 0.0 for d in days}
    for s in sessions:
        m = overlap_minutes(s.started_at, s.ended_at or now, f, t)
        if m <= 0:
            continue
        day = aware_utc(s.started_at).astimezone(MANILA).date().isoformat()
        if day in mins:
            mins[day] += m
    return [{"date": d, "minutes": int(round(mins[d]))} for d in days]


HANDLE_FLAG_DAYS = 3


def handle_flagged(sub, now) -> bool:
    """True when the handle looks renamed/deleted for 3+ days."""
    first = aware_utc(getattr(sub, "first_not_found_at", None))
    return bool(first) and (now - first) >= timedelta(days=HANDLE_FLAG_DAYS)


def per_creator(subs, sessions, f, t, now, platform: str = ""):
    """Ranked rows (idle desc): the warn/remove shortlist on top."""
    by_sub: dict[int, list] = {}
    for s in sessions:
        by_sub.setdefault(s.subscription_id, []).append(s)
    total_min = sum(overlap_minutes(s.started_at, s.ended_at or now, f, t) for s in sessions)
    rows = []
    for sub in subs:
        if platform and (sub.platform or "tiktok").lower() != platform:
            continue
        own = by_sub.get(sub.id, [])
        mins = sum(overlap_minutes(s.started_at, s.ended_at or now, f, t) for s in own)
        last = last_live_of(sub, own, now)
        idle, hist = days_idle_since(last, aware_utc(sub.created_at), now)
        rows.append({
            "id": sub.id,
            "handle": sub.tiktok_username,
            "platform": (sub.platform or "tiktok").lower(),
            "creator_name": sub.author_name,
            "sessions": len(own),
            "live_minutes": int(round(mins)),
            "share_pct": round(100.0 * mins / total_min, 1) if total_min else 0.0,
            "last_live_at": last.isoformat() if last else None,
            "has_history": hist,
            "days_idle": idle,
            "status": status_for(idle),
            "handle_flag": handle_flagged(sub, now),
        })
    rows.sort(key=lambda r: (-r["days_idle"], r["handle"]))
    return rows
