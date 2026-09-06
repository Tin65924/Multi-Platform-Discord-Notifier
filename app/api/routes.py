import logging
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..db import get_session
from ..models import Subscription, GlobalSettings, User, AuditLog, LiveSession
from ..schemas import (
    SubscriptionCreate, GlobalSettingsIn, LoginIn, AdminCreate,
    PasswordChange, SubscriptionStyleIn,
)
from ..security import (
    require_admin,
    require_superadmin,
    verify_password,
    hash_password,
    log_audit,
    SESSION_KEY,
    get_current_user,
)
from ..tiktok import checker
from ..webhook import build_embed, send_webhook, platform_label, display_account, resolve_webhook_cfg
from ..wildlines import WILD_LINES
from ..poller import poll_cycle_try, rss_current_mb, rss_mb

logger = logging.getLogger(__name__)
router = APIRouter()
settings = get_settings()


def _webhook_cfg(gs: GlobalSettings | None):
    return resolve_webhook_cfg(gs)


def _style_for(sub: Subscription, image: str | None, color: str) -> dict:
    """Resolve per-creator style with global fallbacks.

    The embed description is a randomized forest line (Creator Name as NAME);
    the message is always the global template (per-creator overrides retired).
    """
    return {
        "author_name": sub.author_name or None,
        "discord_username": sub.discord_username or None,
        "discord_user_id": sub.discord_user_id or None,
        "image_url": sub.image_url or image,
        "color": sub.color or color,
    }


# --- Auth ---
@router.post("/login")
async def login(payload: LoginIn, request: Request, session: AsyncSession = Depends(get_session)):
    result = await session.execute(
        select(User).where(User.username == payload.username.strip().lower())
    )
    user = result.scalar_one_or_none()
    if not user or not user.is_active or not verify_password(payload.password, user.password_hash):
        raise HTTPException(401, "Invalid username or password")
    request.session[SESSION_KEY] = user.id
    await log_audit(user.username, "login", "dashboard login")
    return {"msg": "Logged in", "username": user.username, "role": user.role}


@router.post("/logout")
async def logout(request: Request):
    user = await get_current_user(request)
    request.session.clear()
    if user:
        await log_audit(user["username"], "logout", "dashboard logout")
    return {"msg": "Logged out"}


@router.get("/me")
async def me(request: Request):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(401, "Login required")
    return user


@router.post("/me/password")
async def change_own_password(
    payload: PasswordChange, request: Request, session: AsyncSession = Depends(get_session),
    user=Depends(require_admin),
):
    db_user = await session.get(User, user["id"])
    if not db_user:
        raise HTTPException(404, "Not found")
    if user["role"] != "superadmin":
        if not payload.current_password or not verify_password(payload.current_password, db_user.password_hash):
            raise HTTPException(400, "Current password is incorrect")
    db_user.password_hash = hash_password(payload.new_password)
    await session.commit()
    await log_audit(user["username"], "password.change", f"password changed for {db_user.username}")
    return {"msg": "Password updated"}


# --- Admins (superadmin only) ---
@router.get("/admins")
async def list_admins(session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    result = await session.execute(select(User).order_by(User.username))
    return [
        {"id": u.id, "username": u.username, "role": u.role, "is_active": u.is_active,
         "created_at": u.created_at.isoformat() if u.created_at else None}
        for u in result.scalars().all()
    ]


@router.post("/admins", status_code=201)
async def create_admin(payload: AdminCreate, session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    existing = await session.execute(select(User).where(User.username == payload.username))
    if existing.scalar_one_or_none():
        raise HTTPException(409, "Username already exists")
    new_user = User(username=payload.username, password_hash=hash_password(payload.password),
                    role=payload.role, is_active=True)
    session.add(new_user)
    await session.commit()
    await log_audit(user["username"], "admin.create", f"created {payload.role} {payload.username}")
    return {"msg": f"Created {payload.role} {payload.username}"}


@router.post("/admins/{uid}/toggle")
async def toggle_admin(uid: int, session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    target = await session.get(User, uid)
    if not target:
        raise HTTPException(404, "Not found")
    if target.id == user["id"]:
        raise HTTPException(400, "You cannot disable yourself")
    target.is_active = not target.is_active
    await session.commit()
    await log_audit(user["username"], "admin.toggle", f"{'enabled' if target.is_active else 'disabled'} {target.username}")
    return {"msg": f"{'Enabled' if target.is_active else 'Disabled'} {target.username}"}


@router.delete("/admins/{uid}")
async def delete_admin(uid: int, session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    target = await session.get(User, uid)
    if not target:
        raise HTTPException(404, "Not found")
    if target.id == user["id"]:
        raise HTTPException(400, "You cannot delete yourself")
    if target.role == "superadmin":
        sup = await session.scalar(select(func.count()).select_from(User).where(User.role == "superadmin"))
        if (sup or 0) <= 1:
            raise HTTPException(400, "Cannot delete the last superadmin")
    await session.delete(target)
    await session.commit()
    await log_audit(user["username"], "admin.delete", f"deleted {target.username}")
    return {"msg": f"Deleted {target.username}"}


# --- Audit (superadmin only) ---
@router.get("/audit")
async def list_audit(limit: int = 100, session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    limit = max(1, min(limit, 500))
    result = await session.execute(select(AuditLog).order_by(AuditLog.id.desc()).limit(limit))
    return [
        {"id": a.id, "actor": a.actor, "action": a.action, "detail": a.detail,
         "created_at": a.created_at.isoformat() if a.created_at else None}
        for a in result.scalars().all()
    ]


# --- Settings: global defaults (admin + superadmin) ---
@router.get("/settings")
async def get_settings_api(session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    gs = await session.get(GlobalSettings, 1)
    url, ping, msg, everyone, image, color = _webhook_cfg(gs)
    return {
        "webhook_configured": bool(url),
        "webhook_url_masked": ("***REDACTED***" + url[-12:] if url else None),
        "raw_webhook_url": url,
        "ping_role_id": ping,
        "ping_everyone": everyone,
        "custom_message": msg,
        "embed_image_url": image,
        "embed_color": color,
        "notifications_enabled": bool(gs.notifications_enabled) if gs and gs.notifications_enabled is not None else True,
        # Which platforms the poller actually live-checks (drives dashboard hints).
        "live_checks": {
            "tiktok": True,
            "youtube": True,
            "kick": bool(settings.KICK_CLIENT_ID and settings.KICK_CLIENT_SECRET),
            "twitch": False,
        },
    }


@router.put("/settings")
async def put_settings(payload: GlobalSettingsIn, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    gs = await session.get(GlobalSettings, 1)
    if not gs:
        gs = GlobalSettings(id=1, webhook_url=payload.webhook_url, ping_role_id=payload.ping_role_id,
                            ping_everyone=payload.ping_everyone, custom_message=payload.custom_message,
                            embed_image_url=payload.embed_image_url, embed_color=payload.embed_color,
                            notifications_enabled=payload.notifications_enabled)
        session.add(gs)
    else:
        gs.webhook_url = payload.webhook_url
        gs.ping_role_id = payload.ping_role_id
        gs.ping_everyone = payload.ping_everyone
        gs.custom_message = payload.custom_message
        gs.embed_image_url = payload.embed_image_url
        gs.embed_color = payload.embed_color
        gs.notifications_enabled = payload.notifications_enabled
    await session.commit()
    await log_audit(user["username"], "webhook.update", "global defaults saved")
    return {"msg": "Settings saved"}


# --- Subscriptions (admin + superadmin) ---
@router.get("/subscriptions")
async def list_subs(session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    result = await session.execute(select(Subscription).order_by(Subscription.platform, Subscription.tiktok_username))
    subs = result.scalars().all()
    return [
        {
            "id": s.id,
            "platform": s.platform or "tiktok",
            "tiktok_username": s.tiktok_username,
            "label": s.label,
            "is_live": s.is_live,
            "author_name": s.author_name,
            "discord_username": s.discord_username,
            "discord_user_id": s.discord_user_id,
            "image_url": s.image_url,
            "color": s.color,
            "last_checked_at": s.last_checked_at.isoformat() if s.last_checked_at else None,
            "last_notified_at": s.last_notified_at.isoformat() if s.last_notified_at else None,
            "enabled": s.enabled,
        }
        for s in subs
    ]


@router.post("/subscriptions", status_code=201)
async def create_sub(payload: SubscriptionCreate, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    platform = payload.platform
    username = payload.handle  # normalized by validator
    label = platform_label(platform)
    count = await session.scalar(select(func.count()).select_from(Subscription))
    if (count or 0) >= settings.MAX_CREATORS:
        raise HTTPException(400, f"Local cap reached: {settings.MAX_CREATORS} creators max. Delete one first.")
    existing = await session.execute(
        select(Subscription).where(Subscription.platform == platform, Subscription.tiktok_username == username)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(409, f"{display_account(platform, username)} is already tracked on {label}")
    sub = Subscription(platform=platform, tiktok_username=username)
    session.add(sub)
    await session.commit()
    await session.refresh(sub)
    await log_audit(user["username"], "creator.add", f"added {display_account(platform, username)} on {label}")
    logger.info(f"created sub id={sub.id} platform={platform} user={username}")
    return {"id": sub.id, "msg": f"Added {display_account(platform, username)} on {label}"}


@router.patch("/subscriptions/{sub_id}/style")
async def update_style(sub_id: int, payload: SubscriptionStyleIn, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    sub.author_name = (payload.author_name or "")[:128] or None
    sub.discord_username = (payload.discord_username or "")[:64] or None
    sub.discord_user_id = (payload.discord_user_id or "")[:32] or None
    # Retired overrides: link text derives from Creator Name, message is
    # always the global template — clear any stale per-creator values.
    sub.message = None
    sub.link_text = None
    sub.image_url = payload.image_url
    sub.color = payload.color
    await session.commit()
    await log_audit(user["username"], "creator.style", f"styled @{sub.tiktok_username}")
    return {"msg": f"Style saved for @{sub.tiktok_username}"}


@router.delete("/subscriptions/{sub_id}")
async def delete_sub(sub_id: int, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    name = sub.tiktok_username
    rows = (
        await session.execute(
            select(LiveSession).where(LiveSession.subscription_id == sub_id)
        )
    ).scalars().all()
    for row in rows:
        await session.delete(row)
    await session.delete(sub)
    await session.commit()
    await log_audit(user["username"], "creator.remove", f"removed @{name}")
    return {"msg": "Deleted"}


def _payload_for(sub: Subscription, msg: str, ping: str | None, everyone: bool, image: str | None, color: str):
    style = _style_for(sub, image, color)
    return build_embed(
        sub.tiktok_username,
        message=msg,
        ping_role_id=ping,
        ping_everyone=everyone,
        image_url=style["image_url"],
        color=style["color"],
        author_name=style["author_name"],
        platform=sub.platform or "tiktok",
        discord_username=style["discord_username"],
        discord_user_id=style["discord_user_id"],
    )


@router.post("/subscriptions/{sub_id}/test")
async def test_webhook(sub_id: int, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    gs = await session.get(GlobalSettings, 1)
    webhook_url, ping, msg, everyone, image, color = _webhook_cfg(gs)
    if not webhook_url:
        raise HTTPException(400, "Configure webhook first (paste it in the top card)")
    payload = _payload_for(sub, msg, ping, everyone, image, color)
    ok = await send_webhook(webhook_url, payload, timeout=settings.WEBHOOK_TIMEOUT_SECONDS)
    if not ok:
        raise HTTPException(502, "Webhook failed - check URL/permissions")
    await log_audit(user["username"], "test.send", f"test notification for @{sub.tiktok_username}")
    return {"msg": "Test sent — check Discord"}


@router.post("/send-custom")
async def send_custom(request: Request, session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    """Superadmin broadcast box: send free text to the channel as a test."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON body")
    content = str(data.get("content") or "").strip()
    if not content:
        raise HTTPException(400, "Message is empty")
    if len(content) > 2000:
        raise HTTPException(400, "Message too long (2000 chars max)")
    gs = await session.get(GlobalSettings, 1)
    webhook_url = _webhook_cfg(gs)[0]
    if not webhook_url:
        raise HTTPException(400, "Configure webhook first (Defaults tab)")
    payload = {
        "username": "Forest Watcher",
        "content": content,
        "allowed_mentions": {"parse": ["everyone", "roles", "users"]},
    }
    ok = await send_webhook(webhook_url, payload, timeout=settings.WEBHOOK_TIMEOUT_SECONDS)
    if not ok:
        raise HTTPException(502, "Webhook failed - check URL/permissions (see logs)")
    await log_audit(user["username"], "custom.send", content[:200])
    return {"msg": "Sent — check Discord"}


@router.post("/subscriptions/{sub_id}/force-notify")
async def force_notify(sub_id: int, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """Send the live notification right now, even if the creator looks offline.

    Bypasses live-status and dedup. Records last_notified_at but otherwise
    leaves card state (is_live, last_room_id) untouched, so the next real
    go-live still notifies normally.
    """
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    gs = await session.get(GlobalSettings, 1)
    webhook_url, ping, msg, everyone, image, color = _webhook_cfg(gs)
    if not webhook_url:
        raise HTTPException(400, "Configure webhook first (paste it in the top card)")
    payload = _payload_for(sub, msg, ping, everyone, image, color)
    ok = await send_webhook(webhook_url, payload, timeout=settings.WEBHOOK_TIMEOUT_SECONDS)
    if not ok:
        raise HTTPException(502, "Webhook failed - check URL/permissions")
    sub.last_notified_at = datetime.now(timezone.utc)
    await session.commit()
    disp = display_account(sub.platform or "tiktok", sub.tiktok_username)
    await log_audit(user["username"], "force.send", f"forced notification for {disp} on {platform_label(sub.platform)}")
    return {"msg": f"Forced notification sent for {disp} — check Discord"}


@router.get("/subscriptions/{sub_id}/payload")
async def payload_preview(sub_id: int, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """Return the exact JSON that would be POSTed to Discord (no secrets in it)."""
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    gs = await session.get(GlobalSettings, 1)
    _, ping, msg, everyone, image, color = _webhook_cfg(gs)
    return _payload_for(sub, msg, ping, everyone, image, color)


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


@router.get("/wildlines")
async def wild_lines(user=Depends(require_admin)):
    """Forest line templates (NAME placeholder) for the Defaults live preview."""
    return list(WILD_LINES)


def _analytics_range(frm: str | None, to: str | None, days: int):
    """UTC instant window from explicit ISO bounds or a trailing day count."""
    from ..analytics import aware_utc

    now = datetime.now(timezone.utc)
    t = aware_utc(datetime.fromisoformat(to)) if to else now
    f = aware_utc(datetime.fromisoformat(frm)) if frm else t - timedelta(days=max(1, min(days, 180)))
    return f, t, now


@router.get("/analytics/overview")
async def analytics_overview(days: int = 30, platform: str = "",
                             session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """Ranked warn/remove shortlist: least active creators on top."""
    from ..analytics import fetch_window, per_creator

    f, t, now = _analytics_range(None, None, days)
    subs, rows = await fetch_window(session, f, t)
    return {"days": max(1, min(days, 180)),
            "rows": per_creator(subs, rows, f, t, now, (platform or "").lower().strip())}


@router.get("/analytics/kpis")
async def analytics_kpis(frm: str | None = None, to: str | None = None, days: int = 30, platform: str = "",
                         session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """Headline KPIs + trends vs previous equal period + daily series + ranked rows."""
    from ..analytics import fetch_window, per_creator, headline, daily_series, overlap_minutes, pct_change

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
    from ..analytics import aware_utc

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


@router.post("/subscriptions/{sub_id}/check-now")
async def check_now(sub_id: int, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    plat = (sub.platform or "tiktok").lower()
    if plat == "youtube":
        from ..youtube import checker as yt_checker
        info = await yt_checker.is_live(sub.tiktok_username, api_key=settings.YOUTUBE_API_KEY or "")
        if info.error and not info.is_live:
            raise HTTPException(502, f"YouTube check inconclusive ({info.error}) — try again next sweep")
        return {"is_live": info.is_live, "room_id": info.room_id}
    if plat == "kick":
        from ..kick import checker as kk_checker
        if not kk_checker.configured(settings.KICK_CLIENT_ID, settings.KICK_CLIENT_SECRET):
            raise HTTPException(400, "Kick checks need KICK_CLIENT_ID/SECRET in .env first")
        info = await kk_checker.is_live(
            sub.tiktok_username,
            client_id=settings.KICK_CLIENT_ID, client_secret=settings.KICK_CLIENT_SECRET,
        )
        if info.error and not info.is_live:
            raise HTTPException(502, f"Kick check inconclusive ({info.error}) — try again next sweep")
        return {"is_live": info.is_live, "room_id": info.room_id}
    if plat != "tiktok":
        raise HTTPException(400, f"Live checks for {platform_label(sub.platform)} aren't supported yet — creator stored for later")
    from ..tiktok import _settings as _tt_settings

    info = await checker.is_live(sub.tiktok_username)
    return {
        "is_live": info.is_live,
        "room_id": info.room_id,
        "session_mode": bool(getattr(_tt_settings, "TIKTOK_SESSION_ID", "") or ""),
    }


@router.post("/cron/poll")
async def cron_poll():
    """Open trigger (no secret): runs a sweep unless one is already running."""
    return await poll_cycle_try()


@router.get("/cron/poll")
async def cron_poll_get():
    """Open trigger (no secret): runs a sweep unless one is already running."""
    return await poll_cycle_try()
