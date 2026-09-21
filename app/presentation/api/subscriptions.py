"""Subscription routes — moved verbatim from app/api/routes.py (Phase 3)."""
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ...db import get_session
from ...models import Subscription, GlobalSettings, LiveSession
from ...schemas import (
    SubscriptionCreate, HandleUpdate, SubscriptionStyleIn, normalize_handle,
)
from ...security import (
    require_admin,
    log_audit,
)
from ...tiktok import checker, fetch_tiktok_profile
from ...webhook import send_webhook, platform_label, display_account
from ...webhook import served_photo_url
from ...images import process_upload, MAX_UPLOAD_BYTES
from .common import _payload_for, _webhook_cfg, logger, settings

router = APIRouter()


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
            "has_photo": bool(s.image_mime),
            "has_avatar": bool(s.avatar_url),
            # Card + browser display URL (relative is fine here): same
            # upload > link > auto precedence as the Discord embeds.
            "photo_url": (f"/api/media/creator/{s.id}" if s.image_mime
                          else (s.image_url or s.avatar_url or None)),
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


@router.post("/subscriptions/{sub_id}/photo")
async def upload_photo(
    sub_id: int, file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session), user=Depends(require_admin),
):
    """Upload a creator photo. Validated + normalized, stored in Postgres.

    Takes precedence over the link field; delete it to fall back to the link.
    """
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    try:
        raw = await file.read(MAX_UPLOAD_BYTES + 1)
    except Exception:
        raise HTTPException(400, "Could not read upload")
    finally:
        try:
            await file.close()
        except Exception:
            pass
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "File too large (max 8MB)")
    try:
        data, mime = process_upload(raw)
    except ValueError as e:
        raise HTTPException(400, str(e))
    sub.image_blob = data
    sub.image_mime = mime
    await session.commit()
    await log_audit(user["username"], "photo.upload",
                    f"photo for @{sub.tiktok_username} ({mime}, {len(data)}b)")
    return {"msg": "Photo saved — it now shows instead of the link",
            "url": served_photo_url(sub.id), "mime": mime, "bytes": len(data)}


@router.post("/subscriptions/{sub_id}/fetch-profile")
async def fetch_profile(sub_id: int, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """On-demand profile fetch: stores the auto avatar (+ id anchor).

    Best-effort — TikTok may refuse anonymous fetches (then it 502s with
    an honest message). Never fails loudly beyond that.
    """
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    if (sub.platform or "tiktok") != "tiktok":
        raise HTTPException(400, "Profile fetch supports TikTok for now")
    prof = await fetch_tiktok_profile(sub.tiktok_username)
    if not prof:
        raise HTTPException(502, "TikTok didn't return a profile (blocked, or handle renamed?)")
    now = datetime.now(timezone.utc)
    sub.avatar_checked_at = now
    if prof["avatar_url"]:
        sub.avatar_url = prof["avatar_url"]
    if prof["user_id"] and not sub.tiktok_user_id:
        sub.tiktok_user_id = prof["user_id"]
    await session.commit()
    await log_audit(user["username"], "profile.fetch", f"fetched profile for @{sub.tiktok_username}")
    if prof["unique_id"] != sub.tiktok_username:
        return {"msg": f"That handle now resolves to @{prof['unique_id']} — update it with the New handle field",
                "avatar_url": sub.avatar_url, "canonical": prof["unique_id"]}
    if not prof["avatar_url"]:
        return {"msg": "Profile found, but it has no usable photo", "avatar_url": None}
    return {"msg": "Profile photo saved", "avatar_url": sub.avatar_url}


@router.delete("/subscriptions/{sub_id}/photo")
async def delete_photo(sub_id: int, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """Remove the uploaded photo — the embed falls back to the link field."""
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    sub.image_blob = None
    sub.image_mime = None
    await session.commit()
    await log_audit(user["username"], "photo.remove", f"photo removed for @{sub.tiktok_username}")
    return {"msg": "Photo removed — link is used again"}


@router.patch("/subscriptions/{sub_id}/handle")
async def update_handle(sub_id: int, payload: HandleUpdate, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """Re-link a creator that renamed on their platform. Style, photo,
    sessions and KPIs are preserved (unlike remove + re-add)."""
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    try:
        handle = normalize_handle(sub.platform or "tiktok", payload.handle)
    except ValueError as e:
        raise HTTPException(400, str(e))
    if handle == sub.tiktok_username:
        # Same handle re-confirmed (e.g. login-walled account flagged by
        # mistake): clears the rename flag, changes nothing else.
        sub.first_not_found_at = None
        await session.commit()
        return {"msg": "Handle confirmed — flag cleared"}
    dup = await session.execute(
        select(Subscription).where(
            Subscription.platform == (sub.platform or "tiktok"),
            Subscription.tiktok_username == handle,
            Subscription.id != sub_id,
        )
    )
    if dup.scalar_one_or_none():
        raise HTTPException(409, f"@{handle} is already tracked")
    old = sub.tiktok_username
    sub.tiktok_username = handle
    sub.first_not_found_at = None
    # Best-effort: anchor the numeric id + grab the avatar for the new name.
    # Never fails the update itself.
    if (sub.platform or "tiktok") == "tiktok":
        try:
            from ...tiktok import fetch_tiktok_profile

            prof = await fetch_tiktok_profile(handle)
            if prof:
                if prof["user_id"]:
                    sub.tiktok_user_id = prof["user_id"]
                if prof["avatar_url"]:
                    sub.avatar_url = prof["avatar_url"]
                    sub.avatar_checked_at = datetime.now(timezone.utc)
        except Exception:
            pass
    await session.commit()
    await log_audit(user["username"], "creator.relink", f"@{old} -> @{handle} on {platform_label(sub.platform)}")
    return {"msg": f"Updated @{old} to @{handle} — next sweep picks up their live"}


@router.post("/subscriptions/{sub_id}/mark-offline")
async def mark_offline(sub_id: int, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    """Force a creator's card to offline (fixes stuck LIVE).

    Closes open LiveSessions, clears dedup cache and evicts the reused
    TikTok client so the next sweep does a fresh check. Admins + superadmins.
    """
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    sub.is_live = False
    # clear dedup cache for all possible prefixes
    try:
        from ...poller import _last_room_cache, _close_open_sessions
        for pfx in ("tt:", "yt:", "kk:", "twitch:"):
            _last_room_cache.pop(pfx + sub.tiktok_username, None)
        if (sub.platform or "tiktok") == "tiktok":
            try:
                from ...tiktok import checker
                checker.drop(sub.tiktok_username)  # pop + close httpx (bare pop leaks)
            except Exception:
                pass
        await _close_open_sessions(session, sub.id, datetime.now(timezone.utc))
    except Exception:
        pass
    await session.commit()
    await log_audit(user["username"], "creator.offline", f"marked @{sub.tiktok_username} offline")
    return {"msg": f"Marked @{sub.tiktok_username} offline — next sweep will re-check fresh"}


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


@router.post("/subscriptions/{sub_id}/check-now")
async def check_now(sub_id: int, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "Not found")
    plat = (sub.platform or "tiktok").lower()
    if plat == "youtube":
        from ...youtube import checker as yt_checker
        info = await yt_checker.is_live(sub.tiktok_username, api_key=settings.YOUTUBE_API_KEY or "")
        if info.error and not info.is_live:
            raise HTTPException(502, f"YouTube check inconclusive ({info.error}) — try again next sweep")
        return {"is_live": info.is_live, "room_id": info.room_id}
    if plat == "kick":
        from ...kick import checker as kk_checker
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
    from ...tiktok import _settings as _tt_settings

    info = await checker.is_live(sub.tiktok_username)
    return {
        "is_live": info.is_live,
        "room_id": info.room_id,
        "session_mode": bool(getattr(_tt_settings, "TIKTOK_SESSION_ID", "") or ""),
    }
