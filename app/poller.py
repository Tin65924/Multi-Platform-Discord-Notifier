import asyncio
import logging
import random
from datetime import datetime, timezone, timedelta

from sqlalchemy import select, or_

from .config import get_settings
from .db import async_session
from .models import Subscription, GlobalSettings
from .tiktok import checker
from .webhook import build_embed, display_account, send_webhook
from .kick import checker as kick_checker
from .youtube import checker as youtube_checker

logger = logging.getLogger(__name__)
settings = get_settings()

_last_room_cache: dict[str, str] = {}
_is_running = False


def _aware(dt):
    """SQLite returns naive datetimes — treat them as UTC for safe comparison."""
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

async def get_global_settings():
    async with async_session() as session:
        gs = await session.get(GlobalSettings, 1)
        url = (gs.webhook_url if gs and gs.webhook_url else None) or settings.DASHBOARD_WEBHOOK_URL or None
        ping = (gs.ping_role_id if gs and gs.ping_role_id else None) or (settings.PING_ROLE_ID or None)
        msg = (gs.custom_message if gs and gs.custom_message else None) or settings.CUSTOM_MESSAGE
        everyone = bool(gs.ping_everyone) if gs and gs.ping_everyone is not None else True
        image = (gs.embed_image_url if gs and gs.embed_image_url else None) or (settings.EMBED_IMAGE_URL or None)
        color = (gs.embed_color if gs and gs.embed_color else None) or "#FF0050"
        return url, ping, msg, everyone, image, color

async def poll_once():
    async with async_session() as session:
        # Live-checking only supports TikTok for now — other platforms are
        # stored in the dashboard until their pollers land. NULL is treated
        # as tiktok so pre-platform rows keep polling even if backfill lagged.
        result = await session.execute(
            select(Subscription).where(
                Subscription.enabled == True,  # noqa
                or_(Subscription.platform == "tiktok", Subscription.platform.is_(None)),
            )
        )
        subs = result.scalars().all()
        if not subs:
            return {"checked": 0, "notified": 0}

        cfg = await get_global_settings()
        webhook_url, ping_role_id, custom_message, ping_everyone, embed_image_url, embed_color = cfg
        if not webhook_url:
            logger.warning("poll skipped: no webhook configured in DB or env")
            return {"checked": 0, "notified": 0, "error": "no webhook"}

        usernames = list(set(s.tiktok_username for s in subs))
        random.shuffle(usernames)

        by_user = {s.tiktok_username: s for s in subs}
        checked = 0
        notified = 0

        for username in usernames:
            checked += 1
            live_info = await checker.is_live(username)
            now = datetime.now(timezone.utc)
            sub = by_user[username]
            sub.last_checked_at = now

            if live_info.is_live:
                room_id = live_info.room_id or f"live-{username}"
                if _last_room_cache.get("tt:" + username) == room_id:
                    sub.is_live = True  # still live — only the notification is skipped
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                if sub.last_room_id == room_id and sub.last_notified_at:
                    if (now - _aware(sub.last_notified_at)) < timedelta(seconds=900):
                        _last_room_cache["tt:" + username] = room_id
                        sub.is_live = True  # still live — only the notification is skipped
                        await session.commit()
                        await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                        continue

                payload = build_embed(
                    username,
                    message=sub.message or custom_message,
                    ping_role_id=ping_role_id,
                    ping_everyone=ping_everyone,
                    link_text=sub.link_text,
                    image_url=sub.image_url or embed_image_url,
                    color=sub.color or embed_color,
                    author_name=sub.author_name,
                    discord_username=sub.discord_username,
                )
                ok = await send_webhook(webhook_url, payload, timeout=settings.WEBHOOK_TIMEOUT_SECONDS)
                if ok:
                    sub.last_room_id = room_id
                    sub.last_notified_at = now
                    sub.last_live_at = now
                    sub.is_live = True
                    sub.consecutive_failures = 0
                    notified += 1
                    _last_room_cache["tt:" + username] = room_id
                    logger.info(f"notified @{username}")
                else:
                    sub.consecutive_failures += 1
            else:
                sub.is_live = False

            await session.commit()
            await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)

        return {"checked": checked, "notified": notified}

async def poll_youtube():
    """Sweep YouTube rows: keyless /live parse + optional API confirm.

    Mirrors poll_once. Check failures (blocked/ambiguous pages) leave the
    card state untouched instead of flipping it offline.
    """
    async with async_session() as session:
        result = await session.execute(
            select(Subscription).where(
                Subscription.enabled == True,  # noqa
                Subscription.platform == "youtube",
            )
        )
        subs = result.scalars().all()
        if not subs:
            return {"checked": 0, "notified": 0}

        cfg = await get_global_settings()
        webhook_url, ping_role_id, custom_message, ping_everyone, embed_image_url, embed_color = cfg
        if not webhook_url:
            logger.warning("youtube poll skipped: no webhook configured in DB or env")
            return {"checked": 0, "notified": 0, "error": "no webhook"}

        api_key = settings.YOUTUBE_API_KEY or ""
        handles = list(set(s.tiktok_username for s in subs))
        random.shuffle(handles)

        by_handle = {s.tiktok_username: s for s in subs}
        checked = 0
        notified = 0

        for handle in handles:
            checked += 1
            live_info = await youtube_checker.is_live(handle, api_key=api_key)
            now = datetime.now(timezone.utc)
            sub = by_handle[handle]
            sub.last_checked_at = now
            disp = display_account("youtube", handle)

            if live_info.error and not live_info.is_live:
                # Blocked or ambiguous page — keep last known state, try again next sweep.
                logger.debug(f"youtube check inconclusive user={handle} err={live_info.error}")
                await session.commit()
                await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                continue

            if live_info.is_live:
                room_id = live_info.room_id or f"live-yt-{handle}"
                if _last_room_cache.get("yt:" + handle) == room_id:
                    sub.is_live = True  # still live — only the notification is skipped
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                if sub.last_room_id == room_id and sub.last_notified_at:
                    if (now - _aware(sub.last_notified_at)) < timedelta(seconds=900):
                        _last_room_cache["yt:" + handle] = room_id
                        sub.is_live = True  # still live — only the notification is skipped
                        await session.commit()
                        await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                        continue

                payload = build_embed(
                    handle,
                    message=sub.message or custom_message,
                    ping_role_id=ping_role_id,
                    ping_everyone=ping_everyone,
                    link_text=sub.link_text,
                    image_url=sub.image_url or embed_image_url,
                    color=sub.color or embed_color,
                    author_name=sub.author_name,
                    discord_username=sub.discord_username,
                    platform="youtube",
                )
                ok = await send_webhook(webhook_url, payload, timeout=settings.WEBHOOK_TIMEOUT_SECONDS)
                if ok:
                    sub.last_room_id = room_id
                    sub.last_notified_at = now
                    sub.last_live_at = now
                    sub.is_live = True
                    sub.consecutive_failures = 0
                    notified += 1
                    _last_room_cache["yt:" + handle] = room_id
                    logger.info(f"notified yt {disp}")
                else:
                    sub.consecutive_failures += 1
            else:
                sub.is_live = False

            await session.commit()
            await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)

        return {"checked": checked, "notified": notified}


async def poll_kick():
    """Sweep Kick rows: one batched official-API call per cycle.

    Skipped entirely without KICK_CLIENT_ID/SECRET. Inconclusive per-slug
    results leave the card state untouched instead of flipping it offline.
    """
    kick_id = settings.KICK_CLIENT_ID or ""
    kick_secret = settings.KICK_CLIENT_SECRET or ""
    if not kick_checker.configured(kick_id, kick_secret):
        return {"checked": 0, "notified": 0, "skipped": "unconfigured"}
    async with async_session() as session:
        result = await session.execute(
            select(Subscription).where(
                Subscription.enabled == True,  # noqa
                Subscription.platform == "kick",
            )
        )
        subs = result.scalars().all()
        if not subs:
            return {"checked": 0, "notified": 0}

        cfg = await get_global_settings()
        webhook_url, ping_role_id, custom_message, ping_everyone, embed_image_url, embed_color = cfg
        if not webhook_url:
            logger.warning("kick poll skipped: no webhook configured in DB or env")
            return {"checked": 0, "notified": 0, "error": "no webhook"}

        handles = list(set(s.tiktok_username for s in subs))
        random.shuffle(handles)

        statuses = await kick_checker.check_many(handles, kick_id, kick_secret)
        by_handle = {s.tiktok_username: s for s in subs}
        checked = 0
        notified = 0

        for handle in handles:
            checked += 1
            live_info = statuses[handle].info
            now = datetime.now(timezone.utc)
            sub = by_handle[handle]
            sub.last_checked_at = now
            disp = display_account("kick", handle)

            if live_info.error and not live_info.is_live:
                # Inconclusive for this slug — keep last known state.
                logger.debug(f"kick check inconclusive user={handle} err={live_info.error}")
                await session.commit()
                await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                continue

            if live_info.is_live:
                room_id = live_info.room_id or f"live-kk-{handle}"
                if _last_room_cache.get("kk:" + handle) == room_id:
                    sub.is_live = True  # still live — only the notification is skipped
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                if sub.last_room_id == room_id and sub.last_notified_at:
                    if (now - _aware(sub.last_notified_at)) < timedelta(seconds=900):
                        _last_room_cache["kk:" + handle] = room_id
                        sub.is_live = True  # still live — only the notification is skipped
                        await session.commit()
                        await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                        continue

                payload = build_embed(
                    handle,
                    message=sub.message or custom_message,
                    ping_role_id=ping_role_id,
                    ping_everyone=ping_everyone,
                    link_text=sub.link_text,
                    image_url=sub.image_url or embed_image_url,
                    color=sub.color or embed_color,
                    author_name=sub.author_name,
                    discord_username=sub.discord_username,
                    platform="kick",
                )
                ok = await send_webhook(webhook_url, payload, timeout=settings.WEBHOOK_TIMEOUT_SECONDS)
                if ok:
                    sub.last_room_id = room_id
                    sub.last_notified_at = now
                    sub.last_live_at = now
                    sub.is_live = True
                    sub.consecutive_failures = 0
                    notified += 1
                    _last_room_cache["kk:" + handle] = room_id
                    logger.info(f"notified kick {disp}")
                else:
                    sub.consecutive_failures += 1
            else:
                sub.is_live = False

            await session.commit()
            await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)

        return {"checked": checked, "notified": notified}


async def poll_loop():
    global _is_running
    if _is_running:
        return
    _is_running = True
    logger.info(f"poller started interval={settings.CHECK_INTERVAL_SECONDS}s local comfort")
    while True:
        try:
            result = await poll_once()
            yt = await poll_youtube()
            kk = await poll_kick()
            logger.info(
                f"poll sweep checked={result['checked']}+{yt['checked']}+{kk['checked']} "
                f"notified={result['notified']}+{yt['notified']}+{kk['notified']}"
            )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"poll_loop error {type(e).__name__}")
        await asyncio.sleep(settings.CHECK_INTERVAL_SECONDS + random.uniform(0, settings.CHECK_JITTER_SECONDS))