import asyncio
import logging
import random
from datetime import datetime, timezone, timedelta

from sqlalchemy import select, or_

from .config import get_settings
from .db import async_session, open_session
from .models import Subscription, GlobalSettings, LiveSession
from .tiktok import checker
from .webhook import build_embed, display_account, effective_image, resolve_webhook_cfg, send_webhook
from .kick import checker as kick_checker
from .youtube import checker as youtube_checker

logger = logging.getLogger(__name__)
settings = get_settings()

_last_room_cache: dict[str, str] = {}
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


def should_recycle(peak: float | None) -> bool:
    """Recycle only while still climbing past the ceiling.

    A stable-high peak (equilibrium) is left alone — restarting it would
    loop forever. Strictly-greater means still growing toward the OOM line.
    """
    global _last_rss
    if peak is None:
        return False
    climbing = _last_rss is not None and peak > _last_rss
    _last_rss = peak
    return climbing and peak > 450

# One cycle at a time, process-wide: the loop and external cron triggers
# share this. A trigger that finds a running cycle skips (busy) instead of
# overlapping it (overlap caused double notifications + connection pileup).
_sweep_lock = asyncio.Lock()


async def poll_cycle():
    """Full cycle: all three platform sweeps concurrently.

    Holds the cycle lock so cron triggers skip while the loop runs (and
    vice versa). Sweep-level isolation is by platform; they share nothing
    mutable except disjoint _last_room_cache keys.
    """
    async with _sweep_lock:
        results = await asyncio.gather(
            poll_once(), poll_youtube(), poll_kick(), return_exceptions=True
        )
        out = []
        for name, r in zip(("tiktok", "youtube", "kick"), results):
            if isinstance(r, Exception):
                logger.exception(f"poll {name} error {type(r).__name__}")
                out.append({"checked": 0, "notified": 0})
            else:
                out.append(r)
        return out[0], out[1], out[2]


async def poll_cycle_try():
    """Cron-trigger entry: run a full cycle, or skip cleanly when busy."""
    if _sweep_lock.locked():
        logger.info("cron sweep skipped: cycle already running")
        return {"checked": 0, "notified": 0, "skipped": "busy"}
    base, yt, kk = await poll_cycle()
    return {
        "checked": base["checked"] + yt["checked"] + kk["checked"],
        "notified": base["notified"] + yt["notified"] + kk["notified"],
    }


def _aware(dt):
    """SQLite returns naive datetimes — treat them as UTC for safe comparison."""
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

async def _open_session(session, sub, room_id: str, now):
    """Get-or-create the open session row for this live. Restart-safe.

    Same room still open -> reuse (no duplicates). Different room open
    (e.g. fallback id -> real id) -> close the old one, open a new one.
    """
    res = await session.execute(
        select(LiveSession).where(
            LiveSession.subscription_id == sub.id,
            LiveSession.ended_at.is_(None),
        )
    )
    for o in res.scalars().all():
        if o.room_id == room_id:
            return o
        o.ended_at = now
    row = LiveSession(
        subscription_id=sub.id,
        platform=sub.platform or "tiktok",
        handle=sub.tiktok_username,
        room_id=room_id,
        started_at=now,
    )
    session.add(row)
    return row


async def notifications_enabled(session) -> bool:
    """Global kill-switch. NULL (pre-migration rows) counts as ON."""
    gs = await session.get(GlobalSettings, 1)
    if gs is None or gs.notifications_enabled is None:
        return True
    return bool(gs.notifications_enabled)


async def _close_open_sessions(session, sub_id: int, now):
    """Stamp ended_at on any open rows. Idempotent — safe on every check."""
    res = await session.execute(
        select(LiveSession).where(
            LiveSession.subscription_id == sub_id,
            LiveSession.ended_at.is_(None),
        )
    )
    for o in res.scalars().all():
        o.ended_at = now


async def get_global_settings():
    session = await open_session()
    try:
        gs = await session.get(GlobalSettings, 1)
        return resolve_webhook_cfg(gs)
    finally:
        await session.close()

async def poll_once():
    # Snapshot ids/handles in one short session; every creator below gets
    # its own session. A DB connection is never held across network checks.
    async with async_session() as session:
        # NULL is treated as tiktok so pre-platform rows keep polling even
        # if backfill lagged.
        result = await session.execute(
            select(Subscription).where(
                Subscription.enabled == True,  # noqa
                or_(Subscription.platform == "tiktok", Subscription.platform.is_(None)),
            )
        )
        rows = [(s.id, s.tiktok_username) for s in result.scalars().all()]
        notify_on = await notifications_enabled(session)
    if not rows:
        return {"checked": 0, "notified": 0}

    cfg = await get_global_settings()
    webhook_url, ping_role_id, custom_message, ping_everyone, embed_image_url, embed_color = cfg
    if not webhook_url:
        logger.warning("poll skipped: no webhook configured in DB or env")
        return {"checked": 0, "notified": 0, "error": "no webhook"}

    usernames = list({u for _, u in rows})
    random.shuffle(usernames)

    by_id = {u: sid for sid, u in rows}
    checked = 0
    notified = 0

    for username in usernames:
        checked += 1
        live_info = await checker.is_live(username)
        now = datetime.now(timezone.utc)
        async with async_session() as session:
            sub = await session.get(Subscription, by_id[username])
            if sub is None:
                continue  # removed mid-sweep
            sub.last_checked_at = now

            if live_info.is_live:
                room_id = live_info.room_id or f"live-{username}"
                if _last_room_cache.get("tt:" + username) == room_id:
                    sub.is_live = True  # still live — only the notification is skipped
                    await _open_session(session, sub, room_id, now)
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                if sub.last_room_id == room_id and sub.last_notified_at:
                    if (now - _aware(sub.last_notified_at)) < timedelta(seconds=900):
                        _last_room_cache["tt:" + username] = room_id
                        sub.is_live = True  # still live — only the notification is skipped
                        await _open_session(session, sub, room_id, now)
                        await session.commit()
                        await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                        continue

                payload = build_embed(
                    username,
                    message=custom_message,
                    ping_role_id=ping_role_id,
                    ping_everyone=ping_everyone,
                    image_url=effective_image(sub, embed_image_url),
                    color=sub.color or embed_color,
                    author_name=sub.author_name,
                    discord_username=sub.discord_username,
                    discord_user_id=sub.discord_user_id,
                )
                sess_row = await _open_session(session, sub, room_id, now)
                if sess_row.notified:
                    # This exact live was already announced (restart after
                    # cooldown, etc.). Only a new room_id re-notifies.
                    sub.is_live = True
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                ok = False
                if notify_on:
                    ok = await send_webhook(webhook_url, payload, timeout=settings.WEBHOOK_TIMEOUT_SECONDS)
                sub.last_room_id = room_id
                sub.last_live_at = now
                sub.is_live = True
                if ok:
                    sub.last_notified_at = now
                    sess_row.notified = True
                    notified += 1
                    _last_room_cache["tt:" + username] = room_id
                    logger.info(f"notified @{username}")
            else:
                sub.is_live = False
                await _close_open_sessions(session, sub.id, now)

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
        rows = [(s.id, s.tiktok_username) for s in result.scalars().all()]
        notify_on = await notifications_enabled(session)
    if not rows:
        return {"checked": 0, "notified": 0}

    cfg = await get_global_settings()
    webhook_url, ping_role_id, custom_message, ping_everyone, embed_image_url, embed_color = cfg
    if not webhook_url:
        logger.warning("youtube poll skipped: no webhook configured in DB or env")
        return {"checked": 0, "notified": 0, "error": "no webhook"}

    api_key = settings.YOUTUBE_API_KEY or ""
    handles = list({u for _, u in rows})
    random.shuffle(handles)

    by_id = {u: sid for sid, u in rows}
    checked = 0
    notified = 0

    for handle in handles:
        checked += 1
        live_info = await youtube_checker.is_live(handle, api_key=api_key)
        now = datetime.now(timezone.utc)
        disp = display_account("youtube", handle)
        async with async_session() as session:
            sub = await session.get(Subscription, by_id[handle])
            if sub is None:
                continue  # removed mid-sweep
            sub.last_checked_at = now

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
                    await _open_session(session, sub, room_id, now)
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                if sub.last_room_id == room_id and sub.last_notified_at:
                    if (now - _aware(sub.last_notified_at)) < timedelta(seconds=900):
                        _last_room_cache["yt:" + handle] = room_id
                        sub.is_live = True  # still live — only the notification is skipped
                        await _open_session(session, sub, room_id, now)
                        await session.commit()
                        await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                        continue

                payload = build_embed(
                    handle,
                    message=custom_message,
                    ping_role_id=ping_role_id,
                    ping_everyone=ping_everyone,
                    image_url=effective_image(sub, embed_image_url),
                    color=sub.color or embed_color,
                    author_name=sub.author_name,
                    discord_username=sub.discord_username,
                    discord_user_id=sub.discord_user_id,
                    platform="youtube",
                )
                sess_row = await _open_session(session, sub, room_id, now)
                if sess_row.notified:
                    # This exact live was already announced (restart after
                    # cooldown, etc.). Only a new room_id re-notifies.
                    sub.is_live = True
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                ok = False
                if notify_on:
                    ok = await send_webhook(webhook_url, payload, timeout=settings.WEBHOOK_TIMEOUT_SECONDS)
                sub.last_room_id = room_id
                sub.last_live_at = now
                sub.is_live = True
                if ok:
                    sub.last_notified_at = now
                    sess_row.notified = True
                    notified += 1
                    _last_room_cache["yt:" + handle] = room_id
                    logger.info(f"notified yt {disp}")
            else:
                sub.is_live = False
                await _close_open_sessions(session, sub.id, now)

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
        rows = [(s.id, s.tiktok_username) for s in result.scalars().all()]
        notify_on = await notifications_enabled(session)
    if not rows:
        return {"checked": 0, "notified": 0}

    cfg = await get_global_settings()
    webhook_url, ping_role_id, custom_message, ping_everyone, embed_image_url, embed_color = cfg
    if not webhook_url:
        logger.warning("kick poll skipped: no webhook configured in DB or env")
        return {"checked": 0, "notified": 0, "error": "no webhook"}

    handles = list({u for _, u in rows})
    random.shuffle(handles)

    statuses = await kick_checker.check_many(handles, kick_id, kick_secret)
    by_id = {u: sid for sid, u in rows}
    checked = 0
    notified = 0

    for handle in handles:
        checked += 1
        live_info = statuses[handle].info
        now = datetime.now(timezone.utc)
        disp = display_account("kick", handle)
        async with async_session() as session:
            sub = await session.get(Subscription, by_id[handle])
            if sub is None:
                continue  # removed mid-sweep
            sub.last_checked_at = now

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
                    await _open_session(session, sub, room_id, now)
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                if sub.last_room_id == room_id and sub.last_notified_at:
                    if (now - _aware(sub.last_notified_at)) < timedelta(seconds=900):
                        _last_room_cache["kk:" + handle] = room_id
                        sub.is_live = True  # still live — only the notification is skipped
                        await _open_session(session, sub, room_id, now)
                        await session.commit()
                        await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                        continue

                payload = build_embed(
                    handle,
                    message=custom_message,
                    ping_role_id=ping_role_id,
                    ping_everyone=ping_everyone,
                    image_url=effective_image(sub, embed_image_url),
                    color=sub.color or embed_color,
                    author_name=sub.author_name,
                    discord_username=sub.discord_username,
                    discord_user_id=sub.discord_user_id,
                    platform="kick",
                )
                sess_row = await _open_session(session, sub, room_id, now)
                if sess_row.notified:
                    # This exact live was already announced (restart after
                    # cooldown, etc.). Only a new room_id re-notifies.
                    sub.is_live = True
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                ok = False
                if notify_on:
                    ok = await send_webhook(webhook_url, payload, timeout=settings.WEBHOOK_TIMEOUT_SECONDS)
                sub.last_room_id = room_id
                sub.last_live_at = now
                sub.is_live = True
                if ok:
                    sub.last_notified_at = now
                    sess_row.notified = True
                    notified += 1
                    _last_room_cache["kk:" + handle] = room_id
                    logger.info(f"notified kick {disp}")
            else:
                sub.is_live = False
                await _close_open_sessions(session, sub.id, now)

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
            # Bound the dedup cache: keys are per-creator, but never evicted.
            if len(_last_room_cache) > 500:
                _last_room_cache.clear()
            result, yt, kk = await poll_cycle()
            peak = rss_mb()
            logger.info(
                f"poll sweep checked={result['checked']}+{yt['checked']}+{kk['checked']} "
                f"notified={result['notified']}+{yt['notified']}+{kk['notified']} "
                f"rss={peak}MB cur={rss_current_mb()}MB"
            )
            if should_recycle(peak):
                # Still climbing past the ceiling toward Render's 512MB kill
                # line: exit cleanly between sweeps so Render restarts fresh.
                # ~60s gap; the DB cooldown + session rows guard duplicates.
                logger.warning(f"memory ceiling hit (peak {peak}MB) — recycling process")
                import os as _os

                _os._exit(0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"poll_loop error {type(e).__name__}")
        await asyncio.sleep(settings.CHECK_INTERVAL_SECONDS + random.uniform(0, settings.CHECK_JITTER_SECONDS))