import asyncio
import logging
import random
import time
from datetime import datetime, timezone, timedelta

from sqlalchemy import select, or_

from .config import get_settings
from .db import async_session, open_session
from .models import Subscription, GlobalSettings, LiveSession, SweepLog
from .security import log_audit
from .tiktok import checker, fetch_tiktok_profile
from .webhook import build_embed, display_account, effective_image, resolve_webhook_cfg, send_webhook
from .kick import checker as kick_checker
from .youtube import checker as youtube_checker

logger = logging.getLogger(__name__)
settings = get_settings()

_last_room_cache: dict[str, str] = {}
_is_running = False
_next_sweep_at: datetime | None = None
_last_sweep_at: datetime | None = None
_sweep_in_progress: bool = False
_last_sweep_order: list[str] = []  # usernames in last sort order (for per-creator ETA)


def get_schedule_state() -> dict:
    """Snapshot for /api/schedule — cheap, no DB."""
    return {
        "next_sweep_at": _next_sweep_at.isoformat() if _next_sweep_at else None,
        "last_sweep_at": _last_sweep_at.isoformat() if _last_sweep_at else None,
        "in_progress": _sweep_in_progress or _sweep_lock.locked(),
        "interval": int(settings.CHECK_INTERVAL_SECONDS),
        "jitter": int(settings.CHECK_JITTER_SECONDS),
        "per_check_sleep": float(settings.PER_CHECK_SLEEP_SECONDS),
    }


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


def _add_sweep_log(session, platform: str, handle: str, sub_id: int | None,
                   is_live: bool | None, error: str | None, notified: bool,
                   room_id: str | None, detail: str | None, duration_ms: int | None, now):
    """Queue a SweepLog row in the given session (caller commits). Best-effort."""
    try:
        session.add(SweepLog(
            platform=platform, handle=handle, subscription_id=sub_id,
            is_live=is_live, error=(error or None), notified=bool(notified),
            room_id=(room_id or None), detail=(detail or "")[:500] or None,
            duration_ms=duration_ms, created_at=now,
        ))
    except Exception:
        pass


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
        rows = [(s.id, s.tiktok_username, s.last_live_at) for s in result.scalars().all()]
        notify_on = await notifications_enabled(session)
    if not rows:
        return {"checked": 0, "notified": 0}

    cfg = await get_global_settings()
    webhook_url, ping_role_id, custom_message, ping_everyone, embed_image_url, embed_color = cfg
    if not webhook_url:
        logger.warning("poll skipped: no webhook configured in DB or env")
        return {"checked": 0, "notified": 0, "error": "no webhook"}

    # Build lookup: username -> last_live_at (None if never live)
    live_map = {u: ld for _, u, ld in rows}

    # Sort so creators who went live most recently are checked first.
    # None values sort last, so never‑live creators end up at the bottom.
    usernames = sorted(
        [u for _, u, _ in rows],
        key=lambda u: live_map.get(u) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    # Expose order for schedule ETA (next sweep = _next_sweep_at + index*PER_CHECK_SLEEP)
    global _last_sweep_order
    _last_sweep_order = list(usernames)

    by_id = {u: sid for sid, u in rows}
    checked = 0
    notified = 0

    for username in usernames:
        t0 = time.monotonic()
        live_info = await checker.is_live(username)
        duration_ms = int((time.monotonic() - t0) * 1000)
        now = datetime.now(timezone.utc)
        async with async_session() as session:
            sub = await session.get(Subscription, by_id[username])
            if sub is None:
                continue  # removed mid-sweep
            sub.last_checked_at = now
            err = getattr(live_info, "error", None)
            room_id_val = getattr(live_info, "room_id", None)
            if err == "not_found":
                if sub.first_not_found_at is None:
                    sub.first_not_found_at = now
                    logger.info(f"handle not resolving @{sub.tiktok_username} — tracking for rename")
                await _close_open_sessions(session, sub.id, now)
                _add_sweep_log(session, "tiktok", username, sub.id, False, "not_found", False, room_id_val, "not_found — tracking rename", duration_ms, now)
                await session.commit()
                await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                continue
            if err is None and sub.first_not_found_at is not None:
                sub.first_not_found_at = None  # clean read clears streak

            if live_info.is_live:
                room_id = live_info.room_id or f"live-{username}"
                if _last_room_cache.get("tt:" + username) == room_id:
                    sub.is_live = True
                    await _open_session(session, sub, room_id, now)
                    _add_sweep_log(session, "tiktok", username, sub.id, True, None, False, room_id, "dedup skip — same room", duration_ms, now)
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                if sub.last_room_id == room_id and sub.last_notified_at:
                    if (now - _aware(sub.last_notified_at)) < timedelta(seconds=900):
                        _last_room_cache["tt:" + username] = room_id
                        sub.is_live = True
                        await _open_session(session, sub, room_id, now)
                        _add_sweep_log(session, "tiktok", username, sub.id, True, None, False, room_id, "cooldown skip — 15m", duration_ms, now)
                        await session.commit()
                        await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                        continue

                # Fresh avatar for live notification + creator card (replaces 3-day refresh).
                try:
                    prof = await fetch_tiktok_profile(username)
                    if prof and prof.get("avatar_url"):
                        sub.avatar_url = prof["avatar_url"]
                        sub.avatar_checked_at = now
                except Exception:
                    pass

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
                    sub.is_live = True
                    _add_sweep_log(session, "tiktok", username, sub.id, True, None, False, room_id, "already notified this room", duration_ms, now)
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
                    _add_sweep_log(session, "tiktok", username, sub.id, True, None, True, room_id, "notified", duration_ms, now)
                else:
                    detail = "live but notifications paused" if not notify_on else f"live — webhook failed"
                    _add_sweep_log(session, "tiktok", username, sub.id, True, None, False, room_id, detail, duration_ms, now)
            else:
                sub.is_live = False
                await _close_open_sessions(session, sub.id, now)
                # log offline or error detail
                detail = "offline" if err is None else f"error:{err}"
                _add_sweep_log(session, "tiktok", username, sub.id, False, err, False, room_id_val, detail, duration_ms, now)

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
        t0 = time.monotonic()
        live_info = await youtube_checker.is_live(handle, api_key=api_key)
        duration_ms = int((time.monotonic() - t0) * 1000)
        now = datetime.now(timezone.utc)
        disp = display_account("youtube", handle)
        async with async_session() as session:
            sub = await session.get(Subscription, by_id[handle])
            if sub is None:
                continue  # removed mid-sweep
            sub.last_checked_at = now
            err = getattr(live_info, "error", None)
            room_id_val = getattr(live_info, "room_id", None)
            if err == "not_found":
                if sub.first_not_found_at is None:
                    sub.first_not_found_at = now
                    logger.info(f"handle not resolving @{sub.tiktok_username} — tracking for rename")
                await _close_open_sessions(session, sub.id, now)
                _add_sweep_log(session, "youtube", handle, sub.id, False, "not_found", False, room_id_val, "not_found — tracking rename", duration_ms, now)
                await session.commit()
                await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                continue
            if err is None and sub.first_not_found_at is not None:
                sub.first_not_found_at = None

            if live_info.error and not live_info.is_live:
                logger.debug(f"youtube check inconclusive user={handle} err={live_info.error}")
                _add_sweep_log(session, "youtube", handle, sub.id, None, live_info.error, False, room_id_val, f"inconclusive:{live_info.error}", duration_ms, now)
                await session.commit()
                await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                continue

            if live_info.is_live:
                room_id = live_info.room_id or f"live-yt-{handle}"
                if _last_room_cache.get("yt:" + handle) == room_id:
                    sub.is_live = True
                    await _open_session(session, sub, room_id, now)
                    _add_sweep_log(session, "youtube", handle, sub.id, True, None, False, room_id, "dedup skip — same room", duration_ms, now)
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                if sub.last_room_id == room_id and sub.last_notified_at:
                    if (now - _aware(sub.last_notified_at)) < timedelta(seconds=900):
                        _last_room_cache["yt:" + handle] = room_id
                        sub.is_live = True
                        await _open_session(session, sub, room_id, now)
                        _add_sweep_log(session, "youtube", handle, sub.id, True, None, False, room_id, "cooldown skip — 15m", duration_ms, now)
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
                    sub.is_live = True
                    _add_sweep_log(session, "youtube", handle, sub.id, True, None, False, room_id, "already notified this room", duration_ms, now)
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
                    _add_sweep_log(session, "youtube", handle, sub.id, True, None, True, room_id, "notified", duration_ms, now)
                else:
                    detail = "live but notifications paused" if not notify_on else "live — webhook failed"
                    _add_sweep_log(session, "youtube", handle, sub.id, True, None, False, room_id, detail, duration_ms, now)
            else:
                sub.is_live = False
                await _close_open_sessions(session, sub.id, now)
                detail = "offline" if err is None else f"error:{err}"
                _add_sweep_log(session, "youtube", handle, sub.id, False, err, False, room_id_val, detail, duration_ms, now)

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
            err = getattr(live_info, "error", None)
            room_id_val = getattr(live_info, "room_id", None)
            if err == "not_found":
                if sub.first_not_found_at is None:
                    sub.first_not_found_at = now
                    logger.info(f"handle not resolving @{sub.tiktok_username} — tracking for rename")
                await _close_open_sessions(session, sub.id, now)
                _add_sweep_log(session, "kick", handle, sub.id, False, "not_found", False, room_id_val, "not_found — tracking rename", None, now)
                await session.commit()
                await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                continue
            if err is None and sub.first_not_found_at is not None:
                sub.first_not_found_at = None

            if live_info.error and not live_info.is_live:
                logger.debug(f"kick check inconclusive user={handle} err={live_info.error}")
                _add_sweep_log(session, "kick", handle, sub.id, None, live_info.error, False, room_id_val, f"inconclusive:{live_info.error}", None, now)
                await session.commit()
                await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                continue

            if live_info.is_live:
                room_id = live_info.room_id or f"live-kk-{handle}"
                if _last_room_cache.get("kk:" + handle) == room_id:
                    sub.is_live = True
                    await _open_session(session, sub, room_id, now)
                    _add_sweep_log(session, "kick", handle, sub.id, True, None, False, room_id, "dedup skip — same room", None, now)
                    await session.commit()
                    await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)
                    continue
                if sub.last_room_id == room_id and sub.last_notified_at:
                    if (now - _aware(sub.last_notified_at)) < timedelta(seconds=900):
                        _last_room_cache["kk:" + handle] = room_id
                        sub.is_live = True
                        await _open_session(session, sub, room_id, now)
                        _add_sweep_log(session, "kick", handle, sub.id, True, None, False, room_id, "cooldown skip — 15m", None, now)
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
                    sub.is_live = True
                    _add_sweep_log(session, "kick", handle, sub.id, True, None, False, room_id, "already notified this room", None, now)
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
                    _add_sweep_log(session, "kick", handle, sub.id, True, None, True, room_id, "notified", None, now)
                else:
                    detail = "live but notifications paused" if not notify_on else "live — webhook failed"
                    _add_sweep_log(session, "kick", handle, sub.id, True, None, False, room_id, detail, None, now)
            else:
                sub.is_live = False
                await _close_open_sessions(session, sub.id, now)
                detail = "offline" if err is None else f"error:{err}"
                _add_sweep_log(session, "kick", handle, sub.id, False, err, False, room_id_val, detail, None, now)

            await session.commit()
            await asyncio.sleep(settings.PER_CHECK_SLEEP_SECONDS)

    return {"checked": checked, "notified": notified}


AVATAR_REFRESH_DAYS = 3
_maint_attempts: dict[int, float] = {}  # sub_id -> last migrate-attempt epoch


async def _try_migrate(session, sub, prof, now) -> bool:
    """Move a row to prof's canonical handle when safe. Returns True if moved."""
    new = prof["unique_id"]
    if new == sub.tiktok_username:
        return False
    dup = await session.execute(
        select(Subscription).where(
            Subscription.platform == (sub.platform or "tiktok"),
            Subscription.tiktok_username == new,
            Subscription.id != sub.id,
        )
    )
    if dup.scalar_one_or_none():
        logger.info(f"rename auto-migrate skipped @{sub.tiktok_username}: @{new} already tracked")
        return False
    if sub.tiktok_user_id and prof["user_id"] and prof["user_id"] != sub.tiktok_user_id:
        # Handle now belongs to a different account — never auto-follow.
        logger.warning(f"rename blocked @{sub.tiktok_username}: handle taken over, needs human confirm")
        await log_audit("system", "rename.blocked", f"@{sub.tiktok_username} now resolves elsewhere")
        return False
    old = sub.tiktok_username
    sub.tiktok_username = new
    if prof["user_id"]:
        sub.tiktok_user_id = prof["user_id"]
    if prof["avatar_url"]:
        sub.avatar_url = prof["avatar_url"]
        sub.avatar_checked_at = now
    sub.first_not_found_at = None
    await session.commit()
    await log_audit("system", "creator.rename", f"auto-migrated @{old} -> @{new}")
    logger.info(f"rename auto-migrated @{old} -> @{new}")
    return True


async def _maintenance_renames(session, now):
    """Auto-migrate handles TikTok still resolves to a new canonical name.

    Attempts start at the first flagged sweep (fresh renames still resolve
    best) — the 3-day rule gates only the dashboard attention flag.
    """
    from .tiktok import fetch_tiktok_profile

    res = await session.execute(
        select(Subscription).where(
            Subscription.enabled == True,  # noqa
            Subscription.platform == "tiktok",
            Subscription.first_not_found_at.is_not(None),
        )
    )
    for sub in res.scalars().all():
        if time.time() - _maint_attempts.get(sub.id, 0.0) < 7 * 86400:
            continue  # one attempt per sub per week max
        _maint_attempts[sub.id] = time.time()
        try:
            prof = await fetch_tiktok_profile(sub.tiktok_username)
        except Exception:
            continue
        if prof:
            try:
                await _try_migrate(session, sub, prof, now)
            except Exception as e:
                logger.debug(f"rename migrate failed @{sub.tiktok_username} err={type(e).__name__}")


AVATAR_FETCH_EVERY_SECONDS = 24 * 3600 // 5  # one fetch per ~4.8h ≈ 5 creators/day
_avatar_next_due: float = 0.0


async def _maintenance_avatars(session, now):
    """Deprecated no-op: avatars now refresh on-notify (see poll_once). Kept for compat."""
    return
    from .tiktok import fetch_tiktok_profile

    global _avatar_next_due
    if time.time() < _avatar_next_due:
        return
    stale = now - timedelta(days=AVATAR_REFRESH_DAYS)
    res = await session.execute(
        select(Subscription).where(
            Subscription.enabled == True,  # noqa
            Subscription.platform == "tiktok",
            or_(
                Subscription.avatar_checked_at.is_(None),
                Subscription.avatar_checked_at < stale,
            ),
        )
        .order_by(Subscription.avatar_checked_at.asc().nulls_first())
        .limit(1)
    )
    for sub in res.scalars().all():
        _avatar_next_due = time.time() + AVATAR_FETCH_EVERY_SECONDS  # slot consumed even on failure
        try:
            prof = await fetch_tiktok_profile(sub.tiktok_username)
        except Exception:
            continue
        sub.avatar_checked_at = now
        if not prof:
            await session.commit()
            continue
        if prof["avatar_url"]:
            sub.avatar_url = prof["avatar_url"]
        if not sub.tiktok_user_id and prof["user_id"]:
            sub.tiktok_user_id = prof["user_id"]
        try:
            await _try_migrate(session, sub, prof, now)
        except Exception as e:
            logger.debug(f"avatar rename check failed @{sub.tiktok_username} err={type(e).__name__}")
        await session.commit()


async def maintenance():
    """Bounded background upkeep, once per cycle. Never raises."""
    try:
        now = datetime.now(timezone.utc)
        async with async_session() as session:
            await _maintenance_renames(session, now)
            # _maintenance_avatars removed: avatars now refresh on-notify
            # (fresh avatar fetched right before each notification and saved to card)
            # Drop checker clients for removed creators; cap attempt memory.
            try:
                from .tiktok import checker as _tt

                rows = await session.execute(
                    select(Subscription.tiktok_username).where(
                        Subscription.enabled == True  # noqa
                    )
                )
                _tt.evict_missing({r[0] for r in rows.all()})
                if len(_maint_attempts) > 500:
                    _maint_attempts.clear()
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"maintenance skipped err={type(e).__name__}")


async def poll_loop():
    global _is_running, _next_sweep_at, _last_sweep_at, _sweep_in_progress
    if _is_running:
        return
    _is_running = True
    logger.info(f"poller started interval={settings.CHECK_INTERVAL_SECONDS}s local comfort")
    # first next estimate so schedule page has data before first sweep finishes
    _next_sweep_at = datetime.now(timezone.utc) + timedelta(seconds=settings.CHECK_INTERVAL_SECONDS)
    while True:
        try:
            # Bound the dedup cache: keys are per-creator, but never evicted.
            if len(_last_room_cache) > 500:
                _last_room_cache.clear()
            _sweep_in_progress = True
            _last_sweep_at = datetime.now(timezone.utc)
            result, yt, kk = await poll_cycle()
            await maintenance()  # rename attempts + avatar refreshes (bounded, best-effort)
            peak = rss_mb()
            logger.info(
                f"poll sweep checked={result['checked']}+{yt['checked']}+{kk['checked']} "
                f"notified={result['notified']}+{yt['notified']}+{kk['notified']} "
                f"rss={peak}MB cur={rss_current_mb()}MB"
            )
            if should_recycle(peak):
                logger.warning(f"memory ceiling hit (peak {peak}MB) — recycling process")
                import os as _os

                _os._exit(0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"poll_loop error {type(e).__name__}")
        finally:
            _sweep_in_progress = False
        # schedule next sweep (jitter chosen now so API can show accurate ETA)
        _next_sweep_at = datetime.now(timezone.utc) + timedelta(
            seconds=settings.CHECK_INTERVAL_SECONDS + random.uniform(0, settings.CHECK_JITTER_SECONDS)
        )
        await asyncio.sleep((_next_sweep_at - datetime.now(timezone.utc)).total_seconds())