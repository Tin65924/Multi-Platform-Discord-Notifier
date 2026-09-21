import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta

from sqlalchemy import select, text

from .application.sweep import SweepConfig, run_platform_sweep
from .config import get_settings
from .domain.entities import SweepRecord, WebhookTarget
from .infrastructure.checkers.kick import checker as kick_checker
from .infrastructure.checkers.tiktok import TikTokLivePort, TikTokProfilePort
from .infrastructure.checkers.youtube import YouTubeLivePort
from .infrastructure.notify.discord import resolve_webhook_cfg
from .infrastructure.persistence.database import async_session, open_session
from .infrastructure.persistence.models import GlobalSettings, Subscription
from .infrastructure.persistence.repos import (
    SessionRepo, SubscriptionRepo, SweepLogSink, WebhookNotifier, payload_builder)
from .infrastructure.scheduler import state as sched_state
from .security import log_audit

logger = logging.getLogger(__name__)
settings = get_settings()

_last_room_cache: dict[str, str] = {}


async def poll_cycle():
    """Full cycle: all three platform sweeps concurrently.

    Holds the cycle lock so cron triggers skip while the loop runs (and
    vice versa). Sweep-level isolation is by platform; they share nothing
    mutable except disjoint _last_room_cache keys.
    """
    async with sched_state.sweep_lock:
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
    if sched_state.sweep_lock.locked():
        logger.info("cron sweep skipped: cycle already running")
        return {"checked": 0, "notified": 0, "skipped": "busy"}
    base, yt, kk = await poll_cycle()
    return {
        "checked": base["checked"] + yt["checked"] + kk["checked"],
        "notified": base["notified"] + yt["notified"] + kk["notified"],
    }


async def notifications_enabled(session) -> bool:
    """Global kill-switch. NULL (pre-migration rows) counts as ON."""
    gs = await session.get(GlobalSettings, 1)
    if gs is None or gs.notifications_enabled is None:
        return True
    return bool(gs.notifications_enabled)


async def get_global_settings():
    session = await open_session()
    try:
        gs = await session.get(GlobalSettings, 1)
        return resolve_webhook_cfg(gs)
    finally:
        await session.close()

# --- Composition root --------------------------------------------------------
# Thin wrappers: resolve config, build infrastructure adapters, run the generic
# application sweep. No branching logic lives here anymore.
async def _run_sweep(platform: str, cache_prefix: str, checker, profiles,
                   build, cfg, notify_on: bool, extra: dict | None = None):
    """Shared wrapper: run the generic sweep, expose order for schedule ETA."""
    out = await run_platform_sweep(
        checker=checker, repo=SubscriptionRepo(), sessions=SessionRepo(),
        sink=SweepLogSink(), notifier=WebhookNotifier(cfg[0]), profiles=profiles,
        build_payload=build,
        target=WebhookTarget(url=cfg[0], message=cfg[2],
                             ping_role_id=cfg[1], ping_everyone=cfg[3],
                             image_url=cfg[4], color=cfg[5]),
        notify_on=notify_on, room_cache=_last_room_cache,
        config=SweepConfig(platform=platform, cache_prefix=cache_prefix,
                           per_check_sleep=settings.PER_CHECK_SLEEP_SECONDS))
    if platform == "tiktok":
        # Expose order for schedule ETA (next sweep = _next_sweep_at + index*PER_CHECK_SLEEP)
        sched_state.set_last_sweep_order(out["order"])
    result = {"checked": out["checked"], "notified": out["notified"]}
    if extra:
        result.update(extra)
    return result


async def poll_once():
    """TikTok sweep via the generic application sweep (same state machine)."""
    async with async_session() as session:
        notify_on = await notifications_enabled(session)

    cfg = await get_global_settings()
    if not cfg[0]:
        logger.warning("poll skipped: no webhook configured in DB or env")
        return {"checked": 0, "notified": 0, "error": "no webhook"}
    return await _run_sweep("tiktok", "tt:", TikTokLivePort(), TikTokProfilePort(),
                            payload_builder("tiktok"), cfg, notify_on)

async def poll_youtube():
    """YouTube sweep via the generic application sweep (no avatar fetch)."""
    async with async_session() as session:
        notify_on = await notifications_enabled(session)

    cfg = await get_global_settings()
    if not cfg[0]:
        logger.warning("youtube poll skipped: no webhook configured in DB or env")
        return {"checked": 0, "notified": 0, "error": "no webhook"}
    return await _run_sweep("youtube", "yt:",
                            YouTubeLivePort(api_key=settings.YOUTUBE_API_KEY or ""),
                            None, payload_builder("youtube"), cfg, notify_on)


async def poll_kick():
    """Kick sweep via the generic application sweep (no avatar fetch).

    Skipped entirely without KICK_CLIENT_ID/SECRET.
    """
    from .infrastructure.checkers.kick import KickLivePort
    kick_id = settings.KICK_CLIENT_ID or ""
    kick_secret = settings.KICK_CLIENT_SECRET or ""
    if not kick_checker.configured(kick_id, kick_secret):
        return {"checked": 0, "notified": 0, "skipped": "unconfigured"}
    async with async_session() as session:
        notify_on = await notifications_enabled(session)

    cfg = await get_global_settings()
    if not cfg[0]:
        logger.warning("kick poll skipped: no webhook configured in DB or env")
        return {"checked": 0, "notified": 0, "error": "no webhook"}
    return await _run_sweep("kick", "kk:",
                            KickLivePort(kick_id, kick_secret),
                            None, payload_builder("kick"), cfg, notify_on)


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
    from .infrastructure.checkers.tiktok import fetch_tiktok_profile

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


async def maintenance():
    """Bounded background upkeep, once per cycle. Never raises."""
    try:
        now = datetime.now(timezone.utc)
        async with async_session() as session:
            await _maintenance_renames(session, now)
            # Auto-heal stuck LIVE cards (user reported 7:00-9:30 still LIVE at 10:48)
            # If a card has been LIVE for >3h, it's likely a stale reused client.
            try:
                stuck_cut = now - timedelta(hours=3)
                res = await session.execute(
                    select(Subscription).where(
                        Subscription.is_live == True,  # noqa
                        Subscription.last_live_at != None,  # noqa
                        Subscription.last_live_at < stuck_cut,
                    )
                )
                for sub in res.scalars().all():
                    lc = sub.last_checked_at
                    if lc is not None:
                        if lc.tzinfo is None:
                            lc = lc.replace(tzinfo=timezone.utc)
                        if (now - lc).total_seconds() > 3600:
                            continue  # not checked recently — let next sweep decide
                    platform = sub.platform or "tiktok"
                    handle = sub.tiktok_username
                    sub_id = sub.id
                    last_live = sub.last_live_at
                    await SubscriptionRepo().mark_offline(sub_id, now)
                    prefix = "tt:" if platform == "tiktok" else f"{platform}:"
                    _last_room_cache.pop(prefix + handle, None)
                    if platform == "tiktok":
                        try:
                            from .infrastructure.checkers.tiktok import checker as _tt2
                            _tt2.drop(handle)
                        except Exception:
                            pass
                    await SweepLogSink().flush([SweepRecord(
                        platform, handle, sub_id, False, None, False, None,
                        "auto-heal stuck LIVE -> offline", None, now)])
                    logger.info(f"auto-heal stuck LIVE @{handle} last_live {last_live} -> offline")
                await session.commit()
            except Exception as e:
                logger.debug(f"stuck heal skipped err={type(e).__name__}")
            # Retention: sweep_logs 2-day auto-delete (also pruned on boot in db.init_db)
            try:
                cut2 = now - timedelta(days=2)
                await session.execute(text("DELETE FROM sweep_logs WHERE created_at < :cut"), {"cut": cut2})
                await session.commit()
            except Exception as e:
                logger.debug(f"sweep prune skipped err={type(e).__name__}")
            # Stale-live guard is now the targeted >3h heal above (per-card, rare).
            # The old 30m blind clear wiped all warm clients without close() and
            # re-created them next sweep — a 20MB spike each time on top of the
            # per-sweep churn. Removed: warm reuse is the steady state.
            # _maintenance_avatars removed: avatars now refresh on-notify
            # (fresh avatar fetched right before each notification and saved to card)
            # Drop checker clients for removed creators; cap attempt memory.
            try:
                from .infrastructure.checkers.tiktok import checker as _tt

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


