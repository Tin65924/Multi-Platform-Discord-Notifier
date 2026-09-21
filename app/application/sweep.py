"""Generic platform sweep — ONE implementation for tiktok/youtube/kick.

Canonical sweep state machine:
  snapshot → sort by last_live_at desc → per creator: stuck-heal, check,
  not_found track / inconclusive keep / live notify pipeline / offline flip,
  per-check sleep → single bulk log flush.

Platform differences are adapter config (cache prefix, fallback room shape),
never branches here. The sweep depends only on domain ports; poller.py (the
temporary composition root) injects SQLAlchemy/webhook adapters until Phase 4
moves them to infrastructure/.
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from ..domain.entities import Creator, SweepRecord, WebhookTarget
from ..domain.ports import LiveChecker, Notifier, ProfileLookup, SubscriptionRepo, SweepLogSink
from ..domain.result import CheckOutcome
from .notify_policy import decide_live

logger = logging.getLogger(__name__)


@dataclass
class SweepConfig:
    platform: str = "tiktok"
    cache_prefix: str = "tt:"          # room-dedup key prefix ("tt:"/"yt:"/"kk:")
    cooldown_seconds: int = 900        # notify-once window per room
    stuck_live_hours: float = 3.0      # stuck-LIVE heal threshold
    per_check_sleep: float = 1.0       # politeness gap between creators


class UtcClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


def _sort_live_first(rows: list[Creator]) -> list[str]:
    """Creators who went live most recently check first; never-live last."""
    def _key(u: str, by_live: dict) -> datetime:
        v = by_live.get(u)
        if v is not None and v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v or datetime.min.replace(tzinfo=timezone.utc)

    live_map = {c.handle: c.last_live_at for c in rows}
    return sorted([c.handle for c in rows],
                  key=lambda u: _key(u, live_map), reverse=True)


async def run_platform_sweep(
    *,
    checker: LiveChecker,
    repo: SubscriptionRepo,
    sessions,  # SessionRepo-like: ensure_open/close_open (port lands fully in Phase 4)
    sink: SweepLogSink,
    notifier: Notifier,
    profiles: ProfileLookup | None,
    build_payload,  # (card, target) -> dict; infra embed builder injected by caller
    target: WebhookTarget,
    notify_on: bool,
    room_cache: dict[str, str],
    clock=None,
    config: SweepConfig | None = None,
) -> dict:
    """Run one full platform sweep. Returns {checked, notified, order}."""
    cfg = config or SweepConfig()
    now_clock = clock or UtcClock()

    creators = await repo.snapshot(cfg.platform)
    if not creators:
        return {"checked": 0, "notified": 0, "order": []}
    by_handle = {c.handle: c for c in creators}
    usernames = _sort_live_first(creators)
    order = list(usernames)

    checked = 0
    notified = 0
    pending: list[SweepRecord] = []
    fallback_of = lambda u: f"{checker.fallback_room_prefix}{u}"  # noqa: E731

    for username in usernames:
        checked += 1
        # Stuck-heal: card LIVE for >stuck threshold -> fresh client next check.
        try:
            c0 = by_handle.get(username)
            ll = c0.last_live_at if c0 else None
            if c0 and c0.is_live and ll:
                if ll.tzinfo is None:
                    ll = ll.replace(tzinfo=timezone.utc)
                if (now_clock.now() - ll).total_seconds() > cfg.stuck_live_hours * 3600:
                    checker.drop(username)
        except Exception:
            pass

        t0 = time.monotonic()
        result = await checker.check(username)
        duration_ms = int((time.monotonic() - t0) * 1000)
        now = now_clock.now()

        card = await repo.check_in(by_handle[username].id, now)
        if card is None:
            continue  # removed mid-sweep (legacy counts it checked, no sleep)

        if result.outcome == CheckOutcome.NOT_FOUND:
            await repo.mark_not_found(card.id, now)
            await sessions.close_open(card.id, now)
            room_cache.pop(cfg.cache_prefix + username, None)
            pending.append(SweepRecord(cfg.platform, username, card.id, False,
                                       "not_found", False, result.room_id,
                                       "not_found — tracking rename", duration_ms, now))
            await asyncio.sleep(cfg.per_check_sleep)
            continue

        if result.outcome in (CheckOutcome.LIVE, CheckOutcome.OFFLINE):
            await repo.clear_not_found(card.id)  # clean read clears the streak

        if result.outcome == CheckOutcome.INCONCLUSIVE:
            pending.append(SweepRecord(cfg.platform, username, card.id, None,
                                       result.reason or "check_failed", False,
                                       result.room_id, f"inconclusive:{result.reason or 'check_failed'}",
                                       duration_ms, now))
            await asyncio.sleep(cfg.per_check_sleep)
            continue

        if result.outcome == CheckOutcome.LIVE:
            room_id = result.room_id or fallback_of(username)
            verdict = decide_live(
                room_id=room_id, fallback_room=fallback_of(username),
                cached_room=room_cache.get(cfg.cache_prefix + username),
                last_room_id=card.last_room_id, last_notified_at=card.last_notified_at,
                now=now, cooldown_seconds=cfg.cooldown_seconds)
            if verdict.action in ("dedup", "cooldown"):
                if verdict.cache_room:
                    room_cache[cfg.cache_prefix + username] = room_id
                await repo.mark_seen_live(card.id, cfg.platform, username, room_id, now)
                await sessions.ensure_open(card.id, cfg.platform, username, room_id, now)
                pending.append(SweepRecord(cfg.platform, username, card.id, True, None,
                                           False, room_id, verdict.detail, duration_ms, now))
                await asyncio.sleep(cfg.per_check_sleep)
                continue

            # Fresh live: avatar refresh, embed, session-notified guard, send.
            if profiles is not None:
                try:
                    prof = await profiles.fetch(username)
                    if prof and prof.get("avatar_url"):
                        await repo.update_avatar(card.id, prof["avatar_url"], now)
                except Exception:
                    pass
            payload = build_payload(card, target)
            sess = await sessions.ensure_open(card.id, cfg.platform, username, room_id, now)
            if sess.notified:
                await repo.mark_seen_live(card.id, cfg.platform, username, room_id, now)
                pending.append(SweepRecord(cfg.platform, username, card.id, True, None,
                                           False, room_id, "already notified this room",
                                           duration_ms, now))
                await asyncio.sleep(cfg.per_check_sleep)
                continue
            ok = False
            if notify_on:
                try:
                    ok = await notifier.send(payload)
                except Exception as e:
                    logger.debug(f"notify send failed user={username} err={type(e).__name__}")
            await repo.record_live(card.id, cfg.platform, username, room_id, now, ok)
            if ok:
                notified += 1
                room_cache[cfg.cache_prefix + username] = room_id
                logger.info(f"notified @{username}")
                pending.append(SweepRecord(cfg.platform, username, card.id, True, None,
                                           True, room_id, "notified", duration_ms, now))
            else:
                detail = ("live but notifications paused" if not notify_on
                          else "live — webhook failed")
                pending.append(SweepRecord(cfg.platform, username, card.id, True, None,
                                           False, room_id, detail, duration_ms, now))
        else:  # OFFLINE
            await repo.mark_offline(card.id, now)
            await sessions.close_open(card.id, now)
            room_cache.pop(cfg.cache_prefix + username, None)
            # NOTE: warm client reuse — no evict on the hot path (OOM fix, kept).
            detail = "offline" if result.reason is None else f"error:{result.reason}"
            pending.append(SweepRecord(cfg.platform, username, card.id, False,
                                       result.reason, False, result.room_id,
                                       detail, duration_ms, now))

        await asyncio.sleep(cfg.per_check_sleep)

    await sink.flush(pending)
    return {"checked": checked, "notified": notified, "order": order}
