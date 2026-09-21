"""Port implementations — SQLAlchemy + Discord adapters for the domain ports.

Moved here from poller.py (Phase 4). They hold NO branching logic; every
decision lives in application/sweep.py + notify_policy.py. Session economy
matches the legacy poller: one short session per method.
"""
import logging
from datetime import datetime

from sqlalchemy import or_, select

from ...domain.entities import Creator, CreatorCard, SessionState, SweepRecord, WebhookTarget
from .database import async_session
from .models import LiveSession, Subscription
from ..notify.discord import build_embed, effective_image, send_webhook

logger = logging.getLogger(__name__)


async def _open_session(session, sub, room_id: str, now: datetime):
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


async def _close_open_sessions(session, sub_id: int, now: datetime):
    """Stamp ended_at on any open rows. Idempotent — safe on every check."""
    res = await session.execute(
        select(LiveSession).where(
            LiveSession.subscription_id == sub_id,
            LiveSession.ended_at.is_(None),
        )
    )
    for o in res.scalars().all():
        o.ended_at = now


def card_of(sub: Subscription) -> CreatorCard:
    """ORM Subscription -> detached domain CreatorCard."""
    return CreatorCard(
        id=sub.id, handle=sub.tiktok_username, platform=sub.platform or "tiktok",
        is_live=bool(sub.is_live), last_live_at=sub.last_live_at,
        last_room_id=sub.last_room_id, last_notified_at=sub.last_notified_at,
        first_not_found_at=sub.first_not_found_at, author_name=sub.author_name,
        discord_username=sub.discord_username, discord_user_id=sub.discord_user_id,
        image_url=sub.image_url, avatar_url=sub.avatar_url,
        image_mime=sub.image_mime, color=sub.color)


class SubscriptionRepo:
    """domain SubscriptionRepo over SQLAlchemy (one short session per method)."""

    async def snapshot(self, platform: str) -> list[Creator]:
        async with async_session() as session:
            conds = [Subscription.platform == platform]
            if platform == "tiktok":
                # NULL counts as tiktok so pre-platform rows keep polling.
                conds.append(Subscription.platform.is_(None))
            result = await session.execute(
                select(Subscription).where(
                    Subscription.enabled == True,  # noqa
                    or_(*conds),
                )
            )
            return [Creator(id=s.id, handle=s.tiktok_username,
                            platform=s.platform or "tiktok", is_live=bool(s.is_live),
                            last_live_at=s.last_live_at, last_room_id=s.last_room_id,
                            last_notified_at=s.last_notified_at,
                            first_not_found_at=s.first_not_found_at)
                    for s in result.scalars().all()]

    async def check_in(self, sub_id: int, at: datetime) -> CreatorCard | None:
        async with async_session() as session:
            sub = await session.get(Subscription, sub_id)
            if sub is None:
                return None  # removed mid-sweep
            sub.last_checked_at = at
            await session.commit()
            return card_of(sub)

    async def mark_not_found(self, sub_id: int, at: datetime) -> None:
        async with async_session() as session:
            sub = await session.get(Subscription, sub_id)
            if sub is None:
                return
            if sub.first_not_found_at is None:
                sub.first_not_found_at = at
                logger.info(f"handle not resolving @{sub.tiktok_username} — tracking for rename")
            sub.is_live = False
            await _close_open_sessions(session, sub.id, at)
            await session.commit()

    async def clear_not_found(self, sub_id: int) -> None:
        async with async_session() as session:
            sub = await session.get(Subscription, sub_id)
            if sub is None:
                return
            if sub.first_not_found_at is not None:
                sub.first_not_found_at = None  # clean read clears streak
                await session.commit()

    async def mark_offline(self, sub_id: int, at: datetime) -> None:
        async with async_session() as session:
            sub = await session.get(Subscription, sub_id)
            if sub is None:
                return
            sub.is_live = False
            await _close_open_sessions(session, sub.id, at)
            await session.commit()

    async def mark_seen_live(self, sub_id: int, platform: str, handle: str,
                             room_id: str, at: datetime) -> None:
        async with async_session() as session:
            sub = await session.get(Subscription, sub_id)
            if sub is None:
                return
            sub.is_live = True
            await _open_session(session, sub, room_id, at)
            await session.commit()

    async def record_live(self, sub_id: int, platform: str, handle: str,
                          room_id: str, at: datetime, did_notify: bool) -> None:
        async with async_session() as session:
            sub = await session.get(Subscription, sub_id)
            if sub is None:
                return
            sub.last_room_id = room_id
            sub.last_live_at = at
            sub.is_live = True
            if did_notify:
                sub.last_notified_at = at
            row = await _open_session(session, sub, room_id, at)
            if did_notify:
                row.notified = True
            await session.commit()

    async def update_avatar(self, sub_id: int, avatar_url: str, at: datetime) -> None:
        async with async_session() as session:
            sub = await session.get(Subscription, sub_id)
            if sub is None:
                return
            sub.avatar_url = avatar_url
            sub.avatar_checked_at = at
            await session.commit()


class SessionRepo:
    """Open/close LiveSession rows (own short sessions, never card state)."""

    async def ensure_open(self, sub_id: int, platform: str, handle: str,
                          room_id: str, at: datetime) -> SessionState:
        async with async_session() as session:
            sub = await session.get(Subscription, sub_id)
            if sub is None:
                return SessionState(notified=False)
            row = await _open_session(session, sub, room_id, at)
            notified = bool(row.notified)
            await session.commit()
            return SessionState(notified=notified)

    async def close_open(self, sub_id: int, at: datetime) -> None:
        async with async_session() as session:
            await _close_open_sessions(session, sub_id, at)
            await session.commit()


class SweepLogSink:
    """Bulk sweep-log writer — one session + one commit per flush."""

    async def flush(self, rows: list[SweepRecord]) -> None:
        if not rows:
            return
        try:
            async with async_session() as session:
                session.add_all([_row_of(r) for r in rows])
                await session.commit()
        except Exception as e:
            if 'sweep_logs' in str(e).lower() or 'no such table' in str(e).lower():
                try:
                    from .database import Base, engine
                    async with engine.begin() as conn:
                        await conn.run_sync(Base.metadata.create_all)
                    async with async_session() as session:
                        session.add_all([_row_of(r) for r in rows])
                        await session.commit()
                        return
                except Exception:
                    pass
            logger.debug(f'sweep_log bulk flush failed {type(e).__name__} rows={len(rows)}')


def _row_of(r: SweepRecord):
    from .models import SweepLog
    return SweepLog(
        platform=r.platform, handle=r.handle, subscription_id=r.sub_id,
        is_live=r.is_live, error=(r.error or None), notified=bool(r.notified),
        room_id=(r.room_id or None), detail=(r.detail or "")[:500] or None,
        duration_ms=r.duration_ms, created_at=r.created_at)


async def fetch_analytics_window(session, f, t):
    """All sessions overlapping [f, t), plus the enabled roster."""
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


class WebhookNotifier:
    """domain Notifier over Discord webhooks (direct or Worker relay)."""

    def __init__(self, url: str, timeout: int = 10):
        self.url = url
        self.timeout = timeout

    async def send(self, payload: dict) -> bool:
        return await send_webhook(self.url, payload, timeout=self.timeout)


def payload_builder(platform: str):
    """Embed builder for one platform: (card, target) -> Discord payload."""
    def build(card: CreatorCard, target: WebhookTarget) -> dict:
        return build_embed(
            card.handle, message=target.message, ping_role_id=target.ping_role_id,
            ping_everyone=target.ping_everyone,
            image_url=effective_image(card, target.image_url),
            color=card.color or target.color, author_name=card.author_name,
            platform=platform,
            discord_username=card.discord_username, discord_user_id=card.discord_user_id)
    return build
