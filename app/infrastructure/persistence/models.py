from sqlalchemy import String, Boolean, DateTime, LargeBinary, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime, timezone
from .database import Base

ROLE_SUPERADMIN = "superadmin"
ROLE_ADMIN = "admin"


def utcnow():
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False, default=ROLE_ADMIN)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    @property
    def is_superadmin(self) -> bool:
        return self.role == ROLE_SUPERADMIN


class AuditLog(Base):
    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    actor: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class GlobalSettings(Base):
    __tablename__ = "global_settings"
    id: Mapped[int] = mapped_column(primary_key=True)  # always 1
    webhook_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    ping_role_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    ping_everyone: Mapped[bool] = mapped_column(Boolean, default=True)
    custom_message: Mapped[str] = mapped_column(Text, default="{discord} is LIVE!")
    # Global defaults used when a creator leaves their own field empty
    embed_image_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    embed_color: Mapped[str] = mapped_column(String(7), default="#FF0050")
    # Global kill-switch (game maintenance etc.): False pauses automatic
    # Discord posts. Detection, sessions and analytics keep running.
    notifications_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Subscription(Base):
    __tablename__ = "subscriptions"
    # One row = one platform account. The same creator may have rows on several
    # platforms, but a (platform, handle) pair is unique — a platform account
    # belongs to exactly one tracked creator.
    __table_args__ = (
        UniqueConstraint("platform", "tiktok_username", name="uq_subscriptions_platform_username"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(16), nullable=False, default="tiktok", server_default="tiktok", index=True)
    tiktok_username: Mapped[str] = mapped_column(String(64), nullable=False, index=True)  # platform handle (see platform)
    label: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Per-creator embed customization (empty = use global default).
    # Previewed live in the dashboard before saving.
    author_name: Mapped[str | None] = mapped_column(String(128), nullable=True)  # Creator Name: embed header + link text
    discord_username: Mapped[str | None] = mapped_column(String(64), nullable=True)  # display name for {discord} fallback
    discord_user_id: Mapped[str | None] = mapped_column(String(32), nullable=True)  # numeric ID -> real <@id> mention
    message: Mapped[str | None] = mapped_column(Text, nullable=True)  # content override, tags: {account} {link} {ping_role}
    link_text: Mapped[str | None] = mapped_column(String(128), nullable=True)  # default: Watch user's LIVE! (no @)
    image_url: Mapped[str | None] = mapped_column(Text, nullable=True)  # link fallback, default global image
    # Uploaded photo (Neon-backed; Render disk is ephemeral so files can't live there).
    # deferred: list queries never load the bytes — only the media endpoint reads them.
    image_blob: Mapped[bytes | None] = mapped_column(LargeBinary, deferred=True, nullable=True)
    image_mime: Mapped[str | None] = mapped_column(String(16), nullable=True)  # set <=> photo exists
    color: Mapped[str | None] = mapped_column(String(7), nullable=True)  # hex like #FF0050, default global color

    # Auto avatar (refreshed weekly from the platform profile page)
    display_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    avatar_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    avatar_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cover_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Rename tracking: immutable numeric platform id (hijack guard) + start
    # of the current "handle not found" streak (None = resolving fine).
    tiktok_user_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    first_not_found_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    is_live: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    last_room_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_live_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_notified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class LiveSession(Base):
    """One detected live session per row — the analytics source of truth.

    A session is NOT a notification: cooldown-skipped lives still open rows.
    Open rows (ended_at NULL) mean currently live. Created by create_all on
    fresh and existing DBs alike — no migration statements needed.
    """

    __tablename__ = "live_sessions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscription_id: Mapped[int] = mapped_column(nullable=False, index=True)
    platform: Mapped[str] = mapped_column(String(16), nullable=False, default="tiktok")
    handle: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    room_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notified: Mapped[bool] = mapped_column(Boolean, default=False)


class SweepLog(Base):
    """Per-creator check trace for the Logs page (sweeps only).

    One row per creator per sweep: what was checked, what happened, was it
    notified. Created by create_all — no manual migration needed.
    Auto-deleted after 2 days (+ 90k-row safety cap); pruned on boot and
    every sweep (see database.init_db + poller.maintenance).
    """

    __tablename__ = "sweep_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    platform: Mapped[str] = mapped_column(String(16), nullable=False, default="tiktok", index=True)
    handle: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    subscription_id: Mapped[int | None] = mapped_column(nullable=True, index=True)
    is_live: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    error: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    notified: Mapped[bool] = mapped_column(Boolean, default=False)
    room_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
