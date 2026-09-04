from sqlalchemy import String, Boolean, DateTime, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime, timezone
from .db import Base

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
    custom_message: Mapped[str] = mapped_column(Text, default="{ping_role}\n{discord} is LIVE!!!")
    # Global defaults used when a creator leaves their own field empty
    embed_image_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    embed_color: Mapped[str] = mapped_column(String(7), default="#FF0050")
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
    image_url: Mapped[str | None] = mapped_column(Text, nullable=True)  # big photo, default global image
    color: Mapped[str | None] = mapped_column(String(7), nullable=True)  # hex like #FF0050, default global color

    # Legacy TikTok profile cache (no longer fetched, kept for stored avatars)
    display_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    avatar_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    cover_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    is_live: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    last_room_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_live_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_notified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(default=0)

    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
