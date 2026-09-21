"""Domain entities — plain dataclasses, no ORM, no framework.

These are the shapes the application layer reasons about. Infrastructure
(SQLAlchemy rows) maps to/from them at repository boundaries (Phase 4).
Field sets mirror exactly what the sweep reads today — nothing more.
"""
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class Creator:
    """A tracked platform account as the sweep sees it."""
    id: int
    handle: str
    platform: str = "tiktok"
    is_live: bool = False
    last_live_at: datetime | None = None
    last_room_id: str | None = None
    last_notified_at: datetime | None = None
    first_not_found_at: datetime | None = None


@dataclass(frozen=True)
class CreatorCard(Creator):
    """Full card for embed building + policy (style fields included)."""
    author_name: str | None = None
    discord_username: str | None = None
    discord_user_id: str | None = None
    image_url: str | None = None
    avatar_url: str | None = None
    image_mime: str | None = None
    color: str | None = None


@dataclass(frozen=True)
class SessionState:
    """Open-session lookup result: has this room already notified?"""
    notified: bool


@dataclass(frozen=True)
class WebhookTarget:
    """Resolved global webhook config for one sweep."""
    url: str
    message: str = "{discord} is LIVE!"
    ping_role_id: str | None = None
    ping_everyone: bool = True
    image_url: str | None = None
    color: str = "#FF0050"


@dataclass(frozen=True)
class SweepRecord:
    """One per-creator check trace for the Logs page (maps 1:1 to sweep_logs)."""
    platform: str
    handle: str
    sub_id: int | None
    is_live: bool | None
    error: str | None
    notified: bool
    room_id: str | None
    detail: str | None
    duration_ms: int | None
    created_at: datetime


@dataclass
class NotifyDecision:
    """Pure notify-policy verdict (see application/notify_policy.py, Phase 2)."""
    action: str  # "notify" | "dedup" | "cooldown" | "already_notified" | "paused" | "failed"
    room_id: str | None = None
    detail: str = ""
    # Cache write the caller must apply on non-notify live paths:
    cache_room: bool = False
    touch_live: bool = False
