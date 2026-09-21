"""Shared router helpers — moved verbatim from app/api/routes.py (Phase 3).

No logic changes: _webhook_cfg, _style_for, _payload_for, _analytics_range.
"""
import logging
from datetime import datetime, timedelta, timezone

from ...config import get_settings
from ...infrastructure.persistence.models import Subscription
from ...infrastructure.notify.discord import build_embed, effective_image, resolve_webhook_cfg

logger = logging.getLogger(__name__)
settings = get_settings()


def _webhook_cfg(gs):
    return resolve_webhook_cfg(gs)


def _style_for(sub: Subscription, image: str | None, color: str) -> dict:
    """Resolve per-creator style with global fallbacks.

    The embed description is a randomized forest line (Creator Name as NAME);
    the message is always the global template (per-creator overrides retired).
    """
    return {
        "author_name": sub.author_name or None,
        "discord_username": sub.discord_username or None,
        "discord_user_id": sub.discord_user_id or None,
        "image_url": effective_image(sub, image),
        "color": sub.color or color,
    }


def _payload_for(sub: Subscription, msg: str, ping: str | None, everyone: bool, image: str | None, color: str):
    style = _style_for(sub, image, color)
    return build_embed(
        sub.tiktok_username,
        message=msg,
        ping_role_id=ping,
        ping_everyone=everyone,
        image_url=style["image_url"],
        color=style["color"],
        author_name=style["author_name"],
        platform=sub.platform or "tiktok",
        discord_username=style["discord_username"],
        discord_user_id=style["discord_user_id"],
    )


def _analytics_range(frm: str | None, to: str | None, days: int):
    """UTC instant window from explicit ISO bounds or a trailing day count."""
    from ...analytics import aware_utc

    now = datetime.now(timezone.utc)
    t = aware_utc(datetime.fromisoformat(to)) if to else now
    f = aware_utc(datetime.fromisoformat(frm)) if frm else t - timedelta(days=max(1, min(days, 180)))
    return f, t, now
