import logging
import time
from pathlib import Path

import httpx
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Discord sits behind Cloudflare, which challenges the default
# "python-httpx/..." User-Agent — a browser UA passes far more often.
_DISCORD_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
)

# Process-wide send pause after a 429, so we stop hammering a flagged IP.
# (Failing sends never mark a live as notified, so without this the poller
# would retry every sweep and extend the rate-limit.)
_backoff_until: float = 0.0

from .config import get_settings as _get_settings

try:
    _settings = _get_settings()
except Exception:
    _settings = None

# --- HARDCODED FALLBACKS ---------------------------------------------------
# Last-resort defaults when neither creator nor global settings define them.
# Prefer the dashboard (global) or .env over editing here.
HARDCODED_IMAGE_URL = ""
HARDCODED_COLOR = "#FF0050"  # TikTok pink

STATIC_COVER_PATH = Path(__file__).resolve().parent / "static" / "cover.jpg"


def resolve_image(setting_url: str | None, env_url: str = "", public_base: str = "") -> str | None:
    for candidate in (setting_url, env_url, HARDCODED_IMAGE_URL):
        if candidate and candidate.strip().startswith("http"):
            return candidate.strip()
    if public_base and STATIC_COVER_PATH.exists():
        return public_base.rstrip("/") + "/static/cover.jpg"
    return None


def parse_color(value: str | None) -> int:
    """Hex '#FF0050'/'FF0050' -> int. Falls back to pink on garbage."""
    raw = (value or "").strip().lstrip("#")
    try:
        if len(raw) == 6:
            return int(raw, 16)
    except ValueError:
        pass
    return int(HARDCODED_COLOR.lstrip("#"), 16)


def resolve_color(value: str | None) -> tuple[str, int]:
    """Return (normalized_hex, int) for storage + sending."""
    raw = (value or "").strip()
    if not raw.startswith("#"):
        raw = "#" + raw
    return raw.upper() if len(raw) == 7 else HARDCODED_COLOR, parse_color(raw)


from .wildlines import random_wild_line


def discord_mention(discord_user_id: str | None, discord_username: str | None, account: str) -> str:
    """Real <@id> mention when the numeric user ID is known, else @text."""
    uid = (discord_user_id or "").strip()
    if uid:
        return f"<@{uid}>"
    text = f"@{(discord_username or '').strip().lstrip('@')}" or account
    return account if text == "@" else text


# --- Multi-platform ----------------------------------------------------------
# Live-checking currently only supports TikTok — other platforms are stored,
# listed and filtered in the dashboard until their pollers land. Embeds and
# profile links are already platform-aware so Test/Payload work everywhere.
PLATFORMS = ("tiktok", "youtube", "twitch", "kick")

PLATFORM_LABELS = {
    "tiktok": "TikTok",
    "youtube": "YouTube",
    "twitch": "Twitch",
    "kick": "Kick",
}

# Platforms the poller actually live-checks (others are stored, not swept).
# Kick requires KICK_CLIENT_ID/SECRET — without them its rows are skipped.
LIVE_CHECK_PLATFORMS = ("tiktok", "youtube", "kick")


def is_youtube_channel_id(handle: str) -> bool:
    h = (handle or "").strip()
    return len(h) == 24 and h.startswith("UC") and all(c.isalnum() or c in "-_" for c in h)


def platform_label(platform: str | None) -> str:
    return PLATFORM_LABELS.get((platform or "tiktok").lower(), "TikTok")


def display_account(platform: str | None, handle: str) -> str:
    """Human account text: @handle everywhere, except raw YouTube channel IDs."""
    if (platform or "tiktok").lower() == "youtube" and is_youtube_channel_id(handle):
        return "this channel"
    return f"@{handle}"


def platform_profile_url(platform: str | None, handle: str) -> str:
    p = (platform or "tiktok").lower()
    if p == "twitch":
        return f"https://www.twitch.tv/{handle}"
    if p == "kick":
        return f"https://kick.com/{handle}"
    if p == "youtube":
        if is_youtube_channel_id(handle):
            return f"https://www.youtube.com/channel/{handle}"
        return f"https://www.youtube.com/@{handle}"
    return f"https://www.tiktok.com/@{handle}"


def platform_live_url(platform: str | None, handle: str) -> str:
    p = (platform or "tiktok").lower()
    if p == "twitch":
        return f"https://www.twitch.tv/{handle}"
    if p == "kick":
        return f"https://kick.com/{handle}"
    if p == "youtube":
        base = platform_profile_url(p, handle)
        return f"{base}/live"
    return f"https://www.tiktok.com/@{handle}/live"


def _ping_mention(ping_everyone: bool, ping_role_id: str | None) -> str:
    if ping_everyone:
        return "@everyone"
    if ping_role_id:
        return f"<@&{ping_role_id}>"
    return ""


def build_embed(
    username: str,
    message: str | None = None,
    ping_role_id: str | None = None,
    ping_everyone: bool = True,
    image_url: str | None = None,
    color: str | None = None,
    author_name: str | None = None,
    platform: str | None = "tiktok",
    discord_username: str | None = None,
    discord_user_id: str | None = None,
) -> dict:
    """
    Fully owned embed — nothing fetched from any platform.
      content: message template ("<@user-id> is LIVE!")
      embed:   handle header, one randomized forest line, big image,
               color bar, Watch Stream / Profile buttons.
    Template tags: {account} {discord} {link} {ping_role}
      {account} = platform handle (@user); {discord} = real <@id> mention
      when discord_user_id is set, else @username text, else {account}.
      {ping_role} is opt-in: only fires when present in the template
      (no automatic prefixing).
    """
    link = platform_live_url(platform, username)
    profile_link = platform_profile_url(platform, username)
    account = display_account(platform, username)
    discord = discord_mention(discord_user_id, discord_username, account)
    wild_name = (author_name or "").strip().lstrip("@") or account.lstrip("@")

    ping_mention = _ping_mention(ping_everyone, ping_role_id)
    content = (
        (message or "{discord} is LIVE!")
        .replace("{discord}", discord)
        .replace("{account}", account)
        .replace("{link}", link)
        .replace("{ping_role}", ping_mention)
    ).strip()[:2000]
    if not content:
        content = f"{discord} is LIVE!"

    image = resolve_image(
        image_url,
        env_url=getattr(_settings, "EMBED_IMAGE_URL", "") or "",
        public_base=getattr(_settings, "PUBLIC_BASE_URL", "") or "",
    )
    embed: dict = {
        "author": {"name": account},
        "description": f"[{random_wild_line(wild_name)}]({link})",
        "color": parse_color(color),
        "url": link,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": f"Forest Watcher • {platform_label(platform)}"},
    }
    if image:
        embed["image"] = {"url": image}

    payload: dict = {
        "username": "Forest Watcher",
        "embeds": [embed],
        "components": [
            {
                "type": 1,
                "components": [
                    {"type": 2, "style": 5, "label": "Watch Stream", "url": link},
                    {"type": 2, "style": 5, "label": "Profile", "url": profile_link},
                ],
            }
        ],
    }
    if content:
        payload["content"] = content
    # User mentions only parse when "users" is allowed — add it whenever the
    # message actually contains a real <@id> mention.
    mention_users = bool((discord_user_id or "").strip()) and f"<@{(discord_user_id or '').strip()}>" in content
    if ping_everyone:
        payload["allowed_mentions"] = {"parse": ["everyone"] + (["users"] if mention_users else [])}
    elif ping_role_id:
        payload["allowed_mentions"] = {"parse": ["roles"] + (["users"] if mention_users else [])}
    else:
        payload["allowed_mentions"] = {"parse": ["users"] if mention_users else []}
    return payload


def with_components_param(webhook_url: str) -> str:
    """Discord strips components (buttons) from plain webhook posts.

    Per Discord docs, webhook sends need `?with_components=true` or the
    buttons are silently ignored. Stored URLs stay clean — this is applied
    at send time only.
    """
    if "with_components=" in webhook_url:
        return webhook_url
    sep = "&" if "?" in webhook_url else "?"
    return f"{webhook_url}{sep}with_components=true"


def _retry_after_seconds(resp) -> int:
    """Honor Discord/Cloudflare Retry-After (seconds or HTTP date)."""
    try:
        ra = (resp.headers.get("retry-after") or "").strip()
        if ra.isdigit():
            return int(ra)
        if ra:
            from email.utils import parsedate_to_datetime

            dt = parsedate_to_datetime(ra)
            return max(0, int((dt - datetime.now(timezone.utc)).total_seconds()))
    except Exception:
        pass
    return 0


async def send_webhook(webhook_url: str, payload: dict, timeout: int = 10) -> bool:
    global _backoff_until
    if not webhook_url or "discord.com/api/webhooks" not in webhook_url:
        logger.error("webhook url invalid")
        return False
    now = time.monotonic()
    if now < _backoff_until:
        logger.debug(f"webhook backing off {int(_backoff_until - now)}s left (rate limited)")
        return False
    try:
        emb = (payload.get("embeds") or [{}])[0]
        comps = payload.get("components") or []
        n_btns = sum(len(r.get("components", [])) for r in comps)
        logger.info(
            "webhook sending has_content=%s has_image=%s buttons=%s",
            bool(payload.get("content")), bool((emb.get("image") or {}).get("url")), n_btns,
        )
        relay_url = ""
        relay_secret = ""
        try:
            relay_url = (getattr(_settings, "RELAY_URL", "") or "").strip()
            relay_secret = (getattr(_settings, "RELAY_SECRET", "") or "").strip()
        except Exception:
            pass
        async with httpx.AsyncClient(timeout=timeout, headers={"User-Agent": _DISCORD_UA}) as client:
            if relay_url and relay_secret:
                # Cloudflare Worker relay: it forwards to Discord from clean IPs.
                # Query params (with_components) ride along; Worker forwards them.
                logger.info("webhook sending via relay")
                resp = await client.post(
                    with_components_param(relay_url),
                    json={"secret": relay_secret, "payload": payload},
                )
            else:
                resp = await client.post(with_components_param(webhook_url), json=payload)
            if resp.status_code in (200, 204):
                _backoff_until = 0.0
                return True
            if resp.status_code == 429:
                wait = _retry_after_seconds(resp) or 300
                wait = min(max(wait, 60), 900)
                _backoff_until = time.monotonic() + wait
                logger.warning(
                    f"webhook 429 rate-limited, backing off {wait}s "
                    f"(retry-after={resp.headers.get('retry-after')})"
                )
                return False
            logger.warning(f"webhook failed status={resp.status_code} body={resp.text[:200]}")
            return False
    except Exception as e:
        logger.warning(f"webhook exception {type(e).__name__}")
        return False
