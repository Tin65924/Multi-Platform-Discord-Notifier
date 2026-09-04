from pydantic import BaseModel, field_validator, model_validator
import re

TIKTOK_URL_RE = re.compile(r"(?:https?://)?(?:www\.)?tiktok\.com/@([A-Za-z0-9._]{2,24})")
TIKTOK_USERNAME_RE = re.compile(r"^[A-Za-z0-9._]{2,24}$")

TWITCH_URL_RE = re.compile(r"(?:https?://)?(?:www\.)?twitch\.tv/([A-Za-z0-9_]{4,25})")
TWITCH_LOGIN_RE = re.compile(r"^[A-Za-z0-9_]{4,25}$")

KICK_URL_RE = re.compile(r"(?:https?://)?(?:www\.)?kick\.com/([A-Za-z0-9_]{3,25})")
KICK_SLUG_RE = re.compile(r"^[A-Za-z0-9_]{3,25}$")

YOUTUBE_URL_RE = re.compile(
    r"(?:https?://)?(?:www\.|m\.)?youtube\.com/"
    r"(?:@([A-Za-z0-9._-]{3,30})|channel/(UC[A-Za-z0-9_-]{22})|c/([A-Za-z0-9._-]{3,})|user/([A-Za-z0-9._-]{3,}))"
)
YOUTUBE_HANDLE_RE = re.compile(r"^@?[A-Za-z0-9._-]{3,30}$")
YOUTUBE_CHANNEL_RE = re.compile(r"^UC[A-Za-z0-9_-]{22}$")

PLATFORMS = ("tiktok", "youtube", "twitch", "kick")

DISCORD_WEBHOOK_RE = re.compile(r"^https://(discord\.com|discordapp\.com)/api/webhooks/\d+/[A-Za-z0-9_\-]+$")


def normalize_handle(platform: str, raw: str) -> str:
    """Turn a pasted profile link or username into the stored platform handle."""
    v = (raw or "").strip()
    if not v:
        raise ValueError("Paste a profile link or username")
    if platform == "twitch":
        m = TWITCH_URL_RE.search(v)
        name = (m.group(1) if m else v.lstrip("@")).lower()
        if not TWITCH_LOGIN_RE.match(name):
            raise ValueError("Invalid Twitch username or channel URL")
        return name
    if platform == "kick":
        m = KICK_URL_RE.search(v)
        name = (m.group(1) if m else v.lstrip("@")).lower()
        if not KICK_SLUG_RE.match(name):
            raise ValueError("Invalid Kick username or channel URL")
        return name
    if platform == "youtube":
        m = YOUTUBE_URL_RE.search(v)
        if m:
            channel = m.group(2)
            if channel:
                return channel  # channel IDs are case-sensitive — store verbatim
            name = next((g for g in m.groups() if g), "")
            return name.lstrip("@").lower()
        if YOUTUBE_CHANNEL_RE.match(v):
            return v
        name = v.lstrip("@").lower()
        if not YOUTUBE_HANDLE_RE.match("@" + name):
            raise ValueError("Invalid YouTube channel link or @handle")
        return name
    # tiktok (default)
    m = TIKTOK_URL_RE.search(v)
    if m:
        v = m.group(1)
    v = v.lstrip("@").lower()
    if not TIKTOK_USERNAME_RE.match(v):
        raise ValueError("Invalid TikTok username or URL")
    return v


class SubscriptionCreate(BaseModel):
    platform: str = "tiktok"
    creator_input: str | None = None
    tiktok_input: str | None = None  # legacy alias (implies tiktok)
    handle: str = ""  # resolved by validator

    @field_validator("platform", mode="before")
    @classmethod
    def normalize_platform(cls, v) -> str:
        v = str(v or "tiktok").strip().lower()
        if v not in PLATFORMS:
            raise ValueError(f"Platform must be one of: {', '.join(PLATFORMS)}")
        return v

    @model_validator(mode="after")
    def resolve_handle(self):
        raw = (self.creator_input or "").strip() or (self.tiktok_input or "").strip()
        self.handle = normalize_handle(self.platform, raw)
        return self


HEX_COLOR_RE = re.compile(r"^#?[0-9a-fA-F]{6}$")


def _norm_color(v):
    if v is None or v == "":
        return None
    v = v.strip()
    if not HEX_COLOR_RE.match(v):
        raise ValueError("Color must be hex like #FF0050")
    return ("#" + v.lstrip("#")).upper()


class GlobalSettingsIn(BaseModel):
    webhook_url: str
    ping_role_id: str | None = None
    ping_everyone: bool = True
    custom_message: str = "{discord} is LIVE!"
    embed_image_url: str | None = None
    embed_color: str = "#FF0050"

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook(cls, v: str) -> str:
        v = v.strip()
        if not DISCORD_WEBHOOK_RE.match(v):
            raise ValueError("Invalid Discord webhook URL. Discord → Channel → Integrations → Webhooks → Copy URL")
        return v

    @field_validator("ping_role_id")
    @classmethod
    def validate_role(cls, v):
        if v is None or v == "":
            return None
        v = str(v).strip().replace("<@&", "").replace(">", "").replace("<@", "")
        if v.lower() == "everyone":
            return None
        if not v.isdigit():
            raise ValueError("Role ID must be numeric (Developer Mode → Copy ID)")
        return v

    @field_validator("embed_image_url")
    @classmethod
    def validate_image(cls, v):
        if v is None or v == "":
            return None
        v = v.strip()
        if not v.startswith("http"):
            raise ValueError("Embed image must be a public https:// URL (Discord fetches it)")
        return v

    @field_validator("embed_color")
    @classmethod
    def validate_color(cls, v):
        return _norm_color(v) or "#FF0050"


class SubscriptionStyleIn(BaseModel):
    """Per-creator embed customization. All optional, empty = global default.

    Link text always derives from author_name (Creator Name) and the message
    is always the global template — no per-creator overrides for either.
    """

    author_name: str | None = None
    discord_username: str | None = None
    discord_user_id: str | None = None
    image_url: str | None = None
    color: str | None = None

    @field_validator("author_name", "discord_username", "discord_user_id", "image_url")
    @classmethod
    def empty_to_none(cls, v):
        if v is None:
            return None
        v = v.strip()
        return v or None

    @field_validator("discord_username")
    @classmethod
    def normalize_discord(cls, v):
        if v is None or v == "":
            return None
        v = v.strip().lstrip("@")
        if not v or len(v) > 64:
            raise ValueError("Discord username: 1-64 chars (with or without @)")
        return v

    @field_validator("discord_user_id")
    @classmethod
    def normalize_discord_id(cls, v):
        if v is None or v == "":
            return None
        v = v.strip().replace("<@", "").replace(">", "")
        if not v.isdigit() or len(v) > 32:
            raise ValueError("Discord user ID must be numeric (Developer Mode → Copy User ID)")
        return v

    @field_validator("image_url")
    @classmethod
    def validate_image(cls, v):
        if v is None or v == "":
            return None
        v = v.strip()
        if not v.startswith("http"):
            raise ValueError("Image must be a public https:// URL (Discord fetches it)")
        return v

    @field_validator("color")
    @classmethod
    def validate_color(cls, v):
        return _norm_color(v)


class LoginIn(BaseModel):
    username: str
    password: str


class AdminCreate(BaseModel):
    username: str
    password: str
    role: str = "admin"

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: str) -> str:
        v = v.strip().lower()
        if not re.match(r"^[a-z0-9._]{3,32}$", v):
            raise ValueError("Username: 3-32 chars, letters/numbers/._")
        return v

    @field_validator("password")
    @classmethod
    def validate_password(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        return v

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        if v not in ("admin", "superadmin"):
            raise ValueError("Role must be admin or superadmin")
        return v


class PasswordChange(BaseModel):
    current_password: str | None = None
    new_password: str

    @field_validator("new_password")
    @classmethod
    def validate_new(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        return v
