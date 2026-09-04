import os
import secrets
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # DB - Neon Postgres in prod requires ?ssl=require (see render.yaml)
    DATABASE_URL: str = "sqlite+aiosqlite:///./playtopia.db"
    APP_SECRET_KEY: str = secrets.token_urlsafe(32)
    # External cron (cron-job.org) authenticates with this on /api/cron/poll.
    # Random per process unless set — MUST be set in prod or cron calls 403.
    CRON_SECRET: str = secrets.token_urlsafe(32)
    # Legacy single-user basic auth (unused now that session login exists, kept for compat)
    DASHBOARD_USER: str = "admin"
    DASHBOARD_PASS: str = ""  # leave empty for local no-auth
    # Bootstrap superadmin (created on first run if no users exist)
    SUPERADMIN_USER: str = "superadmin"
    SUPERADMIN_PASS: str = "changeme123"
    DASHBOARD_WEBHOOK_URL: str = ""  # paste Discord webhook here
    # Cloudflare Worker relay (bypasses flagged host IPs). When both are set,
    # posts go to RELAY_URL as {secret, payload} instead of Discord directly.
    RELAY_URL: str = ""
    RELAY_SECRET: str = ""
    PING_ROLE_ID: str = ""
    CUSTOM_MESSAGE: str = "{ping_role}\n{account} is LIVE!"
    # Hardcoded embed image (public https URL) — can also be set in dashboard
    EMBED_IMAGE_URL: str = ""
    # Optional TikTok login session (browser cookie "sessionid" from tiktok.com).
    # Logged-in calls are flagged far less and can read age-restricted lives.
    # Treat like a password: .env only, never commit, never share.
    TIKTOK_SESSION_ID: str = ""
    # Automatic cookie minting via headless browser (hourly). Needs Playwright
    # browsers installed — default OFF here (Render has none); "on" for local.
    TT_COOKIE_PROVIDER: str = "off"
    # Platform live-check credentials (server-side only, never commit/share).
    # Twitch + Kick: app client ID + secret (client-credentials flow, no user OAuth).
    # YouTube: Data API v3 key, used only to confirm candidate streams (never search.list).
    TWITCH_CLIENT_ID: str = ""
    TWITCH_CLIENT_SECRET: str = ""
    KICK_CLIENT_ID: str = ""
    KICK_CLIENT_SECRET: str = ""
    YOUTUBE_API_KEY: str = ""
    TT_COOKIE_RETRY_SECONDS: int = 1800  # backoff after a failed mint
    # Base URL of this app if publicly reachable + app/static/cover.jpg exists
    PUBLIC_BASE_URL: str = ""
    MAX_CREATORS: int = 60  # Render Free comfort cap across all platforms
    CHECK_INTERVAL_SECONDS: int = 45  # gap between sweeps (prod: 3-platform sweep)
    CHECK_JITTER_SECONDS: int = 10
    PER_CHECK_SLEEP_SECONDS: float = 1.0
    WEBHOOK_TIMEOUT_SECONDS: int = 10
    LOG_LEVEL: str = "INFO"
    PORT: int = 8000

    def is_prod(self) -> bool:
        return os.getenv("RENDER") == "true" or os.getenv("ENV") == "production"

@lru_cache
def get_settings() -> Settings:
    return Settings()