"""Phase 0 harness: force local sqlite BEFORE any app import.

The repo's .env may point DATABASE_URL at Neon — tests must never touch it.
pydantic-settings prioritizes real env vars over .env, so these win.
"""
import os

os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_phase0.db"
os.environ["APP_SECRET_KEY"] = "phase0-test-secret"
os.environ.setdefault("TT_COOKIE_PROVIDER", "off")
