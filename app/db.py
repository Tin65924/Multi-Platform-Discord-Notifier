from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool
import logging
from .config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Accept any pasted Postgres URL form and adapt it to our async stack:
#   postgres://... | postgresql://... | postgresql+psycopg2://...
#     -> postgresql+asyncpg://...  (asyncpg is our async driver; psycopg2
#        is sync and is NOT installed, so sync-driver URLs crash at import)
# SSL: asyncpg understands ?sslmode=require natively (Neon's default form),
# so we only append it when talking to Neon without an explicit sslmode.
def normalize_db_url(url: str) -> str:
    u = (url or "").strip()
    if u.startswith("postgres://"):
        u = "postgresql://" + u[len("postgres://"):]
    if u.startswith("postgresql://"):
        u = "postgresql+asyncpg://" + u[len("postgresql://"):]
    elif u.startswith("postgresql+psycopg2://"):
        u = "postgresql+asyncpg://" + u[len("postgresql+psycopg2://"):]
    elif u.startswith("postgresql+psyc://"):
        u = "postgresql+asyncpg://" + u[len("postgresql+psyc://"):]
    u = u.replace("ssl=require", "sslmode=require")
    return u


DB_URL = normalize_db_url(settings.DATABASE_URL)

# Render free 512MB + Neon free (max ~20 connections): keep the pool tiny.
# NullPool for serverless Neon (avoids holding idle conns across Render sleep).
_is_pg = DB_URL.startswith("postgresql")
_is_neon = "neon.tech" in DB_URL
if _is_neon and "sslmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "sslmode=require"
_engine_kw: dict = {"echo": False, "future": True}
if _is_neon:
    _engine_kw["poolclass"] = NullPool
elif _is_pg:
    _engine_kw.update(pool_size=5, max_overflow=0)

logger.info(f"db dialect: {DB_URL.split(':')[0] if ':' in DB_URL else 'unknown'} "
            f"pool={'null' if _is_neon else 'sized'}")

engine = create_async_engine(DB_URL, **_engine_kw)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_session():
    async with async_session() as session:
        yield session


# Desired-state columns for pre-existing DBs (create_all only covers fresh DBs).
_SUBSCRIPTION_COLS = (
    ("display_name", "VARCHAR(128)"),
    ("avatar_url", "TEXT"),
    ("cover_url", "TEXT"),
    ("author_name", "VARCHAR(128)"),
    ("message", "TEXT"),
    ("link_text", "VARCHAR(128)"),
    ("image_url", "TEXT"),
    ("color", "VARCHAR(7)"),
    ("platform", "VARCHAR(16) DEFAULT 'tiktok'"),
)
_GLOBAL_SETTINGS_COLS_SQLITE = (
    ("ping_everyone", "BOOLEAN DEFAULT 1"),
    ("embed_image_url", "TEXT"),
    ("embed_color", "VARCHAR(7) DEFAULT '#FF0050'"),
)
_GLOBAL_SETTINGS_COLS_PG = (
    ("ping_everyone", "BOOLEAN DEFAULT true"),
    ("embed_image_url", "TEXT"),
    ("embed_color", "VARCHAR(7) DEFAULT '#FF0050'"),
)


def postgres_migration_statements() -> list:
    """Idempotent DDL for existing Postgres DBs (IF NOT EXISTS everywhere)."""
    stmts = []
    for col, typ in _SUBSCRIPTION_COLS:
        stmts.append(f"ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS {col} {typ}")
    for col, typ in _GLOBAL_SETTINGS_COLS_PG:
        stmts.append(f"ALTER TABLE global_settings ADD COLUMN IF NOT EXISTS {col} {typ}")
    stmts.append("UPDATE subscriptions SET platform='tiktok' WHERE platform IS NULL")
    # Uniqueness moves from (handle) to (platform, handle): same creator name
    # may be tracked on several platforms, but never twice on one platform.
    stmts.append("DROP INDEX IF EXISTS ix_subscriptions_tiktok_username")
    stmts.append(
        "CREATE INDEX IF NOT EXISTS ix_subscriptions_platform_username "
        "ON subscriptions (platform, tiktok_username)"
    )
    stmts.append(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_subscriptions_platform_username "
        "ON subscriptions (platform, tiktok_username)"
    )
    return stmts


async def _migrate_sqlite(conn):
    """Probe-and-add for pre-existing SQLite files (PRAGMA-based, as before)."""
    for table, cols in (
        ("subscriptions", _SUBSCRIPTION_COLS),
        ("global_settings", _GLOBAL_SETTINGS_COLS_SQLITE),
    ):
        try:
            existing = (await conn.execute(text(f"PRAGMA table_info({table})"))).fetchall()
            names = {c[1] for c in existing}
        except Exception:
            continue
        for col, typ in cols:
            if col not in names:
                try:
                    await conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {typ}"))
                except Exception:
                    pass
    # --- platform support: one row per platform account ------------------
    try:
        await conn.execute(text("UPDATE subscriptions SET platform='tiktok' WHERE platform IS NULL"))
        idx = {
            r[0]
            for r in (await conn.execute(text("SELECT name FROM sqlite_master WHERE tbl_name='subscriptions' AND type='index'"))).fetchall()
        }
        if "ix_subscriptions_tiktok_username" in idx:
            await conn.execute(text("DROP INDEX ix_subscriptions_tiktok_username"))
            idx.discard("ix_subscriptions_tiktok_username")
        if "ix_subscriptions_platform_username" not in idx:
            await conn.execute(text("CREATE INDEX ix_subscriptions_platform_username ON subscriptions (platform, tiktok_username)"))
        if "uq_subscriptions_platform_username" not in idx:
            await conn.execute(text("CREATE UNIQUE INDEX uq_subscriptions_platform_username ON subscriptions (platform, tiktok_username)"))
    except Exception:
        pass


async def init_db():
    from . import models  # noqa: F401 - register tables
    from .security import hash_password

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        if engine.dialect.name == "postgresql":
            for stmt in postgres_migration_statements():
                try:
                    await conn.execute(text(stmt))
                except Exception as e:
                    logger.warning(f"pg migrate skipped: {stmt[:60]} err={type(e).__name__}")
        else:
            await _migrate_sqlite(conn)
    logger.info("db initialized")

    # Bootstrap superadmin on first run
    async with async_session() as session:
        from sqlalchemy import select
        from .models import User

        count = await session.scalar(select(User).limit(1).with_only_columns(User.id))
        if count is None:
            session.add(
                User(
                    username=settings.SUPERADMIN_USER,
                    password_hash=hash_password(settings.SUPERADMIN_PASS),
                    role="superadmin",
                    is_active=True,
                )
            )
            await session.commit()
