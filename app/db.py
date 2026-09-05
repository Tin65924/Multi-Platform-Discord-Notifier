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
    return u


# Query keys asyncpg.connect() actually accepts. Everything else in a pasted
# URL (sslmode, channel_binding, ...) would be forwarded by SQLAlchemy as a
# kwarg and crash with TypeError — so we translate or drop them here.
_ASYNCPG_QUERY_ALLOWLIST = frozenset({
    "database", "user", "password", "host", "port", "timeout",
    "command_timeout", "statement_cache_size", "max_cacheable_statement_size",
    "max_cached_statement_lifetime", "target_session_attrs", "server_settings",
    "direct_tls",
})


def _sanitize_query(url: str) -> tuple[str, str | None]:
    """Split a DB URL into (clean_url, ssl_mode).

    - Translates ?sslmode=... / ?ssl=... into an ssl mode (applied later as
      a real SSLContext via connect_args).
    - Drops anything asyncpg doesn't understand (e.g. channel_binding) with
      a log line instead of crashing at connect time.
    """
    from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

    parts = urlsplit(url)
    if not parts.netloc:
        # SQLite-style URLs have no authority section — urlunsplit would
        # collapse their leading slashes into an unparseable URL. Query
        # sanitizing only applies to server DBs (asyncpg) anyway.
        return url, None
    q = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)]
    mode = None
    kept = []
    dropped = []
    for k, v in q:
        if k == "sslmode" and mode is None:
            mode = v
        elif k == "ssl":
            mode = mode or ("require" if v not in ("0", "false", "disable") else "disable")
        elif k in _ASYNCPG_QUERY_ALLOWLIST:
            kept.append((k, v))
        else:
            dropped.append(k)
    if dropped:
        logger.warning(f"db url: ignoring unsupported query params {sorted(set(dropped))}")
    clean = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(kept), parts.fragment))
    return clean, mode


DB_URL = normalize_db_url(settings.DATABASE_URL)

# Render free 512MB + Neon free (max ~20 connections): keep the pool tiny.
# NullPool for serverless Neon (avoids holding idle conns across Render sleep).
_is_pg = DB_URL.startswith("postgresql")
_is_neon = "neon.tech" in DB_URL
DB_URL, _ssl_mode = _sanitize_query(DB_URL)
if _ssl_mode is None and _is_neon:
    _ssl_mode = "require"  # Neon refuses unencrypted connections
_engine_kw: dict = {"echo": False, "future": True}
if _is_neon:
    _engine_kw["poolclass"] = NullPool
elif _is_pg:
    _engine_kw.update(pool_size=5, max_overflow=0)
if _is_pg and _ssl_mode and _ssl_mode != "disable":
    import ssl as _ssl

    _engine_kw["connect_args"] = {"ssl": _ssl.create_default_context()}

logger.info(f"db dialect: {DB_URL.split(':')[0] if ':' in DB_URL else 'unknown'} "
            f"pool={'null' if _is_neon else 'sized'} ssl={_ssl_mode or 'off'}")

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
    ("discord_username", "VARCHAR(64)"),
    ("message", "TEXT"),
    ("link_text", "VARCHAR(128)"),
    ("image_url", "TEXT"),
    ("discord_user_id", "VARCHAR(32)"),
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
    # Retired tracking column (never read, writes removed) — drop if present.
    stmts.append("ALTER TABLE subscriptions DROP COLUMN IF EXISTS consecutive_failures")
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
    # Retired tracking column (never read, writes removed) — drop if present.
    try:
        names = {
            c[1]
            for c in (await conn.execute(text("PRAGMA table_info(subscriptions)"))).fetchall()
        }
        if "consecutive_failures" in names:
            await conn.execute(text("ALTER TABLE subscriptions DROP COLUMN consecutive_failures"))
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

    # One-time normalization: stock message templates from earlier formats
    # move to the current one ("{discord} is LIVE!"). Only rows still on an
    # untouched stock template move — customized messages are left alone.
    async with async_session() as session:
        from sqlalchemy import select
        from .models import GlobalSettings, User

        try:
            gs = await session.get(GlobalSettings, 1)
            if gs and gs.custom_message in (
                "{ping_role}\n**{discord}** is LIVE!!!",
                "{ping_role}\n{discord} is LIVE!!!",
            ):
                gs.custom_message = "{discord} is LIVE!"
                await session.commit()
                logger.info("db normalized stock custom_message to forest version")
        except Exception as e:
            logger.warning(f"custom_message normalize skipped err={type(e).__name__}")

    # Retention: audit_log is append-only — keep the newest 5000 rows.
    # Runs every boot (cheap, indexed); dialect-safe for PG + SQLite.
    try:
        async with async_session() as session:
            await session.execute(
                text(
                    "DELETE FROM audit_log WHERE id NOT IN "
                    "(SELECT id FROM audit_log ORDER BY id DESC LIMIT 5000)"
                )
            )
            await session.commit()
    except Exception as e:
        logger.warning(f"audit prune skipped err={type(e).__name__}")

    # Retention: closed live sessions older than 180 days go; open rows
    # (currently live) are kept regardless of age.
    try:
        from datetime import datetime, timedelta, timezone

        from .models import LiveSession

        cutoff = datetime.now(timezone.utc) - timedelta(days=180)
        async with async_session() as session:
            await session.execute(
                text(
                    "DELETE FROM live_sessions "
                    "WHERE ended_at IS NOT NULL AND ended_at < :cutoff"
                ),
                {"cutoff": cutoff},
            )
            await session.commit()
    except Exception as e:
        logger.warning(f"sessions prune skipped err={type(e).__name__}")

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
