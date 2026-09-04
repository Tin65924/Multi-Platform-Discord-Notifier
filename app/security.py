import hashlib
import hmac
import secrets
from fastapi import Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .db import async_session

settings = get_settings()

SESSION_KEY = "uid"

# --- Password hashing (stdlib pbkdf2, no extra deps) ---
_ITERATIONS = 200_000


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), _ITERATIONS)
    return f"pbkdf2${_ITERATIONS}${salt}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _, iters, salt, hexdk = stored.split("$")
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), int(iters))
        return hmac.compare_digest(dk.hex(), hexdk)
    except Exception:
        return False


async def log_audit(actor: str, action: str, detail: str | None = None):
    """Fire-and-forget audit entry (own session so it never blocks callers)."""
    try:
        from .models import AuditLog

        async with async_session() as session:
            session.add(AuditLog(actor=actor, action=action, detail=(detail or "")[:1000]))
            await session.commit()
    except Exception:
        pass


async def get_current_user(request: Request):
    from .models import User

    uid = request.session.get(SESSION_KEY)
    if not uid:
        return None
    async with async_session() as session:
        user = await session.get(User, int(uid))
        if not user or not user.is_active:
            return None
        # detach values before session closes
        return {"id": user.id, "username": user.username, "role": user.role}


async def require_login(request: Request):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Login required")
    return user


async def require_admin(request: Request):
    user = await require_login(request)
    if user["role"] not in ("admin", "superadmin"):
        raise HTTPException(status_code=403, detail="Admin required")
    return user


async def require_superadmin(request: Request):
    user = await require_login(request)
    if user["role"] != "superadmin":
        raise HTTPException(status_code=403, detail="Superadmin required")
    return user


# Backwards-compat alias (old basic-auth dep name used across routes)
async def verify_basic_auth(request: Request):
    return await require_admin(request)


def verify_cron_secret(request: Request):
    """External cron (cron-job.org) authenticates with a shared secret.

    Accepts `X-Cron-Secret` header or `?secret=` query param, compared in
    constant time. A random CRON_SECRET is generated per process unless set,
    so unset-in-prod cron calls fail closed (403) instead of passing open.
    """
    cron = request.headers.get("x-cron-secret") or request.query_params.get("secret")
    if not hmac.compare_digest(cron or "", settings.CRON_SECRET):
        raise HTTPException(status_code=403, detail="Invalid cron secret")
    return True


async def require_cron_or_admin(request: Request):
    """Cron trigger auth: admin session OR valid cron secret.

    Scoped to poll triggers only — never use on data routes. The cron
    identity can start a sweep and nothing else.
    """
    user = await get_current_user(request)
    if user and user["role"] in ("admin", "superadmin"):
        return user
    verify_cron_secret(request)
    return {"id": 0, "username": "cron", "role": "cron"}
