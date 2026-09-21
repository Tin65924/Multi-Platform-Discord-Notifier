import hashlib
import hmac
import secrets
from fastapi import HTTPException, Request

from .infrastructure.persistence.database import async_session

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
        from .infrastructure.persistence.models import AuditLog

        async with async_session() as session:
            session.add(AuditLog(actor=actor, action=action, detail=(detail or "")[:1000]))
            await session.commit()
    except Exception:
        pass


async def get_current_user(request: Request):
    from .infrastructure.persistence.models import User
    from .infrastructure.persistence.database import open_session

    uid = request.session.get(SESSION_KEY)
    if not uid:
        return None
    session = await open_session()
    try:
        user = await session.get(User, int(uid))
        if not user or not user.is_active:
            return None
        # detach values before session closes
        return {"id": user.id, "username": user.username, "role": user.role}
    finally:
        await session.close()


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



