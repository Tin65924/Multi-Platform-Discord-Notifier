"""Auth routes (Phase 5: login rate-limit)."""
import time

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...infrastructure.persistence.database import get_session
from ...infrastructure.persistence.models import User
from ...schemas import LoginIn, PasswordChange
from ...security import (
    require_admin,
    verify_password,
    hash_password,
    log_audit,
    SESSION_KEY,
    get_current_user,
)
from .common import logger

router = APIRouter()

# Brute-force guard: pbkdf2 at 200k iterations makes every attempt CPU-heavy,
# so attempts per IP are capped (10 per 5 min window -> 429). Behind Render's
# proxy the real client IP rides X-Forwarded-For.
_login_attempts: dict[str, list] = {}  # ip -> [count, window_start_epoch]
LOGIN_LIMIT = 10
LOGIN_WINDOW_SECONDS = 300
_LOGIN_MAP_CAP = 5000


def _client_ip(request: Request) -> str:
    try:
        fwd = (request.headers.get("x-forwarded-for") or "").strip()
        if fwd:
            return fwd.split(",")[0].strip() or "unknown"
        return request.client.host if request.client else "unknown"
    except Exception:
        return "unknown"


def _login_allowed(ip: str, *, now: float | None = None,
                   limit: int = LOGIN_LIMIT, window: int = LOGIN_WINDOW_SECONDS) -> bool:
    """Count-first gate: every call consumes one attempt. Testable via `now`."""
    now = time.time() if now is None else now
    ent = _login_attempts.get(ip)
    if ent is None or now - ent[1] >= window:
        if len(_login_attempts) >= _LOGIN_MAP_CAP:
            # Evict oldest windows first (dicts preserve insertion order).
            for k in list(_login_attempts)[:1000]:
                del _login_attempts[k]
        _login_attempts[ip] = [1, now]
        return True
    ent[0] += 1
    return ent[0] <= limit


@router.post("/login")
async def login(payload: LoginIn, request: Request, session: AsyncSession = Depends(get_session)):
    ip = _client_ip(request)
    if not _login_allowed(ip):
        raise HTTPException(429, "Too many login attempts — try again in a few minutes")
    result = await session.execute(
        select(User).where(User.username == payload.username.strip().lower())
    )
    user = result.scalar_one_or_none()
    if not user or not user.is_active or not verify_password(payload.password, user.password_hash):
        raise HTTPException(401, "Invalid username or password")
    _login_attempts.pop(ip, None)  # success resets the window
    request.session[SESSION_KEY] = user.id
    await log_audit(user.username, "login", "dashboard login")
    return {"msg": "Logged in", "username": user.username, "role": user.role}


@router.post("/logout")
async def logout(request: Request):
    user = await get_current_user(request)
    request.session.clear()
    if user:
        await log_audit(user["username"], "logout", "dashboard logout")
    return {"msg": "Logged out"}


@router.get("/me")
async def me(request: Request):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(401, "Login required")
    return user


@router.post("/me/password")
async def change_own_password(
    payload: PasswordChange, request: Request, session: AsyncSession = Depends(get_session),
    user=Depends(require_admin),
):
    db_user = await session.get(User, user["id"])
    if not db_user:
        raise HTTPException(404, "Not found")
    if user["role"] != "superadmin":
        if not payload.current_password or not verify_password(payload.current_password, db_user.password_hash):
            raise HTTPException(400, "Current password is incorrect")
    db_user.password_hash = hash_password(payload.new_password)
    await session.commit()
    await log_audit(user["username"], "password.change", f"password changed for {db_user.username}")
    return {"msg": "Password updated"}
