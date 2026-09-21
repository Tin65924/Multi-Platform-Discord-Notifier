"""Auth routes — moved verbatim from app/api/routes.py (Phase 3)."""
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...db import get_session
from ...models import User
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


@router.post("/login")
async def login(payload: LoginIn, request: Request, session: AsyncSession = Depends(get_session)):
    result = await session.execute(
        select(User).where(User.username == payload.username.strip().lower())
    )
    user = result.scalar_one_or_none()
    if not user or not user.is_active or not verify_password(payload.password, user.password_hash):
        raise HTTPException(401, "Invalid username or password")
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
