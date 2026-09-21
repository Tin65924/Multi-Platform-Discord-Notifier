"""Admin + audit routes (superadmin only) — moved verbatim from app/api/routes.py (Phase 3)."""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ...db import get_session
from ...models import User, AuditLog
from ...schemas import AdminCreate
from ...security import (
    require_superadmin,
    hash_password,
    log_audit,
)
from .common import logger

router = APIRouter()


@router.get("/admins")
async def list_admins(session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    result = await session.execute(select(User).order_by(User.username))
    return [
        {"id": u.id, "username": u.username, "role": u.role, "is_active": u.is_active,
         "created_at": u.created_at.isoformat() if u.created_at else None}
        for u in result.scalars().all()
    ]


@router.post("/admins", status_code=201)
async def create_admin(payload: AdminCreate, session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    existing = await session.execute(select(User).where(User.username == payload.username))
    if existing.scalar_one_or_none():
        raise HTTPException(409, "Username already exists")
    new_user = User(username=payload.username, password_hash=hash_password(payload.password),
                    role=payload.role, is_active=True)
    session.add(new_user)
    await session.commit()
    await log_audit(user["username"], "admin.create", f"created {payload.role} {payload.username}")
    return {"msg": f"Created {payload.role} {payload.username}"}


@router.post("/admins/{uid}/toggle")
async def toggle_admin(uid: int, session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    target = await session.get(User, uid)
    if not target:
        raise HTTPException(404, "Not found")
    if target.id == user["id"]:
        raise HTTPException(400, "You cannot disable yourself")
    target.is_active = not target.is_active
    await session.commit()
    await log_audit(user["username"], "admin.toggle", f"{'enabled' if target.is_active else 'disabled'} {target.username}")
    return {"msg": f"{'Enabled' if target.is_active else 'Disabled'} {target.username}"}


@router.delete("/admins/{uid}")
async def delete_admin(uid: int, session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    target = await session.get(User, uid)
    if not target:
        raise HTTPException(404, "Not found")
    if target.id == user["id"]:
        raise HTTPException(400, "You cannot delete yourself")
    if target.role == "superadmin":
        sup = await session.scalar(select(func.count()).select_from(User).where(User.role == "superadmin"))
        if (sup or 0) <= 1:
            raise HTTPException(400, "Cannot delete the last superadmin")
    await session.delete(target)
    await session.commit()
    await log_audit(user["username"], "admin.delete", f"deleted {target.username}")
    return {"msg": f"Deleted {target.username}"}


@router.get("/audit")
async def list_audit(limit: int = 100, session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    limit = max(1, min(limit, 500))
    result = await session.execute(select(AuditLog).order_by(AuditLog.id.desc()).limit(limit))
    return [
        {"id": a.id, "actor": a.actor, "action": a.action, "detail": a.detail,
         "created_at": a.created_at.isoformat() if a.created_at else None}
        for a in result.scalars().all()
    ]
