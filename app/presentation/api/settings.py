"""Settings + broadcast + wildlines routes — moved verbatim from app/api/routes.py (Phase 3)."""
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from ...infrastructure.persistence.database import get_session
from ...infrastructure.persistence.models import GlobalSettings
from ...schemas import GlobalSettingsIn
from ...security import require_admin, require_superadmin, log_audit
from ...infrastructure.notify.discord import send_webhook
from ...infrastructure.notify.wildlines import WILD_LINES
from .common import _webhook_cfg, logger, settings

router = APIRouter()


@router.get("/settings")
async def get_settings_api(session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    gs = await session.get(GlobalSettings, 1)
    url, ping, msg, everyone, image, color = _webhook_cfg(gs)
    return {
        "webhook_configured": bool(url),
        "webhook_url_masked": ("***REDACTED***" + url[-12:] if url else None),
        "raw_webhook_url": url,
        "ping_role_id": ping,
        "ping_everyone": everyone,
        "custom_message": msg,
        "embed_image_url": image,
        "embed_color": color,
        "notifications_enabled": bool(gs.notifications_enabled) if gs and gs.notifications_enabled is not None else True,
        # Uploaded photos need a public base URL for Discord to fetch them.
        "public_base_set": bool((settings.PUBLIC_BASE_URL or "").strip()),
        # Which platforms the poller actually live-checks (drives dashboard hints).
        "live_checks": {
            "tiktok": True,
            "youtube": True,
            "kick": bool(settings.KICK_CLIENT_ID and settings.KICK_CLIENT_SECRET),
            "twitch": False,
        },
    }


@router.put("/settings")
async def put_settings(payload: GlobalSettingsIn, session: AsyncSession = Depends(get_session), user=Depends(require_admin)):
    gs = await session.get(GlobalSettings, 1)
    if not gs:
        gs = GlobalSettings(id=1, webhook_url=payload.webhook_url, ping_role_id=payload.ping_role_id,
                            ping_everyone=payload.ping_everyone, custom_message=payload.custom_message,
                            embed_image_url=payload.embed_image_url, embed_color=payload.embed_color,
                            notifications_enabled=payload.notifications_enabled)
        session.add(gs)
    else:
        gs.webhook_url = payload.webhook_url
        gs.ping_role_id = payload.ping_role_id
        gs.ping_everyone = payload.ping_everyone
        gs.custom_message = payload.custom_message
        gs.embed_image_url = payload.embed_image_url
        gs.embed_color = payload.embed_color
        gs.notifications_enabled = payload.notifications_enabled
    await session.commit()
    await log_audit(user["username"], "webhook.update", "global defaults saved")
    return {"msg": "Settings saved"}


@router.post("/send-custom")
async def send_custom(request: Request, session: AsyncSession = Depends(get_session), user=Depends(require_superadmin)):
    """Superadmin broadcast box: send free text to the channel as a test."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON body")
    content = str(data.get("content") or "").strip()
    if not content:
        raise HTTPException(400, "Message is empty")
    if len(content) > 2000:
        raise HTTPException(400, "Message too long (2000 chars max)")
    gs = await session.get(GlobalSettings, 1)
    webhook_url = _webhook_cfg(gs)[0]
    if not webhook_url:
        raise HTTPException(400, "Configure webhook first (Defaults tab)")
    payload = {
        "username": "Forest Watcher",
        "content": content,
        "allowed_mentions": {"parse": ["everyone", "roles", "users"]},
    }
    ok = await send_webhook(webhook_url, payload, timeout=settings.WEBHOOK_TIMEOUT_SECONDS)
    if not ok:
        raise HTTPException(502, "Webhook failed - check URL/permissions (see logs)")
    await log_audit(user["username"], "custom.send", content[:200])
    return {"msg": "Sent — check Discord"}


@router.get("/wildlines")
async def wild_lines(user=Depends(require_admin)):
    """Forest line templates (NAME placeholder) for the Defaults live preview."""
    return list(WILD_LINES)
