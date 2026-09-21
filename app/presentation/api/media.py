"""Public creator-photo route — moved verbatim from app/api/routes.py (Phase 3)."""
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from ...infrastructure.persistence.database import get_session
from ...infrastructure.persistence.models import Subscription
from .common import logger

router = APIRouter()


@router.get("/media/creator/{sub_id}")
async def serve_photo(sub_id: int, session: AsyncSession = Depends(get_session)):
    """Public: Discord fetches embed images here. No auth by design."""
    sub = await session.get(Subscription, sub_id)
    if not sub:
        raise HTTPException(404, "No photo")
    # image_blob is deferred (list queries never load it) — async sessions
    # can't lazy-load, so refresh it explicitly.
    await session.refresh(sub, attribute_names=["image_blob", "image_mime"])
    if not sub.image_blob or not sub.image_mime:
        raise HTTPException(404, "No photo")
    return Response(
        content=bytes(sub.image_blob),
        media_type=sub.image_mime,
        headers={"Cache-Control": "public, max-age=86400"},
    )
