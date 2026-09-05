"""TikTok status checks — TikTokLive lib, only what the notifier needs.

We need exactly two facts per creator:
  1. is_live: bool (via TikTokLiveClient.is_live)
  2. room_id: stable id of the current live session, for dedup
     (via web.fetch_room_id_from_api, only called when live)

No profile fetching, no titles, no covers — all visuals are owned by us
(per-creator image / link text / color in the dashboard).
"""
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

from TikTokLive import TikTokLiveClient  # type: ignore

logger = logging.getLogger(__name__)

from .config import get_settings as _get_settings

try:
    _settings = _get_settings()
except Exception:
    _settings = None

_session_logged = False


@dataclass
class LiveInfo:
    is_live: bool
    room_id: Optional[str] = None
    username: str = ""


async def _apply_auth(client):
    """Attach best-available cookies. Returns mode string for logging."""
    global _session_logged
    mode = "anonymous"
    try:
        from .cookie_provider import get_cookies

        auto = await get_cookies()
        for k, v in auto.items():
            try:
                client.web.cookies.set(k, v)
            except Exception:
                pass
        if auto:
            mode = f"auto-cookies ({','.join(sorted(auto.keys()))})"
    except Exception as e:
        logger.debug(f"cookie provider err={type(e).__name__}")
    if mode == "anonymous":
        sid = (getattr(_settings, "TIKTOK_SESSION_ID", "") or "").strip()
        if sid:
            try:
                client.web.set_session_id(sid)
                mode = "session"
            except Exception as e:
                logger.warning(f"tiktok auth: set_session_id failed err={type(e).__name__}")
    if not _session_logged:
        logger.info(f"tiktok auth: {mode} mode")
        _session_logged = True
    return mode


class TikTokChecker:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(2)

    async def is_live(self, username: str) -> LiveInfo:
        clean = username.strip().lstrip("@").lower()
        async with self.semaphore:
            # NOTE: the client MUST be closed (finally below). Each check
            # builds a fresh TikTokLiveClient holding an httpx session; never
            # closing them leaked connections until the host OOM'd (Render 512MB).
            client = None
            try:
                client = TikTokLiveClient(unique_id=f"@{clean}")
                await _apply_auth(client)
                try:
                    live = await asyncio.wait_for(client.is_live(), timeout=12)
                except Exception:
                    return LiveInfo(is_live=False, username=clean)
                if not live:
                    return LiveInfo(is_live=False, username=clean)
                # Live — resolve the session id for dedup (best effort).
                # Stable fallback keeps dedup working even if this fails.
                room_id = f"live-{clean}"
                try:
                    rid = await asyncio.wait_for(
                        client.web.fetch_room_id_from_api(unique_id=f"@{clean}"),
                        timeout=10,
                    )
                    if rid:
                        room_id = str(rid)
                except Exception as e:
                    logger.debug(f"room_id failed user={clean} err={type(e).__name__}")
                return LiveInfo(is_live=True, room_id=room_id, username=clean)
            except asyncio.TimeoutError:
                return LiveInfo(is_live=False, username=clean)
            except Exception as e:
                logger.warning(f"tiktok error user={clean} err={type(e).__name__}")
                return LiveInfo(is_live=False, username=clean)
            finally:
                if client is not None:
                    try:
                        await client.close()
                    except Exception:
                        pass


checker = TikTokChecker()
