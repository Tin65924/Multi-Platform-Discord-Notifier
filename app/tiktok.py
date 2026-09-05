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
    """One long-lived TikTokLiveClient per creator, reused across sweeps.

    Why reuse: building a client per check churns httpx pools, SSL
    contexts and signer objects every ~30s, and CPython never returns all
    of that RSS to the OS — the process ratcheted to Render's 512MB cap.
    Reuse keeps N bounded clients with warm, reused connections (also
    faster checks: no TLS handshake per sweep).
    Safety: each username is checked at most once per sweep and sweeps
    never overlap (cycle lock), so a per-user lock + the global semaphore
    rule out concurrent use of one client. Auth is applied once, at creation.
    """

    def __init__(self):
        self.semaphore = asyncio.Semaphore(2)
        self._clients: dict[str, TikTokLiveClient] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _client_for(self, clean: str) -> tuple[TikTokLiveClient, asyncio.Lock, bool]:
        client = self._clients.get(clean)
        created = client is None
        if created:
            client = TikTokLiveClient(unique_id=f"@{clean}")
            self._clients[clean] = client
        lock = self._locks.get(clean)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[clean] = lock
        return client, lock, created

    async def is_live(self, username: str) -> LiveInfo:
        clean = username.strip().lstrip("@").lower()
        client, lock, created = self._client_for(clean)
        async with self.semaphore:
            async with lock:
                try:
                    if created:
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


checker = TikTokChecker()
