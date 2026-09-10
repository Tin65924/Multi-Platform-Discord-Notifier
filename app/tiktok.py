"""TikTok status checks — TikTokLive lib, only what the notifier needs.

We need exactly two facts per creator:
  1. is_live: bool (via TikTokLiveClient.is_live)
  2. room_id: stable id of the current live session, for dedup
     (via web.fetch_room_id_from_api, only called when live)

Plus, rarely: profile identity (canonical name, numeric id, avatar) for
rename-follow and auto avatars — one shared page fetch, best-effort.
"""
import asyncio
import json
import logging
import re
from dataclasses import dataclass
from typing import Optional

import httpx
from TikTokLive import TikTokLiveClient  # type: ignore
from TikTokLive.client.errors import UserNotFoundError  # type: ignore

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
    error: Optional[str] = None  # "not_found" when the handle looks renamed/deleted


_PROFILE_RE = re.compile(
    r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">(.*?)</script>',
    re.S,
)
_profile_client: httpx.AsyncClient | None = None


def _profile_http() -> httpx.AsyncClient:
    """One shared client for rare profile-page fetches (identity + avatar)."""
    global _profile_client
    if _profile_client is None:
        _profile_client = httpx.AsyncClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            },
            follow_redirects=True,
            timeout=15,
        )
    return _profile_client


async def fetch_tiktok_profile(handle: str) -> dict | None:
    """Profile page -> {unique_id, user_id, avatar_url}. None on block/fail.

    Never raises. One request serves rename-checks, id anchoring and avatars.
    """
    try:
        r = await _profile_http().get(f"https://www.tiktok.com/@{handle.strip().lstrip('@').lower()}")
        m = _PROFILE_RE.search(r.text)
        if not m:
            return None
        user = (
            json.loads(m.group(1))
            .get("__DEFAULT_SCOPE__", {})
            .get("webapp.user-detail", {})
            .get("userInfo", {})
            .get("user", {})
        )
        uid = str(user.get("uniqueId") or "").strip().lower()
        if not uid:
            return None
        nid = str(user.get("id") or "").strip() or None
        av = str(user.get("avatarLarger") or "").strip() or None
        if av and not av.startswith("http"):
            av = None
        return {"unique_id": uid, "user_id": nid, "avatar_url": av}
    except Exception:
        return None


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
                    except UserNotFoundError:
                        # Dead handle (renamed/deleted) — distinct from offline
                        # so the poller can track and surface it.
                        return LiveInfo(is_live=False, username=clean, error="not_found")
                    except Exception:
                        return LiveInfo(is_live=False, username=clean)
                    if not live:
                        return LiveInfo(is_live=False, username=clean)
                    # Live — resolve the session id for dedup (best effort).
                    # Stable fallback keeps dedup working even if this fails.
                    # NOTE: any failure here keeps the LIVE result — only the
                    # primary check above may report not_found.
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
