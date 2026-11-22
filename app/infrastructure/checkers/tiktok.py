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
from TikTokLive.client.web.web_client import TikTokWebClient
from TikTokLive.client.errors import UserNotFoundError  # type: ignore

logger = logging.getLogger(__name__)

from ...config import get_settings as _get_settings

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
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=2),
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


async def close_profile_client():
    """Release the process-owned profile pool on application shutdown."""
    global _profile_client
    if _profile_client is not None:
        await _profile_client.aclose()
        _profile_client = None


async def _apply_auth(web):
    """Attach best-available cookies. Returns mode string for logging."""
    global _session_logged
    mode = "anonymous"
    try:
        from .cookies import get_cookies

        auto = await get_cookies()
        for k, v in auto.items():
            try:
                web.cookies.set(k, v, domain=".tiktok.com")
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
                web.set_session(sid, None)
                mode = "session"
            except Exception as e:
                logger.warning(f"tiktok auth: set_session failed err={type(e).__name__}")
    if not _session_logged:
        logger.info(f"tiktok auth: {mode} mode")
        _session_logged = True
    return mode


class TikTokChecker:
    """Fixed-size pool of HTTP-only clients, independent of creator count.

    A slot is exclusively leased for the whole status/room-id check. The
    account is passed to each route, so no per-account clients, locks, or
    websocket/event machinery need to survive a check. Acquire BEFORE
    allocating: concurrent manual checks cannot inflate the pool.
    """

    def __init__(self, pool_size: int = 2):
        if not 1 <= pool_size <= 4:
            raise ValueError("TikTok pool size must be between 1 and 4")
        self._clients: list[TikTokWebClient | None] = [None] * pool_size
        self._available: asyncio.Queue[int] = asyncio.Queue(maxsize=pool_size)
        for slot in range(pool_size):
            self._available.put_nowait(slot)
        self._close_lock = asyncio.Lock()
        self._closing = False

    def start(self) -> None:
        """Allow a new lifespan after the previous one was fully closed."""
        if self._closing:
            if any(self._clients) or self._available.qsize() != len(self._clients):
                raise RuntimeError("TikTok checker is still closing")
            self._available = asyncio.Queue(maxsize=len(self._clients))
            for slot in range(len(self._clients)):
                self._available.put_nowait(slot)
            self._close_lock = asyncio.Lock()
            self._closing = False

    def pool_status(self) -> dict:
        return {"capacity": len(self._clients),
                "allocated": sum(c is not None for c in self._clients),
                "in_use": len(self._clients) - self._available.qsize(),
                "closing": self._closing}

    def evict_missing(self, alive: set[str]) -> int:
        """Compatibility hook: the pool retains no per-creator state."""
        return 0

    def drop(self, clean: str) -> None:
        """No cached account/room data to invalidate; never close an active lease."""

    async def aclose(self) -> None:
        """Drain active leases and close every allocated client, once.

        Waiters recheck _closing after acquiring and return without issuing
        requests. No fire-and-forget close tasks or replacement-client churn.
        """
        async with self._close_lock:
            self._closing = True
            held = []
            try:
                for _ in self._clients:
                    held.append(await self._available.get())
                errors = []
                for slot, client in enumerate(self._clients):
                    if client is not None:
                        try:
                            await client.close()
                            self._clients[slot] = None
                        except Exception as exc:
                            errors.append(exc)
                if errors:
                    raise errors[0]
            finally:
                for slot in held:
                    self._available.put_nowait(slot)

    async def is_live(self, username: str) -> LiveInfo:
        clean = username.strip().lstrip("@").lower()
        if self._closing:
            return LiveInfo(is_live=False, username=clean, error="check_failed")
        slot = await self._available.get()
        try:
            if self._closing:
                return LiveInfo(is_live=False, username=clean, error="check_failed")
            client = self._clients[slot]
            if client is None:
                client = TikTokWebClient(httpx_kwargs={
                    "timeout": 12,
                    "limits": httpx.Limits(max_connections=1, max_keepalive_connections=1),
                })
                self._clients[slot] = client
            # Refresh auth in the exclusively leased slot. A cancelled first
            # check must not leave a permanently unauthenticated cached client.
            await _apply_auth(client)
            referer = f"https://www.tiktok.com/@{clean}/live"
            client.params.update(referer=referer, root_referer=referer)
            try:
                live = await asyncio.wait_for(client.fetch_is_live(unique_id=clean), timeout=12)
            except UserNotFoundError:
                logger.info(f"tiktok not_found user={clean}")
                return LiveInfo(is_live=False, username=clean, error="not_found")
            except Exception as e:
                logger.info(f"tiktok check failed user={clean} err={type(e).__name__}")
                return LiveInfo(is_live=False, username=clean, error="check_failed")
            if not live:
                return LiveInfo(is_live=False, username=clean)
            # A failed room-id lookup must preserve the confirmed LIVE result.
            room_id = f"live-{clean}"
            try:
                rid = await asyncio.wait_for(
                    client.fetch_room_id_from_api(unique_id=f"@{clean}"), timeout=10,
                )
                if rid:
                    room_id = str(rid)
            except Exception as e:
                logger.debug(f"room_id failed user={clean} err={type(e).__name__}")
            return LiveInfo(is_live=True, room_id=room_id, username=clean)
        except Exception as e:
            logger.warning(f"tiktok error user={clean} err={type(e).__name__}")
            return LiveInfo(is_live=False, username=clean, error="check_failed")
        finally:
            self._available.put_nowait(slot)


checker = TikTokChecker()


class TikTokLivePort:
    """domain LiveChecker over the shared warm-client checker.

    Error mapping is exact-legacy: "not_found" stays distinct, any other
    error string is inconclusive (never flips a card).
    """
    platform = "tiktok"
    fallback_room_prefix = "live-"

    async def check(self, handle: str):
        from ...domain.result import from_legacy
        info = await checker.is_live(handle)
        return from_legacy(info.is_live, getattr(info, "error", None),
                           handle, getattr(info, "room_id", None))

    def drop(self, handle: str) -> None:
        checker.drop(handle)


class TikTokProfilePort:
    """domain ProfileLookup over the shared profile-page fetch."""
    async def fetch(self, handle: str) -> dict | None:
        return await fetch_tiktok_profile(handle)
