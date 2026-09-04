"""Kick live checks — official API, app token, one batched call per sweep.

Auth: OAuth 2.1 client_credentials against id.kick.com (no user login, no
scopes for public channel data). Token cached with margin, refreshed once on
401. Detection: GET api.kick.com/public/v1/channels?slug=... (up to 50 slugs
per call) and read stream.is_live. Slugs absent from the response count as
offline. No polling happens at all without KICK_CLIENT_ID/SECRET configured.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)

TOKEN_URL = "https://id.kick.com/oauth/token"
CHANNELS_URL = "https://api.kick.com/public/v1/channels"
SLUGS_PER_REQUEST = 50


@dataclass
class KickLiveInfo:
    is_live: bool
    room_id: str | None = None  # stream start_time (session id for dedup)
    username: str = ""
    error: str | None = None
    title: str = ""
    viewers: int = 0


@dataclass
class KickStatus:
    info: KickLiveInfo = field(default_factory=lambda: KickLiveInfo(is_live=False))
    user_id: int | None = None


class KickChecker:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(2)
        self._token = ""
        self._token_exp = 0.0

    def configured(self, client_id: str, client_secret: str) -> bool:
        return bool((client_id or "").strip() and (client_secret or "").strip())

    async def _app_token(self, client: httpx.AsyncClient, client_id: str, client_secret: str) -> str:
        if self._token and time.time() < self._token_exp - 60:
            return self._token
        r = await client.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id.strip(),
                "client_secret": client_secret.strip(),
            },
            timeout=15,
        )
        r.raise_for_status()
        body = r.json()
        token = (body.get("access_token") or "").strip()
        if not token:
            raise ValueError("kick token response had no access_token")
        self._token = token
        self._token_exp = time.time() + int(body.get("expires_in", 3600) or 3600)
        return self._token

    def _parse_channels(self, payload: dict) -> dict[str, KickStatus]:
        out: dict[str, KickStatus] = {}
        items = payload.get("data", []) if isinstance(payload, dict) else []
        for ch in items:
            if not isinstance(ch, dict):
                continue
            slug = str(ch.get("slug") or "").lower()
            if not slug:
                continue
            stream = ch.get("stream") or {}
            live = stream.get("is_live") is True
            start = stream.get("start_time")
            out[slug] = KickStatus(
                info=KickLiveInfo(
                    is_live=live,
                    room_id=str(start) if live and start else None,
                    username=slug,
                    title=str(ch.get("stream_title") or ""),
                    viewers=int(stream.get("viewer_count") or 0),
                ),
                user_id=ch.get("broadcaster_user_id"),
            )
        return out

    async def check_many(
        self, slugs: list[str], client_id: str, client_secret: str,
        client: httpx.AsyncClient | None = None,
    ) -> dict[str, KickStatus]:
        """Batch status for up to any number of slugs (chunked at 50). Missing = offline."""
        wanted = [s.strip().lower() for s in slugs if (s or "").strip()]
        out: dict[str, KickStatus] = {
            s: KickStatus(info=KickLiveInfo(is_live=False, username=s)) for s in wanted
        }
        if not wanted or not self.configured(client_id, client_secret):
            return out
        owned = client is not None
        client = client or httpx.AsyncClient(timeout=15)
        try:
            async with self.semaphore:
                for i in range(0, len(wanted), SLUGS_PER_REQUEST):
                    chunk = wanted[i:i + SLUGS_PER_REQUEST]
                    params = [("slug", s) for s in chunk]
                    try:
                        token = await self._app_token(client, client_id, client_secret)
                        r = await client.get(
                            CHANNELS_URL, params=params,
                            headers={"Authorization": f"Bearer {token}"},
                        )
                        if r.status_code == 401:
                            self._token = ""
                            token = await self._app_token(client, client_id, client_secret)
                            r = await client.get(
                                CHANNELS_URL, params=params,
                                headers={"Authorization": f"Bearer {token}"},
                            )
                        r.raise_for_status()
                        out.update(self._parse_channels(r.json()))
                    except Exception as e:
                        logger.debug(f"kick batch failed err={type(e).__name__}")
                        for s in chunk:
                            out[s] = KickStatus(
                                info=KickLiveInfo(is_live=False, username=s, error="fetch-failed")
                            )
        finally:
            if not owned:
                await client.aclose()
        return out

    async def is_live(self, slug: str, client_id: str = "", client_secret: str = "") -> KickLiveInfo:
        clean = (slug or "").strip().lower()
        if not self.configured(client_id, client_secret):
            return KickLiveInfo(is_live=False, username=clean, error="unconfigured")
        try:
            res = await self.check_many([clean], client_id, client_secret)
            return res[clean].info
        except Exception as e:
            logger.warning(f"kick error user={clean} err={type(e).__name__}")
            return KickLiveInfo(is_live=False, username=clean, error="error")


checker = KickChecker()
