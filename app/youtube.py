"""YouTube live checks — keyless page parse + optional API confirm.

Detection is keyless (no OAuth, no key): fetch the channel's /live page and
look for YouTube's own live markers. A configured YOUTUBE_API_KEY is then used
to *confirm* a candidate videoId via videos.list (1 quota unit per 50 ids) —
this resolves Premiere/upcoming ambiguity and covers page-markup drift.

Conservative by design: ambiguous pages report "unknown" and never flip a
card to LIVE. Only an explicit live marker (+ optional API confirm) notifies.
"""
import asyncio
import logging
import re
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
CONSENT_COOKIES = {"CONSENT": "YES+cb.20230621-17-p0.en+FX+410", "SOCS": "CAI"}

# videoId closely followed by an explicit LIVE broadcast flag.
_LIVE_RE = re.compile(r'"videoId":"([A-Za-z0-9_-]{11})".{0,2000}?"liveBroadcastContent":"LIVE"')
# The player component YouTube itself embeds only when the resolved /live page
# IS a live broadcast. NOTE: bare "style":"LIVE" badges must NOT be used on
# /live pages — shelves/end-screens carry them for *other* channels' streams.
_LIVE_RENDERER_RE = re.compile(r'"liveStreamabilityRenderer":\{"videoId":"([A-Za-z0-9_-]{11})"')
# Own-video grid item on a /streams (live tab) page: the first videoId before a
# LIVE overlay with no other videoId in between belongs to that overlay.
# Browse grids contain only the channel's own videos, so this is safe there.
_STREAMS_LIVE_RE = re.compile(
    r'"videoId":"([A-Za-z0-9_-]{11})"(?:(?!"videoId").){0,4000}?"thumbnailOverlayTimeStatusRenderer"'
)
_OFFLINE_PHRASES = (
    "The channel is not currently live",
    '"liveBroadcastContent":"NONE"',
)
_CHANNEL_ID_RE = re.compile(r"^UC[A-Za-z0-9_-]{22}$")


@dataclass
class YTLiveInfo:
    is_live: bool
    room_id: str | None = None  # YouTube videoId of the live session (for dedup)
    username: str = ""
    error: str | None = None  # set when the check itself failed (blocked/ambiguous)


def profile_base(handle: str) -> str:
    """Channel URL prefix for a stored handle (@handle or raw UC channel ID)."""
    h = (handle or "").strip()
    if _CHANNEL_ID_RE.match(h):
        return f"https://www.youtube.com/channel/{h}"
    if not h.startswith("@"):
        h = "@" + h
    return f"https://www.youtube.com/{h}"


def parse_live_page(html: str, kind: str = "live") -> tuple[bool | None, str | None, str]:
    """Return (is_live | None=unknown, video_id | None, reason). Never raises.

    kind="live": the channel's /live URL (resolves to a watch page when live).
    kind="streams": the channel's /streams tab (own-video grid, no shelves).
    """
    if not html or "ytInitialData" not in html or "consent.youtube.com" in html:
        return None, None, "blocked"
    m = _LIVE_RENDERER_RE.search(html)
    if m:
        return True, m.group(1), "live-renderer"
    m = _LIVE_RE.search(html)
    if m:
        return True, m.group(1), "live-marker"
    if kind == "streams":
        m = _STREAMS_LIVE_RE.search(html)
        if m:
            return True, m.group(1), "streams-grid"
    for phrase in _OFFLINE_PHRASES:
        if phrase in html:
            return False, None, "offline-marker"
    # Complete page, no live player of any kind — definitively not live.
    return False, None, "no-player"


async def confirm_with_api(client: httpx.AsyncClient, api_key: str, video_id: str) -> bool | None:
    """Official ground truth for a candidate video. True/False, or None on API failure."""
    try:
        r = await client.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params={"part": "snippet,liveStreamingDetails", "id": video_id, "key": api_key},
            timeout=15,
        )
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items:
            return False
        item = items[0]
        lbc = (item.get("snippet") or {}).get("liveBroadcastContent", "none")
        lsd = item.get("liveStreamingDetails") or {}
        live = lbc == "live" and "actualStartTime" in lsd and "actualEndTime" not in lsd
        return live
    except Exception as e:
        logger.debug(f"youtube api confirm failed err={type(e).__name__}")
        return None


class YouTubeChecker:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(2)

    async def is_live(self, handle: str, api_key: str = "") -> YTLiveInfo:
        raw = (handle or "").strip()
        clean = raw if _CHANNEL_ID_RE.match(raw) else raw.lstrip("@").lower()
        async with self.semaphore:
            try:
                async with httpx.AsyncClient(
                    headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"},
                    cookies=CONSENT_COOKIES,
                    follow_redirects=True,
                    timeout=15,
                ) as client:
                    base = profile_base(clean)
                    blocked = False
                    for path, kind in (("/live", "live"), ("/streams", "streams")):
                        try:
                            r = await client.get(base + path)
                        except Exception as e:
                            logger.debug(f"youtube fetch failed user={clean} page={kind} err={type(e).__name__}")
                            continue
                        live, video_id, reason = parse_live_page(r.text, kind)
                        if reason == "blocked":
                            blocked = True
                            continue
                        if live and video_id:
                            if api_key:
                                confirmed = await confirm_with_api(client, api_key, video_id)
                                if confirmed is False:
                                    return YTLiveInfo(is_live=False, username=clean)
                                if confirmed is None:
                                    logger.debug(f"youtube confirm failed user={clean}, trusting page marker")
                            return YTLiveInfo(is_live=True, room_id=video_id, username=clean)
                        if live is False:
                            return YTLiveInfo(is_live=False, username=clean)
                    return YTLiveInfo(
                        is_live=False, username=clean,
                        error="blocked" if blocked else "fetch-failed",
                    )
            except Exception as e:
                logger.warning(f"youtube error user={clean} err={type(e).__name__}")
                return YTLiveInfo(is_live=False, username=clean, error="error")


checker = YouTubeChecker()
