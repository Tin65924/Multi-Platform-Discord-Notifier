"""Observe public TikTok embeds; publish only independently dated, claimed videos."""
import asyncio
import json
import logging
import re
import time
from collections import Counter

import httpx
from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError

from .config import get_settings
from .infrastructure.persistence.database import async_session
from .infrastructure.persistence.models import GlobalSettings, Subscription, UploadNotice
from .schemas import DISCORD_WEBHOOK_RE

log = logging.getLogger(__name__)
settings = get_settings()
ROOT = "https://www.tiktok.com"
PERIOD = 300
MAX_AGE = 900
MAX_BYTES = 2 * 1024 * 1024
HANDLE = re.compile(r"[A-Za-z0-9._]{1,32}")
VIDEO = re.compile(r"[1-9][0-9]{14,19}")


async def page(client, path, script):
    async with asyncio.timeout(12):
        async with client.stream("GET", ROOT + path) as r:
            r.raise_for_status()
            data = bytearray()
            async for chunk in r.aiter_bytes(chunk_size=32768):
                if len(data) + len(chunk) > MAX_BYTES:
                    raise ValueError("page too large")
                data.extend(chunk)
    match = re.search(rb'<script\b[^>]*\bid="' + script + rb'"[^>]*>(.*?)</script>', data, re.S)
    if not match:
        raise ValueError("missing page state")
    return json.loads(match[1])


def feed(state, handle):
    row = state["source"]["data"]["/embed/@" + handle]
    user = row["userInfo"]
    owner = str(user["id"])
    if (row["isReady"] is not True or row["isError"] is not False
            or user["privateAccount"] is not False or user["uniqueId"].lower() != handle
            or not owner.isdecimal()):
        raise ValueError("unconfirmed profile")
    videos = row["videoList"]
    if not isinstance(videos, list) or len(videos) > 20:
        raise ValueError("unexpected feed")
    ids = []
    for item in videos:
        vid = item["id"]
        if not VIDEO.fullmatch(vid) or item["authorUniqueId"].lower() != handle:
            raise ValueError("unexpected video")
        if item["privateItem"] is False and vid not in ids:
            ids.append(vid)
    return owner, ids


def publication(state, handle, owner, vid):
    detail = state["__DEFAULT_SCOPE__"]["webapp.video-detail"]
    item = detail["itemInfo"]["itemStruct"]
    if (detail.get("statusCode", 0) != 0 or item["id"] != vid
            or item["author"]["uniqueId"].lower() != handle
            or str(item["author"]["id"]) != owner or item.get("privateItem") is True):
        raise ValueError("unconfirmed video")
    return int(item["createTime"])


async def advance(sub_id, handle, owner, ids, now, mode):
    """Commit the snapshot before any external effect. Only the CAS winner proceeds."""
    async with async_session() as db:
        identity = (Subscription.id == sub_id, Subscription.enabled.is_(True),
                    Subscription.platform == "tiktok", Subscription.tiktok_username == handle,
                    or_(Subscription.tiktok_user_id.is_(None), Subscription.tiktok_user_id == owner))
        row = (await db.execute(select(Subscription.upload_checkpoint).where(*identity))).first()
        if row is None:
            return None
        old = row[0]
        prior = json.loads(old) if old else {}
        if now <= prior.get("at", 0):
            return None
        checkpoint = dict(handle=handle, owner=owner, ids=ids, at=now, mode=mode)
        result = await db.execute(update(Subscription).where(
            *identity, Subscription.upload_checkpoint == old
        ).values(upload_checkpoint=json.dumps(checkpoint)))
        await db.commit()
        if (result.rowcount != 1 or prior.get("handle") != handle or prior.get("owner") != owner
                or prior.get("mode") != mode or now - prior["at"] > MAX_AGE):
            return None
        return prior["at"], [vid for vid in ids if vid not in prior["ids"]]


def destination(gs):
    url = settings.TIKTOK_UPLOAD_WEBHOOK_URL
    if (not DISCORD_WEBHOOK_RE.fullmatch(url) or not url.startswith("https://discord.com/")
            or settings.RELAY_URL or (gs and gs.notifications_enabled is False)):
        return None
    webhook_id = url.split("/")[-2]
    for live in (settings.DASHBOARD_WEBHOOK_URL, gs.webhook_url if gs else None):
        if live and (not DISCORD_WEBHOOK_RE.fullmatch(live) or live.split("/")[-2] == webhook_id):
            return None
    return url


async def notice(client, sub_id, handle, vid, published, mode):
    async with async_session() as db:
        sub = await db.get(Subscription, sub_id)
        if not sub or not sub.enabled or sub.platform != "tiktok" or sub.tiktok_username != handle:
            return "removed"
        url = destination(await db.get(GlobalSettings, 1)) if mode == "deliver" else None
        status = "claimed" if url else ("observed" if mode == "observe" else "suppressed")
        row = UploadNotice(video_id=vid, handle=handle, status=status)
        db.add(row)
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            return "duplicate"
        if not url:
            return status
        if not 0 <= time.time() - published <= MAX_AGE:
            row.status = "stale"
        else:
            try:
                response = await client.post(url, params={"wait": "true"}, json={
                    "content": f"@{handle} posted a TikTok: {ROOT}/@{handle}/video/{vid}",
                    "allowed_mentions": {"parse": []},
                })
                if response.is_success:
                    row.status = "sent"
                elif response.is_client_error:
                    row.status = "rejected"
                else:
                    row.status = "uncertain"
            except httpx.RequestError:
                row.status = "uncertain"
        await db.commit()
        return row.status


PAGE_ERRORS = (httpx.HTTPError, TimeoutError, KeyError, TypeError, ValueError, AttributeError, RecursionError)


async def check(client, sub_id, handle, mode, counts):
    if not HANDLE.fullmatch(handle):
        counts["invalid_handle"] += 1
        return
    now = int(time.time())
    try:
        state = await page(client, "/embed/@" + handle, b"__FRONTITY_CONNECT_STATE__")
        owner, ids = feed(state, handle)
    except PAGE_ERRORS:
        counts["feed_unverified"] += 1
        return
    changes = await advance(sub_id, handle, owner, ids, now, mode)
    if changes is None:
        counts["baseline_or_skipped"] += 1
        return
    since, candidates = changes
    counts["checked"] += 1
    for vid in candidates:
        try:
            state = await page(client, f"/@{handle}/video/{vid}?lang=en", b"__UNIVERSAL_DATA_FOR_REHYDRATION__")
            published = publication(state, handle, owner, vid)
        except PAGE_ERRORS:
            counts["date_unverified"] += 1
            continue
        if since < published <= now:
            counts[await notice(client, sub_id, handle, vid, published, mode)] += 1
        else:
            counts["outside_window"] += 1


async def upload_loop():
    async with httpx.AsyncClient(timeout=10, limits=httpx.Limits(max_connections=2),
                                 follow_redirects=False) as client:
        while True:
            started = time.monotonic()
            counts = Counter()
            try:
                async with async_session() as db:
                    creators = (await db.execute(select(Subscription.id, Subscription.tiktok_username)
                                .where(Subscription.platform == "tiktok", Subscription.enabled.is_(True))
                                .order_by(Subscription.id))).all()
                for sub_id, handle in creators:
                    try:
                        await check(client, sub_id, handle, settings.TIKTOK_UPLOAD_MODE, counts)
                    except Exception as exc:
                        log.warning("upload check failed sub=%d error=%s", sub_id, type(exc).__name__)
                    await asyncio.sleep(1)
                log.info("TikTok uploads mode=%s results=%s", settings.TIKTOK_UPLOAD_MODE, dict(counts))
            except Exception as exc:
                log.warning("upload sweep unavailable: %s", type(exc).__name__)
            await asyncio.sleep(max(1, PERIOD - (time.monotonic() - started)))
