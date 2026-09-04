"""Fully-automatic TikTok cookie minting — no manual sessionid copy-paste.

How it works: once per hour (lazily, on first TikTok call after expiry), a
headless Chromium visits tiktok.com like a normal viewer. TikTok itself mints
msToken / tt_webid_v2 / odin_tt cookies, which we cache to disk and inject
into every TikTokLive API call. Zero manual work.

Degrades gracefully: if the browser is unavailable or TikTok withholds
cookies (flagged IP/fingerprint), we back off and callers fall back to
TIKTOK_SESSION_ID env, then anonymous mode. Never raises.

Windows note: uvicorn may run a SelectorEventLoop (no subprocess support),
which Playwright needs to launch the browser. Minting therefore runs in a
worker thread with its own fresh event loop.
"""
import asyncio
import json
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_FILE = Path(__file__).resolve().parent.parent / ".tt_cookies.json"
REFRESH_SECONDS = 3600
RETRY_SECONDS = 1800  # wait this long after a failed mint before retrying
WANTED = ("msToken", "tt_webid_v2", "tt_webid", "odin_tt")

_lock = asyncio.Lock()
_mem_cache: dict = {"at": 0.0, "cookies": {}}
_fail_at: float = 0.0
_warned_recently: float = 0.0


def _retry_after() -> float:
    try:
        from .config import get_settings

        return float(getattr(get_settings(), "TT_COOKIE_RETRY_SECONDS", RETRY_SECONDS))
    except Exception:
        return RETRY_SECONDS


def _load_disk() -> dict:
    try:
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("cookies"), dict):
            return data
    except Exception:
        pass
    return {"at": 0.0, "cookies": {}}


def _save_disk(cookies: dict):
    try:
        CACHE_FILE.write_text(json.dumps({"at": time.time(), "cookies": cookies}), encoding="utf-8")
    except Exception as e:
        logger.debug(f"cookie cache save failed err={type(e).__name__}")


async def _mint_fresh_coro() -> dict:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.info("cookie provider: playwright not installed, skipping")
        return {}
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, args=["--disable-blink-features=AutomationControlled"]
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
                locale="en-US",
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            page = await ctx.new_page()
            await page.goto("https://www.tiktok.com/", wait_until="domcontentloaded", timeout=45000)
            await page.mouse.move(400, 300)
            await page.wait_for_timeout(8000)
            cookies = {c["name"]: c["value"] for c in await ctx.cookies()}
            await browser.close()
            fresh = {k: v for k, v in cookies.items() if k in WANTED and v}
            logger.info(f"cookie provider: minted {sorted(fresh.keys()) or 'nothing'}")
            return fresh
    except Exception as e:
        raise e


def _mint_in_worker() -> dict:
    # Fresh event loop in a worker thread: on Windows this is a
    # ProactorEventLoop, which supports the subprocesses Playwright needs
    # (uvicorn's own loop may be a SelectorEventLoop, which does not).
    return asyncio.run(_mint_fresh_coro())


async def _try_mint() -> dict:
    global _fail_at, _warned_recently
    try:
        fresh = await asyncio.to_thread(_mint_in_worker)
    except Exception as e:
        _fail_at = time.time()
        # Warn loudly the first time, then stay quiet until backoff expires
        if time.time() - _warned_recently > 3600:
            logger.warning(f"cookie provider: mint failed err={type(e).__name__}, backing off")
            _warned_recently = time.time()
        else:
            logger.debug(f"cookie provider: mint failed err={type(e).__name__}")
        return {}
    if fresh:
        return fresh
    _fail_at = time.time()
    return {}


async def get_cookies() -> dict:
    """Return usable cookies (memory → disk → fresh mint). Never raises."""
    global _mem_cache
    try:
        from .config import get_settings

        if (getattr(get_settings(), "TT_COOKIE_PROVIDER", "on") or "on").lower() == "off":
            return {}
    except Exception:
        pass
    now = time.time()
    if _mem_cache["cookies"] and now - _mem_cache["at"] < REFRESH_SECONDS:
        return _mem_cache["cookies"]
    disk = _load_disk()
    if disk["cookies"] and now - disk["at"] < REFRESH_SECONDS:
        _mem_cache = disk
        return disk["cookies"]
    # Negative cache: a failed mint backs off instead of retrying every check
    if _fail_at and now - _fail_at < _retry_after():
        return disk["cookies"] or {}
    async with _lock:
        if _mem_cache["cookies"] and time.time() - _mem_cache["at"] < REFRESH_SECONDS:
            return _mem_cache["cookies"]
        fresh = await _try_mint()
        if fresh:
            _mem_cache = {"at": time.time(), "cookies": fresh}
            _save_disk(fresh)
            return fresh
        if disk["cookies"]:
            return disk["cookies"]
        return {}
