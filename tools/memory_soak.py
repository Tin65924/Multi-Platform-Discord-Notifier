"""Offline client-allocation comparison using real HTTPX/TLS client objects.

Run each mode in a NEW process, from the repository root:
  python -m tools.memory_soak --mode pooled --checks 10000
  python -m tools.memory_soak --mode per-creator --creators 60 --checks 10000

Only HTTP send is stubbed; HTTPX transports and TLS contexts are really
allocated. This measures client lifecycle, NOT live TikTok/Render behavior.
No database, browser, credentials, Discord, or external network is used.
"""
import argparse
import asyncio
import gc
import json
import logging
import os
import sys
from pathlib import Path

os.environ["TT_COOKIE_PROVIDER"] = "off"
os.environ["TIKTOK_SESSION_ID"] = ""

import httpx
from TikTokLive import TikTokLiveClient
from app.infrastructure.checkers import tiktok


def current_rss_mb():
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
                (name, ctypes.c_size_t) for name in (
                    "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                    "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                    "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]

        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        get_process = ctypes.windll.kernel32.GetCurrentProcess
        get_process.restype = wintypes.HANDLE
        get_memory = ctypes.windll.psapi.GetProcessMemoryInfo
        get_memory.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        if not get_memory(get_process(), ctypes.byref(counters), counters.cb):
            raise ctypes.WinError()
        return round(counters.WorkingSetSize / (1024 * 1024), 2)
    if Path("/proc/self/statm").exists():
        pages = int(Path("/proc/self/statm").read_text().split()[1])
        return round(pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024), 2)
    return None


async def fake_send(request, **kwargs):
    # Real I/O yields. Without this, a tight synchronous mock starves asyncio's
    # cleanup of cancelled timeout handles and creates artificial RSS growth.
    await asyncio.sleep(0)
    return httpx.Response(200, request=request, json={
        "message": "", "data": {"liveRoom": {"status": 4}, "user": {"roomId": "123"}},
    })


async def run(args):
    logging.disable(logging.CRITICAL)
    original = tiktok.TikTokWebClient
    created = []
    legacy = {}
    checker = tiktok.TikTokChecker()

    def factory(**kwargs):
        client = original(**kwargs)
        client.httpx_client.send = fake_send
        created.append(client)
        return client

    tiktok.TikTokWebClient = factory
    before = current_rss_mb()
    samples = []
    try:
        for i in range(args.checks):
            name = f"creator{i % args.creators}"
            if args.mode == "per-creator":
                if name not in legacy:
                    client = TikTokLiveClient(unique_id=f"@{name}")
                    client.web.httpx_client.send = fake_send
                    legacy[name] = client
                    created.append(client.web)
                assert not await legacy[name].is_live()
            else:
                result = await checker.is_live(name)
                assert result.error is None and not result.is_live
                checker.drop(name)  # repeated remove/refresh requests must not allocate
            if (i + 1) % 1000 == 0 or i + 1 == args.checks:
                gc.collect()
                samples.append({"checks": i + 1, "rss_mb": current_rss_mb()})
        if args.mode == "pooled":
            assert len(created) <= 2
    finally:
        await checker.aclose()
        for client in legacy.values():
            # TikTokLive 7.0.0's full client.close() calls run_until_complete
            # from this running loop. These clients never opened a websocket;
            # close their real HTTP resources directly for a clean comparison.
            await client.web.close()
        tiktok.TikTokWebClient = original
    assert all(client.httpx_client.is_closed for client in created)
    print(json.dumps({"mode": args.mode, "creators": args.creators,
                      "checks": args.checks, "clients_created": len(created),
                      "rss_before_mb": before, "samples": samples,
                      "all_clients_closed": True}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("pooled", "per-creator"), default="pooled")
    parser.add_argument("--creators", type=int, default=60)
    parser.add_argument("--checks", type=int, default=10000)
    args = parser.parse_args()
    if not 1 <= args.creators <= 500 or not 1 <= args.checks <= 100000:
        parser.error("creators must be 1..500 and checks must be 1..100000")
    asyncio.run(run(args))
