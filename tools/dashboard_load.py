"""Run with python -m tools.dashboard_load. Uses disposable SQLite and no provider calls."""
import asyncio
import os
import statistics
import tempfile
import time
from pathlib import Path

import httpx


async def run():
    from app.infrastructure.persistence.database import async_session, engine, init_db
    from app.infrastructure.persistence.models import Subscription
    from app.main import app

    try:
        await init_db()
        async with async_session() as db:
            db.add_all(Subscription(platform="tiktok", tiktok_username=f"load{i}") for i in range(100))
            await db.commit()
        # ASGITransport does not start the application lifespan or its polling task.
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://local.test") as client:
            route = "/api/subscriptions"
            assert (await client.get(route)).status_code == 401
            login = await client.post("/api/login", json={"username": "localqa", "password": "local-only-load"})
            login.raise_for_status()
            paths = ["/", "/api/me", route, "/api/schedule", "/api/logs"]
            for concurrency in (5, 25, 50):
                times = []

                async def worker(number):
                    for i in range(20):
                        path = paths[(i + number) % len(paths)]
                        start = time.perf_counter()
                        response = await client.get(path)
                        response.raise_for_status()
                        if path == route:
                            assert len(response.json()) == 100
                        times.append(time.perf_counter() - start)

                start = time.perf_counter()
                async with asyncio.TaskGroup() as group:
                    for number in range(concurrency):
                        group.create_task(worker(number))
                rate = len(times) / (time.perf_counter() - start)
                p95 = statistics.quantiles(times, n=100)[94] * 1000
                print(f"{concurrency} clients: {len(times)} requests, {rate:.1f} req/s, p95 {p95:.0f} ms", flush=True)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp:
        os.environ.update(DATABASE_URL="sqlite+aiosqlite:///" + (Path(tmp) / "load.db").as_posix(),
                          SUPERADMIN_USER="localqa", SUPERADMIN_PASS="local-only-load",
                          APP_SECRET_KEY="isolated-fixture", TT_COOKIE_PROVIDER="off")
        asyncio.run(run())
