"""Phase 3: API surface inventory — the split must not add, drop or move any route."""
from fastapi import FastAPI

EXPECTED = {
    ("POST", "/api/login"), ("POST", "/api/logout"),
    ("GET", "/api/me"), ("POST", "/api/me/password"),
    ("GET", "/api/admins"), ("POST", "/api/admins"),
    ("POST", "/api/admins/{uid}/toggle"), ("DELETE", "/api/admins/{uid}"),
    ("GET", "/api/audit"),
    ("GET", "/api/logs"), ("GET", "/api/logs/meta"),
    ("GET", "/api/schedule"),
    ("GET", "/api/settings"), ("PUT", "/api/settings"),
    ("POST", "/api/send-custom"), ("GET", "/api/wildlines"),
    ("GET", "/api/subscriptions"), ("POST", "/api/subscriptions"),
    ("PATCH", "/api/subscriptions/{sub_id}/style"),
    ("POST", "/api/subscriptions/{sub_id}/photo"),
    ("DELETE", "/api/subscriptions/{sub_id}/photo"),
    ("POST", "/api/subscriptions/{sub_id}/fetch-profile"),
    ("PATCH", "/api/subscriptions/{sub_id}/handle"),
    ("POST", "/api/subscriptions/{sub_id}/mark-offline"),
    ("DELETE", "/api/subscriptions/{sub_id}"),
    ("POST", "/api/subscriptions/{sub_id}/test"),
    ("POST", "/api/subscriptions/{sub_id}/force-notify"),
    ("GET", "/api/subscriptions/{sub_id}/payload"),
    ("POST", "/api/subscriptions/{sub_id}/check-now"),
    ("GET", "/api/media/creator/{sub_id}"),
    ("GET", "/api/analytics/overview"), ("GET", "/api/analytics/kpis"),
    ("GET", "/api/analytics/creator/{sub_id}"),
    ("GET", "/api/debug/memory"),
    ("POST", "/api/cron/poll"), ("GET", "/api/cron/poll"),
}


def _surface():
    from app.presentation.api import (
        analytics, admins, auth, logs, media, misc, schedule, settings,
        subscriptions,
    )
    app = FastAPI()
    for mod in (auth, admins, subscriptions, logs, schedule, settings,
                media, analytics, misc):
        app.include_router(mod.router, prefix="/api")
    got = set()
    for route in app.routes:
        if route.path in ("/docs", "/docs/oauth2-redirect", "/openapi.json", "/redoc"):
            continue  # FastAPI built-ins, not ours
        for method in (route.methods or set()) - {"HEAD", "OPTIONS"}:
            got.add((method, route.path))
    return got


def test_api_surface_unchanged():
    got = _surface()
    assert got == EXPECTED, (
        "route drift:\n missing=%s\n extra=%s"
        % (sorted(EXPECTED - got), sorted(got - EXPECTED))
    )


def test_app_imports_with_split_routers():
    import app.main as main
    assert len(main.app.routes) > len(EXPECTED)
