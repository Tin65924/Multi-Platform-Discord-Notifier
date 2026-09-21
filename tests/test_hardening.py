"""Phase 5: cron secret + login rate-limit behavior."""
import pytest


def test_cron_open_when_unset():
    from app.presentation.api import misc
    old = misc.settings.CRON_SECRET
    misc.settings.CRON_SECRET = ""
    try:
        assert misc._cron_authorized(None) is True
        assert misc._cron_authorized("anything") is True
    finally:
        misc.settings.CRON_SECRET = old


def test_cron_secret_enforced():
    from app.presentation.api import misc
    old = misc.settings.CRON_SECRET
    misc.settings.CRON_SECRET = "s3cr3t"
    try:
        assert misc._cron_authorized("s3cr3t") is True
        assert misc._cron_authorized("wrong") is False
        assert misc._cron_authorized(None) is False
        assert misc._cron_authorized("") is False
    finally:
        misc.settings.CRON_SECRET = old


def test_cron_endpoints_reject_without_secret(monkeypatch):
    from fastapi import HTTPException
    from app.presentation.api import misc
    monkeypatch.setattr(misc.settings, "CRON_SECRET", "s3cr3t", raising=False)
    with pytest.raises(HTTPException) as ei:
        misc._require_cron(None)
    assert ei.value.status_code == 403
    misc._require_cron("s3cr3t")  # must not raise


def test_login_rate_limit_blocks_and_resets():
    from app.presentation.api import auth
    auth._login_attempts.clear()
    ip = "9.9.9.9"
    assert all(auth._login_allowed(ip, now=1000.0, limit=3, window=60) for _ in range(3))
    assert auth._login_allowed(ip, now=1001.0, limit=3, window=60) is False
    # other IPs unaffected
    assert auth._login_allowed("8.8.8.8", now=1001.0, limit=3, window=60) is True
    # window expiry resets
    assert auth._login_allowed(ip, now=1000.0 + 61, limit=3, window=60) is True
    auth._login_attempts.clear()


def test_client_ip_prefers_forwarded_for():
    from app.presentation.api.auth import _client_ip

    class Req:
        def __init__(self, headers, host):
            self.headers = headers
            self.client = type("C", (), {"host": host})()

    assert _client_ip(Req({"x-forwarded-for": "1.2.3.4, 5.6.7.8"}, "9.9.9.9")) == "1.2.3.4"
    assert _client_ip(Req({}, "9.9.9.9")) == "9.9.9.9"
