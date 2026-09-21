"""Phase 0 characterization tests: pin CURRENT pure behavior.

No DB, no network. If a refactor changes any of these outcomes, the test
failure is the signal — update the test only when the behavior change is
intentional ( Phases 1-6 ).
"""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

UTC = timezone.utc

# --- analytics: tier boundaries -------------------------------------------

def test_status_tiers():
    from app.analytics import status_for
    assert status_for(0) == "active"
    assert status_for(2) == "active"
    assert status_for(3) == "soft"
    assert status_for(6) == "soft"
    assert status_for(7) == "hard"
    assert status_for(9) == "hard"
    assert status_for(10) == "terminate"
    assert status_for(365) == "terminate"


def test_aware_utc_treats_naive_as_utc():
    from app.analytics import aware_utc
    naive = datetime(2026, 9, 1, 12, 0, 0)
    assert aware_utc(naive) == naive.replace(tzinfo=UTC)
    aware = naive.replace(tzinfo=UTC)
    assert aware_utc(aware) is aware
    assert aware_utc(None) is None


def test_days_idle_never_live_counts_from_tracking_start():
    from app.analytics import days_idle_since
    from app.analytics import MANILA
    now = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    created = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)  # 10 Manila days back
    idle, hist = days_idle_since(None, created, now)
    expected = (now.astimezone(MANILA).date() - created.astimezone(MANILA).date()).days
    assert idle == expected == 10
    assert hist is False


def test_overlap_minutes_clips_and_open_ends_at_t():
    from app.analytics import overlap_minutes
    f = datetime(2026, 9, 20, 0, 0, tzinfo=UTC)
    t = datetime(2026, 9, 21, 0, 0, tzinfo=UTC)
    # fully inside
    assert overlap_minutes(f + timedelta(hours=1), f + timedelta(hours=2), f, t) == 60.0
    # overlapping start clamps to f
    assert overlap_minutes(f - timedelta(hours=1), f + timedelta(hours=1), f, t) == 60.0
    # open session ends at t
    assert overlap_minutes(t - timedelta(hours=2), None, f, t) == 120.0
    # disjoint -> 0
    assert overlap_minutes(t + timedelta(hours=1), None, f, t) == 0.0


def test_pct_change_none_on_zero_prev():
    from app.analytics import pct_change
    assert pct_change(10, 0) == None  # noqa: E711
    assert pct_change(150, 100) == 50.0
    assert pct_change(50, 100) == -50.0


def test_handle_flagged_needs_3_days():
    from app.analytics import handle_flagged
    now = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    assert handle_flagged(SimpleNamespace(first_not_found_at=None), now) is False
    assert handle_flagged(SimpleNamespace(first_not_found_at=now - timedelta(days=2)), now) is False
    assert handle_flagged(SimpleNamespace(first_not_found_at=now - timedelta(days=3)), now) is True


def test_last_live_prefers_sessions_over_columns():
    from app.analytics import last_live_of
    now = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    sub = SimpleNamespace(last_live_at=datetime(2026, 9, 1, tzinfo=UTC),
                          last_notified_at=None, created_at=datetime(2026, 1, 1, tzinfo=UTC))
    assert last_live_of(sub, [], now) == datetime(2026, 9, 1, tzinfo=UTC)
    sess = [SimpleNamespace(started_at=datetime(2026, 9, 20, tzinfo=UTC))]
    assert last_live_of(sub, sess, now) == datetime(2026, 9, 20, tzinfo=UTC)


# --- webhook: mentions / embeds --------------------------------------------

def test_discord_mention_prefers_numeric_id():
    from app.infrastructure.notify.discord import discord_mention
    assert discord_mention("123", "name", "@h") == "<@123>"
    assert discord_mention(" 123 ", None, "@h") == "<@123>"
    assert discord_mention("", "name", "@h") == "@name"
    assert discord_mention(None, None, "@h") == "@h"


def test_parse_color_falls_back_to_pink():
    from app.infrastructure.notify.discord import parse_color
    assert parse_color("#FF0050") == 0xFF0050
    assert parse_color("FF0050") == 0xFF0050
    assert parse_color("garbage") == 0xFF0050
    assert parse_color(None) == 0xFF0050


def test_build_embed_shape_and_mentions():
    from app.infrastructure.notify.discord import build_embed
    p = build_embed("somehandle", message="{discord} is LIVE!", ping_role_id=None,
                    ping_everyone=False, image_url=None, color="#FF0050",
                    author_name="Creator", discord_username="du", discord_user_id="999")
    assert p["username"] == "Forest Watcher"
    assert p["content"] == "<@999> is LIVE!"
    assert p["allowed_mentions"] == {"parse": ["users"]}
    emb = p["embeds"][0]
    assert emb["author"]["name"] == "@somehandle"
    assert "https://www.tiktok.com/@somehandle/live" in emb["description"]
    assert emb["color"] == 0xFF0050
    btns = p["components"][0]["components"]
    assert [b["label"] for b in btns] == ["Watch Stream", "Profile"]


def test_build_embed_everyone_prepend():
    from app.infrastructure.notify.discord import build_embed
    p = build_embed("h", message="{discord} is LIVE!", ping_everyone=True,
                    discord_username="du")
    assert p["content"].startswith("@everyone\n")
    assert "everyone" in p["allowed_mentions"]["parse"]


def test_effective_image_precedence():
    from app.infrastructure.notify.discord import effective_image
    sub = SimpleNamespace(id=1, image_mime=None, image_url="http://link/img.png",
                          avatar_url="http://av/a.png")
    assert effective_image(sub, "http://global/g.png") == "http://link/img.png"
    sub.image_url = None
    assert effective_image(sub, "http://global/g.png") == "http://av/a.png"
    sub.avatar_url = None
    assert effective_image(sub, "http://global/g.png") == "http://global/g.png"


def test_resolve_webhook_cfg_row_beats_env():
    from app.infrastructure.notify.discord import resolve_webhook_cfg
    gs = SimpleNamespace(webhook_url="https://discord.com/api/webhooks/1/abc",
                         ping_role_id="77", custom_message="hi {discord}",
                         ping_everyone=False, embed_image_url="http://i/x.png",
                         embed_color="#00FF00")
    url, ping, msg, everyone, image, color = resolve_webhook_cfg(gs)
    assert (url, ping, msg, everyone, image, color) == (
        "https://discord.com/api/webhooks/1/abc", "77", "hi {discord}",
        False, "http://i/x.png", "#00FF00")


# --- schemas: handle normalization ------------------------------------------

def test_normalize_handle_tiktok():
    from app.schemas import normalize_handle
    assert normalize_handle("tiktok", "https://www.tiktok.com/@SomeUser") == "someuser"
    assert normalize_handle("tiktok", "@SomeUser") == "someuser"
    try:
        normalize_handle("tiktok", "!!")
        assert False, "should raise"
    except ValueError:
        pass


def test_normalize_handle_youtube_channel_id_verbatim():
    from app.schemas import normalize_handle
    cid = "UC" + "A" * 22
    assert normalize_handle("youtube", cid) == cid
    assert normalize_handle("youtube", "https://www.youtube.com/@SomeChan") == "somechan"
    assert normalize_handle("kick", "https://kick.com/SomeSlug") == "someslug"
    assert normalize_handle("twitch", "SomeTw") == "sometw"


# --- db url helpers ----------------------------------------------------------

def test_normalize_db_url_to_asyncpg():
    from app.infrastructure.persistence.database import normalize_db_url
    assert normalize_db_url("postgres://u:p@h/db").startswith("postgresql+asyncpg://")
    assert normalize_db_url("postgresql://u:p@h/db").startswith("postgresql+asyncpg://")
    assert normalize_db_url("sqlite+aiosqlite:///./x.db").startswith("sqlite")


def test_sanitize_query_drops_unknown_keeps_ssl():
    from app.infrastructure.persistence.database import _sanitize_query
    clean, mode = _sanitize_query("postgresql+asyncpg://u:p@h/db?sslmode=require&channel_binding=require")
    assert mode == "require"
    assert "channel_binding" not in clean
    assert "sslmode" not in clean  # translated into connect_args, not the URL


# --- poller: recycle policy ---------------------------------------------------

def test_should_recycle_only_when_climbing_past_ceiling():
    from app.infrastructure.scheduler import loop
    loop._last_rss = None
    assert loop.should_recycle(400.0) is False      # first sample arms, never fires
    assert loop.should_recycle(440.0) is False      # climbing but under ceiling
    assert loop.should_recycle(440.0) is False      # flat high -> equilibrium, leave alone
    assert loop.should_recycle(451.0) is True       # climbing past 450 -> recycle
    assert loop.should_recycle(300.0, 461.0) is True  # cur is the real OOM line


def test_rss_helpers_never_raise():
    from app.infrastructure.scheduler.loop import rss_mb, rss_current_mb
    assert rss_mb() is None or isinstance(rss_mb(), float)
    assert rss_current_mb() is None or isinstance(rss_current_mb(), float)


# --- security -----------------------------------------------------------------

def test_password_roundtrip():
    from app.security import hash_password, verify_password
    h = hash_password("correct horse 123")
    assert verify_password("correct horse 123", h) is True
    assert verify_password("wrong", h) is False
    assert verify_password("x", "not-a-hash") is False


# --- wildlines ------------------------------------------------------------------

def test_wildlines_count_and_render():
    from app.infrastructure.notify.wildlines import WILD_LINES, random_wild_line
    assert len(WILD_LINES) == 100
    line = random_wild_line("TestName")
    assert isinstance(line, str) and len(line) > 0
