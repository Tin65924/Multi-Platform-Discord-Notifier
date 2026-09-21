# PlaytopiaLiveNotifier

Multi-platform LIVE → Discord notifier (TikTok, YouTube, Kick; Twitch stored, poller pending). Optimized for ~60 creators on Render Free + Neon.

## Deploy (GitHub -> Render, no Docker)

1. Push repo to GitHub.
2. Render -> New Web Service -> Connect repo -> Runtime Python (or apply `render.yaml`)
   - Build: `pip install -r requirements.txt`
   - Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
   - Health: `/health`
3. Env vars in Render (see `render.yaml` for the full list):
   - `DATABASE_URL` = Neon `postgresql://...?ssl=require`
   - `APP_SECRET_KEY` = Generate
   - `SUPERADMIN_USER=superadmin`, `SUPERADMIN_PASS` = Generate (copy it — first login)
   - Platform keys as needed: `KICK_CLIENT_ID/SECRET`, `TWITCH_CLIENT_ID/SECRET`, `YOUTUBE_API_KEY`, `TIKTOK_SESSION_ID`
   - Leave `TT_COOKIE_PROVIDER=off` on Render (no browsers installed)
4. Deploy -> open `https://your-app.onrender.com/` -> log in as superadmin -> Defaults tab -> save webhook.
5. Keep alive + force sweeps (cron-job.org):
   - `GET https://your-app.onrender.com/health` every 14 min (no auth)
   - `GET https://your-app.onrender.com/api/cron/poll` every 5 min (skips if a sweep is already running). Set `CRON_SECRET` in Render env and append `?secret=...` to lock the trigger; unset = open.

First deploy runs DB migrations automatically (`init_db`): creates tables, backfills the `platform` column, and swaps the handle-unique index for the composite `(platform, handle)` one. Safe to redeploy — all steps are idempotent.

## Local dev

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-test.txt  # pytest, local/CI only (never on Render)
copy .env.example .env  # sqlite default; fill platform keys as needed
.\venv\Scripts\python -m pytest tests -q
uvicorn app.main:app --reload
```

## Security

- No secrets in repo, only `.env.example` (`.env` is gitignored)
- Session login (admin/superadmin roles, pbkdf2 hashes, 10-attempts/5-min per-IP rate limit); `/api/cron/poll` accepts an optional `CRON_SECRET`
- Webhook URL validated by regex; creator handles normalized per platform; SQL via ORM
- Logs redacted (webhook URLs, DB URL) — see `app/logging_config.py`
- Platform secrets are server-side only, never sent to the frontend

## Architecture (pragmatic Clean Architecture)

Dependency direction only: `presentation → application → domain ← infrastructure`.

- `app/domain/` — pure entities, `CheckOutcome` taxonomy, ports (Protocols). No third-party imports (enforced by test).
- `app/application/` — one generic sweep (`sweep.py`) + pure notify policy. All branching lives here; fully unit-tested.
- `app/infrastructure/` — side effects: `checkers/` (one folder per platform + port), `persistence/` (models, engine, repo implementations), `notify/` (Discord embeds/sender), `media/` (photo uploads), `scheduler/` (poll loop, memory guard, schedule state).
- `app/presentation/api/` — one router per resource; thin (parse → service → shape).
- `app/poller.py` — sweep wrappers + maintenance only. `app/core` stays at top level (`config.py`, `security.py`, `logging_config.py`).
- `tests/` — 50+ tests: pure characterization, sweep fakes, sqlite end-to-end, route inventory.

## How it works

- One row per platform account (`platform` + handle, composite unique). Same name can live on TikTok *and* Kick; never twice on one platform.
- One generic sweep (`application/sweep.py`) drives all platforms; adapters live in `infrastructure/checkers/` (TikTok via TikTokLive lib, YouTube keyless page parse + optional API confirm, Kick official API).
- Deduplication by session id (`room_id`/`videoId`/`start_time`) + 15 min cooldown. Inconclusive checks preserve card state instead of flipping offline.
- Dashboard: platform filter card, creator cards grid, EDIT/PREVIEW/REMOVE/TEST/FORCE per card, global defaults, admins + audit (superadmin).
