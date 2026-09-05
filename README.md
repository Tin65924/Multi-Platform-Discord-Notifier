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
   - `GET https://your-app.onrender.com/api/cron/poll` every 5 min (triggers a sweep, no secret needed; skips if a sweep is already running)

First deploy runs DB migrations automatically (`init_db`): creates tables, backfills the `platform` column, and swaps the handle-unique index for the composite `(platform, handle)` one. Safe to redeploy — all steps are idempotent.

## Local dev

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env  # sqlite default; fill platform keys as needed
uvicorn app.main:app --reload
```

## Security

- No secrets in repo, only `.env.example` (`.env` is gitignored)
- Session login (admin/superadmin roles, pbkdf2 hashes); `/api/cron/poll` is an open trigger that skips when a sweep is already running
- Webhook URL validated by regex; creator handles normalized per platform; SQL via ORM
- Logs redacted (webhook URLs, DB URL) — see `app/logging_config.py`
- Platform secrets are server-side only, never sent to the frontend

## How it works

- One row per platform account (`platform` + handle, composite unique). Same name can live on TikTok *and* Kick; never twice on one platform.
- Poller sweeps TikTok (unofficial TikTokLive lib) + YouTube (keyless page parse + optional API confirm) + Kick (official API, one batched call) every `CHECK_INTERVAL_SECONDS`.
- Deduplication by session id (`room_id`/`videoId`/`start_time`) + 15 min cooldown. Inconclusive checks preserve card state instead of flipping offline.
- Dashboard: platform filter card, creator cards grid, EDIT/PREVIEW/REMOVE/TEST/FORCE per card, global defaults, admins + audit (superadmin).
