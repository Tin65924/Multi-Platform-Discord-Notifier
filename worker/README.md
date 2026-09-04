# Discord Relay Worker (free, ~2 minutes)

Forwards our Discord webhook posts through Cloudflare's network so a
flagged host IP can't 429 them.

## Deploy

1. https://dash.cloudflare.com → **Workers & Pages** → **Create Worker**
2. Replace the default code with `relay.js` → **Deploy**
3. Worker → **Settings** → **Variables & Secrets** → add **secrets**:
   - `DISCORD_WEBHOOK_URL` = your full Discord webhook URL
   - `RELAY_SECRET` = a long random string (e.g. `openssl rand -hex 32`)
4. Copy the Worker URL (`https://xxx.workers.dev`)

## Point the app at it

Render (or `.env`):

- `RELAY_URL` = the Worker URL from step 4
- `RELAY_SECRET` = same value as step 3

Leave both empty to send to Discord directly (old behavior).

## Verify

Watch the app logs on the next live detection: a `204` means Discord
accepted the relayed post. A `403` means `RELAY_SECRET` mismatches; a
`500` means the Worker's `DISCORD_WEBHOOK_URL` secret is missing.
