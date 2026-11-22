# Discord relay setup

Update the existing Worker under **Cloudflare Dashboard → Workers & Pages → your relay → Edit code**. Deploy the module in [relay.js](relay.js). In **Settings → Variables and Secrets** set these as secrets and deploy the changes:

| Cloudflare secret | Value |
| --- | --- |
| `DISCORD_WEBHOOK_URL` | Existing livestream Discord webhook URL |
| `DISCORD_UPLOAD_WEBHOOK_URL` | A separate Discord webhook created in the uploads channel |
| `RELAY_SECRET` | A long random shared secret. Keep the existing value when upgrading. |

On the **Render web service → Environment**, set `RELAY_URL` to the HTTPS Worker URL and `RELAY_SECRET` to the identical shared secret. Deploy the updated Render app. In relay mode neither Discord URL is required on Render. For automatic uploads use the default `TIKTOK_UPLOAD_MODE=deliver` and enable Uploads for the creator. The global notification pause also applies to Test upload.

Deploy the Worker first. Existing app versions send livestreams through the updated Worker. The new app's upload requests are rejected by an old Worker, preventing a test or upload from landing in the livestream channel during rollout.

Verify with **Creators → TikTok → Test upload**; expect “Testing upload” and the video link in the uploads channel. The next livestream should appear in the livestream channel. A 403 from the relay can indicate mismatched shared secrets or an old Worker; 503 can indicate invalid, missing or identical webhook destinations.
