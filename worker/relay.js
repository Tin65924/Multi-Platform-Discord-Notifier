/* PlaytopiaLiveNotifier — Discord webhook relay.
 *
 * Why: some cloud egress IPs get flagged by Cloudflare in front of Discord
 * (HTML 429s). This Worker forwards payloads from Cloudflare's own network,
 * which Discord trusts.
 *
 * Setup (dashboard only, ~2 minutes):
 *   1. https://dash.cloudflare.com -> Workers & Pages -> Create Worker
 *   2. Paste this file as the Worker code -> Deploy
 *   3. Worker -> Settings -> Variables & Secrets -> add secrets:
 *        DISCORD_WEBHOOK_URL = https://discord.com/api/webhooks/ID/TOKEN
 *        RELAY_SECRET        = any long random string
 *   4. Copy the Worker URL (https://xxx.workers.dev) plus RELAY_SECRET into
 *      the app's env as RELAY_URL / RELAY_SECRET.
 *
 * Protocol: app POSTs JSON {secret, payload} to the Worker URL.
 * Query params (?with_components=true) are forwarded to Discord untouched.
 * Discord's status code, body, and retry-after header pass straight through.
 */

export default {
  async fetch(request, env) {
    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }
    let body;
    try {
      body = await request.json();
    } catch {
      return new Response("Bad Request", { status: 400 });
    }
    const got = String(body.secret ?? "").trim();
    const want = String(env.RELAY_SECRET ?? "").trim();
    if (!want || got !== want) {
      return new Response("Forbidden", { status: 403 });
    }
    if (!env.DISCORD_WEBHOOK_URL || !env.DISCORD_WEBHOOK_URL.includes("discord.com/api/webhooks")) {
      return new Response("Relay not configured", { status: 500 });
    }

    const target = new URL(env.DISCORD_WEBHOOK_URL);
    const incoming = new URL(request.url);
    incoming.searchParams.forEach((v, k) => {
      if (k !== "secret") target.searchParams.set(k, v);
    });

    let upstream;
    try {
      upstream = await fetch(target.toString(), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "User-Agent": "PlaytopiaLiveNotifier/1.0",
        },
        body: JSON.stringify(body.payload ?? {}),
      });
    } catch (e) {
      return new Response("Upstream fetch failed", { status: 502 });
    }

    const respBody = await upstream.text();
    const out = new Response(respBody, { status: upstream.status });
    const ra = upstream.headers.get("retry-after");
    if (ra) out.headers.set("retry-after", ra);
    return out;
  },
};
