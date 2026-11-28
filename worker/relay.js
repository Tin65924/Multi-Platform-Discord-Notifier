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
 *        DISCORD_UPLOAD_WEBHOOK_URL = a separate webhook for uploads
 *        RELAY_SECRET        = any long random string
 *   4. Copy the Worker URL (https://xxx.workers.dev) plus RELAY_SECRET into
 *      the app's env as RELAY_URL / RELAY_SECRET.
 *
 * Legacy {secret, payload} means live. Uploads require Bearer auth + kind=upload.
 * Query params (?with_components=true) are forwarded to Discord untouched.
 * Discord's status code, body, and retry-after header pass straight through.
 */

/* Constant-time secret compare (SHA-256 digests, no early exit). */
async function secretsEqual(a, b) {
  const enc = new TextEncoder();
  const [da, db] = await Promise.all([
    crypto.subtle.digest("SHA-256", enc.encode("relay:" + a)),
    crypto.subtle.digest("SHA-256", enc.encode("relay:" + b)),
  ]);
  const xa = new Uint8Array(da);
  const xb = new Uint8Array(db);
  let diff = xa.length ^ xb.length;
  const n = Math.max(xa.length, xb.length);
  for (let i = 0; i < n; i++) diff |= xa[i % xa.length] ^ xb[i % xb.length];
  return diff === 0;
}

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
    if (!body || typeof body !== "object") return new Response("Bad Request", { status: 400 });
    const kind = body.kind === undefined ? "live" : body.kind;
    if (kind !== "live" && kind !== "upload") return new Response("Unknown route", { status: 400 });
    const got = kind === "upload"
      ? (request.headers.get("Authorization") || "").match(/^Bearer (.+)$/)?.[1] || ""
      : String(body.secret ?? "").trim();
    const want = String(env.RELAY_SECRET ?? "").trim();
    if (!want || !(await secretsEqual(got, want))) {
      return new Response("Forbidden", { status: 403 });
    }
    const webhook = /^https:\/\/(?:discord\.com|discordapp\.com)\/api\/webhooks\/([1-9]\d*)\/[A-Za-z0-9_-]+$/;
    const live = webhook.exec(env.DISCORD_WEBHOOK_URL || "");
    const upload = webhook.exec(env.DISCORD_UPLOAD_WEBHOOK_URL || "");
    if (!live || (kind === "upload" && (!upload || upload[1] === live[1]))) {
      return new Response("Webhook routing not configured", { status: 503 });
    }
    const target = new URL(kind === "upload" ? upload[0] : live[0]);
    const query = new URL(request.url).searchParams;
    for (const key of ["wait", "with_components"]) {
      if (query.get(key) === "true") target.searchParams.set(key, "true");
    }

    let upstream;
    try {
      upstream = await fetch(target.toString(), {
        method: "POST",
        // Workers supports manual/follow, but not the standard "error" mode.
        redirect: "manual",
        headers: {
          "Content-Type": "application/json",
          "User-Agent": "PlaytopiaLiveNotifier/1.0",
        },
        body: JSON.stringify(body.payload ?? {}),
      });
    } catch (e) {
      // Only record an allowlisted category; exception messages can contain webhook tokens.
      const message = String(e?.message || "");
      const code = message.match(/\b(1019|1021|1022|1024|1042|1101|1102|1103|1105)\b/)?.[1];
      const category = code ? "cloudflare_" + code
        : /redirect/i.test(message) ? "redirect"
        : /timeout/i.test(message) ? "timeout"
        : "network_or_other";
      console.error("discord_upstream_fetch", {kind, category});
      return new Response("Upstream fetch failed", { status: 502 });
    }

    // Never follow a redirect or return its Location, which may contain a token.
    if (upstream.status >= 300 && upstream.status < 400) {
      console.error("discord_upstream_redirect", {kind, status: upstream.status});
      return new Response("Upstream redirect rejected", {status: 502});
    }
    const respBody = upstream.status === 204 ? null : await upstream.text();
    const out = new Response(respBody, { status: upstream.status });
    const ra = upstream.headers.get("retry-after");
    if (ra) out.headers.set("retry-after", ra);
    return out;
  },
};
