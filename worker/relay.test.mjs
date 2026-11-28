import assert from 'node:assert/strict';
import { test } from 'node:test';
import worker from './relay.js';

test('routes LIVE and uploads to distinct secrets and rejects ambiguous delivery', async (t) => {
  const env = { RELAY_SECRET: 'shared', DISCORD_WEBHOOK_URL: 'https://discord.com/api/webhooks/11/live',
    DISCORD_UPLOAD_WEBHOOK_URL: 'https://discord.com/api/webhooks/22/upload' };
  const payload = { content: 'hello' };
  const calls = [];
  t.mock.method(globalThis, 'fetch', async (url, init) => {
    calls.push(url);
    assert.deepEqual(JSON.parse(init.body), payload);
    return new Response(null, {status: 204});
  });
  const post = (body, auth, vars = env) => worker.fetch(new Request('https://relay.test/?wait=true', {
    method: 'POST', headers: auth ? {Authorization: 'Bearer shared'} : {}, body: JSON.stringify({...body, payload})
  }), vars);
  for (const body of [{secret: 'shared'}, {kind: 'live', secret: 'shared'}])
    assert.equal((await post(body)).status, 204);
  assert.equal((await post({kind:'upload'}, true)).status, 204);
  assert.deepEqual(calls, [env.DISCORD_WEBHOOK_URL, env.DISCORD_WEBHOOK_URL, env.DISCORD_UPLOAD_WEBHOOK_URL].map(u => u+'?wait=true'));
  assert.equal((await post({kind:'upload', secret:'shared'})).status, 403);
  assert.equal((await post({}, true)).status, 403);
  assert.equal((await post({kind:'unknown',secret:'shared'})).status, 400);
  for (const value of ['', 'https://discord.com/api/webhooks/11/other'])
    assert.equal((await post({kind:'upload'}, true, {...env, DISCORD_UPLOAD_WEBHOOK_URL:value})).status, 503);
  assert.equal(calls.length, 3);
});

test('does not expose webhook credentials in fetch error logs', async (t) => {
  const env = {RELAY_SECRET:'shared', DISCORD_WEBHOOK_URL:'https://discord.com/api/webhooks/11/private-token'};
  let logs = [];
  t.mock.method(console, 'error', (...args) => logs.push(JSON.stringify(args)));
  t.mock.method(globalThis, 'fetch', async () => {throw new Error('1042 https://discord.com/api/webhooks/11/private-token');});
  const req = new Request('https://relay.test/', {method:'POST', body: JSON.stringify({secret:'shared',payload:{content:'hi'}})});
  assert.equal((await worker.fetch(req,env)).status, 502);
  assert.match(logs[0], /cloudflare_1042/);
  assert.doesNotMatch(logs[0], /private-token|discord.com/);
});
