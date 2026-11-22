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
