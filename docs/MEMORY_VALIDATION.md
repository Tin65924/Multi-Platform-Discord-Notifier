# Client memory lifecycle

TikTok checks use two lazily created HTTP-only TikTokLive clients. Each slot
has at most one connection and serves one account at a time. The number of
clients is independent of the number of creators, renames, manual checks,
and calls to the legacy `drop`/`evict_missing` hooks. Those hooks now have no
account cache to discard. The existing live/offline and room-ID routes and
notification policy are retained.

The pinned TikTokLive 7.0.0 full client's `close()` also invokes
`run_until_complete` from its cleanup path, which raises when called inside
the app's running event loop. Previously, eviction swallowed that exception.
Using the HTTP-only client's async close avoids that path.

Application shutdown cancels and awaits its poll/cookie-warm tasks, drains
the TikTok pool, closes the shared profile and YouTube clients, and disposes
the SQLAlchemy engine. Cancellation returns leased slots; a restarted
lifespan can start monitoring again. `/api/debug/memory` (superadmin only)
now includes `tiktok_pool` allocation/capacity/in-use counters.

## Reproduce locally

Use the existing test requirements and run:

```text
python -m pytest tests -q
python -m tools.memory_soak --mode pooled --checks 10000
python -m tools.memory_soak --mode per-creator --creators 60 --checks 10000
```

Run the comparison commands in separate processes. The second mode
reproduces the old per-creator client allocation strategy, not the complete
old application. Both use the installed TikTokLive routes, real HTTPX
transport/TLS allocation, and stubbed HTTP sends. No external service or
credential is used. The pooled mode asserts that at most two clients are
created, and both modes assert that all clients close. RSS samples are
reported on Windows/Linux; they are observations, not universal thresholds.

Regression tests cover account churn, retained Python heap after warm-up,
concurrent waiters, cancellation, shutdown during an active request, close
failure/retry, repeated lifespan startup, supported authentication cookies,
and LIVE/OFFLINE/not-found/blocked/timeout/missing-room-ID results.

## Production validation still required

These checks do not prove that all native memory growth or provider failures
are eliminated. Compare actual Render current RSS after warm-up across at
least 24 hours with a stable roster. Check that allocated TikTok clients stay
at two, idle `in_use` returns to zero, and normal sweeps continue without
memory-ceiling restarts. Also test real live/offline transitions, manual
checks, roster changes and a redeploy. Keep the existing emergency memory
ceiling during this observation period.

This patch does not change Neon polling frequency, quotas, retention,
notification delivery guarantees, upload support, or role policy.
