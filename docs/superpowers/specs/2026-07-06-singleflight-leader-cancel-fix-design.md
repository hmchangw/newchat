# Singleflight leader-context cancellation fix — design

**Date:** 2026-07-06
**Branch:** `ds-feat/fix_singleflight`
**Status:** Approved (pending spec review)
**Author:** include2dev (co-authored with Claude)

## 1. Background

Every LRU + `singleflight` cache in the repo coalesces concurrent misses on the
same key into a single load, and hands that load's `(value, error)` to every
waiting caller. In several caches the load closure captures the **leader**
caller's request `context.Context`. When the leader's context is cancelled
(client timeout/disconnect, parent cancel, an errgroup sibling failing), the
load returns `context.Canceled` / `DeadlineExceeded`, and **every coalesced
caller receives that error — even callers whose own context is still valid.**
This is "leader-context cancellation poisoning" (P1).

A 9-site audit classified each LRU+singleflight cache:

| # | Site | `Do`/`DoChan` | ctx → load | P1 | Status |
|---|------|--------------|-----------|:--:|--------|
| 1 | `media-service/cache.go` | DoChan + select | `WithoutCancel` | safe | reference |
| 2 | `pkg/atrest/cipher.go` | Do | `WithTimeout(WithoutCancel)` | safe | reference |
| 3 | `broadcast-worker/keycache.go` | DoChan + select | `WithoutCancel` | safe | reference |
| 4 | `pkg/emoji/cache.go` | Do | leader | **present** | fix |
| 5 | `pkg/roommetacache/roommetacache.go` | Do | leader | **present** | fix |
| 6 | `pkg/userstore/cache.go` | Do | leader | **present** | fix |
| 7 | `notification-worker/members.go` | Do | leader | **present** | fix |
| 8 | `message-gatekeeper/subcache.go` | Do | leader | **present** | fix |
| 9 | `history-service/internal/readcache/readcache.go` | Do | leader | **present** | fix |

**6 of 9 sites carry the bug; 3 already implement the correct pattern and have
leader-cancel regression tests.** The fix is to bring the 6 affected sites onto
the pattern the 3 safe sites already prove in production.

Severity is bounded/self-healing: errors are never written to the LRU and
`singleflight` auto-drops the key when the load returns, so P1 is transient
availability wobble in the concurrent-miss window, not corruption or persistent
poisoning. Worst blast radius: `notification-worker`, `history` room-times
(keyed by `roomID` alone), and the message-hot-path caches (`userstore`,
`roommetacache`).

## 2. Scope

**In scope:** fix P1 at the 6 affected sites.

**Out of scope (explicitly not touched):**

- **P2** (shared mutable value / aliasing): several caches hand out a shared
  pointer/slice without a defensive copy. There is no live data race today (all
  current callers are read-only). Tracked as a separate follow-up.
- The 3 already-safe sites (`media-service`, `atrest`, `broadcast-worker`).
- Any shared/generic cache helper extraction.
- Client API: no `chat.user.*` handler or `pkg/model` request/response/event
  struct changes, so `docs/client-api.md` and its derived views are **not**
  affected.

## 3. The standard fix: `DoChan` with a detached, time-bounded load

Every affected site is converted from blocking `Do` to `DoChan` + a per-caller
`select`, with the load running on a **detached, time-bounded** context.

**Before:**

```go
v, err, _ := c.sf.Do(key, func() (any, error) {
    return c.load(ctx, key) // leader's ctx: his cancel fails every coalesced caller
})
```

**After:**

```go
ch := c.sf.DoChan(key, func() (any, error) {
    // Detach from any single caller's cancellation, but keep a hard upper bound
    // so a hung backend can't leak the flight goroutine or pin the sf key.
    fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
    defer cancel()
    v, err := c.load(fetchCtx, key)
    if err != nil {
        return nil, err // not added to the LRU; singleflight auto-forgets the key
    }
    c.lru.Add(key, v)
    return v, nil
})
select {
case res := <-ch:
    if res.Err != nil {
        return zero, res.Err
    }
    return res.Val.(V), nil
case <-ctx.Done():
    // This caller honors its OWN deadline; the shared flight keeps running and
    // still populates the cache for the others.
    return zero, ctx.Err()
}
```

Why each part:

- **`DoChan` + `select`** — each caller waits on the shared result *or* its own
  `ctx.Done()`, whichever comes first. A caller that gives up returns its own
  `ctx.Err()` and never poisons the shared flight. `DoChan`'s result channel is
  buffered (size 1), so a caller bailing on `ctx.Done()` cannot block the
  singleflight goroutine — no goroutine leak.
- **`context.WithoutCancel(ctx)`** — the shared load is decoupled from any one
  caller's cancellation, so the leader's timeout/disconnect can no longer fail
  the followers. `WithoutCancel` preserves context *values* (trace/request IDs
  keep flowing) while dropping cancellation and deadline.
- **`context.WithTimeout(..., fetchTimeout)`** — because `WithoutCancel` also
  drops the deadline, we re-impose an explicit upper bound (the `atrest`
  precedent). This closes the only real risk the detachment introduces: a hung
  Mongo call leaking a detached goroutine and pinning the singleflight key.

## 4. Per-site changes

| Site | Change | Notes |
|------|--------|-------|
| `pkg/emoji/cache.go` | `CustomEmojiExists` (`Do` at `:84`) → DoChan+select | Returns `bool`; zero value `false`. Keep the in-closure `lru.Get` re-check. **Pilot site.** |
| `pkg/roommetacache/roommetacache.go` | `Get` (single `Do`) → DoChan+select | Closure calls `c.loader(ctx, roomID)` which internally does L2 Valkey → Mongo; the single `fetchTimeout` bounds the whole load. Returns value `Meta`. |
| `pkg/userstore/cache.go` | **Two** sites: `FindUserByID` (`:75`) and `FindUserByAccount` (`:103`) → DoChan+select each | Both must be changed. Returns `*model.User`; zero value `nil`. |
| `notification-worker/members.go` | `GetMembers` (`Do` at `:46`) → DoChan+select | Closure has **three** ctx uses (Valkey `Get`, Mongo `load`, Valkey `Set`) — all switch to the detached `fetchCtx`. Returns `[]roomsubcache.Member`. |
| `message-gatekeeper/subcache.go` | Single `Do` (`:77`) → DoChan+select | Closure calls `c.Store.GetSubscription(ctx, …)`. Callers are deadline-free JetStream workers (P1 bounded here), converted for uniformity. |
| `history-service/internal/readcache/readcache.go` | Generic `getOrLoad[V]` (`Do` at `:61`) → DoChan+select | **One change fixes all three caches** (subscription, room-times, min-seen). Uses `var zero V` on the error/cancel paths. |

Each affected package gains a small unexported const:

```go
const fetchTimeout = 10 * time.Second
```

## 5. Decisions

- **Detached context = `context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)`**,
  applied uniformly to all 6 sites — including the Valkey-touching
  ones (`roommetacache`, `notification-worker`) even though go-redis already has
  a built-in default read timeout, so the pattern is identical everywhere.
- **`fetchTimeout = 10s`, a package-level const** (mirrors `atrest`'s
  `dekFetchTimeout` const, not env config). 10s is comfortably above healthy
  single-doc Mongo/Valkey latency (single-digit ms) yet bounds a hung load.
  Promotable to env config later if ops needs per-deployment tuning; not needed
  now.
- **No `sf.Forget`.** `singleflight` removes the in-flight key once the load
  returns, and errors/negatives are never added to the LRU, so there is no
  residual entry to forget. Matches all 3 safe sites.
- **Metrics.** The caller-cancel (`<-ctx.Done()`) path records `metrics.Error`,
  mirroring `broadcast-worker`. Any hit/miss count skew under coalescing is
  pre-existing and cosmetic — not addressed here.
- **`context.WithoutCancel` requires Go 1.21+.** Repo is Go 1.25 — fine.

## 6. Testing (TDD, red → green)

Each affected site gets two focused regression tests, modeled on the existing
tests in the 3 safe sites (`TestEIDCache_WinnerCancelDoesNotPoisonWaiters`,
`TestCachedKeyProvider_CallerCtxCancelReturnsCtxErr`,
`TestCipher_Singleflight_LeaderCancelDoesNotPoisonWaiters`):

1. **`…_LeaderCancelDoesNotPoisonWaiters`** — two callers coalesce on one key;
   the leader's ctx is cancelled mid-load; assert a follower with a valid ctx
   **still receives the value** and the entry is cached. Written first: it
   **FAILS against the current `Do` code** (red), proving the bug, then passes
   after the fix (green).
2. **`…_CallerCancelReturnsCtxErr`** — a caller whose own ctx cancels returns
   `ctx.Err()`, and the shared load is **not** aborted (it completes and
   populates the cache).

Rules:

- Deterministic synchronization only — a controllable fake loader/inner that
  blocks on a channel until signaled, plus `sync.WaitGroup`/channels to force
  coalescing. **No `time.Sleep`** (CLAUDE.md).
- `history` tests target the generic `getOrLoad` (covers all three caches) plus
  a per-cache smoke.
- Existing dedup/collapse tests are retained. Any existing test that encoded the
  *old* (buggy) behavior is updated — expected in a bug fix.
- Run with `-race` (the Makefile default).

## 7. Risks & mitigations

| Risk | Mitigation |
|------|-----------|
| Detached load loses the ctx deadline → a hung backend leaks a goroutine / pins the sf key | `WithTimeout` re-imposes a 10s bound; Valkey is additionally self-bounded by go-redis defaults |
| Behavior change: caller now bails on its own ctx instead of blocking on the leader | This is the intended fix and an improvement (honors deadlines; cleaner graceful shutdown). Loaders are read-only, so a detached load completing after the requester left has no side effects |
| An existing test asserted the old behavior | Full `-race` suite catches it; update the test to the corrected behavior |
| Mechanical slip (missed ctx swap, only 1 of userstore's 2 sites, wrong generic zero value) | Per-site regression tests + review + **pilot-first rollout** |
| New data race | None introduced — no new shared state; the LRU is already concurrency-safe; P2 is untouched |
| A detached load can complete and write the cache after an `Invalidate` lands, briefly resurrecting stale data | Bounded by `fetchTimeout` (≤10s) then the entry's TTL; self-healing; inherent to cache-aside + singleflight; affects sites with `Invalidate` (emoji, notification-worker) |

## 8. Rollout

1. **Pilot `pkg/emoji`** end-to-end: write the two red tests, apply the fix,
   green, self-review the diff. Validates the exact template on the smallest
   site (single `Do`, `bool` return).
2. Replicate the validated template to the remaining 5 sites, one commit each:
   `roommetacache`, `userstore` (both methods), `notification-worker`,
   `message-gatekeeper`, `history readcache` (generic `getOrLoad`).
3. One branch (`ds-feat/fix_singleflight`), one PR.

## 9. Verification

- `make test SERVICE=<name>` per affected service/package, then `make test`
  (race detector on).
- `make lint` and `make sast` (blocking CI gate) before push.
- `make generate` **not** required — no store interface changes.
- No `docs/client-api.md` change — no client-facing surface touched.
