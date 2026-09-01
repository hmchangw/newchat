# bot-room-service — Production Readiness Review

**Service:** `bot-room-service` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

Boundaries are genuinely good — narrow consumer-defined `RoomStore`/`RoomKeyStore`, `pkg/subject` builders throughout, `pkg/outbox.Publish` for all federation, `pkg/shutdown.Wait` with correct ordering — and the remove/key-rotation test suite is the service's strongest work.

The problem is that it writes into collections four other services read, and **its subscription documents have a different shape from every other writer's**. It omits `joinedAt` and `roles`, which room-service's `member.list` projects and paginates on. And for channel members it sets `siteId` to the **member's** home site rather than the room's — while user-service's `subscription.list` groups rows by `sub.SiteID` to fetch room metadata *from that site*. The DM and owner paths get it right, which is what makes the channel path a bug rather than a convention.

Alongside that: **every membership RPC is a serial per-user N+1** with no batch cap; the room-key fan-out is O(room size) serial publishes on an unbounded roster load inside a 10 s deadline; and **both deferred safety nets run on the request context they were meant to survive** — the failure they exist for is precisely the one that exhausts the budget first. Coverage is 49.0%, with the entire Mongo layer and every DM error path at zero.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 12 | 20 | 16 | 7 | **56** |

---

## 2. Go code quality — 4 / 5

Idiomatic, lint-clean Go with correct `errcode` tiering, `pkg/subject` builders throughout and no logging violations; the defects are two real correctness-adjacent lapses plus a cluster of small CLAUDE.md deviations.

### Findings
- `medium` — the two deferred safety nets (subauthcache bust, fallback key rotation) run on the request context `c`, which `natsrouter.DefaultGuarded` bounds with `REQUEST_TIMEOUT` (10s default, `main.go:58,80`) — `handler.go:436` and `handler.go:446-454`
  Both nets exist for the case where a mid-batch failure leaves deletes committed. The most likely cause of that failure is a slow Mongo, which is also what trips the guard deadline — so exactly then, `BustSubs` and `rotateAndFanOut` fire on an already-cancelled ctx and silently do nothing. `context.WithoutCancel(c)` + a short fresh timeout is the fix.
- `medium` — the sysmsg dedup id can never dedup a retry, contradicting its own comment — `sysmsg.go:385-388`
  The suffix is a fresh wall clock on every invocation (`handler.go:277` `create:%d` from `createdAt`, `:371` `add:%d`, `:539` `h.now().UnixMilli()`), so `Nats-Msg-Id` differs per attempt and a client retry emits a second system message. Derive it from something stable (roomID+sorted member ids, or the caller's request id).
- `low` — bare `return err`, explicitly prohibited by CLAUDE.md §3 — `handler.go:606`
  `roomkeystore.CommitRotation`'s error surfaces with no `rotateAndFanOut`/roomID frame.
- `low` — bare `return nil, err` on federation infra errors — `handler.go:172`, `handler.go:352`, `handler.go:531`
  These are `outbox.Publish`/marshal failures, not typed `errcode`, so they should be wrapped ("federate member added for room %s: %w"). (The `parseIdentity`/`loadRoomAndAssertOwner` passthroughs at `:105,:185,:299,:407,:303,:410` are correctly left unwrapped — they carry `*errcode.Error`.)
- `low` — no `//go:generate mockgen` in `store.go` and no `mock_store_test.go`; tests use hand-written fakes (`handler_test.go:24`, `roomkey_test.go:14` — whose comment admits "bot-room-service has no gomock/mockgen infrastructure") — `store.go:198,229`
  Contra CLAUDE.md §1/§4, and unlike the sibling `room-worker/store.go:23`. Hand fakes drift silently when `RoomStore`/`RoomKeyStore` gains a method.
- `low` — `Room`, `Participant`, `Subscription` carry no `json`/`bson` tags — `store.go:246,259,272`
  CLAUDE.md §3 requires both. Every field is instead hand-mapped in `store_mongo.go` (`participantBSON:193`, the `$setOnInsert` literal at `:103-111`, the anonymous decode structs at `:72-81,:132-136`), so a field added to `Subscription` compiles and silently never persists.
- `low` — the same file encodes the same embedded participant two different ways: rooms.u as `id`/`username` (`store_mongo.go:194-196`) vs subscriptions.u as `_id`/`account` (`store_mongo.go:106`), undocumented — `store_mongo.go:193`
  No in-repo consumer reads the rooms.u `id`/`username` form; if it is a legacy-stack shape it needs the one-line comment `roomTypeChannel`/`roomTypeDM` got at `handler.go:27`.
- `low` — audit-coverage gap, not a service defect: gosec and the repo-owned semgrep rules are clean repo-wide, but `govulncheck` and the semgrep registry packs could not run (egress blocked). Dependency-CVE exposure for this service is unverified.
- `nitpick` — `added := []string{}` / `newAccounts := []string{}` / `removed := []string{}` — `handler.go:312-313,421-422`
  Non-idiomatic vs `var x []string`, but here load-bearing: the response marshals `[]` rather than `null`. Worth one comment so a future cleanup doesn't "fix" it into a wire change.

### Recommendations
- `medium` — Wrap both deferred nets in `ctx := context.WithoutCancel(c)` with an independent 5s timeout so they survive a guard-deadline abort; assert it with a test that cancels the handler ctx before returning.
- `medium` — Make the sysmsg dedup suffix deterministic across retries (hash of roomID + sorted affected user ids + msgType) and correct the `sysmsg.go:385` comment to match whatever it actually guarantees.
- `low` — Wrap the four bare error returns (`handler.go:606,172,352,531`) with the operation this function was performing; leave the `*errcode.Error` passthroughs alone.
- `low` — Add `//go:generate mockgen -destination=mock_store_test.go -package=main . RoomStore,RoomKeyStore` to `store.go` and replace the hand fakes, matching `room-worker/store.go:23`.
- `low` — Put `json`/`bson` tags on `Room`/`Participant`/`Subscription` and marshal the structs directly instead of hand-built `bson.M`, eliminating the silent field-drop class of bug.
- `low` — Document (or unify) the rooms.u `id`/`username` vs subscriptions.u `_id`/`account` divergence in `participantBSON`.
- `low` — Track the blocked `govulncheck`/registry-pack scans as an environment issue so this service's dependency posture gets verified in CI rather than assumed.
