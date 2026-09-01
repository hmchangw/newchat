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

---

## 3. Architecture — 3 / 5

Boundaries are genuinely good — narrow consumer-defined `RoomStore`/`RoomKeyStore`, `pkg/subject` builders throughout, `pkg/outbox.Publish` for all federation, `pkg/shutdown.Wait` with correct ordering — but the service is missing the mandated `bootstrap.go` stream contract and CI pipeline, re-declares a shared config knob, and has no mockgen infrastructure.

### Findings
- `high` — No `bootstrap.go` and no `Bootstrap bootstrapConfig` field, yet the service publishes to two JetStream streams: `OUTBOX-{siteID}` via `outbox.Publish` (`bot-room-service/handler.go:646`, `outboxpublish.go:36`) and `BOT-MESSAGES-CANONICAL-{siteID}` via `subject.BotCanonicalCreated` (`bot-room-service/sysmsg.go:66`). CLAUDE.md: "New services that interact with JetStream MUST follow this convention." Both sibling producers verify-when-disabled so a misprovisioned deploy fails at startup (`bot-message-handler/bootstrap.go:34`, `room-worker/bootstrap.go:56`). Here a missing stream surfaces as a per-request failure returned to the bot on member add/remove, and as a swallowed `slog.Warn` for sysmsgs (`sysmsg.go:67`).
- `high` — `deploy/docker-compose.yml:21` hardcodes `ROOM_KEY_RETIRED_TTL=30m` (and `:20` `ROOM_KEY_GRACE_PERIOD=24h`) instead of `${ROOM_KEY_RETIRED_TTL:-30m}`. Every cohort peer uses the overridable form: `room-service/deploy/docker-compose.yml:35`, `room-worker/deploy/docker-compose.yml:27`, `broadcast-worker/deploy/{user,bot}/docker-compose.yml:36`. CLAUDE.md requires all three key-writing services be configured *identically*; an operator raising the shared var moves the other three and silently leaves this one short, expiring retired versions its peers still consider resolvable — `key.get` then permanently fails for messages already on the wire.
- `medium` — No `deploy/azure-pipelines.yml`. 29 of 37 service `deploy/` dirs have one (`room-service/deploy/azure-pipelines.yml`, `room-worker/deploy/azure-pipelines.yml`); CLAUDE.md §5 "When Creating Services" mandates it. The service has no CI/CD build path.
- `medium` — `MAX_CONCURRENCY`/`REQUEST_TIMEOUT` re-declared locally (`main.go:57-58`) and hand-assembled into a `natsrouter.GuardConfig` at `main.go:80`, instead of mounting the owning package's bundle as a named field. `pkg/natsrouter/guard.go:12,21` documents the mount, and `room-service/main.go:61`, `room-worker/main.go:52`, `media-service/config.go:110`, `search-service/main.go:107` all use it. This is exactly the "declared once in the owning package, never re-declare the env tag and `envDefault`" rule; the local default (200) already diverges from the package default (256).
- `medium` — `store.go` carries no `//go:generate mockgen` directive and there is no `mock_store_test.go`; the store is faked by hand (`handler_test.go:24`, `roomkey_test.go:14`, whose own comment states "bot-room-service has no gomock/mockgen infrastructure"). 25 services ship generated mocks. Hand fakes silently absorb interface changes — adding a `RoomStore` method compiles fine while the fake keeps the old behaviour.
- `low` — Two handler dependencies are set after construction rather than injected: `h.valkey = subValkey` (`main.go:143`) and `h.sysmsgPub = jsPublishAdapter{js: js}` (`main.go:145`). The valkey pattern matches siblings (`room-worker/main.go:280`), but `sysmsgPub` does not, and its nil guard (`sysmsg.go:36`) turns a wiring omission into a silent loss of every system message instead of a compile error.
- `low` — `keySender *roomkeysender.Sender` is a concrete pointer in the handler struct and constructor (`handler.go:65,76`), violating "accept interfaces". `fanOutKey` (`handler.go:487`) therefore cannot be exercised without a live NATS conn. Same shape as `room-worker/handler.go:84`, so this is a fleet pattern rather than a novel defect.
- `nitpick` — `store.go:74` calls `Participant` "the shared shape stored on rooms.u + subscriptions.u", but the two writers use different key sets: `participantBSON` emits `id`/`username` (`store_mongo.go:195-196`) while `UpsertSubscription` emits `_id`/`account` (`store_mongo.go:106`). No comment explains the divergence, and every repo reader of the subscription doc keys on `u._id`/`u.account` (`room-service/store_mongo.go:743`).

### Recommendations
- `high` — Add `bot-room-service/bootstrap.go` with `bootstrapConfig{Enabled bool \`env:"STREAMS" envDefault:"false"\`}` and a `bootstrapStreams(ctx, js, siteID, enabled)` that sets only `Name + Subjects` from `stream.Outbox`/`stream.BotMessagesCanonical` when enabled and calls `js.Stream(...)` to fail fast when disabled; wire it in `run()` before `newHandler` and set `BOOTSTRAP_STREAMS=${BOOTSTRAP_STREAMS:-true}` in the compose file.
- `high` — Change `deploy/docker-compose.yml:20-21` to `${ROOM_KEY_GRACE_PERIOD:-24h}` / `${ROOM_KEY_RETIRED_TTL:-30m}` so the cohort moves together under one operator override.
- `medium` — Add `deploy/azure-pipelines.yml` modelled on `room-service/deploy/azure-pipelines.yml`.
- `medium` — Replace `main.go:57-58,80` with `Guard natsrouter.GuardConfig` mounted as a named field; if 200 is deliberately below the package default, set it via the deploy env, not a re-declared `envDefault`.
- `medium` — Add `//go:generate mockgen` to `store.go` for `RoomStore` and `RoomKeyStore`, generate `mock_store_test.go`, and migrate `fakeStore`/`fakeKeyStore` onto it.
- `low` — Move `sysmsgPub` (and ideally `valkey`) into `newHandler`'s parameter list so the wiring is checked by the compiler.
- `low` — Introduce a one-method `keySender` interface in `handler.go` so `fanOutKey`'s per-recipient failure path is unit-testable.

---

## 4. Test coverage — 1 / 5

Coverage is 49.0% (410 stmts) — below the 60% CLAUDE.md floor, so the dimension is floored at 1; the remove/key-rotation suite is genuinely good, but the whole Mongo store, the identity gate, and every DM/create error path have zero coverage.

### Findings
- `critical` — 49.0% statement coverage (410 stmts), far under the CLAUDE.md §4 80% floor and under 60% — `scratchpad/pr/coverage_by_service.txt`
- `high` — the entire Mongo layer is 0%: `InsertRoom`, `FindRoom`, `FindUser`, `participantBSON` have neither unit nor integration tests — `bot-room-service/store_mongo.go:47,71,150,193`
  `integration_test.go` covers only `EnsureIndexes`/`UpsertSubscription`/`DeleteSubscription`/`ListRoomMemberAccounts`. The `ErrDuplicate`/`ErrNotFound` sentinels every handler branches on are produced only by the hand-written fake, so nothing proves `mongo.IsDuplicateKeyError`/`ErrNoDocuments` actually map to them.
- `high` — `parseIdentity`'s three rejection branches are all 0% — `bot-room-service/handler.go:696,701,705`
  This is the identity gate for all five bot RPCs; missing header, malformed JSON, and empty id/account (all `errcode.BotInvalidHeader`) are untested, as is every caller's error return (`handler.go:104,183,298`).
- `high` — the "room already exists" path is untested end to end: `ErrDuplicate → errcode.Conflict(BotRoomExists)` is 0% — `bot-room-service/handler.go:206-210`
  Paired with `InsertRoom` at 0%, the idempotent-retry contract for create-room has no coverage at any layer.
- `high` — every `handleDMEnsure` error path is 0% — `bot-room-service/handler.go:107-114,117-122,138-140,155-157,171-173`
  Uncovered: `BotContentInvalid` (empty target), `BotCannotDMSelf`, `BotDMTargetNotFound`, both subscription-upsert failures, and the remote-target federation failure — i.e. the whole non-happy half of a client-facing RPC.
- `medium` — no mockgen infrastructure: `store.go` has no `//go:generate mockgen` directive and there is no `mock_store_test.go`; the service uses hand-written fakes — `bot-room-service/store.go:11`, `bot-room-service/roomkey_test.go:11-13`
  CLAUDE.md §4 mandates `go.uber.org/mock` generated mocks in `mock_store_test.go`; the comment acknowledges the deviation rather than fixing it.
- `medium` — shared mutable state across tests: package-level `testKeyStore` / `testKeySender` wrapping one `&fakePublisher{}` that appends into shared slices — `bot-room-service/roomkey_test.go:76-79,54-62`
  Violates "no shared mutable state between tests"; unsynchronized slice appends would race the moment any test adds `t.Parallel()`.
- `medium` — effectively no table-driven tests: 1 `t.Run` across ~53 test functions — `bot-room-service/handler_test.go` (0 `t.Run`), `handler_remove_federation_test.go`, `bustsub_test.go`
  CLAUDE.md requires table-driven handler tests "covering all documented scenarios"; `parseIdentity`'s three variants and the create/get validation matrix are textbook tables.
- `medium` — `Register` is 0%, so nothing asserts the five subjects/patterns — `bot-room-service/handler.go:87-98`
  `handleAdd`/`handleRemove` read `c.Params.Get("roomID")` (`handler.go:289,397`); if the token in `pkg/subject/bot.go:55` drifts, both silently return `BadRequest` and no test fails.
- `medium` — `jsPublishAdapter.PublishWithMsgID` is 0% — `bot-room-service/sysmsg.go:26-31`
  The `jetstream.WithMsgID` dedup that `emitSysmsg` relies on to "defeat double-emit on retry" is unverified in unit *and* integration tests.
- `low` — `loadRoomAndAssertOwner`'s `FindRoom` failure (both `BotRoomNotFound` and infra) is 0% — `bot-room-service/handler.go:641-645`; `roomTypeToModel`'s `roomTypeDM → RoomTypeBotDM` case is 0% — `handler.go:379-380`; `handleCreate`'s self-in-`Members` dedup `continue` is 0% — `handler.go:240-241`; `rotateAndFanOut`'s "list survivors" failure is 0% — `handler.go:590-592`.
- `nitpick` — `parsePeers` is 0% despite being a pure function — `bot-room-service/main.go:172-185`; `ALL_SITE_IDS` self-exclusion and blank-token trimming are unverified.

### Recommendations
- `critical` — Add integration tests for `InsertRoom` (duplicate `_id` → `ErrDuplicate`), `FindRoom`/`FindUser` (miss → `ErrNotFound`, and the projected field set incl. `createdByBot`/`lastMsgAt`), and assert the `rooms.u` shape `participantBSON` writes, since inbox-worker/room-service read `u._id`/`u.account`.
- `high` — Table-drive `parseIdentity` over its three rejections, asserting the `errcode` category and `BotInvalidHeader` reason; then add one error-path case per handler for the `parseIdentity` return.
- `high` — Cover `handleDMEnsure` fully: empty target, self-DM, target-not-found, both upsert failures, and remote-target federation failure (assert `BotCannotDMSelf`/`BotDMTargetNotFound` reasons, which clients branch on).
- `high` — Cover create-room's `ErrDuplicate → BotRoomExists` conflict and the `BotUnsupported` orgs rejection (`handler.go:189-192`), plus `BotMemberNotFound` in the seed loop (`handler.go:244-250`).
- `medium` — Replace the hand-written `fakeStore`/`fakeKeyStore` with `mockgen` output in `mock_store_test.go` and add the `//go:generate mockgen` directive to `store.go`, per CLAUDE.md.
- `medium` — Delete the package-level `testKeyStore`/`testKeySender`; construct doubles per test (or per subtest) so state cannot leak.
- `medium` — Add a `Register` test that drives a router and asserts all five subjects resolve, including that the add/remove patterns bind `roomID`; add an integration test for `PublishWithMsgID` proving a repeat `msgID` is deduped by JetStream.
