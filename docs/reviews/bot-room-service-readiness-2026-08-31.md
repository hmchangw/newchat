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
- `high` — Add `bot-room-service/bootstrap.go` with `bootstrapConfig{Enabled bool \`env:"STREAMS" envDefault:"false"\`}` and a `bootstrapStreams(ctx, js, siteID, enabled)`, wired in `run()` before `newHandler`. **The two streams are not symmetric, and an earlier draft of this report got one of them wrong by treating them as if they were.** `OUTBOX-{siteID}` must be **verified only, in both modes** — never created here: CLAUDE.md gives OUTBOX a single owning service, and `outbox-worker/bootstrap.go:41` is the repo's only `stream.Outbox` creator. `BOT-MESSAGES-CANONICAL-{siteID}` follows the ordinary shape — `CreateOrUpdateStream` with only `Name + Subjects` from `stream.BotMessagesCanonical` when enabled, `js.Stream(...)` when disabled — matching `bot-message-handler/bootstrap.go:25` and `bot-message-worker/bootstrap.go:24`, which already do exactly that with the same schema, so the call is idempotent rather than a competing definition. Set `BOOTSTRAP_STREAMS=${BOOTSTRAP_STREAMS:-true}` in the compose file, and note that a fresh-NATS dev standup then needs `outbox-worker` alongside it (the compose currently declares neither) — which is the point of verify-only: the dependency fails loudly at startup instead of at a bot's first member add.
- `high` — Change `deploy/docker-compose.yml:20-21` to `${ROOM_KEY_GRACE_PERIOD:-24h}` / `${ROOM_KEY_RETIRED_TTL:-30m}` so the cohort moves together under one operator override.
- `medium` — Add `deploy/azure-pipelines.yml` modelled on `room-service/deploy/azure-pipelines.yml`.
- `medium` — Replace `main.go:57-58,80` with `Guard natsrouter.GuardConfig` mounted as a named field; if 200 is deliberately below the package default, set it via the deploy env, not a re-declared `envDefault`.
- `medium` — Add `//go:generate mockgen` to `store.go` for `RoomStore` and `RoomKeyStore`, generate `mock_store_test.go`, and migrate `fakeStore`/`fakeKeyStore` onto it.
- `low` — Move `sysmsgPub` (and ideally `valkey`) into `newHandler`'s parameter list so the wiring is checked by the compiler.
- `low` — Introduce a one-method `keySender` interface in `handler.go` so `fanOutKey`'s per-recipient failure path is unit-testable.

---

## 4. Test coverage — 1 / 5

Coverage is 49.0% (410 stmts) — below the 60% line this audit scores as `critical` (CLAUDE.md §4 requires 80%), so the dimension is floored at 1; the remove/key-rotation suite is genuinely good, but the whole Mongo store, the identity gate, and every DM/create error path have zero coverage.

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

---

## 5. Maintainability — 3 / 5

Layout, naming and store-interface discipline are clean and the comments genuinely explain WHY, but `handleRemove` has outgrown a single function, and three key-lifecycle helpers plus `parseIdentity` are copy-pasted from sibling services and have already drifted.

### Findings
- `high` — `handleRemove` is 150 lines carrying four interlocking invariants (batch pre-validation, a deferred `BustSubs`, a deferred rotation safety-net gated on a `rotated` flag, and a deferred-until-after-rotation federation queue), 64 of those 150 lines being comment — `bot-room-service/handler.go:396-542`
  Every invariant is correct and documented, but they are only *readable* via the comments; the ordering contract is invisible in the code. Adding a fifth step (thread cleanup, audit event) means re-deriving all four orderings by hand.

- `medium` — `keyPairOrHeal`, `rotateAndFanOut` and `fanOutKey` are re-implementations of `room-worker`'s, and have already diverged in behaviour, not just style — `bot-room-service/handler.go:546-613` vs `room-worker/handler.go:1702-1722`, `room-worker/handler.go:346-359`, `room-worker/handler.go:2571`
  Divergences: bot's `keyPairOrHeal` tolerates `roomkeystore.ErrNoCurrentKey` from `Get` while room-worker's treats *every* `Get` error as fatal; bot emits no `roomkeymetrics.RecordStoreError`; bot's `fanOutKey` is serial and emits no `RecordFanoutErrors`, so bot-driven key fan-out failures are invisible on the dashboards that cover the user pipeline. `pkg/roomkeystore` already hosts `CommitRotation`/`GenerateKeyPair`, so the extraction point exists.

- `medium` — `parseIdentity` is byte-identical (bar the doc comment) to `bot-message-handler`'s — `bot-room-service/handler.go:694-710` and `bot-message-handler/handler.go:206-222`
  Both validate the same `model.HeaderBotIdentity` contract with the same `errcode.BotInvalidHeader` reason. A third bot service will copy it a third time; a validation change (e.g. requiring `AppID`) has to be found in N places.

- `medium` — dead config plumbing: `ALL_SITE_IDS` → `parsePeers` → `handler.allSiteIDs` is stored and never read — `bot-room-service/handler.go:61,79`, `bot-room-service/main.go:29,172-185`
  The only consumer is the startup `slog.Info` line (`main.go:150`). The field's own comment claims it drives "per-destination outbox federation", but federation destinations come from `u.SiteID` (`handler.go:262,348,488`). A misleading knob operators will set and expect to matter.

- `medium` — the sysmsg dedup comment claims more than the code delivers — `bot-room-service/sysmsg.go:61` vs `handler.go:539`
  "defeats double-emit on retry", but the suffix is a wall-clock read: `handleRemove` calls `h.now()` a *second* time at emit (`handler.go:539`) while create/add reuse the request-scoped timestamp (`:277`, `:371`). No variant survives a client retry, since each request mints a new `now()`. Inconsistent derivation plus a WHY-comment that is wrong is worse than no comment.

- `low` — the "org expansion not yet supported" guard is triplicated with three different field names (`req.Orgs`, `req.OrgIDs`, `req.OrgIDs`) — `handler.go:189-192,293-296,401-404`
  Concrete measure of "how hard is one new feature": shipping org expansion touches three handlers, three response builders, and `model.MembersAdded.Orgs`.

- `low` — a 13-entry type-alias block re-labels `pkg/model` types into `package main`, including a rename (`OwnerResp = model.BotOwnerResp`) — `handler.go:33-46`
  Cross-service grep for `BotOwnerResp` misses every use site here.

- `nitpick` — `captureOutbox.mu struct{ int32 }` is a field named like a mutex that is an anonymous struct, atomically incremented at `handler_test.go:62` and never read, while the adjacent `c.calls` append it appears to guard is genuinely unsynchronized — `handler_test.go:56-63`

- `nitpick` — `store.go` has no `//go:generate mockgen` directive and the package hand-rolls `fakeStore`/`fakeKeyStore`/`captureOutbox`; `roomkey_test.go:13` states this as settled fact rather than a deviation from CLAUDE.md §4 — `bot-room-service/store.go`, `roomkey_test.go:11-19`

### Recommendations
- `high` — Split `handleRemove` into `collectRemovals` (validate + delete loop, returns `[]memberRemoval` + `removed`/`removedAccounts`) and a small orchestrator that owns the two defers and the rotate→federate ordering. The four invariants then live in three named units instead of one 150-line body, and the comment block shrinks to a one-line contract per unit.
- `medium` — Lift `keyPairOrHeal` + `rotateAndFanOut` + `fanOutKey` into `pkg/roomkeystore` (or a `pkg/roomkeyops`) beside the existing `CommitRotation`, with `roomkeymetrics` wired once. Reconcile the `ErrNoCurrentKey` handling divergence explicitly during the move — pick one semantic and test it.
- `medium` — Move `parseIdentity` to `pkg/model` (or `pkg/botidentity`) next to `HeaderBotIdentity`, and have both bot services call it.
- `medium` — Delete `ALL_SITE_IDS`, `parsePeers`, and `handler.allSiteIDs`, or wire them to something real. Keeping a documented-but-unread knob is the more expensive option.
- `medium` — Make the sysmsg dedup suffix request-derived (e.g. the NATS message ID or a caller-supplied idempotency key) so the comment becomes true, and use one derivation across all three call sites.
- `low` — Extract the org-expansion guard into one `rejectOrgExpansion(orgs []string) error` helper so the eventual feature has a single seam.
- `nitpick` — Drop `captureOutbox.mu` and the `atomic.AddInt32`; assert on `len(c.calls)` instead.

---

## 6. Integration — 3 / 5

Federation plumbing is correct and contract-aligned (subject builders, `outbox.Publish` on the ordered lane, dedup IDs, inbox naming/LastMsgAt), but the subscription documents this service writes diverge from every other writer's shape, and one shared-config knob is pinned in a way that defeats the retired-key TTL contract.

### Findings
- `high` — Subscriptions written here omit `joinedAt` and `roles`; every other writer sets both (`room-worker/handler.go:1510`, `inbox-worker/handler.go:322`). room-service's `member.list` projects `roles`+`joinedAt` and paginates on the `{roomId,joinedAt,_id}` index — `bot-room-service/store_mongo.go:101-109`, `room-service/store_mongo.go:140,745-750`.
  Bot-added members sort ahead of everyone (missing key sorts first) and render a zero join time.
- `high` — Channel member subscriptions get `siteId` = the *member's* home site, not the room's site — `bot-room-service/handler.go:254`, `:335`. The convention is the room's site (`room-worker/handler.go:1170` `SiteID: room.SiteID`; `inbox-worker/handler.go:~310` `SiteID: event.SiteID`), and user-service's `subscription.list` groups rows by `sub.SiteID` to fetch room meta from that site (`user-service/service/subscriptions.go:237,789-795`, `user-service/service/apps.go:91`). The DM/owner paths correctly use `h.siteID` (`handler.go:153,163,215`), so the service is inconsistent with itself.
- `medium` — `ROOM_KEY_RETIRED_TTL` is hardcoded in compose (`bot-room-service/deploy/docker-compose.yml:21`) while the three peers take an override (`room-service/deploy/docker-compose.yml:35`, `room-worker/deploy/docker-compose.yml:27`, `broadcast-worker/deploy/user/docker-compose.yml:36` all `${ROOM_KEY_RETIRED_TTL:-30m}`). Raising the fleet value leaves this service short — exactly the split-brain CLAUDE.md §Retired room keys names (`key.get` fails permanently). Go defaults do match (`main.go:42`).
- `medium` — `BotIdentity.AppID/AppName/EngName/ChineseName` are persisted into `rooms.u` and returned as the create response owner (`handler.go:196-200`, `:281`), but no producer ever populates them: BP builds identity as `{ID, Account, SiteID}` only (`botplatform-service/bot_forwarder.go:62,112-116`, `botplatform-service/dm_ensurer.go:43`). The owner enrichment and `owner.appId/appName` are dead-empty in production.
- `medium` — bot-room-service is an undocumented fifth OUTBOX producer riding the per-destination FIFO lane: `pkg/outbox/outbox.go:2` and CLAUDE.md §JetStream Streams both enumerate only room-service/room-worker/message-worker/broadcast-worker. Its events do partition correctly (`InboxMemberAdded`/`InboxMemberRemoved` ∈ `OrderedEventTypes`, `pkg/outbox/outbox.go:52-56`), so this is contract-doc drift, not a stranded type.
- `low` — `ALL_SITE_IDS` → `parsePeers` → `handler.allSiteIDs` is dead: the field is assigned (`handler.go:79`) and never read; federation destinations come from each user's `siteId`. The config comment claims it drives "per-destination outbox federation" (`main.go:29-30`), which an operator will believe.
- `low` — `handleGet` is registered on `subject.ServerBotRoomGet` (`handler.go:94-95`) with no in-repo caller — an exposed RPC whose contract nothing exercises.
- `low` — The sysmsg dedup ID is not deterministic across retries: the suffix embeds a fresh wall-clock ms (`handler.go:277,371,539`), so BP's 15s-timeout retry (`botplatform-service/bot_forwarder.go:74`) yields a different `Nats-Msg-Id`, contradicting the "defeats double-emit on retry" claim at `sysmsg.go:63`.
- `nitpick` — Federated events set `Type: "member_added"` / `"member_removed"` as string literals (`handler.go:661,683`) instead of `model.InboxMemberAdded`/`InboxMemberRemoved`, which the sibling producers use (`room-worker/handler.go:1176`).

Verified clean: `pkg/subject` builders used for every subject (no `fmt.Sprintf`); OUTBOX subject is `chat.outbox.{origin}.{dest}.{type}` via `outbox.Publish`; `Timestamp` present and set at the publish site on both federated events (`handler.go:670,685`) and on the OUTBOX envelope; JetStream `PublishMsg` + `WithMsgID` rather than core NATS (`outboxpublish.go:36`); `idgen.BuildDMRoomID` for DMs, `GenerateID` for channel rooms, `GenerateUUIDv7` for subscriptions, `GenerateMessageID` for sysmsgs; no stream creation (correctly defers to inbox/outbox/bot-message-handler owners); no `chat.user.` subject, so `docs/client-api.md` is not implicated.

### Recommendations
- `high` — Add `joinedAt` (= `createdAt`) and `roles: ["user"]` for channel rows to `UpsertSubscription`'s `$setOnInsert` (`store_mongo.go:101-109`), matching `room-worker.newSub`.
- `high` — Change `SiteID: u.SiteID` to `h.siteID` at `handler.go:254` and `:335`; the destination for federation already comes from `u.SiteID` separately, so nothing else moves.
- `medium` — Switch the compose entry to `${ROOM_KEY_RETIRED_TTL:-30m}` (and `${ROOM_KEY_GRACE_PERIOD:-24h}`) so the four services move together.
- `medium` — Either populate `AppID`/`AppName` in BP's `BotIdentity` (it has the bot session) or drop those fields from the room owner doc and `OwnerResp`; today the API advertises data it never returns.
- `medium` — Add bot-room-service to the producer list in `pkg/outbox/outbox.go`'s package doc and CLAUDE.md §JetStream Streams OUTBOX bullet.
- `low` — Delete `ALL_SITE_IDS`, `parsePeers`, and `handler.allSiteIDs`, or use them to reject a federation destination outside the configured peer set (which would also catch a peer outbox-worker has no consumer for).
- `low` — Derive the sysmsg dedup suffix from stable inputs (sorted user IDs, or the caller's request ID) instead of `now()`, or drop the comment's determinism claim.

---

## 7. Performance — 3 / 5

Clean goroutine hygiene, correct shutdown ordering and a bounded request guard, but every membership RPC is a serial per-user N+1 against Mongo with no batch cap, the room-key fan-out is unbounded inside a 10s deadline, and the two deferred recovery nets inherit that same deadline.

### Findings
- `high` — N+1 member resolution: `handleAdd` and `handleCreate` do one `FindUser` + one `UpsertSubscription` + one JetStream OUTBOX publish **per user, serially**, with no batch size cap on `req.UserIDs`/`req.Members` — `bot-room-service/handler.go:325`, `:333`, `:350`, `:243`, `:252`, `:263`; `pkg/model/bot.go:74`.
  A 200-member add is ≥400 sequential Mongo round trips plus 200 blocking PubAcks inside a 10s `REQUEST_TIMEOUT` (`main.go:57`). Prior art for batching exists in-repo: `$in` reads (`room-service/store_mongo.go:867`, `:2014`) and `DeleteMany` (`room-worker/store_mongo.go:468`); `store.go` exposes no batch method at all.
- `high` — `handleRemove` is the same N+1 (`FindUser` then `FindOneAndDelete` per user) — `bot-room-service/handler.go:466`, `:476`, `store_mongo.go:131`. Removal is the most expensive path because each removal also triggers a full-roster key rotation below.
- `high` — Both deferred safety nets run on the **cancelled** request context. `defer subauthcache.BustSubs(c, …)` (`handler.go:436`) and the deferred `rotateAndFanOut(c, roomID)` (`handler.go:446-458`) take `c`, whose deadline is set by `HandlerTimeout` (`pkg/natsrouter/middleware.go:191`, wired at `pkg/natsrouter/guard.go:69`, `main.go:76`). The failure mode these defers exist for — a slow/failing Mongo mid-batch — is exactly the one that exhausts the 10s budget, so on timeout the bust and the rotation both fail immediately against `context.DeadlineExceeded`, leaving removed accounts authorized for the L2 TTL and still holding a working key. The N+1 above makes deadline exhaustion likely rather than theoretical.
- `medium` — Room-key fan-out is O(room size) serial publishes with an unbounded roster load: `ListRoomMemberAccounts` does a `Find` with no limit and `cur.All` into a slice (`store_mongo.go:169-183`), then `fanOutKey` loops `SendData` per account (`handler.go:553-559`), all inside the request path (`handler.go:589`). Removing one member from a 5k-member room does 5k publishes before the RPC replies.
- `medium` — `ListRoomMemberAccounts` projects `{"u.account": 1}` without `"_id": 0` (`store_mongo.go:171`), so the read cannot be served covered by the `roomId_1_u.account_1` index the service verifies at startup (`store_mongo.go:36`) and must fetch every subscription document. `DeleteSubscription` gets this right (`store_mongo.go:135`).
- `medium` — Every create/add/remove reply blocks on a JetStream PubAck for a **best-effort, failure-logged-only** system message, with a 2s timeout charged to the client's latency budget — `sysmsg.go:62-68`, called at `handler.go:270`, `:379`, `:520`.
- `low` — `FindUser` over-projects: `engName`, `chineseName` and `roles` are fetched (`store_mongo.go:155`) but only `ID`/`Account`/`SiteID` are ever read (`handler.go:253-263`, `:334-350`, `:489`); the owner participant comes from the identity header (`handler.go:199`), not the user doc. `roles` is an array — this is per-member on the N+1 path.
- `low` — `handleAdd` resolves (and may *mint*) the room key before knowing whether any member is new (`handler.go:319`), so an empty or all-duplicate batch still pays a `keyStore.Get` and can write a fresh key into a legacy room.
- `nitpick` — `fanOutKey` calls `keySender.SendData` (`handler.go:556`), which discards the caller's context via `context.Background()` (`pkg/roomkeysender/roomkeysender.go:63-64`); `SendDataContext` exists and would keep the failure metric trace-correlated.

Not findings, verified: no goroutines are launched in service code, so no leak surface; no `time.Sleep`; no JetStream consumers, so `jsretry`/`BackOff` rules do not apply; `encoding/json` is correct here (not a designated sonic hot-path worker); `FindRoom`/`FindUser`/`DeleteSubscription` project explicitly and handle `mongo.ErrNoDocuments`; no `$lookup`; indexes are ensured at startup with a bounded context (`main.go:107-111`).

### Recommendations
- `high` — Add `FindUsers(ctx, ids []string) (map[string]*model.User, error)` (`$in`) and a bulk `UpsertSubscriptions` (`BulkWrite`, unordered) to `store.go`; collapse the three loops to one batch read + one bulk write, keeping the per-user newly-added diff from `BulkWriteResult`.
- `high` — Cap batch size (`len(UserIDs)`/`len(Members)`) with an `errcode.BadRequest` above a configured limit, so a single RPC cannot exceed the request budget.
- `high` — Run the deferred bust and deferred rotation on a context detached from the request deadline (`context.WithoutCancel(c)` plus a short independent timeout), so the recovery nets survive the timeout they exist to cover.
- `medium` — Add `"_id": 0` to the `ListRoomMemberAccounts` projection to make it index-covered, and drop `engName`/`chineseName`/`roles` from the `FindUser` projection.
- `medium` — Bound and parallelize the key fan-out: page `ListRoomMemberAccounts` (or `SetBatchSize`) and publish with a small bounded worker pool joined by `sync.WaitGroup` before returning.
- `low` — Move `keyPairOrHeal` in `handleAdd` after the first newly-added member is known, so no-op batches cost nothing.
- `low` — Make the sysmsg publish non-blocking on the reply (publish after responding, or via a bounded worker with an explicit termination path wired into `shutdown.Wait`).

---

## 8. Prioritized action list

| # | Sev | Action | Dim | Evidence | Why |
|---|-----|--------|-----|----------|-----|
| 1 | `high` | **Write `joinedAt` and `roles` on every subscription** | Integration | `store_mongo.go:101-109`; every other writer sets both (`room-worker/handler.go:1510`, `inbox-worker/handler.go:322`); consumed at `room-service/store_mongo.go:140`, `:745-750` | room-service's `member.list` **projects `roles` and paginates on the `{roomId, joinedAt, _id}` index.** A subscription without them is a row that sorts wrong and reports no roles — in a collection four services read. |
| 2 | `high` | **Use the room's site, not the member's, for channel subscriptions** | Integration | `handler.go:254`, `:335`; convention at `room-worker/handler.go:1170` (`SiteID: room.SiteID`) and `inbox-worker/handler.go:~310`; consumed at `user-service/service/subscriptions.go:237`, `:789-795` | user-service's `subscription.list` **groups rows by `sub.SiteID` to fetch room metadata from that site** — so a cross-site member's row sends the lookup to the wrong site and the room renders without metadata. **The DM and owner paths in this same service use `h.siteID` correctly**, which is what makes this a bug rather than a convention choice. |
| 3 | `high` | **Add `bootstrap.go` and the `Bootstrap bootstrapConfig` field** — verify-only for OUTBOX (`outbox-worker` owns it), create-when-enabled for BOT-MESSAGES-CANONICAL | Architecture | publishes to `OUTBOX-{siteID}` (`handler.go:646`, `outboxpublish.go:36`) and `BOT-MESSAGES-CANONICAL-{siteID}` (`sysmsg.go:66`) with no bootstrap helper | CLAUDE.md: "New services that interact with JetStream MUST follow this convention." **Both sibling producers verify-when-disabled so a misprovisioned deploy fails at startup**; this one discovers it at first publish, in a handler, on a client's request. |
| 4 | `high` | **Make `ROOM_KEY_RETIRED_TTL` overridable in compose** | Architecture / Integration | hardcoded `30m` at `deploy/docker-compose.yml:21` (and `ROOM_KEY_GRACE_PERIOD=24h` at `:20`); **every cohort peer uses `${ROOM_KEY_RETIRED_TTL:-30m}`** — `room-service/deploy/docker-compose.yml:35`, `room-worker/deploy/docker-compose.yml:27`, `broadcast-worker/deploy/{user,bot}/docker-compose.yml:36` | CLAUDE.md requires all key-writing services be configured **identically**, and names the failure: a service configured short expires versions its peers still consider resolvable, and **`key.get` then fails permanently for messages already on the wire.** An operator raising the shared variable moves the other three and silently leaves this one behind. |
| 5 | `high` | **Fix the N+1 on every membership RPC** | Performance | `handleAdd`/`handleCreate` do one `FindUser` + one `UpsertSubscription` + one OUTBOX publish **per user, serially**, with no cap on `req.UserIDs`/`req.Members` — `handler.go:243`, `:252`, `:263`, `:325`, `:333`, `:350`; `handleRemove` the same at `:466`, `:476` | Three round trips per member inside a 10 s request budget, unbounded in batch size. **Removal is the worst path** because each removal also triggers a full-roster key rotation (item 6). |
| 6 | `high` | **Run the deferred safety nets on a context that survives the request** | Performance / Quality | `defer subauthcache.BustSubs(c, …)` at `handler.go:436` and the deferred `rotateAndFanOut(c, roomID)` at `:446-458`, both on `c`, whose deadline comes from `HandlerTimeout` (`pkg/natsrouter/middleware.go:191`, wired at `main.go:76`) | **The failure mode these defers exist for — a slow or failing Mongo mid-batch — is exactly the one that exhausts the 10 s budget.** So on timeout the cache bust and the key rotation both fail too, precisely when they are needed. `context.WithoutCancel` with its own budget. |
| 7 | `critical` | **Raise coverage from 49.0%** | Test coverage | below the 60% line; **the entire Mongo layer 0%** (`store_mongo.go:47`, `:71`, `:150`, `:193`) with neither unit nor integration tests; `parseIdentity`'s three rejection branches 0% (`handler.go:696`, `:701`, `:705`); the `ErrDuplicate → Conflict(BotRoomExists)` path 0% (`:206-210`); **every `handleDMEnsure` error path 0%** (`:107-114`, `:117-122`, `:138-140`, `:155-157`, `:171-173`) | The identity gate, the whole persistence layer and every DM failure path are unexercised — in the service that writes the subscription documents items 1 and 2 are about. Add mockgen infrastructure while here: `store.go` has **no `//go:generate` directive** and the fakes are hand-written, whose own comment records the gap. |
| 8 | `medium` | **Bound and batch the room-key fan-out** | Performance | `ListRoomMemberAccounts` does a `Find` with no limit into `cur.All` (`store_mongo.go:169-183`); `fanOutKey` loops `SendData` per account (`handler.go:553-559`), all inside the request path (`:589`) | **Removing one member from a 5k-member room does 5k serial publishes before the RPC replies.** While here, add `"_id": 0` to the projection at `store_mongo.go:171` so the read can be served covered by the index the service verifies at startup — `DeleteSubscription` already gets this right. |
| 9 | `medium` | **Split `handleRemove`** | Maintainability | `handler.go:396-542` — 150 lines carrying four interlocking invariants (batch pre-validation, a deferred `BustSubs`, a deferred rotation gated on a `rotated` flag, a deferred-until-after-rotation federation queue), **64 of those 150 lines being comment** | It is the hardest function in the service to change safely, and it is where items 5, 6 and 8 all land. The comment volume is a signal that the structure is carrying more than it should. |
| 10 | `medium` | **Mount `natsrouter.GuardConfig`; add the CI pipeline** | Architecture | `MAX_CONCURRENCY`/`REQUEST_TIMEOUT` re-declared at `main.go:57-58` and hand-assembled at `:80`, against `pkg/natsrouter/guard.go:12`, `:21` and four peers that mount it correctly; no `deploy/azure-pipelines.yml` (29 of 37 services have one) | The shared-knob rule again — and, more practically, **the service has no CI build path at all**, which is why items 7 and 3 went unnoticed. |

**Two contract-documentation fixes.** `bot-room-service` is an **undocumented fifth OUTBOX producer** riding the per-destination FIFO lane: `pkg/outbox/outbox.go:2` and CLAUDE.md's JetStream Streams section both enumerate only room-service, room-worker, message-worker and broadcast-worker. Its events do partition correctly (`InboxMemberAdded`/`InboxMemberRemoved` ∈ `OrderedEventTypes`), so this is doc drift rather than a stranded type — but the OUTBOX owner's own audit flagged the same omission independently, which is how doc drift on a federation contract usually surfaces. Second, `BotIdentity.AppID/AppName/EngName/ChineseName` are persisted into `rooms.u` and returned as the create-response owner (`handler.go:196-200`, `:281`) but **no producer ever populates them** — botplatform builds identity as `{ID, Account, SiteID}` only. The owner enrichment is dead-empty in production; decide it at the producer.

**Also worth doing.** De-duplicate `keyPairOrHeal`, `rotateAndFanOut` and `fanOutKey`, which are re-implementations of `room-worker`'s and **have already diverged in behaviour, not just style** (`handler.go:546-613` vs `room-worker/handler.go:1702-1722`, `:346-359`, `:2571`) — and `parseIdentity`, which is byte-identical to `bot-message-handler`'s. Stop blocking every create/add/remove reply on a JetStream PubAck for a **best-effort, failure-logged-only** system message with a 2 s timeout charged to the client (`sysmsg.go:62-68`). Fix the sysmsg dedup id, which can never dedup a retry despite its comment claiming it does (`sysmsg.go:385-388`). Delete the dead `ALL_SITE_IDS` → `parsePeers` → `handler.allSiteIDs` plumbing, stored and never read. Cover `Register` (`handler.go:87-98`) so the five subjects are asserted. And fix the shared mutable state across tests — package-level `testKeyStore`/`testKeySender` wrapping one `&fakePublisher{}` that appends into shared slices — plus the near-total absence of table-driven tests (1 `t.Run` across ~53 test functions).
