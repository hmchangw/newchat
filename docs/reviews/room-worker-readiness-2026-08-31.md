# room-worker — Production Readiness Review

**Service:** `room-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.3 / 5

The federation plumbing is correct and unusually well-reasoned — every OUTBOX type is correctly partitioned onto the FIFO lane, subjects all come from `pkg/subject`, `bootstrapStreams` is *stricter* than the spec, and the high-throughput consumer pattern is textbook. The problems are elsewhere. **A rename can permanently diverge `rooms.name` from `subscriptions.name`**: the room-name `$set` is unguarded and commits before a NAK-able federate, while the subscription write *is* high-water-mark guarded and refuses to follow it back. **The teams-mode deploy silently serves live client DM-create RPCs** on the shared queue group. And structurally the service is the hardest in the fleet to change safely: a 476-line function inside a 2,625-line `handler.go`, a 7,920-line test file, a 31-method store interface, and five copy-pasted federation blocks.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 2 / 5 |
| 4 | Maintainability | 2 / 5 |
| 5 | Integration | 4 / 5 |
| 6 | Performance | 4 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 0 | 12 | 22 | 15 | 6 | **55** |

> **Audit-coverage caveat.** `gosec` and the repo-owned `semgrep` rules are clean repo-wide; `govulncheck` and the registry packs could not run (blocked egress), so dependency-CVE coverage is unverified.

---

## 2. Go code quality — 4 / 5

Disciplined, idiomatic Go — correct `%w` wrapping, `errors.Is` never string comparison, clean `errcode` Tier-1 usage and `jsretry.SettleQuiet` — held back by 22 systematically-discarded marshal errors and context-less log calls.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **22 `json.Marshal` errors discarded to `_`** with no justifying comment (§3: "never ignore errors silently — comment if intentionally discarded"). A nil `data` is published as an **empty body**, so a marshal regression ships a malformed event instead of failing. The same file does it right at `handler.go:1977`, which makes this stylistic drift rather than policy | `handler.go:187`, `:452`, `:467`, `:480`, `:504`, `:506`, `:532`, `:668`, `:682`, `:693`, `:708`, `:731`, `:1188`, `:1195`, `:1210`, `:1234`, `:1259`, `:1296`, `:1348`, `:1868`, `:1876`, `:1898` |
| medium | `mustMarshal` violates the Go `must*` convention: it **swallows the error and returns `nil`** rather than panicking. The name promises a guarantee the body does not provide; every caller reads it as infallible | `handler.go:1347` |
| medium | Non-`Context` `slog` variants used inside functions holding a `ctx`, dropping the correlation ID §3 requires. These are the publish-failure and rename paths — precisely the lines an operator needs to join to a request — while 20+ sibling sites in the same file use `*Context` correctly | `handler.go:112`, `:2305`, `:2426`, `:2434`, `:2449`, `:2462`, `:2584` |
| medium | Raw decoder error text interpolated into a **client-facing** `errcode.BadRequest` — §3: "Never expose raw internal errors to clients". The other three unmarshal sites use static strings, so this is an outlier | `handler.go:2302` |
| low | `SubscriptionStore` declares 30 methods spanning rooms, users, apps, orgs, threads, room-members and cross-site flags; the `<Domain>Store` name no longer names a domain | `store.go:64` |
| low | 14 bare `return err`. Mostly benign (the callee wraps), but at `handler.go:417` the surviving text is only `pkg/outbox`'s "publish outbox event for {dest}" — **the caller's room operation is lost** | `handler.go:354`, `:417`, `:545`, `:626`, `:758`, `:861`, `:1014`, `:1300`, `:1566`, `:1643`, `:1901`, `:2408`, `:2546`; `teamsroomcreate.go:85` |
| low | `loadAddMemberInputs` uses a plain `errgroup.Group`, not `errgroup.WithContext`, so a failing branch leaves up to four sibling Mongo queries running to completion | `handler.go:780-783` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | Inconsistent log-key casing: `room_id` (24 sites) vs `roomId` vs `roomID`; `request_id` (22) vs `requestID` — breaks dashboard filters keyed on the dominant form | `handler.go:2308`, `:2309`, `:2584`, `:2610` |
| nitpick | Store result DTOs carry only `bson` tags, no `json` | `store.go:30`, `:37`, `:55` |
| nitpick | `errors.New("chat has no id")` states no operation, unlike every other error in the file | `teamsroomcreate.go:47` |

### Recommendations
- `medium` — Replace the 22 discarded marshals with the existing `publishCanonical` shape on error-returning paths, and a single logged-and-skipped branch on best-effort fan-outs. The payloads are all `pkg/model` structs, so the errors are unreachable — **that is the argument for a one-line comment, not for `_`**.
- `medium` — Either make `mustMarshal` actually panic (matching `text/template.Must`) or rename it `marshalOrEmpty` and document that callers may publish an empty body.
- `medium` — Convert the seven non-`Context` `slog` calls to `*Context`; add a `forbidigo`/semgrep rule so the plain variants cannot be reintroduced in `package main` handlers.
- `medium` — Drop `err.Error()` from `handler.go:2302`; use the static `errcode.BadRequest` form the other three sites use.
- `low` — Rename `SubscriptionStore` to `RoomWorkerStore` or split off the thread-cleanup and org-display groups; switch `loadAddMemberInputs` to `errgroup.WithContext`; normalize log keys to `snake_case`.

---

## 3. Architecture — 4 / 5

The federation boundary, stream-bootstrap opt-in, consumer pattern and shutdown ordering are all correct and unusually well-reasoned. The deductions are a mode leak, constructor DI bypassed by field pokes, and units that have outgrown the flat layout.

### Verified clean
All cross-site publishes route through `federate` → `outbox.Publish` (`handler.go:341-343`, call sites `:544`, `:757`, `:1299`, `:1900`, `:2406`, `:2515`, `teamsroomcreate.go:328`, `:392`) — **no direct remote-INBOX publish anywhere**; only same-site `subject.InboxInternal` search-feed writes. Every type used is in exactly one `pkg/outbox` partition set. `bootstrapStreams` sets only `Name + Subjects` and, when disabled, **verifies** the stream rather than no-op'ing — stricter than the spec. The consumer is `cons.Messages()` + `MAX_WORKERS` semaphore with `PullMaxMessages(2*MaxWorkers)`, never mixed with `Consume()`, and `BackOff` comes from `stream.DurableConsumerDefaults`. Shutdown follows `iter.Stop → wg.Wait → Drain → DB` at 25 s. Config is a typed `caarlos0/env` struct with fail-fast validation; no `os.Getenv`.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | The **`teams`-mode deploy unconditionally registers the production sync-RPC** `chat.server.request.room.{site}.create.dm` on the shared `room-worker` queue group. `Mode` gates the stream, the durable and `bootstrapStreams` — but **not the router**. A Teams-migration pod sized for a batch job silently takes production DM-create traffic | `main.go:283-285` vs `:213`, `:449-452`; `bootstrap.go:43-46` |
| medium | Four dependencies assigned by **direct field poke after `NewHandler`** (`publishUsers`, `dekProvisioner`, `valkey`, `reconcileTTL`). The comment states the reason is avoiding churn — which is exactly what the functional-option pattern already used in-repo (`broadcast-worker/handler.go:102`, `inbox-worker/handler.go:172`) solves. A zero-value `reconcileTTL` silently means "recompute every add" | `main.go:266`, `:279-281`; `handler.go:64-67` |
| medium | `SubscriptionStore` is a **31-method interface** spanning subscriptions, rooms, users, apps, room_members, thread state, org-display rollups and room creation, with no seam for the Teams-only methods — so the default-mode deploy carries the migration surface | `store.go:64` |
| medium | `handler.go` is 2,625 lines / 111 KB with remove, add, create, rename, key-fan-out and sync-DM flows in one file (`processAddMembers` alone spans ~475 lines); `store_mongo.go` is 829 lines | `handler.go:834-1309` |
| low | Cross-service coherence knobs re-declared per service: `ROOM_KEY_RETIRED_TTL` duplicated verbatim in four services that `CLAUDE.md` requires to agree. `roomkeystore` owns the collection and should own a mounted config struct, as `roommetacache/ttlconfig.go:13` does | `main.go:70`; `room-service/main.go:67`; `bot-room-service/main.go:42`; `broadcast-worker/main.go:82` |
| low | `ROOM_META_CACHE_TTL` defaults to **60 s here vs 2 m** in broadcast-worker, message-gatekeeper and notification-worker. Per-process L1 caches, so not a shared-key bug — but exactly the drift the declare-once rule prevents | `main.go:58` |
| low | The single `PublishFunc` picks its transport **implicitly from whether `msgID` is empty** (core NATS vs JetStream), coupling durability choice to a parameter's emptiness | `main.go:239-262`; `handler.go:50` |
| nitpick | JetStream dispatch matches subjects with `strings.HasSuffix`/`Contains` rather than `pkg/subject` parsers; correctness depends on `.teams.create` being tested before `.create` | `handler.go:256-274` |
| nitpick | `MongoStore`/`NewMongoStore` exported from `package main` | `store_mongo.go:22`, `:42` |

### Recommendations
- `medium` — Gate `natsrouter.Register` (and the router construction) behind `cfg.Mode == "default"`, or give teams mode its own queue group.
- `medium` — Replace the four post-construction assignments with `NewHandler(..., opts ...handlerOption)`, mirroring broadcast-worker.
- `medium` — Split `SubscriptionStore` along the flows the handler already has; move the Teams surface behind its own interface; rename the residual to `RoomStore`.
- `medium` — Adopt the sanctioned sub-package layout: extract `processAddMembers`/`processRemoveMember` into `member.go`, create into `create.go`, key fan-out into `keyfanout.go`.
- `low` — Move `ROOM_KEY_RETIRED_TTL` + `ROOM_KEY_GRACE_PERIOD` into a `roomkeystore.Config` mounted in all four services; align `ROOM_META_CACHE_TTL` or document why 60 s.
- `nitpick` — Add `subject.ParseRoomCanonical`-style helpers so dispatch uses parsed tokens rather than suffix matching.

---

## 4. Test coverage — 2 / 5

Coverage is **62.8% (1701 statements)**, below the §4 80% floor, so the dimension is floored at 2. The harness is otherwise well built — injected publisher, generated mocks, correct `testutil` integration setup — but **every federation-failure and Ack/Nak branch is structurally untestable because the publisher double cannot fail.**

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | 62.8%, under the §4 80% floor | `coverage_by_service.txt` |
| high | **`HandleJetStreamMsg` is 0%** — the service's only subject router and its only `jsretry.SettleQuiet` Ack/Nak decision point. Nothing tests that `.member.add`/`.member.remove`/the transitional `.teams.create` vs `.create` suffix ordering routes correctly, nor that a `permanent()` error Acks while a transient one Naks with backoff. A routing regression silently misroutes a whole event class | `handler.go:238-278` |
| high | **Every cross-site `federate()` failure branch is uncovered** — the `return err` on OUTBOX publish failure, i.e. the path that must NAK so an ordered `member_added`/`room_renamed` is retried rather than lost. `:544` and `:757` *are* covered, so this is inconsistent, not absent by design | `handler.go:1299`, `:1900`, `:2407`; `teamsroomcreate.go:328`, `:392` |
| high | **Root cause of the above: the test publisher always returns `nil`.** With no error-injection seam, no publish/federate error path in the service can be exercised — which also explains the uncovered `publishSubscriptionUpdate`, `publishRoomEvent` and `publishMemberEvent` | `mock_publisher_test.go:19-25` |
| medium | `permanent()` is used at **26 sites** but only **2** assertions reference `errcode.IsPermanent` in the whole suite. The Ack-poison-vs-retry classification — the highest-consequence per-error decision in a JetStream worker — is essentially unasserted | `handler_test.go:3878` |
| medium | Four Mongo store implementations are exercised **only through gomock**, never against real Mongo. These are hand-written aggregation pipelines with explicit projections — exactly the class of defect (wrong field name, wrong projection) a mock provably cannot catch. (The 0% shown for `store_mongo.go` is a tag artifact; the other ~28 methods are covered properly) | `store_mongo.go:269`, `:586`, `:713`, `:737` |
| medium | `requireKeyPair`'s nil guard is never tested (50%, happy branch only) — this is the invariant "**nothing keyless is ever published**"; its metric emission and permanent-error return are unverified | `handler.go:2534-2540` |
| medium | `roomLocalityForMember` is 28.6%; only the `!UsesLocal()` early return runs. Both the `GetRoomMeta` success path and the documented fail-open-to-global branch are uncovered — a regression silently routes member events to the wrong namespace | `handler.go:2475-2481` |
| low | `cleanupThreadMembership` error wraps uncovered | `handler.go:363`, `:366` |
| low | No NATS integration test: `integration_test.go` uses only `testutil.MongoDB`, never `testutil.NATS`, so the OUTBOX publish path is never validated against real JetStream (dedup `Nats-Msg-Id`, subject acceptance) | `integration_test.go:74` |
| nitpick | 195 top-level test funcs vs 26 `t.Run` calls in a 7,920-line file; near-identical scenarios are copy-pasted per function. Positively: no build-tag violations, `TestMain` correct, no inline `GenericContainer`, no real DB/NATS in unit tests, mocks generated and unedited | `handler_test.go` |

### Recommendations
- `high` — Give `mockPublisher` a failure seam (e.g. `failOn func(subj string) error`); then add table-driven tests asserting each `federate()` site returns a **non-permanent** error so JetStream retries. This one change unlocks four of the findings above.
- `high` — Add a `HandleJetStreamMsg` table test over a fake `jetstream.Msg` covering all five subject suffixes plus the transitional `.teams.create`, the corrupt-payload branch and the unknown-subject default, asserting Ack vs `NakWithDelay` per `jsretry.DefaultBackoff`.
- `medium` — Assert `errcode.IsPermanent` in every test that drives a `permanent()` site, not just the two current ones.
- `medium` — Add integration tests for the four untested pipelines against `testutil.MongoDB`, asserting the projected field set.
- `medium` — Cover `requireKeyPair(nil)` and both `roomLocalityForMember` branches.
- `low` — Add a `testutil.NATS(t)` test driving one `member_added` through `outbox.Publish`, asserting the subject and dedup header; collapse the near-duplicate test functions into tables.

---

## 5. Maintainability — 2 / 5

Exceptional WHY-comment discipline and a few well-extracted helpers, but a 2,625-line `handler.go` containing a 476-line function, a 7,920-line test file, a 31-method store interface and five copy-pasted federation blocks make any new membership feature a high-friction, high-risk edit. **This is the lowest maintainability score in the fleet, and it is the dimension most likely to cause the next incident.**

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | `processAddMembers` is **476 lines** performing ~10 distinct phases (cross-site marking, candidate splitting, user lookup, key self-heal, two bulk writes, org backfill, count reconcile, room re-read, four publish fan-outs). Nothing can be tested or changed in isolation; every new add-member rule lands here | `handler.go:834` |
| high | `handler.go` is 2,625 lines / 53 functions spanning six unrelated flows: add-members, remove-individual, remove-org, create-room, rename, and the synchronous `serverCreateDM` RPC plus key fan-out | `handler.go:1` |
| high | `handler_test.go` is **7,920 lines** with 195 top-level tests and four near-duplicate fixture constructors | `handler_test.go:2351`, `:2854`, `:3990`, `:6418` |
| medium | A `publishCanonical` helper already exists, yet three sites hand-roll the identical `MessageEvent` + marshal + `CanonicalDedupID` + publish block | `handler.go:1970` vs `:526`, `:725`, `:1253` |
| medium | The cross-site relay is copy-pasted **five times** — the same `payloadSeed` → `InboxDedupID` → `federate` shape — wrapped in three near-identical destination loops. A change to dedup-seed composition must be made in five places or the sites silently diverge | `handler.go:542`, `:755`, `:1297`, `:1877`, `:2514`; loops at `:746`, `:1282`, `:1883` |
| medium | **`deploy/teams/` is a diverged fork, not a variant**: `deploy/teams/Dockerfile` is byte-identical to `deploy/Dockerfile`, and its compose **drops** `ROOM_SUBJECT_MODE`, `ROOM_KEY_RETIRED_TTL`, `MONGO_KEY_READ_PREFERENCE` and the whole `ATREST_*`/`VAULT_*` block while hardcoding values the default parameterises. `CLAUDE.md` requires `ROOM_KEY_RETIRED_TTL` identical across three services — the fork is exactly how that drifts | `deploy/teams/docker-compose.yml:10` |
| medium | JetStream dispatch is an **order-dependent** `strings.HasSuffix` chain: `.teams.create` must be matched before `.create` or teams messages are misrouted, a constraint enforced only by a comment | `handler.go:254-273` |
| medium | The two binary modes carry **different, undocumented ack contracts**: teams mode logs and swallows every per-chat error and always returns `nil`, so a teams batch can never NAK, while default mode routes everything through `jsretry.SettleQuiet` | `teamsroomcreate.go:34-38` vs `handler.go:277` |
| low | Four dependencies bypass the constructor via post-construction field assignment; `main()` is 306 lines of unfactored wiring | `main.go:100`, `:264-273` |
| nitpick | Orphaned step numbering (`// 6.`, `// 8.`, `// 10.` with no 1–5/7/9) and ticket/branch-relative comments that no longer resolve ("Task 20.15", "Task 35/36/37", "this PR's dept-aware match", "feat/migrated-user-fanout"); a transitional pre-cutover subject match with no removal trigger or owner | `handler.go:200`, `:259-261`, `:1118`, `:1827`; `store.go:48`; `main.go:265` |

### Recommendations
- `high` — Split `handler.go` by flow into `handler_addmembers.go`, `handler_removemember.go`, `handler_createroom.go`, `handler_rename.go`, `handler_syncdm.go`, `handler_roomkey.go`, keeping the struct, constructor and dispatch in `handler.go`. Split `handler_test.go` to match. **Pure file move — highest value, lowest risk, do it first.**
- `high` — Decompose `processAddMembers` into named stages returning explicit structs: `classifyCrossSite`, `resolveAddWrites`, `commitAddWrites`, `emitAddEvents`. Each becomes independently table-testable.
- `medium` — Extract one `emitMembershipChange(ctx, room, evt, accountsBySite)` covering the room event + internal publish + per-destination federate loop, plus a `federationSeed(...)` function; replace the five copies.
- `medium` — Route the four hand-rolled `MessageEvent` publishes through `publishCanonical`.
- `medium` — Delete `deploy/teams/Dockerfile` (point the teams pipeline at the shared one) and rebuild its compose from the default with only `MODE`/`OTEL_SERVICE_NAME` overridden, restoring the dropped vars.
- `medium` — Replace the suffix-chain dispatch with an explicit map keyed on `pkg/subject` constants, removing the ordering hazard.
- `low` — Move the four poked dependencies into `NewHandler` as an options struct; sweep the stale ticket/PR-relative comments.

---

## 6. Integration — 4 / 5

Federation plumbing is correct and disciplined — every OUTBOX type is in the partition, all subjects come from `pkg/subject`, every envelope stamps `Timestamp` at the publish site — but the rename path can permanently diverge two documents, and the teams deploy silently serves live client RPCs.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **`processRoomRename` commits an unconditional room-name `$set` and then returns (NAKs) on every downstream failure**, so a redelivered stale rename re-applies the old name to the room doc — while the subscription write **is** high-water-mark guarded and refuses to follow it back, leaving `rooms.name` and `subscriptions.name` **permanently divergent**. `UpdateRoomName` is a bare `$set` with no `nameUpdatedAt` guard, unlike `UpdateSubscriptionNamesForRoom`. The code's own comment at `handler.go:2386-2392` states exactly this hazard as the reason the internal-lane publish is best-effort — then returns on the OUTBOX federate 17 lines below | `handler.go:2311`, `:2409`; `store_mongo.go:767-771` vs `:812-826` |
| medium | The DM-create RPC is registered unconditionally, so `MODE=teams` pods answer live client `chat.server.request.room.{siteID}.create.dm` traffic on the shared queue group, even though the JetStream durable **is** mode-split. A migration-only deploy joins the serving path for a synchronous, user-facing operation; a teams batch saturating the Mongo pool degrades DM creation fleet-wide | `main.go:283-285`, `:452-456` |
| medium | The teams deploy omits `ROOM_SUBJECT_MODE` (falls back to `global`) while the default deploy sets `dual`. Harmless today only because the teams stream path publishes no room-scoped events; combined with the finding above it becomes a live divergence the moment that path grows one | `deploy/teams/docker-compose.yml:9-31` vs `deploy/docker-compose.yml:17` |
| medium | `ROOM_KEY_RETIRED_TTL` / `ROOM_KEY_GRACE_PERIOD` re-declared with their own tag and `envDefault` in four services rather than owned by `pkg/roomkeystore`. Values currently agree (30 m / 24 h), so drift risk rather than live defect — but the documented failure mode (`key.get` permanently failing for messages already on the wire) is prevented only by four copies happening to agree | `main.go:67`, `:70`; `room-service/main.go:65`, `:67`; `bot-room-service/main.go:39`, `:42`; `broadcast-worker/main.go:78`, `:82` |
| low | JetStream dispatch routes on raw `strings.HasSuffix`/`Contains` literals rather than a `pkg/subject` parser, so a builder rename silently falls through to `default:`, which logs a Warn and **Acks (drops)** the message | `handler.go:254-275` |
| low | The same federated event type is carried by **two different payload structs**: `model.MemberAddEvent` on the live paths and `model.InboxMemberEvent` on the Teams path. They decode compatibly today purely because the JSON tags overlap — nothing enforces that | `handler.go:1283`; `teamsroomcreate.go:280` |

### Verified clean
All four federated types (`member_added`, `member_removed`, `room_renamed`, `member_joinedat_refreshed`) are in exactly one `pkg/outbox` set, the first three on the **ORDERED** lane (`pkg/outbox/outbox.go:47-51`). No direct `InboxExternal` publish anywhere. OUTBOX subject is the `CLAUDE.md` form `chat.outbox.{origin}.{dest}.{type}` via `subject.Outbox`. Zero raw `fmt.Sprintf` subject construction. Every `InboxEvent` envelope and inner event sets `Timestamp` at the publish site. Every internal-lane type is covered by `subject.InboxMemberEventSubjects`. `bootstrapStreams` sets only `Name + Subjects` and fail-fast-verifies in prod. IDs use `idgen` with the correct per-entity format. No bare `Nak()`/`NakWithDelay(0)`. No client-facing `chat.user.` handler, so no `docs/client-api.md` obligation is outstanding.

### Recommendations
- `high` — Guard `UpdateRoomName` with the same monotonic `nameUpdatedAt` high-water mark the subscription update already uses, so a redelivered stale rename is a no-op on **both** documents rather than only one.
- `high` — Make the post-commit tail of `processRoomRename` best-effort-with-log (as the internal publish already is), or move the OUTBOX federate ahead of the Mongo commits. Returning an error after a non-idempotent `$set` **guarantees** the divergence.
- `medium` — Gate `natsrouter.Register(subject.RoomCreateDMSync…)` on `cfg.Mode == "default"`, mirroring the mode split already applied to the durable.
- `medium` — Move `RoomKeyRetiredTTL`/`RoomKeyGracePeriod` into a `roomkeystore.TTLConfig` mounted as a named field in all four services.
- `low` — Add `ROOM_SUBJECT_MODE` and `ROOM_KEY_RETIRED_TTL` explicitly to the teams compose so both deploys of one binary are provably configured alike.
- `low` — Replace suffix matching with a `pkg/subject` parse helper and make the `default:` branch a permanent error rather than a silent Ack; collapse the two member-event payload structs, or add a wire-compat round-trip test.

---

## 7. Performance — 4 / 5

Genuinely performance-engineered on the live add path — concurrent input loads, O(1) `$inc` counts with TTL reconcile, bounded single-marshal key fan-out, correct `jsretry` discipline, projections on most reads — but the rename path, the remove path's unconditional recompute, and the Teams reconciler each carry an avoidable room-sized cost.

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **`processRoomRename` reads every full subscription document in the room just to extract accounts.** The projected `GetSubscriptionAccounts` returns exactly the list this code needs. On a 10k-member channel a rename pulls ~10k whole docs (which carry room view, roles, HR info) purely to build a string slice, then feeds all 10k into a `$in` | `handler.go:2353`; `store_mongo.go:89-90` vs `:706` |
| medium | **N+1 user lookup in the Teams reconciler**: `resolveMember` → `store.GetUser` once per member inside the loop, while the live add path batches the identical need via `FindUsersByAccounts`. The L1/L2 cache softens but does not remove it — a first-migration batch is all-miss by construction. Same function also does an unprojected room-wide `ListByRoom` | `teamsroomcreate.go:124` → `:249`, `:89`; cf. `handler.go:955` |
| medium | The remove path never got the add path's counter optimization: `ReconcileMemberCounts` (two `CountDocuments` over the room's subscriptions) runs **unconditionally** on every individual removal, every org removal, every room create and every Teams chat. `ApplyMemberCountDelta` already provides the O(1)+TTL alternative and is used only by `processAddMembers` — **removals have an exactly-known delta too** | `handler.go:420`, `:634`, `:1800`; `teamsroomcreate.go:196`; `store_mongo.go:145` |
| medium | **Long handlers can outlive `AckWait` with no `InProgress()` heartbeat.** `AckWait` defaults to 30 s while `processRemoveOrg` runs two correlated-`$lookup` aggregations plus a rotate and key fan-out to every survivor, and `processTeamsRoomCreate` loops a whole batch of chats in one message. `msg.InProgress()` appears **nowhere in the repo**, so a slow room gets redelivered and processed concurrently with itself | `pkg/stream/consumer.go:19`; `handler.go:562`, `:643`, `:2571`; `teamsroomcreate.go:33` |
| medium | Four reads fetch whole documents against "always project precisely"; `GetRoom` is on the add-member hot path (post-write re-read) where only `lastMsgAt` plus the subscription-room view is consumed | `store_mongo.go:175`, `:299`, `:738`, `:90` |
| low | The four `$lookup` stages carry prose comments but not the mandated `// $lookup justification:` marker. `GetOrgMembersWithIndividualStatus` runs **two correlated `room_members` subqueries per matched user** — a server-side N+1 scaling with org size, not room size | `store_mongo.go:329`, `:345`, `:398`, `:418` |
| low | `resolveSubUpdateCounterpart` issues a `GetApp` Mongo read per subscription inside the fan-out loop; bounded to DM/botDM (≤2 subs) today, but uncached and on a hot path | `handler.go:2231` → `:2175` |
| nitpick | `jsretry.DefaultBackoff` is used for all four handlers, including user-visible member add/remove whose async job result the requester is blocking on; `LowLatencyBackoff` is the documented choice. Otherwise `jsretry` usage is exemplary — no bare `Nak()`, no hardcoded `cc.BackOff` | `handler.go:278`; `main.go:449` |

### Recommendations
- `high` — Replace `ListByRoom` with `GetSubscriptionAccounts` at `handler.go:2353`, and drop `ListByRoom` from the `SubscriptionStore` interface so no future caller reaches for it (keep the concrete method for integration tests).
- `medium` — In `reconcileTeamsRoom`, hoist a single `FindUsersByAccounts(chat.Members…)` before the loop and let `resolveMember` handle only genuine misses; batch the resulting `publishUsers` into one publish per chat.
- `medium` — Extend `ApplyMemberCountDelta` to the removal paths with a negative delta, and derive create-time counts from `subs` instead of `ReconcileMemberCounts`.
- `medium` — Start an `InProgress()` ticker (≈`AckWait/2`) around `HandleJetStreamMsg` in `runJobWithRecovery`, or split the Teams batch loop so one message is one chat.
- `medium` — Add projections to `GetRoom`, `GetSubscription`, `FindDMSubscriptionPair` — starting with `GetRoom`, the add-member hot path.
- `low` — Add the four `$lookup` justification comments and consider replacing `GetOrgMembersWithIndividualStatus`'s two per-user subqueries with two batched reads joined in Go.

---

## 8. Prioritized action list

| # | Sev | Action | Dimension | Evidence | Why |
|---|-----|--------|-----------|----------|-----|
| 1 | `high` | Guard `UpdateRoomName` with a monotonic `nameUpdatedAt` high-water mark, and make the post-commit tail of `processRoomRename` best-effort | Integration | `handler.go:2311`, `:2409`; `store_mongo.go:767` | **Permanent data divergence.** A redelivered rename re-applies a stale name to `rooms` while the guarded subscription write refuses to follow — the two documents disagree forever, with no heal path. The code's own comment identifies the hazard, then does the unsafe thing 17 lines later. |
| 2 | `high` | Give `mockPublisher` a failure seam, then test every `federate()` failure branch and `HandleJetStreamMsg` | Test coverage | `mock_publisher_test.go:19-25`; `handler.go:238-278` | One missing seam makes **all** federation-failure and Ack/Nak paths untestable — including the NAK that keeps an ordered `member_added` from being lost. Highest coverage value per unit of effort. |
| 3 | `high` | Replace `ListByRoom` with the projected `GetSubscriptionAccounts` on the rename path | Performance | `handler.go:2353` | A rename on a 10k-member channel currently pulls 10k full documents to build a list of strings. One-line fix, large constant-factor win. |
| 4 | `medium` | Gate the DM-create RPC registration on `cfg.Mode == "default"` | Architecture / Integration | `main.go:283-285` | A Teams-migration pod silently serves **live, user-facing** DM creation on the shared queue group; a batch job saturating its Mongo pool degrades DM creation fleet-wide. |
| 5 | `high` | Split `handler.go` by flow and decompose `processAddMembers` into named stages | Maintainability | `handler.go:834`, `:1` | 476 lines in one function inside 2,625 in one file, mirrored by a 7,920-line test. Pure file moves first, then staged decomposition — this is what makes items 1–2 safe to land. |
| 6 | `medium` | Add an `InProgress()` heartbeat around long handlers, or split the Teams batch to one chat per message | Performance | `handler.go:562`; `teamsroomcreate.go:33` | `msg.InProgress()` appears nowhere in the repo; a slow org-removal or Teams batch exceeds `AckWait` and is redelivered **concurrently with itself**. |
| 7 | `medium` | Extract one `emitMembershipChange` helper and one `federationSeed` function to replace the five copied relay blocks | Maintainability | `handler.go:542`, `:755`, `:1297`, `:1877`, `:2514` | Five copies of dedup-seed composition; a change to any one of them silently diverges the others. |
| 8 | `medium` | Extend `ApplyMemberCountDelta` to the removal paths | Performance | `handler.go:420`, `:634` | Every single removal pays two full `CountDocuments` over the room when the delta is exactly known. |
| 9 | `medium` | Rebuild `deploy/teams/` from the default deploy with only `MODE` overridden | Maintainability / Integration | `deploy/teams/docker-compose.yml:10` | A diverged fork that drops `ROOM_KEY_RETIRED_TTL` — the very knob `CLAUDE.md` requires identical across three services — plus the entire at-rest encryption block. |
| 10 | `medium` | Replace the 22 discarded `json.Marshal` errors and the `mustMarshal` misnomer | Code quality | `handler.go:187`…`:1898`, `:1347` | A marshal regression currently publishes an **empty body** rather than failing; `mustMarshal` promises a guarantee it does not provide. |

### Verdict

**Ship-capable, with item 1 fixed first.** The federation design here is sound and the OUTBOX partitioning is exactly right — this service gets the hard distributed-systems parts correct. What it lacks is the *safety net*: item 1 is a real, permanent data divergence that the code already knows about; item 2 is why nobody would notice items like it. The maintainability score of 2 is the leading indicator — at 476 lines per function and five copies of the relay, the next correctness fix is disproportionately likely to introduce the following one.
