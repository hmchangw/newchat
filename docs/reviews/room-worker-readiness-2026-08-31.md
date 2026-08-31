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

