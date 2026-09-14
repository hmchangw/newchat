# history-service — Production Readiness Review

**Service:** `history-service`
**Date:** 2026-08-31
**Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents (code quality, architecture, test coverage, maintainability, integration, performance), each judging against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 3.2 / 5

The largest service in the repo (~25.7k lines) and, on the evidence, one of the better-engineered ones: error wrapping, `errcode` tiering, projection discipline, goroutine termination and WHY-comment quality are all genuinely strong, and the bucket-walk read path is deliberately and thoughtfully bounded. Three things hold it back. First, **test coverage at 55.0% sits below the repo's 60% critical line**, with the entire store layer (`cassrepo` 32.1%, `mongorepo` 3.5%) and `cmd/` (11.3%) effectively unexercised outside integration builds. Second, a **real correctness defect in reaction mirroring**: reactions on a `TShow=true` thread reply never reach `messages_by_room`, so the channel timeline permanently loses them on reload. Third, the service is the repo's **only** user of a `cmd/` + `internal/` layout, which `CLAUDE.md` §1 forbids and which blocks reuse of genuinely reusable code. None of these is a shipping blocker on its own; the reaction-mirroring bug is the one that silently corrupts user-visible state.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 4 / 5 |

### Findings by severity

| Severity | Count |
|----------|-------|
| critical | 1 |
| high | 6 |
| medium | 17 |
| low | 16 |
| nitpick | 7 |
| **Total** | **47** |

> **Audit-coverage caveat.** `gosec` and the 18 repo-owned `semgrep` rules ran clean repo-wide (0 findings; rule fixtures 2/2). `govulncheck` and the `semgrep` registry packs could **not** run — `vuln.go.dev` and `semgrep.dev` are blocked by this environment's egress policy (403). Dependency-CVE coverage is therefore unverified and must be re-run on a network-permitted runner before shipping.

---

## 2. Go code quality — 4 / 5

Error wrapping, `errcode` tiering, sentinel/`errors.Is` discipline and goroutine bounding are exemplary across ~8k lines of production code. The deductions are a genuine log-and-return violation, inconsistent `slog` context propagation, and store-interface naming drift.

### Evidence

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | Logs **and** returns the same error, double-logging once the boundary classifies it (§3 "Never log AND return") | `internal/cassrepo/utils.go:161` |
| medium | Nine request-path log sites use the non-`Context` `slog` form, dropping ctx-carried trace/span correlation | `internal/service/threads.go:65`, `:121`, `:374`; `internal/service/migration.go:139`, `:145`; `internal/service/messages.go:405`, `:737`, `:742`; `internal/cassrepo/utils.go:162` |
| medium | Three of those carry no `request_id` at all — a publish/marshal failure cannot be tied to a request | `internal/service/messages.go:737`, `:742`; `internal/service/threads.go:374` |
| low | Store interfaces named `*Repository`, not `<Domain>Store` per §3 — and inconsistent with `UserStore`/`AppStore` declared in the same file | `internal/service/service.go:52`, `:57`, `:68`, `:105`, `:120` |
| low | Interpolated log message instead of a structured key: `"skipped malformed "+what+" attachment blobs"` | `internal/service/attachments.go:28` |
| low | Dynamic attribute key — `slog.Error("invalid config", c.name, c.value)` emits a different JSON field per knob, so the field is unqueryable | `cmd/main.go:56` |
| low | 9 of 19 `service` test files sit in external `package service_test`, forcing `UnavailableQuoteMsg` to stay exported for tests only | `internal/service/utils.go:97`, `internal/service/threads_test.go:607` |
| low | `getOrLoad`'s doc comment is separated from its function by `remove`, so godoc misattributes it | `internal/readcache/readcache.go:56` |
| low | Audit-coverage gap (environmental, not a service defect): `govulncheck` + semgrep registry packs blocked by egress | — |
| nitpick | `ErrEncryptedRowCipherDisabled` exported and documented for cross-package `errors.Is`, but no external referencer | `internal/cassrepo/decrypt.go:15` |
| nitpick | Mixed log-key casing: `messageID`/`gotRoom`/`wantRoom` beside the repo's `room_id`/`request_id` | `internal/service/threads.go:374` |
| nitpick | `ThreadSubRow` carries `bson` tags but no `json` tags | `internal/mongorepo/threadsubscription.go:21` |

The nine bare-`slog` sites are **drift, not house style** — the rest of the service correctly uses `WarnContext` (e.g. `internal/service/rooms.go:142`), which is what makes them worth fixing rather than accepting.

### Recommendations

- `medium` — Delete the `slog.Warn` at `internal/cassrepo/utils.go:162` and keep only the returned error; the boundary logs it once at the right level with the request ID.
- `medium` — Convert the nine bare `slog.Warn`/`slog.Error` request-path calls to `WarnContext(ctx, …)`. Where the context is a `*natsrouter.Context`, pass it directly — that also removes the hand-written `"request_id", natsutil.RequestIDFromContext(...)` pairs at `internal/service/threads.go:66`, `:121`.
- `low` — Rename the store interfaces in `internal/service/service.go` to `<Domain>Store` (`MessageStore`, `SubscriptionStore`, …), matching `UserStore`/`AppStore` already there, and regenerate `internal/service/mocks/` in the same change.
- `low` — Replace the concatenated message at `internal/service/attachments.go:28` with a constant message plus a `"kind"` attribute; normalize `messageID`→`message_id`, `gotRoom`→`got_room_id`.
- `low` — Move the nine `package service_test` files into `package service`, then unexport `UnavailableQuoteMsg`.
- `low` — Re-run `make sast-vuln` from an environment with egress to `vuln.go.dev` before shipping.
- `nitpick` — Move `getOrLoad`'s doc block back above it; either unexport `ErrEncryptedRowCipherDisabled` or add the cross-package `errors.Is` check its comment promises.

---

## 3. Architecture — 4 / 5

Genuinely strong layering — consumer-defined interfaces, a disciplined decorator composition root, correct config and shutdown wiring — held back by a directory layout matching neither `CLAUDE.md` rule, and by repository/transport types leaking through a boundary the code itself calls "transport-agnostic".

### Verified clean

Every subject goes through `pkg/subject` builders with zero raw `fmt.Sprintf`; no stream creation anywhere; no `os.Getenv`; shared knobs are mounted as named fields with `envPrefix` (`mongoutil.PoolConfig`, `subauthcache.TTLConfig`, `atrest.BreakerConfig`, `roomtimescache.TTLConfig`) rather than re-declared; config fails fast with a thoughtful *relational* bucket-budget check (`internal/config/config.go:242-266`); `pkg/shutdown.Wait` runs the correct order (router → drain → warm-back drain → Mongo → Cassandra → Vault → health → Valkey → obs) with a sub-budget for the optional drain; `deploy/` is complete with the mandated base images.

### Evidence

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | Client-facing wire structs live under `internal/`, so no other module can import them and copies drift — `tools/loadgen` hand-duplicates the RPC contract twice, each with a comment admitting it cannot import | `internal/models/message.go:24`; `tools/loadgen/history_generator.go:100`; `tools/loadgen/soak_wire.go:10` |
| medium | `HistoryService` is documented "transport-agnostic" but 35 signatures take `*natsrouter.Context`, including purely internal helpers. The transport type is **pooled** (`pkg/natsrouter/context.go:36`), so pushing it to background depth is the shape that eventually leaks a recycled context | `internal/service/service.go:187`; `internal/service/messages.go:652` |
| medium | Consumer-defined interfaces are typed in the *implementers'* packages — `MessageReader` is expressed in `cassrepo.PageRequest`/`Page[…]`, `GetRoomTimesByIDs` returns `map[string]mongorepo.RoomTimes`. The interface is nominally in the consumer, but the dependency edge still points at Cassandra/Mongo | `internal/service/service.go:22-29`, `:70`, `:119` |
| medium | Layout deviates from **both** `CLAUDE.md` forms — the flat rule ("no `cmd/` or `internal/`") and the sanctioned sub-package exception. This is the only service in the repo with either | `cmd/main.go:1`; `internal/config/config.go:1` |
| low | `service.New` takes the whole `*config.Config`, coupling the domain layer to the env-parsing package for six fields | `internal/service/service.go:232`; `internal/service/room_times.go:63` |
| low | No `bootstrap.go` and no `BOOTSTRAP_STREAMS` field, though the service publishes to MESSAGES-CANONICAL. Production behaviour is correct (it creates nothing); the gap is the documented dev-bootstrap contract that twelve peers carry | `internal/config/config.go:59`; `internal/service/messages.go:561` |
| nitpick | Warm-back metrics bypass the injected o11y SDK, using the OTel global at package-init while every other instrument comes from `sdk.MeterProvider()` and respects `sdk.Toggles.Metrics` | `internal/service/warmback.go:46` vs `cmd/main.go:120` |

### Recommendations

- `medium` — Move the client-facing request/response structs into `pkg/model` as the `RoomsGet` trio already was, and delete the two loadgen mirrors that exist only because `internal/` forbids the import.
- `medium` — Either drop the "transport-agnostic" claim or make it true: a thin transport shim extracts account/roomID/requestID, and every non-entrypoint helper takes a plain `context.Context`.
- `medium` — Move `Page`/`PageRequest`, `RoomTimes` and `ThreadSubRow` into `internal/models` (or `pkg/model`) so `service`'s interfaces stop importing `cassrepo`/`mongorepo` — the repos then depend on models, not the reverse.
- `medium` — Flatten to the sanctioned shape (`history-service/{main.go, config/, models/, mongorepo/, cassrepo/, service/, service/mocks/}`), matching `user-service`. Mechanical: an import-path rewrite plus a Dockerfile build path.
- `low` — Replace the `*config.Config` parameter with an explicit `service.Params` struct carrying the six values actually used.
- `low` — Add `bootstrap.go` + a `Bootstrap` field no-op'ing on `BOOTSTRAP_STREAMS=false`, and set it true in `deploy/docker-compose.yml`.
- `nitpick` — Thread the SDK's `MeterProvider` into `newPreviewWarmer` instead of the package-level `otel.Meter` global.

---

## 4. Test coverage — 1 / 5

Statement-weighted coverage is **55.0% of 2569 statements**, below the `CLAUDE.md` §4 60% line ("MUST NOT be merged"), so the dimension is floored at 1. The shape matters, though: `internal/service` is genuinely well tested at 93.2%, and the entire deficit sits in the two repository packages and `cmd/`.

### Per-package breakdown

| Package | Coverage | Statements |
|---------|----------|-----------|
| `internal/config` | 96.4% | 55 |
| `internal/service` | 93.2% | 1139 |
| `internal/readcache` | 89.2% | 83 |
| `internal/publisher` | 87.5% | 24 |
| **`internal/cassrepo`** | **32.1%** | 545 |
| **`cmd`** | **11.3%** | 194 |
| **`internal/mongorepo`** | **3.5%** | 144 |
| `internal/service/mocks` | 0.0% | 385 |

### Evidence

| Sev | Finding | Evidence |
|-----|---------|----------|
| critical | 55.0% overall, below the §4 60% bar | `internal/cassrepo/write.go:1`; `internal/mongorepo/room.go:29` |
| high | `cassrepo` and `mongorepo` have **no unit tests at all** for their exported surface — every covering `*_test.go` is `//go:build integration`, so the whole store layer reads 0% in the default profile | `internal/cassrepo/messages_by_room.go:94`; `internal/mongorepo/room.go:53` |
| high | `startBucketFromCursor` is 0% despite being a **pure function and an anti-abuse guard** — it rejects out-of-range cursor buckets so tampered cursors cannot consume `maxBuckets` empty reads. Needs no Cassandra to test | `internal/cassrepo/messages_by_room.go:24` |
| high | `checkConfig` is 0% — the only guard on `MESSAGE_BUCKET_HOURS`, which `CLAUDE.md` says must match across services or reads and writes silently target different partitions. Untestable as written: calls `os.Exit(1)` inline instead of returning an error | `cmd/main.go:43` |
| medium | `PreviewCache.Invalidate` is 0% — the eviction that stops a deleted/edited message being served as a room preview for the whole TTL. The comment itself flags this as a known correctness edge, yet nothing asserts eviction happens | `internal/readcache/readcache.go:287` |
| medium | The at-rest edit path is uncovered by unit tests, including two pure helpers needing no session. A regression silently drops attachments/card on edit of a legacy row — exactly what the doc comment says the code exists to prevent | `internal/cassrepo/write.go:91`, `:116` |
| medium | `internal/service/mocks` is a **non-test package**, so its 385 generated statements (15% of the denominator) enter at 0% and depress the figure. Excluding it, the service is ~64.7% | `internal/service/service.go:19` |
| medium | Tests are almost entirely one-function-per-scenario, not table-driven: `messages_test.go` has 133 `func Test…` and **zero** `t.Run`; `pin_test.go` 39/0 | `internal/service/messages_test.go:1` |
| low | Breaker-wrapped `GetRoomTimesByIDs` and `GetRoomUserCount` are 0%, so the RoomsGet and large-room-pin paths have no evidence they fail fast under a Mongo outage | `cmd/roomrepo_breaker.go:72` |
| nitpick | No `t.Parallel()` anywhere; ~25k lines of tests run strictly serially | — |

### Integration hygiene — clean

All integration files carry `//go:build integration`; all three packages have `TestMain` → `testutil.RunTests(m)`; zero inline `testcontainers.GenericContainer`; containers all from `pkg/testutil`; shared Valkey correctly paired with `t.Cleanup(testutil.FlushValkey)` (`internal/service/integration_test.go:495`); mocks generated via `go:generate mockgen` and confirmed non-stale repo-wide; no real DB/NATS in unit tests; no `os.Setenv`; package-level test vars are immutable timestamps only, so no order dependence.

### Recommendations

- `critical` — Unit-test the pure and mockable halves of `cassrepo`/`mongorepo`: cursor decode, bucket-range guards, the `structScan`/`Fetch` error branches (23.1% / 18.2%), and the pipeline builders at `internal/mongorepo/pipelines.go:10`. This alone clears 60% without touching the integration suite.
- `high` — Unit-test `startBucketFromCursor` in both directions: cursor above `defaultBucket`, below `floorBucket`, malformed encoding, empty-cursor default. Tampered cursors are the DoS vector it was written for.
- `high` — Refactor `checkConfig` into `validateConfig(cfg) error` with `main` doing the `os.Exit`, then table-test each of the five knobs at 0, 1 and negative.
- `medium` — Exclude `internal/service/mocks` from the coverage denominator so the number reflects hand-written code.
- `medium` — Add a `PreviewCache.Invalidate` test asserting a subsequent `Get` re-loads, plus an edit/delete test proving the service calls it.
- `medium` — Cover `buildEditPayload` with a fake cipher for both branches, and `blankQuotedBody` for its nil-in/nil-out contract.
- `low` — Collapse the `LoadHistory_*` and `pin_test.go` families into table-driven suites; ~170 near-duplicate functions are the main maintenance drag.

---

## 5. Maintainability — 3 / 5

Well-crafted code with exceptional WHY-comment discipline and tight per-concern files, held back by a 330-line untestable `main`, a repo-unique directory layout, and three hand-maintained decorators over a duplicated interface that make adding one room-read feature a four-file edit.

### Evidence

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | `main()` is 330 lines with 15 `os.Exit(1)` sites and no `run() error` seam, so all startup wiring (Vault cipher, three cache tiers, two breakers, a three-layer decorator stack, nine shutdown closers) is untestable and measured at 0.0%. **The wiring is where the outage-survival semantics live** — which source is base, which layer seeds Valkey — and a mis-ordered decorator would compile and ship silently | `cmd/main.go:90` |
| high | `service.RoomRepository` (8 methods) is re-declared verbatim as `readcache.RoomSource`, then hand-implemented method-by-method twice more by `breakerRoomRepo` and `RoomCache`; neither embeds. Adding one room read means editing the interface, the duplicate interface, two decorators, the mock and `roomTimesSeeder`. Five of `RoomCache`'s eight methods are pure delegation boilerplate | `internal/service/service.go:67`; `internal/readcache/readcache.go:157`; `cmd/roomrepo_breaker.go:26` |
| medium | The post-read enrichment pipeline (`redactUnavailableQuotes` → `setDecodedAttachments` → `resolveRemovedMemberNames`) is copy-pasted at 8 call sites, and 3 silently drop the third step with no comment saying why — the asymmetry is indistinguishable from a bug | `internal/service/messages.go:90-92`; `internal/service/threads.go:139-140`, `:383-384`; `internal/service/pin.go:209-210` |
| medium | Only service in the monorepo using `cmd/` + `internal/`; §1 forbids both. `internal/` also blocks `roomrepo_breaker.go` and `roomtimes_seeder.go` — real, tested logic — from ever being reused | `cmd/main.go:1` |
| medium | `internal/service/utils.go` is a 317-line junk drawer of five unrelated concerns; ~115 lines of it are one cohesive page-fitting algorithm that **already has its own 476-line test file named after it**. The test file names the concern the source file refuses to | `internal/service/utils.go:202-317`; `internal/service/pagefit_test.go` |
| low | `internal/cassrepo/utils.go` mixes three unrelated primitives — cursor codec, reflection-based `structScan`, and `QueryBuilder` | `internal/cassrepo/utils.go:19`, `:86`, `:107` |
| nitpick | `messages_test.go` is 3,088 lines / 131 KB and `write_integration_test.go` 1,773 lines; production code is only 8.0k of the service's 25.7k lines | `internal/service/messages_test.go:1` |

`CLAUDE.md` bans `utils` as a *package* name; a `utils.go` catch-all is the same anti-pattern one level down.

### Recommendations

- `high` — Extract `main()` into a testable `run(ctx, cfg) (closers, error)` plus small `wireSubscriptions()` / `wireRooms()` / `wireCiphers()` builders returning `(T, error)`; keep `os.Exit` only in `main`. This alone makes the decorator ordering assertable.
- `high` — Delete `readcache.RoomSource`, import `service.RoomRepository`, and have `RoomCache`/`breakerRoomRepo` **embed** it — as `roomTimesSeeder` already correctly does — overriding only the methods they actually intercept. Removes ~90 lines of delegation and makes interface growth a one-file change.
- `medium` — Introduce one `enrichPage(ctx, msgs, accessSince)` used by all eight sites; if threads and pinned genuinely must skip name resolution, express it as an explicit option with a WHY comment rather than an omission.
- `medium` — Split `internal/service/utils.go` into `pagefit.go` (matching the existing test file), `authz.go` and `quotes.go`; same treatment for `cassrepo/utils.go` → `cursor.go` / `scan.go` / `query.go`.
- `medium` — Migrate to the sanctioned flat layout, **or** amend `CLAUDE.md` §1 to sanction `cmd/`+`internal/`. One of the two must move: today the largest service is also the only one a reader cannot navigate by convention.
- `low` — Split `messages_test.go` along the handler boundaries the source already has; the source files are well-factored, the tests are not.

---

## 6. Integration — 3 / 5

Subject builders, cross-service RPC types and `docs/client-api.md` coverage are all correct and complete. The score is pulled down by one Cassandra mirror-write branch that diverges from every other write path in the service, and by inconsistent application of the event-level `Timestamp` convention.

### Evidence

| Sev | Finding | Evidence |
|-----|---------|----------|
| high | **Reactions on a `TShow=true` thread reply are never mirrored into `messages_by_room`**, so the channel-timeline copy of the row permanently lacks them | `internal/cassrepo/reactions.go:39` (and the identical branch at `:62`) |
| medium | Event-level `Timestamp` is bound to a *domain* timestamp instead of publish time, contrary to §"Event Timestamps" | `internal/service/messages.go:633`, `:554`; `internal/service/reactions.go:100`; `internal/service/pin.go:137` |
| low | The two derived doc views carry divergent `last synced` markers, so at least one is behind the other by construction — and both referenced commits are unreachable in this clone, so the claim cannot be checked mechanically | `docs/client-api/request-reply.md:3` vs `docs/client-api/events.md:3` |
| low | Publishes into `MESSAGES-CANONICAL-{siteID}` but has no `Bootstrap` config field and no `bootstrap.go`; the local compose cannot stand up against a fresh NATS on its own | `internal/config/config.go`; `deploy/docker-compose.yml:13` |

**On the reaction bug.** `AddReaction`/`RemoveReaction` route on `msg.ThreadParentID == ""` alone. Every other writer of the same row uses the wider rule `hasRoomTimelineRow` = `ThreadParentID == "" || TShow` — pin at `internal/cassrepo/pin.go:51`, edit at `write.go:269`, delete at `write.go:348`, and create at `message-worker/store_cassandra.go:291`. `reactions` is in the room-timeline projection (`internal/cassrepo/messages_by_room.go:16`), so `msg.history` / `msg.next` / `msg.surrounding` return the reply with **no** reactions while `msg.thread` returns it with them. The canonical `message_reacted` event masks this live; a reload exposes it.

**On the timestamps.** `deletedAtMs` comes from `actualDeletedAt`, which `MessageWriter.SoftDeleteMessage` documents (`internal/service/service.go:39-45`) as *the existing value when a concurrent delete won the race* — so the canonical event can carry a `Timestamp` materially older than the publish. `internal/service/pin.go:179` and `migration.go:78`, `:127` do it correctly and even carry the comment explaining the distinction, so the service already knows the rule.

### Verified clean

All 17 registrations go through `pkg/subject` builders with zero inline `fmt.Sprintf` (`internal/service/service.go:270-302`). All 13 `chat.user.…` subjects appear in `docs/client-api.md`, `client-api/request-reply.md` and, where they fan out, `client-api/events.md`, with request/response tables matching `internal/models/message.go` field-for-field. `RoomsGet`, `ThreadSubscriptionList` and both migration RPCs share `pkg/model` types with their only callers. `MESSAGE_BUCKET_HOURS` defaults to 360 identically across `message-worker`, `bot-message-worker`, `es-index-migrator` and this service; the sizer is threaded into every bucketed statement via `r.bucket.Of`; and `internal/config/config.go:270-279` actively warns when `MESSAGE_READ_MAX_BUCKETS` is too small for the configured window. No JetStream consumers, no OUTBOX/INBOX participation, no `idgen` use.

### Recommendations

- `high` — Replace the `msg.ThreadParentID == ""` branch in `reactions.go` with the existing `hasRoomTimelineRow(msg)` predicate, adding the `messages_by_room` cell write/delete for `TShow` replies alongside the thread mirror. Add an integration case that reacts to a `TShow=true` reply and asserts the reaction is readable from **both** `GetThreadMessages` and `GetMessagesBefore`.
- `medium` — Set `Timestamp: time.Now().UTC().UnixMilli()` at the four construction sites, leaving the domain values on `Message.EditedAt`/`UpdatedAt`/`PinnedAt` and the RPC responses untouched.
- `low` — Regenerate both derived views from the current `docs/client-api.md` in one commit so they carry the same `last synced` marker.
- `low` — Add `cmd/bootstrap.go` with the standard no-op helper (schema only: `Name + Subjects` from `pkg/stream`), a `Bootstrap` field on `config.Config`, and `BOOTSTRAP_STREAMS` in the local compose.
- `nitpick` — Add a short comment above the `reactions.go` mirror branch stating the `TShow` rule once fixed, so the next writer of this row does not re-derive it from `pin.go`.

---

## 7. Performance — 4 / 5

The read hot path is unusually well-engineered: every Mongo find carries an explicit projection, the two `$lookup`s carry justification comments, per-request reads are parallelised, user lookups are batched, and every goroutine has a termination path. The deductions are about *composition* — per-request bounds that multiply — and one over-fetching Cassandra read.

### Evidence

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **Per-request concurrency bounds compose multiplicatively** and nothing caps outstanding Cassandra work service-wide: `MAX_CONCURRENCY` defaults to 256 in-flight handlers, each `RoomsGet` spawns up to 16 concurrent room walks, and each walk fans out to 8 concurrent bucket queries — ~32k concurrent queries against `CASSANDRA_NUM_CONNS=8` per host. Each layer is individually bounded and documented; the product is not, and a cold `rooms.get` burst is exactly the shape that reaches it | `pkg/natsrouter/guard.go:21`; `internal/service/rooms.go:22`; `internal/cassrepo/repository.go:17`; `internal/config/config.go:33` |
| medium | `GetAllPinnedMessages` reads all 24 pinned columns — bodies, `attachments`, `enc_payload`, `quoted_parent_message` — while its only caller reads `MessageID`/`PinnedAt`. Its own doc comment says so | `internal/cassrepo/pin.go:41`, `:130`; `internal/service/pin.go:57-67` |
| low | `defaultWalkFanout = 8` is a compile-time constant, not an env knob, even though it is the primary lever on the clock-ceilinged walk: an idle room can cross up to `MESSAGE_READ_MAX_BUCKETS=122` buckets, i.e. ~16 sequential waves of Cassandra RTT | `internal/cassrepo/repository.go:17`; `internal/config/config.go:70` |
| low | Every paginated reply is JSON-encoded **twice**: `pagefit.rowSizes` encodes each row into a length counter, then the router marshals the same page for the wire. The measure pass runs unconditionally — `Fit`'s "fits comfortably" early return happens *after* `rowSizes`, so the common under-budget 100-row page pays full double encoding | `pkg/pagefit/pagefit.go:67-72`, `:144-158`; `internal/service/messages.go:95` |
| low | One OpenTelemetry span **per decrypted row**: `atrest.Decrypt` starts a span, invoked per message inside the scan loop and per room in `openStoredPreview`. A 100-message page emits 100 spans of span-processor and attribute allocation | `pkg/atrest/cipher.go:81-83`; `internal/cassrepo/messages_by_room.go:68`; `internal/mongorepo/room.go:112-137` |
| low | Fronts the shared user store with an L1 cache only and never mounts `userstore.TTLConfig` the way its peers do, re-declaring the L1 knobs inline — the sixth service to copy the same env tag and default | `internal/config/config.go:154-155`; `pkg/userstore/ttlconfig.go:13` |
| nitpick | `resolveRemovedMemberName` copies the whole `models.Message` into and back out of a one-element slice on every single-message read | `internal/service/sysmsgname.go:106-109` |

No `time.Sleep` synchronisation, no bare `Nak()`/`NakWithDelay(0)` (the service runs no JetStream consumers), no `mongo.ErrNoDocuments` mishandling, and no N+1 — user names go through `FindUsersByAccounts`, and the per-row DEK fetch is LRU-cached and singleflighted.

### Recommendations

- `medium` — Add a service-wide semaphore (or a `CASSANDRA_MAX_INFLIGHT` knob) shared by the bucket walker and `fetchByIDs`, sized against `NUM_CONNS × hosts`, so `MAX_CONCURRENCY × maxRoomsGetConcurrency × walkFanout` cannot be the effective query concurrency.
- `medium` — Give `GetAllPinnedMessages` its own `SELECT message_id, pinned_at FROM pinned_messages_by_room WHERE room_id = ?`; `pinnedByRoomQuery` stays for `GetPinnedMessages`.
- `low` — Promote `defaultWalkFanout` to `MESSAGE_READ_WALK_FANOUT` (envDefault 8), validated `>= 1`, and log it at startup beside `MESSAGE_READ_MAX_BUCKETS`.
- `low` — In `pagefit.Fit`/`FitWindow`, cheap-path the common case: measure the whole slice with one counting encode and compute per-row sizes only when that total exceeds the budget.
- `low` — Make the per-row `atrest.Decrypt` span conditional, or hoist one span around the page-level scan loop, so span cost scales per request rather than per row.
- `low` — Move `USER_CACHE_SIZE`/`USER_CACHE_TTL` into a `userstore` config type mounted as a named field, and mount `userstore.TTLConfig` so this service gets the same L2 outage buffer as its peers.

---

## 8. Prioritized action list

Ordered by severity first, then impact ÷ effort.

| # | Sev | Action | Dimension | Evidence | Why |
|---|-----|--------|-----------|----------|-----|
| 1 | `high` | Use `hasRoomTimelineRow(msg)` instead of `msg.ThreadParentID == ""` in the reaction mirror, and write/delete the `messages_by_room` cell for `TShow` replies | Integration | `internal/cassrepo/reactions.go:39`, `:62` | The only finding that **silently corrupts user-visible state**. Reactions on a shown thread reply vanish from the channel timeline on reload. Every sibling writer already uses the wider rule, so the fix is a one-line predicate swap plus a test. |
| 2 | `critical` | Unit-test the pure/mockable halves of `cassrepo` + `mongorepo` (cursor decode, bucket-range guards, `structScan`/`Fetch` error branches, pipeline builders) | Test coverage | `internal/mongorepo/pipelines.go:10`; `internal/cassrepo/messages_by_room.go:24` | Clears the 60% merge bar without touching the integration suite. Highest coverage gain per unit of effort in the service. |
| 3 | `high` | Test `startBucketFromCursor` in both directions (cursor above `defaultBucket`, below `floorBucket`, malformed, empty) | Test coverage | `internal/cassrepo/messages_by_room.go:24` | It is an **anti-abuse guard** against tampered cursors consuming `maxBuckets` empty reads — a DoS vector — and it is a pure function needing no Cassandra. |
| 4 | `high` | Refactor `checkConfig` → `validateConfig(cfg) error`, `main` keeps the `os.Exit`; table-test each knob | Test coverage / Maintainability | `cmd/main.go:43` | It is the only guard on `MESSAGE_BUCKET_HOURS`, whose mismatch `CLAUDE.md` says silently loses data across services. Currently untestable by construction. |
| 5 | `high` | Extract `main()` into `run(ctx, cfg) (closers, error)` plus `wireRooms`/`wireCiphers` builders | Maintainability | `cmd/main.go:90` | 330 lines at 0.0% coverage holding the outage-survival semantics; a mis-ordered decorator compiles and ships silently. Unlocks items 4 and 6. |
| 6 | `high` | Delete `readcache.RoomSource`; have `RoomCache`/`breakerRoomRepo` **embed** `service.RoomRepository` | Maintainability | `internal/readcache/readcache.go:157`; `cmd/roomrepo_breaker.go:26` | Turns a four-file edit into a one-file edit for every future room read, and removes ~90 lines of pure delegation. `roomTimesSeeder` already proves the pattern. |
| 7 | `medium` | Bound total Cassandra concurrency with one service-wide semaphore shared by the walker and `fetchByIDs` | Performance | `internal/cassrepo/repository.go:17`; `pkg/natsrouter/guard.go:21` | `256 × 16 × 8` composes to ~32k concurrent queries against 8 conns/host. Each layer is bounded; the product is not, and a cold `rooms.get` burst reaches it. |
| 8 | `medium` | Set `Timestamp` from `time.Now().UTC().UnixMilli()` at the four event-construction sites | Integration | `internal/service/messages.go:554`, `:633`; `internal/service/pin.go:137`; `internal/service/reactions.go:100` | A concurrent delete makes the canonical event carry a timestamp materially older than the publish. The service already documents the rule correctly elsewhere. |
| 9 | `medium` | Convert the nine bare `slog` request-path calls to `*Context` and add the missing `request_id`s | Code quality | `internal/service/threads.go:65`, `:121`, `:374`; `internal/service/messages.go:737`, `:742` | Publish/marshal failures currently cannot be joined to the request that caused them — exactly the lines an operator needs during an incident. |
| 10 | `medium` | Decide the layout question: flatten to the sanctioned shape, or amend `CLAUDE.md` §1 | Architecture / Maintainability | `cmd/main.go:1` | The repo's largest service is the only one a reader cannot navigate by convention, and `internal/` blocks reuse of genuinely reusable tested code. Cheap as a mechanical import rewrite; expensive as accumulated drift. |

### Verdict

**Ship-capable with one fix first.** Item 1 is a real data-visibility defect and should not wait. Items 2–4 are what stand between this service and the repo's own merge bar. The remaining items are quality-of-life for the team that maintains it, and item 10 is a decision the repo owes itself either way.
