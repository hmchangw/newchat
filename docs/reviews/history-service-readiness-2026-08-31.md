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

