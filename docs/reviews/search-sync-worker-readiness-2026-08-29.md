# Production-Readiness Review — `search-sync-worker`

| | |
|---|---|
| **Service** | `search-sync-worker` |
| **Date** | 2026-08-29 |
| **Branch** | `claude/search-sync-service-prod-ready-cj09nf` |
| **Overall score** | **3.2 / 5** |
| **Method** | Six parallel expert audits (code quality, architecture, test coverage, maintainability, integration, performance) against `CLAUDE.md` and industry practice for Go microservices at scale |

> Requested as `search-sync-service`; no such directory exists. Audited `search-sync-worker`, the closest match and the service the branch name refers to, as confirmed by the requester.

## Executive summary

`search-sync-worker` is a competently built ingestion worker with real engineering behind its hot path — a genuine ES `_bulk` pipeline with slot-bounded concurrency, per-item 429 classification, precomputed OTel attribute sets, disciplined `jsretry`/`errcode` poison-vs-retry tiering, zero raw `fmt.Sprintf` subject construction, and correct federation hygiene (it never bootstraps INBOX, leaving `inbox-worker` as sole owner). Unit-test craft is above the repo average: ~130 descriptively named subtests, error paths covered deliberately, metrics asserted against a real `ManualReader`. It is not, however, production-ready as it stands. One `critical` defect will crashloop the pod: the bot-message collection binds `BOT-MESSAGES-CANONICAL-{siteID}` but filters on `chat.msg.canonical.{siteID}.*` (`messages.go:129`), a subject that stream does not carry — consumer creation fails and `main.go:348` exits. A second contract gap silently under-restricts search results: `room_restricted` never reaches the user-room ACL index, so a room restricted after members joined leaves their docs unrestricted and full-history message search is granted. Structurally, the service is the only JetStream worker in the repo without a `bootstrap.go`, inlining stream creation at `main.go:310` with no production existence check; `main()` has grown to 319 lines and absorbed the entire consumer runtime; dependencies are injected by post-construction field mutation; `Collection.BuildAction` takes no `ctx`, forcing two `context.Background()` network calls (one an unbounded Mongo query) that sever tracing and outlive shutdown. Package coverage is **66.8%**, below the repo's 80% floor, concentrated almost entirely in an untestable `main.go` — excluding it the package sits at ~84.8%. `gosec` passes clean repo-wide; `govulncheck` and `semgrep` could not run in this sandbox (proxy 403 / tool absent) and remain unverified here.

## Dimension scores

| # | Dimension | Score | One-line verdict |
|---|---|---|---|
| 1 | Go code quality | **4 / 5** | Disciplined idioms and worker tiering; context propagation and one swallowed `Fetch` error are the gaps |
| 2 | Architecture | **3 / 5** | Clean federation and subject hygiene, undermined by the missing `bootstrap.go` and a `main.go` that became the runtime |
| 3 | Test coverage | **2 / 5** | Floored by the 80% rule at 66.8%; craft would otherwise merit 4 |
| 4 | Maintainability | **3 / 5** | Good `Collection` abstraction; wiring-by-mutation and a 319-line `main()` fight it |
| 5 | Integration | **3 / 5** | Excellent subject/stream discipline with one crashlooping filter mismatch and one ACL gap |
| 6 | Performance | **4 / 5** | Strong bulk design; missing deadlines and an uncached N+1 are the real risks |

**Average: 3.2 / 5**

## Findings by severity

| Severity | Count |
|---|---|
| `critical` | 1 |
| `high` | 15 |
| `medium` | 24 |
| `low` | 6 |
| `nitpick` | 6 |
| **Total** | **52** |

Counts are per-dimension and overlap by design: four experts independently flagged the missing `ctx` on `Collection.BuildAction`, three flagged the un-backed-off `Fetch` error loop, and two each flagged the absent `bootstrap.go` and the size of `main.go`. Convergence from independent lenses is the strongest signal in this report — those four items are the structural core of the remediation list in Chapter 8.

---

## 1. Go code quality — 4 / 5

Strong baseline: `jsretry`/`errcode` worker tiering is applied correctly (`handler.go:158-173`, `handler.go:317`), no `time.Sleep` for synchronization, no goroutine leaks (`consumer_source.go:49-55` is drained unconditionally by `main.go:554`), shutdown ordering is exactly right (`main.go:399-419`), metric attribute sets are nil-safe and precomputed (`metrics.go:362-389`), and compile-time interface assertions guard the seams (`spotlight_org.go:248`, `consumer_source.go:31-34`). No bare `err` returns on domain paths. Deductions concentrate in context propagation, one swallowed error, and test-only API left in production code.

### SAST (blocking CI gate, CLAUDE.md §5)

| Scan | Result |
|---|---|
| `gosec` | **PASS** — 0 medium+ findings repo-wide (`-tests=true`) |
| `govulncheck` | **FAIL (environmental)** — `vuln.go.dev` blocked by the agent proxy (`CONNECT tunnel failed, response 403`) |
| `semgrep` | **FAIL (environmental)** — binary not installed in this sandbox |

No SAST finding touches `search-sync-worker/`. The two failures are tooling/network, not code — but the gates are **unverified** here and must be confirmed in CI before this ships. `medium`

### Findings

- `high` — **`Fetch` error discarded entirely: no log, no metric, no backoff, no comment.** `main.go:538-550` `continue`s immediately, so a persistent failure (consumer deleted, NATS down, auth revoked) becomes a silent hot spin with zero operator signal. Direct violation of CLAUDE.md §3 "never ignore errors silently — comment if intentionally discarded". Compounded by `consumer_source.go:38` returning the raw `jetstream` error unwrapped, so even once logged it would not name the consumer.
- `high` — **Network I/O under `context.Background()` on the hot path.** `Collection.BuildAction(data []byte)` (`collection.go:42`) carries no `ctx`, yet two implementations do real I/O behind it: an ES self-lookup at `messages.go:220` (bounded by `parentResolveTimeout`, `thread_parent_resolver.go:33`) and an **unbounded Mongo query** at `messages.go:304`. `pkg/mongoutil` sets no client-level timeout, so a stalled Mongo blocks the teams consumer goroutine indefinitely and shutdown cannot cancel it. Trace context is severed at both hops.
- `medium` — **No request-ID propagation anywhere in the service.** `message-gatekeeper/handler.go:508` stamps `X-Request-ID` onto the canonical message via `natsutil.NewMsg(ctx, …)` and sibling workers log it (`message-worker/handler.go:81,109`), but `search-sync-worker` never calls `natsutil.StampRequestID`/`RequestIDFromContext` — zero hits. Every error log (`handler.go:91,106,128,293`) is uncorrelatable to the producing request. Violates CLAUDE.md §3 "Request Logging & Tracing".
- `medium` — **Test-only API in production code.** `Handler.Add` (`handler.go:81-83`), `Handler.Flush` (`handler.go:223-225`) and `Handler.MessageCount` (`handler.go:363-367`) have 42 call sites, all in `*_test.go`. `Add` additionally hardcodes `context.Background()`. The variadic `tracers ...trace.Tracer` in `NewHandler` (`handler.go:63`) exists for the same reason and is an optional-arg anti-pattern. CLAUDE.md §4: "test helpers belong in `_test.go` files only — NEVER put test helpers in production code."
- `medium` — **Trace correlation dropped on the two highest-severity log lines.** `handler.go:242` and `handler.go:261` use `slog.Error` (no context) inside a function that holds the span-carrying `bulkCtx` created at `handler.go:235`; `handler.go:293` in the same function correctly uses `ErrorContext`. Same pattern at `messages.go:240,247,306,373`. Repo-wide style is `slog.*Context` (`broadcast-worker/handler.go:195,204,469`).
- `medium` — **Possible message-body leak into logs.** `handler.go:295` logs `results[i].Error` verbatim. ES `mapper_parsing_exception` reasons routinely embed the offending field value — i.e. message content — into the reason string. `bulkItemError` (`handler.go:158-164`) deliberately excludes it from the returned error; the log line does not. CLAUDE.md §3 forbids logging full message bodies.
- `medium` — **Exported ES document type stranded in `package main`.** `SpotlightOrgIndex` (`spotlight_org.go:193`) plus `spotlightOrgTemplateBody` (`spotlight_org.go:205-244`) live in the service, while every peer doc type lives in `pkg/searchindex` (`messagedoc.go:17`, `spotlightdoc.go:8`). The reader duplicates the shape by hand at `search-service/response.go:127` with copied JSON tags — silent-drift hazard, no compiler link. Naming also breaks the `…Doc` convention, and it is exported from a package nothing can import (CLAUDE.md §3: "export only what other packages consume").
- `medium` — **`_update_by_query` runs synchronously inside the fetch loop.** `handler.go:112`. A room-rename over a large spotlight index blocks the collection's consumer for the full cluster round-trip while already-buffered messages sit unacked, burning their `AckWait`.
- `low` — **`json.Marshal` errors discarded without comment** at `messages.go:279` and `messages.go:376`, unlike `messages.go:160` / `spotlight.go:47` which justify theirs. `buildDocument` returns `nil` on failure and still emits a `BulkAction`.
- `nitpick` — **`context.Context` stored in a struct.** `handler.go:26` (`pendingMsg`). Pragmatic for span linking across the batch boundary, but the `containedctx` smell deserves a one-line justification.

### Recommendations

1. `high` — Log at warn + record a metric + apply jittered backoff on the `Fetch` error at `main.go:539`, and wrap the error at `consumer_source.go:38` (`fmt.Errorf("fetch from %s: %w", …)`).
2. `high` — Add `ctx` to `Collection.BuildAction`/`BuildActionSeq` (`collection.go:42`) and thread `AddWithContext`'s context through, deleting both `context.Background()` calls; give `ResolveIdentities` an explicit `context.WithTimeout`.
3. `medium` — Call `natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())` at the top of `AddWithContext` and add `request_id` to every log line in the service.
4. `medium` — Delete `Add`, `Flush`, `MessageCount` and the variadic tracer; have tests call `AddWithContext` / `Take`+`FlushBatch` and inject the tracer as a plain struct field.
5. `medium` — Convert `handler.go:242,261` and `messages.go:240,247,306,373` to `slog.*Context`; drop or redact `results[i].Error` at `handler.go:295`.
6. `medium` — Move `SpotlightOrgIndex` and its template body into `pkg/searchindex` as `SpotlightOrgDoc` and have `search-service/response.go` import it instead of hand-copying the shape.
7. `medium` — Run `govulncheck` and `semgrep` in CI and confirm both are green; neither gate could be verified in this sandbox.

---

## 2. Architecture — 3 / 5

### What is right (no action needed)

Worth stating plainly, because it is the load-bearing half of this dimension. Every subject comes from a `pkg/subject` builder — **zero** raw `fmt.Sprintf` subject construction anywhere in the service (`inbox_stream.go:36`, `messages.go:125,129`, `spotlight_org.go:81`). Every `jetstream.StreamConfig` is `Name + Subjects` copied from `pkg/stream` (`messages.go:99-112`, `inbox_stream.go:27-33`, `spotlight_org.go:71-74`) — no inline literals, and no `Sources`/`SubjectTransform` anywhere, so `inbox-worker` remains the sole INBOX owner as CLAUDE.md §6 requires. INBOX and HR bootstrap are explicitly excluded (`main.go:306-323`). Shutdown order is exactly `stop → drain workers → nc.Drain → Mongo → health → obs` (`main.go:399-419`). No `os.Getenv`; `caarlos0/env` with `required` on all secrets and URLs. No bare `Nak()` — `jsretry` throughout (`handler.go:114,317,355`). `Store`, `msgFetcher`, `parentCreatedAtResolver` and `teamsUserResolver` are all consumer-defined interfaces.

### Findings

- `high` — **No `bootstrap.go`, no `bootstrapStreams` helper, no production stream verification.** CLAUDE.md §6 mandates `bootstrapStreams(ctx, js, siteID, enabled) error` in `bootstrap.go`; this is the **only** JetStream service in the repo without one — eleven peers have it (`inbox-worker/bootstrap.go`, `message-worker/bootstrap.go`, `hr-sync-worker/bootstrap.go`, …). The logic is inlined at `main.go:310-323`, which the guideline explicitly forbids ("never inline"). Worse, unlike every peer it has **no `else` branch verifying the stream exists** when bootstrap is disabled: peers fail fast at startup (`inbox-worker/bootstrap.go:59-61`), whereas here a misprovisioned production deploy surfaces only as an opaque `create consumer failed`. It is also untestable — there is no `streamManager` seam like the peers have.
- `high` — **`Fetch` error is silently discarded and busy-loops.** `main.go:538-550`: on error the loop checks `stopCh`, maybe flushes, then `continue`s with no log, metric, backoff or comment. During a NATS outage all five collection goroutines spin at full CPU indefinitely. (Same defect as Code Quality finding 1 — flagged independently by three experts.)
- `high` — **Unbounded, context-less Mongo lookup on the synchronous ingest path (teams mode).** `messages.go:304` calls `ResolveIdentities(context.Background(), …)`, reaching `teams_user_store.go:37,55` with no deadline, and `pkg/mongoutil` sets no client-level timeout. A stalled Mongo blocks the consumer loop's `add` and therefore blocks graceful shutdown past the 25s budget. The root cause is architectural: `Collection.BuildAction(data []byte)` (`collection.go:42`) has no `ctx` parameter, forcing `context.Background()` here and at `messages.go:220`.
- `medium` — **Runtime machinery has leaked into `main.go`.** Of 618 lines only ~320 are config/wiring/startup/shutdown. The rest belongs in its own file per the per-service layout in CLAUDE.md §1: `flushPipeline` (`main.go:438-480`), `consumerTuning`/`runConsumer` — the actual message-processing loop (`main.go:482-569`), `checkBatchAckCoupling` (`main.go:575-588`), the `Store` implementation `engineAdapter` (`main.go:590-601`, which should be `store_search.go`), and `consumerSource`/`buildConsumerConfig` (`main.go:603-618`, whose sibling `msgFetcher` already lives correctly in `consumer_source.go`).
- `medium` — **Dependencies injected by post-construction field mutation, not by constructor.** `main.go:219,225-226,231-232,361` set `.teamsUsers`, `.parentResolver` and `.metrics` after `newXCollection`/`NewHandler` have returned. `NewHandler`'s variadic `tracers ...trace.Tracer` (`handler.go:63`) is optional-arg-by-variadic. Both contradict CLAUDE.md §3 ("handler structs hold dependencies injected via constructor") and leave every collection legally constructible in a half-wired state — which the code then has to nil-guard for at `messages.go:214,297`, silently degrading rather than failing.
- `medium` — **The JetStream consumer pattern is an undocumented third variant.** Neither `cons.Messages()`+`MAX_WORKERS` semaphore nor `cons.Consume()` (CLAUDE.md §6); it is a `cons.Fetch()` loop plus `flushPipeline` (`main.go:498-569`). It is the only service in the repo using `.Fetch(`, and there is no `MAX_WORKERS`. The ES-bulk rationale is sound and well-commented — batching for `_bulk` genuinely does not fit either sanctioned pattern — but it is an unratified deviation a future maintainer cannot discover from the guidelines.
- `low` — **`Collection.StreamConfig`'s doc comment invites forbidden configuration.** `collection.go:15-18` describes the method as letting a collection "configure Subjects, Sources, SubjectTransforms, etc." — precisely what CLAUDE.md §6 forbids app code from setting on INBOX. Every implementation behaves correctly, but a consumer-only worker needs only a stream *name*.
- `nitpick` — **`Handler.Add` (`handler.go:81-83`) is production API with only test callers**, and discards trace context via `context.Background()`.

### Recommendations

1. `high` — Add `search-sync-worker/bootstrap.go` with a `streamManager` seam and `bootstrapStreams(ctx, js, siteID, mode, enabled) error` mirroring `hr-sync-worker/bootstrap.go`: create when enabled, **`js.Stream(ctx, name)`-verify and fail fast when disabled**, keeping the INBOX/HR skip inside the helper. Move the `createdStreams` dedup there and delete `main.go:302-323`.
2. `high` — On `Fetch` error: log at warn with the collection name, record a metric, and sleep an equal-jittered backoff (reuse `jsretry`'s curve shape) before continuing; keep the `stopCh` check first.
3. `high` — Add `ctx` to `Collection.BuildAction`/`BuildActionSeq`/`BuildByQuery` and thread `AddWithContext`'s context through, then wrap the teams identity lookup in an explicit `context.WithTimeout`. This removes both `context.Background()` sites and restores trace correlation on the parent-resolve span.
4. `medium` — Split `main.go` into `consumer.go` (`runConsumer`, `consumerTuning`, `flushPipeline`, `checkBatchAckCoupling`, `consumerSource`, `buildConsumerConfig`) and `store_search.go` (`engineAdapter`), leaving `main.go` at config/wire/start/stop only.
5. `medium` — Convert the post-construction field sets into constructor parameters or functional options (`newMessageCollection(..., withParentResolver(r), withMetrics(m))`, `NewHandler(store, coll, bulkSize, WithMetrics(m), WithTracer(t))`) so no collection or handler can be observed half-wired.
6. `low` — Rewrite the `Collection.StreamConfig` doc comment to state it returns schema only (`Name` + `Subjects` from `pkg/stream`), or narrow the interface to `StreamName(siteID) string` plus a bootstrap-only schema accessor.
7. `low` — Document the `Fetch`+pipeline consumer pattern as a sanctioned third option in CLAUDE.md §6 (bulk-sink workers), or record the deviation rationale in a `consumer.go` header comment.

---

## 3. Test coverage — 2 / 5

**Score floored at 2 by the CLAUDE.md §4 coverage rule.** Test *craft* in this service would otherwise merit a 4 — see "Positive signals" below.

### Measurements

| Check | Result |
|---|---|
| `make test SERVICE=search-sync-worker` | **PASS** (`go test -race ./search-sync-worker/...`, ok, 2.06s) |
| `go vet -tags=integration ./search-sync-worker/...` | clean — integration suite compiles |
| `go tool cover -func` total | **66.8% of statements** |
| `make generate SERVICE=search-sync-worker` | **no diff** — mocks are current, nothing to revert |
| Working tree after audit | clean (`git status --porcelain` empty) |

Uncovered statements by file: `main.go` 165/225 · `teams_user_store.go` 24/24 · `thread_parent_resolver.go` 18/24 · everything else ≤10. **Excluding `main.go` the package sits at ~84.8%** — this is one concentrated hole, not a diffuse deficit.

### Findings

- `high` — **coverage below repo minimum 80%, currently 66.8%.**
- `high` — **`main.go:102` `main` — 0.0%, ~160 untestable statements.** Lines 108-160 are a startup-validation cascade (MODE enum; `FETCH_BATCH_SIZE`/`BULK_BATCH_SIZE`/`BULK_FLUSH_INTERVAL`/`PIPELINE_DEPTH` > 0; `-v<N>` suffix on all three index names; `SYNC_MESSAGES_FROM` parse) written inline with an `os.Exit(1)` at each site, so none of it is reachable from a test. Lines 210-240 carry real logic too — the mode-gated collection set and per-collection resolver/metrics wiring. `config_test.go` exercises only 4 fields via `env.ParseAs`; the validation rules themselves are entirely untested.
- `high` — **`teams_user_store.go:32` `ResolveIdentities` — 0.0%, no test of any kind.** This is a store implementation (two Mongo `FindMany` calls with projections plus an in-app account→id join) and CLAUDE.md §4 requires integration tests with testcontainers for store implementations. `testutil.MongoDB(t, prefix)` is available and unused anywhere in this service. The consumer path is tested only against `fakeTeamsUserResolver` (`messages_test.go:634`), so an `accountToID` mapping bug or a wrong projection ships silently — and per the comment at `config_test.go:57` a resolver miss is **durable**: the message is Acked with empty author fields and nothing retries.
- `medium` — **Integration tests use hardcoded static ES index names with zero teardown.** `integration_test.go:254` (`"msgs-inttest-v1"`), `:502` (`"analyzer-test-v1"`), `:635` (`"spotlightorg-site-org-int-v1"`), `inbox_integration_test.go:154,247`. There is not a single `t.Cleanup` or index DELETE across the three integration files, and `testutil.ElasticsearchIndex(t, prefix)` is never called. CLAUDE.md §4 requires "a per-test unique index name and DELETE on cleanup". Isolation currently rests entirely on hand-picked distinct literals against a process-shared ES.
- `medium` — **Package-level shared NATS connection across all integration tests.** `integration_test.go:34-38` (`testNATSCon`/`testNATSOnce`), closed only in `TestMain`. CLAUDE.md §4 asks for a per-test `*nats.Conn` with a `Drain` cleanup; this is shared mutable state spanning tests.
- `medium` — **`handler.go:104-110` `BuildByQuery` poison path (Ack + `dispAckedPoison`) is uncovered.** The by-query success and store-error branches are both tested (`handler_test.go:116,128`); only the parse-error branch is not. Same for `handler.go:90-95`, the `natsutil.DecodePayload` failure branch — `TestHandler_Add_MalformedJSON` (`handler_test.go:95`) exercises malformed *JSON*, which fails later in `BuildAction`, not a corrupt-zstd decode.
- `medium` — **`messages.go:305` `resolveTeamsIdentities` resolver-error branch uncovered** (function at 80.0%). The fake never returns an error, so the "degrade to nil identities" behaviour is unverified.
- `low` — **`main.go:595,599` `engineAdapter.Bulk`/`UpdateByQuery` at 0.0%**, and `messages.go:104,109` `botMessagesStreamCfg`/`teamsMessagesStreamCfg` at 0.0%. Thin passthroughs — but the two stream configs are wire-facing constants worth pinning, and the bot one is wrong today (see Chapter 6, `critical`).
- `nitpick` — **`integration_test.go:176` `time.Sleep(2s)`** inside the ES-health poll. It is a poll interval, not goroutine synchronization, so it does not breach the CLAUDE.md §3 rule — but `require.Eventually` would be idiomatic.

### Positive signals (why craft ≠ score)

Unit tests are genuinely strong and should not be rewritten in the course of fixing the above: 100+ test functions, ~130 `t.Run` subtests with descriptive names, table-driven wherever variation exists (`consumer_config_test.go:21`, `spotlight_test.go:312`, `messages_test.go:764`). Error paths are covered deliberately rather than incidentally — 404-on-delete, retry pacing with jitter bounds (`handler_test.go:60` `assertJitteredDelay`), poison-vs-nak dispositions. Metrics are asserted against a real OTel `ManualReader` rather than a mock (`metrics_test.go:20`), fresh per test. Per-subtest `gomock.NewController`; `t.Setenv` throughout, never raw `os.Setenv`; no `t.Parallel` colliding with env; no shared mutable globals in unit tests. `TestMain` (`integration_test.go:44`) correctly uses `testutil.PrewarmFailFast` + `TerminateAll`, with the custom-wrap deviation justified in a comment. **No inline `testcontainers.GenericContainer` anywhere.** `testdata/events.json` follows the fixture convention. Every commit touching the service ships tests alongside implementation.

### Recommendations

1. `high` — Extract the `main.go:108-160` validation cascade into `func (c config) validate() (time.Time, error)` returning a wrapped error, with `main` doing one `slog.Error` + `os.Exit(1)`. Table-drive it in `config_test.go`, one case per rule plus the valid baseline. This alone converts ~50 dead statements into covered ones and makes the rules regression-proof.
2. `high` — Extract the mode-gated collection assembly (`main.go:210-240`) into `buildCollections(cfg, engine, db, esMetrics) []Collection`. Assert: `teams` mode yields exactly the Teams collection with a non-nil `teamsUsers`; `default` yields the five collections each with a wired `parentResolver` and per-collection metrics. (This pairs naturally with the constructor-options fix in Chapter 2.)
3. `high` — Add `teams_user_store_integration_test.go` (`//go:build integration`, `testutil.MongoDB(t, "teamsuser")`) covering: empty input; a `teams_user` hit with no matching `users` row (UserID empty); full resolution; blank-account rows skipped; IDs with no `teams_user` row absent from the map.
4. `medium` — Switch integration tests to `testutil.ElasticsearchIndex(t, prefix)` (or derive from `t.Name()`) and register `t.Cleanup` DELETEs, per CLAUDE.md §4.
5. `medium` — Give each integration test its own `*nats.Conn` with a `Drain` cleanup instead of the package-level `testNATSCon`.
6. `medium` — Add two `Handler` subtests: a corrupt-zstd payload (`handler.go:90`) and a `BuildByQuery` returning an error (`handler.go:104`), both asserting Ack + `dispAckedPoison`.
7. `low` — Extend `fakeTeamsUserResolver` with an error-injection field and assert `resolveTeamsIdentities` degrades to `nil` rather than propagating (`messages.go:305`).

---

## 4. Maintainability — 3 / 5

Well-commented, well-tested code with one genuinely good abstraction (`Collection`, `collection.go`), undermined by a `main.go` that has become the service's runtime, a missing repo-standard `bootstrap.go`, and wiring by struct-field mutation.

### Findings

- `high` — **`main()` is 319 lines and does far more than wire.** `main.go:102-420`, ~45 branch points (cyclomatic ≈ 25-30). Inside it: 8 inline config validations (`:126-160`), a template upsert loop (`:243-254`), stored-script registration (`:263-271`), inline stream bootstrap (`:310-323`), a hardcoded HR-domain consumer special case (`:330-358`), per-collection handler wiring, goroutine launch and shutdown. `main.go` also holds the entire consumer runtime — `flushPipeline` (`:448-480`), `runConsumer` (`:498-569`, ~72 lines), `checkBatchAckCoupling` (`:575-588`), `engineAdapter` (`:591-601`), `buildConsumerConfig` (`:611`). CLAUDE.md §1 scopes `main.go` to config/wiring/startup/shutdown; peer `message-worker/main.go` is 351 lines and `inbox-worker/main.go` is 1020 of which 690 is a store implementation — both keep their loops out of `main()`.
- `high` — **No `bootstrap.go` — the only JetStream service in the repo without one.** Eleven peers expose `bootstrapStreams(ctx, js, siteID, enabled) error`; this service inlines `js.CreateOrUpdateStream` at `main.go:314-322`, which CLAUDE.md §6 explicitly forbids. It also skips the peers' production path (verify-and-fail-fast). The INBOX/HR skip is a name comparison (`main.go:307-308,314`) rather than a `Collection` capability, so a third externally-owned stream means a third string literal.
- `high` — **Dependencies injected by mutating unexported fields from `main`.** `main.go:219` (`teamsMsgColl.teamsUsers = …`), `:225-226` (`msgResolver.metrics`, `msgColl.parentResolver`), `:231-232`, `:361` (`handler.metrics`). Constructors silently produce half-built objects; forget a line and the collection degrades quietly (`resolveTeamsIdentities` returns nil on a nil resolver, `messages.go:297`). Peers use an options pattern (`inbox-worker/handler.go:158` `HandlerOption`, `broadcast-worker/handler.go:98`). `NewHandler`'s `tracers ...trace.Tracer` (`handler.go:63`) is a variadic that exists only for tests.
- `high` — **`BuildAction([]byte)` has no `context`, so blocking I/O escapes through `context.Background()`.** `collection.go:42` fixes the signature, forcing `messages.go:220` (ES parent lookup, 2s timeout) and `messages.go:304` (Mongo two-collection lookup, unbounded) to fabricate their own context. Both run on the fetch loop; both drop the trace and deadline the o11y consumer span just established — the exact correlation CLAUDE.md's observability rules exist to preserve. `esParentResolver` records latency metrics into a detached context (`thread_parent_resolver.go:33`).
- `medium` — **`SpotlightOrgIndex` is exported from `package main` and hand-mirrored in another service.** `spotlight_org.go:193` plus `spotlightOrgTemplateBody` (`:205-244`) put an index doc + ES template in the service instead of `pkg/searchindex/`, where `MessageDoc`, `SpotlightDoc` and `UserRoomDoc` all live. `search-service/response.go:123-134` duplicates the nine fields by hand, with a comment admitting the mirror. Exported but unimportable. Renaming a field breaks search silently.
- `medium` — **Adding one INBOX event type touches every INBOX collection.** Concrete trace: `pkg/subject.InboxMemberEventSubjects` → the `spotlight.go:85` switch (`default:` returns an error → Ack-drop) → the `user_room.go:80` switch (same). Collections that do not care must add an explicit skip, exactly as `user_room.go:52` already does for `InboxRoomRenamed` via `peekInboxEventType`. The safe default for a new event type is therefore *silent poison-drop in every collection that ignores it* — a failure mode that produces no signal.
- `medium` — **`teams` mode requires and validates four env vars it never uses.** `SPOTLIGHT_INDEX`, `SPOTLIGHT_ORG_INDEX`, `USER_ROOM_INDEX` and `HR_CENTRAL_SITE_ID` are `required` (`main.go:61-67`) and version-validated (`:148-155`), but only reached in the `else` branch (`:237-239`). Inversely, Mongo is connected unconditionally (`:198`) though `db` is used solely at `:219` in teams mode — so a default-mode pod carries a required `MONGO_URI` and a live connection pool it never queries.
- `medium` — **Test-only API in production code.** `Handler.Add` (`handler.go:81`), `Handler.Flush` (`:223`), `Handler.MessageCount` (`:363`) have zero production callers — production uses `AddWithContext`, `Take`+`FlushBatch`, `ActionCount`. Three exported methods and ~30 lines of doc comment maintained solely for tests.
- `low` — **Duplication.** `spotlight.go:71-81` and `user_room.go:61-77` repeat the same roomId/accounts/empty-account validation; `user_room.go:81-102` has two identical `BulkAction` literals differing only in `body`; `messages.go:99-112` is three copies of one three-line function.
- `nitpick` — **Stale comments.** `consumer_source.go:24` still says "runConsumer (Task 2) *will* hold a msgFetcher" (it does); `handler.go:351` says "the two defensive paths in *Flush*" (they are in `FlushBatch`); `collection.go:13` says implementing `Collection` is enough to add a collection — it is not (registration + metrics + resolver wiring in `main` are also required).

### Recommendations

1. `high` — Extract `bootstrap.go` with `bootstrapStreams(ctx, js, collections, siteID, enabled) error` matching the peer signature, including the production `Stream()` existence check. Replace the `inboxName`/`hrName` string comparisons with a `Collection` method (`OwnsStream() bool`).
2. `high` — Move `runConsumer`, `flushPipeline`, `consumerTuning`, `checkBatchAckCoupling` into `consumer.go` and `engineAdapter` into `store_search.go`; move config parsing/validation into `config.go`. Target a `main()` under 120 lines that only wires.
3. `high` — Add `ctx context.Context` to `Collection.BuildAction`/`BuildActionSeq`/`BuildByQuery` and thread the o11y message context from `Handler.AddWithContext`, deleting both `context.Background()` calls in `messages.go`.
4. `high` — Convert post-construction mutation to constructor options: `newMessageCollection(..., withParentResolver(r), withMetrics(m))`, `NewHandler(store, coll, size, withHandlerMetrics(m))`. Drop the `tracers ...` variadic.
5. `medium` — Move `SpotlightOrgIndex` and `spotlightOrgTemplateBody` to `pkg/searchindex/spotlightorgdoc.go` and have `search-service` import it, deleting the hand-copied `orgSearchHit`.
6. `medium` — Split config by mode (a `teamsConfig`/`defaultConfig`, or make the four indices non-required and validate them only on the branch that uses them), and skip the Mongo connect entirely when `Mode != "teams"`. Name `"default"`/`"teams"` as `const modeDefault`/`modeTeams`.
7. `low` — Hoist the shared member-event validation into `inbox_stream.go` (e.g. `validateMemberPayload(payload) error`) and give `Collection` an explicit "ignored event types" hook, so a new INBOX type fails loudly at registration rather than Ack-dropping.

---

## 5. Integration — 3 / 5

Integration hygiene is genuinely strong — zero raw subject construction, no INBOX/HR bootstrap, `jsretry` everywhere, no client-facing surface — but one cross-service contract is broken outright and one ACL-relevant event lane is missing.

### Findings

- `critical` — **The bot-message consumer's filter subject does not exist on the stream it binds.** `messages.go:129` returns `subject.MsgCanonicalMessageWildcard(siteID)` = `chat.msg.canonical.{siteID}.*`, but that collection binds `BOT-MESSAGES-CANONICAL-{siteID}` (`messages.go:104-107`), whose only subject is `chat.bot.canonical.{siteID}.>` (`pkg/stream/stream.go:97`, `pkg/subject/bot.go:73`). The producer publishes `chat.bot.canonical.{siteID}.created` (`bot-message-handler/handler.go:199`), and the sibling consumer gets it right (`bot-message-worker/main.go:208`). The filter is not a subset of the stream's interest, so `js.CreateOrUpdateConsumer` fails and `main.go:348-355` calls `os.Exit(1)` — **the pod crashloops in `default` mode wherever the bot stream exists**; where it does not exist, bot messages are simply never indexed. `messages_test.go:621-622` asserts the wrong string, so nothing catches it, and no integration test wires the bot collection.
- `high` — **`room_restricted` never reaches the user-room ACL index.** `subject.InboxMemberEventSubjects` (`pkg/subject/subject.go:332-343`) filters only `member_added`/`member_removed`/`member_joinedat_refreshed`/`room_renamed`. `HistorySharedSince` is written into `restrictedRooms` only from member events (`user_room.go:271-274`), yet `room_restricted` is produced independently (`room-service/handler.go:2123`, outbox lane `pkg/outbox/outbox.go:30`) and applied to Mongo locally with no INBOX-internal publish. So a room restricted **after** its members joined leaves every pre-existing member's MV doc unrestricted, and `search-service/query_messages.go:127` (via `store.go:32 RestrictedRooms`) then grants full-history message search. This is an access-control regression, not merely staleness.
- `medium` — **`context.Background()` inside `BuildAction` severs trace and shutdown propagation.** `messages.go:220` (ES thread-parent resolve) and `messages.go:304` (Mongo Teams identity resolve) both discard the consumer-span context that `AddWithContext` carefully threads through (`handler.go:88`). Traces break at exactly the two cross-service hops, and neither call is cancelled by `shutdown.Wait`.
- `medium` — **Third JetStream consumer pattern, undocumented in CLAUDE.md.** `runConsumer` (`main.go:498-568`) is a `cons.Fetch()` batch loop with a `flushPipeline`, not `cons.Messages()`+semaphore nor `cons.Consume()`. Well-reasoned for ES bulk, but undiscoverable from §6. The `(depth+1)*bulk ≤ MaxAckPending` coupling (`main.go:575`) is only a warning, while the compose file must override `CONSUMER_MAX_ACK_PENDING=1500` (`deploy/docker-compose.yml:40`) for batching to work correctly — a silent misconfiguration away from stalling.
- `medium` — **Message index-name agreement is by convention only.** The writer builds `{MSG_INDEX_PREFIX}-{YYYY-MM}` (`pkg/searchindex/messagedoc.go:117`); the reader hardcodes `[]string{"messages-*", "*:messages-*"}` (`search-service/query_messages.go:18`). `SPOTLIGHT_INDEX`/`SPOTLIGHT_ORG_INDEX`/`USER_ROOM_INDEX` are shared env vars, but the message prefix is not — a prefix rename silently yields zero hits with no error on either side.
- `low` — **`USER_ROOM_INDEX` is the only index env with no startup validation** (`main.go:67`, versus the `StripVersion` checks at `main.go:144-155`), and unlike `search-service` (`main.go:97-99`) it lacks `notEmpty`, so an empty value reaches ES as an empty index name.
- `nitpick` — **`Collection.StreamConfig`'s doc comment invites `Sources`/`SubjectTransforms`** (`collection.go:15-18`), which CLAUDE.md forbids for INBOX; every implementation correctly returns `Name`+`Subjects` only.

### Verified clean

Every subject comes from a `pkg/subject` builder — no `fmt.Sprintf` subjects anywhere. `buildConsumerConfig` (`main.go:611-618`) uses `stream.DurableConsumerDefaults` with no hardcoded `cc.BackOff`. No bare `Nak()`/`NakWithDelay(0)` — only `jsretry.Nak`/`SettleQuiet` (`handler.go:114,317,355`). INBOX and HR bootstrap explicitly skipped (`main.go:306-314`), preserving `inbox-worker` as sole INBOX owner. The service **publishes no NATS events**, so the `Timestamp int64` rule is N/A. It registers **no `chat.user.*` subject and no HTTP route** beyond `health.ServeWithPprof` — so `docs/client-api.md` correctly needs no entry for it. No Cassandra or `msgbucket` use, so `MESSAGE_BUCKET_HOURS` is correctly absent. No ad-hoc ID generation. Consumed envelopes match their producers: `model.InboxEvent`+`InboxMemberEvent` from `room-worker/handler.go:1205,476,2378`; `model.MessageEvent` from `message-gatekeeper/handler.go:507`; `TeamsBatchRequest` from `message-worker/main.go:243`; the HR array from `teams-hr-sync/publisher.go:38`.

### Recommendations

1. `critical` — Fix the bot filter subject to `subject.BotCanonicalWildcard`/`BotCanonicalCreated` and correct the assertion at `messages_test.go:622`; add an integration test that creates **every** default-mode consumer against real streams, so a filter/stream mismatch fails CI instead of at rollout.
2. `high` — Publish `room_restricted` on the INBOX internal lane and add it to `InboxMemberEventSubjects`, with a `userRoomCollection` branch that updates `restrictedRooms`; or re-emit member events on a restriction change.
3. `medium` — Thread `ctx` through `Collection.BuildAction`/`BuildActionSeq` and drop both `context.Background()` calls in `messages.go`.
4. `medium` — Promote `checkBatchAckCoupling` (`main.go:575`) from warn to fail-fast — it already computes the exact remedy, and this matches the fail-fast-on-bad-config rule the file's other checks follow.
5. `medium` — Share the message index prefix as a common env var read by both `search-service` and `search-sync-worker`, or derive the reader's pattern via a `searchindex.IndexPattern` helper instead of the literal `"messages-*"`.
6. `low` — Add `,notEmpty` plus a name-shape check for `USER_ROOM_INDEX`.
7. `low` — Document the `Fetch`+pipeline consumer pattern as a sanctioned third option in CLAUDE.md §6, or note in `main.go` why it deviates.

---

## 6. Performance — 4 / 5

Genuinely strong bulk-indexing design: a real `_bulk` API (never per-document), a slot-bounded flush pipeline, per-item 429/backpressure classification, precomputed metric attribute sets, an ack-pending/batch-size coupling check, and existing benchmarks. Findings concentrate in missing deadlines, one uncached N+1, and reflection-heavy fan-out marshalling.

### Benchmarks (ran clean, no containers required)

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `BuildAction_Message` | 8,556 | 1,690 | 22 |
| `UserRoom` (accounts=1000) | 11,700,000 | 2.68 MB | **39 allocs + 2.6 KB per account** |
| `Spotlight` (accounts=1000) | 2,600,000 | — | 7 allocs per account |

### Findings

- `high` — **No context deadline on any ES call.** `main.go:172` creates `ctx := context.Background()`, passes it to `runConsumer` (`main.go:370`), which flows to `pipe.run(ctx, …)` → `h.store.Bulk(bulkCtx, …)` (`handler.go:239`). The ES client is built with no HTTP timeout either (`pkg/searchengine/factory.go:33-37` — only the startup Ping is bounded, `:66`). A hung ES socket holds a pipeline slot forever; at `PIPELINE_DEPTH=2` two hangs stall the collection permanently, and shutdown's `pipe.wait()` (`main.go:508`) blocks until the 25s drain timeout expires. Same for `UpdateByQuery` (`handler.go:112`), which runs a cluster-wide `_update_by_query` inline in the fetch loop. Violates CLAUDE.md §6 "Always set timeouts".
- `high` — **Uncached, serial N+1 ES lookup per thread reply.** `messages.go:213-223` → `thread_parent_resolver.go:116`: each thread reply lacking `ThreadParentMessageCreatedAt` issues a `_search` across `messages-<prefix>-*` — every monthly index — with a 2s timeout, synchronously, on the single `message-sync` goroutine. Parent `createdAt` is immutable and highly repeatable (every reply in a thread hits the same parent), yet there is no cache. On a `DeliverPolicy=All` rebuild of legacy data this serializes the entire firehose behind ES search latency.
- `medium` — **Fan-out update bodies built from nested `map[string]any`.** `pkg/searchindex/userroomdoc.go:93-119` allocates three `map[string]any` layers, an empty `[]string{}`, an empty `map[string]int64{}`, and re-runs `time.UnixMilli(ts).UTC().Format(RFC3339Nano)` **per account** — even though `ts` is identical across the whole fan-out. That is the 39-allocs/account measured above. The correct pattern already exists one file over: `spotlight_org.go:176-177` ("Typed rather than nested `map[string]any` so marshalling stays allocation-cheap on the fan-out path").
- `medium` — **Redundant whole-envelope JSON passes per INBOX message.** Spotlight parses the envelope in `BuildByQuery` (`spotlight.go:233`) for *every* message, returns `ok=false` for member events, then `parseMemberEvent` (`inbox_stream.go:422`) parses it again. User-room does `peekInboxEventType` (`inbox_stream.go:402`, a full scan) then `parseMemberEvent`. Two envelope decodes plus a `RawMessage` payload copy per message. The benchmarks call `BuildAction` directly, so they **under-measure the real handler path** — they also skip `DecodePayload` and the ndjson build.
- `medium` — **Bulk ndjson buffer never presized or pooled.** `pkg/searchengine/adapter.go:116-150`: a fresh `bytes.Buffer` grows from zero for up to 500 actions (~19 reallocs, ~2× total bytes copied), and each action marshals a one-entry `map[string]bulkActionMeta` by reflection (~3 allocs/action) on the hottest loop in the worker.
- `medium` — **Busy-spin on `Fetch` error.** `main.go:538-549` `continue`s with no backoff. A fast-failing error (consumer gone, connection closed) spins the loop at 100% CPU per collection. (Third independent sighting of this defect.)
- `low` — **Unbounded Mongo call in the consumer loop.** `messages.go:304` calls `ResolveIdentities` with `context.Background()` and no timeout (`teams_user_store.go:213,231`). The query is correctly batched with `$in` and precisely projected, but `users.account` is queried without the repo's `mongoutil.WarnMissingIndexes` startup check that `search-service/store_mongo.go:49` and `history-service` both perform.
- `nitpick` — `fmt.Sprintf("%s_%s", …)` per account in the spotlight fan-out (`spotlight.go:195`); `idSet := make(map[string]struct{})` with no capacity hint (`messages.go:232`); a goroutine plus unbuffered channel per HR `Fetch` (`consumer_source.go:290-299` — correctly terminated, so not a leak).

### No findings

Metric cardinality is safe — all label values are closed enums with attribute sets precomputed per collection (`metrics.go:362-389`) and `bulkStatusLabel` buckets (`metrics.go:435`). No goroutine leaks. No lock held across I/O: `Take()` holds `mu` only for the slice swap (`handler.go:209-219`). MongoDB projection discipline is followed throughout.

### Recommendations

1. `high` — Give every ES call a deadline: derive a per-flush `context.WithTimeout` (e.g. 30s, tied to `AckWait`) in `flushPipeline.run`, a shorter one for `UpdateByQuery`, and set an explicit `http.Client` timeout on the ES transport in `searchengine.New`.
2. `high` — Put a bounded TTL cache (or an LRU keyed by messageID) in front of `esParentResolver`; parent `createdAt` is immutable, so hit rates on a replay approach 1. Consider moving the resolve off the fetch goroutine entirely.
3. `medium` — Convert `BuildAddRoomUpdateBody`/`BuildRemoveRoomUpdateBody` to typed structs mirroring `spotlightOrgUpdateBody`, and hoist the `now` format plus invariant params out of the per-account loop. Should cut the 39 allocs/account by roughly 4-5×.
4. `medium` — Decode the INBOX envelope once in `Handler.AddWithContext` and pass `*model.InboxEvent` to `BuildByQuery`/`BuildAction`, eliminating the double parse.
5. `medium` — Presize the bulk buffer (`buf.Grow(len(actions) * ~256)`) and pool it via `sync.Pool`; replace the per-action `map[string]bulkActionMeta` marshal with a typed three-variant struct or a direct `append`.
6. `medium` — Add jittered backoff to the `Fetch` error path (`main.go:549`) instead of a bare `continue`.
7. `low` — Adopt `sonic` for the messages collection: it consumes the same MESSAGES-CANONICAL firehose as `message-worker`/`broadcast-worker`, `MessageDoc` and `model.Message` carry no struct-keyed map (no `Reactions`), and nothing hashes or signs the bytes — ES parses them. Safe, and warms via `jsonwarm.Pretouch` like its peers. Also extend `perf_bench_test.go` to benchmark `Handler.AddWithContext` + `searchengine.Bulk` end-to-end, since the current benchmarks miss the redundant parses and the ndjson build.

---

## 7. Prioritized action list

Ordered by severity first, then impact ÷ effort. Items 1-2 are release blockers; 3-5 are the structural core that four independent experts converged on.

### 1. `critical` — Fix the bot-message consumer filter subject
**Dimension:** Integration · **Where:** `search-sync-worker/messages.go:129` (test at `messages_test.go:621-622`)
The bot collection binds `BOT-MESSAGES-CANONICAL-{siteID}` but filters `chat.msg.canonical.{siteID}.*`, a subject that stream does not carry (`pkg/stream/stream.go:97`). `CreateOrUpdateConsumer` fails and `main.go:348` exits — the pod crashloops in `default` mode wherever the bot stream exists, and bot messages are never indexed where it does not. Use `subject.BotCanonicalWildcard(siteID)`, matching `bot-message-worker/main.go:208`, and correct the test that currently pins the wrong string. Add an integration test that creates every default-mode consumer against real streams so this class of mismatch fails CI, not rollout. Small diff, complete outage averted.

### 2. `high` — Close the `room_restricted` ACL gap
**Dimension:** Integration · **Where:** `pkg/subject/subject.go:332-343`, `search-sync-worker/user_room.go:271-274`
`room_restricted` (produced at `room-service/handler.go:2123`) never reaches the user-room index, so a room restricted after its members joined leaves their docs unrestricted and `search-service/query_messages.go:127` grants full-history message search. This is an access-control regression, not staleness. Publish it on the INBOX internal lane, add it to `InboxMemberEventSubjects`, and handle it in `userRoomCollection`.

### 3. `high` — Add `ctx` to the `Collection` interface and delete both `context.Background()` calls
**Dimension:** Code quality / Architecture / Maintainability / Integration (all four) · **Where:** `collection.go:42`, `messages.go:220`, `messages.go:304`
`BuildAction([]byte)` has no context, forcing two network calls to fabricate one — including an **unbounded** Mongo query that no shutdown can cancel and that can block a consumer goroutine indefinitely. Also severs tracing at exactly the two cross-service hops. Thread `AddWithContext`'s context through `BuildAction`/`BuildActionSeq`/`BuildByQuery` and give `ResolveIdentities` an explicit timeout. Highest impact-per-line change in the report.

### 4. `high` — Log, meter and back off the `Fetch` error loop
**Dimension:** Code quality / Architecture / Performance (three) · **Where:** `main.go:538-550`, `consumer_source.go:38`
The error is discarded with no log, metric, backoff or comment, so a NATS outage spins all five collection goroutines at 100% CPU with zero operator signal. Wrap the error at `consumer_source.go:38` so it names the consumer, then log at warn + record a metric + apply an equal-jittered backoff. Roughly a dozen lines.

### 5. `high` — Add `bootstrap.go` with a production stream-existence check
**Dimension:** Architecture / Maintainability · **Where:** `main.go:302-323`
The only JetStream service in the repo without one; eleven peers have it. Beyond the convention breach, the missing verify-when-disabled branch means a misprovisioned production deploy surfaces as an opaque `create consumer failed` instead of a fail-fast at startup. Mirror `hr-sync-worker/bootstrap.go`, add a `streamManager` seam for testability, and replace the `inboxName`/`hrName` string comparisons with a `Collection.OwnsStream() bool` capability.

### 6. `high` — Set deadlines on every ES call
**Dimension:** Performance · **Where:** `main.go:172`, `handler.go:239`, `handler.go:112`, `pkg/searchengine/factory.go:33-37`
A `context.Background()` flows from startup all the way into `store.Bulk`, and the ES client has no HTTP timeout. A hung socket holds a pipeline slot forever; at `PIPELINE_DEPTH=2`, two hangs stall the collection permanently and shutdown blocks to the 25s limit. Per-flush `context.WithTimeout` tied to `AckWait`, a shorter one for `UpdateByQuery`, plus an explicit transport timeout.

### 7. `high` — Raise coverage above the 80% floor by making `main.go` testable
**Dimension:** Test coverage · **Where:** `main.go:108-160`, `main.go:210-240`, `teams_user_store.go:32`
66.8% today; ~84.8% excluding `main.go`. Extract the validation cascade into `config.validate() error` and the mode-gated wiring into `buildCollections(...)`, then table-drive both — that alone converts ~50 dead statements. Add the missing `teams_user_store` integration test (`testutil.MongoDB`), the only store implementation in the service with zero tests of any kind, whose failure mode is a *durable* silent Ack with empty author fields.

### 8. `high` — Cache the thread-parent resolver
**Dimension:** Performance · **Where:** `messages.go:213-223`, `thread_parent_resolver.go:116`
Every thread reply issues a synchronous `_search` across all monthly message indices on the single sync goroutine, uncached, for an immutable value that repeats across every reply in a thread. A bounded TTL/LRU cache approaches a 100% hit rate on replay. Small change, large throughput effect on backfill.

### 9. `high` — Replace wiring-by-field-mutation with constructor options
**Dimension:** Architecture / Maintainability · **Where:** `main.go:219,225-226,231-232,361`, `handler.go:63`
Five post-construction field assignments mean every collection is legally constructible half-wired, and the code nil-guards rather than fails (`messages.go:297` silently returns nil identities). Follow the peer pattern (`inbox-worker/handler.go:158`), and drop the test-only variadic `tracers ...trace.Tracer`. Pairs naturally with action 7, which needs `buildCollections` to be assertable.

### 10. `medium` — Split `main.go` and move the ES doc type into `pkg/searchindex`
**Dimension:** Maintainability / Code quality · **Where:** `main.go:438-618`, `spotlight_org.go:193-244`
`main()` is 319 lines and holds the entire consumer runtime; extract `consumer.go`, `store_search.go` and `config.go` to get it under 120. Separately, `SpotlightOrgIndex` is exported from `package main` — unimportable — and hand-mirrored in `search-service/response.go:127`, so a field rename breaks search silently. Move it to `pkg/searchindex/spotlightorgdoc.go` and have the reader import it.

### Also worth scheduling

- `medium` — Add request-ID propagation (`natsutil.StampRequestID`); every error log in the service is currently uncorrelatable to its producing request.
- `medium` — Confirm `govulncheck` and `semgrep` are green in CI; both gates were unverifiable in this sandbox (proxy 403 / tool absent). `gosec` passed clean.
- `medium` — Drop or redact `results[i].Error` at `handler.go:295` — ES parse-exception reasons can embed message content into logs.
- `medium` — Promote `checkBatchAckCoupling` (`main.go:575`) from warning to fail-fast; it already computes the remedy.
- `medium` — Per-test ES index names with `t.Cleanup` DELETEs, and per-test NATS connections, in the integration suite.
