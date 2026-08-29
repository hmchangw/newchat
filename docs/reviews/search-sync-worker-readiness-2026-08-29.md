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
