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
