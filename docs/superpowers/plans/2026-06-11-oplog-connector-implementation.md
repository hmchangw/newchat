# oplog-connector Implementation Plan

> **Status: HISTORICAL — superseded by the as-built (PR #311).** This plan captures the *original* design; two pivots landed during implementation, so the async/pre-image tasks below were NOT built as written:
> - **Synchronous publish.** The repo's `oteljetstream` wrapper exposes no async publish, so the reader→channel→**publisher**→**confirmer** pipeline (`PublishMsgAsync`/`PubAckFuture`/`MAX_INFLIGHT_PUBLISHES`/`PUBLISH_CHANNEL_BUFFER`) was replaced by **one watcher per collection**: `Next → PublishMsg (blocks on pub-ack) → persist checkpoint`. Checkpointing uses `CHECKPOINT_EVERY` + a periodic `CHECKPOINT_MAX_AGE` flush.
> - **No pre-images / no lookups.** `PREIMAGE_COLLECTIONS`, `fullDocumentBeforeChange`, `updateLookup`, and the `PreImage` envelope field were dropped; the envelope carries `UpdateDescription` (the raw delta) instead. The connector forwards native oplog content only.
>
> See the design spec banner and `data-migration/README.md` for the shipped shape. The checklist is retained as the execution record.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Follow the Red-Green-Refactor TDD cycle in CLAUDE.md §4 for every task — write the failing test first, confirm it fails, then implement.

**Goal:** Build `data-migration/oplog-connector` — a single-replica, per-site CDC pump that tails the legacy source MongoDB via change streams and publishes raw, uninterpreted change events into `MIGRATION_OPLOG_{siteID}`. It is a "dumb pump": no transformation, opaque documents, at-least-once per-collection ordered delivery, lossless crash/restart via persisted resume tokens.

**Design source of truth:** `docs/superpowers/specs/2026-06-08-oplog-connector-design.md` — read it fully before starting. This plan implements that contract; if the two disagree, the spec wins.

**Architecture:** The connector is a JetStream **producer**, not a consumer — it does NOT use the `cons.Consume()` pattern. Per watched collection it runs an independent pipeline: a **reader** goroutine pulls change events from a Mongo change stream into a bounded channel; a **publisher** goroutine drains the channel in oplog order and calls `PublishMsgAsync` (one connection → wire order = stream-sequence order); a **confirmer** goroutine consumes pub-ack futures in FIFO order, advancing a **contiguous ack frontier** and persisting the resume token only after the ack. Crash → resume `startAfter(lastToken)`; duplicates collapse on the `Nats-Msg-Id` = change-stream `_id._data` dedup header.

**Tech Stack:** Go 1.25, `mongo-driver/v2` change streams, `nats.go` + `nats.go/jetstream` (async publish), `caarlos0/env`, `log/slog`, `stretchr/testify`, `go.uber.org/mock`, `testcontainers-go` via `pkg/testutil`.

**Key reference files (read before starting):**
- `pkg/stream/stream.go` — `func MessagesCanonical(siteID string) Config` is the exact shape to mirror for `MigrationOplog`.
- `pkg/subject/subject.go` — builder + wildcard func conventions (`fmt.Sprintf`, single `*` / multi `>` wildcards).
- `pkg/model/event.go` + `pkg/model/model_test.go` — event-struct shape (json+bson tags, `Timestamp int64`) and the generic `roundTrip[T]` helper.
- `inbox-worker/main.go` — main wiring reference: config parse, NATS/JetStream connect, `bootstrapStreams` call, `shutdown.Wait` ordering. (Use its wiring skeleton, NOT its `cons.Consume` body.)
- `inbox-worker/bootstrap.go` — `bootstrapStreams(ctx, js, siteID, enabled)` exact shape + `bootstrapConfig{ Enabled bool \`env:"STREAMS" envDefault:"false"\` }`.
- `broadcast-worker/store.go` + `store_mongo.go` — `//go:generate mockgen` directive, store interface, `EnsureIndexes`, `mongo.ErrNoDocuments` check.
- `notification-worker/emit.go` — `msg.Header.Set("Nats-Msg-Id", id)` dedup pattern; `history-service` uses `jetstream.WithMsgID(id)`.
- `inbox-worker/integration_test.go` — `//go:build integration`, `TestMain → testutil.RunTests(m)`, `testutil.MongoDB(t, prefix)`, `testutil.NATS(t)`.
- `pkg/mongoutil/mongo.go` — `Connect(ctx, uri, username, password)` / `Disconnect(ctx, client)`.
- `pkg/shutdown/shutdown.go` — `Wait(ctx, timeout, ...func(context.Context) error)`.
- `inbox-worker/deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}` — deploy templates; bases `golang:1.25.11-alpine` → `alpine:3.21`, build context = repo root.

**Module path:** `github.com/hmchangw/chat`.

**NEW symbols this plan introduces (do not redefine elsewhere):**
- `pkg/stream`: `MigrationOplog(siteID string) Config`
- `pkg/subject`: `MigrationOplog(siteID, collection, op string) string`, `MigrationOplogWildcard(siteID string) string`
- `pkg/model`: `OplogEvent` (+ round-trip test)
- `data-migration/oplog-connector` package `main`: `config`, `bootstrapConfig`, `CheckpointStore`, `Checkpoint`, `mongoCheckpointStore`, `changeEvent`, `changeSource`, `asyncPublisher`, `pubAckFuture`, `Handler` (watcher engine), `buildEnvelope`, `resolveStartPoint`.

**Not client-facing:** the connector has no `nc.QueueSubscribe`/`natsrouter`/Gin handler and no `errcode` boundary — **do NOT** update `docs/client-api.md`. All errors are internal, wrapped with `fmt.Errorf("…: %w", err)`.

**Commands:**
- Unit tests: `make test SERVICE=data-migration/oplog-connector`
- Integration tests: `make test-integration SERVICE=data-migration/oplog-connector`
- Regenerate mocks: `make generate SERVICE=data-migration/oplog-connector`
- Lint: `make lint` · SAST: `make sast` · Build: `make build SERVICE=data-migration/oplog-connector`
- Model tests: `make test SERVICE=pkg/model` (stream/subject: `make test SERVICE=pkg/stream` / `SERVICE=pkg/subject`)

---

## ⚠️ Decisions to resolve before Task 13 (integration)

1. **Mongo change streams require a replica set.** `testutil.MongoDB(t, …)` almost certainly starts a standalone `mongod`, on which `Watch` fails. **Verify first.** If standalone: either (a) add `testutil.MongoReplicaSet(t)` (single-node `--replSet`, auto-`rs.initiate()`) following the `pkg/testutil` shape (`Xxx(t)` + `EnsureXxx()` + `TerminateXxx()` wired into `TerminateAll`) — preferred if any sibling migration service will also need it; or (b) per the CLAUDE.md inline-container exception, run an inline RS container in `integration_test.go` storing the ref + `t.Cleanup(c.Terminate)`. **Recommend (a)** so `history-migrator` can reuse it.
2. **Pre-images are per-collection.** The source collection must be created with `changeStreamPreAndPostImages: {enabled: true}` for `fullDocumentBeforeChange` to work. The integration test must enable it on the test collection; in prod it's an ops/source-DB concern (note in `data-migration/README.md`).
3. **Read preference in tests.** Spec reads from `secondary`; a single-node test RS has none. Make `READ_PREFERENCE` config-driven (already in the spec) and set `primary`/`primaryPreferred` in tests.

Surface these to the human if unresolved — do not silently change the spec.

---

## Phase A — Shared packages (no service code yet)

### Task 1: `pkg/stream.MigrationOplog(siteID)`

**Files:** Edit `pkg/stream/stream.go`; edit/create `pkg/stream/stream_test.go`.

- [ ] **Step 1: Write the failing test** — assert name/subjects:
```go
func TestMigrationOplog(t *testing.T) {
	cfg := MigrationOplog("site1")
	assert.Equal(t, "MIGRATION_OPLOG_site1", cfg.Name)
	assert.Equal(t, []string{"chat.oplog.site1.>"}, cfg.Subjects)
}
```
- [ ] **Step 2:** `make test SERVICE=pkg/stream` → FAIL (`undefined: MigrationOplog`).
- [ ] **Step 3:** Implement mirroring `MessagesCanonical`:
```go
func MigrationOplog(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MIGRATION_OPLOG_%s", siteID),
		Subjects: []string{fmt.Sprintf("chat.oplog.%s.>", siteID)},
	}
}
```
- [ ] **Step 4:** Test green. Commit `feat(stream): add MIGRATION_OPLOG stream config`.

### Task 2: `pkg/subject` oplog builders

**Files:** Edit `pkg/subject/subject.go`; edit `pkg/subject/subject_test.go`.

- [ ] **Step 1: Failing test** — concrete subject + wildcard:
```go
func TestMigrationOplog(t *testing.T) {
	assert.Equal(t, "chat.oplog.site1.rocketchat_message.insert",
		MigrationOplog("site1", "rocketchat_message", "insert"))
	assert.Equal(t, "chat.oplog.site1.>", MigrationOplogWildcard("site1"))
}
```
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement both with `fmt.Sprintf` (no raw `Sprintf` outside this package — that's the rule these builders satisfy).
- [ ] **Step 4:** Green. Commit `feat(subject): add oplog subject builders`.

### Task 3: `pkg/model.OplogEvent` + round-trip

**Files:** Create `pkg/model/oplog_event.go`; edit `pkg/model/model_test.go`.

- [ ] **Step 1: Failing test** registering `OplogEvent` with the generic helper, exercising both opaque-doc presence and omitempty:
```go
func TestOplogEventJSON(t *testing.T) {
	evt := model.OplogEvent{
		EventID: "82650...", Op: "update", DB: "rocketchat", Collection: "rocketchat_message",
		DocumentKey:  json.RawMessage(`{"_id":"abc"}`),
		ClusterTime:  1718100000000,
		FullDocument: json.RawMessage(`{"_id":"abc","msg":"hi"}`),
		SiteID:       "site1", Timestamp: 1718100000123,
	}
	roundTrip(t, &evt, &model.OplogEvent{})
}
```
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Add the struct exactly per spec §2.4 — `json.RawMessage` for `DocumentKey`/`FullDocument`/`PreImage` (opaque, deferred decode), `omitempty` on `FullDocument`/`PreImage`, `Timestamp int64` event-level field. Keep json+bson tags per CLAUDE.md (bson harmless even though it's a wire-only struct — match the model convention).
- [ ] **Step 4:** Green (`make test SERVICE=pkg/model`). Commit `feat(model): add OplogEvent envelope`.

---

## Phase B — Service scaffolding

### Task 4: `config.go` + parsing

**Files:** Create `data-migration/oplog-connector/config.go`, `config_test.go`.

Config per spec §5.2 (`caarlos0/env`). `WATCH_COLLECTIONS` / `PREIMAGE_COLLECTIONS` are comma-lists → `[]string` (env supports comma-split into slices). Required: `SITE_ID`, `SOURCE_MONGO_URI`, `NATS_URL`, `WATCH_COLLECTIONS`.

- [ ] **Step 1: Failing test** — set env, parse, assert slices split and defaults applied (`CHECKPOINT_DB=migration`, `START_MODE=now`, `PUBLISH_CHANNEL_BUFFER=1024`, `MAX_INFLIGHT_PUBLISHES=256`, `PREIMAGE_COLLECTIONS=[rocketchat_message]`). Use `t.Setenv`.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Define `config` struct with `env`/`envDefault`/`required`/`envPrefix:"BOOTSTRAP_"` tags; `parseConfig() (config, error)` via `env.ParseAs[config]()`. Validate `START_MODE ∈ {now,beginning,time}` and that `START_MODE=time` requires `START_AT_TIME`; return wrapped error otherwise.
- [ ] **Step 4:** Green. Commit `feat(oplog-connector): config`.

### Task 5: `bootstrap.go`

**Files:** Create `data-migration/oplog-connector/bootstrap.go`, `bootstrap_test.go`.

- [ ] **Step 1: Failing test** — define a fake `streamManager` capturing `CreateOrUpdateStream` calls; assert: `enabled=false` → no-op (zero calls); `enabled=true` → exactly one call with `Name="MIGRATION_OPLOG_site1"`, `Subjects=["chat.oplog.site1.>"]`, and **no** `Sources`/`SubjectTransforms` (federation is ops-owned).
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement `bootstrapStreams(ctx, js streamManager, siteID string, enabled bool) error` mirroring `inbox-worker/bootstrap.go`; consume `stream.MigrationOplog(siteID)`; `js.CreateOrUpdateStream` only when enabled. Define the minimal `streamManager` interface in this file (consumer-defined).
- [ ] **Step 4:** Green. Commit `feat(oplog-connector): stream bootstrap`.

---

## Phase C — Checkpoint store

### Task 6: `CheckpointStore` + Mongo impl

**Files:** Create `store.go` (interface + `//go:generate mockgen -destination=mock_store_test.go -package=main . CheckpointStore`), `store_mongo.go`, `integration_test.go` (store cases). Run `make generate SERVICE=data-migration/oplog-connector`.

- [ ] **Step 1: Failing integration test** (`//go:build integration`, `TestMain → testutil.RunTests(m)`): with `db := testutil.MongoDB(t, "oplogcp")`:
  - `Load` of absent key → `(nil, nil)`.
  - `Save` then `Load` round-trips `ResumeToken bson.Raw` byte-identical, plus `Source`/`ClusterTime`/`EventID`.
  - Second `Save` same `_id` **upserts** (one doc, updated fields).
- [ ] **Step 2:** FAIL (types undefined).
- [ ] **Step 3:** Define `Checkpoint` (spec §4.1, `bson` tags, `_id = "{siteID}:{collection}"`) and `CheckpointStore{ Load(ctx, collection) (*Checkpoint, error); Save(ctx, *Checkpoint) error }`. Implement `mongoCheckpointStore` over the `oplog_checkpoints` collection in `CHECKPOINT_DB` on the **source** RS; `Save` = `ReplaceOne` with `upsert`; `Load` returns `(nil,nil)` on `mongo.ErrNoDocuments`. `NewMongoCheckpointStore(col)`.
- [ ] **Step 4:** Green (`make test-integration …`). Commit `feat(oplog-connector): checkpoint store`.

---

## Phase D — Pure logic (the easily-testable core)

### Task 7: `buildEnvelope` — change event → (subject, msgID, OplogEvent)

**Files:** Create `envelope.go`, `envelope_test.go`. Define the internal `changeEvent` struct here (decoded change-stream doc: `ID bson.Raw` for `_id`, `OperationType string`, `Ns struct{DB,Coll string}`, `DocumentKey bson.Raw`, `FullDocument bson.Raw`, `FullDocumentBeforeChange bson.Raw`, `ClusterTime` → ms, `resumeToken bson.Raw`, `eventID string` = `_id._data`).

- [ ] **Step 1: Table-driven failing test** over `insert|update|replace|delete`:
  - subject = `chat.oplog.{site}.{coll}.{op}`; `msgID == evt.EventID == _id._data`.
  - `OplogEvent` fields populated; `FullDocument` present for insert/update/replace, empty for delete.
  - `PreImage` populated only when the collection is in the configured preimage set AND the source provided `FullDocumentBeforeChange`; empty otherwise.
  - `Timestamp` is the injected publish-time ms (inject a clock/`nowMs` arg — do NOT call `time.Now()` inside, keep it pure for determinism).
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** `func buildEnvelope(ev changeEvent, siteID string, preimage bool, nowMs int64) (subject, msgID string, evt model.OplogEvent)`. Map op type → `op` token (reject/skip non-data ops upstream, not here). `ClusterTime` = source op time ms; `Timestamp` = `nowMs`.
- [ ] **Step 4:** Green. Commit `feat(oplog-connector): envelope mapping`.

### Task 8: `resolveStartPoint` — precedence

**Files:** Create `startpoint.go`, `startpoint_test.go`. Define a `startPoint` result describing one of: `startAfter(token)`, `startAtOperationTime(tsMs)`, or `fromNow` / `fromBeginning`.

- [ ] **Step 1: Table-driven failing test** for spec §4.2 precedence:
  - `START_RESUME_TOKEN` set → `startAfter(token)`, ignores checkpoint.
  - `START_AT_TIME` override (no token) → `startAtOperationTime`.
  - no override + checkpoint present → `startAfter(cp.ResumeToken)`; token absent but `ClusterTime` set → `startAtOperationTime(cp.ClusterTime)`.
  - no override + no checkpoint → cold start per `START_MODE` (`now`/`beginning`/`time`+`START_AT_TIME`).
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** `func resolveStartPoint(cfg config, cp *Checkpoint) (startPoint, error)`. Pure; error if `time` mode without `START_AT_TIME` (belt-and-suspenders vs Task 4).
- [ ] **Step 4:** Green. Commit `feat(oplog-connector): start-point resolution`.

---

## Phase E — Watcher engine

### Task 9: Sequential publisher + contiguous ack frontier  ← **the crux**

**Files:** Create `handler.go` (the watcher engine), `handler_test.go`.

Seams (define in `handler.go`, consumer-side, so unit tests need no NATS):
```go
type pubAckFuture interface { Ok() <-chan *jetstream.PubAck; Err() <-chan error }
type asyncPublisher interface { PublishMsgAsync(msg *nats.Msg, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) }
```
`jetstream.JetStream` satisfies `asyncPublisher`; `jetstream.PubAckFuture` satisfies `pubAckFuture`. The engine runs, per collection, a **publisher** (range the event channel in order → `PublishMsgAsync` with `WithMsgID(eventID)` → enqueue `(future, checkpoint)` onto a confirm channel buffered to `MAX_INFLIGHT_PUBLISHES`; full channel = backpressure) and a **confirmer** (range the confirm channel **in FIFO order** → `select` on `Ok()`/`Err()`; on `Ok` advance the frontier and `Save` the checkpoint; on `Err` retry-with-backoff, never advancing past the gap). FIFO confirmation makes the frontier inherently contiguous.

- [ ] **Step 1: Failing unit tests** with a fake `asyncPublisher` returning controllable futures (channels the test fires):
  - **happy path:** publish 3 events; fire acks in order → frontier reaches event 3; `Save` called with event 3's token; payloads captured carry the right subject + `Nats-Msg-Id`.
  - **gap stalls frontier:** ack events 1 and 3 but make event 2's future emit on `Err()` first then succeed on retry → assert the checkpoint is NEVER saved with event 3's token before event 2 is acked (token never persisted past a gap).
  - **post-ack only:** assert no `Save` happens before the corresponding `Ok()` fires.
  - **backpressure:** with `MAX_INFLIGHT=2`, the publisher blocks on the 3rd publish until the 1st confirms (assert via a gated fake).
  - **ordering:** captured publish order == input order (single sequential publisher).
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement the engine. Inject the publisher and `CheckpointStore` (per CLAUDE.md: inject publish fn so tests capture without NATS). Throttle `Save` (e.g. coalesce to the latest frontier on a short interval or every N) but ALWAYS `Save` the final frontier on shutdown. Use `chan`/`sync.WaitGroup` for lifecycle — never `time.Sleep` for sync.
- [ ] **Step 4:** Green, `-race` clean. Commit `feat(oplog-connector): publisher + ack frontier`.

### Task 10: Change-stream source + reader loop

**Files:** Add to `handler.go` (reader loop) + create `source_mongo.go` (real impl). Define the seam:
```go
type changeSource interface {
	Next(ctx context.Context) (changeEvent, error) // blocks; io.EOF-style sentinel on close
	Close(ctx context.Context) error
}
```

- [ ] **Step 1: Failing unit test** — a fake `changeSource` yielding scripted `changeEvent`s drives the reader → channel → (Task 9 engine) producing the expected envelopes on the fake publisher. Verifies reader→publisher wiring end-to-end in-process (no Mongo, no NATS).
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Reader goroutine: loop `Next` → decode → send on the bounded channel; on ctx-cancel/close, drain and exit. Implement `mongoChangeSource` wrapping `*mongo.ChangeStream`: build `options.ChangeStream()` with `SetFullDocument(updateLookup)`, `SetFullDocumentBeforeChange(whenAvailable)` only for preimage collections, `SetStartAfter`/`SetStartAtOperationTime` from `resolveStartPoint`, majority read concern, configured read preference. Map driver event → `changeEvent` (extract `_id._data` → `eventID`, resume token, cluster time → ms).
- [ ] **Step 4:** Green. Commit `feat(oplog-connector): mongo change-stream source`.

---

## Phase F — Wiring & lifecycle

### Task 11: `main.go` full wiring

**Files:** Create `main.go`.

- [ ] **Step 1:** (covered by Task 13 integration; no separate unit test for `main`.)
- [ ] **Step 2/3:** Wire, mirroring `inbox-worker/main.go` skeleton:
  1. `parseConfig`; slog JSON logger at `LOG_LEVEL`; fail-fast on error (`os.Exit(1)`).
  2. `mongoutil.Connect` to **source** RS (`SOURCE_MONGO_URI`) — used for BOTH change streams and the checkpoint collection (`CHECKPOINT_DB`).
  3. Connect NATS + JetStream (oteljetstream as the repo does).
  4. `bootstrapStreams(ctx, js, SiteID, Bootstrap.Enabled)`.
  5. For each `WATCH_COLLECTIONS` entry: load checkpoint → `resolveStartPoint` → open `mongoChangeSource` → start reader+publisher+confirmer goroutines (one bounded channel each). Track all goroutines in a `sync.WaitGroup`.
  6. `shutdown.Wait(ctx, 25*time.Second, …)` ordering per spec §7.3: **stop readers → close change streams → drain channels / await in-flight acks (bounded) → persist final frontier per collection → `nc.Drain()` → `mongoutil.Disconnect`.**
- [ ] **Step 4:** `make build SERVICE=data-migration/oplog-connector` compiles. Commit `feat(oplog-connector): main wiring`.

### Task 12: Error handling, retry, observability

**Files:** `handler.go`, `source_mongo.go`, `main.go`.

- [ ] **Step 1: Failing unit tests:**
  - resume-token-lost: a `changeSource.Next` returning a Mongo error with code **286** (`ChangeStreamHistoryLost`) → the engine signals fatal (returns/propagates a sentinel) so `main` exits non-zero; assert it does NOT silently reseed-from-now. Use `errors.As` on a `mongo.CommandError`/`mongo.ServerError` (verify the driver's code-bearing type — do NOT string-match).
  - publish error → retry with capped backoff; frontier holds until success (already partly in Task 9 — extend for backoff bound).
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement: classify code 286 as fatal (loud `slog.Error`, return up to `main` → `os.Exit(1)`); backoff retry for publish (context-aware, no `time.Sleep`-for-sync — use a timer/`context`); structured `slog` with `EventID` correlation field; emit metrics (lag = `nowMs - ClusterTime`, events/sec/collection, publish errors, in-flight depth, frontier position) per spec §7.4. All infra errors wrapped `fmt.Errorf("…: %w", err)`; no `errcode`.
- [ ] **Step 4:** Green, `-race`. Commit `feat(oplog-connector): fatal-token handling, retry, metrics`.

---

## Phase G — Integration

### Task 13: End-to-end integration tests

**Files:** Extend `integration_test.go` (resolve the §"Decisions" RS/pre-image items FIRST).

- [ ] **Step 1: Failing integration tests** (`//go:build integration`) against a Mongo **replica set** + NATS (`testutil.NATS(t)`), `BOOTSTRAP_STREAMS` on:
  - **CRUD → stream:** insert/update/replace/delete a doc in a watched source collection → assert one message lands on `chat.oplog.{site}.{coll}.{op}` with the right `Nats-Msg-Id` and decoded `OplogEvent` (incl. `FullDocument`; `PreImage` on delete for the preimage collection).
  - **resume after restart:** publish N, stop the engine, restart from the persisted checkpoint → no gap, no missing seq.
  - **dedup:** force redelivery of the same `EventID` → exactly one message on the stream (JetStream msg-id dedup).
  - **seed start:** pre-insert a seed checkpoint (`Source:"seed"`) → engine `startAfter` begins exactly after the seeded point.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Make them pass (fixing wiring as needed).
- [ ] **Step 4:** Green (`make test-integration SERVICE=data-migration/oplog-connector`). Commit `test(oplog-connector): integration coverage`.

---

## Phase H — Deploy, docs, finalize

### Task 14: `deploy/`

**Files:** `data-migration/oplog-connector/deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}`.

- [ ] Dockerfile from the `inbox-worker` template (`golang:1.25.11-alpine` → `alpine:3.21`, build context = repo root, copy `pkg/` + `data-migration/oplog-connector/`, non-root user). Build path `./data-migration/oplog-connector/`.
- [ ] docker-compose.yml: source Mongo as **single-node replica set** with `changeStreamPreAndPostImages` enabled on the message collection (init script), NATS with `--jetstream --http_port 8222`, `BOOTSTRAP_STREAMS=true`, sample `WATCH_COLLECTIONS`.
- [ ] azure-pipelines.yml: copy sibling; scope triggers to `data-migration/oplog-connector/**`.
- [ ] Verify `make up SERVICE=data-migration/oplog-connector` stands up. Commit `chore(oplog-connector): deploy assets`.

### Task 15: Suite README

**Files:** Create `data-migration/README.md`.

- [ ] Copy the spec §0 diagram; list the sibling components (`history-migrator`, `oplog-transformer`) and the `pkg/migration/` shared-code namespace; document the source-side prereqs (replica set, per-collection pre-images, checkpoint DB). Commit `docs: data-migration suite README`.

### Task 16: Final gates

- [ ] `make generate SERVICE=data-migration/oplog-connector` (mocks current) → `make lint` → `make test` (`-race`) → `make test-integration SERVICE=data-migration/oplog-connector` → `make sast` (no medium+). 
- [ ] Coverage: `go test -coverprofile` ≥ **80%** floor, **90%** target on `handler.go` + `store_mongo.go` + the pure-logic files (`envelope.go`, `startpoint.go`). Add cases for any gap (use the `coverage_gap` skill if needed).
- [ ] Update spec §3 cross-refs if any signature drifted from this plan.
- [ ] Open PR (per CLAUDE.md: branch only, never push to main directly). Confirm no client-api.md change is needed (it is not — no client-facing surface).

---

## Task dependency graph

```text
A1 stream ┐
A2 subject├─▶ (independent, do in parallel)
A3 model ─┘
B4 config ─▶ B5 bootstrap
C6 store (needs A-none; uses model? no) ─┐
D7 envelope (needs A2,A3) ───────────────┤
D8 startpoint (needs B4,C6 Checkpoint) ──┤
                                         ▼
E9 publisher+frontier (needs D7,C6) ─▶ E10 source+reader (needs D8,E9)
                                         ▼
F11 main (needs all above) ─▶ F12 errors/metrics
                                         ▼
G13 integration (needs F11,F12 + RS decision)
                                         ▼
H14 deploy ─▶ H15 README ─▶ H16 gates
```

Critical path: **A3 → D7 → E9 → E10 → F11 → F12 → G13 → H16.** Phase A and B4/B5/C6 are parallelizable up front.
