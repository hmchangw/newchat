# oplog-connector — live-sync CDC pump (Design)

*source Mongo → `MIGRATION_OPLOG_{site}`*

> **Status:** IMPLEMENTED (PR #311). Two pivots from the original design landed and are reflected in §§2.4/3.1/5.2/6/7: **(1) synchronous publish** — one watcher per collection calls `PublishMsg` and blocks on the pub-ack (the repo's `oteljetstream` exposes no async publish), replacing the async reader→channel→publisher→confirmer pipeline; **(2) no pre-images / no lookups** — the connector forwards native oplog content only. Any residual async/buffer wording elsewhere describes the superseded original design.

*A small, single-replica, per-site service whose only job is to tail the legacy ("source") MongoDB via change streams and pump **raw, uninterpreted** change events into a single JetStream stream. It does no enrichment, no lookups, no per-collection schema knowledge — a downstream **transformer** (out of scope here) consumes `MIGRATION_OPLOG_{site}` and does the actual modelling. This is the "dumb pump" half of a two-stage migration.*

---

## 0. Context — where this sits in the migration

We are migrating off a legacy RocketChat-style Mongo onto the new distributed chat stack. The migration has **two independent halves**:

1. **History migration (separate service, out of scope).** A bulk DB→DB copy of everything already in the source at a consistent cut. It captures a **resume token / clusterTime at its snapshot point** and hands that off.
2. **Live-sync CDC (this service).** Picks up **exactly after the migrated cut** and streams every subsequent change. The handoff is the **init checkpoint**: the connector seeds its change stream from the token the history migration captured, so live sync begins precisely where the bulk copy ended — **zero gap**, and any overlap collapses on `Nats-Msg-Id` dedup.

```text
                    consistent cut (clusterTime T, resume token R)
                                 │
  source Mongo ─────────────────┼──────────────────────────────▶ time
                                │
   history-migrator   ◀── bulk copy of state ≤ T ──┐
   (data-migration/, separate owner)               │ captures R
                                                    ▼
   oplog-connector    seed init checkpoint = R ──▶ startAfter(R) ──▶ live CDC ─▶ MIGRATION_OPLOG_{site}
   (data-migration/, this service)
```

So the connector's job reduces to: **resume from a given point, and never lose or reorder an event after it.**

---

## 1. Goal & non-goals

**Goal.** Reliably mirror source Mongo change events into `MIGRATION_OPLOG_{site}` such that, per collection:
- every event after the init checkpoint is delivered **at least once** (loss is unacceptable; duplicates are fine — dedup collapses them),
- events are delivered **in oplog order** end to end,
- the connector can **crash/restart losslessly** by resuming from a persisted checkpoint,
- an operator can **seed the start point** (the migration handoff) and, in recovery, **re-seed** it.

**Non-goals.**
- No interpretation/transformation of documents — opaque pass-through only (the transformer owns modelling).
- No global cross-collection total order (see §6 — inherent to per-collection watchers).
- No bulk/history backfill — that is the separate `history-migrator` (a sibling component under `data-migration/`).
- No client-facing request/reply surface — this service has no `errcode` boundary.

---

## 2. The B1↔B2 contract (what the transformer consumes)

### 2.1 Stream

- **Stream:** `MIGRATION_OPLOG_{siteID}` (added to `pkg/stream/stream.go` as `MigrationOplog(siteID)`).
- **Subjects:** `["chat.migration.oplog.{siteID}.>"]`.
- **Retention:** soak / time-based, sized over the **worst-case transformer outage** so the transformer can be down and replay without data loss. (Exact window is an ops/IaC decision; the connector does not depend on it.)
- **Ownership:** the connector **owns** this stream and bootstraps it in dev only (§5).

### 2.2 Subject

```text
chat.migration.oplog.{siteID}.{rawCollection}.{op}      op ∈ insert | update | replace | delete
```

**All ops, every collection — identically.** Every change-stream operation type — `insert`, `update`, `replace`, `delete` — is traced and published for **every** watched collection. There is no per-collection op allow-listing, no op filtering, and **no per-collection knobs at all** — collections are pure config, all handled the same way. The connector mirrors the full mutation history of each collection so the transformer can reconstruct exact state. Change-stream control events (`invalidate`, `drop`, `rename`) are not data ops and are handled per §7.2, not published.

`rawCollection` is the **raw source collection name** (e.g. `rocketchat_message`) — the connector does not rename. Built via a new `pkg/subject` builder, never `fmt.Sprintf`.

### 2.3 Dedup key

`Nats-Msg-Id` header = the change-stream event id (`_id._data`). JetStream message-dedup collapses the migration-handoff overlap and any redelivery after a crash.

### 2.4 Published envelope (`pkg/model/oplog_event.go`)

Typed envelope, but documents stay **opaque** (`json.RawMessage`) so the dumb connector remains collection-agnostic — the transformer decodes per collection. (This is deferred decoding, not `map[string]interface{}`; it complies with the "typed structs" rule.)

```go
type OplogEvent struct {
    EventID           string          `json:"eventId"`      // change-stream _id._data; also Nats-Msg-Id
    Op                string          `json:"op"`           // insert | update | replace | delete
    DB                string          `json:"db"`
    Collection        string          `json:"coll"`         // raw source name
    DocumentKey       json.RawMessage `json:"documentKey"`  // { _id: ... }
    ClusterTime       int64           `json:"clusterTime"`  // source op time, unix ms
    FullDocument      json.RawMessage `json:"fullDocument,omitempty"`      // native document; insert/replace only (no lookup)
    UpdateDescription json.RawMessage `json:"updateDescription,omitempty"` // raw delta; update only
    SiteID            string          `json:"siteId"`
    Timestamp         int64           `json:"timestamp"`    // publish time, unix ms (event-level, per CLAUDE.md)
}
```

The opaque **resume token** is kept **internally** for checkpointing and is *not* in the payload.

**Encode-failure degradation (lossless).** If an opaque field (`documentKey`/`fullDocument`/`updateDescription`) fails to encode, that field is omitted and the event is published with `Degraded=true` and a `DegradedReason` (first failure wins) — it is **never dropped**, so the checkpoint advances legitimately and the stream stays lossless. The downstream transformer recovers degraded events.

**No lookups — native oplog content only.** The connector never enriches: it forwards exactly what the change stream carries natively. `FullDocument` is the document for `insert`/`replace` (it's in the oplog entry — free); `update` carries only `UpdateDescription` (the raw delta), **not** an `updateLookup` post-image; `delete` carries only the `documentKey`. No `updateLookup` (an extra source read, and it returns the *current* doc, which can be newer than the event) and **no pre-images** (a source-side feature with storage cost). All enrichment — resolving a delete's `_id`, applying a delta, joining to a room/user — is the downstream transformer's job, against the state it is building. This keeps the pump dumb and the `MIGRATION_OPLOG` stream a faithful raw-change record for retention/replay.

---

## 3. Architecture & components

The connector is one component of the **data-migration suite**, which is grouped under a single `data-migration/` folder in the monorepo (other components — a bulk `history-migrator` and the downstream `oplog-transformer` — are owned by others and developed in parallel). The grouping gives the migration effort ownership seams and a single blast radius to delete once migration is done. Each component remains a standard flat `package main` service (CLAUDE.md §"per-service file organization"), nested one level under `data-migration/`. Shared migration code lives in the **root** `pkg/migration/` — there is no nested `pkg/` or `internal/` inside `data-migration/` (one source of shared truth, per the monorepo convention). The Makefile treats `SERVICE` as a path fragment, so `make {test,build,up} SERVICE=data-migration/oplog-connector` works unchanged.

```text
data-migration/
  README.md          how the suite's components compose (the §0 diagram)
  oplog-connector/   ← THIS service. publishes to MIGRATION_OPLOG_{site}
    main.go            config parse, connect source Mongo + NATS, bootstrap, wire, start watchers, shutdown.Wait
    config.go          typed Config (caarlos0/env)
    handler.go         the watcher engine (read → channel → per-collection sequential publisher → frontier)
    bootstrap.go       bootstrapStreams (owns MIGRATION_OPLOG_{site}, gated by BOOTSTRAP_STREAMS)
    store.go           CheckpointStore interface + //go:generate mockgen
    store_mongo.go     Mongo impl over the `migration` DB on the source RS
    handler_test.go    unit: mocked store + injected publish fn
    integration_test.go //go:build integration — testcontainers Mongo + NATS
    mock_store_test.go  generated
    deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}
  oplog-transformer/ (separate spec, separate owner) — consumes MIGRATION_OPLOG_{site}
  history-migrator/  (separate spec, separate owner) — bulk batch copy ≤ the consistent cut

pkg/model/oplog_event.go     OplogEvent (+ pkg/model round-trip test entry)
pkg/stream/stream.go         MigrationOplog(siteID)
pkg/subject/...              oplog subject builder
pkg/migration/...            shared migration code (checkpoint, source, sink, transform, progress) — root pkg/, not nested
```

### 3.1 Watcher engine (per configured collection)

One change stream per collection. For each:

```text
 change stream cursor ──▶ ONE watcher goroutine: Next ──▶ PublishMsg (blocks on pub-ack) ──▶ persist token (post-ack)
   (a periodic flusher also persists the latest acked frontier every CHECKPOINT_MAX_AGE)
```

- **Read options:** **no `updateLookup`, no pre-images** — native oplog content only (§2.4). `majority` read concern; read from **secondary** (configurable). The change stream is opened with no `fullDocument`/`fullDocumentBeforeChange` options, so it performs no extra source reads.
- **Single active reader per collection** — guaranteed by `replicas=1` (see §7 HA). No leader election.

### 3.2 Checkpoint store

Interface in `store.go` (consumer-defined, minimal):

```go
type CheckpointStore interface {
    Load(ctx context.Context, collection string) (*Checkpoint, error) // nil, nil when absent
    Save(ctx context.Context, cp *Checkpoint) error                   // upsert by _id
}
```

Mongo impl writes to the `oplog_checkpoints` collection in the `migration` DB **on the source replica set** (reuses the connection the connector already has; no new cluster; checkpoints die with the source when migration ends). `EnsureIndexes` (just `_id`, which is implicit) in the constructor.

---

## 4. Checkpoints & start-point resolution

### 4.1 Checkpoint document (`oplog_checkpoints`)

One doc per collection, `_id = "{siteID}:{collection}"`:

```go
type Checkpoint struct {
    ID          string   `bson:"_id"`          // "{siteID}:{collection}"
    SiteID      string   `bson:"siteId"`
    Collection  string   `bson:"collection"`   // raw source name
    ResumeToken bson.Raw `bson:"resumeToken"`  // change-stream token {_data:"..."} — fed back verbatim
    ClusterTime int64    `bson:"clusterTime"`  // op time of last acked event, unix s — fallback + observability
    EventID     string   `bson:"eventId"`      // _id._data of last acked event
    Source      string   `bson:"source"`       // "seed" | "runtime" — provenance
    UpdatedAt   int64    `bson:"updatedAt"`    // last persist time, unix ms
}
```

The **resume token is the real checkpoint** (opaque, raw BSON so it round-trips exactly). `ClusterTime` is a coarse fallback (feeds `startAtOperationTime` if a token is ever absent) and lets ops eyeball lag without decoding the token.

### 4.2 Start-point resolution (per collection, precedence top-down)

```text
1. ENV override (forces a reseed; ignores any stored checkpoint)
     START_RESUME_TOKEN=<_data hex>     → startAfter(token)
     START_AT_TIME=<RFC3339|unix-ms>    → startAtOperationTime(ts)
2. Persisted checkpoint
     startAfter(cp.ResumeToken)         (or startAtOperationTime(cp.ClusterTime) if token absent)
3. Cold start default (no checkpoint, no override)
     START_MODE = now (default) | time(+START_AT_TIME)
```

### 4.3 The two operator inputs

- **Init checkpoint (the migration handoff) — "provide a checkpoint".** The `history-migrator` captures the resume token `R` at its consistent cut. Operationally, **pre-insert one seed doc per collection** into `oplog_checkpoints` (`Source:"seed"`, `ResumeToken:R`) before first start — per-collection, no env juggling. This is the **preferred** path. For a one-off, the global `START_RESUME_TOKEN` env does the same. The connector then `startAfter(R)` → live sync begins exactly after the migrated cut.
- **Initial start point — "init start point".** `START_MODE` / `START_AT_TIME` cover cold start when **no** checkpoint exists (e.g. a brand-new collection with no migration handoff).

> ⚠️ **The Tier-1 env overrides (`START_RESUME_TOKEN`, `START_AT_TIME`) are one-off only.** They ignore the stored checkpoint *on every start*, so if either is left set in a long-lived environment (e.g. a k8s Deployment) the connector **reseeds on every restart** — re-streaming from the seed point each time (deduped, never lost, but a large pointless replay, and the checkpoint never effectively sticks). Seed via the pre-inserted checkpoint doc (Tier 2) instead; if you must use an env override, **unset it after first start**. The connector logs a `WARN` at startup when either is set.

### 4.4 `startAfter`, not `resumeAfter`

Tokens are fed back with **`startAfter`**: it survives invalidate events (collection drop/rename mid-migration) where `resumeAfter` hard-fails. Same token format, strictly safer for this workload.

---

## 5. Bootstrap & config

### 5.1 Stream bootstrap (CLAUDE.md §JetStream)

`bootstrap.go` defines `bootstrapStreams(ctx, js, siteID, enabled) error`, gated by `BOOTSTRAP_STREAMS` (env `STREAMS`, default `false`), no-op when disabled. When enabled (dev), creates **only** `MIGRATION_OPLOG_{site}`'s schema (`Name + Subjects` from `pkg/stream.MigrationOplog(siteID)`) via `js.CreateOrUpdateStream`. No federation `Sources`/`SubjectTransforms` — those are ops/IaC owned.

### 5.2 Config (`caarlos0/env`, fail-fast on required)

| Env | Req? | Default | Purpose |
|-----|------|---------|---------|
| `SITE_ID` | ✓ | — | site scope for subjects/stream/checkpoint `_id` |
| `SOURCE_MONGO_URI` | ✓ | — | source RS connection (read change streams + write checkpoints) |
| `CHECKPOINT_DB` | | `migration` | DB on source RS holding `oplog_checkpoints` |
| `NATS_URL` | ✓ | — | publish target |
| `WATCH_COLLECTIONS` | ✓ | — | comma-list of raw collections to tail (see §5.3) |
| `READ_PREFERENCE` | | `secondary` | source read preference |
| `CHECKPOINT_EVERY` | | `100` | persist the resume token every N acked events |
| `CHECKPOINT_MAX_AGE` | | `30` | also persist at least every N seconds (bounds replay for low-volume collections) |
| `START_MODE` | | `now` | cold-start default: `now` \| `time` |
| `START_AT_TIME` | | — | RFC3339 or unix-ms; used with `START_MODE=time` or as override |
| `START_RESUME_TOKEN` | | — | one-off global seed override (`startAfter`) |
| `BOOTSTRAP_STREAMS` | | `false` | dev-only stream creation |
| `LOG_LEVEL` | | `info` | slog level |

### 5.3 Watched collections

The connector tails these 8 source collections (`WATCH_COLLECTIONS`):

| Raw source collection | Maps to (new stack, roughly) |
|---|---|
| `rocketchat_message` | messages |
| `rocketchat_room` | rooms (channels / DMs) |
| `rocketchat_subscription` | subscriptions |
| `rocketchat_uploads` | uploads / file metadata |
| `company_room_members` | room members |
| `company_thread_subscriptions` | thread subscriptions |
| `company_hr_acct_org` | HR account / org mapping |
| `users` | users |

For each of these, **all four op types** (`insert`/`update`/`replace`/`delete`) are traced — no op filtering, and **every collection is handled identically** (§2.2): no pre-images, no lookups. The connector stays collection-agnostic — these names are pure config, fed verbatim to `startAfter`/subjects with no per-collection schema logic.

> Spellings confirmed against the source (2026-06-11): `rocketchat_message` is **singular** (not the RocketChat-default plural `rocketchat_messages`), and the rooms collection is `rocketchat_room`. The names are otherwise exact — a misspelled entry yields a watcher that silently tails nothing (no error), so the implementer copies this list verbatim.

---

## 6. Ordering invariants

Three monotonic positions answer "what we init / what we pushed / what's next / what comes after":

| Question | Tracked by |
|---|---|
| What we init from | seed / `START_*` start point (resume token or clusterTime) |
| What we've pushed | `Checkpoint.ResumeToken` = the **contiguous ack frontier** (every event ≤ here is pub-acked + durable) |
| What's next to push | the next event the cursor yields after the frontier / head of the channel |
| What comes after | the strictly oplog-ordered tail |

- **Resume tokens are monotonic** (encode oplog position) — comparable; the frontier only moves forward along the *contiguous* acked prefix, never jumping a gap.
- **JetStream stream sequence is monotonic** — a second independent ordering the transformer reads off the consumer side.

**Invariant — per-collection order preserved end to end.** Per collection there is exactly **one watcher** publishing in oplog order on one connection, so wire order = stream-sequence order. Each `PublishMsg` blocks on its pub-ack before the next event; on a failed ack the watcher **stalls and retries** — it never advances the frontier past a gap. This is what makes "what's next" truly the next event, not a hole.

**Caveat — no global cross-collection total order.** Watchers are independent change streams, so order is strict *within* a collection but **not** between collections. `ClusterTime` gives a coarse cross-collection sort (ties possible), not a strict total order. A transformer needing cross-entity causal order must tolerate reordering rather than assume one global sequence. This is inherent to per-collection watchers; the alternative (one whole-DB stream) trades away per-collection parallelism and the clean per-collection checkpoint, and is rejected.

---

## 7. HA, error handling & lifecycle

### 7.1 HA — single replica + resume

`replicas=1`. No leader election. Failover = pod restart; losslessness comes from the persisted checkpoint + the soak/dedup window, not from a hot standby.

### 7.2 Error handling

- **Invalid / expired resume token** (`ChangeStreamHistoryLost`, code 286): loud `slog.Error`, **exit non-zero**. The connector does **not** silently reseed-from-now — that would drop events. Recovery is operator-driven and uses the same seeding model as §4.3: re-snapshot via the `history-migrator`, update the seed doc / `START_RESUME_TOKEN`, restart.
- **Publish failure (no pub-ack):** retry with backoff. The contiguous frontier does **not** advance past an un-acked event, so the token is never persisted ahead of durably-stored data. Sustained failure → the bounded channel applies backpressure, the reader stalls, the lag metric climbs, an alert fires.
- **Checkpoint persistence:** the token for collection C is `Save`d only after **every event ≤ that position** has a pub-ack — at-least-once, never loss. Crash → resume `startAfter` the last persisted token → duplicates collapse on `Nats-Msg-Id`.
- **No client-facing `errcode`:** no request/reply handlers; all errors are internal/operational (wrapped with `fmt.Errorf("...: %w", err)` per CLAUDE.md §3).

### 7.3 Graceful shutdown (`pkg/shutdown.Wait`, ≤25s)

cancel watchers → await watcher exit (bounded timeout) → each persists its final frontier → close change-stream cursors → `nc.Drain()` → disconnect Mongo.

### 7.4 Observability

`log/slog` JSON. Correlation field per event = `EventID` (the resume-token data). OTel tracing + Prometheus metrics via `otelutil` (`InitTracer`/`InitMeter`), exposed on a `/metrics` + `/healthz` listener at `METRICS_ADDR` (the k8s probe target). Metrics, all by `collection`: `oplog_replication_lag_ms` (now − `clusterTime` at publish), `oplog_events_published_total` (throughput), `oplog_publish_errors_total`, `oplog_events_skipped_total` (poison). For this single-replica pump, **alert on lag + sustained publish errors** — the signal that a retry-forever stall is eating the oplog window (§5/§6).

### 7.5 Deployment topology — message vs collection split (2026-07-09)

The connector runs as **two deployments of the same binary** with disjoint `WATCH_COLLECTIONS`
(see `2026-07-09-oplog-connector-deployment-split-design.md`):

- `oplog-connector-messages` — watches only `rocketchat_message`; federation-origin `$match` active.
- `oplog-connector-collections` — watches everything else; filter inactive (nothing to filter).

Rationale: a fatal watcher error tears down its whole process, so message CDC must not share a
failure domain with low-value operational collections. Checkpoints are per-collection and the
stream's subjects are per-collection, so the split needs no data or contract change.

**Cross-deployment invariant (ops/IaC-owned, not code-enforced):** the two watch sets are
disjoint, and exactly one deployment includes the message collection. `MESSAGE_COLLECTION` must
be non-empty everywhere but need not be watched; each pod's startup log states its role
("federation-origin filter active" vs "… filter inactive (collections role)").

---

## 8. Testing (CLAUDE.md §4 — TDD, 80% floor / 90% target on handler + store)

### 8.1 Unit (`handler_test.go`) — mocked `CheckpointStore` + injected publish fn capturing payloads

Table-driven over:
- op types (insert/update/replace/delete) → correct subject + envelope fields,
- `fullDocument` present for insert/replace, `updateDescription` (delta) for update, neither for delete — no lookups,
- **frontier advances only along the contiguous acked prefix**,
- **publish failure stalls the frontier** (token not persisted past a gap),
- **token persisted only after ack**,
- start-point resolution precedence (override > checkpoint > cold-start default),
- envelope marshal/unmarshal.

### 8.2 Integration (`integration_test.go`, `//go:build integration`)

`testutil.MongoDB` + `testutil.NATS`; `TestMain` → `testutil.RunTests`. Insert/update/delete in source → assert envelope lands on `MIGRATION_OPLOG_{site}` with right subject/headers; **resume-after-restart** from a persisted token (no gap); **dedup** on redelivery (same `Nats-Msg-Id`); seed-checkpoint start (`startAfter`) begins exactly after the seeded point.

### 8.3 Model (`pkg/model`)

`OplogEvent` round-trips via the existing generic `roundTrip` helper.

---

## 9. Open / deferred

- ~~Final `WATCH_COLLECTIONS` set~~ — **resolved**: the 8 collections are fixed in §5.3 (no `PREIMAGE_COLLECTIONS` — the connector does no lookups/pre-images).
- Soak retention window sizing — ops/IaC decision.
- The downstream **`oplog-transformer`** (consumes `MIGRATION_OPLOG_{site}`) is a separate spec / sibling component under `data-migration/`.
