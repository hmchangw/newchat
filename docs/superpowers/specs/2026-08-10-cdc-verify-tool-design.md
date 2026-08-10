# CDC Migration Verification Tool (`tools/cdc-verify`) — Design

**Date:** 2026-08-10 · **Status:** Draft — awaiting user review
**Companion docs:** `data-migration/README.md`, `data-migration/CDC_COVERAGE.md`, `tools/nats-debug/README.md`

## 1. Purpose

A live dashboard that answers, during the CDC migration window: *"is the pipeline
actually converging the destination to the source?"* It sits beside the
`oplog-connector → MIGRATION-OPLOG-{site} → transformers` pipeline as a passive
observer:

1. **Stream telemetry** — total messages, per-subject (collection.op) counts,
   publish rate, and transformer consumer lag on `MIGRATION-OPLOG-{siteID}`.
2. **Per-event verification** — for every CDC event observed, look up the
   current doc in the **source Mongo** and the mapped record in the
   **destination store** (target Mongo or Cassandra), compare them field-by-field
   using a **JSON-configurable field mapping** per source→dest pair.
3. **Result surfaces** — a capped tailing table of recent verification results
   (frozen once matched) and a separate accumulated table of failures.

The tool is **strictly read-only** on every store and uses an **ephemeral**
JetStream consumer, so it can be started/stopped freely without perturbing the
migration (no durable state, no acks that matter, no writes).

Like `data-migration/`, this tool is deletable once the source is retired.

## 2. Non-goals

- Not a bulk/full-scan consistency checker — it verifies only keys that emit
  CDC events while the tool is watching. (A bulk sweeper can be a later, separate tool.)
- No repair/backfill actions — observe and report only.
- No persistence of results — in-memory, capped; results die with the process
  (failures are exportable as JSON before shutdown).
- No auth/multi-tenant UI — same trust level as `tools/nats-debug`, an
  operator-only tool on a private network.
- Not a replacement for the transformers' own metrics — it complements them
  with end-to-end data-level evidence.

## 3. Approaches considered

| | Approach | Trade-offs |
|---|---|---|
| **A (chosen)** | **Browser UI, `nats-debug` style** — flat `package main` under `tools/cdc-verify`, plain `net/http` + SSE + single static page | Matches an existing, proven repo pattern; live colour-coded tables are easy; zero new dependencies; shareable on a screen during cutover |
| B | Terminal TUI (bubbletea/tview) | Good over SSH but needs a new third-party dependency (CLAUDE.md: ask first) and re-invents tables the browser gives us free |
| C | Plain CLI with periodic snapshot logs | Simplest, but loses the live tailing/freeze UX that is the point of the tool |

One deliberate divergence from `nats-debug`: connections are **configured by env
at startup** (fail-fast), not entered per browser session. Verification state is
global — every viewer sees the same single pipeline — so the browser is a pure
read-only viewer over SSE and there is no per-session hub.

## 4. Architecture

```
                    ┌────────────────────────── tools/cdc-verify ──────────────────────────┐
MIGRATION-OPLOG ────▶ watcher (ephemeral consumer, deliver-new) ─▶ verifier engine ─▶ results store (ring buffers)
   {siteID}     ────▶ stats poller (StreamInfo + consumer lag)  ──────────────┐             │
                    │                                                         ▼             ▼
                    │        source Mongo ◀── lookups ──▶ target Mongo / Cassandra    SSE broadcaster
                    └─────────────────────────────────────────────────────────────────┬─────┘
                                                                                      ▼
                                                                              browser (static page)
```

Components (each independently testable):

- **watcher** (`watcher.go`) — ephemeral pull consumer on `MIGRATION-OPLOG-{siteID}`,
  `DeliverNew` (default) or `DeliverByStartTime` via env for short replays. Parses
  subject → `(collection, op)`, extracts the document key from the raw CDC payload
  (`documentKey._id` — present for every op), hands `(collection, op, key, event)`
  to the verifier. Always acks (results don't depend on redelivery).
- **stats poller** (`stats.go`) — every `STATS_INTERVAL` (default 5s) fetches
  `StreamInfo` with subject filtering (`chat.migration.oplog.{siteID}.>`) for
  total/per-subject message counts and bytes; computes rate from count deltas;
  fetches `ConsumerInfo.NumPending`+`NumAckPending` for the configured
  transformer durables (`TRACK_CONSUMERS`, comma list) as **lag**.
- **verifier engine** (`verifier.go`) — the core; see §5.
- **mapping config** (`mapping.go`) — loads/validates the JSON mapping file at
  startup; see §6.
- **lookups** (`lookup_source.go`, `lookup_target_mongo.go`, `lookup_cassandra.go`) —
  read-only point lookups behind small consumer-defined interfaces
  (`sourceLookuper`, `targetLookuper`) so the engine is unit-testable with mocks.
- **results store** (`results.go`) — two in-memory structures guarded by a mutex:
  a ring buffer of recent results (`RECENT_CAP`, default 200) and a capped
  failures list (`FAILED_CAP`, default 1000, oldest evicted with an eviction
  counter so silent loss is visible). Also serves `GET /failures.json`.
- **SSE broadcaster + UI** (`hub.go`, `handler.go`, `static/`) — pushes stats
  ticks and result transitions to all connected browsers; static page renders
  three panels: **Stream stats**, **Recent verifications** (capped, tailing,
  newest on top), **Failures**.

## 5. Verification semantics

### 5.1 Model: event-triggered convergence check (not event-replay check)

For each event we verify **current source state vs current destination state**
for that document key — the event is only the *trigger* that tells us the key
changed. We do **not** try to verify "the destination reflects exactly this
event", because with async transformers a later update legitimately overwrites
the earlier event's values.

This directly addresses the freeze problem raised in the request: an old
event's expected values go stale when a newer update lands. Under the
convergence model both sides are read *now*, so a check can only flip from
mismatch→match as the pipeline catches up — and we freeze it at match.

### 5.2 Check lifecycle

One **check** per observed event (one table row), deduplicated per key while pending:

```
event observed ──▶ PENDING ── poll compare every VERIFY_POLL (default 2s) ──▶ MATCHED  (frozen — never re-checked)
                     │                                                   └──▶ FAILED   (VERIFY_TIMEOUT elapsed, default 60s;
                     │                                                          moved to failures table with field-level diff)
                     ├──▶ SUPERSEDED (newer event for the same key arrived while pending —
                     │        the new event's check takes over; not a failure)
                     └──▶ SKIPPED  (unmapped collection, or op marked skip in the mapping —
                              matches CDC_COVERAGE.md's ❌ rows; counted, not failed)
```

- **Fan-out sub-checks:** because a source collection maps to *multiple*
  destination targets (§6), one check fans out into **one sub-check per target**.
  Each sub-check looks up its own dest record and compares only the fields
  mapped to that target, polls independently, and **freezes on its own match**.
  The row is MATCHED (and frozen) when *all* sub-checks have matched; it is
  FAILED when the deadline passes with any sub-check unmatched — the failure
  records which targets failed and their per-field diffs.
- **Freeze on match:** a MATCHED row is immutable. Later events for the same key
  start *new* checks with new rows.
- **First comparison happens immediately**, then polls — most events verify on
  the first or second poll once the transformer has applied them.
- A `FAILED` row records the final diff: per-field `(mapped source value, dest value)`
  for mismatched fields, or `doc missing in dest` / `doc missing in source`.
- **Deletes:** the source doc is gone, so for mappings where delete is migrated
  (direct-transfer collections) the check asserts **absence** in the destination
  (`verify-absent`). Collections where delete is intentionally not migrated
  (rooms, subscriptions, thread-subs per the coverage matrix) mark delete `skip`.
- **Concurrency:** a worker pool (`MAX_CHECKS`, default 32) with a semaphore;
  per-key pending dedup keeps the DB load bounded to distinct hot keys.
- **Sampling:** `SAMPLE_PERCENT` (default 100) applied per collection-op after
  skip-classification, for high-volume message bursts.

### 5.3 Comparison rules

- Values are compared after **normalization**: BSON/CQL scalars to canonical Go
  forms (all ints/floats → comparison as numbers, `primitive.DateTime`/`time.Time`
  → unix ms, ObjectID/UUID → canonical string). Optional per-field `transform`
  (see §6) applies before comparison.
- A source field that is **absent/null** matches a dest field that is
  absent/zero-value, unless the mapping marks the field `required`.
- Two modes per mapping: `fields` (explicit list — only listed fields compared)
  and `verbatim` (deep-equal whole docs minus an `ignore` list — for the
  `oplog-direct-transfer` collections).

## 6. Mapping configuration (JSON)

The mapping is **cross-table and cross-DB with fan-out**: one source field may be
verified against fields in *several* destination tables, and several source
collections may each verify their own slice of *one* destination table. Source
is always Mongo; each destination target is Mongo **or** Cassandra.

Shape: per source collection, (a) a set of named **targets** — each declaring
where the dest record lives and how to locate it from the source doc — and
(b) a field-centric **fields** map in exactly the requested form,
`source field → [dest table.field, …]`:

```jsonc
{
  "sources": [
    {
      "collection": "rocketchat_message",
      "ops": { "insert": "verify", "update": "verify", "replace": "verify", "delete": "verify-absent" },
      "targets": {
        "msgById":   { "kind": "cassandra", "table": "messages_by_id",
                       "key": { "message_id": "_id" } },
        "msgByRoom": { "kind": "cassandra", "table": "messages_by_room",
                       "key": { "room_id": "rid",
                                "bucket":     { "from": "ts", "transform": "msgBucket" },
                                "created_at": { "from": "ts", "transform": "unixMilli" },
                                "message_id": "_id" } }
      },
      "fields": {
        "msg":   [ "msgById.body", "msgByRoom.body" ],          // 1 source field → 2 dest tables
        "u._id": [ "msgById.sender_account", "msgByRoom.sender_account" ],
        "ts":    [ { "dest": "msgById.created_at",   "transform": "unixMilli" },
                   { "dest": "msgByRoom.created_at", "transform": "unixMilli" } ]
      }
    },
    {
      "collection": "rocketchat_subscription",
      "ops": { "insert": "verify", "update": "verify", "replace": "verify", "delete": "skip" },
      "targets": {
        "subs": { "kind": "mongo", "collection": "subscriptions",   // re-keyed dest: located by
                  "key": { "roomId": "rid", "account": "u.username" } }, // field query, not _id
        "members": { "kind": "mongo", "collection": "room_members",
                     "key": { "roomId": "rid", "member.id": "u.username" } }
      },
      "fields": {
        "open": [ "subs.joined" ],
        "f":    [ "subs.favorite" ],
        "ts":   [ { "dest": "subs.createdAt", "transform": "unixMilli" },
                  { "dest": "members.joinedAt", "transform": "unixMilli" } ]
      }
    },
    {
      "collection": "rocketchat_avatar",
      "ops": { "insert": "verify", "update": "verify", "replace": "verify", "delete": "verify-absent" },
      "targets": {
        "avatar": { "kind": "mongo", "collection": "rocketchat_avatar",
                    "key": { "_id": "_id" },
                    "mode": "verbatim", "ignore": [ "_updatedAt" ] }   // direct-transfer: whole-doc
      }
    }
  ]
}
```

Schema rules:

- **Target** = `{ kind: "mongo"|"cassandra", collection|table, key, mode?, ignore? }`.
  The **key spec** maps *dest* key fields ← *source* dot-paths (optionally through a
  transform, e.g. `msgBucket` for the bucketed partition key via `pkg/msgbucket`).
  Mongo lookup = filter + projection limited to compared fields (per CLAUDE.md);
  Cassandra lookup = `SELECT <cols> … WHERE <key>` on the named table.
- **Fields map** = `sourcePath: [destRef, …]` — fan-out is the normal case. A
  `destRef` is either the shorthand string `"<targetAlias>.<destFieldPath>"`
  (alias is the first dot-segment; aliases must not contain dots) or the object
  form `{ "dest": "...", "transform": "...", "required": true }`.
- **Many→one (derived values):** an optional `derived` list per source supports
  multiple source fields combining into one dest field through a named transform,
  e.g. `{ "from": ["u._id", "peer._id"], "transform": "dmRoomID", "dest": ["rooms._id"] }`.
  Key specs accept the same multi-`from` form.
- **Two source collections → one dest table** needs no special syntax: each
  source entry declares its own target pointing at the same dest table, with its
  own key spec, and verifies only the fields it maps. (No cross-collection joins
  in v1 — each event verifies from its own source doc.)
- **Verbatim mode** is per-target (`mode: "verbatim"` + `ignore` list) for the
  `oplog-direct-transfer` collections; a verbatim target takes no `fields` entries.
- **Transform vocabulary (initial, deliberately small):** `unixMilli`,
  `toString`, `msgBucket`, plus named domain transforms registered in Go (e.g.
  `roomType`, `dmRoomID`). Adding one = a Go function + registry entry; the JSON
  references it by name only.
- **Source doc for the check** is always re-read from source Mongo by
  `documentKey._id` (never trusted from the event payload — updates carry only
  deltas anyway). Read preference `primaryPreferred`, same rationale as the transformer.
- **Startup validation, fail-fast:** unknown transform names, `destRef` aliases
  with no matching target, empty key specs, a `cassandra` target without a
  `table`, a verbatim target that is also referenced by `fields`. Collections
  observed on the stream with no `sources` entry are classified SKIPPED
  (`unmapped`) and counted — coverage gaps are visible, never silent.
- The shipped default lives at `tools/cdc-verify/mapping.example.json`, covering
  the pairs in `CDC_COVERAGE.md`; ops teams tune per environment. *(Field lists
  above are illustrative — exact per-collection lists are an implementation-plan
  task, derived from the transformers + `SOURCE_DATA.md`.)*

## 7. UI

Single static page (`static/index.html`, vanilla JS like nats-debug), three panels:

1. **Stream stats header** — stream totals, msgs/s (1m sliding window), bytes,
   per-`collection.op` count chips, and per-tracked-consumer lag badges
   (green/amber/red thresholds).
2. **Recent verifications** — capped tailing table (newest first): time,
   collection, op, key, state badge (PENDING spinner / MATCHED green frozen /
   FAILED red / SKIPPED grey / SUPERSEDED grey), duration-to-match, attempts,
   and one per-target chip per sub-check (e.g. `msgById ✓ msgByRoom …`) so
   partial convergence is visible. Rows update in place via SSE until frozen.
   Filter box by collection/key.
3. **Failures** — separate accumulated table: everything from the recent-row plus
   expandable **per-target** field-level diff; counters for total failed /
   evicted; a **Download JSON** button (`/failures.json`).

Also `GET /healthz` and a summary counters strip (checked / matched / failed /
skipped / superseded since start).

## 8. Configuration (env)

| Env | Req | Default | Purpose |
|---|---|---|---|
| `SITE_ID` | ✓ | — | stream name + subject scope |
| `NATS_URL` | ✓ | — | JetStream containing `MIGRATION-OPLOG-{site}` |
| `NATS_CREDS_FILE` | | `""` | optional creds (checked at startup) |
| `SOURCE_MONGO_URI` / `SOURCE_MONGO_USERNAME` / `SOURCE_MONGO_PASSWORD` | ✓/–/– | — | source RS, creds out of URI (same rule as connector) |
| `SOURCE_DB` | | `rocketchat` | source database |
| `TARGET_MONGO_URI` / `TARGET_MONGO_USERNAME` / `TARGET_MONGO_PASSWORD` | ✓/–/– | — | destination Mongo |
| `TARGET_DB` | | `chat` | destination database |
| `CASSANDRA_HOSTS` / `CASSANDRA_KEYSPACE` | ◐ | — | destination Cassandra (via `pkg/cassutil`); required only when the mapping file references a `cassandra` dest — validated at startup |
| `MAPPING_FILE` | ✓ | — | JSON mapping path |
| `MESSAGE_BUCKET_HOURS` | | `72` | bucket window for the `msgBucket` transform — MUST match the services writing `messages_by_room` |
| `TRACK_CONSUMERS` | | `""` | comma list of durable consumer names to show lag for |
| `START_AT_TIME` | | `""` | optional replay start (RFC3339) instead of deliver-new |
| `VERIFY_POLL` / `VERIFY_TIMEOUT` | | `2s` / `60s` | check polling cadence / failure deadline |
| `MAX_CHECKS` | | `32` | concurrent check workers |
| `SAMPLE_PERCENT` | | `100` | per-event sampling after skip-classification |
| `RECENT_CAP` / `FAILED_CAP` | | `200` / `1000` | table caps |
| `STATS_INTERVAL` | | `5s` | stream stats poll cadence |
| `PORT` | | `8091` | UI port |

Fail-fast on all required vars; missing mapping file or invalid JSON exits non-zero.

## 9. Error handling

- Lookup errors (source or dest store unavailable) do **not** fail a check —
  the poll retries until `VERIFY_TIMEOUT`; the row shows the last error. A
  distinct `error` cause is recorded on failure rows so "store down" is
  distinguishable from "data mismatch".
- Watcher consume errors: log + reconnect with backoff; stats panel shows
  watcher liveness so a stalled feed is visible, never silent.
- Standard graceful shutdown via `pkg/shutdown.Wait`: stop watcher iterator →
  drain check workers (`wg.Wait` with timeout) → close SSE → `nc.Drain()` →
  disconnect Mongo/Cassandra.
- Internal errors wrapped with context per CLAUDE.md; no `errcode` needed (no
  client-facing NATS/Gin API surface beyond the viewer endpoints).

## 10. Testing (TDD throughout)

- **Unit** (mocked lookups + fake clock): mapping load/validation table tests
  (fan-out refs, derived multi-`from`, alias resolution, verbatim rules);
  comparison/normalization table tests (types, absent/null, transforms, verbatim
  ignore); check lifecycle (multi-target fan-out — partial then full match,
  per-target freeze, one-target timeout→failed with per-target diff, supersede,
  skip, sampling); ring-buffer caps + eviction counters; stats rate math;
  subject parsing.
- **Integration** (`//go:build integration`, `pkg/testutil` containers:
  `NATS(t)`, `MongoDB(t, ...)` ×2 (source/target), `CassandraKeyspace(t, ...)`):
  end-to-end — publish a CDC event, pre-seed source+target, assert MATCHED;
  delayed target write → PENDING→MATCHED; wrong value → FAILED with diff;
  delete → verify-absent; `TestMain` via `testutil.RunTests`.
- Mocks via `make generate` (mockgen) for the lookup interfaces.
- Coverage: ≥80% floor, targeting 90%+ on verifier/mapping/compare.

## 11. Repo placement & delivery

- `tools/cdc-verify/` — flat `package main`, per-service layout adapted:
  `main.go`, `watcher.go`, `verifier.go`, `mapping.go`, `compare.go`,
  `lookup_*.go`, `results.go`, `stats.go`, `hub.go`, `handler.go`, `static/`,
  `mapping.example.json`, `README.md`, tests alongside.
- `deploy/docker-compose.yml` — the tool + (for local demo) NATS with JetStream,
  two Mongos, Cassandra; `deploy/Dockerfile` multi-stage per repo convention.
  No `azure-pipelines.yml` (operator tool, run ad hoc — same as nats-debug precedent).
- No new third-party dependencies.

## 12. Open decisions (defaults chosen, please confirm)

1. **UI form** — browser UI (Approach A) over TUI/CLI. *(Assumed from the
   nats-debug reference; flag if you wanted a terminal TUI.)*
2. **Convergence semantics** (§5.1) — verify current-vs-current triggered by
   events, freeze on match, rather than per-event value replay. This matches
   the freeze rationale in the request but is worth an explicit yes.
3. **Env-configured connections** with the browser as pure viewer (vs
   nats-debug's connect-from-the-UI sessions).
4. ~~`messages_by_id` as the sole Cassandra verification table~~ — resolved by
   the fan-out mapping: messages verify against both `messages_by_id` and
   `messages_by_room` (bucket key derived via the `msgBucket` transform;
   `MESSAGE_BUCKET_HOURS` added to config to keep bucket math aligned).
5. **Failure retention** — in-memory capped at `FAILED_CAP` with JSON export;
   no persistence.
