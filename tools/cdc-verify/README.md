# CDC Migration Verification Tool

A live dashboard that answers, during the CDC migration window: *"is the
pipeline actually converging the destination to the source?"* It sits beside
the `oplog-connector → MIGRATION-OPLOG-{site} → transformers` pipeline as a
passive observer: it watches CDC events, re-reads the current source and
destination state for the affected key, compares them field-by-field against
a JSON-configurable mapping, and streams the results to a browser dashboard.

The tool is **strictly read-only** on every store and uses an **ephemeral**
JetStream consumer (deliver-new by default), so it can be started and
stopped freely without perturbing the migration — no durable state, no acks
that matter, no writes. Like `data-migration/`, it is **deletable** once the
source is retired.

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

## Quick Start

### Option 1 — Docker Compose (recommended)

Spins up NATS+JetStream, a source Mongo, a target Mongo, a Cassandra node,
and the tool in one command:

```bash
cp tools/cdc-verify/mapping.example.json tools/cdc-verify/deploy/mapping.local.json
docker compose -f tools/cdc-verify/deploy/docker-compose.yml up
```

Open http://localhost:8091.

First boot takes **about a minute**: Cassandra is slow to initialize, and a
`cassandra-init` one-shot service waits for its healthcheck before applying
the repo's real schema (keyspace `chat` plus the `messages_by_id` /
`messages_by_room` tables `mapping.example.json` references) — the same
`docker-local/cassandra/init/*.cql` files the main dev stack uses.
`cdc-verify` itself waits on that init job to complete (and on NATS's
healthcheck) before starting, and restarts on failure, so it comes up clean
instead of racing a not-yet-ready Cassandra.

`mapping.local.json` is operator-provided next to the compose file (bind-mounted
read-only, gitignored) — the copied `mapping.example.json` works as the
mapping input out of the box. The compose stack sets `BOOTSTRAP_STREAMS=true`
so the tool creates `MIGRATION-OPLOG-{siteID}` itself (this stack has no
`oplog-connector` standing it up); in production that stream already exists
and the gate stays off (see Configuration below). With no `oplog-connector`
or seeded data feeding it, the stack starts **idle** — an empty dashboard —
until you seed matching documents in source/target and publish oplog events
onto the stream (or point `NATS_URL`/`SOURCE_MONGO_URI`/`TARGET_MONGO_URI` at
a real migration environment instead).

### Option 2 — Run the binary directly

Requires NATS (with JetStream), source/target Mongo, and (if the mapping
references any Cassandra target) Cassandra already running and reachable.

```bash
# Build
make build SERVICE=tools/cdc-verify

# Run
SITE_ID=site1 \
NATS_URL=nats://localhost:4222 \
SOURCE_MONGO_URI=mongodb://localhost:27017 \
TARGET_MONGO_URI=mongodb://localhost:27018 \
MAPPING_FILE=tools/cdc-verify/mapping.example.json \
./bin/cdc-verify
```

Open http://localhost:8091 (or the port set by `PORT`).

## The UI

Two tabs, updated live over Server-Sent Events (`GET /api/events`):

**NATS tab** — what is flowing through the stream:

- Stream summary cards: total messages, msgs/s (sliding window), bytes, and
  the first–last sequence range, plus watcher liveness (a stalled feed is
  visible, never silent).
- **Subjects** table: one row per observed subject with its collection, op,
  and message count, sorted by volume.
- **Consumers** table: per-tracked-consumer pending/ack-pending and a
  caught-up / behind / lagging status, for the durables named in
  `TRACK_CONSUMERS`.
- **Event feed**: a capped live ticker of decoded CDC events (time,
  collection, op, doc id, disposition — including skipped/unmapped events).

**Verification tab** — what the checks concluded:

- A counters strip (checked / matched / failed / skipped / superseded).
- **Mongo → Mongo** and **Mongo → Cassandra** sections, each with a
  **separate capped tailing table per collection pair** from the mapping
  (e.g. `rocketchat_subscription → subscriptions`, `rocketchat_message →
  messages_by_id`). Each row shows that pair's own sub-check status
  (matched / cause / diff count) alongside the overall check state,
  attempts, and duration-to-match. Rows update in place until they reach a
  terminal state (frozen).
- **Failures** — an accumulated table with an expandable per-target
  field-level diff and a **Download JSON** button (`GET /failures.json`).

Also `GET /healthz`; `GET /api/state` serves the initial snapshot including
the mapping's pair list.

## Verification Semantics

### Convergence model

For each observed event, the check verifies **current source state vs
current destination state** for that document's key — the event is only the
trigger telling the tool the key changed. It never tries to verify "the
destination reflects exactly this event's payload", because with async
transformers a later update can legitimately overwrite the earlier event's
values. Both sides are read *now* on every poll, so a check can only flip
mismatch → match as the pipeline catches up, and it freezes there.

### Check lifecycle

One check per observed event (one table row), deduplicated per key while
pending:

```
event observed ──▶ PENDING ── poll compare every VERIFY_POLL (default 2s) ──▶ MATCHED  (frozen — never re-checked)
                     │                                                   └──▶ FAILED   (VERIFY_TIMEOUT elapsed, default 60s;
                     │                                                        moved to failures table with field-level diff)
                     ├──▶ SUPERSEDED (a newer event for the same key arrived while pending —
                     │        the new event's check takes over; not a failure)
                     └──▶ SKIPPED  (unmapped collection, or op marked "skip" in the mapping;
                              counted, not failed)
```

- **Fan-out sub-checks:** a source collection can map to *multiple*
  destination targets. One check fans out into one sub-check per target;
  each looks up its own dest record, compares only the fields mapped to
  that target, polls independently, and freezes on its own match. The row
  is MATCHED once *all* sub-checks have matched; it is FAILED if the
  deadline passes with any sub-check unmatched.
- **Freeze on match:** a MATCHED row is immutable — later events for the
  same key start new checks with new rows.
- **Deletes:** for mappings where delete is migrated, the check asserts
  **absence** in the destination (`verify-absent`, cause `still-present` if
  the record hasn't been removed yet). Collections where delete is
  intentionally not migrated mark the op `skip`.
  - **`verify-absent` constraint:** a delete event carries only the source
    document's `_id` — no other fields — so `verify-absent` only works for a
    target whose `key` derives entirely from `_id`. A target keyed on other
    source fields (e.g. `messages_by_room`'s `room_id`/`bucket`/`created_at`,
    derived from the now-gone document) can't build a lookup key on delete;
    map that collection's `delete` op to `skip` instead.
- **Concurrency:** a worker pool (`MAX_CHECKS`) bounds concurrent checks;
  per-key pending dedup keeps DB load bounded to distinct hot keys.
- **Sampling:** `SAMPLE_PERCENT` (default 100) is applied per event after
  skip-classification, for high-volume bursts.

### Failure causes

A sub-check's `lastCause` (and a FAILED row's per-target cause) is one of:

| Cause | Meaning |
|---|---|
| `mismatch` | Both records exist but one or more compared fields differ — the failure row carries the per-field diff. |
| `dest-missing` | The destination record was never found before the deadline. |
| `source-missing` | The source document disappeared between the event and the check (superseded by that document's own delete check). |
| `resolver-miss: <alias>` | A `resolvers` lookup found nothing — the dest-side record this check depends on may not exist yet. |
| `ambiguous-key` | A dest lookup matched more than one document/row — a mapping bug (non-unique key), fails immediately without waiting for the deadline. |
| `key-unresolvable` | A key column's source path had no value, or its transform failed. |
| `still-present` | A `verify-absent` check (delete) found the destination record still there. |
| `lookup-error: <err>` | The source or destination store returned an error on lookup — retried until the deadline; distinguishes "store down" from "data mismatch". |

## Mapping Configuration Reference

The mapping file (`MAPPING_FILE`) is JSON, one entry per source Mongo
collection under `sources`. See `mapping.example.json` for a runnable
template covering each shape below; exact per-collection field lists for a
production run are derived from `data-migration/SOURCE_DATA.md` and the
transformers, then tuned per environment.

```jsonc
{
  "sources": [
    {
      "collection": "<source Mongo collection>",
      "ops": { "insert": "verify", "update": "verify", "replace": "verify", "delete": "verify-absent" },
      "resolvers": { "<alias>": { "db": "source|target", "collection": "...", "key": {...}, "fields": [...] } },
      "targets": { "<alias>": { "kind": "mongo|cassandra", "collection|table": "...", "key": {...}, "mode": "verbatim", "ignore": [...] } },
      "fields": { "<sourcePath>": [ "<alias>.<destPath>", ... ] },
      "derived": [ { "from": [...], "transform": "...", "dest": [...] } ]
    }
  ]
}
```

- **`ops`** maps a change-stream op (`insert` / `update` / `replace` /
  `delete`) to an action: `verify` (compare), `verify-absent` (assert the
  destination no longer has the record — used for migrated deletes), or
  `skip` (count, don't check). An op with no entry defaults to `skip`.
- **`targets`** — one entry per destination the collection verifies against.
  `kind: "mongo"` requires `collection`; `kind: "cassandra"` requires
  `table`. `key` maps each destination key column to a source dot-path (or
  `{"from": "path"|[...paths], "transform": "name"}`) — the same shape used
  everywhere a key is built. A dest lookup matching more than one
  document/row fails the sub-check immediately with `ambiguous-key`.
  `mode: "verbatim"` (with an optional `ignore` field list) deep-compares
  the whole document instead of an explicit field list — for
  oplog-direct-transfer collections; a verbatim target takes no `fields`
  entries.
- **`resolvers`** — named intermediate point lookups for when the
  destination's lookup key isn't in the source document (e.g. a dest keyed
  by `username` when the source doc only carries `u._id`). Each resolver
  hops once against `db: "source"` or `db: "target"` Mongo, projected to its
  `fields` list, and is cached per check attempt. Resolved values are
  addressable elsewhere as `@alias.field` (in `targets[].key`, `fields`
  source paths, and `derived[].from`). Resolver keys use plain source paths
  only — no `@` chaining. A resolver that finds nothing marks dependent
  sub-checks' current attempt `resolver-miss: <alias>`; they keep polling
  until the deadline.
- **`fields`** — `sourcePath: [destRef, ...]`, the normal fan-out case: one
  source field verified against several destination fields (possibly across
  different targets). A `destRef` is either the shorthand string
  `"<targetAlias>.<destFieldPath>"` or the object form
  `{"dest": "...", "transform": "...", "required": true}`.
- **`derived`** — for many→one values: several source fields combine into
  one destination field through a named transform, e.g.
  `{"from": ["u._id"], "transform": "toString", "dest": ["msgById.sender.account"]}`.
- **Transform vocabulary:** `unixMilli` (coerce a time/int/float to Unix
  milliseconds), `toString` (stringify a scalar), `msgBucket` (bucket a
  timestamp via `pkg/msgbucket`, keyed off `MESSAGE_BUCKET_HOURS` — MUST
  match the bucket window the services writing `messages_by_room` use).
  Adding a transform is a Go function plus a registry entry in
  `transform.go`; the JSON references it by name only.
- **Startup validation (fail-fast):** unknown transform names, `destRef`
  aliases with no matching target, `@alias` references with no matching
  resolver, a resolver key using `@` chaining, empty key specs, a
  `cassandra` target without a `table`, a verbatim target also referenced by
  `fields`, duplicate source collections. Collections observed on the
  stream with no `sources` entry are classified SKIPPED (`unmapped`) and
  counted — coverage gaps are visible, never silent.

## Configuration

| Env | Required | Default | Purpose |
|---|---|---|---|
| `SITE_ID` | yes | — | Stream name + subject scope (`MIGRATION-OPLOG-{siteID}`) |
| `NATS_URL` | yes | — | JetStream containing `MIGRATION-OPLOG-{siteID}` |
| `NATS_CREDS_FILE` | no | `""` | Optional NATS creds file (JWT + NKey); checked for existence at startup |
| `SOURCE_MONGO_URI` | yes | — | Source Mongo connection string |
| `SOURCE_MONGO_USERNAME` / `SOURCE_MONGO_PASSWORD` | no | `""` | Source Mongo auth |
| `SOURCE_DB` | no | `rocketchat` | Source database name |
| `TARGET_MONGO_URI` | yes | — | Destination Mongo connection string |
| `TARGET_MONGO_USERNAME` / `TARGET_MONGO_PASSWORD` | no | `""` | Target Mongo auth |
| `TARGET_DB` | no | `chat` | Destination database name |
| `CASSANDRA_HOSTS` / `CASSANDRA_KEYSPACE` | conditional | `""` | Destination Cassandra, via `pkg/cassutil`; required only when the mapping file references a `cassandra` target — validated at startup |
| `CASSANDRA_USERNAME` / `CASSANDRA_PASSWORD` | no | `""` | Cassandra auth |
| `MAPPING_FILE` | yes | — | Path to the JSON mapping file |
| `BOOTSTRAP_STREAMS` | no | `false` | Dev-only: create `MIGRATION-OPLOG-{siteID}` (schema only — Name+Subjects) if it doesn't exist. In production this stays off — the stream is owned by `oplog-connector`/ops (CLAUDE.md "Stream bootstrap is opt-in"), and the tool stays strictly read-only there. `docker-compose.yml` sets this `true` since the local stack has no `oplog-connector`. |
| `MESSAGE_BUCKET_HOURS` | no | `72` | Bucket window for the `msgBucket` transform — MUST match the services writing `messages_by_room` |
| `TRACK_CONSUMERS` | no | `""` | Comma-separated durable consumer names to show lag for |
| `START_AT_TIME` | no | `""` | Optional replay start (RFC3339) instead of deliver-new |
| `VERIFY_POLL` | no | `2s` | Check polling cadence |
| `VERIFY_TIMEOUT` | no | `60s` | Deadline before a pending check fails |
| `MAX_CHECKS` | no | `32` | Concurrent check worker budget |
| `SAMPLE_PERCENT` | no | `100` | Per-event sampling after skip-classification (0-100) |
| `RECENT_CAP` | no | `200` | Recent-results ring buffer size |
| `FAILED_CAP` | no | `1000` | Failures table cap (oldest evicted, counted) |
| `STATS_INTERVAL` | no | `5s` | Stream stats poll cadence |
| `PORT` | no | `8091` | HTTP port the UI listens on |

Fail-fast on all required vars; a missing mapping file or invalid JSON exits
non-zero at startup.

## Read-only and Ephemeral Guarantees

- Every store connection the tool opens is used **read-only**: `FindByID` /
  `FindOne` against Mongo, `SELECT` against Cassandra. It never writes to
  source or destination.
- The JetStream consumer on `MIGRATION-OPLOG-{siteID}` is an **ordered,
  ephemeral** consumer (`deliver-new` by default, or `deliver-by-start-time`
  for a bounded replay via `START_AT_TIME`) — no durable consumer state is
  created, so starting or stopping the tool never perturbs the transformer
  pipeline's own consumers or lag.
- Results live **in memory only**, capped and process-lifetime — nothing is
  persisted. Export failures to disk via `GET /failures.json` before
  shutting down if you need a record.
- Graceful shutdown (`pkg/shutdown.Wait`) drains the watcher and in-flight
  checks, drains the NATS connection, then disconnects Mongo/Cassandra.

Like `data-migration/`, this tool is deletable once the source is retired.
