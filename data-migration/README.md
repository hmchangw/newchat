# data-migration

Migration tooling for moving off the legacy ("source") RocketChat-style MongoDB
onto the distributed chat stack. This folder groups the migration components so
they share ownership seams and a single blast radius — **the whole folder is
deletable once the source is retired.** Each component is a standard flat
`package main` service; shared code lives in the root `pkg/migration/` (there is
no nested `pkg/`/`internal/` here).

```text
                    consistent cut (clusterTime T, resume token R)
                                 │
  source Mongo ─────────────────┼──────────────────────────────▶ time
                                │
   history-migrator   ◀── bulk copy of state ≤ T ──┐ captures R
   (sibling, separate owner)                       │
                                                    ▼
   oplog-connector    seed = R ──▶ startAfter(R) ──▶ live CDC ─▶ MIGRATION_OPLOG_{site}
   (this folder)                                                       │
                                                                       ▼
   oplog-transformer  ── consumes, models per collection ─▶ new stack DBs
   (sibling, separate owner)
```

## Components

| Component | Status | Role |
|-----------|--------|------|
| **oplog-connector** | implemented | "Dumb pump": tails source change streams and publishes raw, opaque CDC events to `MIGRATION_OPLOG_{site}`. Lossless, ordered per collection, resumable. |
| history-migrator | not started | Bulk DB→DB copy of state ≤ the consistent cut; captures the resume token the connector seeds from. |
| **oplog-transformer** | implemented (messages) | Consumes `MIGRATION_OPLOG_{site}`, formats migrated **messages**, and re-injects them into the new-stack pipeline. Other collections deferred. |

See `docs/superpowers/specs/2026-06-08-oplog-connector-design.md` + `…2026-06-15-oplog-transformer-design.md` (designs)
and the matching `docs/superpowers/plans/2026-06-11-…` + `…2026-06-15-oplog-transformer-implementation.md` (plans).

## oplog-transformer at a glance

- **Scope:** the `rocketchat_message` path only (message text + core fields). Reactions,
  attachments, pins, system messages, and all non-message collections are deferred.
- **Insert** → maps the full doc → `model.MessageEvent{created}` → **confirmed** publish to
  `chat.msg.canonical.{site}.created` (message-worker persists, search-sync indexes). A **thread
  reply** (`tmid` set) resolves the parent's `createdAt` from the source so message-worker can link
  the reply to its thread (best-effort: a missing/corrupt parent publishes without the link).
- **Correlation** → a `request_id` is stamped at consume time and propagates via `ctx` into the
  history RPC and the canonical publish, so the transformer→history→worker hop shares one id.
- **Update / replace** → resolves the full doc (source lookup for `update`, event-doc for `replace`),
  classifies **message vs system message** (`t==null` migrate, `t=="rm"` delete, any other `t` skip)
  and **edit vs soft-delete**, then **sync request/reply** to `chat.migration.internal.{site}.msg.{edit,delete}`
  → history-service applies the Cassandra edit/soft-delete (**retry-until-present, not upsert**) + best-effort republishes canonical.
- **Delete** → both a hard `op=delete` and a soft-delete (`t:"rm"`) collapse to **delete-by-id**: history-service
  resolves `roomId`/`createdAt` from the message id via `GetMessageByID`. No source doc needed.
- **Federation-origin filter** → only locally-authored messages are migrated. The connector's `$match` drops
  foreign insert/replace at the source; the transformer skips foreign **updates** (`federation.origin` set) via
  the source lookup it already does. Foreign copies arrive via the new app's own cross-site federation, not the pump.
- **Degraded events** → the connector may publish an event with `Degraded=true` (a field that wouldn't encode is omitted,
  never dropped). The transformer **recovers** these (source-lookup) rather than dropping — never silent loss.
- **History errors are classified** → `NotFound`/infra → retry (Nak); a non-retryable category → `Term`+metric (no silent loop).
- **MaxDeliver exhaustion is visible** → an event that Naks all the way to the redelivery cap is **Term'd** with
  `oplog_transformer_exhausted_total` + an ERROR log, instead of being silently dropped by JetStream. Hard
  **deletes** use a shorter cap (`DELETE_MAX_DELIVER`) — a foreign hard-delete has no doc to filter on, so it
  Terms in ~minutes instead of churning history RPC + Cassandra reads to the global `MAX_DELIVER`.
- **`X-Migration: live` header** on every re-injected event — `broadcast-worker` and
  `notification-worker` skip it so historical messages are **not** re-delivered/notified to users.
- **Single active replica** (sequential durable consumer) — ordering depends on it; do **not** scale horizontally.

> **Ops — required:**
> - The `chat.migration.internal.{site}.msg.*` subjects bypass the user-facing edit/delete auth.
>   They **MUST** be restricted to **server identities** in the NATS account permissions (no client publish).
> - Set `SOURCE_READ_PREFERENCE=primaryPreferred` (default) — the update lookup must not read a lagging secondary.
>
> **Accepted limitation:** thread reply counts and mention/unread aggregates are **not** maintained for
> migrated messages and are **not** recomputed at cutover. Migrated threads/rooms may show stale/zero counts.
>
> **Cleanup PR (at source retirement):** delete `data-migration/` and remove the broadcast/notification
> `X-Migration` skips + the history-service migration handlers.

## oplog-connector at a glance

- **Watched collections (8):** `rocketchat_message`, `rocketchat_room`,
  `rocketchat_subscription`, `rocketchat_uploads`, `company_room_members`,
  `company_thread_subscriptions`, `company_hr_acct_org`, `users`. All op types
  (insert/update/replace/delete) are traced for every collection, identically.
- **No lookups.** The connector forwards native oplog content only — `fullDocument`
  for insert/replace, `updateDescription` (the delta) for update, `documentKey` for
  delete. No `updateLookup`, no pre-images. All enrichment is the transformer's job.
- **Federation-origin filter (`$match`).** On the **`MESSAGE_COLLECTION`** watcher only, a change-stream
  `$match` drops `insert`/`replace` events whose `fullDocument.federation.origin` is set — only
  locally-authored messages are pumped. It's a subscription filter, not a lookup (insert/replace carry
  `fullDocument` natively); `update`/`delete` carry no doc, so foreign updates are filtered downstream
  by the transformer. Other watched collections are forwarded unfiltered.
  - **Known limitation — checkpoint stall under `$match` (accepted, replay-not-loss):** because the
    `$match` surfaces only matching events, a long foreign-only burst delivers nothing on the message
    watcher, so its resume token doesn't advance and the checkpoint stalls. On restart the connector
    replays from an older token — **deduped, so no loss**, just rescan amplification (worst case ≈ the
    delete-cap reseed window). It's **scoped to the one message collection** (other watchers advance
    normally), and is a **connector-internal** concern; advancing from the post-batch resume token (PBRT)
    is a future connector change, not part of the federation-filter work.
- **Subjects:** `chat.migration.oplog.{siteID}.{rawCollection}.{op}`; dedup via
  `Nats-Msg-Id` = change-stream `_id._data`.
- **Checkpoints:** one doc per collection in `oplog_checkpoints` (in `CHECKPOINT_DB`
  on the source RS). The resume token is the real checkpoint; saved only after a
  pub-ack (lossless).
- **Observability:** `/healthz` on `HEALTH_ADDR` for k8s probes; metrics are
  exported by the o11y SDK on its own Prometheus endpoint —
  `oplog_events_published_total`, `oplog_publish_errors_total`,
  `oplog_events_skipped_total`, `oplog_replication_lag_ms` (all by `collection`).
  For a single-replica pump, **alert on lag and sustained publish errors** — that's
  how a soft stall (retry-forever) is caught before the oplog window closes.

### Configuration (the interface)

All configuration is via environment variables. Required vars have no default and
fail-fast at startup.

| Env | Req | Default | Purpose |
|-----|-----|---------|---------|
| `SITE_ID` | ✓ | — | site scope for subjects, stream name, checkpoint `_id` |
| `SOURCE_MONGO_URI` | ✓ | — | source replica set (change streams + checkpoint writes). **Keep credentials out of the URI** — use the username/password vars below. |
| `SOURCE_MONGO_USERNAME` / `SOURCE_MONGO_PASSWORD` | | `""` | source auth (preferred over creds-in-URI) |
| `SOURCE_DB` | | `rocketchat` | DB the watched collections live in |
| `CHECKPOINT_DB` | | `migration` | DB holding `oplog_checkpoints` (on the source RS) |
| `NATS_URL` | ✓ | — | publish target |
| `NATS_CREDS_FILE` | | `""` | NATS credentials file |
| `WATCH_COLLECTIONS` | ✓ | — | comma-list of raw collections to tail (one watcher each; **no duplicates**) |
| `MESSAGE_COLLECTION` | | `rocketchat_message` | the one watched collection the federation-origin `$match` is scoped to; others are forwarded unfiltered |
| `READ_PREFERENCE` | | `secondary` | source read pref (`primary`\|`primaryPreferred`\|`secondary`\|`secondaryPreferred`\|`nearest`) |
| `CHECKPOINT_EVERY` | | `100` | persist the resume token every N acked events |
| `CHECKPOINT_MAX_AGE` | | `30` | also persist it at least every N seconds (bounds replay for low-volume collections) |
| `START_MODE` | | `now` | cold-start when no checkpoint exists: `now`\|`time` |
| `START_AT_TIME` | | `""` | RFC3339 or unix-ms; used by `START_MODE=time` **and** as an override (see below) |
| `START_RESUME_TOKEN` | | `""` | `_data` hex; one-off seed override (see below) |
| `BOOTSTRAP_STREAMS` | | `false` | dev-only stream creation; **keep `false` in prod** (ops/IaC owns the stream) |
| `HEALTH_ADDR` | | `:9090` | bind addr for the `/healthz` probe listener (metrics are on the o11y SDK's own endpoint) |

**Where the change stream starts (per collection, first match wins):**

1. **Env override** — `START_RESUME_TOKEN` (→ `startAfter`) or `START_AT_TIME`
   (→ `startAtOperationTime`). Forces a reseed; **ignores the stored checkpoint.**
2. **Stored checkpoint** — `startAfter(resumeToken)` (the normal restart path).
3. **Cold start** — `START_MODE`: `now` (default) \| `time`.

### Source-side prerequisites (ops)

1. **Replica set.** Change streams require the source Mongo to be a replica set.
2. **Network egress** from the connector to the source RS (read change streams +
   write checkpoints).
3. **Seed the handoff.** Before first start, pre-insert one seed checkpoint per
   collection (`Source:"seed"`, `ResumeToken:R`) — **preferred** — so live sync
   begins exactly after the migrated cut.

No `changeStreamPreAndPostImages` / pre-image setup is required — the connector
does no lookups.

> ⚠️ **Do not leave `START_RESUME_TOKEN` / `START_AT_TIME` set in the
> environment.** They are one-off overrides that *ignore the stored checkpoint
> and reseed on every restart*. Use them for a manual one-off only, then unset;
> seed via the pre-inserted checkpoint doc instead. The connector logs a `WARN`
> at startup when either is set.
>
> ⚠️ **Do not embed credentials in `SOURCE_MONGO_URI`** — connection strings
> leak through process listings and driver error output. Use
> `SOURCE_MONGO_USERNAME` / `SOURCE_MONGO_PASSWORD` instead.

### Implementation note — synchronous publish

The repo's `oteljetstream` wrapper intentionally does not expose async publish.
The connector therefore uses **one synchronous-publish goroutine per
collection** (`Next → PublishMsg (blocks on pub-ack) → checkpoint`) rather than
the async pipeline + in-flight window in the original design. This preserves the
same guarantees — per-collection order, at-least-once, checkpoint-only-after-ack
— at the cost of per-collection pipelining throughput, which is acceptable for
live-sync volumes. Per-collection parallelism is retained (one goroutine each).

### Local dev

```text
make up SERVICE=data-migration/oplog-connector
```

Stands up a single-node replica set, JetStream NATS, and
the connector with `BOOTSTRAP_STREAMS=true`.

### Tests

```text
make test SERVICE=data-migration/oplog-connector              # unit (race)
make test-integration SERVICE=data-migration/oplog-connector  # store + live CDC e2e (Docker)
```
