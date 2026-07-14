# oplog-direct-transfer — verbatim CDC copy of opaque collections (Design)

> **Status:** DESIGN — a sibling to the `data-migration` CDC suite (`oplog-connector`, `oplog-transformer`, `oplog-collections-transformer`). Branch: `claude/oplog-collections-directwrite`, based on the (unmerged) `oplog-transformer-collections` branch so it builds on the shared CDC infrastructure.

A new service that **verbatim-copies** a configured set of source (legacy RocketChat) Mongo collections into the same-named collections in the new-stack **per-site Mongo**, keyed by the source `_id`, mirroring every op (insert / replace / update / delete). No field mapping, no classification, no inbox publish — the opposite end of the complexity spectrum from `oplog-collections-transformer`.

---

## 0. Where this sits

```text
 (per site) source Mongo ──change stream──▶ oplog-connector ──▶ MIGRATION_OPLOG_{site}
            startAfter(checkpoint): live CDC tail          │  chat.migration.oplog.{site}.{coll}.{op}
                                                            │  (subjects for the direct-transfer collections)
                                                            ▼
                                        oplog-direct-transfer ──upsert/delete by _id──▶ target per-site Mongo
                                                                                        (chat DB, SAME collection names)
```

**Scope boundary — live CDC tail only.** Like the sibling transformers, a separate owner bulk-migrates all state ≤ checkpoint and hands off the resume point; this service applies only the tailed changes at and after the checkpoint. It does not snapshot or backfill.

**Reuses the shared connector.** The `oplog-connector` already tails every collection listed in `WATCH_COLLECTIONS` (one watcher + one checkpoint per collection) and publishes raw `model.OplogEvent`s to `MIGRATION_OPLOG_{site}`. We add the direct-transfer collections to `WATCH_COLLECTIONS`; this service is a **new durable consumer** filtered to their subjects.

**Deployment is per-site**, mirroring the suite: each site runs its own connector + this service against that site's source and target Mongo.

---

## 1. Scope

### 1.1 Collections (direct transfer)
Source → destination is **1:1, verbatim, same collection name**, into the new-stack per-site Mongo (`chat` DB):

`rocketchat_avatar`, `company_apps_v`, `company_bot_cmd_men`, `company_tsso_tokens`, `rocketchat_uploads`, `company_bot_authorization`, `ufsTokens`, `user_devices`

The set is **config-driven** (`DIRECT_COLLECTIONS`), so adding/removing a collection is an env + `WATCH_COLLECTIONS` change, not a code change. The default is the eight above.

### 1.2 Out of scope
- **File/blob bytes.** `rocketchat_uploads`, `ufsTokens`, `rocketchat_avatar` are the RocketChat file-store (UFS) **metadata** collections; the actual bytes live in GridFS chunks or external storage. Only the Mongo metadata docs are copied here — blob content is a separate owner's concern. **Documented gap.**
- Any transformation, dedup, or cross-collection resolution.

---

## 2. Data model — opaque verbatim docs

Docs are treated as **opaque** `bson.D` — no typed `pkg/model` struct. The connector encodes each event's `fullDocument` as type-preserving extended JSON, so decoding via `bson.UnmarshalExtJSON` into a `bson.D` and writing it back preserves BSON types (dates, ObjectIds, nested docs) and the original `_id`. This is what makes the copy verbatim.

Because the destination adopts the **source `_id`**, every op is directly addressable by `_id` — unlike `oplog-collections-transformer`, whose destinations re-key (which is why *it* can't action deletes and *this* service can).

---

## 3. Per-collection mapping — CDC op handling

| Op | Connector payload | Action |
|---|---|---|
| `insert` / `replace` | full `fullDocument` | upsert verbatim by `_id` (`ReplaceOne` with upsert) |
| `update` | `updateDescription` only (no post-image) | **re-read** the full current source doc by `_id` (`pkg/migration.SourceLookup`), then upsert; doc vanished between event and re-read → skip |
| `delete` | `documentKey._id` only | **delete by `_id`** (idempotent; missing row = no-op) |

- One `SourceLookup` per watched collection (same construction as `oplog-collections-transformer`).
- **Degraded insert/replace recovery:** when the connector couldn't encode `fullDocument` (empty + `degraded=true`), the handler re-reads the live source doc by `_id` (same path as `update`) rather than poisoning it — mirrors `oplog-collections-transformer`. A non-degraded empty `fullDocument` is a contract violation → poison.
- **Non-string `_id` on the re-read path:** `SourceLookup.FindByID` keys by a **string** `_id`. `update` (and degraded recovery) therefore require a string `_id`; a non-string `_id` (ObjectID/int) is **poisoned loudly (Term + log)**, never silently stringified-and-mis-skipped — surfacing a non-string-keyed collection (still an open confirmation, §12) instead of dropping the change. `insert`/`replace`/`delete` handle any `_id` type natively (no string lookup). Follow-up if a non-string-keyed collection ever needs updates: widen `SourceLookup` to accept `any`.
- Collection-level ops (`drop` / `rename` / `dropDatabase` / `invalidate`) — **out of scope, deferred** (operator re-points the connector), consistent with the suite; metered as `unknown_op`.

---

## 4. Ordering & idempotency

- **Idempotent by construction:** upsert-by-`_id` and delete-by-`_id` both tolerate JetStream redelivery and reprocessing across the checkpoint boundary.
- **In-order within a collection:** the connector's per-collection watcher preserves source order, and this service consumes sequentially (`cons.Consume`), so `insert → update → delete` for one doc apply in order.
- **Last-write-wins, no high-water guard.** These are low-stakes operational collections and most lack a uniform monotonic field, so no per-doc timestamp guard is applied. **Known caveat:** a JetStream *redelivery* of a stale `insert` after a newer `update` could momentarily regress a doc; the next real event for that `_id` re-syncs it. Accepted for this data.
  - *Rejected alternative:* re-read the current source doc on **every** non-delete op (fully convergent, order-independent) — cleaner but adds one source read per event. Not chosen; revisit only if a collection turns out to need strict convergence.

---

## 5. Disposition, observability, security

Mirror the sibling transformers' conventions via `pkg/migration`:
- **Disposition:** decode / contract violation (bad `documentKey`, undecodable doc) → **Term** (poison); source/target Mongo unavailable → **Nak** (transient); `MAX_DELIVER` exhaustion → **Term + metric** (no silent JetStream drop); a delete of an absent row or an update whose doc vanished → Ack (skip + metric).
- **Correlation:** stamp `request_id` at consume; propagate via `context.Context` into every log line.
- **Metrics (Prometheus):** processed / nak / term / skipped / exhausted, labelled by `op` + `collection`; plus upsert-vs-delete counts.
- **Security / logging (important):** `company_tsso_tokens`, `ufsTokens`, `company_bot_authorization`, and `user_devices` carry tokens / credentials / device secrets. The handler logs **only** `_id`, `collection`, and `op` — **never document contents**. No token or full body reaches a log line or an error `cause` (CLAUDE.md secret-logging rule; keeps `gosec`/`semgrep` clean).

---

## 6. Configuration (env, `caarlos0/env`)

Mirrors `oplog-collections-transformer`:

| Var | Notes |
|---|---|
| `SITE_ID` | required; site code |
| `DIRECT_COLLECTIONS` | comma-separated; default = the 8 collections |
| `NATS_URL` | required |
| `SOURCE_MONGO_URI` / `SOURCE_DB` / `SOURCE_MONGO_USERNAME` / `SOURCE_MONGO_PASSWORD` | source read (update re-read); `SOURCE_DB` default `rocketchat` |
| `TARGET_MONGO_URI` / `TARGET_DB` / creds | destination write; `TARGET_DB` default `chat` |
| `SOURCE_READ_PREFERENCE` | default `primaryPreferred` |
| `CONSUMER_DURABLE` | default `oplog-direct-transfer` |
| `MAX_DELIVER` | delivery cap before Term-exhausted |
| `BOOTSTRAP_STREAMS` | default `false`; this service owns no streams (waits for the connector to create `MIGRATION_OPLOG` via `createConsumerWithRetry`) |
| `METRICS_ADDR` | default `:9090` |
| `LOG_LEVEL` | default `info` |

Required scalars are trimmed and validated non-empty at startup (fail fast).

---

## 7. Service structure

New flat service `data-migration/oplog-direct-transfer/`, following the per-service file organization:
- `main.go` — config parse, Mongo/NATS wiring, per-collection `SourceLookup` map, consumer creation-with-retry, graceful shutdown.
- `config.go` — typed `Config` + validation.
- `handler.go` — decode `oplogEvent`; dispatch by op → upsert / re-read+upsert / delete.
- `store_mongo.go` — target store: `UpsertByID(coll, id, doc)` and `DeleteByID(coll, id)`.
- `metrics.go` — Prometheus counters (nil-safe).
- `handler_test.go`, `store_*_test.go`, `integration_test.go` (testcontainers Mongo + NATS), `config_test.go`.
- `deploy/` — `Dockerfile`, `docker-compose.yml`, `azure-pipelines.yml`.

The `oplog-connector` `deploy/` `WATCH_COLLECTIONS` (compose + pipeline env) is extended with the 8 collections so the connector tails them.

**Rejected:** extending `oplog-collections-transformer` — it would tangle a complex map + inbox-publish path with a dumb verbatim copy in one binary/config, against the suite's §10 separation rationale.

---

## 8. Testing

- **Unit** (mocked source lookup + target store): op→action mapping (insert/replace upsert; update re-read+upsert; update-doc-gone skip; delete delete-by-id; bad documentKey Term); disposition mapping; verbatim fidelity (extended-JSON round-trip preserves types); no doc contents in logs.
- **Integration** (testcontainers Mongo + NATS): source CDC event → `MIGRATION_OPLOG` → this service → target Mongo, for insert, update, and delete of a sample doc across ≥2 collections; idempotent redelivery leaves one doc.
- **Coverage:** ≥80% per CLAUDE.md; cover error/skip paths.

---

## 9. Footprint / teardown

Same blast-radius discipline as the rest of `data-migration/`: the whole folder is deletable at source retirement. This service owns no streams and no new collections (it writes existing/legacy-named collections in the target DB), so teardown is: stop the service, drop the direct-transfer collections from `WATCH_COLLECTIONS`, delete the folder.

---

## 10. Docs to update (same PR)

- `data-migration/CDC_COVERAGE.md` — add a **direct-transfer** section listing the 8 collections and the insert/update/replace/**delete** verbatim coverage (delete *is* actionable here).
- `data-migration/SOURCE_DATA.md` — note that the "Collection for direct transfer" list is copied verbatim, metadata-only, with the blob-bytes out-of-scope caveat.
- No `docs/client-api.md` change — no client-facing (`chat.user.*`) handler is touched.

## 11. Indexing (design position)

This service creates **no** indexes on the destination collections. Docs are keyed by `_id` (auto-indexed), which is all the verbatim upsert/delete needs. Any secondary/query indexes are the consuming app's or ops' concern, not the migration's.

**No TTL index on the destination.** Some source collections (`*tokens`, `user_devices`) likely carry a TTL index that expires docs. We deliberately do **not** replicate it: a source TTL expiry fires a `delete` change event, which this service mirrors — so the destination tracks the source lifecycle via CDC. Adding an independent destination TTL index would risk the destination expiring docs on its own clock and diverging from source. Removal is CDC-driven, full stop.

## 12. Open confirmations (source engineers)

- Do all 8 collections carry a stable `_id` safe to adopt as the destination `_id`? (Assumed yes — RocketChat/Company collections are `_id`-keyed.)
- Confirm each source collection has TTL/expiry handled by a `delete` change event (so mirrored deletes suffice) rather than any out-of-band expiry the change stream wouldn't surface.
- Confirm the file-store split: metadata in these collections, bytes elsewhere (out of scope).
