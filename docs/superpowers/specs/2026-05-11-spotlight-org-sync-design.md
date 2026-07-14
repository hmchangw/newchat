# Spotlight Org Sync Design

**Status:** Draft
**Date:** 2026-05-11
**Branch:** `claude/spotlight-org-sync-design-fS2qh`

## 1. Problem

Today the org-shape data (sections, departments, divisions) used by the
spotlight-org Elasticsearch index has to be derived from MongoDB change
streams on the `users` and `hr_acct_org` collections. A new `hr-syncer`
service — a daily 8am cronjob that reads HR account data from a MinIO
file — will instead publish JetStream events directly, replacing the
change-stream pipeline.

This design adds a new collection inside `search-sync-worker` that
consumes those events and writes the spotlight-org index. The events
are batched arrays of employee changes, possibly compressed, possibly
carrying only the subset of fields that changed for an account. The
target index is keyed by `sectId` — many employees collapse to one ES
document.

## 2. Goals & Non-goals

**Goals**

- Add a `spotlight-org` collection to `search-sync-worker` that
  consumes `hr.sync.{siteID}.employees.upsert`.
- Maintain an ES index `spotlightorg-{siteID}` where `_id` is the
  `sectId` and the document carries the nine org fields listed below.
- Handle partial-field updates correctly: an event that carries only
  some org fields must NOT clobber other stored fields.
- Stay consistent with existing `search-sync-worker` patterns
  (`Collection` interface, bulk-flush handler, JetStream pull consumer,
  externally-owned streams).
- Reuse one project-wide `DEV_MODE` env var to flip every ES template
  in this worker to a single-shard / no-replica topology.

**Non-goals**

- Publisher side (`hr-syncer`). Mat owns that; this design only nails
  down the consumer contract and the on-wire envelope.
- Consuming `hr.sync.{siteID}.users.upsert`. That subject feeds a
  different consumer in a different service. The envelope type is
  shared so a future consumer can reuse it.
- Cross-batch strict LWW (painless `params.ts > stored` guards). Daily
  cron + JetStream retry semantics make doc-merge idempotency sufficient.
- Deleting sections. Subject is upsert-only; delete handling is
  deferred to a future `hr.sync.*.delete` subject if it becomes needed.

## 3. On-the-wire contract

### 3.1 Stream

`HR_SYNC_{siteID}` with subjects `hr.sync.{siteID}.>`. Owned by
`hr-syncer` (publisher) — `search-sync-worker` is a pure consumer and
must NOT bootstrap it, the same way it must not bootstrap `INBOX`.

The site-ID token is in the subject (`hr.sync.{siteID}.employees.upsert`)
so multi-site NATS clusters keep HR traffic per-site. Every other stream
in `pkg/stream/stream.go` follows this convention; we follow it here.

### 3.2 Envelope

A new type in `pkg/model/hrsync.go`:

```go
// HRSyncEvent is the envelope hr-syncer publishes on every hr.sync.*
// subject. Mirrors OutboxEvent: small fixed metadata + opaque payload
// bytes the consumer types per-subject.
type HRSyncEvent struct {
    Timestamp int64           `json:"timestamp"` // ms since epoch, set at publish site
    BatchID   string          `json:"batchId"`   // UUIDv7 for end-to-end trace of one cron run
    Gzip      bool            `json:"gzip"`      // true → Payload is gzip(JSON)
    Payload   json.RawMessage `json:"payload"`   // typed by subject at consume time
}
```

Why an envelope rather than raw `[]Employee`:

- Same envelope serves `users.upsert` and any future `hr.sync.*` subject
  — payload type varies per subject, picked at consume time.
- Carries the event-level timestamp used downstream as a tie-breaker if
  we ever add stricter LWW.
- Carries a `Gzip` flag so dev tooling can publish uncompressed without
  changing the consumer.

### 3.3 Inner payload

The `Payload` on `hr.sync.{siteID}.employees.upsert` is a JSON array
of HR account rows. The publisher (`hr-syncer`, owned by Mat) defines
its own internal `Employee` / `Org` types with the full HR field set;
those types live in the publisher's package and are not imported here.

The consumer does **not** declare a public `pkg/model.Employee`. Doing
so would conflict on merge with the internal repo's existing
`pkg/model/employee.go` (which already defines a fuller `Employee` and
`Org` for that repo's other consumers). Instead, `search-sync-worker`
defines a local projection — `SpotlightOrgIndex` in
`search-sync-worker/spotlight_org.go` — that carries only the nine org
fields it reads, with matching json tags so the wire format is
identical:

```
sectId, sectTCName, sectName, sectDescription,
deptId, deptTCName, deptName, deptDescription,
divisionId
```

All `string` with `omitempty`. Same struct serves three roles in the
consumer: wire-side row type to unmarshal into, ES doc projection on
write, and source of truth for the ES mapping via
`esPropertiesFromStruct[SpotlightOrgIndex]()`. One source of truth, no
copying between shapes.

Empty strings serialize to absent JSON — the consumer treats absent
keys as "this field did not change" for doc-merge upsert.

## 4. Worker-side design

### 4.1 New `spotlightOrgCollection`

File: `search-sync-worker/spotlight_org.go`. Implements the existing
`Collection` interface alongside `messageCollection`,
`spotlightCollection`, and `userRoomCollection`.

```go
func (c *spotlightOrgCollection) StreamConfig(siteID string) jetstream.StreamConfig {
    cfg := stream.HRSync(siteID)
    return jetstream.StreamConfig{Name: cfg.Name, Subjects: cfg.Subjects}
}

func (c *spotlightOrgCollection) ConsumerName() string {
    return "spotlight-org-sync"
}

func (c *spotlightOrgCollection) FilterSubjects(siteID string) []string {
    return []string{subject.HRSyncEmployeesUpsert(siteID)}
}
```

### 4.2 `BuildAction` flow

1. Unmarshal `data` into `model.HRSyncEvent`. Reject `Timestamp <= 0`
   with an error (NAK + redeliver).
2. If `envelope.Gzip`, decompress `envelope.Payload` via
   `compress/gzip`. Corrupt gzip → error.
3. Unmarshal the (possibly decompressed) bytes into
   `[]SpotlightOrgIndex` — the local projection type defined alongside
   the collection. Fields not in the projection are ignored by the
   decoder.
4. **Dedup by `SectID`.** Walk the slice and keep the last occurrence
   per `SectID` in a `map[string]SpotlightOrgIndex`. Rows with empty
   `SectID` are silently skipped (employees not yet assigned to a
   section). If the result is empty, return `(nil, nil)` and the
   handler will ack the JS message with no ES write.
5. For each unique `sectId`, build the ES `_update` body using the
   deduped row directly as `doc`:

   ```go
   body := map[string]any{
       "doc":           row, // SpotlightOrgIndex
       "doc_as_upsert": true,
   }
   ```

6. Emit one `searchengine.BulkAction` per unique `sectId`:

   ```go
   searchengine.BulkAction{
       Action: searchengine.ActionUpdate,
       Index:  c.indexName,
       DocID:  sectID, // overrides ES _id with sectId — the explicit requirement
       Doc:    bodyJSON,
       // No Version: ActionUpdate must NOT use external versioning.
       // handler.go::isBulkItemSuccess depends on this.
   }
   ```

### 4.3 Why doc-merge upsert (no painless script)

ES `_update` with `doc_as_upsert:true` overwrites only the keys present
in `doc`; absent keys preserve their stored value. Combined with
`omitempty` on the projection struct, an event carrying only
`{SectID, SectName}` produces a body containing only `sectId` and
`sectName`, leaving the other seven stored fields untouched.

Idempotency on JetStream redelivery is automatic: re-applying a
doc-merge with the same fields is a no-op at the application level,
even though ES may bump `_version`. The handler's existing 404/409
logic in `isBulkItemSuccess` covers `ActionUpdate` correctly.

Trade-off accepted: doc-merge cannot *clear* a field (overwrite "Eng"
back to ""). If HR sync ever needs to wipe a field, add an explicit
`clearFields []string` to the envelope or switch the projection to
`*string`. YAGNI until then.

### 4.4 Fan-out characteristics

A 10K-employee batch with ~500 unique sections produces 500 ES actions
— a fan-out collection where `ActionCount() > MessageCount()`. The
existing mid-batch flush in `main.go::runConsumer` handles this:
`fetchCount` is clamped to remaining bulk capacity, and a single big
event that pushes the buffer over `BULK_BATCH_SIZE` triggers an
immediate flush.

## 5. Index template

### 5.1 Document struct + mapping

```go
type SpotlightOrgIndex struct {
    SectID          string `json:"sectId,omitempty"          es:"search_as_you_type,custom_analyzer"`
    SectTCName      string `json:"sectTCName,omitempty"      es:"search_as_you_type,custom_analyzer"`
    SectName        string `json:"sectName,omitempty"        es:"search_as_you_type,custom_analyzer"`
    SectDescription string `json:"sectDescription,omitempty" es:"search_as_you_type,custom_analyzer"`
    DeptID          string `json:"deptId,omitempty"          es:"search_as_you_type,custom_analyzer"`
    DeptTCName      string `json:"deptTCName,omitempty"      es:"search_as_you_type,custom_analyzer"`
    DeptName        string `json:"deptName,omitempty"        es:"search_as_you_type,custom_analyzer"`
    DeptDescription string `json:"deptDescription,omitempty" es:"search_as_you_type,custom_analyzer"`
    DivisionID      string `json:"divisionId,omitempty"      es:"search_as_you_type,custom_analyzer"`
}
```

The same struct drives THREE roles in the consumer: unmarshal target
for the wire payload, document body on ES write (via `json.Marshal`),
and mapping source for the index template (via
`esPropertiesFromStruct[SpotlightOrgIndex]()`). One source of truth,
no copying between shapes.

### 5.2 Analysis (shared with `spotlight`)

The custom analyzer and tokenizer are shared with the existing
`spotlight` (room-typeahead) template, factored into a helper in
`template.go`:

```go
func customAnalyzerSettings() map[string]any {
    return map[string]any{
        "analyzer": map[string]any{
            "custom_analyzer": map[string]any{
                "type":      "custom",
                "tokenizer": "custom_tokenizer",
                "filter":    []string{"lowercase"},
            },
        },
        "tokenizer": map[string]any{
            "custom_tokenizer": map[string]any{
                "type":        "whitespace",
                "token_chars": []string{"letter", "digit", "punctuation", "symbol"},
            },
        },
    }
}
```

The existing `spotlight.go` is updated to call `customAnalyzerSettings()`
and gains the `token_chars` setting (verified to be accepted on
target ES 8.11). The now-stale comment in `spotlightTemplateBody`
about `token_chars` being rejected is removed.

### 5.3 Topology & dev mode

A shared helper toggles shard/replica counts:

```go
// indexTopology returns (shards, replicas) for an ES index template.
// In dev mode every template collapses to 1/0 regardless of prod
// values, so a single DEV_MODE toggle gives every index a fast local
// footprint without per-template env vars.
func indexTopology(prodShards, prodReplicas int, devMode bool) (int, int) {
    if devMode {
        return 1, 0
    }
    return prodShards, prodReplicas
}
```

Applied to all four templates:

| Template | Prod | Dev |
|---|---|---|
| `messageTemplateBody` | 4 shards / 2 replicas | 1 / 0 |
| `spotlightTemplateBody` | 3 / 1 | 1 / 0 |
| `spotlightOrgTemplateBody` | 3 / 1 | 1 / 0 |
| `userRoomTemplateBody` | 1 / 1 | 1 / 0 |

The `refresh_interval:30s` on the messages template stays put — it's a
write-perf knob, not a topology one.

## 6. Config & wiring

### 6.1 New env vars on `search-sync-worker/main.go::config`

```go
SpotlightOrgIndex string `env:"SPOTLIGHT_ORG_INDEX" envDefault:""`
DevMode           bool   `env:"DEV_MODE"            envDefault:"false"`
```

`DEV_MODE` reuses the project-wide env var already used by
`auth-service` and `chat-frontend` (the same variable, not a new one
scoped to this worker).

`SPOTLIGHT_ORG_INDEX` defaults to `spotlightorg-{siteID}` when empty,
mirroring how `SPOTLIGHT_INDEX` defaults.

### 6.2 Stream bootstrap skip

`main.go` currently skips `INBOX_{siteID}` when `BOOTSTRAP_STREAMS=true`
because `inbox-worker` owns it. Extend the skip list to also exclude
`HR_SYNC_{siteID}` (owned by `hr-syncer`):

```go
hrSyncName := stream.HRSync(cfg.SiteID).Name
inboxName  := stream.Inbox(cfg.SiteID).Name
// ...
if cfg.Bootstrap.Enabled && streamCfg.Name != inboxName && streamCfg.Name != hrSyncName {
    js.CreateOrUpdateStream(ctx, streamCfg)
}
```

### 6.3 Collection wiring

```go
collections := []Collection{
    newMessageCollection(cfg.MsgIndexPrefix, cfg.DevMode),
    newSpotlightCollection(cfg.SpotlightIndex, cfg.DevMode),
    newSpotlightOrgCollection(cfg.SpotlightOrgIndex, cfg.DevMode),
    newUserRoomCollection(cfg.UserRoomIndex, cfg.DevMode),
}
```

Every collection constructor accepts a `devMode bool`, stores it on the
struct, and threads it into `TemplateBody()` via the corresponding
`*TemplateBody(indexName, devMode bool)` function signature.

### 6.4 Consumer config

`buildConsumerConfig` is unchanged. `spotlightOrgCollection.FilterSubjects`
narrows the consumer to `hr.sync.{siteID}.employees.upsert`. The
existing 1s/5s/30s `BackOff` progression applies — a daily-cron source
benefits from progressive retries on transient ES failures, same as
the other collections.

## 7. Files touched / created

| File | Action | Purpose |
|---|---|---|
| `pkg/stream/stream.go` | Edit | Add `HRSync(siteID) Config` |
| `pkg/stream/stream_test.go` | Edit | Test for `HRSync` |
| `pkg/subject/subject.go` | Edit | Add `HRSyncEmployeesUpsert`, `HRSyncUsersUpsert` |
| `pkg/subject/subject_test.go` | Edit | Tests for new subject builders |
| `pkg/model/hrsync.go` | Create | `HRSyncEvent` envelope type |
| `pkg/model/model_test.go` | Edit | Roundtrip test for `HRSyncEvent` |
| `search-sync-worker/spotlight_org.go` | Create | New collection |
| `search-sync-worker/spotlight_org_test.go` | Create | Unit tests for new collection |
| `search-sync-worker/spotlight.go` | Edit | Thread `devMode`, use shared analyzer helper, add `token_chars` |
| `search-sync-worker/spotlight_test.go` | Edit | `devMode=true` subtest |
| `search-sync-worker/messages.go` | Edit | Thread `devMode` into template |
| `search-sync-worker/messages_test.go` | Edit | `devMode=true` subtest |
| `search-sync-worker/user_room.go` | Edit | Thread `devMode` into template |
| `search-sync-worker/user_room_test.go` | Edit | `devMode=true` subtest |
| `search-sync-worker/template.go` | Edit | Add `indexTopology` + `customAnalyzerSettings` helpers |
| `search-sync-worker/main.go` | Edit | New config fields, default index name, skip HR_SYNC bootstrap, wire collection |
| `search-sync-worker/integration_test.go` | Edit | Add `TestSearchSync_SpotlightOrg_Integration` |
| `search-sync-worker/deploy/docker-compose.yml` | Edit | `DEV_MODE=${DEV_MODE:-true}`, optional `SPOTLIGHT_ORG_INDEX` |
| `search-sync-worker/consumer_config_test.go` | Possibly edit | Cover new collection if it asserts by-name |

## 8. Testing

### 8.1 Unit tests (`spotlight_org_test.go`)

Table-driven, mocked store via existing `mock_store_test.go`:

| Test | Scenario |
|---|---|
| `BuildAction_HappyPath` | Valid envelope, gzip=false, 3 employees / 2 unique sectIds → 2 actions; last-wins on dup sectId verified by marshaled doc. |
| `BuildAction_Gzip` | Same with `Gzip=true` and a gzipped payload; assert decompress + parse path. |
| `BuildAction_PartialFields` | Employee with only `SectID + SectName` → resulting update body's `doc` contains exactly those keys. No empty-string keys for the other seven. |
| `BuildAction_EmptySectID` | Mix of employees, some empty `SectID` → empty ones skipped, non-empty emitted. No error. |
| `BuildAction_AllEmptySectIDs` | All empty `SectID` → returns `(nil, nil)`. Handler acks JS message with no ES write. |
| `BuildAction_DocAsUpsertSet` | Bulk body contains `"doc_as_upsert":true`. |
| `BuildAction_DocIDIsSectID` | `BulkAction.DocID == employee.SectID`. |
| `BuildAction_NoVersionOnUpdate` | `BulkAction.Version == 0` (handler 409 logic depends on this). |
| `BuildAction_InvalidEnvelope` | Malformed JSON, zero timestamp, corrupt gzip → error, NAK + redeliver. |
| `BuildAction_EmptyEmployees` | `payload=[]` → returns `(nil, nil)`. |
| `SpotlightOrgTemplateBody_Prod` | `devMode=false` → shards=3, replicas=1, all nine fields present as `search_as_you_type,custom_analyzer`. |
| `SpotlightOrgTemplateBody_Dev` | `devMode=true` → shards=1, replicas=0. |

### 8.2 Updates to existing unit tests

- `messages_test.go`, `spotlight_test.go`, `user_room_test.go`: add a
  `devMode=true` subtest to each `*TemplateBody` test asserting 1/0
  topology. Existing assertions remain valid for the default
  `devMode=false` path.
- `mock_store_test.go`: untouched (the `Store` interface didn't
  change).

### 8.3 Integration test

In `integration_test.go` (build tag `//go:build integration`), reuse
the existing `nats` + `searchengine` testcontainers:

`TestSearchSync_SpotlightOrg_Integration`:
1. Create the `HR_SYNC_{siteID}` stream via `js.CreateOrUpdateStream`
   (the test owns it; `hr-syncer` isn't running).
2. Publish a real gzipped `HRSyncEvent` with three employees across
   two `sectId`s.
3. Run the worker briefly with `DEV_MODE=true` and
   `BOOTSTRAP_STREAMS=true`.
4. Assert:
   - ES index `spotlightorg-{siteID}` exists with the expected mapping
     (read via `_index_template/spotlight_org_template`).
   - Two docs exist, keyed by `sectId`.
   - A second envelope carrying only `{SectID, SectName}` for one of
     the sectIds preserves the other stored fields (doc-merge worked).

### 8.4 Coverage

Per `CLAUDE.md` Section 4: ≥80% required, ≥90% target for new core
code. The matrix above covers happy path, idempotency, fan-out,
partial fields, dedup, malformed input, and template shape.

## 9. Local dev

`search-sync-worker/deploy/docker-compose.yml`:

```yaml
environment:
  DEV_MODE: ${DEV_MODE:-true}
  BOOTSTRAP_STREAMS: "true"
  # SPOTLIGHT_ORG_INDEX defaults to spotlightorg-{siteID} when unset.
```

The `HR_SYNC_{siteID}` stream itself is owned by `hr-syncer`'s
compose. To exercise the new collection end-to-end locally without
running `hr-syncer`, the integration test path (step 8.3) demonstrates
how to publish a synthetic event.

## 10. Open questions / future work

- **Painless LWW guard.** Current design relies on doc-merge
  idempotency + single-publisher cron. If multiple HR sync sources are
  ever introduced, add a per-field timestamp guard mirroring
  `user-room`'s `roomTimestamps` flattened map.
- **Field-clearing.** Doc-merge can't clear a stored field. If HR data
  ever requires explicit clears, add a `clearFields []string` to
  `HRSyncEvent` or switch the projection to `*string`.
- **Section deletes.** Subject is upsert-only today. A
  `hr.sync.{siteID}.employees.delete` subject would be a separate
  filter on the same consumer with a `delete` branch in `BuildAction`.
- **Users collection.** `hr.sync.{siteID}.users.upsert` is out of
  scope here. The shared `HRSyncEvent` envelope is intentional so a
  future consumer (different service, different MongoDB collection)
  can reuse it.
