# Search: delete/edit correctness + tcard & file (attachment) search — Design

## Problem

1. Messages soft-deleted via `history-service` (hidden from history RPCs) still appear in
   search results.
2. Tcard data (`Card`/`CardAction` on messages) and file attachment data (`Title` = filename,
   `Description`) are not searchable — only `content` is indexed in Elasticsearch.

## Findings

- The forward sync path already exists at HEAD: `history-service` publishes
  `chat.msg.canonical.{siteID}.deleted` / `.updated`, and `search-sync-worker` maps
  `EventDeleted` → ES delete, `EventUpdated` → full-doc re-index
  (`search-sync-worker/messages.go`). Lingering search hits therefore come from
  (a) messages deleted before this pipeline shipped/deployed, and/or (b) the best-effort
  canonical publish being dropped (`publishCanonicalBestEffort` logs and swallows failures).
- Live bug: `EventPinned`/`EventUnpinned` canonical events carry no `content`
  (`history-service/internal/service/pin.go`) yet take the full-doc-replace upsert path in
  the sync worker — pinning a message wipes its content from the search index, and
  unpinning a soft-deleted pin resurrects a stub document.

## Design

### Sync worker (`search-sync-worker`)

- **Event allowlist** in `BuildAction`: upsert on `EventCreated` / `EventUpdated` / legacy
  unstamped `""`; delete on `EventDeleted`; skip everything else (`Reacted`, `Pinned`,
  `Unpinned`, `ThreadReplyAdded`, future types). Fixes the pin wipe/resurrect bug and is
  future-proof against new slim event types.
- **Ordering invariant (external versioning)**: every index/delete action carries the
  event's timestamp as the ES external version. An edit is a full-document replace at a
  higher version; a delete leaves a tombstone at `deletedAt`, so a stale redelivered
  create/update (older version) is rejected with 409 — which the flush path treats as
  success. Delete protection holds as long as the tombstone's version metadata lives:
  ES garbage-collects it after `index.gc_deletes` (default 60s), after which a late
  redelivered create would be accepted again. Accepted for now (owner decision,
  2026-07-23 — no template change ships); if the window ever proves material, ops
  can raise `index.gc_deletes` dynamically to exceed JetStream redelivery/backoff.
  Known limitation: two distinct events stamped in the same millisecond tie on version,
  so the second is 409-dropped (e.g. an edit in the same ms as the create keeps the old
  content). Accepted — publishes are sequential per message in `history-service`, so
  the window is sub-millisecond in practice, and the next event heals the doc; a
  timestamp+sequence version scheme was considered and rejected as producer-side
  churn disproportionate to the sub-ms risk.
- **New ES fields** on `MessageSearchIndex`:

  | Field | JSON | ES type |
  |---|---|---|
  | `AttachmentText string` | `attachmentText` | `text,custom_analyzer` (searched — every attachment's title + description joined into one string, so AND terms may mix both) |
  | `CardData string` | `cardData` | `text,custom_analyzer` (searched, never returned) |
  | `Attachments []cassandra.Attachment` | `attachments` | `object`, `enabled:false` (render-only) |
  | `Card *cassandra.Card` | `card` | `object`, `enabled:false` (render-only) |

  Render payloads (owner decision, 2026-07-22): the whole `Attachment` objects and the
  `card` (template + data) are stored as-is — `enabled:false`, so ES keeps them in
  `_source` without indexing — and returned on hits so the frontend can render results
  (file row, tcard) without a history-service load. Search still touches only the flat
  analyzed projections above. Accepted trade-off: a carded doc stores the card body
  twice (`cardData` raw text + `card.data` base64) — full-text search needs the
  analyzed copy, lossless render needs the object; hits ship only the latter.
- Attachments decoded from the event's raw `[][]byte` blobs via the existing lenient
  `cassandra.DecodeAttachments`.
- **Create-path card gap (known)**: no in-repo `.created` producer sets `Message.Card` —
  `SendMessageRequest` has no card field and the oplog transformer maps neither card
  field, so live tcard messages become card-searchable only via the enriched `.updated`
  path (edit/migration replay). Indexing on `.created` is forward-compatible with
  external producers whose events carry `card`; wiring cards into the send path is a
  product decision deferred out of this branch.
- **Card data indexing** (owner decision, 2026-07-22, replaces the earlier allowlist
  extraction): `Card.Data` is indexed **verbatim** as analyzed text (`cardData`);
  `CardAction` is not indexed. Accepted trade-off: structural JSON tokens (keys,
  type names) become searchable noise — accepted for simplicity. A template-expansion
  "rendered display text" approach was fully designed and then explicitly rejected;
  its spec/plan documents were removed from the branch.
- **Mapping rollout**: index templates only apply to future monthly indices, so worker
  startup additionally PUTs the additive mapping onto existing `messages-*` indices
  (`allow_no_indices=true&ignore_unavailable=true`). Components: a new
  `pkg/searchengine.UpdateMapping(pattern, properties)` on the shared ES adapter (sole
  addition to that pre-existing package) and a `Collection.MappingUpdate()` hook that
  only the rolling-index messages collection implements. All changes are additive — no
  index version bump or reindex. Docs indexed before this change lack the new fields
  until the message is edited or a backfill replay occurs.
  Ops note: the mapping push fails startup (exit 1 → crash-loop) if a pushed field
  conflicts with an existing index's mapping. That is intentional fail-fast: only
  additive changes are allowed on this path; a type change requires an index version
  bump (new indices + reindex), never an in-place mapping fight.

### Event source (`history-service`)

- The `.updated` canonical event additionally carries `Attachments` and `Card`
  (`CardAction` deliberately excluded — no `.updated` consumer reads it and its `Data`
  blob would inflate every edit event)
  (the handler already holds the full row), so the full-doc re-index cannot wipe the new
  fields. Applies to both the client edit path and the migration edit path — the latter
  now resolves the full row first (like the migration delete path), which also fixes
  migrated edits erasing `userAccount`/`userId` from the index. Delete events are
  unchanged (the document is removed).

### Query + response (`search-service`)

- `multi_match` fields become `["content", "attachmentText", "cardData"]` (same
  `bool_prefix` + `AND` semantics; pooling titles+descriptions into one field lets an
  AND query mix words from both — owner decision, 2026-07-23).
- `SearchMessage` gains `attachments []Attachment` and `card *Card` (owner decision,
  2026-07-22, superseding the earlier flat-projection wire shape): the render payloads
  mirrored as-is from the ES doc — same wire shape as history reads (`card.data` is
  base64 bytes) — so the frontend renders hits without a follow-up history load. Both
  `omitempty`; documented in `docs/client-api.md` and derived views. `cardData` stays
  search-only (`_source`-excluded); the returned card carries the data instead.

### Stale deleted docs (no reconciliation — decision)

- **No reconcile tooling ships** (owner decision, 2026-07-22, reversing the earlier
  reconcile-mode design): documents deleted before this pipeline existed are out of
  scope, and once a `.deleted` event is in MESSAGES_CANONICAL, the worker's Nak/retry
  makes the ES delete at-least-once. The accepted residual gap is a publish-side drop:
  the Cassandra soft-delete commits before `publishCanonicalBestEffort`, so a JetStream
  publish failure (stream ack timeout / no quorum / limits, while request/reply still
  works) leaves the doc searchable with no retry path. Judged rare enough to accept;
  the fallback if it ever matters is a durable (outbox-style) canonical publish.

## Verification

- TDD throughout (unit tests written first per CLAUDE.md). Key end-to-end guarantees are
  pinned by integration tests against real containers:
  - `search-sync-worker`: delete removes the doc, edit replaces content, pin/unpin
    neither wipes nor resurrects, attachment/card fields land in ES and match via the
    production `multi_match` shape.
  - `search-service` (`TestIntegration_SearchMessages_EditedAndDeletedDocs`): through the
    real NATS RPC and query builder — edited messages found only with post-edit content,
    deleted messages return zero hits, a stale replayed create 409s against the delete
    tombstone and stays gone.
- Integration tests seed indices with production-equivalent keyword/text mappings —
  ES dynamic mapping types `roomId` as `text`, which silently breaks the
  terms-lookup room gate (found on first real run).

## Alternatives considered

1. **Separate typed ES fields** (chosen) — precise, boostable, response can expose what
   matched.
2. Single catch-all `searchText` field — simpler query but loses field-level control;
   changing composition later forces a reindex.
3. Nested attachment/card sub-documents — nested-query cost with no current need (YAGNI).

For stale docs: a `-reconcile` one-shot mode (ES scan verified against Cassandra
`messages_by_id`) was fully built and then removed by owner decision — pre-pipeline
deletions don't matter, and the event flow's Nak/retry covers everything downstream of
a published event.

## Out of scope

Scope decision (owner, 2026-07-21): keep every RPC/API flow unchanged — this work is a
search enhancement only. Delete/edit must propagate to Elasticsearch; tcard and file
data become extra searchable fields. The following surveyed gaps were reviewed and
explicitly deferred:

- Durable (non-best-effort) canonical publish — Cassandra remains the source of truth;
  a publish-side drop (delete committed, event never published) leaves a stale search
  doc with no recovery path — accepted (see "Stale deleted docs" decision above).
- Backfilling attachment/card fields for historical documents (including legacy tcard
  messages, which never flowed through the client send path — they stay unsearchable
  by card data until edited or replayed).
- Purging legacy plaintext columns (`msg`, `attachments`, `card`, `card_action`,
  `quoted_parent_message`) on soft-delete — pre-encryption rows keep content at rest
  behind `deleted=true`; encrypted rows are purged via `enc_payload=null`.
- Deleting Drive/MinIO file bodies when their message is deleted (needs a reference
  story for quoted messages sharing the same fileId, and per-site Drive routing).
- Retracting quoted-message snapshots when the quoted original is deleted (quotes copy
  content by value).
