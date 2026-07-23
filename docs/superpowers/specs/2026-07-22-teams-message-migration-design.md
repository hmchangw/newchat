# Teams Message-History Migration — Design

**Goal:** ingest Teams message history into nextgen during a bounded, multi-day bulk
sync — idempotently, reusing the persistence pipeline. Migrated messages are indexed by
a dedicated search-sync consumer on the batch subject (no re-publish, no Mongo).

## Overview

A durable JetStream consumer on the canonical stream, filtered to
`chat.msg.canonical.{siteID}.teams.batch`, served by `message-worker`. A server-side
producer publishes batches of Teams messages onto that subject; the consumer transforms
each into a canonical `model.Message` and feeds it straight through message-worker's own
persist pipeline with `isMigration=true` — the same persist + thread/mention/quote path
the live `.created` consumer runs. It does **not** re-publish to canonical, and there is
no reply.

## Reuse via `isMigration` — no re-publish

Feeding the transformed message through `processMessage(ctx, evt, isMigration=true)`
reuses the entire persistence path (Cassandra insert, thread materialization, quote
re-projection) with the source's live side-effects suppressed. Nothing is re-published
to the `.created` event, so no fan-out or notification happens. Search indexing is not
driven off `.created` here; it is handled separately by a dedicated search-sync consumer
on the batch subject (see below).

**Cross-consumer isolation (the `.teams.batch` subject rides the canonical wildcard).**
`.teams.batch` is a two-token tail under `chat.msg.canonical.{siteID}.>`, whereas every
per-message event (`.created`/`.updated`/`.deleted`/`.pinned`/`.unpinned`/`.reacted`) is
single-token. Consumers that handle message events therefore bind
`chat.msg.canonical.{siteID}.*` (single-token, via `subject.MsgCanonicalMessageWildcard`)
so the batch envelope is never delivered to them: `message-worker`'s message consumer and
`notification-worker` were already filtered to `.created`; `broadcast-worker` (via its
`INPUT_SUBJECT_FILTER` env, defaulted to `.*`) and `search-sync-worker`'s message consumer
narrow from `.>` to `.*` for the same reason. The batch is received by two dedicated
consumers: `message-worker`'s `.teams.batch` consumer, which persists it, and
`search-sync-worker`'s new `message-sync-teams` consumer, which indexes it.

**Delivery + retries:** the consumer Acks a batch once every message has been handled.
A per-message transform error is logged as that message's result and does **not** block
the batch (a deterministic bad message must not poison-loop the whole batch). An infra
failure in the persist pipeline surfaces so the batch is Nak'd — at-least-once
redelivery re-runs it, safe because the deterministic message id makes every write
idempotent.

## `MessageTransformer` seam

```go
type MessageTransformer interface {
    Transform(ctx context.Context, raw json.RawMessage) (model.Message, error)
}
```

`raw` is opaque JSON so the seam is source-agnostic. `DefaultTransformer` composes:
HTML→supported-markdown (unsupported markup degrades to raw text), message type
(user vs system), and the reply(quote) shape via `QuotedParentMessage`. It resolves
the sender and each mention through the same resolver.

- **Forward branch: stubbed** — it depends on a `Forwarded` model field that lands with
  the forward feature. Until then a forwarded message migrates as a plain message; no
  forward field is set.
- **Reactions:** the reaction→shortcode table exists and is tested, but `model.Message`
  has no reactions field, so reactions are not attached on the created event; they
  migrate as separate reacted events in a follow-up.

## Sender resolution

Resolve a Teams user (graph id + display name) to a nextgen identity, per-batch cache.
The store is message-worker-local (`mongoHRIdentityStore`); `employeeIDFromGraphID` is a
per-service copy — the shared HR store was removed in a prior merge, so there is no
shared `pkg/hrstore`. `employeeId` is globally unique, so the reads/upsert key on it
alone (no site term).

1. Read by `employeeId = employeeIDFromGraphID(graphId)` (the same hash the HR sync
   uses) — the authoritative key. Found → reuse (a `FindUserByEmployeeId` read).
2. Else a unique display-name match (`FindUserByDisplayName` exactly-one across all
   sites; ambiguous/zero falls through) — the fuzzy fallback. `employeeId` being global,
   the name lookup is global too (no site term).
3. Else create via `UpsertUserIdentities`, keyed on `employeeId`. Reaching this only for
   genuinely-new users means the upsert never overwrites an existing identity — no clobber.

`account = employeeId` for a created identity (no UPN at the message layer); the display
name lands in `chineseName`, mirroring the HR mapping.

## Idempotency

Message id = deterministic hash of the Teams id in valid `idgen` message-id format, so a
re-run of the same batch (or a Nak redelivery) overwrites the same Cassandra row.
Sender-create is an upsert on `employeeId`. Both make the multi-day sync safe to retry.

A message with an empty `roomId` is `skipped`: its deterministic id would collide across
conversations and the message would orphan, so it is never persisted.

## Error handling

Per message, a malformed payload or transform/resolve failure is logged as that
message's `status: error` and the batch continues (Ack) — a deterministic bad message
must not poison-loop the whole batch. A message with no id, or no roomId, is `skipped`.
An infra failure in the persist pipeline Naks the batch for redelivery.

## Search indexing (dedicated consumer, no Mongo)

Because the migration emits no `.created` event, search-sync indexes migrated messages via
a second consumer, `message-sync-teams`, bound to `.teams.batch` on the same
`MESSAGES_CANONICAL` stream and writing the same message index. It derives every field from
the raw payload — crucially the author key `UserID = employeeIDFromGraphID(from.id)`, the
same hash the migration writes as the user's `_id` — so it needs no Mongo lookup. It applies
the same skips as the persist consumer (no id / no roomId; system messages) so the index
matches what was persisted, and reuses the deterministic message id + `createdAt` as the ES
external version, making a batch replay idempotent.

## Testing

- Unit: `DefaultTransformer` shapes (reply/system, HTML→md incl. unsupported fallback,
  mention resolution, quoted-parent scoped by the outer room), reaction-shortcode table,
  deterministic-id stability, sender-resolution matrix (display-name reuse /
  ambiguous→create / create / cache), per-message status + Nak-on-infra.
- Integration: batch → real persist pipeline → Cassandra persist + idempotent re-run
  (single row).
