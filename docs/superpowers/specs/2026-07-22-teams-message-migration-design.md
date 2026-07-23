# Teams Message-History Migration — Design

**Goal:** ingest Teams message history into nextgen during a bounded, multi-day bulk
sync — idempotently, reusing the persistence pipeline; search indexing is deferred (see Known limitation).

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
to the `.created` event, so no fan-out, notification, or indexing happens — the silent
migration by design.

**Cross-consumer isolation (the `.teams.batch` subject rides the canonical wildcard).**
`.teams.batch` is a two-token tail under `chat.msg.canonical.{siteID}.>`, whereas every
per-message event (`.created`/`.updated`/`.deleted`/`.pinned`/`.unpinned`/`.reacted`) is
single-token. Consumers that handle message events therefore bind
`chat.msg.canonical.{siteID}.*` (single-token, via `subject.MsgCanonicalMessageWildcard`)
so the batch envelope is never delivered to them: `message-worker`'s message consumer and
`notification-worker` were already filtered to `.created`; this PR narrows
`broadcast-worker` and `search-sync-worker` from `.>` to `.*` for the same reason.
Only `message-worker`'s dedicated `.teams.batch` consumer receives the batch.

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

## Known limitation

Migrated messages are **persisted but NOT indexed in search**. Search indexing keys on
the `.created` canonical NATS event, which this path intentionally does not emit (that
is what gives the silent no-fan-out migration). A search backfill for migrated history
is a follow-up.

## Testing

- Unit: `DefaultTransformer` shapes (reply/system, HTML→md incl. unsupported fallback,
  mention resolution, quoted-parent scoped by the outer room), reaction-shortcode table,
  deterministic-id stability, sender-resolution matrix (display-name reuse /
  ambiguous→create / create / cache), per-message status + Nak-on-infra.
- Integration: batch → real persist pipeline → Cassandra persist + idempotent re-run
  (single row).
