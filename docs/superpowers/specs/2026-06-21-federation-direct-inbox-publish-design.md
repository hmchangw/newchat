# Federation: Direct Inbox Publish (replace Outbox/Inbox sourcing)

**Date:** 2026-06-21
**Status:** Approved
**Branch:** `claude/federation-outbox-refactor-r2crlw`

## Problem

Cross-site federation currently uses an Outbox/Inbox JetStream pattern:

1. A service at site A publishes a federation event to its **OUTBOX** stream on
   subject `outbox.{siteA}.to.{siteB}.{eventType}`.
2. Ops/IaC configures site B's **INBOX** stream to *source* from site A's OUTBOX,
   rewriting the subject via a `SubjectTransform`:
   `outbox.{siteA}.to.{siteB}.>` → `chat.inbox.{siteB}.aggregate.>`.
3. `inbox-worker` at site B consumes the `aggregate.>` lane and applies the event
   to site B's MongoDB.

Same-site member events are published by `room-worker` to the *local* lane
`chat.inbox.{siteID}.member_added` / `member_removed` purely as a search-indexing
feed for `search-sync-worker`; `inbox-worker` deliberately ignores them (it already
filters to `aggregate.>`) because `room-worker` writes those subscriptions to Mongo
synchronously.

The OUTBOX hop adds an extra stream, a sourcing relationship, and a
`SubjectTransform` owned by ops/IaC. We want to remove it.

## Goal

- Remove all OUTBOX JetStream streams and `outbox.*` subjects.
- For cross-site federation, the origin site's service uses a **JetStream publish
  directly to the destination site's INBOX** using subject
  `chat.inbox.{destSiteID}.external.{eventType}` (routed across the NATS
  supercluster to the destination's INBOX stream).
- Preserve the local-vs-remote distinction so `inbox-worker` still only applies
  remote-origin events to the DB, never re-applying same-site member events.

## Subject scheme

Two explicit lanes replace today's local / `aggregate` split:

| Lane | Subject | Origin | Consumed by |
|------|---------|--------|-------------|
| external | `chat.inbox.{siteID}.external.{eventType}` | a **remote** site publishing into this site | inbox-worker (applies to DB), search-sync-worker (member events) |
| internal | `chat.inbox.{siteID}.internal.{eventType}` | a **local** service (room-worker member feed) | search-sync-worker (member events) only |

`inbox-worker` consumes the `external.>` lane exclusively — this is the structural
guard that prevents re-applying same-site events (which were already written to
Mongo by their originating service). This maps 1:1 onto today's behavior where
`inbox-worker` filtered `aggregate.>` and skipped the local lane.

## Architecture changes

### `pkg/subject/subject.go`

Remove: `Outbox`, `OutboxWildcard`, `InboxMemberAddedAggregate`,
`InboxMemberRemovedAggregate`, `InboxAggregateAll`, and the old single-token
`InboxMemberAdded`/`InboxMemberRemoved` shapes.

Add:

| Builder | Subject |
|---------|---------|
| `InboxExternal(siteID, eventType)` | `chat.inbox.{siteID}.external.{eventType}` |
| `InboxInternal(siteID, eventType)` | `chat.inbox.{siteID}.internal.{eventType}` |
| `InboxExternalAll(siteID)` | `chat.inbox.{siteID}.external.>` |
| `InboxMemberEventSubjects(siteID)` | `[internal.member_added, internal.member_removed, external.member_added, external.member_removed]` |

### `pkg/stream/stream.go`

- Remove `Outbox(siteID)`.
- `Inbox(siteID)` subjects → `["chat.inbox.{siteID}.external.>", "chat.inbox.{siteID}.internal.>"]`.

### `pkg/model/event.go` — rename Outbox* → Inbox*

- `OutboxEvent` → `InboxEvent` (struct shape unchanged: `Type, SiteID, DestSiteID, Payload, Timestamp`). `SiteID` is the origin site (the value inbox-worker trusts); `DestSiteID` is now also encoded in the subject.
- `OutboxEventType` → `InboxEventType`.
- Constants `OutboxMemberAdded` … `OutboxUserStatusUpdated` → `InboxMemberAdded` … `InboxUserStatusUpdated` (10 total).
- `RoomRenamedOutboxPayload` → `RoomRenamedInboxPayload`; `RoomRestrictedOutboxPayload` → `RoomRestrictedInboxPayload`.

### `pkg/natsutil`

- `OutboxDedupID(ctx, destSiteID, payloadSeed)` → `InboxDedupID(ctx, destSiteID, payloadSeed)` (behavior unchanged).

### Publishers

| Service | Events | Change |
|---------|--------|--------|
| room-worker | member_added/removed, room_renamed | same-site → `InboxInternal(siteID, type)`; cross-site → `InboxExternal(destSiteID, type)` |
| room-service | role_updated, subscription_read, thread_read, subscription_mute_toggled, subscription_favorite_toggled, room_restricted | `Outbox(...)` → `InboxExternal(destSiteID, type)` (already remote-only) |
| message-worker | thread_subscription_upserted | `Outbox(...)` → `InboxExternal(ownerSiteID, type)` |
| user-service | user_status_updated | `Outbox(...)` → `InboxExternal(dest, type)` **and** switch from core-NATS publish to JetStream publish so the event lands in the remote INBOX stream across the supercluster |

`user-service` is the one non-mechanical change: its status fan-out currently uses
a core-NATS publisher. A core publish cannot be relied on to land in a remote
cluster's stream; it must use a JetStream publish (with `Nats-Msg-Id` dedup via
`InboxDedupID`). Verify/extend user-service wiring to expose a JS publish func.

### Consumers

- **inbox-worker**: `buildConsumerConfig` FilterSubjects → `InboxExternalAll(siteID)`;
  `isMembershipSubject` → external member subjects; dispatch switch constants → `Inbox*`.
- **search-sync-worker**: `FilterSubjects` → `InboxMemberEventSubjects(siteID)`
  (now internal+external); `parseMemberEvent` decodes `InboxEvent`.

## Data flow (after)

Same-site member add (room-worker at site A):
- Writes subscriptions to Mongo synchronously.
- Publishes `chat.inbox.A.internal.member_added` → search-sync-worker(A) indexes.
- inbox-worker(A) does NOT consume internal lane → no double write.

Cross-site member add (room-worker at site A, member home = site B):
- JetStream publish `chat.inbox.B.external.member_added` → routed to INBOX_B.
- inbox-worker(B) applies subscriptions to Mongo(B).
- search-sync-worker(B) indexes from the external lane.

## Error handling

Unchanged. inbox-worker keeps its permanent-vs-transient Ack/Nak logic and the
serial membership lane / concurrent fan-out lane split. The membership lane now
keys on `external.member_added` / `external.member_removed`.

## Testing

- Unit tests: update every `subject.Outbox` / `OutboxEvent` / `OutboxDedupID` /
  `Outbox*` constant reference across handler tests.
- Integration tests: repoint OUTBOX-creating / `aggregate.*` / local-lane test
  setup to the `external` / `internal` lanes (inbox-worker, search-sync-worker,
  room-worker, room-service, message-worker, user-service).
- Verify inbox-worker's `external.>` filter excludes `internal.*` publishes; verify
  search-sync-worker indexes both lanes.
- Maintain the 80% coverage floor.

## Infra / dev

- Remove OUTBOX from any in-repo docker-compose / dev NATS config / bootstrap path.
- Ensure dev + integration stand up INBOX with both `external.>` and `internal.>`
  subjects and exercise direct `external` publish (no sourcing/SubjectTransform).

## Docs

- `docs/nats-subject-naming.md`: drop the OUTBOX section; rewrite INBOX with the
  internal/external lanes and the direct-publish flow.
- `CLAUDE.md`: update the federation / event-flow description, the OUTBOX JetStream
  bullet, and the `outbox.{siteID}.to.{destSiteID}.{eventType}` subject line.
- No `docs/client-api.md` change (no `chat.user.` client-facing subject changes).

## Out of scope / assumptions

- The NATS supercluster / gateway routing that makes a remote JetStream publish to
  `chat.inbox.{destSiteID}.external.*` land in the destination's INBOX stream is an
  ops/IaC concern, not authored in this repo. The design assumes that routing
  exists (replacing today's INBOX-sources-from-OUTBOX wiring).
