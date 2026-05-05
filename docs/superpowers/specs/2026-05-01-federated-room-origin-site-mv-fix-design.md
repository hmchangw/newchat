# Federated Room Origin-Site MV Fix — Design

**Date:** 2026-05-01
**Status:** Draft
**Services:** `room-worker`, `inbox-worker`
**Related specs:**
- `2026-04-09-room-spotlight-user-room-design.md` (user-room and spotlight collections)
- `2026-04-21-search-service-sync-worker-extension-design.md` (search-sync-worker INBOX consumer)
- `2026-04-27-inbox-stream-ownership-design.md` (INBOX stream schema/federation split)

## Problem

`search-sync-worker` consumes the `INBOX_{siteID}` stream on each site with two
subject filters:

- `chat.inbox.{siteID}.member_added` / `member_removed` — **local lane** for
  events from same-site services.
- `chat.inbox.{siteID}.aggregate.member_added` / `member_removed` — **federated
  lane**, sourced from remote OUTBOX streams via JetStream `SubjectTransform`
  (`outbox.{remote}.to.{siteID}.>` → `chat.inbox.{siteID}.aggregate.>`).

The federated lane works. The local lane is empty: `room-worker` publishes
member events to `chat.room.{rid}.event.member` (UI fan-out for clients
watching the room) and to `outbox.{origin}.to.{dest}.member_added` (cross-site
delivery to remote sites only). Nothing publishes to
`chat.inbox.{originSiteID}.member_added`.

Result: when a federated room on site `s1` adds members, the origin site (`s1`)
never updates its own `user-room-{siteID}` or `spotlight-{siteID}` ES indexes —
for either same-site or remote users. CCS queries hitting `s1` find no MV doc
for the user, so the `terms_lookup` filter matches nothing and `s1` returns
zero messages. Since `s1` owns the room's messages, **federated room search
returns empty results** for any user CCS-queried against `s1`.

### Concrete trace

Admin on `s1` adds `[charlie@s1, alice@s2, bob@s3]` to room `r1` (owned by
`s1`). Today:

| Subject | Stream | Accounts | Effect on origin (s1) MV |
|---|---|---|---|
| `chat.room.r1.event.member` | core (no stream) | all three | none — UI fan-out only |
| `outbox.s1.to.s2.member_added` | OUTBOX_s1 | [alice] | none on s1; s2's MV gets alice += r1 |
| `outbox.s1.to.s3.member_added` | OUTBOX_s1 | [bob] | none on s1; s3's MV gets bob += r1 |

s1's MV gains zero entries. Alice CCS-querying r1 messages from s2 fans out to
s1, which has no MV doc for alice → empty result. The messages live on s1.

## Goals

- `user-room-mv` and `spotlight` on the origin site contain correct entries for
  every member added to or removed from a room that site owns, regardless of
  whether the member is same-site or remote.
- Fix lives in `room-worker` plus a one-line tightening of `inbox-worker`'s
  consumer filter — no new services, no new model types, no changes to
  search-sync-worker, stream config, or subject builders.
- The existing `chat.room.{rid}.event.member` UI fan-out and per-dest-site
  OUTBOX publishes remain untouched.

## Non-Goals

- **Fixing the same outbound-only pattern for `role_updated` and `room_sync`.**
  These follow the same shape (room-worker publishes only to OUTBOX, never to
  the origin's local INBOX lane), but no consumer needs them today. When a
  future feature adds an INBOX consumer that cares about role changes (e.g., an
  audit-log search index), repeat the pattern from this spec: publish to
  `chat.inbox.{originSiteID}.role_updated` alongside the existing OUTBOX
  fan-out.
- **Backfilling pre-fix federated rooms.** Forward-only deployment.
  Pre-existing memberships on origin-site MVs stay missing until the room
  experiences add/remove churn or the index is rebuilt out-of-band. Documented
  under "Known Limitations" so SRE expects it.
- **Any change to `chat.room.{rid}.event.member` or its consumers.** UI
  fan-out path is correct; not in scope.
- **Refactoring duplicate `subject.MemberEvent` and `subject.RoomMemberEvent`
  builders** (both return `chat.room.{rid}.event.member`). Cosmetic dedup
  deferred to a separate PR; not in scope per CLAUDE.md "don't refactor
  unrelated code".

## Design

### NATS Subjects

No new subjects. Existing builders in `pkg/subject/subject.go`:

```go
// chat.inbox.{siteID}.member_added — local lane
subject.InboxMemberAdded(siteID)

// chat.inbox.{siteID}.member_removed — local lane
subject.InboxMemberRemoved(siteID)
```

Both already match the `chat.inbox.{siteID}.*` pattern declared by
`pkg/stream.Inbox(siteID).Subjects` and the FilterSubjects already configured
on search-sync-worker's `user-room-sync` and `spotlight-sync` consumers.

### Wire format

The local INBOX publish wraps the existing `MemberAddEvent` / `MemberRemoveEvent`
in `model.OutboxEvent`, matching the federated-lane wire format byte-for-byte
so `search-sync-worker/inbox_stream.go::parseMemberEvent` decodes it
identically.

```go
outbox := model.OutboxEvent{
    Type:       "member_added",       // or "member_removed"
    SiteID:     room.SiteID,          // origin
    DestSiteID: room.SiteID,          // self — local publish
    Payload:    memberEvtData,        // marshalled MemberAddEvent / MemberRemoveEvent
    Timestamp:  now.UnixMilli(),
}
```

The inner payload is reused as-is from the cross-site OUTBOX path. The
`Accounts` slice carries the **full** add/remove set — same-site + remote
together — so the origin's MV gets a doc for every affected member from a
single event.

### Dedup ID

Same shape as the existing cross-site OUTBOX publishes:

```go
payloadSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.RequesterAccount, req.Timestamp)
dedupID := outboxDedupID(ctx, room.SiteID, payloadSeed)
```

`destSiteID = room.SiteID` (self). The `outboxDedupID` helper namespaces by
destination, so the self-targeted ID won't collide with the per-remote-site IDs
the cross-site OUTBOX loop publishes for the same operation.

### End-to-end flow after the fix

For the same `[charlie@s1, alice@s2, bob@s3]` add to room `r1` on `s1`:

| Subject | Stream | Accounts | Consumer |
|---|---|---|---|
| `chat.room.r1.event.member` | core | [charlie, alice, bob] | UI fan-out (unchanged) |
| **`chat.inbox.s1.member_added`** (NEW) | INBOX_s1 (local lane) | [charlie, alice, bob] | s1's `user-room-sync` + `spotlight-sync` → s1's MV/spotlight gain docs for all three |
| `outbox.s1.to.s2.member_added` | OUTBOX_s1 | [alice] | SubjectTransform → `chat.inbox.s2.aggregate.member_added` → s2's search-sync-worker |
| `outbox.s1.to.s3.member_added` | OUTBOX_s1 | [bob] | SubjectTransform → `chat.inbox.s3.aggregate.member_added` → s3's search-sync-worker |

End state: each site's `user-room-{site}` index has docs for charlie, alice,
bob, each containing `r1` in `rooms[]`. CCS terms-lookup queries from any of
the three resolve correctly on every site.

### inbox-worker consumer scoping

`inbox-worker/main.go:157-160` currently creates its consumer with **no
`FilterSubjects`**, so it consumes every subject the INBOX stream accepts —
including the new `chat.inbox.{siteID}.member_added` local lane. Without a
filter fix, inbox-worker would also process the new events:

1. `handleMemberAdded` → `FindUsersByAccounts` on `s1`.
   - Local users (e.g., charlie@s1) → found.
   - Remote users (alice@s2, bob@s3) → not in `s1`'s user collection →
     `userMap` lookup misses → logged "user not found for account"
     (`inbox-worker/handler.go:84`) → skipped.
2. `BulkCreateSubscriptions` for found users.
3. The subscriptions collection has a unique index on `(roomId, u.account)`
   (`room-service/store_mongo.go:55-59`), and room-worker already created those
   subscriptions for local users at `room-worker/handler.go:542` before
   publishing the local INBOX event.
4. → Duplicate-key errors. Swallowed by `inbox-worker/main.go:103`:
   `if err != nil && !mongo.IsDuplicateKeyError(err)`.

Functionally idempotent (no data corruption), but every cross-site member adds
to log spam, and ack'ing a misrouted event is a poor architectural signal.

**Fix:** scope inbox-worker's consumer to the federated lane only.

```go
cons, err := js.CreateOrUpdateConsumer(ctx, inboxCfg.Name, jetstream.ConsumerConfig{
    Durable:        "inbox-worker",
    AckPolicy:      jetstream.AckExplicitPolicy,
    FilterSubjects: []string{fmt.Sprintf("chat.inbox.%s.aggregate.>", cfg.SiteID)},
})
```

Safe because every event type inbox-worker handles today (`member_added`,
`member_removed`, `role_updated`, `room_sync`) arrives via the SubjectTransform
on the aggregate lane. No event type is published to the local lane today, and
this spec reserves the local lane for search-sync-worker.

## Code Changes

### Change 1 — `room-worker/handler.go:657` (add members)

After the existing `subject.RoomMemberEvent` publish, before the OUTBOX loop
at line 690, add a local INBOX publish:

```go
// existing UI fan-out (UNCHANGED)
if err := h.publish(ctx, subject.RoomMemberEvent(req.RoomID), memberAddData, ""); err != nil {
    slog.Error("member add event publish failed", "error", err, "roomID", req.RoomID)
}

// NEW — local INBOX publish for search-sync-worker (origin-site MV update)
inboxOutbox := model.OutboxEvent{
    Type:       "member_added",
    SiteID:     room.SiteID,
    DestSiteID: room.SiteID,
    Payload:    memberAddData,
    Timestamp:  now.UnixMilli(),
}
inboxData, _ := json.Marshal(inboxOutbox)
inboxSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.RequesterAccount, req.Timestamp)
inboxDedupID := outboxDedupID(ctx, room.SiteID, inboxSeed)
if err := h.publish(ctx, subject.InboxMemberAdded(room.SiteID), inboxData, inboxDedupID); err != nil {
    slog.Error("local inbox member_added publish failed", "error", err, "roomID", req.RoomID)
}
```

`memberAddData` is the same `MemberAddEvent`-marshalled payload used for the
cross-site OUTBOX loop. Already carries the full accounts list, timestamp, and
`HistorySharedSince`. No new construction.

If `actualAccounts` is empty (every requested account was a duplicate or
missing user), skip the local INBOX publish entirely — no point emitting an
empty event.

### Change 2 — `room-worker/handler.go:272` (remove individual / self-leave)

After the existing `subject.MemberEvent` publish:

```go
// existing UI fan-out (UNCHANGED) — uses evtType ("member_left" or "member_removed")
if err := h.publish(ctx, subject.MemberEvent(req.RoomID), memberEvtData, ""); err != nil {
    slog.Error("member event publish failed", "error", err, "roomID", req.RoomID)
}

// NEW — local INBOX publish (always "member_removed", matching cross-site convention)
inboxOutbox := model.OutboxEvent{
    Type:       "member_removed",   // collapsed: self-leave and admin-remove are one type below the UI
    SiteID:     h.siteID,
    DestSiteID: h.siteID,
    Payload:    memberEvtData,
    Timestamp:  now.UnixMilli(),
}
inboxData, _ := json.Marshal(inboxOutbox)
inboxSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.Account, req.Timestamp)
inboxDedupID := outboxDedupID(ctx, h.siteID, inboxSeed)
if err := h.publish(ctx, subject.InboxMemberRemoved(h.siteID), inboxData, inboxDedupID); err != nil {
    slog.Error("local inbox member_removed publish failed", "error", err, "roomID", req.RoomID)
}
```

The OutboxEvent wrapper sets `Type: "member_removed"` regardless of whether
the inner `MemberRemoveEvent.Type` is `"member_left"` or `"member_removed"`.
This matches the existing cross-site OUTBOX wrapper at line 310. search-sync-
worker dispatches on the OutboxEvent.Type, not the inner type, so leave and
remove correctly converge to the same MV operation (rid removed from
`rooms[]`).

### Change 3 — `room-worker/handler.go:401` (remove org / batch removal)

Same pattern as Change 2, but the dedup seed uses `req.OrgID` instead of a
per-user account, matching the existing cross-site OUTBOX path at line 455:

```go
inboxSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.OrgID, req.Timestamp)
inboxDedupID := outboxDedupID(ctx, h.siteID, inboxSeed)
```

The `accounts` slice for the org-removal batch carries every removed user
(same-site + remote), and the local INBOX publish emits one event covering all
of them. The existing publish is already gated on `len(accounts) > 0` (line
391); the new local INBOX publish goes inside that same `if` block, so a
zero-account batch produces no publish.

### Change 4 — `inbox-worker/main.go:157` (filter to aggregate lane only)

```go
cons, err := js.CreateOrUpdateConsumer(ctx, inboxCfg.Name, jetstream.ConsumerConfig{
    Durable:        "inbox-worker",
    AckPolicy:      jetstream.AckExplicitPolicy,
    FilterSubjects: []string{fmt.Sprintf("chat.inbox.%s.aggregate.>", cfg.SiteID)},
})
```

One-line addition. Scopes inbox-worker to the federated lane so the new
local-lane events don't reach it.

### Error handling

All three new publishes use the same log-and-continue pattern as the existing
`subject.MemberEvent` / `subject.RoomMemberEvent` publishes immediately above
them. The local INBOX publish failing should not fail the add-members /
remove-member request — JetStream redelivery and the search-sync-worker's
Painless last-write-wins guard handle transient publish failures
self-correctingly. Failing the whole user-facing request because the local
search index didn't update would be the wrong trade-off.

### What is NOT changed

- `pkg/subject/subject.go` — `InboxMemberAdded` / `InboxMemberRemoved` already
  exist; we just call them.
- `pkg/stream/stream.go` — `INBOX_{siteID}` already accepts
  `chat.inbox.{siteID}.*`.
- `pkg/model` — no new types; reuses `OutboxEvent`, `MemberAddEvent`,
  `MemberRemoveEvent`.
- `outboxDedupID` — already in `room-worker`; reused with `siteID` as both
  source and destination.
- `search-sync-worker`, `message-worker`, `broadcast-worker`,
  `history-service` — untouched.

### Diff size estimate

- `room-worker/handler.go`: +~45 lines (3 publish blocks × ~15 lines each).
- `inbox-worker/main.go`: +1 line.
- Tests: see Testing.

## Testing

### Unit tests — `room-worker/handler_test.go`

Extend three existing tables (one per affected handler method). The handler
injects `publish` as a field; tests already capture publishes into a slice — we
assert one additional entry per case.

For each scenario, assert that `chat.inbox.{siteID}.member_added` (or
`.member_removed`) is published exactly once, with:

| Field | Expected |
|---|---|
| Subject | `subject.InboxMemberAdded(room.SiteID)` / `InboxMemberRemoved(h.siteID)` |
| Payload `OutboxEvent.Type` | `"member_added"` or `"member_removed"` |
| Payload `OutboxEvent.SiteID` and `DestSiteID` | both equal to origin site (self-loop) |
| Payload `OutboxEvent.Timestamp` | matches `now.UnixMilli()` (use injected clock) |
| Inner `MemberAddEvent.Accounts` / `MemberRemoveEvent.Accounts` | full set — same-site + remote |
| Inner `MemberAddEvent.HistorySharedSince` | matches the value carried on cross-site OUTBOX |
| `Nats-Msg-Id` | `outboxDedupID(ctx, room.SiteID, "{rid}:{requester}:{timestamp}")` |

Cases per handler method:

- **Add members** (`processAddMembers`):
  - all same-site users
  - all cross-site users
  - mixed same-site + cross-site (the bug-trigger case — assert one local INBOX
    publish carrying all of them, plus the per-dest OUTBOX publishes for
    remote ones)
  - empty accounts after dedup → assert NO local INBOX publish
  - publish error → asserted as logged-and-continue, request still succeeds
- **Remove individual** (`processRemoveIndividual`):
  - admin-removed user, same-site
  - admin-removed user, cross-site
  - self-leave, same-site (assert OutboxEvent.Type = `"member_removed"` even
    though inner payload `MemberRemoveEvent.Type` is `"member_left"`)
  - self-leave, cross-site
- **Remove org** (`processRemoveOrg`):
  - org with all same-site users
  - org with mixed-site users
  - org with no actual matches (zero accounts) → no local INBOX publish

### Integration tests

#### `room-worker/integration_test.go::TestRoomWorker_AddMembers_PublishesLocalInbox`

Spin up NATS via testcontainers, create `INBOX_{siteID}` stream from
`pkg/stream.Inbox`, run the add-members flow end-to-end, then:

1. Subscribe with the FilterSubjects search-sync-worker uses
   (`subject.InboxMemberEventSubjects(siteID)`).
2. Assert one message arrives on `chat.inbox.{siteID}.member_added` (the local
   lane) within a bounded timeout.
3. Decode it via the same `parseMemberEvent` helper search-sync-worker uses
   (in `search-sync-worker/inbox_stream.go`) to prove payload-shape
   compatibility — the test's main value over the unit tests.
4. Assert `Nats-Msg-Id` is present on the message header.

A sibling `TestRoomWorker_RemoveMembers_PublishesLocalInbox` covers the remove
path.

#### `inbox-worker/integration_test.go::TestInboxWorker_DoesNotConsumeLocalLane`

Publishes a synthetic `chat.inbox.{siteID}.member_added` directly, runs
inbox-worker briefly, asserts the consumer's pending count for that subject
remains 0 (filter excludes it). Locks in Change 4.

### Out of scope for new tests

- search-sync-worker's ES write path — already covered by
  `search-sync-worker/inbox_integration_test.go` against the aggregate lane.
  The local lane uses an identical payload shape decoded by the same
  `parseMemberEvent`, so duplicating that test against the local lane adds
  little.
- The cross-site OUTBOX path — untouched by this change; existing tests cover
  it.
- The UI fan-out path on `chat.room.{rid}.event.member` — untouched.

### Coverage target

Combined unit + integration coverage for the three modified handler methods
stays above the 80% project minimum (CLAUDE.md). Spot-check via
`go tool cover -func=coverage.out` after implementation.

## Rollout

Both changes are backward-compatible:

- room-worker publishing to `chat.inbox.{siteID}.member_added` is purely
  additive. An old inbox-worker still on the old code (without the filter
  scoping) would receive the events but harmlessly swallow them via
  duplicate-key suppression. Search-sync-worker has always filtered on these
  subjects and just starts receiving traffic.
- inbox-worker filter scoping is safe to deploy independently. Every event
  type it processes today already arrives on the `aggregate.>` lane via
  SubjectTransform.

Recommended: deploy both services in the same release per site. No
coordinated multi-site rollout needed — each site is self-contained.

### Per-site verification after deploy

1. Inspect inbox-worker's consumer: `nats consumer info INBOX_{siteID}
   inbox-worker` should show `filter_subjects: ["chat.inbox.{siteID}.aggregate.>"]`.
2. Add a test member to a federated room owned by the site. Within seconds,
   query the site's `user-room-{siteID}` ES index and confirm the user's doc
   contains the new room ID.

## Observability

- **Logs:** new publishes use `slog.Error` log-and-continue. Failure messages:
  `"local inbox member_added publish failed"` /
  `"local inbox member_removed publish failed"`. Search these post-deploy to
  confirm zero failures.
- **Metrics:** none added. Existing JetStream stream-level metrics on
  `INBOX_{siteID}` will show throughput on `chat.inbox.{siteID}.member_added`
  rise from zero to roughly the rate of cross-site OUTBOX publishes — that
  rise is itself the success signal.
- **Traces:** the new publish calls inherit the request context, so OTel trace
  IDs propagate end-to-end (room-worker → INBOX → search-sync-worker → ES bulk
  write all under one trace).

## Known Limitations

**No backfill for pre-existing federated rooms.** Federated rooms whose
origin-site MV is missing entries from before this fix shipped will stay
missing on that site until either:

- A member is added to or removed from the room (any churn re-emits the event
  for the full member set affected by that operation), OR
- The index is rebuilt out-of-band by an operator.

If the gap proves operationally painful at any later point, write a one-time
backfill script under `tools/` then. The script would read federated-room
subscriptions from MongoDB on each site and replay synthetic
`chat.inbox.{siteID}.member_added` events; the Painless last-write-wins guard
in `search-sync-worker/user_room.go` ensures idempotent replay.

## Risks

- **Log spam from inbox-worker if room-worker deploys first.** During the
  window where room-worker is publishing to the local lane but inbox-worker
  hasn't yet picked up the filter scoping, inbox-worker logs "user not found
  for account" warnings for every cross-site member added. Mitigation:
  deploy both services in the same release.
- **Future service unintentionally publishing to the local lane.** The
  `chat.inbox.{siteID}.*` subject is permissive at the stream level.
  Mitigation: doc comments on `subject.InboxMemberAdded` /
  `InboxMemberRemoved` (already in `pkg/subject/subject.go`) document them as
  the only intended publishers; this spec adds to that contract.
