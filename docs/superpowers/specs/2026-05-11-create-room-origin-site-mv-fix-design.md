# Create-Room Origin-Site MV Fix — Design

**Date:** 2026-05-11
**Status:** Draft
**Services:** `room-worker`
**Related specs:**
- `2026-04-09-room-spotlight-user-room-design.md` (user-room and spotlight collections)
- `2026-04-21-search-service-sync-worker-extension-design.md` (search-sync-worker INBOX consumer)
- `2026-05-01-federated-room-origin-site-mv-fix-design.md` (sibling fix for add/remove flows; this spec applies the same pattern to the create flow)

## Problem

PR #145 closed the origin-site MV gap for `member.add` / `member.remove` by adding a local `chat.inbox.{siteID}.member_added` / `member_removed` publish from `room-worker`, plus a per-remote-site `outbox.{origin}.to.{remote}.member_added` outbox event that arrives on every federated site's INBOX (via JetStream Sources + SubjectTransform) as `chat.inbox.{remote}.aggregate.member_added`. `search-sync-worker` on each site consumes both lanes and updates its `user-room-{siteID}` and `spotlight-{siteID}` ES indexes.

The room-creation path still has the same gap. `room-worker.finishCreateRoom` writes the auto-enrolled `Subscription` rows (creator + DM recipient + every initial channel member) and emits a per-remote-site `outbox.{origin}.to.{remote}.room_created` for federation — but **never publishes a `member_added` event** on either the origin-INBOX local lane or the cross-site outbox. `search-sync-worker` never sees the create.

Result: a freshly-created room is invisible to search until the next add/remove operation re-emits the event.

### User-visible consequences

1. **Spotlight (room typeahead) returns nothing for the new room.** The creator types the room name; `search-sync-worker` has no spotlight doc; no result.
2. **Cross-site message search returns empty for the new room.** CCS terms-lookup against the user's `user-room-{siteID}` doc reports the user as not subscribed; message hits are filtered out as unauthorized.
3. **Self-corrects on churn.** Both indexes catch up on the next `member.add` or `member.remove` (PR #145's publish fires). Until then, the room is silently invisible to search.

### Concrete trace

Alice on `s1` creates a channel `r1` with `Orgs: [eng-org]` (org expands to `[bob@s1, charlie@s1, dave@s2]`). Today:

| Subject | Stream | Effect |
|---|---|---|
| `chat.user.{account}.event.subscription.update` × 4 | core | Frontend left-panel updates for alice, bob, charlie, dave |
| `chat.room.canonical.s1.create` (sys-message only, channel-only) | core | "alice created the room" |
| `outbox.s1.to.s2.room_created` | OUTBOX_s1 | `inbox-worker` on s2 mirrors dave's `Subscription` row |

`s1`'s `user-room-s1` gains zero entries. `s2`'s `user-room-s2` gains zero entries. Alice CCS-querying `r1` from any site → empty result.

## Goals

- `user-room-{siteID}` and `spotlight-{siteID}` on the origin site **and every federated remote site** contain correct entries for every member auto-enrolled at room creation, regardless of room type.
- Fix lives **entirely in `room-worker.finishCreateRoom`** — no changes to `inbox-worker`, `search-sync-worker`, stream config, or any new model types.
- Wire format byte-for-byte compatible with PR #145's existing publishes so `search-sync-worker/inbox_stream.go::parseMemberEvent` decodes all `member_added` events identically (whether create-time or add-member, origin-local or federated-aggregate).

## Non-Goals

- **Backfilling pre-fix rooms.** Forward-only deployment per agreement. Rooms created before this fix lands stay missing in their MV until any later add/remove churn re-emits the event.
- **Changing `chat.user.{account}.event.subscription.update` or `chat.room.canonical.{siteID}.create`.** UI fan-out and sys-message paths are correct; not in scope.
- **Refactoring `finishCreateRoom`** beyond the two added publishes.
- **Mint-on-create for the room encryption key.** Separate concern, deferred until `ENCRYPTION_ENABLED=true` is required in prod.

## Design

### Why this lives in room-worker alone

The cross-site federation path for `member_added` already exists from PR #145:

```
outbox.{origin}.to.{remote}.member_added
  → (JetStream Sources + SubjectTransform)
  → chat.inbox.{remote}.aggregate.member_added
  → search-sync-worker on {remote} updates user-room-{remote}/spotlight-{remote}
```

`search-sync-worker` on the remote site already has `chat.inbox.{remote}.aggregate.member_added` in its consumer's `FilterSubjects`. By making `room-worker.finishCreateRoom` emit the **same** outbox event it already emits in `processAddMembers`, we reuse the entire federation lane end-to-end and `search-sync-worker` indexes the new room without any extra hop through `inbox-worker`.

An alternative considered: have `inbox-worker.handleRoomCreated` re-emit a local `chat.inbox.{remote}.member_added` after creating the subs. Rejected because (i) it duplicates federation work `room-worker` already does for add-members; (ii) adds a second hop on the remote side; (iii) requires `inbox-worker.Handler` to grow a `publish` field and a `siteID` field with all the test churn that implies. The symmetric "publish the same outbox events as add-members" path is materially smaller.

### NATS subjects (all already exist)

```go
// chat.inbox.{siteID}.member_added — origin-site local lane (PR #145 added)
subject.InboxMemberAdded(siteID)

// outbox.{origin}.to.{destSiteID}.member_added — federation lane (PR #145 added)
subject.Outbox(siteID, destSiteID, model.OutboxMemberAdded)
```

### Wire format

Both publishes wrap `model.MemberAddEvent` in `model.OutboxEvent`. The local publish has `SiteID == DestSiteID == originSite` (self-loop convention); the cross-site publish has the per-remote `DestSiteID`. The inner `MemberAddEvent` carries:

| Field | Value |
|---|---|
| `Type` | `model.OutboxMemberAdded` |
| `RoomID` | `room.ID` |
| `RoomName` | `room.Name` (empty for DM/botDM) |
| `Accounts` | Expanded individual accounts (see below) |
| `SiteID` | `room.SiteID` — the origin |
| `JoinedAt` | `req.Timestamp` |
| `HistorySharedSince` | Always `nil` — no prior history at create time |
| `Timestamp` | `now.UnixMilli()` |

### Accounts list

| Publish | `Accounts` source |
|---|---|
| **Origin-local INBOX** (`chat.inbox.{origin}.member_added`) | Every entry in `subs[]` (creator + every auto-enrolled member, including cross-site members for s1's own MV) |
| **Cross-site OUTBOX** (`outbox.{origin}.to.{remote}.member_added`) | Only members whose `SiteID == remote` (per-destination split, matches PR #145's batched outbox shape) |

For channel rooms with `Orgs`, expansion has already happened in `processCreateRoomChannel` before `finishCreateRoom` runs. `subs[]` already carries expanded individual accounts.

### Dedup IDs

Reuse `natsutil.OutboxDedupID(ctx, destSiteID, payloadSeed)` with seed `"{roomID}:{requesterAccount}:{timestamp}"`. PR #145 uses the identical seed shape for the add-members path; identical seed at the same `{destSiteID}` is fine because there's exactly one create per room per requester per timestamp.

### End-to-end flow after the fix

For `[bob@s1, charlie@s1, dave@s2]` channel-create on `s1`:

| Subject | Stream | Effect |
|---|---|---|
| `chat.user.{account}.event.subscription.update` × 4 | core | UI fan-out (unchanged) |
| `chat.room.canonical.s1.create` (sys-message only) | core | "alice created the room" (unchanged) |
| **`chat.inbox.s1.member_added`** (NEW) | INBOX_s1 (local lane) | s1's `search-sync-worker` updates `user-room-s1` + `spotlight-s1` for alice + bob + charlie + dave |
| `outbox.s1.to.s2.room_created` (existing) | OUTBOX_s1 | s2's `inbox-worker` mirrors dave's `Subscription` row |
| **`outbox.s1.to.s2.member_added`** (NEW) | OUTBOX_s1 → INBOX_s2 aggregate lane | s2's `search-sync-worker` updates `user-room-s2` + `spotlight-s2` for dave |

End state: every site's MV/spotlight indexes contain the new room for every locally-affected member.

### Ordering

Both new publishes go inside `finishCreateRoom`:

- **Origin-local INBOX** publish: after the `subscription.update` loop, before the per-remote-site OUTBOX loop. Same position as PR #145's local INBOX publishes in `processAddMembers`/`processRemoveIndividual`/`processRemoveOrg`.
- **Cross-site OUTBOX `member_added`** publish: inside the existing `for destSiteID, accounts := range remoteSiteAccounts` loop, immediately after the existing `room_created` publish for the same dest site.

The federation lane delivers `room_created` and `member_added` to the remote site's INBOX in publish order. `inbox-worker` (which consumes the aggregate lane via `FilterSubjects: aggregate.>`) and `search-sync-worker` (whose `FilterSubjects` matches `aggregate.member_added` but not `aggregate.room_created`) operate on disjoint event types, so the order they execute relative to each other doesn't matter — `search-sync-worker` doesn't read MongoDB and doesn't depend on `Subscription` rows existing.

### Idempotency

`natsutil.OutboxDedupID` produces a stable `Nats-Msg-Id` per `(room, requester, timestamp, destSiteID)`. JetStream stream-level dedup drops redeliveries within its dedup window. Beyond that window, `search-sync-worker`'s Painless last-write-wins guard makes ES replay idempotent.

## Code Changes

### Change 1 — `room-worker/handler.go::finishCreateRoom` (origin-local INBOX publish)

After the existing `subscription.update` loop and channel sys-message publish, before the per-remote-site OUTBOX loop:

```go
accounts := make([]string, 0, len(subs))
for _, sub := range subs {
    accounts = append(accounts, sub.User.Account)
}
inner := model.MemberAddEvent{
    Type:      model.OutboxMemberAdded,
    RoomID:    room.ID,
    RoomName:  room.Name,
    Accounts:  accounts,
    SiteID:    room.SiteID,
    JoinedAt:  req.Timestamp,
    Timestamp: now.UnixMilli(),
}
innerData, _ := json.Marshal(inner)
outbox := model.OutboxEvent{
    Type:       model.OutboxMemberAdded,
    SiteID:     room.SiteID,
    DestSiteID: room.SiteID,
    Payload:    innerData,
    Timestamp:  now.UnixMilli(),
}
outboxData, _ := json.Marshal(outbox)
payloadSeed := fmt.Sprintf("%s:%s:%d", room.ID, requester.Account, req.Timestamp)
if err := h.publish(ctx, subject.InboxMemberAdded(room.SiteID), outboxData, natsutil.OutboxDedupID(ctx, room.SiteID, payloadSeed)); err != nil {
    slog.Error("local inbox member_added publish failed", "error", err, "roomID", room.ID, "requestID", requestID)
}
```

Log-and-continue on publish failure — JetStream redelivery + `search-sync-worker`'s last-write-wins guard handle transient failures self-correctingly.

### Change 2 — `room-worker/handler.go::finishCreateRoom` (cross-site OUTBOX member_added publish)

Inside the existing per-remote-site loop, right after the existing `room_created` publish:

```go
memberEvt := model.MemberAddEvent{
    Type:      model.OutboxMemberAdded,
    RoomID:    room.ID,
    RoomName:  room.Name,
    Accounts:  accounts, // per-dest accounts (loop variable)
    SiteID:    room.SiteID,
    JoinedAt:  req.Timestamp,
    Timestamp: now.UnixMilli(),
}
memberData, _ := json.Marshal(memberEvt)
memberEnvelope := model.OutboxEvent{
    Type:       model.OutboxMemberAdded,
    SiteID:     room.SiteID,
    DestSiteID: destSiteID,
    Payload:    memberData,
    Timestamp:  now.UnixMilli(),
}
memberOutboxData, _ := json.Marshal(memberEnvelope)
memberSeed := fmt.Sprintf("%s:%s:%d", room.ID, requester.Account, req.Timestamp)
if err := h.publish(ctx, subject.Outbox(room.SiteID, destSiteID, model.OutboxMemberAdded), memberOutboxData, natsutil.OutboxDedupID(ctx, destSiteID, memberSeed)); err != nil {
    return fmt.Errorf("publish member_added outbox to %s: %w", destSiteID, err)
}
```

The cross-site publish returns an error on failure (rather than log-and-continue) to match the surrounding `room_created` publish — JetStream NAKs the create, and `room-worker` redelivers from `MESSAGES_{siteID}`.

### Change 3 — `pkg/natsutil/request_id.go::OutboxDedupID`

Lift `room-worker`'s private `outboxDedupID` to `natsutil.OutboxDedupID` — pure logic, identical at both call sites in `room-worker` and consistent with `natsutil`'s ownership of `RequestIDFromContext` and `NewMsg`. Removes a copy I would otherwise introduce.

### What is NOT changed

- `pkg/subject`, `pkg/stream`, `pkg/model` — no new types/subjects.
- `inbox-worker` — untouched. It continues to consume `aggregate.room_created` for sub creation (its current job) and ignores `aggregate.member_added` for fresh rooms because the BulkCreateSubscriptions path is gated on the room having been created via `aggregate.room_created` first (sub creation happens in `handleRoomCreated`; `handleMemberAdded` adds members to an existing room).

  **Subtle:** if the remote site's `inbox-worker.handleMemberAdded` arrives **before** `handleRoomCreated` for a fresh room (out-of-order delivery), it will try to `BulkCreateSubscriptions` and either (a) the unique index `(roomId, u.account)` rejects the duplicate later when `handleRoomCreated` runs, or (b) the first delivery succeeds and the second is a no-op. Either way the end state is correct because both events carry the same `Accounts` list for the locally-resolved subset. JetStream publish order from `OUTBOX_{origin}` is preserved, so out-of-order delivery is unlikely in practice.

- `search-sync-worker`, `message-worker`, `broadcast-worker`, `history-service` — untouched.

### Diff size estimate

- `room-worker/handler.go`: +~50 lines (two publish blocks).
- `pkg/natsutil/request_id.go`: +~13 lines (new `OutboxDedupID` helper, called from 9 existing sites in `room-worker`).
- `room-worker/handler.go` callers: 9 lines updated to use `natsutil.OutboxDedupID` instead of the private helper; private helper deleted.
- Tests: 3 new unit tests (2 for origin-local INBOX publish: DM + channel; 1 for cross-site OUTBOX member_added).

## Testing

Unit tests only. Handler tests inject `publish` as a field already; tests capture publishes and assert on the entries.

### Unit tests — `room-worker/handler_test.go`

- `TestProcessCreateRoom_DM_PublishesLocalInbox`: DM across sites; assert single publish to `subject.InboxMemberAdded(room.SiteID)` with both creator+recipient accounts, `RoomName` empty, `HistorySharedSince` nil, expected `Nats-Msg-Id`.

- `TestProcessCreateRoom_Channel_PublishesLocalInbox`: channel mixed-site; assert single publish to `subject.InboxMemberAdded(room.SiteID)` with creator + every initial member (same-site + cross-site), expected `Nats-Msg-Id`.

- `TestProcessCreateRoom_Channel_PublishesCrossSiteMemberAdded`: channel with at least one cross-site member; assert single publish to `subject.Outbox(origin, remote, model.OutboxMemberAdded)` carrying only the remote-site accounts, with `DestSiteID == remote`, `RoomName` set, `HistorySharedSince` nil. Confirms the existing `room_created` outbox is still emitted on the same loop.

### Out of scope for new tests

- Integration tests against real NATS / Mongo — not in scope (would double diff size for marginal coverage gain).
- `search-sync-worker`'s ES write path — already covered by `search-sync-worker/inbox_integration_test.go` against the aggregate lane.

### Coverage target

Combined unit coverage for `finishCreateRoom` stays above the 80% project minimum.

## Rollout

Both changes are backward-compatible:

- The origin-local INBOX publish is additive on the local site.
- The cross-site OUTBOX `member_added` publish is additive on the federation lane. PR #145 already established this path for add-members; remote sites' `search-sync-worker` consumers already filter for `aggregate.member_added`.

No coordinated multi-site rollout needed. Deploy `room-worker` and the rest of the stack normally.

### Per-site verification after deploy

1. Create a federated room (channel or DM) with members on at least one remote site.
2. Within seconds, query each site's `user-room-{siteID}` ES index and confirm:
   - The creator's doc on the origin site contains the new room ID.
   - Every channel member's / DM recipient's doc on their respective home site contains the new room ID.
3. Confirm spotlight typeahead returns the new room for the creator on the origin site.

## Observability

- **Logs:** new publishes use `slog.Error` log-and-continue (origin local) or return-error (cross-site OUTBOX, matching surrounding `room_created` publish). Failure message: `"local inbox member_added publish failed"` (origin) or `"publish member_added outbox to %s: %w"` (cross-site).
- **Metrics:** none added. Existing JetStream stream-level metrics on `INBOX_{siteID}` and `OUTBOX_{siteID}` will show throughput on the `member_added` subject rise from "PR #145's add/remove rate" to "that plus the create rate".
- **Traces:** the new publishes inherit the request context, so OTel trace IDs propagate end-to-end (room-worker → INBOX → search-sync-worker → ES bulk write all under one trace).

## Risks

- **Stale spec drift if the create path grows new sub sources.** If a future change adds members to a room outside the `subs []*model.Subscription` slice passed to `finishCreateRoom`, the new INBOX publish would miss them. Mitigation: keep `subs` as the single source of truth for "who got auto-enrolled at create time".
- **Cross-site `member_added` for a brand-new room arriving before `room_created`.** Both events flow through OUTBOX in publish order, so JetStream preserves order on the federated stream — out-of-order delivery on the receiver side is theoretically possible only if `inbox-worker` parallelizes consumers across event types. Today it doesn't. If it ever does, the unique index on `(roomId, u.account)` makes the race idempotent.
