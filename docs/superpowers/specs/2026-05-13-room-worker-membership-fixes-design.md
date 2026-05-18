# Room-Worker Membership Fixes — Design

**Date:** 2026-05-13
**Branch:** `claude/fix-org-membership-duplication-l5LfI`
**Service:** `room-worker`

## 1. Bugs

1. **Org-member duplication.** Users brought in via org expansion get both an individual `room_members` doc AND the org `room_members` doc, so they appear twice in `ListRoomMembers`.
2. **`members_added` empty body.** Sys-message carries `SysMsgData` but no `Content` — channel bubble renders blank.
3. **`member_removed` / `member_left` no sender + empty body.** Sys-message sets no `UserID`, `UserAccount`, or `Content` — UI renders "Unknown sent the message" with empty body.

No wire-schema changes. No data migration for existing duplicates — rooms with pre-existing duplicate `room_members` docs remain duplicated until a separate cleanup job (out of scope) is run.

## 2. Fixes

### 2.1 Individual `room_members` write rule

A user gets an individual `room_members` doc iff their account is in the request's direct-individual set:

- `processAddMembers` ([handler.go:649-661](../../../room-worker/handler.go#L649-L661)): direct set = `req.Users`.
- `processCreateRoomChannel` ([handler.go:1056-1063](../../../room-worker/handler.go#L1056-L1063)): direct set = `req.ResolvedUsers ∪ {requester.Account}`.

Channel-ref accounts/orgs are already merged into `req.Users`/`req.Orgs` (and `req.ResolvedUsers`/`req.ResolvedOrgs`) upstream in room-service, so the whitelist picks them up automatically. Org doc writing and the `writeIndividuals` gate are unchanged.

This filter applies *inside* the existing gates — `processCreateRoomChannel`'s `if len(req.ResolvedOrgs) > 0` block at [handler.go:1054](../../../room-worker/handler.go#L1054) (no-orgs lite-mode continues to skip `room_members` entirely per the comment at [handler.go:1076-1079](../../../room-worker/handler.go#L1076-L1079)) and `processAddMembers`'s `writeIndividuals` gate. Neither gate is replaced.

### 2.2 Backfill gate

The backfill at [handler.go:677-708](../../../room-worker/handler.go#L677-L708) materializes individual docs for pre-existing subscribers. Current gate (`writeIndividuals && len(req.Orgs) > 0`) fires on every org-bearing add; after 2.1 it would re-introduce the duplication for previously org-expanded users.

Tighten to the **first-org transition only**: run backfill iff `len(req.Orgs) > 0 && !hadOrgsBefore`.

The current block at [handler.go:637-644](../../../room-worker/handler.go#L637-L644) short-circuits `HasOrgRoomMembers` when `len(req.Orgs) > 0`, so `hadOrgsBefore` isn't actually available in the org-bearing path. Restructure to always query first:

```go
hadOrgsBefore, err := h.store.HasOrgRoomMembers(ctx, req.RoomID)
if err != nil {
    slog.Warn("check existing org room members failed", "error", err, "roomID", req.RoomID)
}
writeIndividuals := len(req.Orgs) > 0 || hadOrgsBefore
```

Cost: one extra indexed Mongo read on the org-bearing path (which is already about to do a bulk insert). Benefit: the backfill gate reduces to a trivial boolean check and is correct in every path.

### 2.3 `members_added` Content

- `processAddMembers` ([handler.go:776-784](../../../room-worker/handler.go#L776-L784)) — count-sensitive on `len(subs)`, but only the **direct-add** case takes the single form:
  - `len(subs) == 1 && len(req.Orgs) == 0`: `"{req.engName} {req.chineseName} added {u.engName} {u.chineseName} to the channel"`
  - otherwise (multi-direct, or any org-bearing add even when the org expands to one user): `"{req.engName} {req.chineseName} added members to the channel"`

  Rationale: when the requester adds an org that happens to have one member, the message "Alice added Bob to the channel" misleadingly implies an individual add — and future org members would later appear without any matching sys-message. Pinning the single form to `len(req.Orgs) == 0` keeps the message honest about what was actually added.
- `publishChannelSysMessages` ([handler.go:1248-1256](../../../room-worker/handler.go#L1248-L1256)) — always multi form:
  - `"{req.engName} {req.chineseName} added members to the channel"`

`processAddMembers` needs the requester's `EngName`/`ChineseName`; fetch via `store.GetUser(ctx, req.RequesterAccount)`. The alternative — appending the requester to the existing `FindUsersByAccounts` call at [handler.go:572](../../../room-worker/handler.go#L572) and excluding it from the sub-build loop at [handler.go:606-631](../../../room-worker/handler.go#L606-L631) — saves one Mongo roundtrip but adds branching to the hot path; on a low-throughput RPC the dedicated fetch is the cleaner choice.

A miss is a permanent error: `newPermanent("requester %s not found", req.RequesterAccount)`. Empty `EngName` or `ChineseName` is also a permanent error, mirroring the create-room validation at [handler.go:1025-1027](../../../room-worker/handler.go#L1025-L1027). The same empty-name check must be added for the *added* users returned by `FindUsersByAccounts` — existing code at [handler.go:585-589](../../../room-worker/handler.go#L585-L589) only checks for missing accounts.

`publishChannelSysMessages` already has `requester *model.User` in scope (validated at create-room time).

### 2.4 `member_removed` / `member_left` — sender + Content

**Sender envelope (when emitted):** set `UserAccount = req.Requester`. `UserID` is unused by broadcast-worker's sender enrichment ([handler.go:73](../../../broadcast-worker/handler.go#L73)), so it stays empty — keeps `RemoveMemberRequest` wire schema untouched.

**Content text (passive voice — no requester name in the body):**

| Path | Sys-type | Content |
|---|---|---|
| `processRemoveIndividual` self-leave | `member_left` | `"{user.engName} {user.chineseName} left the channel"` |
| `processRemoveIndividual` other | `member_removed` | `"{user.engName} {user.chineseName} has been removed from the channel"` |
| `processRemoveOrg` | `member_removed` | `"{sectName} has been removed from the channel"` |

The removed user is already fetched at [handler.go:259](../../../room-worker/handler.go#L259). No requester lookup is needed on the remove paths. If the fetched user has empty `EngName` or `ChineseName`, return a permanent error (same validation as [handler.go:1025-1027](../../../room-worker/handler.go#L1025-L1027)) so the formatter never produces a malformed body.

**`sectName` source for `processRemoveOrg`:** `SectName` is NOT on `RemoveMemberRequest`. Currently harvested from `toRemove` ([handler.go:433-437](../../../room-worker/handler.go#L433-L437)), which is empty when every org member also has an individual sub. Change the loop to iterate the **unfiltered** `members` slice returned by `GetOrgMembersWithIndividualStatus` and pick the first non-empty `OrgMemberStatus.SectName`. If every member's `SectName` is empty (a data inconsistency upstream), return a permanent error rather than emit a malformed sys-message. The same value continues to populate `MemberRemoved.SectName`.

### 2.5 When to emit on the remove paths

- `processRemoveIndividual` full removal (no org overlap; sub + indiv room_members both deleted): **emit**.
- `processRemoveIndividual` demote-only (user is also reachable via an org; indiv room_members deleted, sub preserved, owner→member if applicable): **skip**. The user has not actually left.
- `processRemoveOrg`: **always emit** (org's room_members doc is deleted). Holds even when some org members keep individual subs.

### 2.6 Helpers

New `room-worker/sysmsg.go`:

- `formatAddedSingle(requester, added *model.User) string` — `"{req.engName} {req.chineseName} added {added.engName} {added.chineseName} to the channel"`. Used by `processAddMembers` when `len(subs) == 1`.
- `formatAddedMulti(requester *model.User) string` — `"{req.engName} {req.chineseName} added members to the channel"`. Used by `processAddMembers` when `len(subs) >= 2` and by `publishChannelSysMessages` unconditionally.
- `formatRemovedUserContent(user *model.User) string`
- `formatRemovedOrgContent(sectName string) string`
- `formatLeftContent(user *model.User) string`

Display-name composition: `strings.TrimSpace(u.EngName + " " + u.ChineseName)`. Empty-name inputs are *not* handled by the formatters — callers validate `EngName`/`ChineseName` non-empty before invocation (§2.3, §2.4). The formatters trust their inputs.

Unit tests in `room-worker/sysmsg_test.go`. The membership-correctness work above does not change the store interface; §3.4 adds `UpdateDMParticipants` to `SubscriptionStore` separately for DM participant persistence.

## 3. DM Participant Fields

Unrelated to the membership-correctness work above, but folded into this spec per project direction: every `dm` and `botDM` room gets two new `model.Room` fields that expose the two participants directly on the room document, so clients and downstream services don't have to follow the room → subscription join to learn who the pair is.

### 3.1 Field shape

Add to `pkg/model/room.go`:

```go
type Room struct {
    // ...existing fields unchanged...
    UIDs     []string `json:"uids,omitempty"     bson:"uids,omitempty"`
    Accounts []string `json:"accounts,omitempty" bson:"accounts,omitempty"`
}
```

Both are `omitempty` on `json` and `bson`. Non-DM rooms (channel, discussion) MUST never carry them — the field is omitted from the BSON document, not stored as an empty array. Legacy `dm`/`botDM` rooms created before this change also continue to omit them (see §3.3).

### 3.2 Pairing invariant

`UIDs` is sorted lexicographically. `Accounts` is permuted to mirror the `UIDs` ordering — that is, `UIDs[i]` and `Accounts[i]` describe the same user. The two arrays are not independently sorted.

New helper in `pkg/model/room.go`:

```go
// BuildDMParticipants returns ([uidA, uidB], [accountA, accountB]) sorted by
// UID and paired by index: UIDs[i] and Accounts[i] describe the same user.
// Callers must pass exactly two distinct *User values; this is enforced
// upstream (room-service capacity check + room-worker counterpart fetch).
func BuildDMParticipants(a, b *User) (uids, accounts []string)
```

### 3.3 Where they are set

Forward-only. No migration for existing DM/botDM rooms — any consumer that filters on `uids`/`accounts` must tolerate absent values for legacy rooms.

Three call sites:

1. **`processCreateRoomDM`** ([room-worker/handler.go](../../../room-worker/handler.go)) — after the existing `GetUser(counterpart)` and `BulkCreateSubscriptions`, call a new store method `UpdateDMParticipants(ctx, roomID, uids, accounts)` to `$set` the two fields on the already-persisted room.
2. **`processCreateRoomBotDM`** ([room-worker/handler.go](../../../room-worker/handler.go)) — same pattern; the bot's `ID` and `Account` populate one slot of the pair.
3. **`handleSyncCreateDM`** ([room-worker/handler.go](../../../room-worker/handler.go)) — already fetches both users before `CreateRoom`. Set `room.UIDs` and `room.Accounts` on the `&model.Room{...}` literal directly. No `UpdateDMParticipants` call needed on this path.

The two-write shape on the async path (one `CreateRoom`, one `UpdateDMParticipants`) is the accepted trade-off for keeping DM-specific logic inside the DM-specific handlers and leaving `processCreateRoom`'s collision-handling dispatch untouched. The `$set` is idempotent so JetStream redelivery converges; a worker crash between the two writes resolves on replay.

### 3.4 Store interface change

Add to `room-worker/store.go`:

```go
type SubscriptionStore interface {
    // ...existing methods unchanged...
    UpdateDMParticipants(ctx context.Context, roomID string, uids, accounts []string) error
}
```

MongoDB implementation: `UpdateOne({"_id": roomID}, {"$set": {"uids": uids, "accounts": accounts}})`. `MatchedCount == 0` is an error: the handler creates the room before this call, so a zero-match means the doc disappeared (race delete, wrong roomID, replica lag) — surface as a wrapped error so the handler returns it and JetStream retries.

`make generate SERVICE=room-worker` regenerates `mock_store_test.go` after the interface change.

### 3.5 Test plan

- `pkg/model/model_test.go` — extend the round-trip helper coverage to include `UIDs`/`Accounts` populated and `nil` cases.
- `pkg/model/room_test.go` — unit test `BuildDMParticipants` covering: sort by UID, accounts mirror permutation, a case where UID order ≠ Account order (proves pairing is honored), and exactly two outputs.
- `room-worker/handler_test.go` — extend existing DM/botDM create tests (`TestProcessCreateRoom_DM_BuildsTwoSubs`, `TestProcessCreateRoom_BotDM_HasIsSubscribed`, the relevant `TestHandleSyncCreateDM_*` cases) to assert the captured room carries the expected `UIDs`/`Accounts`. For the async paths, also assert `UpdateDMParticipants` was called with the correct sorted/paired args.
- `room-worker/handler_test.go` — add a sibling case for create-channel confirming `UIDs`/`Accounts` remain absent on a captured channel room (proves the `omitempty` guarantee).

## 4. Acceptance Criteria

**A. `room_members` correctness**
- A1. Add `Users=[u1], Orgs=[o1]` (o1 has `[u1, u2]`): 1 indiv doc for `u1`, 1 org doc for `o1`. No indiv for `u2`.
- A2. Add `Users=[], Orgs=[o1]` (o1 has `[u1]`): 1 org doc for `o1`. No indiv for `u1`.
- A3. Add `Users=[u1], Orgs=[]` to a room that already has an org member: 1 indiv doc for `u1` only.
- A4. Create channel `ResolvedUsers=[u1], ResolvedOrgs=[o1]` (o1 has `[u1, u2]`), requester `r`: indiv docs for `r` and `u1`, org doc for `o1`. No indiv for `u2`.
- A5. First-org transition: pre-existing direct-individual subs get individual docs materialized.
- A6. Subsequent org-bearing add (room already has org docs): backfill skipped; previously org-only users stay org-only.

**B. `members_added` Content**
- B1. `processAddMembers` `len(subs)==1`: `"{req} added {u} to the channel"`.
- B2. `processAddMembers` `len(subs)≥2`: `"{req} added members to the channel"`.
- B3. `publishChannelSysMessages` any `len(subs)-1 ≥ 1`: `"{req} added members to the channel"` (no single-name special case).

**C. Remove sys-messages**
- C1. Self-leave (full removal): `Type=member_left`, `UserAccount=req.Requester`, Content = `"{user} left the channel"`.
- C2. Removed-by-other (full removal): `Type=member_removed`, `UserAccount=req.Requester`, Content = `"{user} has been removed from the channel"`.
- C3. Org remove (any `len(toRemove)`, including 0): `Type=member_removed`, `UserAccount=req.Requester`, Content = `"{sectName} has been removed from the channel"`. `sectName` is correctly populated even when `toRemove` is empty.
- C4. Demote-only individual remove: no sys-message published.
- C5. Org remove when some org members also have individual subs: sys-message in C3 still published.

**D. Negative**
- D1. `processAddMembers` requester lookup miss → permanent error, no sys-message.
- D2. `processAddMembers` requester has empty `EngName` or `ChineseName` → permanent error, no sys-message.
- D3. `processAddMembers` any added user has empty `EngName` or `ChineseName` → permanent error, no sys-message.
- D4. `processRemoveIndividual` target user has empty `EngName` or `ChineseName` → permanent error, no sys-message.
- D5. `processRemoveOrg` every member's `SectName` is empty → permanent error, no sys-message.

**E. Verification**
- `make lint SERVICE=room-worker` passes.
- `make test SERVICE=room-worker` passes with race detector and ≥80% coverage.
- `make generate SERVICE=room-worker` produces no diff against the regenerated `mock_store_test.go` (after the §3.4 interface change).

**F. DM Participant Fields (§3)**
- F1. Async DM create (`processCreateRoomDM`): the persisted room carries `UIDs = sort([requester.ID, other.ID])` and `Accounts` permuted to mirror that order so `UIDs[i]` and `Accounts[i]` describe the same user. Set via `UpdateDMParticipants` after the counterpart fetch.
- F2. Async botDM create (`processCreateRoomBotDM`): same shape as F1 with the bot's `ID`/`Account` in one slot of the pair.
- F3. Sync DM create (`handleSyncCreateDM`): both fields set on the initial `&model.Room{...}` literal — no `UpdateDMParticipants` call on this path. Same sort/pairing invariant as F1.
- F4. Channel create: `UIDs` and `Accounts` are absent from the BSON document (not stored as empty arrays). Verified by capturing the room passed to `CreateRoom` and confirming both fields are nil.
- F5. Pairing under non-aligned sort: a DM between user `{ID:"zzz", Account:"aaa"}` and user `{ID:"aaa", Account:"zzz"}` yields `UIDs=["aaa","zzz"]` and `Accounts=["zzz","aaa"]` — `UIDs[0]/Accounts[0]` and `UIDs[1]/Accounts[1]` each describe the same user. Independently sorting both arrays would have produced `Accounts=["aaa","zzz"]`, which is incorrect.
- F6. JetStream redelivery: `UpdateDMParticipants` is `$set`, so a replayed `processCreateRoomDM` after a partial-state crash converges to the same final document.
- F7. Backward compatibility: a pre-existing DM room without `uids`/`accounts` is not modified by code paths other than `processCreateRoomDM`/`processCreateRoomBotDM` on a fresh create. No migration job runs.
