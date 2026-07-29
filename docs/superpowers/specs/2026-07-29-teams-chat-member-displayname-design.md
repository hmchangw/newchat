# Carry `teams_user.displayName` onto chat members — Design

**Date:** 2026-07-29
**Status:** Approved

## Purpose

Chat members stored on `teams_chat.members[]` carry only the AAD user id, the
resolved account, and the history-visibility cutoff. `teams_user` already holds
each user's Graph `displayName`, but it stops there — nothing downstream of the
Teams migration pipeline can name a member without a second lookup.

This change adds `displayName` to the stored member subdocument, populated from
`teams_user` by both producers, and carries it through the room-creation event.

Pipeline position: `teams-user-sync` → `teams-chat-sync` → **`teams-chat-member-sync`**
→ `teams-room-creation` → `room-worker`.

## Model changes (`pkg/model`)

`TeamsChatMember` (`pkg/model/teams.go`) gains one field, after `Account`:

```go
type TeamsChatMember struct {
	ID                          string    `json:"id" bson:"id"`
	Account                     string    `json:"account" bson:"account"`
	DisplayName                 string    `json:"displayName" bson:"displayName"`
	VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime" bson:"visibleHistoryStartDateTime"`
}
```

`TeamsRoomCreateMember` (`pkg/model/teamsroom.go`) gains the identical field in
the identical position. `teams-room-creation` converts the stored member into
the wire member with a direct struct conversion
(`model.TeamsRoomCreateMember(m)`), which is only legal while the two structs
stay field-identical — keeping them in lockstep preserves that deliberate
compile-time coupling.

## `teams-chat-member-sync`

The service resolves each member's account from `teams_user` through a batched,
process-wide cache. That resolver now returns two values per user instead of
one.

**`store.go`** — replace the account-only lookup with a two-field one:

```go
// teamsUserRef is the projection of one teams_user consumed by member build.
type teamsUserRef struct {
	account     string
	displayName string
}

type TeamsUserStore interface {
	// UsersByIDs returns userId->ref for the ids present in teams_user;
	// ids without a record are absent from the map.
	UsersByIDs(ctx context.Context, ids []string) (map[string]teamsUserRef, error)
}
```

The absent-id contract is unchanged: a user with no `teams_user` record is
absent from the returned map, which the cache stores as a zero-value ref, so the
member is written with both `account` and `displayName` empty — exactly the
behaviour account resolution has today.

**`store_mongo.go`** — `AccountsByIDs` becomes `UsersByIDs`; the projection goes
from `{_id, account}` to `{_id, account, displayName}`. Still one query, still
served by the read client.

**`syncer.go`** — `accountCache.cache` becomes `map[string]teamsUserRef`. The
batching, the mutex discipline, and the cache-the-miss semantics are unchanged;
only the cached value type widens. `buildMembers` sets `DisplayName` from the
same resolved ref it already reads `Account` from.

**`make generate`** regenerates `mock_store_test.go` for the new interface.

## `teams-chat-sync`

`teams-chat-sync` finalizes oneOnOne and small chats inline, building
`TeamsChatMember` values itself from its own `teams_user` cache. Left untouched,
those chats would store an empty `displayName` while deferred chats stored a
populated one — the same field meaning two different things depending on which
producer wrote it.

- `cachedUser` gains a `displayName` field.
- `ListUsers`' projection adds `displayName` (currently `{_id, siteId, account, from}`).
- The cache fill carries it, and `buildChat` sets it on each member.

## `teams-room-creation` and `room-worker`

No code change beyond the model. `teams-room-creation` already projects
`members: 1` (the whole subdocument), and its direct struct conversion carries
the new field automatically, so `TeamsRoomCreateEvent` payloads gain a
`displayName` per member.

This is an additive JSON field on an internal cross-service event. `room-worker`
does not read it and is unaffected — an old consumer against a new payload
ignores the field, a new consumer against an old payload sees `""`.

`docs/client-api.md` is not touched: `TeamsRoomCreateEvent` is an internal
migration-pipeline event, not a `chat.user.*` client-facing request/reply or a
server→client event.

## Testing

Red-Green-Refactor throughout; tests are written and observed failing before the
implementation lands.

**`pkg/model/model_test.go`** — add `DisplayName` to the existing `TeamsChat`
JSON and BSON round-trips and to the `TeamsRoomCreateEvent` round-trip, so the
new tags are covered in both codecs.

**`teams-chat-member-sync`**
- Rework the `accountCache` tests (batching, only-queries-uncached,
  concurrent-resolve-no-race) and `TestBuildMembers_ResolvesAllViaLookup` onto
  `UsersByIDs`/`teamsUserRef`.
- Add: a member absent from `teams_user` yields both `Account` and
  `DisplayName` empty.
- Add: `displayName` reaches `SetMembersSynced` — asserted on the members
  argument captured from the mocked store.
- Update the mock expectations in `worker_test.go` and `log_test.go`.
- `integration_test.go`: `TestMongoStore_AccountsByIDs` becomes
  `TestMongoStore_UsersByIDs`, asserting both resolved fields and that a
  missing id is absent from the map.

**`teams-chat-sync`**
- `buildChat` test asserts `DisplayName` is carried from the cache.
- `ListUsers` integration test asserts `displayName` survives the projection.

Coverage floor for both services stays at the repo-wide 80% minimum; these are
edits to already-covered paths, so no new uncovered branches are introduced.

## Non-goals

- **No backfill.** The field is additive. `teams_chat` documents written before
  this change keep members without `displayName`, which unmarshals as `""`, and
  fill in whenever the chat is next synced. No consumer reads the field today,
  so stale-empty is harmless. A forced re-sync (flipping `needMemberSync=true`
  across the collection) or a Mongo-only backfill can be done later if a
  consumer ever needs full coverage.
- **No new consumer.** Nothing reads `displayName` yet — in particular
  `room-worker`'s `resolveMember` still provisions new users without a name.
  Wiring that up is separate work.
- **No Graph change.** `msgraph.ChatMemberDetail` is unchanged; `displayName`
  comes from `teams_user`, which `teams-user-sync` already populates from Graph.
