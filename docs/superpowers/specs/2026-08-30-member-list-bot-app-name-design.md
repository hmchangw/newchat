# member.list bot app-name enrichment

## Problem

`member.list` (room-service) enriches members from two sources: the
`room_members` collection, falling back to `subscriptions` when a room has no
`room_members` document. Bot members are enriched inconsistently:

- **subscriptions path** — `attachUserDisplayNames` already partitions accounts
  by the `.bot` suffix and batch-loads `apps.name` into `RoomMemberEntry.Name`.
  It ignores `subscriptions.u.isBot`, so a bot whose account lacks the suffix is
  looked up in `users`, finds nothing, and returns bare.
- **room_members path** — `getRoomMembers` joins only `users`. Bot members get
  no app name at all.

The field is also named `name`, which reads as a generic display name next to
`engName` / `chineseName` / `orgName` when it specifically carries `apps.name`.

## Goals

1. Rename `RoomMemberEntry.Name` → `AppName` (JSON `appName`).
2. Populate `appName` for bot members on **both** lookup paths.
3. Recognise bots on the subscriptions path by `u.isBot` **or** the `.bot`
   suffix.
4. Add at most one extra database round-trip, and only when a returned page
   actually contains a bot member.

## Non-goals

- Enriching bot members in `member.add` responses (`buildAddedMembers`,
  room-worker) — out of scope, and its docs already state `appName` is omitted.
- Backward compatibility for the old `name` JSON key. The rename is a clean
  break; see Compatibility.

## Design

### Model

`pkg/model/member.go`:

```go
// AppName is the app's display name for bot members, from apps.name.
// Humans carry EngName/ChineseName instead. Display: appName ?? engName ?? account.
AppName string `json:"appName,omitempty" bson:"-"`
```

The field stays `bson:"-"` — it is a display value resolved per request, never
persisted.

### Shared enrichment helper

Both paths converge on one helper in `room-service/store_mongo.go`:

```go
func (s *MongoStore) attachAppNames(ctx context.Context, roomID string,
    members []model.RoomMember, botAccounts []string) error
```

It no-ops on an empty `botAccounts`, otherwise issues a single
`findAppsForDisplay` batch (`apps.assistant.name $in [...]`, covered by
`assistant_name_idx`, projected to `{name, assistant.name}`) and copies each
match onto the matching member's `AppName`.

Callers supply the bot-account list because the two paths detect bots
differently, and each already walks its rows once — so collection is free.

### room_members path (`getRoomMembers`)

The enriched branch already loops decoded rows to fold the `display`
sub-document onto each entry. Collect `.bot`-suffix accounts in that same loop,
then call `attachAppNames` alongside the existing `attachOrgDisplay`.

Detection is suffix-only (`model.IsBot`): `room_members` rows carry no `isBot`
field. Adding one would mean a schema change plus a backfill across every
existing membership document, which this change does not justify.

No `$lookup` is added. A correlated `$lookup` on `apps` would need `$expr` to
compare against the row's account, which cannot use `assistant_name_idx` — it
would degrade to a collection scan of `apps` per member row. CLAUDE.md forbids
`$lookup` for this reason, and the neighbouring `attachOrgDisplay` sets the
precedent for a Go-side batch instead.

### subscriptions path (`getRoomSubscriptions`)

- Add `u.isBot` to `roomMemberSubProjection`.
- Partition accounts while `subs` is still in scope, where the `IsBot` flag is
  readable: an account is a bot when `sub.User.IsBot || model.IsBot(account)`.
- `attachUserDisplayNames` takes the pre-partitioned human and bot slices,
  keeps the human `users` lookup, and delegates bots to `attachAppNames`.

`model.IsBot` (a `strings.HasSuffix`) replaces room-service's local
`botAccountPattern` regexp, whose only call site this is. The compiled
`botAccountPattern` var and `store_mongo.go`'s `regexp` import both become dead
and are removed. The `botAccountRegex` **string constant** stays —
`store_mongo.go:1743` embeds it in a `$regexMatch` aggregation expression.

### Known asymmetry

A bot whose account does not end in `.bot` is enriched on the subscriptions
path but not on the room_members path, which has no flag to read. Accepted:
`.bot` is the convention every bot account follows today (`botAccountRegex`
comments record that all non-suffix service accounts are `p_` humans), and the
`isBot` branch exists as defence in depth, not as a supported second naming
scheme.

## Performance

| Scenario | Extra queries |
|---|---|
| Room with no bot members (the common case) | 0 |
| Page containing bot members, either path | 1 |

The added query is a single indexed `$in` bounded by the page size, not by room
size. Accounts are not de-duplicated before the `$in`: the unique
`subscriptions (roomId, u.account)` index makes repeats impossible on the
fallback path, and a repeat on the room_members path would cost one extra index
bound — cheaper than the staging map that would prevent it. Nothing on the human
path changes: same pipeline, same projections, same index use.

## Compatibility

`Member.Name` is written at exactly one site (`store_mongo.go:820`) and read
only by two room-service integration assertions. No other service, tool, or
frontend in the repo reads `member.name`, so the rename is contained. Clients
consuming the `name` key see it become `appName`; `docs/client-api.md` and its
derived views are updated in the same PR per CLAUDE.md.

## Testing

TDD, red before green. Integration tests (`room-service/integration_test.go`,
build tag `integration`, testcontainers Mongo):

| Case | Asserts |
|---|---|
| Bot on room_members path | `appName` resolved from `apps.name` |
| Bot on subscriptions path, `.bot` suffix | `appName` resolved |
| Bot on subscriptions path, `isBot=true`, non-suffix account | `appName` resolved — proves the flag branch |
| Mixed room, both paths | humans get `engName`, bots get `appName`, neither leaks the other |
| Bot with no `apps` document | `appName` empty, no error |
| Human-only room | unchanged behaviour, no apps query |

Plus a `pkg/model` JSON round-trip for the renamed field.
