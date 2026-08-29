# botDM effective room type: bot-viewer DMs and p_admin — design

**Date:** 2026-08-29
**Status:** Approved
**Services:** user-service, history-service, search-service, room-service, room-worker (+ `pkg/model`, docs)

## Problem

A bot can log into the frontend with its auth token and act as a user. From the bot's
own point of view, its DM with a human is an ordinary conversation and belongs in the
common chat section of the sidebar — not the App section. The frontend places a row
using `Subscription.RoomType`, so the server has to report the right type.

Two independent defects block this today.

### 1. The bot's own rows never reach the client

`room-worker/handler.go:1393-1394` creates the botDM pair with the requester (human)
subscribed and the bot not:

```go
preRead(newSub(…, requester, room, nil, bot.Account,       true,  acceptedAt), acceptedAt),
        newSub(…, bot,       room, nil, requester.Account, false, acceptedAt),
```

`inbox-worker/handler.go:737` applies the same rule cross-site — `subscriptionIsSubscribed`
returns false whenever the subscriber is bot-like. Every read gate then filters those
rows out:

| Gate | Site | Effect on the bot's own row |
|---|---|---|
| `AggregateSubscriptions` `current` | `user-service/mongorepo/subscriptions.go:276-279` | dropped (`isSubscribed:true` required) |
| `AggregateSubscriptions` `rooms` | `:281` | dropped (`roomType ∈ {dm, channel}`) |
| `AggregateSubscriptions` `apps` | `:283-284` | dropped (`isSubscribed:true` required) |
| `activeSubscriptionFilter` (badge count) | `:777-779` | dropped |
| `GetDMSubscription` | `:718` | dropped (`roomType:"dm"` hard match) |
| thread unread summary | `user-service/mongorepo/threadsubscriptions.go:63-67` | dropped |
| user thread list | `history-service/internal/mongorepo/pipelines.go:71-75` | dropped |

So a bot logging in sees an empty sidebar, an empty badge count, and no threads.

### 2. `p_admin` is classified as a bot

`room-service/helper.go:178-185` and its mirror `room-worker/handler.go:1698-1704`
classify a single-counterpart DM as `botDM` when the counterpart is `.bot` **or** the
`p_admin` platform-admin pseudo-account. `p_admin` is a human-operated account with a
user record, so its DMs render in the App section with a failed app lookup instead of
in the chat section with `hrInfo`.

This also breaks room creation outright: `handleCreateRoomDMOrBotDM`
(`room-service/handler.go:293-300`) requires an app document for a `botDM` and returns
`errBotNotAvailable` when there is none. `p_admin` has a user record but no app
(`pkg/model/user.go:141-145`), so `room.create` on a `p_admin` DM fails.

## Requirements

1. A bot viewing its own DM with a human sees `roomType="dm"` with the counterpart's
   `hrInfo`, in the subscription list, badge count, thread list, `getDM`, `getByRoomID`,
   `getChannels` and search results.
2. A human's DM with `p_admin` renders as `roomType="dm"` with `hrInfo`, for every
   viewer. `p_admin` viewing its own DMs behaves like any user.
3. A human who unsubscribed from a real `.bot` app keeps that room hidden — the
   existing soft-toggle semantics must not regress.
4. A bot's DM with another bot stays `botDM` and stays subject to the `isSubscribed`
   gate.
5. New user↔`p_admin` DMs are created with `roomType="dm"`. Legacy rooms already stored
   as `botDM` are handled by the read-time mapping; no migration.
6. `p_admin` remains bot-like everywhere else — it still cannot join or own a channel,
   still counts toward `appCount`, still uses bot avatar/pin/mention handling.

## Rejected approaches

- **Gate the mapping on the viewer** (`IsBot(viewer) && !IsBot(counterpart)`). Satisfies
  requirement 1 but not 2: a human's `p_admin` DM has a non-bot viewer, so it would stay
  in the App section. Adding `p_admin` to the viewer test then fails requirement 3's
  symmetry and needs a second predicate.
- **Ignore the `isSubscribed` filter when the requester is a bot.** Necessary but
  insufficient — `type=rooms` excludes botDM by *room type*, not by `isSubscribed`, so
  the rows still never arrive in that bucket. It is also harmful in `type=apps`, where
  dropping the gate puts the bot's human DMs into the App section. And a `.bot`-only
  requester test misses `p_admin`, whose rows carry the same `isSubscribed=false`.
- **Migrate stored `botDM` rows to `dm`.** Explicitly out of scope; the read-time
  mapping makes legacy rows render correctly without touching data.
- **Make `p_admin` an ordinary account everywhere** (15 call sites, 9 services). Changes
  channel membership, ownership, `appCount`, avatars and pins, and needs a backfill.
  Not required by the goal.

## Design

### The predicate: an app room is a botDM with a `.bot` counterpart

One rule replaces every viewer-specific special case:

> A `botDM` is an **app room** only when the counterpart account carries a `.bot`
> suffix. Otherwise it is an ordinary DM: it renders as `dm` with `hrInfo`, and the
> `isSubscribed` gate does not apply to it.

The counterpart account is already on every subscription row as `name` — the
per-subscriber display name that is the bot account for the human's row and the human
account for the bot's row. No viewer parameter is needed, because the two sides of a
botDM carry different `name` values and therefore classify independently, which is
exactly the asymmetry the feature wants.

| Viewer | Counterpart | Effective type | `isSubscribed` gate |
|---|---|---|---|
| user | `weather.bot` | `botDM` + `app` | applies — an unsubscribed app stays hidden |
| user | `p_admin` | `dm` + `hrInfo` | skipped |
| `weather.bot` | user | `dm` + `hrInfo` | skipped |
| `p_admin` | user | `dm` + `hrInfo` | skipped |
| `weather.bot` | `other.bot` | `botDM` + `app` | applies |

Requirement 3 holds because the only rows a human ever unsubscribes from are real apps,
which always have a `.bot` counterpart and therefore keep their gate. `p_admin` has no
app document, so no unsubscribe toggle exists for it to bypass.

### `pkg/model`

Two exported helpers in `pkg/model/subscription.go`, the single source of truth for the
rule:

```go
// IsAppRoom reports whether a room of type t with counterpart account name is an
// app room — a botDM whose counterpart is a real ".bot" app. A botDM with any
// other counterpart (a human, or the p_admin pseudo-account) is an ordinary DM
// from the subscriber's point of view.
func IsAppRoom(t RoomType, name string) bool {
	return t == RoomTypeBotDM && IsBot(name)
}

// EffectiveRoomType is the room type a subscription is presented to its own
// subscriber as: a botDM that is not an app room renders as dm. Every other
// type is returned unchanged.
func EffectiveRoomType(t RoomType, name string) RoomType {
	if t == RoomTypeBotDM && !IsBot(name) {
		return RoomTypeDM
	}
	return t
}
```

Mongo filters cannot call Go, so the same rule is expressed once as a reusable filter
fragment beside them (see below) rather than re-spelled at each site.

### `pkg/pipelines`

`pkg/pipelines` already owns the bot-account regex used by room-service
(`member.go:19-23`) and already imports `bson`. Add the two subscription-filter
fragments to its existing `subscription.go` so user-service and history-service share
one definition:

```go
// AppRoomFilter matches subscription rows that are app rooms — botDM with a
// ".bot" counterpart. NonAppRoomFilter is its complement over botDM rows.
func AppRoomFilter() bson.M      // {roomType: "botDM", name: {$regex: `\.bot$`}}
func NonAppRoomFilter() bson.M   // {roomType: "botDM", name: {$not: {$regex: `\.bot$`}}}
```

The regex is evaluated per candidate document, never as the index-driving term: every
call site already leads with a selective `u.account` (list, badge) or `(u.account,
roomId)` point read (thread joins), so this adds no index pressure.

### user-service

**`mongorepo/subscriptions.go`** — the type buckets follow the *effective* type:

| Bucket | Before | After |
|---|---|---|
| `current` | `dm\|channel` OR `botDM AND isSubscribed` | `dm\|channel` OR `AppRoomFilter AND isSubscribed` OR `NonAppRoomFilter` |
| `rooms` | `dm\|channel` | `dm\|channel` OR `NonAppRoomFilter` |
| `apps` | `botDM AND isSubscribed` | `AppRoomFilter AND isSubscribed` |

`activeSubscriptionFilter` (`:770`) takes the same `current` shape, so the badge count
and the list keep selecting identical rows — the invariant its own comment names.

`GetDMSubscription` (`:716`) matches `roomType:"dm"` OR `NonAppRoomFilter`. The
`$limit: 1` short-circuit stays valid: `(account, name)` still identifies at most one
room, since a given pair has one DM room whatever its stored type.

**`mongorepo/threadsubscriptions.go:63-67`** — the join gate becomes "keep the row
unless it is an app room that is not subscribed", i.e. `$or: [NOT AppRoomFilter,
{isSubscribed: true}]`.

**`service/subscriptions.go`** — `buildListItems` and `distinctListNames` switch on
`model.EffectiveRoomType(subs[i].RoomType, subs[i].Name)`. A row whose effective type is
`dm` takes the DM branch: a `DMSubscription` with `hrInfo` keyed on `Name`, no app
lookup, no app-name swap, and `base.RoomType` set to `dm` before it is marshalled.
`GetDM` normalizes `RoomType` the same way. This covers `subscription.list` (NATS),
`GET /api/v1/subscriptions`, `subscription.getChannels` and `subscription.getByRoomID`,
which all share the seam.

**`service/threads.go`** — `enrichThreadPage` and `distinctDMAndBotNames` use the same
helper against each row's `RoomName` (which holds the counterpart account for dm/botDM
rows), attach `hrInfo`, and rewrite `RoomType` to `dm`.

### history-service

`internal/mongorepo/pipelines.go:71-75` — the membership `$lookup` gate becomes `$or:
[NOT AppRoomFilter, {isSubscribed: true}]`, matching user-service's thread gate. The
sub-pipeline already projects `name` and `roomType`, so the row reaches user-service
with everything `enrichThreadPage` needs; the display mapping stays in user-service and
history-service returns the stored type unchanged.

### search-service

`enrich.go:60-72` and `:112-131` branch on `meta.RoomType` to choose an app lookup over
an HR lookup. Both switch on `model.EffectiveRoomType(meta.RoomType, meta.Name)`, so a
non-app botDM contributes its counterpart to `dmCounterparts`, gets `room.HRInfo` and a
`displayfmt`-composed name, and reports `room.Type = dm`. `AppInfo.IsSubscribed` is only
set on the app-room branch, so it stays absent for these rows — matching
`pkg/model/search.go:84`.

### room-service / room-worker

`determineRoomType` (`room-service/helper.go:178`) and `determineRoomTypeFromPayload`
(`room-worker/handler.go:1698`) drop the `IsPlatformAdminAccount` disjunct, so only a
`.bot` counterpart yields `botDM`:

```go
if model.IsBot(req.Users[0]) {
	return model.RoomTypeBotDM
}
return model.RoomTypeDM
```

Both must change together — room-worker's copy is the authority for the room document
written on the CREATE-ROOM consumer, room-service's for the synchronous validation
reply. A new user↔`p_admin` DM is then a `dm` room, which also skips the
`errBotNotAvailable` app check that fails today.

Nothing else changes: `filterBots` (`helper.go:154`), the `errBotInChannel` guard
(`handler.go:225`), the role rules (`:694`, `:747`) and `newSub`'s `u.isBot` flag
(`room-worker/handler.go:1496`) keep the bot-like union, so `p_admin` still cannot be
added to a channel or made an owner (requirement 6).

`data-migration/oplog-collections-transformer/classify.go:43` keeps its current
classification — migrated `p_admin` DMs land as `botDM` and are handled by the read-time
mapping, per the no-migration decision.

## Unchanged by design

Scanned and confirmed viewer-agnostic — every `botDM` branch in these treats `dm` and
`botDM` identically, so an effective-type remap changes nothing:
`broadcast-worker` (`handler.go:289,365,569,620,691,907,1302`), `notification-worker`
(`routing.go:24`, `handler.go:427,472`), `media-service` (`handler.go:135`,
`avatar.go:22`), `message-gatekeeper` (`helper.go:27`), `inbox-worker`
(`handler.go:725`), `room-service`'s `legacyRoomTypes` (`handler.go:2498`).

`subscriptionIsSubscribed` (`inbox-worker/handler.go:737`) is left alone: it is a write
rule, and the read side no longer depends on its output for non-app rooms. Changing it
would alter existing rows' meaning without fixing anything the read gates do not already
cover.

## Error handling

Every lookup this touches is already best-effort and stays that way. A failed HR lookup
(`lookupHRInfo`, `lookupThreadHRInfo`) logs at warn and degrades to a row without
`hrInfo`; a failed app lookup degrades to the base name. Neither fails the request. The
`hrInfo` lookup for a `p_admin` counterpart will simply miss (no `users` document with
HR fields is guaranteed), and the row renders with the account as its name — the same
degradation path an unknown DM counterpart already takes.

## Testing

- **`pkg/model`** — table tests for `IsAppRoom` and `EffectiveRoomType` across the five
  rows of the predicate table plus non-botDM types (`channel`, `discussion`, `dm`) and
  an empty name.
- **`pkg/pipelines`** — the two filter fragments match and reject the same account
  shapes the Go helpers do (`weather.bot`, `p_admin_ops`, `alice`, `p_qa_bob`).
- **user-service unit** — `buildListItems` and `enrichThreadPage`: bot-viewer row → `dm`
  + `hrInfo` + no `app`; human row with `.bot` counterpart → `botDM` + `app` + name
  swap; `p_admin` counterpart → `dm` + `hrInfo`; HR lookup failure → `dm` without
  `hrInfo`. `GetDM` normalizes the type.
- **user-service integration** (testcontainers) — the three buckets and
  `activeSubscriptionFilter` against seeded rows covering all five predicate cases,
  including the regression: a human's unsubscribed `.bot` app stays hidden in all three
  buckets.
- **history-service integration** — the thread list returns a bot's rows in a human DM
  and still hides a human's unsubscribed app.
- **search-service unit** — `enrich` routes a non-app botDM to the HR lookup and reports
  `room.Type = dm` with no `appInfo`.
- **room-service / room-worker unit** — `determineRoomType` and
  `determineRoomTypeFromPayload` classify `p_admin` as `dm`, `.bot` as `botDM`, `p_qa_*`
  as `dm`, and multi-user requests as `channel`.

## Docs

`docs/client-api.md` and its derived views `docs/client-api/request-reply.md` and
`docs/client-api/events.md` describe `roomType` on `subscription.list`,
`subscription.getDM`, `subscription.getByRoomID`, `subscription.getChannels`,
`thread.list` and the search response. All must state the effective-type rule in the
same PR, per CLAUDE.md.

## Rollout

No migration and no deploy ordering constraint. Every change is read-side except the
room-type classification, which affects only newly created rooms; a mixed fleet during
rollout serves some rows with the old type and some with the new, and both render
correctly on any client that reads `roomType`. Rolling back is a redeploy — no data is
rewritten.
