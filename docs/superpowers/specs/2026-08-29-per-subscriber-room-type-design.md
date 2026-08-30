# Per-subscriber room type, written at creation — design

**Date:** 2026-08-29
**Status:** Approved
**Services:** room-service, room-worker, inbox-worker, bot-room-service, user-service, history-service, search-service (+ `pkg/model`, `pkg/pipelines`, docs)

**Supersedes:** `2026-08-29-botdm-effective-room-type-design.md`. That design derived the
subscriber's room type at read time, in every gate and render switch. This one writes it
once, at subscription creation.

## Problem

A bot signs into the client with its auth token and acts as a user. Its DM with a person
belongs in the common chat section, so `Subscription.RoomType` on the bot's own row must
say `dm`. Three things stand in the way.

**1. One room type for two asymmetric sides.** `room-worker`'s `newSub` stamps
`RoomType: room.Type` on both rows of a DM pair, so a bot↔human room writes `botDM` to
both. But the two sides are not the same relationship: the human faces an app, the bot
faces a person. The row already records that asymmetry in `name` (the counterpart
account) — the type simply ignores it.

**2. The read-time remap leaves the DB and the wire disagreeing.** The superseded design
corrected the type in every reader. That works, but the stored value stays wrong, so the
rule has to be re-applied at ten sites, expressed twice (Go predicate and Mongo filter
fragment), and every future reader has to remember it. A `subscription.update` event and
the row it describes report different types.

**3. Two write paths omit the fields entirely.** `bot-room-service`'s `dm.ensure` upserts
both subscriptions through a `$setOnInsert` that writes neither `roomType` nor `name`
(`bot-room-service/store_mongo.go:100-110`). Rows with no `roomType` match no list
bucket, so a DM the bot platform creates is invisible to **both** parties. No read-time
remap can fix that — there is nothing to remap.

## Requirements

1. A subscription row's `roomType` is the room as **its own subscriber** sees it, written
   at creation. The bot's row in a bot↔human DM is `dm`; the human's is `botDM`.
2. `subscription.update` reports the same `roomType` the row stores. No divergence.
3. The room document keeps a single type: `botDM` when either participant is a `.bot`
   app, `dm` otherwise.
4. Every read gate and render switch works off the stored type with **no remap** — the
   pre-existing `isSubscribed` filters are restored untouched.
5. A human who unsubscribed from a real app keeps that room hidden.
6. `bot-room-service`'s `dm.ensure` writes `roomType` and `name` on both rows.
7. A bot creating a DM with a person succeeds; the app-availability gate does not reject it.
8. Defence in depth: a `subscription.update` carrying `botDM` with a non-`.bot`
   counterpart is corrected to `dm` and enriched with `hrInfo` at the publish site.

## Non-goals

- **Live event delivery to bots.** `broadcast-worker` skips bot-like accounts in every
  fan-out — `publishDMEvents:1171`, `publishMutation:910`, `publishThreadMetadata:693`,
  `threadFanOutAccounts:1329` — keyed on the **account**, not the room type, for `dm`
  rooms as much as `botDM` ones. A bot signed into the client therefore sees a correctly
  typed and placed sidebar row but receives no live messages; it observes new activity
  only on refetch. `p_admin` is skipped by the same union. Deliberately out of scope: it
  is a separate behavioural change across four fan-out sites, and BP already consumes
  those messages through its own server-side path.
- Backfilling existing rows. Pre-launch; the data will be dropped.

## Design

### Two functions, both write-time

```go
// DMRoomType is the room document's type for a two-party DM: botDM when either
// participant is a ".bot" app.
func DMRoomType(a, b string) RoomType {
	if IsBot(a) || IsBot(b) {
		return RoomTypeBotDM
	}
	return RoomTypeDM
}

// SubscriptionRoomType is one row's type — the room as its own subscriber sees
// it: botDM when that row's counterpart is a ".bot" app, dm otherwise.
func SubscriptionRoomType(counterpart string) RoomType {
	if IsBot(counterpart) {
		return RoomTypeBotDM
	}
	return RoomTypeDM
}
```

The room doc holds one type; the two rows may differ, because each keys on its own
counterpart:

| Pair | Room doc | Row A | Row B |
|---|---|---|---|
| alice ↔ weather.bot | `botDM` | alice → `botDM` | weather.bot → **`dm`** |
| alice ↔ bob | `dm` | `dm` | `dm` |
| weather.bot ↔ sales.bot | `botDM` | `botDM` | `botDM` |
| alice ↔ p_admin | `dm` | `dm` | `dm` |

`p_admin` is not a `.bot` account and owns no app document, so it is an ordinary DM
partner on both sides. It stays bot-like everywhere else (channel membership, ownership,
`appCount`, avatars, pins) — only DM classification changes.

`inbox-worker`'s `subscriptionIsSubscribed(roomType, u)` needs no change: fed the
per-subscriber type it already returns `true` for the human's app row (`botDM` + human
subscriber) and `false` for the bot's `dm` row.

### Write sites

| # | Site | Change |
|---|---|---|
| 1 | `room-service/helper.go` `determineRoomType` | `DMRoomType(req.RequesterAccount, req.Users[0])` |
| 2 | `room-service/handler.go:294` | Skip the app-availability gate when the requester is a bot |
| 3 | `room-worker` `determineRoomTypeFromPayload` | Mirror of 1 |
| 4 | `room-worker` `newSub` + DM builders | Per-subscriber type; `buildDMSubs`/`buildBotDMSubs` collapse into one builder |
| 5 | `inbox-worker/handler.go:300` | `RoomType: model.SubscriptionRoomType(name)` |
| 6 | `bot-room-service` `dm.ensure` | Write `roomType` and `name` on both rows |

Site 2 is required by site 1: `handleCreateRoomDMOrBotDM` resolves the app for
`other.Account`, so a bot-initiated `botDM` would look up an app for the *human* and
fail with `errBotNotAvailable`. The gate exists to stop a user opening a DM with a
missing or disabled app; a bot initiating is already authenticated and has no app to
validate on the far side.

`user-service`'s `setAppSubscription` needs no change of its own — it reaches
subscription creation only through `rooms.CreateDMRoom`, which lands on site 4. That is
where the app-subscribe pair gets its `botDM` (human) / `dm` (bot) split.

### Reads: restored, not adapted

Every gate and render switch returns to its pre-PR form and reads the stored type
directly:

- `user-service/mongorepo/subscriptions.go` — the three list buckets,
  `activeSubscriptionFilter`, `dmMatch`
- `user-service/mongorepo/threadsubscriptions.go` — the join gate; the `name` projection
  and `ThreadUnreadRow.Name` are removed with it
- `user-service/service/` — `buildListItems`, `distinctListNames`, `GetDM`,
  `enrichThreadPage`, `distinctDMAndBotNames`, `threadunread`
- `history-service/internal/mongorepo/pipelines.go` — the membership gate
- `search-service/enrich.go` — both enrichment switches
- `pkg/pipelines` — `AppRoomFilter`, `UnsubscribedAppFilter`, `botSuffixRegex` deleted; no
  query needs them

The bot's row is `dm`, so the original `current` branch `roomType ∈ {dm, channel}` — which
carries no `isSubscribed` condition — admits it. `rooms` admits it; `apps`
(`roomType:"botDM", isSubscribed:true`) excludes it. The human's app row is unchanged, so
the soft-unsubscribe toggle and `GetAppSubscription` / `SetAppSubscribed` (both filtering
`roomType:"botDM"`) still resolve the human's row only.

### The event defence

`model.IsAppRoom` and `model.EffectiveRoomType` survive, used at exactly three places:

- `room-worker` `publishSubscriptionAdded` — stamps the effective type on the event's
  subscription copy
- `room-service` `publishSubscriptionUpdate` — same, for `read` / `mute_toggled` /
  `favorite_toggled` / `section_moved` / `opened` / `role_updated`
- `room-worker` `resolveSubUpdateCounterpart` — routes `hrInfo` vs `appInfo` by
  `IsAppRoom`, so a corrected row also gets the right enrichment

On correctly written data the stamp is an identity. It fires only if a row is corrupt.

## Error handling

Unchanged. The HR and app lookups behind `subscription.update` remain best-effort: a
failure logs at warn and omits `hrInfo` / `appInfo` rather than failing the publish. The
new functions are pure and total — no error path.

## Testing

- **`pkg/model`** — table tests for `DMRoomType` and `SubscriptionRoomType` across
  `.bot`, human, `p_admin`, QA `p_`, and empty accounts.
- **`room-worker`** — the stored pair for each of the four pairings; the event's
  `roomType` equals the stored row's; `hrInfo` on the bot's row and `appInfo` on the
  human's.
- **`room-service`** — `determineRoomType` for each pairing; a bot requester passes the
  app gate; a user requester with a missing app still gets `errBotNotAvailable`.
- **`inbox-worker`** — a cross-site row's type derives from its counterpart.
- **`bot-room-service`** — `dm.ensure` writes `roomType` and `name` on both rows.
- **`user-service` integration** — with rows written the new way and the **restored**
  filters, the bot's row appears under `rooms` and `current`, never under `apps`, and a
  human's unsubscribed app stays hidden in all three.

## Rollout

No migration and no deploy ordering constraint. Reads go back to their original form, so
a reader deployed before or after a writer behaves identically on correctly written rows.
Rows written by an old writer during rollout carry the room's type on both sides; the
`subscription.update` stamp corrects the event, and the row itself is pre-launch data due
to be dropped.
