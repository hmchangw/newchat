# Re-subscribe publishes no subscription.update — design

**Date:** 2026-08-13
**Status:** Approved
**Scope:** `subscription.setAppSubscription`'s re-subscribe path only. The
sibling gap on unsubscribe (no `"removed"` event) and the federation gap
(`SetAppSubscribed` never replicates cross-site) are documented follow-ups,
not part of this change.

## Problem

`SetAppSubscription` (user-service/service/apps.go:12) has three outcomes for
`subscribed: true`:

```go
existing, err := s.subs.GetAppSubscription(c, account, botName)
...
if existing == nil {
    if _, err := s.rooms.CreateDMRoom(c, account, botName, model.RoomTypeBotDM); err != nil {
        ...
    }
    return &models.OKResponse{Success: true}, nil
}
if err := s.subs.SetAppSubscribed(c, account, botName, true, false); err != nil {
    ...
}
return &models.OKResponse{Success: true}, nil
```

The fresh path (`existing == nil`) issues the `RoomCreateDMSync` RPC to
room-worker, whose `serverCreateDM` handler fans out a
`SubscriptionUpdateEvent{Action:"added"}` on
`chat.user.{account}.event.subscription.update` before replying
(room-worker/handler.go:2066 → `publishSubscriptionAdded` :2168).

The re-subscribe path (`existing != nil` — the user previously subscribed,
then unsubscribed, and subscribes again) is a Mongo-only
`UpdateOne {$set: {isSubscribed: true, muted: false}}`
(user-service/mongorepo/subscriptions.go:457-465). **No event is published
anywhere in that call path.** Other connected clients — including the same
user's other tabs/devices — get no real-time signal; the sidebar only picks
the bot room back up on the next full `subscription.list` refetch. The symptom
therefore presents as a delayed/missing sidebar update rather than a hard
failure.

`GetAppSubscription` deliberately has no `isSubscribed` condition in its
filter (subscriptions.go:451-454) — an unsubscribed row is still found, which
is exactly why the re-subscribe branch is taken instead of `CreateDMRoom`.

## Corrections to the issue

The issue text was machine-drafted; two claims needed correction and one
needed sharpening:

1. **"user-service has no publish capability wired for this event type"** —
   half-true. The *event construction* logic is missing, but the pipe is not:
   `subscription.update` is delivered as ephemeral, best-effort core NATS
   (room-worker publishes it with an empty msgID → `nc.PublishMsg`,
   room-worker/main.go:211-219), and user-service's `clientPub`
   (`publisher.CorePublisher`, user-service/publisher/core.go) is exactly that
   delivery pattern — its doc comment literally says "same delivery pattern as
   room-worker's subscription.update". It already fans out
   `settings.update` (service/settings.go:119) and `chatlist.update`
   (service/chatlist.go:192).
2. **Option 3's premise — "event-publishing logic owned by room-worker as
   it's for every other sub state transition" — is false.** room-worker
   publishes only `added` and `removed`. room-service publishes the other six
   actions of the very same subject in-handler after its own Mongo writes:
   `role_updated` (room-service/handler.go:796), `read` (:1384),
   `mute_toggled` (:2130), `favorite_toggled` (:2197), `section_moved`
   (:2313/:2334), `opened` (:2379). The repo norm is *the service that
   performs the write fans out the FE event*.
3. **"a new RPC from user-service" (option 1) is not actually needed** —
   room-worker already serves a synchronous RPC (`RoomCreateDMSync`,
   room-worker/main.go:245, its only request/reply registration), and its
   idempotent duplicate-room path already fans out `"added"` unconditionally
   (handler.go:2029→2066). The strongest form of option 1 is a `Reactivate`
   flag on that existing RPC, not a new one.

Everything else in the issue checked out: the re-sub path is a Mongo-only
`UpdateOne` with no publish; the `"added"` event's enrichment
(`resolveSubUpdateCounterpart`, room-worker/handler.go:2107) lives entirely in
room-worker; user-service does not import it; the sidebar converges only on
the next `subscription.list` refetch (the list pipelines filter botDMs on
`isSubscribed: true`, mongorepo/subscriptions.go:246-254).

## Options considered

### Option 1 — route the re-subscribe through room-worker

Two forms:

- **1a (replay):** keep the `SetAppSubscribed(true, false)` write, then call
  the existing `s.rooms.CreateDMRoom` again. Verified to work: the duplicate
  path reconciles to the existing room, `$setOnInsert` no-ops the sub insert,
  and the fan-out is built from a **post-write re-read**
  (handler.go:2061-2066), so the event carries `isSubscribed: true`. ~3 lines.
- **1b (flag):** add `Reactivate bool` to `model.SyncCreateDMRequest`;
  room-worker's duplicate path performs the `$set` itself and user-service
  deletes its re-subscribe branch. One event constructor forever; also fixes a
  latent wart (a replayed `serverCreateDM` after an unsubscribe currently fans
  out an `"added"` whose snapshot still says `isSubscribed: false`, because
  `$setOnInsert` preserves the doc).

Shared costs: every re-subscribe pays the full create path — a blocking Vault
`EnsureDEK` round-trip (handler.go:2017-2021), user lookups, a 5s RPC budget
(roomclient/client.go:18) — and re-subscribe availability becomes coupled to
room-worker *and* Vault, where today it is a single Mongo write. 1a
additionally sends a spurious duplicate `"added"` to the bot counterpart and,
for cross-site botDMs, re-publishes the inbox `member_added` once the
JetStream dedup window lapses; a cross-site inbox publish failure would fail
the whole resub RPC *after* the write committed (handler.go:2069-2071). 1b
additionally requires a `pkg/model` contract change, a room-worker store
method + two mock regens, reverses a documented design decision ("user-service
guards against genuine re-subscribe", handler.go:1977-1981), and has a
deploy-skew hazard: an old room-worker silently ignores the unknown
`Reactivate` field, so during rollout a resub returns success **without
persisting anything** — a silent-divergence failure class the current code
cannot produce.

### Option 2 — user-service publishes the `"added"` event itself (CHOSEN)

Keep the Mongo write exactly as is; after it succeeds, build the
`SubscriptionUpdateEvent{Action:"added"}` in user-service and publish it via
the existing `clientPub` on `subject.SubscriptionUpdate(account)`,
best-effort (log-and-swallow), mirroring the `settings.update` /
`chatlist.update` call sites.

It turned out materially cheaper than the issue anticipated, because the
enrichment room-worker owns is only needed in its botDM slice, and
user-service already holds every ingredient:

- The **full `model.App`** is already fetched at the top of the handler
  (apps.go:18) — a superset of room-worker's `{name,_id}` `GetApp` projection.
  `RoomName`/`AppInfo` reduce to `app.Name` (falling back to the bot account
  when empty, mirroring handler.go:2130-2137) and
  `{ID: app.ID, Name: app.Name, AssistantName: botName}`. The human/`hrInfo`
  branch of `resolveSubUpdateCounterpart` is dead here — resub is botDM-only
  by the repo filter (`roomType: "botDM"`, subscriptions.go:453).
- The **room-derived `Subscription.Room`** comes from the *existing*
  `GetSubscriptionByRoomID` repo method (already on the interface, already
  mocked) plus the *existing* `buildLocalRoom` helper
  (service/subscriptions.go:395) — the same code that shapes
  `subscription.list` rows, so the event matches what a refetch would return.
  No new repo method, no mock regen, no new `$lookup` call site.

### Option 3 — dedicated room-worker RPC for "publish the resub event" (DROPPED)

user-service writes, then synchronously asks room-worker to announce state it
didn't write. All three review lenses scored it far below the others
(3/4/3 vs 7/6/7 and 8/8/8): its founding ownership premise is refuted
(correction 2 above); it mints a permanent server-to-server surface (subject +
two `pkg/model` structs + client method + handler + mocks) whose only job is
to move a *best-effort ephemeral* publish behind a *synchronous, failable* 5s
hop; it has the most ways to lose the event (no-responders, timeout,
room-worker re-read) while fixing neither the duplicated branch nor the
dup-path wart; and it adds a cross-service read-after-write ordering
dependency plus rollout skew (every resub loses its event until room-worker
ships). It is strictly dominated: 1a achieves "room-worker constructs the
event" with zero new code, and option 2 achieves "no new failure modes" with
zero new surface.

## Why option 2 over option 1

Both survived review; the gap is real but option-2-ward on every lens:

| Lens | Option 1 (best form 1b) | Option 2 |
|---|---|---|
| Architecture | Single `"added"` constructor forever, but reverses the documented resub-guard split and moves one direction of the `isSubscribed` flag's writes into a second service | Matches the verified repo norm — write-owner publishes (room-service does this for six sibling actions; user-service for settings/chatlist) |
| Reliability | Couples resub to Vault + room-worker availability; 1b deploy-skew silently drops the write; cross-site inbox failure fails resub post-commit | Zero new failure orderings on the write path; the only addition is an in-process, post-write, log-and-swallow publish |
| Simplicity | `pkg/model` change + two services + two mock regens + a subtle 140-line shared handler | ~50 lines in one leaf service, existing repo method + existing helper, tests slot into the existing table |

Option 1's one durable advantage — the resub event can never drift from the
fresh-subscribe event — is fenced in option 2 by a shape-asserting unit test,
and the drift surface is narrow (botDM slice only). Against that stand
option 1's operational regressions on a working write path. Option 2 wins.

## Design

All changes in `user-service/service/apps.go` (plus docs):

1. The re-subscribe branch, after `SetAppSubscribed` succeeds, calls a new
   unexported `s.publishAppResubscribed(c, account, app, existing)` before
   returning. The RPC reply is not gated on it.
2. `publishAppResubscribed` (best-effort; every failure logs and returns):
   - `enriched, err := s.subs.GetSubscriptionByRoomID(c, account, existing.RoomID)`
     — error or miss → log, skip.
   - `room := buildLocalRoom(enriched)`; `nil` (soft-deleted `Del-` room) →
     skip. Publishing an "added" for a room `subscription.list` would drop is
     wrong; the anomalous state resolves on refetch.
   - Take `enriched.Subscription`, patch `IsSubscribed = true`,
     `Muted = false` (the enriched read runs on the secondary and may race the
     primary `$set`; the event must never ship the pre-write state), attach
     `Room`.
   - Build the event: `UserID: sub.User.ID`, `Action: "added"`,
     `RoomName: app.Name` (fallback `botName` when empty),
     `AppInfo: {ID: app.ID, Name: app.Name, AssistantName: botName}`,
     `HRInfo: nil`, `Timestamp: time.Now().UTC().UnixMilli()`.
   - `json.Marshal` + `s.clientPub.Publish(c, subject.SubscriptionUpdate(account), data)`,
     log-and-swallow — the same pattern as settings.go:119 / chatlist.go:192.
3. **Recipient set:** the re-subscriber only. Fresh creation fans out to both
   pair subs as an artifact of creating them; on resub the bot's subscription
   did not change, so no bot-side copy is sent.
4. No `pkg/model` changes, no interface changes, no mock regen, no new
   subjects.

### Event parity with the fresh-subscribe copy

| Field | Fresh (room-worker) | Resub (this change) |
|---|---|---|
| `userId` | `sub.User.ID` | same, from the enriched row |
| `subscription` | post-write re-read, `Room` attached via `subscriptionRoomFor` | secondary enriched read patched with the two written fields, `Room` via `buildLocalRoom` (the `subscription.list` shape) |
| `subscription.room.privateKey/keyVersion` | nil (keyless botDM pair) | absent unless the room doc carries an `encKey` (then it matches what `subscription.list` returns) |
| `subscription.room.previewMessage` | nil by design | nil (`buildLocalRoom` never sets it) |
| `action` | `"added"` | `"added"` |
| `roomName` | `app.Name`, fallback bot account | same rule |
| `appInfo` | `{app.ID, app.Name, botAccount}` | same values from the already-fetched full App doc |
| `hrInfo` | nil for bots | nil |
| `timestamp` | publish-site `UnixMilli` | same |

## Testing

TDD, all in `user-service/service/apps_test.go` against the existing mock
harness (`newSvc` wires one `MockEventPublisher` as both `pub` and
`clientPub`; expectations are subject-scoped):

- Resub happy path publishes on `subject.SubscriptionUpdate("alice")`;
  captured payload asserts every event field, including the two patched
  subscription fields and the populated `room` object.
- `app.Name == ""` → `roomName` falls back to the bot account.
- `GetSubscriptionByRoomID` error → no publish, RPC still succeeds.
- `GetSubscriptionByRoomID` miss (nil) → no publish, RPC still succeeds.
- Enriched row names a `Del-` room → no publish, RPC still succeeds.
- `Publish` returns an error → RPC still succeeds.
- Existing reactivate tests gain the two new expectations (their previous
  shape is the Red-phase failure evidence).
- Fresh-subscribe and unsubscribe paths assert no user-service publish
  (unchanged behavior).

No mongorepo change ⇒ no new integration tests; `GetSubscriptionByRoomID` is
already covered.

## Documentation impact (same PR, per CLAUDE.md §5)

1. `docs/client-api.md` §`subscription.setAppSubscription`: add a
   `##### Triggered events — success path` block covering **both** the fresh
   path (already fires today via room-worker; currently undocumented) and the
   new resub path, in the room-RPC convention; note the recipient set and
   best-effort delivery.
2. `docs/client-api/request-reply.md`: replace `**Emits:** None.` with the
   standard `**Emits:** subscription.update (action: "added" …)` line.
3. `docs/client-api/events.md`: add `subscription.setAppSubscription
   ("added")` to the Triggered-by roster.

No `pkg/model` struct changes ⇒ the model-docs rule is not tripped.

## Non-goals / follow-ups

- **Unsubscribe still publishes nothing.** The FE-visible gap is symmetric;
  `SubscriptionRemovedEvent`'s lean shape is exactly what a client needs to
  drop the row, but today only channel-typed removals emit it
  (room-worker/handler.go:437-448, :650-660). Same pattern as this change
  would fix it; separate PR.
- **`SetAppSubscribed` never federates.** Mongo-only on all paths; cross-site
  mirrors of the flag stay stale. Pre-existing, out of scope.
- **room-worker's dup-path stale event.** A replayed `serverCreateDM` after an
  unsubscribe fans out `"added"` with `isSubscribed:false`. Latent (no
  production caller reaches it — user-service's guard routes resubs away), but
  worth a room-worker-side fix note.

## Risks

- **Shape drift vs room-worker's fan-out** if the `"added"` event is enriched
  further in the future. Fenced by the field-asserting unit test; the botDM
  slice is narrow and `buildLocalRoom` is shared with `subscription.list`, so
  the event tracks what a refetch returns — arguably the truer contract.
- **Secondary-read staleness** in the room baseline (`lastMsgAt` etc.):
  ms-scale replication lag, display-only fields, and the two state fields the
  write changed are patched in memory. Accepted.
- **Lost event on publish failure**: identical semantics to every existing
  publisher of this subject — best-effort by design, client reconciles on
  refetch (room-service/handler.go:814-817).

## Verification

- `make lint`
- `make test SERVICE=user-service` (race detector on)
- `make test` (full unit sweep)
- Coverage of the new helper ≥ existing package bar (`go test -coverprofile`)
