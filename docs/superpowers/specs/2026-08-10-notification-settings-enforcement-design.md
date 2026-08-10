# Notification Settings Enforcement (Spec 2 of 2)

Spec 1 (`2026-08-08-priority-contacts-storage-api-design.md`) gave users a place to
store notification preferences. Nothing reads them. This spec makes
`notification-worker` honour three of them when deciding whether to push.

## Scope

Three stored settings become load-bearing in the push pipeline:

| Setting | Effect on push |
|---------|----------------|
| `muteAllNotifications` | Suppresses push for the recipient. |
| `alwaysAllowPriorityNotifications` | Lets a message from a priority contact pierce that mute. |
| `showNotificationsInCall` | Opts the recipient in to pushes while their presence is in-call. |

`priorityContacts` is read as the input to the pierce, not gated itself.

### Out of scope

- The other six settings in `UserSettings` — they are client-render preferences
  with no server-side meaning.
- `showPreviewsInNotifications`. It shapes the push *body*, not whether a push is
  sent, and belongs with whatever owns notification rendering. Deliberately left
  out so this spec stays a pure gating change.
- Any new RPC, model field, or event. This spec adds no wire surface — it only
  changes which recipients survive the existing pipeline.

## Corrections to the issue

The issue sketched this as "step 3.5: fetch user settings and filter". Two things
in that sketch are wrong and this spec departs from it deliberately.

**The fetch cannot go before the cheap filters.** Step 3.5 as written sits ahead
of the exclusion/hook/routing stage, which would fetch settings for every member
of the room. In a 500-member room where three people are actually push-eligible,
that reads 500 user documents to use three. The fetch belongs *after* the
candidate loop, where the account list is already narrowed to survivors.

**The fetch cannot go after presence.** `showNotificationsInCall` modifies the
presence decision, so settings must be in hand when presence is evaluated. Step
3.5 leaves the two gates independent, which silently drops the setting on the
floor.

Both constraints resolve to a single placement: **after the candidate loop,
before `Presence.Snapshot`.** That is a narrow window and it is not obvious from
reading the handler, so a test pins it (see Testing).

## Source and caching

`notification-worker` already holds a `*mongo.Database` in `main.go` (line 143)
and already defines a Mongo-backed loader against it (`mongoMemberLoader`). The
settings snapshotter is the same shape, reading the `users` collection.

**No cache.** An earlier draft of this spec put a 30s-TTL Valkey cache in front of
the read. That was wrong: `valkeyutil.Client` exposes only single-key `Get` /
`Set` / `SetNX` / `IncrEx` / `Del` — no MGET and no pipelining. Because settings
are per-user, a cache would need one key per candidate, so a cache hit for a
500-candidate room costs 500 sequential Valkey round trips, strictly worse than
the single Mongo `$in` it was meant to replace. (`roomsubcache` gets away with
Valkey because its key is per-*room*: one key, one Get, one message.) Adding
`MGet` to the shared interface would not rescue it either — per-account keys hash
to different cluster slots and go-redis's `ClusterClient` returns `CROSSSLOT`
rather than splitting the read.

So: an indexed `$in` per 512-account chunk — one query per message for almost
every room — no cache, no TTL staleness. The read is
comparable to the per-message work the pipeline already does (a Valkey read for
members, a NATS RPC for presence). If Mongo load later justifies a cache, it goes
behind the unchanged `UserSettingsSnapshotter` interface — most likely as an
in-process per-pod TTL map, which has no network hop and no slot problem — with
no caller changes.

### Interface

Mirrors `PresenceSnapshotter` exactly, including its fail-open contract:

```go
// UserSettingsSnapshotter batches notification-settings lookups for push-eligible
// accounts. Errors are swallowed; an absent account defaults to current behaviour.
type UserSettingsSnapshotter interface {
	Snapshot(ctx context.Context, accounts []string) (map[string]notifSettings, error)
}
```

`notifSettings` is a narrow internal struct, not `model.UserSettings`. The stored
type is all `*bool`, and threading pointers into the gate makes every read site
re-decide what `nil` means. Resolving once at the edge keeps the gate total:

```go
type notifSettings struct {
	muteAll          bool
	allowPriority    bool
	showInCall       bool
	priorityContacts map[string]struct{}
}
```

**The zero value is exactly today's behaviour** — not muted, no pierce, in-call
suppressed. That is what makes fail-open free: a missing user, an unset settings
sub-document, a Mongo error, and the kill switch all converge on the same struct,
and none of them need a special case in the gate.

### Why fail-open, not fail-closed

Fail-closed — suppressing pushes when settings are unknown — is the wrong trade
here, and the choice is deliberate rather than inherited.

A settings read failure is not per-user; it is a Mongo hiccup affecting every
message in flight. Fail-closed converts that into **total notification silence
across the site** for its duration, and silence is the failure users cannot detect:
nobody notices the push that never arrived, so the outage is discovered when
someone misses something that mattered. Fail-open's worst case is that a muted
user gets a push during a Mongo incident — visible, self-correcting the moment the
read recovers, and recoverable by the user in a way silence is not.

It is also the settled convention on this exact path: `PresenceSnapshotter`
swallows errors and defaults to push, and the hook vetoer logs and allows. A gate
that fails closed while its two neighbours fail open would make the pipeline's
behaviour under partial failure incoherent.

Finally, muting is a delivery preference, not an access control. An unwanted push
to the recipient's own device discloses nothing to anyone else — the recipient is
already a member of the room the message was sent to — so there is no
confidentiality argument for treating unknown settings as deny.

### Mongo implementation

```go
filter := bson.M{"account": bson.M{"$in": chunk}, "active": bson.M{"$ne": false}}
projection := bson.M{
	"_id":     0,
	"account": 1,
	"settings.muteAllNotifications":             1,
	"settings.alwaysAllowPriorityNotifications": 1,
	"settings.showNotificationsInCall":          1,
	"settings.priorityContacts":                 1,
}
```

- Served by the existing unique `account:1` index on `users`.
- The `active: {$ne: false}` clause matches `activeUserFilter` in
  `user-service/mongorepo/users.go`, so this read agrees with user-service about
  what an active user is. Note what it does *not* do: candidates come from
  `subscriptions`, not `users`, so a deactivated account with a live subscription
  is still in the slice and simply misses the settings map, taking the zero value
  and pushing. That is today's behaviour — this service consults `users` nowhere
  at present, so deactivated users already receive pushes. Whether they should is
  a real pre-existing gap and deliberately **out of scope**: fixing it here would
  mean this spec silently changing delivery for a population it never set out to
  touch. It belongs in its own change against the member-loading path.
- Projection is narrow per the no-whole-documents rule. Note this is deliberately
  *not* the `{"_id":0,"settings":1}` whole-sub-document projection that
  user-service's fanouts depend on — nothing here re-publishes the settings
  object, so it takes only the four fields it gates on.
- Chunked at `PRESENCE_BATCH_SIZE`-style granularity via the existing
  `chunkStrings` helper, bounding `$in` size on large rooms.
- `priorityContacts` is decoded from its stored `[]string` into a set at this
  boundary, so the gate is O(1) per candidate rather than a linear scan per
  recipient.

A `noopUserSettings` returning an empty map backs the kill switch, mirroring
`noopPresenceSnapshotter`.

## Pipeline placement

In `handler.go`, immediately after the `len(candidates) == 0` early return and
before the presence snapshot:

```go
if len(candidates) == 0 {
	return nil
}

settings, _ := h.deps.Settings.Snapshot(ctx, accounts) // fail-open: error → empty map
snapshot, _ := h.deps.Presence.Snapshot(ctx, accounts) // fail-open: error → empty map
```

Both are keyed by account over the same narrowed `accounts` slice. The survivor
loop then reads both:

```go
for _, c := range candidates {
	ns := settings[c.Account]
	if !shouldPush(snapshot[c.Account], ns, ns.isPriority(msg.UserAccount)) {
		continue
	}
	survivors = append(survivors, c.Account)
}
```

Sorting, batching, and the `{messageID}-b{N}` dedup ID are untouched — this
changes only which accounts reach `survivors`.

## The gate

`shouldPush` grows two parameters:

```go
func shouldPush(p model.Presence, ns notifSettings, isPrioritySender bool) bool {
	if ns.muteAll && !(ns.allowPriority && isPrioritySender) {
		return false
	}
	if isInCall(p) && !ns.showInCall {
		return false
	}
	return true
}
```

Three decisions worth stating, because each could reasonably have gone the other
way:

**The pierce is any-room.** `alwaysAllowPriorityNotifications` piercing mute does
not depend on room type. A priority contact writing in a busy channel pierces the
same as one writing a DM. The setting says "always"; scoping it to DMs would be a
second, undocumented rule.

**The pierce does not cross the in-call gate.** Mute and in-call are independent
suppressors, and `showNotificationsInCall` is the user's control for the second
one. A priority sender piercing mute still respects in-call unless the user has
also opted in. Reading "always allow" as overriding both would make
`showNotificationsInCall` unreachable for exactly the senders most likely to
trigger it.

**`showNotificationsInCall` governs both suppressed statuses.** Today's gate
suppresses on `"busy"` and `"in-call"` as one bucket. The setting is named for
the second, but splitting the bucket would leave `"busy"` with no user-facing
control at all and no setting to add one. `isInCall` is extracted as a named
predicate over both statuses so the coupling is visible at the call site rather
than implied by a `switch`.

## Both deployments

`notification-worker` ships as **two** deployments of one binary, selected by
`MODE` (`pkg/stream.Pipeline`): `user` consumes `MESSAGES-CANONICAL` and publishes
`PUSH-NOTIFICATION`, `bot` consumes `BOT-MESSAGES-CANONICAL` and publishes
`BOT-PUSH-NOTIFICATION`. They share `handler.go`, so the gate lands in both.

That is intended, not incidental. `EligibleForPush` drops bots as *recipients*
(`m.IsBot → false`), so both pipelines fan out to human room members — bot mode is
bot-*authored* messages reaching people. Spec 1 deliberately allowed `.bot`
accounts in `priorityContacts` ("holds raw accounts — users and `.bot` alike"),
and that allowance only ever pays off here: a user who mutes everything, enables
`alwaysAllowPriorityNotifications`, and lists `helper.bot` as a priority contact
gets pierced by a message travelling the **bot** deployment. Gating user mode
only would leave that Spec 1 affordance permanently dead.

Because the kill switch is per-deployment env, ops can still disable the gate for
one pipeline independently if bot-mode throughput makes the extra query hurt.

### An invariant this breaks

`deploy/user/docker-compose.yml` carries a deliberate comment:

> Title is resolved here from the rooms collection; sender display name is
> pre-composed by message-gatekeeper and propagated on the canonical message,
> so no users-collection lookup runs in this service.

This spec introduces exactly that lookup. The design is still right — settings
live on the user document and there is nowhere else to read them — but the
invariant was written down on purpose, so the comment must be corrected in the
same change rather than left to contradict the code. Whoever implements this
should treat the comment as a prompt to double-check the throughput assumption it
was protecting: an indexed `$in` per 512-account chunk, on the narrowed candidate
set — one query for almost every room — is the whole cost.

## Configuration

Three new env vars on `notification-worker`, following the existing presence trio:

| Var | Default | Meaning |
|-----|---------|---------|
| `USER_SETTINGS_ENABLED` | `true` | `false` → `noopUserSettings`, i.e. today's behaviour exactly. |
| `USER_SETTINGS_BATCH_SIZE` | `512` | `$in` chunk size, mirroring `PRESENCE_BATCH_SIZE`. |
| `USER_SETTINGS_TIMEOUT` | `2s` | Per-`Snapshot` deadline, mirroring `PRESENCE_RPC_TIMEOUT`. |

The two enable flags default differently, so state both explicitly:
**`PRESENCE_RPC_ENABLED` defaults to `false`** because presence-service may not
exist yet, whereas **`USER_SETTINGS_ENABLED` defaults to `true`** because Mongo is
already a hard dependency of this service — and a gate that ships defaulted off is
a gate nobody turns on. The flag is kept as a kill switch so ops can revert the
behaviour change without rolling back the binary.

`USER_SETTINGS_TIMEOUT` bounds the new dependency rather than inheriting whatever
deadline the consumer context carries. On expiry the snapshotter fails open like
any other error, so a slow Mongo degrades to today's behaviour instead of stalling
the fan-out. Latency and error counts come from the existing
`mongoutil.WithObservability` instrumentation the worker already wires — this adds
a dependency to an instrumented client, not a new uninstrumented one, so no
bespoke metrics are needed.

`deploy/user/docker-compose.yml` and `deploy/bot/docker-compose.yml` set all three
explicitly so local dev matches production.

## Testing

TDD per CLAUDE.md — tests first, confirmed red, then implementation.

**`shouldPush` (table-driven, `presence_test.go`).** The full matrix of
`muteAll` × `allowPriority` × `isPrioritySender` × `showInCall` × presence status,
including: unmuted+in-call+opted-in pushes; muted+priority-sender+`allowPriority`
pierces; muted+priority-sender *without* `allowPriority` stays muted; the zero
`notifSettings` reproduces the current truth table exactly.

**Placement (`handler_test.go`) — this is the test that pins the design.** A room
where members are excluded by each upstream filter (self, `Muted`, restricted,
hook veto, `EligibleForPush`) plus one survivor. Assert the accounts slice passed
to `Settings.Snapshot` contains *only* the survivor. This fails loudly if anyone
later hoists the fetch above the candidate loop, which is the exact regression the
issue's step 3.5 would have shipped.

**Fail-open (`handler_test.go`).** Snapshotter returns an error → every candidate
still pushes. Snapshotter returns a partial map → accounts absent from it push.

**Bot-authored pierce (`handler_test.go`).** A muted recipient with
`alwaysAllowPriorityNotifications` and `helper.bot` in `priorityContacts`, receiving
a message whose `UserAccount` is `helper.bot`, is pushed. This is the Spec 1
affordance that only works if the gate runs in bot mode, so it is worth a named
test rather than a row in the `shouldPush` table.

**Integration (`integration_test.go`, `//go:build integration`).** Against
`testutil.MongoDB`: seed users with settings set, partially set, absent entirely,
and `active: false`; assert the returned map, that absent users are simply missing
(not zero-filled rows), and that an `active: false` user is treated as absent.
Verify the `$in` chunking boundary with a batch size below the seeded user count.

Coverage floor 80%; the gate and snapshotter are core logic and should clear 90%.

## Documentation

`docs/client-api.md` §settings (around line 4701) currently describes these three
fields without saying anything enforces them. The three descriptions gain a note
that `notification-worker` honours them for push delivery, and the priority-contact
interaction is stated once: `alwaysAllowPriorityNotifications` piercing
`muteAllNotifications` in any room type.

No request/response schema, model struct, or event changes, so the derived views
(`docs/client-api/request-reply.md`, `docs/client-api/events.md`) are untouched —
the CLAUDE.md same-PR rule for those views is not triggered.

## Rollout

**This deploy changes behaviour for settings users have already stored.** Spec 1
shipped the write path, so accounts already carry `muteAllNotifications: true` set
by users who — reasonably — expected it to do something. The moment this deploys,
those users stop receiving pushes. That is the intended outcome and it is also a
silent, user-visible change in delivery, so it belongs in the release notes rather
than being discovered as a report of missing notifications.

The release note should say: notification settings stored via the settings API are
now enforced for push delivery; users who previously enabled "mute all
notifications" will stop receiving pushes; `USER_SETTINGS_ENABLED=false` reverts to
prior behaviour without a rollback.

Worth a query against production `users` before deploying to size the affected
population — `{"settings.muteAllNotifications": true, "active": {"$ne": false}}` —
so the change lands as a known number rather than a surprise.

## Files

| File | Change |
|------|--------|
| `notification-worker/usersettings.go` | New. `UserSettingsSnapshotter`, `notifSettings`, `isPriority`, Mongo + noop implementations. |
| `notification-worker/usersettings_test.go` | New. Snapshotter unit tests. |
| `notification-worker/presence.go` | `shouldPush` signature; extract `isInCall`. |
| `notification-worker/presence_test.go` | Extend the `shouldPush` table. |
| `notification-worker/handler.go` | `HandlerDeps.Settings`; fetch placement; survivor loop. |
| `notification-worker/handler_test.go` | Placement + fail-open tests. |
| `notification-worker/integration_test.go` | Mongo-backed snapshotter tests. |
| `notification-worker/main.go` | Config vars; wire snapshotter or noop. |
| `notification-worker/deploy/user/docker-compose.yml` | Set both new vars; correct the "no users-collection lookup" comment. |
| `notification-worker/deploy/bot/docker-compose.yml` | Set both new vars. |
| `docs/client-api.md` | Note enforcement on the three settings. |
