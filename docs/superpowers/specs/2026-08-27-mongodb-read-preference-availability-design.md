# MongoDB Read Preference for Primary-Down Availability

**Date:** 2026-08-27
**Status:** Implemented on `claude/mongodb-read-preference-availability-jw7coj`; see §13 for what remains unverified.
**Related:** `pkg/mongoutil/readpref.go`, `pkg/mongoutil/mongo.go:62` (startup
ping — owned by a separate PR, see §3), `room-service/store_mongo.go:56-68`
and `user-service/mongorepo/readpref.go` (the existing per-collection pattern),
`teams-user-sync/main.go:52` (the existing two-client pattern)

## 1. Problem

When a replica set loses its primary and survives as read-only secondaries, the
driver default read preference (`primary`) makes every read fail — not just the
writes that genuinely cannot be served. Reads that would be perfectly serviceable
from a surviving secondary instead block for the full server-selection timeout
and then error, so a partial database outage presents to users as a total one.

Eight services already carry a `READ_PREFERENCE` knob defaulting to
`secondaryPreferred` (`room-service`, `user-service`, `history-service`,
`search-service`, `broadcast-worker`, `notification-worker`,
`user-presence-service`, `portal-service`). The remaining Mongo-using services
have no read preference at all and run on the `primary` default.

This document decides, per service and per collection, which of those should
move to `primaryPreferred` or `secondaryPreferred`, which must stay on
`primary`, and why.

## 2. The governing principle

**`primaryPreferred` and `secondaryPreferred` are identical for availability.**
Both select a secondary when no primary exists. They differ only in steady
state:

| Mode | Steady state | No primary |
|---|---|---|
| `primary` | reads primary | **fails** |
| `primaryPreferred` | reads primary — *no behaviour change* | reads a secondary |
| `secondaryPreferred` | reads a secondary — staleness, primary offload | reads a secondary |

It follows that **`primaryPreferred` is the correct default for an
availability-motivated change.** It buys the entire availability win at zero
steady-state risk. `secondaryPreferred` buys something extra — primary offload —
at the cost of permanent replica lag on every read, and is therefore justified
only where staleness is provably harmless.

## 3. Relationship to the startup-ping work

A separate PR addresses the connection-level blockers: the `client.Ping(ctx, nil)`
in `pkg/mongoutil/mongo.go:62` (which resolves to the *client's* read preference,
so a `primary` client cannot start during an outage), the absent
`ServerSelectionTimeout`, and the two `readyz` handlers that ping the primary
(`admin-service/store_mongo.go:573-574`, `botplatform-service/store_mongo.go:62-63`).

**Those changes are a prerequisite for this one to matter.** Without them, a pod
that restarts mid-outage exits at startup regardless of how its collections are
configured. This document assumes they land first and does not re-specify them.

One interaction is worth stating. `room-service` and `user-service` set their read
preference only at the *collection* level and call `mongoutil.Connect` without
`WithReadPreference` (`room-service/main.go:193`, `user-service/main.go:127`), so
their clients are `primary`. That is not only a startup-ping problem: a collection
created without options inherits the client preference, so every handle that is not
one of their explicit `*Secondary` clones reads from the primary today and fails
during an outage. Setting the client preference is therefore a substantive
availability change for these two, and it is specified here in §5.2 rather than
deferred to the startup-ping PR.

## 4. The classification test

The intuitive test — "is this service read-only against Mongo?" — is wrong. Being
a pure reader says nothing about staleness tolerance. Two questions decide it:

1. **Read-after-write.** Does this read target a document that another component
   (or an earlier step of the same request) wrote moments ago, and is a miss
   user-visible or durable?
2. **Side effects outside Mongo.** Does a stale read drive a write to Cassandra,
   a NATS publish, or an external API call? Those succeed during a Mongo outage,
   so a stale read becomes a durable or externally-visible mistake.

A "yes" to (1) disqualifies `secondaryPreferred` but not `primaryPreferred`, which
is a steady-state no-op. A "yes" to (2) with a *non-idempotent external* effect
disqualifies both — failing closed is the correct behaviour there.

## 5. Decisions

### 5.1 `secondaryPreferred`

| Service | Evidence |
|---|---|
| `tcard-service` | `ListCards` (`store_mongo.go:37`) is a full-collection scan feeding a cache refreshed **once daily** (`TCARD_CACHE_REFRESH_AT`). Nothing in the repo writes `cards`. Data is up to 24h stale by design; replica lag is noise. A full scan is also the single best offload candidate in the repo. |

**`search-sync-worker` was moved to `primaryPreferred` after review.** The
caveat that a miss is durable turned out to disqualify it rather than merely
qualify it: `buildTeamsActions` emits an index action with empty author fields
and `handler.go` Acks the source message once the bulk request succeeds, so
nothing retries the under-enriched write. Only `tcard-service` takes
`secondaryPreferred`.

Reverting it further, to `primary`, would make that outcome *worse*, not safer.
`resolveTeamsIdentities` (`messages.go:296-310`) swallows a resolver error —
logs it and returns `nil` — and `buildTeamsActions` indexes the batch with empty
author fields either way. So during a primary-down incident `primary` guarantees
the under-enriched write (every lookup errors), while `primaryPreferred` reads
`teams_user`/`users` from a secondary and near-certainly gets the right answer:
that mapping is migration-static, so replica lag is irrelevant to it. The only
real fix for the durable miss is repair semantics — failing the batch instead of
swallowing the resolver error — which is a behaviour change independent of read
preference and out of scope here.

### 5.2 `primaryPreferred`

| Service | Why not `secondaryPreferred` |
|---|---|
| `message-gatekeeper` | `subcache.go:45-47` correctly does not cache negative results, so Mongo is hit **only on a cold miss** — which is exactly the just-joined-a-room case. Secondary reads would aim replica lag at the one request that cannot tolerate it, returning `forbidden/not_subscribed` on a user's first message after joining. |
| `message-worker` | See §6 for the full trace. Reads `users`, `thread_rooms`, `thread_subscriptions`, `subscriptions`; writes Cassandra. |
| `bot-message-handler` | Same authz shape as gatekeeper (`FindSubscription`, `FindRoom`, `ListMemberIDs`) with **no cache at all**, so every request rides the preference. |
| `upload-service` | `GetUpload` (`store_mongo.go:61`) fetches an upload doc by `_id` to build a download URL. **Nothing in this repo writes `uploads`** — the writer is external, so the write→read window cannot be bounded. Under secondary reads a just-uploaded file 404s. |
| `botplatform-service` | `InsertSession` (`handler.go:145`) then `FindSessionByHash` on the next request (`handler.go:203`) — the canonical read-after-write. Secondary reads break auth immediately after login. **Session reads are additionally pinned to primary inside `pkg/session` (see §15), so this client preference never reaches them.** |
| `media-service` | `EmojiDoc`/`Avatar` are read right after `UpsertEmoji`/`SetBotAvatar` (upload, then display). Its avatar/emoji *serving* reads are strong offload candidates but need a per-collection split, not a client-level flip. |
| `bot-room-service` | Creates rooms/subscriptions then reads them back. |
| `admin-service` | **Requires a transaction guard — see §5.4.** |
| `notification-worker` — `users` only | Relaxes the existing `readpref.Primary()` pin at `main.go:227`. Stale means notifying a just-muted user: self-correcting, not durable. |
| DEK + room-key sites (4) | See §5.3. |
| `room-service` | **Client-level**, alongside its existing per-collection overrides. Its plain handles (`store_mongo.go:70-76`) carry no collection options, so they inherit the client preference — today `primary`. Only 12 of its methods use a `*Secondary` handle; every other read, List Members included, fails during an outage until the client preference moves. |
| `user-service` | Same shape: 16 of 39 repo methods use a `*Secondary` handle; the rest inherit the client. |

### 5.3 The DEK and room-key pins

**Five sites, not four.** The original audit of this section missed
`pkg/roomkeystore/open.go` — it pins both the `rooms` and `retired_room_keys`
handles and is what serves `room-service`'s `key.get`. Relaxing the producer
(broadcast-worker) without it produced a state worse than the pin: the worker
encrypts and delivers, and the client can never fetch the key. All five now share
one `MONGO_KEY_READ_PREFERENCE` so producer and consumer cannot be configured
apart. The sites are:
`broadcast-worker/main.go:194` (preview DEK) and `:233` (`roomsPrimary`),
`history-service/cmd/main.go:149,155` (at-rest + preview DEKs), and
`bot-message-worker/main.go:116` (at-rest DEK — its *only* Mongo collection), and
`pkg/roomkeystore/open.go` (room keys + retired archive, behind `key.get`).

The stated fear is a freshly-minted key missed on a lagging secondary. The
justification for relaxing to `primaryPreferred` is **not** "no writes happen
during an outage" — that argument does not cover the election window, which is
precisely when the fallback engages. It is that both paths already carry guards
that turn a stale read into a retryable error rather than silent divergence:

- **At-rest DEKs.** `dek_store.go:53` upserts with `$setOnInsert`, so an existing
  row is never overwritten, and `cipher.go:206-217` re-reads and compares — on a
  mismatch it unwraps the *winner's* key. A stale read therefore ends in either
  the correct key or a `"dek row missing after upsert"` error that propagates
  through `dekFor` and is redelivered by JetStream. There is no corruption path.
- **Room keys.** The key is an `encKey` sub-document inside the room doc
  (`roomkeystore_mongo.go:19-21`), and `broadcast-worker/handler.go:996` treats a
  missing key as a hard error (`errNoCurrentKey`) → retry. The subtler case — a
  secondary holding a pre-rotation version, so a message is silently stamped at
  version N-1 — is exactly what `retired_room_keys` and `ROOM_KEY_RETIRED_TTL`
  exist to cover.

**Resolved during implementation.** The margin was exactly at the floor —
`ROOM_KEY_RETIRED_TTL` 20m against `ROOM_KEY_CACHE_TTL` 10m, i.e. precisely the
2x minimum `retiredTTLSafe` enforces, with no slack. Widening the staleness
window against zero margin would have been the riskier choice, so the retired
TTL default moves to 30m across all four services that must agree
(`room-service`, `room-worker`, `bot-room-service`, `broadcast-worker`) and in
the five compose files that pin it. A test now asserts the defaults leave slack
rather than sitting on the floor.

**Original action item:** CLAUDE.md requires retired-key
retention to outlast `ROOM_KEY_CACHE_TTL` plus the client's `key.get` and retry.
A secondary read widens the staleness window that budget absorbs. Re-check the
margin before merging; it is not a blocker but it is not free.

### 5.4 `admin-service` transaction guard

`store_mongo.go` runs `withTransaction` via `StartSession()` with no options. A
transaction's read preference defaults to the session's, which defaults to the
**client's** (`mongo/client.go:461` → `session/client_session.go:236,474`), and
the driver rejects a non-primary one inside a transaction. The guard sets it
explicitly via `SetDefaultTransactionOptions(options.Transaction().
SetReadPreference(readpref.Primary()))` — `SessionOptionsBuilder` has no
`SetDefaultReadPreference` in driver v2.

**It is not load-bearing today, and this section originally overstated it.** The
check lives in `createReadPref` (`x/mongo/driver/operation.go:1886-1895`), which
returns early for `op.Type == Write`. All three current transaction bodies are
write-only — `UpdateUserPasswordAndRevoke` (`UpdateOne` + `DeleteMany`),
`DeactivateAndRevoke` (`FindOneAndUpdate`, a write command, + `DeleteMany`) and
`RecordPermissionChange` (`InsertMany` + `UpdateMany`) — so none of them reaches
the check. Without the guard, nothing breaks now.

Keep it anyway, as future-proofing with a sharp edge behind it: the moment
anyone adds a *read* inside one of these transactions — reading the user doc
before updating it is the obvious next step — it fails with `read preference in
a transaction must be primary`, and it fails in **normal operation**, not only
during an incident, because `primaryPreferred` is already a non-primary mode.
The guard turns a latent landmine into a non-issue for one line.

### 5.5 Stay on `primary` — deliberate

`teams-chat-sync`, `teams-chat-member-sync`, `teams-hr-sync`, `teams-room-verify`,
`teams-room-inspector` read Mongo and then call **MS Graph**. That side effect
succeeds during a Mongo outage, so a stale read can create a duplicate Teams room
or re-invite a removed member — non-idempotent, external, not undone by a retry.
For a sync worker with external side effects, failing closed is correct.

### 5.6 No action

| Service | Reason |
|---|---|
| `teams-user-sync`, `teams-room-creation` | Already use `mongoutil.ConnectRead` (hardcoded `secondaryPreferred`) with a separate write client — `teams-user-sync/main.go:52`, `teams-room-creation/main.go:51`. This is the house two-client pattern. |
| `hr-sync-worker` | Connects only via `MONGO_WRITE_URI` (`config.go:16`); effectively write-only. |
| `room-worker`, `inbox-worker` | JetStream consumers whose work product *is* Mongo writes. They fail and Nak during an outage regardless; a read-preference change buys nothing. |
| `history-service`, `search-service`, `user-presence-service`, `portal-service`, `broadcast-worker` | Already default to `secondaryPreferred` and stay there. `broadcast-worker`'s two `readpref.Primary()` pins are covered by §5.3; `notification-worker`'s by §5.2. |

## 6. `message-worker` trace

Worth recording because the conclusion is counter-intuitive: the service writes
to Cassandra, so it looks like a §4-question-2 hazard, and it is not.

Call order in `processMessage` (thread-reply branch, `handler.go:159-201`):

| # | Operation | Store | On failure |
|---|---|---|---|
| 1 | `handleThreadRoomAndSubscriptions` → `CreateThreadRoom` | Mongo write | return → Nak |
| 2 | `AdvanceThreadSubscriptionLastSeen` | Mongo write | warn + continue |
| 3 | `markThreadMentions` → `GetHistorySharedSince` | Mongo read | return → Nak |
| 4 | `fanOutThreadUnread` → `AddThreadUnread` | Mongo write | return → Nak |
| 5 | `SaveThreadMessage` | **Cassandra write** | return → Nak |
| 6 | `publishThreadReplyEvent` | NATS publish | return → Nak |

Mongo first, Cassandra last — deliberately. `handler.go:159-160` states it:
"Resolve (or create) the thread room first so we have the threadRoomID before
persisting the message to Cassandra." Every Mongo failure returns before step 5,
so an outage aborts at step 1 with no Cassandra row and no NATS event. JetStream
redelivers on recovery.

The one read-after-write — `CreateThreadRoom` → duplicate key →
`GetThreadRoomByParentMessageID` (`handler.go:435`) — is self-protecting:
reaching it requires the insert to have reached a primary and returned a
duplicate, so during a sustained outage it is unreachable (the write fails first
at `handler.go:342`). During a brief election the read can miss, and
`store_mongo.go:73-74` returns `errThreadRoomNotFound` → Nak → replay.

**Note for future maintainers:** `followers := existingRoom.ReplyAccounts`
(`handler.go:439`) is an unguarded pointer deref, safe only because that store
method returns an error rather than the `(nil, nil)` no-match convention used by
`mongoutil.Collection.FindOne` elsewhere. If that method is ever normalised to
the house convention, add a nil check.

## 7. Explicit non-goals

- **Do not add `maxStalenessSeconds`.** With no primary, staleness is measured
  against the freshest surviving secondary; a tight value can make the driver
  reject every member, converting the fallback into a hard failure. Currently set
  nowhere. Keep it that way.
- **Do not add `readConcern: majority`.** If the outage is a loss of majority, the
  majority commit point stalls and majority reads block or return ever-staler
  data. Default `local` is the availability-correct choice. The one
  `readconcern.Majority()` in the repo (`data-migration/oplog-connector/main.go:177`,
  a change stream) is correct and out of scope.
- **Do not use strict `readpref.Secondary()`** on any serving path — it fails when
  only the primary survives, the inverse outage. Its one use (`oplog-connector`,
  default `secondary`) is appropriate.
- **No per-collection split for `media-service`** in this change. Splitting avatar
  and emoji serving reads onto secondaries is a real offload win but is a separate,
  larger change.

## 8. Configuration shape

Follow the existing convention: a `READ_PREFERENCE` env var parsed by
`mongoutil.ParseReadPreference`, validated at config load, logged once at
startup. Services choosing a client-wide preference pass
`mongoutil.WithReadPreference`; services needing a per-collection split follow
`room-service/store_mongo.go:56-68` (a `WithReadPreference` store option plus
`*Secondary` collection handles).

Note that `ParseReadPreference` maps empty to `readpref.Primary()`
(`readpref.go:19-21`), so an unset env var preserves today's behaviour — the
`envDefault` is what actually changes each service.

## 9. Testing

- **Unit.** Table-driven tests per service asserting the parsed preference reaches
  the collection handle, following `room-service/store_mongo_readpref_test.go` and
  `user-service/mongorepo/readpref_test.go`.
- **Config.** Reject invalid values and assert the new `envDefault`, following
  `history-service/internal/config/config_test.go:126-156`.
- **`admin-service` regression.** An integration test on the RS container
  (`pkg/testutil/mongo_replicaset.go`) driving `withTransaction` with a probe
  insert **and read** under a `primaryPreferred` client — the read is what
  consults the session's preference. It does not exercise
  `UpdateUserPasswordAndRevoke` itself; it covers the guard directly. The read
  is what makes it falsifiable: the driver checks the transaction read preference
  during command construction, not server selection, so the single-node
  `directConnection` harness does not weaken it. Remove the pin and this test
  must fail.
- **Outage behaviour.** `tools/loadgen/mongo_outage_recovery_integration_test.go`
  already builds a dedicated Mongo container for outage simulation and is the
  natural home for an end-to-end assertion that reads survive primary loss.

TDD per CLAUDE.md §4: tests first, confirmed failing, before each service's flip.

## 10. Rollout

Ordered by value, each independently shippable:

1. `message-gatekeeper` + `message-worker` — together these keep the plain-message
   send-and-store path alive (§12).
2. `admin-service` with its transaction guard — lands early and alone, though
   see §5.4: the guard is future-proofing, not a live fix.
3. The remaining `primaryPreferred` services.
4. `tcard-service` + `search-sync-worker` (`secondaryPreferred`).
5. The four DEK/room-key sites, after the §5.3 retention re-check.

## 11. What this change actually buys

§12 lists the end state. This section isolates the delta, because much of the
end state is already true today.

**Already available today** (services whose clients default to
`secondaryPreferred`, provided their pods do not restart — see §3): search;
presence; plain-text history reads; message delivery and history reads *for
unencrypted rooms*; and the twelve `room-service` / sixteen `user-service` repo
methods that use an explicit `*Secondary` handle — member statuses, mentionable
subscriptions, read receipts, org members, room app tabs and command menus.

**Moves from unavailable to available:**

| Feature | Blocked today by |
|---|---|
| Message send | `message-gatekeeper` on the `primary` client default |
| Message store (plain messages) | `message-worker` likewise |
| **Encrypted-room message delivery** | `broadcast-worker`'s `roomsPrimary` room-key pin (`main.go:233`); `handler.go:996` treats a key miss as a hard error, so delivery to encrypted rooms fails outright today |
| **Encrypted-message history reads** | the at-rest DEK pins (`history-service/cmd/main.go:149,155`); `cassrepo/decrypt.go:32` cannot decrypt without the DEK |
| Room / subscription reads not on a `*Secondary` handle — List Members among them | `room-service`'s plain handles inheriting the `primary` client |
| User reads not on a `*Secondary` handle | `user-service`, same |
| Reactions, Pin/Unpin, Edit/Delete | their `history-service` authz reads |
| Avatars, emoji serving, file download | `media-service`, `upload-service` on the `primary` default |
| Bot messaging paths | `bot-message-handler`, `bot-message-worker` |
| Existing bot/admin session validation | `botplatform-service` |
| Admin console reads | `admin-service` |
| Push notification delivery | `notification-worker`'s `usersCol` primary pin, which gates the mute check |
| Pod restarts mid-incident | the §3 startup ping — a prerequisite, not part of this change |

The two largest wins are the ones that read as availability but are really
correctness pins: **encrypted rooms cannot send or read history during a
primary-down incident today**, and that is invisible in the read-preference
inventory because both sites look like deliberate, well-commented decisions.

**Moves from available to unavailable: none.** No feature this change touches
becomes less available during an incident; the change only widens the set of
reads that can be served. The regressions to guard are steady-state, not
outage-time:

| Risk | Mitigation |
|---|---|
| A *read* added to any `admin-service` transaction fails in normal operation once the client is non-primary | §5.4 — the guard makes this a non-issue; today's bodies are all write-only |
| `tcard-service`, `search-sync-worker` accept permanent replica lag | §5.1 — argued per service |
| Retired-key retention budget narrows | §5.3 action item |

## 12. Feature availability during a primary-down incident

What follows assumes this change plus the §3 startup-ping work. **Writes fail
throughout** — this is about which reads survive.

### Available

| Area | Detail |
|---|---|
| **Message send + store (plain messages)** | `message-gatekeeper` authorises from a Mongo read; `message-worker`'s non-thread branch (`handler.go:203`) writes only Cassandra. |
| **Message delivery** | `broadcast-worker` fan-out; already `secondaryPreferred`. |
| **History reads** | Load History / Next / Surrounding, Get Message By ID(s), List Pinned, Get Thread Messages, Get Thread Parent Messages — Cassandra-backed with Mongo reads for authz. |
| **Reactions** | `reactions.go` makes no Mongo repo calls — pure Cassandra. |
| **Pin / Unpin** | Mongo reads only for authz (`pin.go:38,75`); the pin write is Cassandra. |
| **Edit / Delete message** | Cassandra write succeeds. The room-preview update is Mongo and best-effort — logged and swallowed (`messages.go:691-704`), so the chat-list snippet goes stale until recovery. |
| **Search** | All five search RPCs — Elasticsearch-backed with Mongo reads. |
| **Presence** | Valkey-backed; Mongo only for a user cache (`user-presence-service/main.go:135`). |
| **Room + subscription reads** | List Members, Get Member Statuses, Get Mentionable Subscriptions, Read Message Receipts, List Org Members, Room App Tabs / Command Menu. |
| **User reads** | `me`, `status.getByName`, `profile.getByName`, `settings.get`, `priorityContacts.get`, all `subscription.*` reads, `apps.list`, `apps.categories`, thread list + unread summary. |
| **Avatars + emoji (serving)** | `media-service` GET routes. |
| **File download** *(conditional on replication)* | `upload-service` `GetUpload`. Replicated files download; a file written by the external uploader immediately before the incident may 404 until replication resumes — see §15. |
| **Bot/admin session validation** | `botplatform-service` `FindSessionByHash` — existing sessions keep working. |
| **Admin console reads** | List users / sessions / audit log / permission grants. |

### Unavailable

| Area | Cause |
|---|---|
| **Thread replies** | Steps 1 and 4 of §6 are Mongo writes. Naks and replays on recovery — no data loss, but the reply does not land until then. |
| **Room lifecycle** | Create Room, Add / Remove Members, Update Member Role, Rename Room, Open Room. |
| **Read state and per-room prefs** | Mark Messages Read, Mark Thread as Read, Toggle Mute, Toggle Favorite, Move Chat to Section, Clear All Thread Unread. |
| **User writes** | `status.set`, `settings.set`, `priorityContacts.add/remove`, `subscription.setAppSubscription`, `sso.set`, `sso.refresh`. |
| **New logins** | `botplatform-service` `InsertSession` is a write. Existing sessions survive; new ones cannot be minted. |
| **Avatar / emoji upload** | `SetBotAvatar`, `UpsertEmoji`, `DeleteEmoji`. |
| **Admin console writes** | Create / update user, set password, revoke sessions, permission grants, resync. |
| **Teams integration** | Deliberately fails closed (§5.5). |
| **New room encryption keys** | Minting a DEK or room key requires a write; rooms with an existing key keep working (§5.3). |

### Confidence

The "available" rows are grounded in the call sites cited above. Two entries
deserve a caveat rather than a flat claim: **file upload** — `upload-service`
performs no Mongo writes, but the `uploads` document is written by a component
outside this repo, so whether a *new* upload completes during an outage is not
determinable from this codebase; and **SSO login** — `auth-service` holds no
Mongo client at all, but the paths it delegates to have not been traced end to
end here.


## 13. Implementation notes

Two deviations from this document, both decided during execution:

1. **Retired-key retention.** See §5.3 — the margin was at the floor and the
   default moved 20m → 30m. This is an ops-visible change to a value four
   services must share; it is deliberate, not incidental.

2. **The end-to-end outage assertion was not delivered as specified.** It needs a
   multi-node replica set. The shared harness (`pkg/testutil/mongo_replicaset.go`)
   is a single node reached over `directConnection`, which puts the driver in
   Single topology — there, read preference no longer selects a server, so the
   test could not distinguish `primary` from `primaryPreferred`. In its place,
   `pkg/mongoutil` now asserts that a chosen preference actually reaches the
   driver's `ClientOptions`, closing the loop between config and client. The
   behavioural claim still needs a real multi-node replica set to prove.

### Verified

`make test` (whole repo, `-race`): 0 failures. `make lint` (whole repo):
0 issues. `make sast`: gosec PASS.

### Not verified here

- **Every integration test** — including the `admin-service` transaction guard,
  since resolved: CI ran it green on `aa1a433` and after —
  this environment has the Docker CLI but no daemon, so testcontainers cannot
  start. They compile and type-check (verified locally; `make lint` covers vet in CI). The transaction guard
  is the highest-value thing to run before merge: it is the one change that can
  break a working path.
- **govulncheck** — blocked by the egress proxy (`vuln.go.dev` returns 403).
- **semgrep** — not installed in this environment.


## 14. Post-review corrections

A review of the branch found a regression this document's §11 had claimed as a
win. `broadcast-worker`'s room-key read was relaxed while `roomkeystore.OpenMongo`
— serving `key.get` — stayed pinned, so encrypted rooms delivered messages whose
keys clients could never fetch. That is worse than the pre-change state, where
both sides failed together and nothing was delivered.

Root cause of the miss: the initial repo-wide read-preference inventory was run
with `head -100` and truncated before `pkg/roomkeystore/open.go`. Any future audit
of this kind must run unbounded.

§11's "encrypted-room message delivery" row is only true with all five sites
relaxed together, which is now the case.


## 15. Review corrections

CodeRabbit reviewed the branch. Three findings changed the design; two did not.

**Session lookups pinned to primary, unconditionally** (CWE-613). Both
`botplatform-service` and `admin-service` build a `pkg/session` store on a client
this branch moved to `primaryPreferred`, so during primary loss a revoked session
that had not replicated could still authenticate. `pkg/session.NewMongoStore` now
pins its collection to primary itself, rather than each service opting in — no
caller should be able to trade authentication freshness for availability. The
cost is that bot/admin session validation no longer survives primary loss; that
was the pre-change behaviour, so it is a declined win rather than a regression.

**`search-sync-worker` moved from `secondaryPreferred` to `primaryPreferred`.**
See §5.1 — a resolver miss is durable and unretried, which disqualifies it.

**File download qualified, not changed.** Under `primaryPreferred`, `GetUpload`
can hit a secondary that has not yet received an externally-written `uploads`
document and return `mongo.ErrNoDocuments`, which the handler surfaces as a 404 —
semantically "gone" where a 503 would say "retry". Reverting to primary would
lose the win for every already-replicated file, which is the overwhelming
majority, so §12's "file download" row is **conditional**: replicated files
download, a file written immediately before the incident may 404 until
replication resumes.

**Retired-key TTL: no migration.** `roomkeystore_mongo.go:318` writes
`expiresAt` per document at rotation against a TTL index at `expireAfterSeconds:
0`, so the 20m → 30m change does not extend documents already archived. Those
expire on the old policy and new ones get the new policy; the discrepancy
self-heals within 20 minutes of deploy. A migration is disproportionate to a
bounded rollout window, but the window is real: avoid rotating keys during the
first 20 minutes after deploying this change.

**Empty-env normalization: not a defect.** The review held that setting
`MONGO_READ_PREFERENCE=""` would override `envDefault` and collapse to `primary`,
silently disabling fallback. Verified against the vendored
`caarlos0/env v11.4.0`: an empty value falls back to `envDefault`, yielding
`primaryPreferred`. No change made.

**Room-key index ensure made non-fatal.** `pkg/roomkeystore.OpenMongo` returned
an error when `EnsureIndexes` failed, so `room-service`, `room-worker` and
`bot-room-service` could not start while no primary existed — `createIndexes` is
a write. That would have negated this change for exactly those three: their key
handles are now read-preference configurable so a restarting pod can serve
`key.get` from a secondary, and a fatal ensure meant the pod never got that far.
The site is now `slog.Warn`-and-continue, which is not new policy but the
repo-wide non-fatal index ensure from #333 (`tcard-service/main.go:88` and
others); `OpenMongo` predates it (#281) and was missed. The only index here is
the archive's TTL, so a later successful start creates it and it then applies to
the documents already archived — no unique constraint is at stake.

This does **not** reopen §3. Pod restarts mid-incident still depend on the
startup-ping PR; this only removes a blocker that sits *behind* the ping and is
specific to the three services whose key handles this change makes
secondary-capable.

**Push after mute: a deliberate outage-time trade, not an oversight.** Three
review threads asked to keep `notification-worker`'s `users` read on `primary`,
because a push is an external side effect that cannot be recalled. The read is
already isolated on its own `MONGO_USER_READ_PREFERENCE`, defaulting to
`primaryPreferred` — a steady-state no-op against `primary`, so nothing changes
while a primary exists. The two options differ only during a primary-down
incident:

| Choice | Outage behaviour |
|---|---|
| `primary` | The mute check cannot be served, so **no push is delivered at all** for the duration. §11 lists this as a thing the change unblocks. |
| `primaryPreferred` | Pushes continue. A mute committed inside the replication window immediately before the primary died may not be honoured. |

The exposure is bounded to that window — mutes set earlier have replicated, and
mutes set during the outage cannot be written at all, so the user is not muting
into a void either way. Losing every notification for an entire incident is the
worse failure for the same user, so this stays `primaryPreferred`; a site that
would rather fail closed sets `MONGO_USER_READ_PREFERENCE=primary`, which is
exactly why that read has its own knob.

This is not the same call as the teams-* workers, which stay on `primary`: a
Graph write is non-idempotent and changes state in another system, where a
missed push is a lost notification and nothing more.

**Empty read preference resolves to `primary`, deliberately.**
`mongoutil.ParseReadPreference("")` returns `readpref.Primary()` rather than an
error. Every caller in this change supplies an `envDefault`, so an unset
variable never reaches the empty branch (verified against `caarlos0/env
v11.4.0`, above). The branch is the fail-safe for any future caller that does
reach it: an empty value can only narrow staleness, never silently widen it.
