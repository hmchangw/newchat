# Notification Worker — Room Subscription Cache & Mobile Notifications

## Status

Draft — brainstorming. This is a design-doc-only PR; no implementation code
lands here. The implementation plan, and the answers to the Open Questions
below, will be produced separately against the team's internal codebase.

## Summary

Four changes to the existing `notification-worker` service, delivered
together:

1. **Cache optimization** — wire in the existing `pkg/roomsubcache` so
   room-member fan-out reads from Valkey instead of MongoDB on every
   canonical message.
2. **Routing & mention-gating** — the worker currently fans a notification
   out to *every* room member. Replace the blanket fan-out with a
   per-recipient routing decision (DM / mention / large-room / bot /
   presence) so only genuine recipients are pushed. For a large channel
   this cuts emitted notifications by orders of magnitude — the single
   largest throughput win in this work.
3. **Mobile push delivery** — hand off to the team's existing (internal)
   push-notification service. The legacy desktop ephemeral publish is
   dropped: desktop banners are computed client-side from the
   broadcast-worker room-event stream, removing a redundant server publish.
4. **Latent bug fixes** — the current fan-out ignores two subscription
   fields: `Muted` (per-room mute) and `HistorySharedSince`
   (restricted-room access window). Both are fixed as part of this work.

The routing decision runs entirely **in-process**, against data loaded in
**bulk per message** — never as a per-recipient RPC. It introduces two
pluggable extension points — an in-process **hook handler** and a
per-message **presence snapshot** — whose detailed rules are owned by
internal services and are captured as Open Questions, not designed here.

## Background

### Current state

`notification-worker` already exists and works. It is a high-throughput
JetStream consumer on `MESSAGES_CANONICAL_{siteID}`. For each canonical
message it:

1. unmarshals a `model.MessageEvent`,
2. calls `ListSubscriptions(roomID)` directly against MongoDB,
3. builds one `model.NotificationEvent`,
4. publishes it (ephemeral core-NATS) to `chat.user.{account}.notification`
   for every room member except the sender.

Files: `main.go`, `handler.go`, `bootstrap.go`, plus `deploy/` and tests.
The consumer pattern, graceful shutdown, and stream bootstrap already follow
the repo conventions and are not changed by this work.

### Why change it

- **Mongo round-trip per message.** Step 2 hits the `subscriptions`
  collection for every published message. For a hot 10k-member channel this
  is ~50–200ms per message and contends with every other subscription
  operation. `pkg/roomsubcache` was built (see
  `2026-05-15-roomsubcache-design.md`) specifically to remove this
  round-trip, but has never been wired into a service.
- **Blanket fan-out amplification.** Step 4 publishes a desktop
  notification to *every* room member except the sender. A channel message
  that mentions nobody still produces one notification per member — for a
  10k-member room, 10k ephemeral NATS publishes where the correct answer is
  typically zero to a handful. This is both a notification-spam defect and
  the worker's largest throughput cost. The legacy system (see the
  integration analysis) gates delivery on a per-recipient routing decision;
  the new worker applies the same gating to the one leg it owns — the
  mobile push. (Desktop banners are derived client-side from the
  broadcast-worker room-event stream, so they are not a worker concern;
  see Delivery.) Mention-gating is therefore not scope creep — it is the
  core of "optimise the worker".
- **No mobile delivery.** The only delivery path is ephemeral core-NATS — a
  user who is not connected simply misses the notification. There is no path
  to mobile push.
- **Two correctness bugs.** The fan-out loop notifies *every* member except
  the sender. It ignores `Subscription.Muted` (a muted user is still
  notified) and `Subscription.HistorySharedSince` (a restricted-room member
  is notified about messages that predate their access window).

## Goals

- Remove the per-message Mongo round-trip from the fan-out hot path.
- Replace the blanket desktop fan-out with a per-recipient routing decision
  (DM / mention / highlight / preference / presence), evaluated in-process
  against data loaded in bulk per message.
- Add a mobile-notification delivery leg that hands off to the existing
  internal push service.
- Fix the mute and restricted-room bugs.
- Provide pluggable, no-op-by-default extension points — an in-process hook
  handler and a per-message presence snapshot.
- Keep the worker's consumer / shutdown / bootstrap conventions unchanged.

## Non-goals

- **Talking to APNs / FCM directly.** The internal push-notification service
  owns device-token storage and APNs/FCM delivery. This worker only hands
  off to it.
- **Building the push service or the presence/status service in this
  repo.** Both already exist in the team's internal codebase. Building
  either here would duplicate a service and pull external push-provider
  (APNs/FCM) credentials and device-token storage into a repo that has no
  business holding them. This work builds only the thin producer side —
  `notification-worker`'s mobile emitter — and consumes presence over a
  bulk RPC. The push hand-off and the bulk presence RPC are specified here
  as contracts; the bulk presence RPC is implemented by a separate PR on the
  presence service.
- **Defining hook-handler behavior.** Owned by the team; captured as an Open
  Question. The *seam* (an in-process predicate) is designed here; the
  *rules* are not.
- **Eager cache invalidation.** v1 is TTL-bounded, consistent with the
  `roomsubcache` design. An invalidation listener is Future Work.
- **Notification payload encryption.** Orthogonal concern; not addressed
  here.

## Design

### Architecture — fan-out pipeline

The worker remains a single JetStream consumer on
`MESSAGES_CANONICAL_{siteID}`. The existing high-throughput consumer (pull
iterator + semaphore + `sync.WaitGroup`) is unchanged. Per message, the
handler runs a fan-out pipeline:

```text
canonical message
   │
   ├─ once per message: parse @all / @here + the mention set; derive the
   │                    large-room flag (member count > LARGE_ROOM_THRESHOLD,
   │                    default 500); for a thread-only reply also load
   │                    the thread-follower set
   │
   ├─ load room members ─► roomsubcache (Valkey) ─miss─► Mongo loader ─► populate cache
   │
   ├─ per recipient — Stage 1: cheap in-memory exclusion filters:
   │     1. skip sender
   │     2. skip muted               (Muted)
   │     3. skip restricted          (msg / thread-parent before HistorySharedSince)
   │     4. skip thread non-follower (thread reply, not following, not mentioned)
   │
   ├─ per surviving recipient — Stage 2: in-process hook veto (suppress-only)
   │                    [rules PARKED — no-op default allows all]
   │
   ├─ per surviving recipient — Stage 3: routing predicate (pure CPU):
   │     decide push eligibility from
   │     DM? · mentioned (incl. @all/@here)? · large-room? · bot?
   │
   ├─ once per message — Stage 4: bulk presence RPC for the eligible set
   │                    ─► one request/reply (chunked for huge rooms), decides push vs no-push
   │
   └─ for each pushed recipient — emit:
         mobile push ─► chat.server.notification.push.{siteID}.send   (JetStream PUSH_NOTIFICATIONS_{siteID} ─► internal push service)

(Desktop banners are computed client-side from the broadcast-worker
room-event stream — the worker has no desktop emit leg.)
```

The pipeline is ordered cheapest-first and every per-recipient step is pure
CPU. The two facts that drive routing — the **mention set** and the
**large-room flag** — are computed **once per message**, not per recipient.
Presence is the only external input routing needs, and it is fetched in
**one batched Valkey read** for the whole survivor set, never one read per
member. There is deliberately no per-recipient RPC anywhere on this path: a
per-recipient network call in a 10k-member fan-out would add ~10k
round-trips per message and erase the throughput this work exists to gain.
This is why the routing logic lives *in the worker* (as an in-process
predicate fed by bulk data) rather than being delegated to an external
status/hook service called once per recipient.

### Component breakdown

The handler stays thin — it orchestrates; the work lives in collaborators,
each a small unit with one job — behind an interface where it wraps
swappable external behavior, or as a pure function where it does not.

| Unit | File | Responsibility |
|---|---|---|
| `Handler` | `handler.go` (changed) | Orchestrates one message: parse the mention set + large-room flag once; load members; run the Stage-1 exclusion filters; run the hook veto; call the routing predicate; apply the presence snapshot; emit. Defines the interfaces it consumes. |
| `cachedMemberLookup` | `members.go` (new) | Implements `MemberLookup`. Wraps `roomsubcache.Cache` + a Mongo loader: hit returns; miss loads from Mongo, populates the cache, returns. |
| routing predicate | `routing.go` (new) | Pure function. Given a recipient's member record + the per-message mention set, large-room flag, and room type, returns whether the recipient is eligible for a mobile push, *before* presence. No I/O, no dependencies. |
| `noopPresenceSource` | `presence.go` (new) | No-op `PresenceSource` — empty snapshot, so every eligible leg fires. The real impl makes one bulk presence RPC per message; depends on a separate presence-service PR. |
| `noopHook` | `hook.go` (new) | No-op in-process `Hook` (always allow). Real rules PARKED. |
| `mobileEmitter` | `emit.go` (new) | JetStream publish to the single subject `chat.server.notification.push.{siteID}.send` (bound to the `PUSH_NOTIFICATIONS_{siteID}` stream via the filter `chat.server.notification.push.{siteID}.>`; async publish — see Performance considerations), handing off to the internal push service. The recipient is in the event payload (`account`), not in the subject. The worker emits no desktop leg — clients compute desktop banners themselves from broadcast-worker room events. |
| wiring + config | `main.go` (changed) | Build the Valkey client, cache lookup, presence source, hook, emitters; new env config; assemble the pipeline. |

Interfaces, all defined in `handler.go` (the consumer, per the repo's
dependency-injection rule):

- `MemberLookup` — `GetMembers(ctx, roomID) ([]Member, error)`
- `PresenceSource` — `Snapshot(ctx, accounts []string) (map[string]Presence, error)`
  — **one bulk presence RPC per message**; see **Presence snapshot**
- `Hook` — `Allow(ctx, msg *model.Message, member roomsubcache.Member) (bool, error)`
  — an **in-process**, suppress-only predicate run as Stage 2; never a
  per-recipient external call
- `Emitter` — `Emit(ctx, evt, recipient) error` — one implementation, the
  mobile push (`mobileEmitter`)

The routing predicate is intentionally *not* an interface: it is a pure
function of already-loaded data, so it is a plain testable function rather
than an injected dependency.

### Cache integration

Member lookup moves behind the `MemberLookup` interface with a cache-backed
implementation:

1. `roomsubcache.Cache.Get(roomID)` is checked first.
2. On `valkeyutil.ErrCacheMiss`, the Mongo loader reads the `subscriptions`
   collection (the current `mongoMemberLookup` becomes that loader).
3. The loaded list populates the cache via `Set` with a configured TTL, and
   is returned.

New config (env vars, `caarlos0/env`, `SCREAMING_SNAKE_CASE`): the Valkey
connection (cluster-mode, via `pkg/valkeyutil`) and the cache TTL —
`ROOMSUBCACHE_TTL_SECONDS`, default `300` (5 minutes).

**Invalidation.** Beyond the TTL, the worker eagerly drops a room's entry on
membership change: it subscribes (fan-out, core NATS) to `SubscriptionUpdate`
and calls `roomsubcache.Invalidate(roomID)` for each event. The TTL stays as
the backstop for any event missed across a restart. See Open Question E2.

#### Projection extension — `roomsubcache.Member`

The cache currently stores `Member{ID, Account}` — deliberately minimal. The
`roomsubcache` design rejected caching the full `model.Subscription` because
its ~10 extra fields would bloat a 10k-member blob from ~600KB to ~5MB.

The exclusion filters, the routing predicate, *and* the push payload need a
handful more fields per member. Because routing is in-process, every input
it reads must already be in the member record — otherwise routing would
require a per-recipient lookup, the exact anti-pattern this design rejects.
So `Member` is extended to carry the fan-out path's full input set:

```go
type Member struct {
    ID                 string `json:"id"`
    Account            string `json:"account"`
    IsBot              bool   `json:"isBot,omitempty"`               // bots never get a mobile push
    ChineseName        string `json:"chineseName,omitempty"`
    EngName            string `json:"engName,omitempty"`
    Muted              bool   `json:"muted,omitempty"`               // per-room mute
    HistorySharedSince *int64 `json:"historySharedSince,omitempty"`  // unix ms; nil = full access
}
```

This stays consistent with the package's stated principle — it stores "only
the fields a fan-out path actually needs". When the package was written,
fan-out meant "publish to every member", so `{ID, Account}` sufficed.
Fan-out now means "decide each member's push eligibility, then build the
push event", so these fields *are* what the fan-out path needs. The cache
is, in effect, the per-member push-routing record, and the Mongo loader's
projection widens to populate the new fields on a cache miss. Per-user
`DesktopNotifications` / `AudioNotifications` preferences are deliberately
*not* carried — they govern client-side desktop banners only, which the
client computes from the message it already receives over the room-event
stream.

`ChineseName` / `EngName` are carried for a specific reason: the mobile push
payload's `Sender` needs the message author's display name. The author is
itself a room member — Stage 1 already locates them (`member.ID ==
message.UserID`, the sender-skip check) — so their display name is read
straight from the loaded member list, with no separate user-cache and no
per-message user lookup. This mirrors the `ClientMessage.Sender *Participant`
pattern. `IsBot` (from `Subscription.User.IsBot`, added in #219) gates the
push — a bot account has no device, so it never receives one.

Size cost: the bool/pointer fields use `omitempty` and cost nothing for a
member who is unmuted, unrestricted, and not a bot. `ChineseName` /
`EngName` are the only added fields most members actually carry — two short
strings each — so a 10k-member blob grows by at most a few hundred KB,
still far below both the ~5MB full-`Subscription` bloat the design rejected
and the 16MiB `Get` size cap.

The `roomsubcache` package is shared. This extension, and its tests, are
part of this work; no other consumer of the package exists today.

### Filter checks and routing

Per message the handler first computes, **once**, the values every
downstream decision reuses: the **mention set** (the message's explicit
mention IDs, plus whether it contains `@all` / `@here`) and the
**large-room flag** (member count over `LARGE_ROOM_THRESHOLD`,
default `500` — the **same env var and default** already used by
`message-gatekeeper` for its post-restriction rule; we deliberately reuse
the existing threshold so operators have a single knob for what counts as
"large") — both free, from the `MessageEvent` and the member list.
For a **thread reply** (`ThreadParentMessageID` set) it additionally needs
the thread-follower set (source — Open Question F2); the thread parent
timestamp is already on the event (`ThreadParentMessageCreatedAt`).

**Stage 1 — exclusion filters** (per recipient, in-memory, drop entirely).
Evaluated in order; the first match drops the recipient from all legs:

1. **Sender** — skip if `member.ID == message.UserID`. *(Already
   implemented.)*
2. **Mute** — skip if `member.Muted`. *(Bug fix.)*
3. **Restricted room** — skip if `member.HistorySharedSince != nil` and the
   relevant timestamp is before `*member.HistorySharedSince`. *(Bug fix.)*
   The relevant timestamp is the message's own `CreatedAt` for a channel
   message, and the thread parent's `ThreadParentMessageCreatedAt` for a
   thread reply (see **Thread messages**). Semantics confirmed from
   `history-service`: `nil` means full access; a non-nil value means the
   member sees only messages from that instant onward (inclusive).
4. **Thread non-follower** — for a thread reply not also posted to the
   channel, skip a recipient who is neither following the thread nor
   `mentioned`. Channel messages skip this check. *(See **Thread
   messages**.)*

**Stage 2 — hook veto** (per surviving recipient, in-process). `Hook.Allow`
is a suppress-only gate: it returns a single bool and may **not** modify
notification content or routing. It runs here — after the exclusion
filters, before routing and presence — so a veto drops the recipient before
any routing work is done. Signature:
`Allow(ctx context.Context, msg *model.Message, member roomsubcache.Member) (bool, error)`.
On error the handler logs and treats the result as allow (fail-open). The
no-op default always allows. Real rules are PARKED (Open Question D) — and
because the hook is in-process, any external data its real implementation
needs must be batch-loaded once per message, never fetched per recipient.

**Stage 3 — routing predicate** (per surviving recipient, pure CPU). A pure
function (`routing.go`) maps `(member record, mention set, large-room flag,
room type)` to a single boolean: is the recipient eligible for a mobile
push? `mentioned` means a direct mention or `@all`/`@here`. The recipient
is **eligible** when **all** of:

- The room is a DM, **or** the recipient is `mentioned`, **or** the room is
  not large (a plain non-mention in a room over
  `LARGE_ROOM_THRESHOLD` does not push).
- The recipient is **not a bot** (`IsBot` — a bot account has no device).

There is no separate desktop or audio leg. **Desktop banners are computed
client-side** by the browser/desktop client from the message it already
receives over the broadcast-worker room-event stream, using the user's
local `DesktopNotifications` / `AudioNotifications` preferences. The
server's push rule and the client's banner rule cover the same shape (DM /
mention / large-room semantics); both live here so they do not drift.

This is the step that replaces the blanket fan-out — for a plain message in
a large channel it returns "no push" for every non-mentioned recipient.

**Stage 4 — presence** (per message, one batched read). After Stage 3 has
produced the push-eligible set, the handler makes a single bulk call —
`PresenceSource.Snapshot(ctx, survivorAccounts)` — and decides per
recipient: push or no-push? (See **Presence snapshot** below for the full
state→push table.) The short version: only `busy` and `in-call` (DND
states) are skipped; everyone else — `online`, `offline`, `away`, and
missing/error — receives the push. **Mobile push and the client-computed
desktop banner are treated as independent channels** (multi-device
awareness): an online user gets both — a phone alert in addition to the
in-app banner. It is one bulk presence RPC per message, never a
per-recipient call. The no-op default returns an empty snapshot and every
push-eligible recipient is pushed.

Stage 1 (inline `if` statements) and Stage 3 (the `routing.go` pure
function) decide entirely from already-loaded data — testable without any
dependency, and a filter-chain framework would be premature abstraction.
Stages 2 and 4 are interfaces: the hook and the presence source wrap
behavior owned by internal services and must be swappable, and their no-op
defaults let the worker ship and run before that behavior is supplied.

#### Thread messages

`model.Message` (embedded in `MessageEvent`) already carries the thread
fields the worker needs — confirmed against the codebase:

- `ThreadParentMessageID string` — set on a thread reply (the legacy
  `ThreadMessageID`).
- `TShow bool` — the "also send to channel" flag (`tshow`; per
  `docs/cassandra_message_model.md`, "from thread [also send to channel]").
- `ThreadParentMessageCreatedAt *time.Time` — the thread parent's timestamp,
  denormalised onto the reply at write time. **No parent-message lookup is
  needed.**

A message is a thread reply when `ThreadParentMessageID != ""`. When it is a
thread reply *and* `TShow == false`, notification eligibility is narrower
than for a channel message:

- **Thread-follower exclusion** (Stage 1, check 4). A recipient who is
  *not* following the thread — i.e. has not replied to the thread parent —
  is skipped, unless they are `mentioned` (direct mention or `@all`/`@here`).
  Following or mentioned → continue; neither → drop.
- **Restricted-room check uses the parent timestamp.** The Stage 1
  restricted check compares `member.HistorySharedSince` against
  `ThreadParentMessageCreatedAt`, not the reply's own `CreatedAt` — a member
  who joined the room after the parent must not be pulled into its thread.
  `history-service` already applies exactly this rule. Legacy thread replies
  may have a `nil` `ThreadParentMessageCreatedAt`; consistent with
  `history-service`, a restricted member is then treated as **without
  access** (skip) rather than risk leaking thread history.

A thread reply with `TShow == true` is treated as an ordinary channel
message — no thread-follower exclusion, restricted check against the reply's
own `CreatedAt`.

The only per-thread-message input still needed is the **thread-follower
set**, read from `thread_subscriptions` (`model.ThreadSubscription`) by
`parentMessageId` — the authoritative "following" set, which includes the
thread's parent author (`message-worker` auto-subscribes them on the first
reply). `ThreadRoom.ReplyAccounts` is deliberately *not* used: it tracks
only accounts that have replied, so it would miss a parent author who has
not replied to their own thread. The lookup needs a `parentMessageId` index
on `thread_subscriptions`, idempotently ensured by the worker at startup. If
that collection is already large at rollout, ops can pre-create the index so
the first build does not slow worker startup — the ensure is then a no-op.

#### Presence snapshot

Stage 3's `PresenceSource` is backed by the internal presence service
through a **bulk presence RPC** (NATS request/reply): per message the worker
sends one request carrying the survivor account list and gets back each
account's status in one reply. The worker does **not** read the presence
service's Valkey directly.

Why an RPC and not a direct Valkey read: the presence service owns its
storage, and that storage is migrating from standalone Valkey to
cluster-mode Valkey. A direct read would couple this worker to the presence
service's key schema and cluster topology and pull that migration into the
notification-worker. The RPC keeps a clean service boundary — the presence
service reads its own Valkey, and the standalone→cluster migration is
invisible here.

> **Dependency — separate PR.** No bulk presence RPC exists yet — the
> presence service has no bulk presence query today. (The existing
> `status.getByName` RPC is unrelated: it serves a user's *status text* — a
> Mongo profile field — not connection presence.) This design **specifies
> the contract** below; the presence service implements the handler in
> **its own PR**. Until it lands the notification-worker ships with
> `noopPresenceSource` (empty snapshot, every eligible leg fires); the real
> `PresenceSource` is wired once the RPC is available, so this worker is
> never blocked on the presence work.

**RPC contract** (specified here — the consumer defines the interface, per
the repo rule; the presence service implements it):

- **Subject** — `chat.presence.{siteID}.request.snapshot`, a NATS
  request/reply subject with one bound `pkg/subject` builder. Site-scoped,
  since presence is per-site. (Proposed shape — the presence service
  confirms it against its own subject namespace.)
- **Request / reply** — typed `pkg/model` structs, `camelCase` JSON, over
  standard `natsutil` request/reply. The `pkg/model` types and the
  `pkg/subject` builder land with **this** (consumer) PR; the presence
  service implements the handler against them.

```go
type PresenceSnapshotRequest struct {
    Accounts []string `json:"accounts"`
}

type PresenceSnapshotReply struct {
    // keyed by account; an account absent from the map has no presence
    // record and is treated fail-open (both legs).
    Presences map[string]Presence `json:"presences"`
}

type Presence struct {
    AggregatedStatus string `json:"aggregatedStatus"` // online|offline|away|busy|in-call
}
```

On error the presence service replies with the repo-standard
`model.ErrorResponse` via `natsutil.ReplyError`; the worker treats any RPC
error as fail-open (see the table below). The presence service folds manual
status overrides into `AggregatedStatus`, so it is the single field routing
needs. It is one of five
values:

- `online` — an active websocket connection.
- `offline` — no active connection.
- `away` — a connection idle past the idle-timeout.
- `busy` — Do-Not-Disturb, set manually by the user.
- `in-call` — an external integration (e.g. Teams) reports the user in a
  meeting.

`PresenceSource.Snapshot(ctx, accounts)` makes the bulk RPC for the
push-eligible set (split across several requests for very large rooms — see
Performance considerations) and returns `map[account]Presence`. A pure
function then maps each account's status to a single push-or-not decision:

| `aggregatedStatus` | Push? | Rationale |
|---|:--:|---|
| `online`  | yes | multi-device awareness — push fires alongside the client-computed desktop banner |
| `offline` | yes | not connected — reach them by push |
| `busy`    | no  | Do-Not-Disturb — suppress the push |
| `in-call` | no  | treated as DND — see note |
| `away`    | yes | treated as fail-open — see note |
| *account absent / RPC error* | yes | fail-open — never drop a notification on a presence gap |

**`in-call`** collapses into `busy`: the user is actively engaged on a
call, so a mid-call phone buzz is unwanted; the push is suppressed (the
message is still in the room). This mirrors Microsoft Teams, which mutes
notifications during calls and meetings by default.

**`away`** is an automatic idle state, *not* Do-Not-Disturb. The user may
have stepped away from the desktop, so the worker pushes — same treatment
as a missing account (fail-open). If they are still at the desk they will
see the banner the client computes from the room event anyway; if they
have wandered off the push reaches their phone.

The per-recipient outcome is a single push-or-not boolean — computed
in-memory from the one bulk reply, never through a per-recipient call.

### Delivery — mobile push leg

The worker has a **single emit leg: mobile push**. If Stages 1–4 leave a
recipient eligible *and* the presence check says "push," the worker hands
off to the internal push-notification service:

- A JetStream publish to the single subject
  `chat.server.notification.push.{siteID}.send` (under the server-only `chat.server.*` namespace (per
  `docs/nats-subject-naming.md`); bound to the `PUSH_NOTIFICATIONS_{siteID}`
  stream. Client JWTs have no permissions on `chat.server.*`, so no browser
  tab can subscribe — important now that notification payloads are not
  encrypted (the team's decision).
- The worker does not contact APNs/FCM and does not store device tokens; it
  publishes the push event and moves on.
- The event's `Sender` is filled from the message author's member record
  (already in the loaded member list — see the projection extension) — no
  extra lookup.

**Desktop banners are not a server leg.** The browser/desktop client
computes them locally from the messages it already receives via the
broadcast-worker room-event stream, using the user's local
`DesktopNotifications` / `AudioNotifications` preferences. This worker
emits no desktop subject and builds no `NotificationEvent` — fewer subjects,
smaller surface, and no server-emitted side-channel for unencrypted
notification content. The trade-off: the server-side push rule (here) and
the client-side banner rule must stay consistent — both live in **Filter
checks and routing** so they do not drift.

The single emit sits behind the `Emitter` interface, so it is independently
unit-testable with recorded publishes.

**The push service is not built here.** It already exists in the team's
internal codebase, owning device-token storage and APNs/FCM delivery. This
work builds only `mobileEmitter` — a thin JetStream producer. The contract
between the two is fixed (see below).

#### Mobile push hand-off contract

- **Transport** — the worker publishes every push event on a **single
  subject**: `chat.server.notification.push.{siteID}.send`, via the
  `subject.PushNotification(siteID)` builder — under `chat.server.*`,
  server-only, clients have no JWT permission to subscribe. The recipient
  account is **not in the subject**; it travels in the event payload
  (`account`). This is a classic queue-of-events shape — the push service
  drains the stream and resolves device tokens from the payload. The stream
  binds the filter `chat.server.notification.push.{siteID}.>` so the `.send`
  leaf is the current event type and there is room for future siblings
  (`.silent`, `.priority`, etc.) without restructuring. The stream is
  **owned by the push service / ops**; the worker only publishes and never
  creates it, so `bootstrap.go` is unchanged — consistent with the repo's
  stream-bootstrap ownership rule.
- **Granularity** — one event per recipient; no batching.
- **Delivery model** — fire-and-forget. The worker publishes and relies on
  the JetStream PubAck for durability; it does not wait for the push service
  to process or deliver. The push consumer acks independently.
- **Token resolution** — the worker sends `account` only; the push service
  resolves device tokens internally.
- **Dedup** — since the subject no longer encodes the recipient, the
  `Nats-Msg-Id` header is set to `{messageID}-{account}` so JetStream's
  per-stream dedup window distinguishes per-recipient publishes (a
  same-message redelivery on `MESSAGES_CANONICAL` does not produce
  duplicate pushes).

Event schema, in `pkg/model/push.go` (NATS payloads are typed `pkg/model`
structs). JSON tags shown; every field also carries a matching `bson` tag
per the repo struct-tag rule:

```go
type PushNotificationEvent struct {
    ID        string               `json:"id"`        // "{messageID}-{account}"
    Account   string               `json:"account"`   // recipient account; push-side device-token key
    Title     string               `json:"title"`     // room name, or sender name for DMs
    Body      string               `json:"body"`      // truncated message content
    Data      PushNotificationData `json:"data"`
    RoomID    string               `json:"roomId"`
    Timestamp int64                `json:"timestamp"` // event publish time, UnixMilli
}

type PushNotificationData struct {
    RoomID            string       `json:"roomId"`           // mirrors the event-level RoomID (legacy payload shape)
    MessageID         string       `json:"messageId"`
    Type              string       `json:"type"`             // "c" channel / "d" DM / "p" private
    Sender            *Participant `json:"sender,omitempty"` // author display info, from the member record
    ThreadMessageID   string       `json:"threadMessageId,omitempty"`
    FileName          string       `json:"fileName,omitempty"`
    FileType          string       `json:"fileType,omitempty"`
    ParentRoomID      string       `json:"parentRoomId,omitempty"`
    PushTime          string       `json:"pushTime"`         // RFC3339; domain-level send time
    AlsoSendToChannel bool         `json:"alsoSendToChannel,omitempty"`
}
```

Three deliberate departures from the legacy schema:

1. **`Timestamp int64` is added.** Every NATS event struct in `pkg/model`
   must carry an event-level timestamp, set at the publish site via
   `time.Now().UTC().UnixMilli()`. It is distinct from `PushTime` — the
   domain-level RFC3339 send time the legacy schema already carried.
2. **Cryptic tags are spelled out.** The legacy `rid` / `tmid` / `prid`
   JSON tags become `roomId` / `threadMessageId` / `parentRoomId` to satisfy
   the `camelCase` JSON-tag rule. `PUSH_NOTIFICATIONS_{siteID}` is a *new*
   stream with no existing consumer, so there is nothing to stay
   backward-compatible with: the push service is wired to this schema once,
   as a single clean cutover — no dual old/new tag support.
3. **Sender is a `*Participant`.** The legacy flat `chineseName` / `engName`
   fields collapse into one `Sender *Participant`, matching the
   `ClientMessage.Sender` pattern. It is populated from the message author's
   member record (see the projection extension), so no user lookup is added.

### Files touched

| File | Change |
|---|---|
| `notification-worker/handler.go` | changed — pipeline orchestration; per-message mention set + large-room flag; inline Stage-1 exclusion filters; consumer interface definitions |
| `notification-worker/members.go` | new — `cachedMemberLookup` (cache + Mongo loader; single-flight loader + in-process L1 — see Performance considerations) |
| `notification-worker/threads.go` | new — thread-follower lookup (`thread_subscriptions` by `parentMessageId`) + `parentMessageId` index ensure |
| `notification-worker/routing.go` | new — pure routing predicate (Stage 3) |
| `notification-worker/presence.go` | new — `PresenceSource` interface, bulk-RPC implementation + status→legs mapping, and no-op default |
| `notification-worker/hook.go` | new — in-process `Hook` interface + no-op default |
| `notification-worker/emit.go` | new — `mobileEmitter` (single emit leg) |
| `pkg/model/push.go` | new — `PushNotificationEvent` / `PushNotificationData` NATS payload structs |
| `pkg/model/presence.go` | new — `PresenceSnapshotRequest` / `PresenceSnapshotReply` / `Presence` bulk-presence-RPC contract types |
| `pkg/subject/subject.go` | changed — adds three builders: `PresenceSnapshot(siteID)` for the bulk presence RPC, `PushNotification(siteID)` for the single mobile push subject (bound to `PUSH_NOTIFICATIONS_{siteID}` via filter `chat.server.notification.push.{siteID}.>`), and a `SubscriptionUpdate` wildcard helper for the eager-invalidation subscription |
| `notification-worker/main.go` | changed — Valkey wiring, config additions, pipeline assembly; the `SubscriptionUpdate` cache-invalidation subscription |
| `notification-worker/bootstrap.go` | unchanged — `PUSH_NOTIFICATIONS_{siteID}` is owned by the push service / ops; this worker only publishes |
| `notification-worker/deploy/docker-compose.yml` | changed — add a Valkey dependency for local dev |
| `pkg/roomsubcache/roomsubcache.go` | changed — `Member` projection extension |
| `notification-worker/*_test.go` | new/changed — unit tests per file; `integration_test.go` extended for the cache path |
| `docs/client-api.md` | changed — note the mute/restricted exclusions and routing on the `notification` event |

### Error handling

- A cache **miss** is normal control flow (`errors.Is(err,
  valkeyutil.ErrCacheMiss)`), not an error — fall through to the Mongo
  loader.
- A cache **error** (not a miss) falls back to the Mongo loader so
  notifications still go out; logged via `slog`, not fatal.
- A Mongo loader error fails the message (`Nak`) so JetStream redelivers —
  same as today.
- Emitter errors are logged per leg and do not abort the other leg or the
  message `Ack` — consistent with the current handler.
- Presence snapshot errors **fail-open**: a failed bulk read yields an empty
  snapshot, so every eligible leg fires (a missed presence narrowing is
  better than a dropped notification). Hook errors are logged and fail-open
  (allow). The no-op defaults never error.

### Performance considerations

The hot path is sound — batched reads, in-process routing, no per-recipient
RPC — so the remaining costs are implementation details plus one inherent
worst case (`@all` to a very large room is always N notifications). Three
items shape the implementation:

**1. Async mobile publish — required for v1.** The worker's one emit leg
is a JetStream publish, which waits for a PubAck. A sync `js.Publish` in
the per-recipient loop turns an `@all` to a 10k-member room into 10k
*sequential* publish→ack round-trips, pinning a worker goroutine for the
whole fan-out. `mobileEmitter` must use `js.PublishAsync` with a bounded
in-flight window and drain `PublishAsyncComplete()` — durability (the
PubAck) is kept, per-recipient serialization is not.

**2. Single-flight on the Mongo loader — v1.** When a hot room's Valkey
entry expires, every concurrently-processing message for that room misses at
once and each runs the (large) `subscriptions` query. A `singleflight.Group`
keyed by `roomID` around the loader collapses concurrent misses into one
query — a few lines, and it prevents a TTL-expiry stampede.

**3. In-process member-list cache — v1 recommended, may be a fast-follow.**
Routing needs the full member list for *every* message, so every message —
not just `@all` — does a `roomsubcache.Get` (multi-MB Valkey payload) plus a
JSON decode. A hot 10k-member room at high message rate repeats that
fetch+decode constantly. A small in-process LRU of decoded `[]Member` (keyed
by `roomID`, short TTL of a few seconds, slice treated read-only) inside
`cachedMemberLookup` collapses it to roughly one fetch+decode per room per
few seconds. It is the broadest steady-state win; it can ship in a
fast-follow if it would complicate the initial cache layer.

**4. Chunked presence RPC — v1.** For an `@all` to a very large room the
Stage-4 survivor set is many thousands of accounts; the bulk presence RPC's
request (the account list) and reply (the statuses) must each stay under the
NATS max message size (~1 MB default). `PresenceSource.Snapshot` therefore
splits a large survivor set across several bulk RPCs — a bounded batch size
(~512–1000 accounts), issued concurrently — and merges the replies. For
ordinary (non-`@all`) messages Stage 3 empties the survivor set first, so
this stays a single small request. The presence service's own Valkey access
— including any cluster-mode key-distribution concern — lives entirely
behind the RPC and is not this worker's problem.

**Awareness — no change needed:**

- Under a presence outage every recipient fails open to *both* legs — a
  load spike, but the correct trade against dropping notifications.

### Testing

Per the repo TDD rules (Red-Green-Refactor, ≥80% coverage, `-race`):

- **Unit** — table-driven handler tests for the Stage-1 exclusion filters
  (sender / mute / restricted / thread non-follower, including the
  thread-parent-timestamp restricted variant); the `routing.go` predicate is
  a pure function, exhaustively table-tested across the DM / mention /
  `@all` / `@here` / large-room / bot combinations;
  `cachedMemberLookup` tested with a fake cache + fake loader covering hit,
  miss-then-populate, and cache-error fallback; the no-op presence/hook
  defaults and the `mobileEmitter` tested with recorded publishes.
- **Integration** (`//go:build integration`) — extend `integration_test.go`
  to cover the cache path against a real Valkey + Mongo via `pkg/testutil`
  (`SharedValkeyCluster`, `MongoDB`).
- **`roomsubcache` package** — extend its tests for the new `Member` fields
  (JSON round-trip, `omitempty` behavior).

## Open Questions

These depend on services in the internal codebase and must be resolved there
before the implementation plan is finalized.

### A. Push notification service (mobile hand-off) — RESOLVED

The worker/push-service contract is fixed; the full schema and rules are in
**Mobile push hand-off contract** in the Design section.

- **A1.** Transport — JetStream stream.
- **A2.** Stream `PUSH_NOTIFICATIONS_{siteID}`, owned by the push service /
  ops; the worker only publishes and never creates it.
- **A3.** Schema — `PushNotificationEvent` / `PushNotificationData` (see the
  Design section). Two repo-convention departures from the legacy schema: an
  event-level `Timestamp` field is added, and the cryptic `rid` / `tmid` /
  `prid` tags are spelled out to `roomId` / `threadMessageId` /
  `parentRoomId`.
- **A4.** Worker sends `account` only; the push service resolves device
  tokens internally.
- **A5.** Fire-and-forget — the worker publishes and relies on the JetStream
  PubAck for durability; it does not wait for the push service to process or
  deliver.
- **A6.** One event per recipient; no batching.

**Remaining coordination item:** the `roomId` / `threadMessageId` /
`parentRoomId` tag renames are a contract change — the internal push service
must be updated to read the new tag names in the same coordinated change
(or, if that is not acceptable, the worker keeps the legacy tags).

### B. User presence / status — RESOLVED

The worker reads presence as a single batched lookup per message; the schema
and state mapping are in **Presence snapshot** in the Design section.

- **B1.** Source — a **bulk presence RPC** (NATS request/reply): one request
  per message carrying the survivor account list, one reply with their
  statuses. The worker does *not* read the presence service's Valkey
  directly — that would couple this worker to the presence service's storage
  and its in-progress standalone→cluster Valkey migration. The RPC contract
  (subject + request/reply schema) is **specified in this design** (see
  **Presence snapshot**); the presence service implements the handler in its
  own PR. Until it lands `noopPresenceSource` ships and the worker is
  unblocked. (The existing `status.getByName` RPC is unrelated — it serves a
  user's *status text*, a Mongo profile field, not connection presence.)
- **B2.** Contract: subject `chat.presence.{siteID}.request.snapshot`;
  request `PresenceSnapshotRequest{accounts}`; reply
  `PresenceSnapshotReply{presences: map[account]Presence}`. The `pkg/model`
  types and `pkg/subject` builder land with this (consumer) PR. The worker
  needs only `aggregatedStatus` from each reply entry — the presence service
  folds manual overrides in.
- **B3.** Status→push mapping confirmed: only `busy`/`in-call` skip the
  push (DND); `online`, `offline`, `away`, and missing all receive the
  push. Mobile push and the client-computed desktop banner are treated as
  independent channels — an online user receives both, providing
  multi-device awareness.
- **B4.** Fail-open — an account missing from the reply, or an RPC error,
  yields a push; a presence gap never drops a notification.

### C. Routing rule and preference fields — RESOLVED

- **C1.** `Member` carries `IsBot`, `Muted`, and `HistorySharedSince`. The
  per-user `DesktopNotifications` / `AudioNotifications` preferences are
  *not* carried — they govern client-side desktop banners only, computed by
  the client from the message it already receives. Highlight keywords are
  **dropped for v1** — not in the projection, not in routing.
- **C2.** Large-room threshold = 500 members, configurable via
  `LARGE_ROOM_THRESHOLD` — the same env var already consumed by
  `message-gatekeeper` for its post-restriction rule, reused here so
  operators have a single "large room" knob.
- **C3.** Server-side routing is mobile-push only — desktop banners are
  client-side. See the Stage-4 presence table for push-vs-no-push.
- **C4.** Confirmed — `Muted` and the restricted-room check are Stage-1
  exclusions and drop the recipient from the push.
- **C5.** Confirmed — `@here` is treated as `@all` for push eligibility.

The server's push rule and the client's banner rule cover the same shape
(DM / mention / large-room semantics) and are documented together in
**Filter checks and routing** to prevent drift.

**Confirmed:** highlight keywords are dropped for v1 — re-introducing them
is logged under Future work.

### D. Hook handler — RESOLVED

- **D1.** Suppress-only — `Allow` returns a single bool (allow / block); the
  hook may not modify notification content or routing.
- **D2.** In-process predicate — never a per-recipient external call.
- **D3.** Signature `Allow(ctx context.Context, msg *model.Message,
  member roomsubcache.Member) (bool, error)`. It runs as pipeline **Stage
  2** — after the exclusion filters, before the routing predicate and the
  presence snapshot — so a veto skips the recipient before any routing work.
- **D4.** Fail-open — on error, log and treat the result as `Allow == true`.

### E. Cache freshness — RESOLVED

- **E1.** TTL = 5 minutes, configurable via `ROOMSUBCACHE_TTL_SECONDS`
  (default `300`).
- **E2.** v1 does **eager invalidation**, with the TTL as the backstop. The
  worker adds one core-NATS subscription on `SubscriptionUpdate` (wildcard
  over the account token): `room-service` and `room-worker` publish a
  `SubscriptionUpdateEvent` there on every membership change — member add,
  member remove, role update, mute toggle — and cross-site changes surface
  the same way (`inbox-worker` processes the INBOX event, `room-worker`
  re-publishes locally). The handler reads `event.Subscription.RoomID` and
  calls `roomsubcache.Invalidate(roomID)`. Two consequences worth stating:
  - The subscription is **fan-out, not a queue group** — every worker
    instance must hear the event to drop its in-process L1 member cache
    (see Performance considerations).
  - `SubscriptionUpdate` is core NATS (ephemeral), so an event missed during
    a worker restart is simply recovered at the next TTL expiry. Eager
    invalidation only shortens convergence; the 5-minute TTL stays the
    correctness guarantee. A `HistorySharedSince` change relies on the TTL
    if it does not emit a `SubscriptionUpdateEvent`.

### F. Thread message data sources

- **F1. RESOLVED.** The canonical `MessageEvent` embeds `model.Message`,
  which already carries every thread field the worker needs:
  `ThreadParentMessageID`, `TShow` (the also-send-to-channel flag), and
  `ThreadParentMessageCreatedAt` (the parent timestamp, denormalised at
  write time). No parent-message lookup is required. One caveat: legacy
  thread replies may carry a `nil` `ThreadParentMessageCreatedAt` — handled
  conservatively (see **Thread messages**).
- **F2. RESOLVED.** The thread-follower set is read from `thread_subscriptions`
  (`model.ThreadSubscription`) by `parentMessageId`. `ThreadRoom.ReplyAccounts`
  is rejected: it tracks only accounts that have *replied*, so it omits the
  thread's parent author — whom `message-worker` auto-subscribes on the first
  reply, and who should be notified of replies to their own thread.
  `thread_subscriptions` is the authoritative "following" set and includes
  them. The lookup needs a `parentMessageId` index on `thread_subscriptions`
  (the field exists; today only `(threadRoomId, userAccount)` is indexed) —
  index creation is idempotent, so the worker ensures it at startup. One
  indexed Mongo read per thread-only message, on the notification path only;
  acceptable, since thread replies are lower-volume than channel messages. A
  TTL'd `threadsubcache` (invalidated on new-reply events) is Future work.

## Future work

- **`threadsubcache`.** A TTL'd cache of thread-follower sets, invalidated on
  new-reply events — removes the per-thread-message Mongo read (F2) for hot
  threads. Mirrors `roomsubcache`.
- **Highlight keywords.** The legacy per-user highlight-keyword match is
  dropped for v1 (not in the `Member` projection, not in routing).
  Re-introducing it — a `HighlightWords []string` projection field plus a
  routing check — is a future enhancement.
- **Encrypted-room mobile pushes.** broadcast-worker encrypts message
  content at delivery (`roomcrypto` + `roomkeystore`), but the canonical
  message the notification-worker consumes is plaintext. For an encrypted
  room the mobile push `Body` preview would reach APNs/FCM in the clear —
  defeating room encryption. Handling it: send a contentless push (sender
  name only, no body). v1 ships content-bearing pushes; this is gated on
  the `encrypt` switch being enabled. (Desktop banners are no longer a
  worker concern — the client computes them after decrypting messages with
  the room key.)
- **PII audit of the push payload.** Review which `PushNotificationData`
  fields are genuinely required on the wire to the push service and the
  device — sender display names, message body, file metadata — and minimise
  the PII that leaves the worker (drop or tokenise anything not needed to
  render the notification).
- **Per-user notification rate-limiting.** A cap on notifications emitted
  per recipient per time window, to damp notification spam (e.g. repeated
  `@all`s in a busy room). Enforced in the worker, ahead of the emit step.
