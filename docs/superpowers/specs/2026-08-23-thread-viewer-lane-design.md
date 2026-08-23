# Thread Viewer Lane — live thread events for non-followers

**Date:** 2026-08-23
**Status:** Proposed
**Related:** `2026-05-28-broadcast-worker-thread-handling-design.md`, `2026-07-28-local-global-room-subjects.md`

A user who opens a thread they have never participated in receives no live
reply events. This adds a per-thread NATS subject that any room member can
subscribe to while the thread pane is open, and unsubscribe from when it
closes. It is a delivery change only: no new persisted state, no change to who
counts as a thread follower.

## 1. Problem

In a channel room, a thread reply is **not** published room-wide. It fans out
per-account on `chat.user.{account}.event.room` to a recipient set built by
`threadFanOutAccounts` (`broadcast-worker/handler.go:1081`):

1. the reply author,
2. the thread parent's author,
3. thread followers — `thread_rooms.replyAccounts` (`broadcast-worker/store_mongo.go:162`),
4. history-gated @-mentioned accounts.

Everyone else in the room receives only `ThreadMetadataUpdatedEvent`
(`pkg/model/event.go:434`), a content-free badge carrying `newTcount`,
`parentMessageId`, `replyMessageId` and `action`. It is enough to update a
reply-count badge and nothing else.

Nothing makes a viewer a follower. There is no follow/subscribe RPC; `Get
Thread Messages` emits nothing, and `Mark Thread as Read` is an idempotent
no-op for a caller with no `ThreadSubscription`. Membership in the fan-out set
is earned only by replying, authoring the parent, or being @-mentioned
(`message-worker/handler.go:330`, `:370`, `:635`).

So a user reading a thread they have not participated in sees a static
transcript. The reply count ticks up; the replies never arrive.

The same gap applies to edits and deletes of thread replies
(`handleThreadUpdated` at `broadcast-worker/handler.go:356`,
`handleThreadDeleted` at `:405`) — both fan out to followers only. A viewer can
sit on content that has since been edited or deleted.

**Not affected:** DM and BotDM rooms, where thread replies already reach every
non-bot member (`publishDMEvents` for creates, `publishMutation` for
edits/deletes). Reactions are also already room-wide for thread replies —
`handleReacted` routes through `publishMutation` regardless of thread status.

## 2. Decisions taken

Recorded here because each closes off a design space that would otherwise be
reopened during implementation.

**Ephemeral, not durable.** Opening a thread grants live delivery for as long
as the pane is open. It writes nothing: no `ThreadSubscription`, no
`replyAccounts` entry, no thread-list row, no unread accumulation, no
notification, no effect on `tcount`. Auto-follow-on-view was considered and
rejected — it conflates "I glanced at this" with "keep telling me about this",
and it would put every glanced-at thread into the user's thread list.

**A subject the client subscribes to, not a server-side viewer registry.** The
alternative was a "viewing thread X" RPC with a Valkey TTL set and heartbeats,
with broadcast-worker unioning live viewers into the per-account fan-out. That
keeps content on the already-authenticated per-account lane, but introduces
per-viewer server state, heartbeat traffic, TTL expiry semantics, and a store
dependency on the message hot path — to deliver a feature whose entire
lifetime is "while a UI panel is open". A subject costs none of that.

**A dedicated per-thread subject, not the existing room subject.** Publishing
`new_thread_message` room-wide on `chat.room.{roomID}.event` would be a
smaller diff, but every member's socket would then carry every thread reply in
the room. A per-thread subject delivers only to sockets that asked for that
thread.

**Full content on the new lane, encrypted.** See §5.

## 3. Subject and routing

New builders in `pkg/subject`:

```go
// ThreadEvent returns the per-thread live lane for a channel room's thread.
func ThreadEvent(roomID, parentMessageID string, global bool) string {
	return roomBase(roomID, global) + ".thread." + parentMessageID + ".event"
}

// ThreadEventTargets returns the thread-lane subject(s) to publish to, routing
// identically to RoomEventTargets.
func ThreadEventTargets(roomID, parentMessageID string, crossSite *bool,
	crossSiteAt *time.Time, mode RoomRouteMode, now time.Time) []string
```

Resulting subjects:

- global: `chat.room.{roomID}.thread.{parentMessageId}.event`
- local: `chat.local.room.{roomID}.thread.{parentMessageId}.event`

`ThreadEventTargets` MUST delegate to the existing `roomRouteGlobals`
(`pkg/subject/subject.go:421`), the same helper behind `RoomEventTargets` and
`RoomMemberEventTargets`. This is not cosmetic reuse: it is what makes the
thread lane honor `ROOM_SUBJECT_MODE`, the sticky `crossSite` flag, and the
post-flip locality grace window. A hand-rolled subject would silently lose
delivery for same-site rooms the moment local mode is enabled, and would lose
dual mode's global safety-net publish.

**Permissions: no change required.** The client JWT already grants
`Sub allow: chat.user.{account}.>, chat.room.>, chat.local.room.>, _INBOX.>,
chat.user.presence.state.*` (documented at `auth-service/handler.go:313`).
Because the new subject sits under `roomBase`, it falls inside the existing
`chat.room.>` / `chat.local.room.>` grants. No platform-team signing-template
change, no `docker-local/setup.sh` change.

**Cardinality.** One subject per (room, thread parent). NATS subjects are
interest-based and cost nothing when nobody is subscribed, so an inactive
thread's subject is free. A client holds at most one such subscription at a
time (the open pane).

## 4. broadcast-worker changes

Each of the three thread handlers gains one additional publish **after** its
existing per-account fan-out. The existing lane is untouched.

| Handler | Existing (unchanged) | Added |
|---|---|---|
| `handleThreadCreated` (`handler.go:241`) | `new_thread_message` → `publishToThreadAccounts` | encrypted copy → thread lane |
| `handleThreadUpdated` (`handler.go:356`) | `message_edited` → `publishToThreadAccounts` | encrypted copy → thread lane |
| `handleThreadDeleted` (`handler.go:405`) | `message_deleted` → `publishToThreadAccounts` | same payload → thread lane |

Only the `model.RoomTypeChannel` branch of each handler is touched. The
DM/BotDM branches already reach every member and are left alone.

New helper, mirroring `publishRoomEvent` (`handler.go:884`):

```go
func (h *Handler) publishThreadLaneEvent(ctx context.Context, roomID, parentMsgID string,
	crossSite *bool, crossSiteAt *time.Time, payload []byte, op string) error
```

It loops `subject.ThreadEventTargets(...)` and publishes each target, with the
same sequential loop and last-error-wins aggregation as `publishRoomEvent`. The
signature mirrors `publishRoomEvent`'s deliberately: `handleThreadCreated`
holds a `roommetacache.Meta` (from `GetRoomMeta`) while `handleThreadUpdated`
and `handleThreadDeleted` hold a `*model.Room` (from `GetRoom`). Taking the
`crossSite` / `crossSiteAt` pair as parameters lets all three call it without
converting between the two types.

**Not `publishToThreadAccounts`'s shape.** That helper
(`handler.go:1021`) fans out to N accounts with one goroutine per account, a
`WaitGroup`, and an all-N-failed error rule. The thread lane is a single
subject (two only during `RouteDual`), so goroutines would be overhead and
"all publishes failed" would be meaningless. Three specifics follow from that:

- **Metrics label: a new `thread_lane` room kind, not `thread`.** The
  `roomKindLabel` enum (`broadcast-worker/nats_metrics.go:15`) is closed, and
  its doc comment states an invariant for `thread`: fan-out recipients and
  delivery attempts are directly comparable because there is one publish per
  recipient. The thread lane is one publish to an audience the publisher cannot
  observe — the same shape as the channel room-stream case the comment
  explicitly excludes from that ratio. Reusing `thread` would corrupt a series
  that is documented as comparable. Add `roomThreadLane roomKindLabel =
  "thread_lane"` to the enum and to `allRoomKinds`. Record **deliveries only**;
  do not record a fan-out recipient count, because there is no honest number to
  record.
- **Never call `natsmetrics.MarkTerminalFromContext`.** `publishToThreadAccounts`
  marks `TerminalPublishExhausted` on partial failure. That is a terminal
  disposition on the underlying canonical JetStream message. The thread lane is
  best-effort and must not influence how that message's redelivery is accounted.
- **Flow log carries no audience count.** The channel path logs
  `"audience", meta.UserCount` at its call site. The thread lane has no such
  number (see below). Log `delivery="thread-lane"` with the room and parent
  message ids and no recipient figure — an invented count is worse than none.

**Failure isolation.** A thread-lane publish failure MUST NOT fail the handler
or block the per-account fan-out — it is logged and swallowed, the way
`publishThreadBadge` (`handler.go:562`) already treats best-effort badge
publishes. The per-account lane remains the delivery guarantee for followers;
the thread lane is a convenience for viewers whose pane is open right now. A
NAK-and-redeliver on a thread-lane failure would re-fan-out the per-account
copy too, turning a cosmetic miss into duplicate delivery for everyone.

### Cost when nobody is subscribed

Core NATS is interest-based: with no subscriber registered for the subject, the
server discards the message on arrival. The publisher is unaffected in every
way that would matter operationally — `Publish` is fire-and-forget
(at-most-once), so there is **no error, no blocking, no queue growth, and no
back-pressure**. A thread nobody has open cannot break or slow the worker.

The cost that *is* paid on every channel thread reply, watched or not:

1. `sonic.Marshal` of the `ClientMessage`
2. the AES encode inside `encryptRoomEvent`
3. `sonic.Marshal` of the encrypted envelope
4. the write into the connection buffer and its syscall

So each thread reply carries roughly two extra marshals and one encryption
whether or not a single person is watching — and most threads are unwatched
most of the time. For scale: the same handler already performs N per-account
publishes (N = fan-out size) plus one or two badge publishes, so this is one
more publish on a path that already does several. It is a real cost on the
message hot path, not a rounding error, but it is proportionate.

**There is no cheap way to avoid it.** Core NATS gives a publisher no API to
ask whether a subject currently has subscribers, so "check before publishing"
is not available. The only designs that avoid the wasted work are a config flag
to disable the lane per deployment, or the viewer-registry approach rejected in
§2 — publishing only to known-live viewers is precisely that approach's one
advantage, bought with server state and heartbeats.

**Recommendation: accept the cost, but make it observable.** The `thread_lane`
delivery counter above is what makes a later decision data-driven rather than
speculative. Revisit only if it shows the lane is a material share of
broadcast-worker's publish volume.

**Cross-site.** For a `crossSite=true` room the subject is the global
`chat.room.>` namespace. NATS gateways propagate on interest, so a thread with
no subscriber anywhere should not generate sustained cross-gateway traffic.
Confirm this with the platform team before rollout rather than treating it as
established — it depends on the supercluster's gateway configuration, which is
outside this repo.

**Extend `.semgrep/room-subject.yml`.** The `room-subject-publish-must-route`
rule (`.semgrep/room-subject.yml:2`) exists to stop anyone passing an inline
`subject.RoomEvent(...)` to a publish call and bypassing routing. Add
`subject.ThreadEvent` to that rule so the thread lane inherits the same
protection.

## 5. Encryption and what is visible on the open lane

`chat.room.>` is subscribable by **any** authenticated user, member or not.
Room-lane confidentiality does not come from the subject; it comes from the
payload. `publishChannelEvent` runs `encryptRoomEvent` (`handler.go:835`)
before publishing, which encrypts the `ClientMessage` with the room key and
sets `evt.Message = nil`. Room keys reach members only, via
`chat.user.{account}.event.room.key`.

Thread replies are plaintext today precisely because the per-account subject is
scoped by the recipient's own JWT. Moving them to the room namespace therefore
requires the payload-level lock.

**Rules:**

1. The thread-lane copy of `new_thread_message` MUST go through
   `encryptRoomEvent`, exactly as `publishChannelEvent` does.
2. The thread-lane copy of `message_edited` MUST go through
   `encryptEditedContent` (`handler.go:802`), which sets `EncryptedNewContent`
   and clears `NewContent`.
3. `message_deleted` carries no message content (`buildDeleteRoomEvent`,
   `handler.go:782`) — no encryption step, one payload serves both lanes.
4. The per-account copy stays **plaintext**. Encrypting it would change the
   wire format for existing clients on a lane that does not need it.
5. **Keep `Mentions` and `MentionAll` on the thread-lane copy**, identical to
   the per-account copy. The frontend renders mentions with dedicated styling
   driven by this resolved `Participant` list (account -> display name), so
   dropping it would make a viewer's thread pane render mentions differently
   from a follower's — breaking the equivalence this design exists to provide.
   These two fields are already plaintext on the encrypted channel lane for
   every ordinary message, so carrying them on the thread lane extends an
   accepted exposure rather than creating a new one.

**Consequence: two encodings.** Both encryption helpers mutate their event in
place and clear the plaintext field, so the implementation must marshal and
publish the plaintext copy to the per-account lane **first**, then encrypt a
separate value for the thread lane. Cost per thread reply when encryption is
on: one extra `sonic.Marshal` plus one `Encode`. The room key comes from
`currentRoomKey` (`handler.go:821`), already served by the key store's cache —
no additional DB read.

**Consequence: `ENCRYPTION_ENABLED=false` deployments.** The flag defaults to
`false` (`broadcast-worker/main.go:37`). With it off, `encryptRoomEvent`
returns early and thread-lane content is plaintext on a lane any authenticated
user can subscribe to. This is the same exposure ordinary channel messages
already have in that configuration — it is not a new class of leak — but it
**is** a reduction for thread replies specifically, which are currently the
best-protected message content in the system. Production runs with encryption
enabled. Anyone standing up a deployment with it disabled must understand that
thread content is no longer better-protected than channel content.

Fields that remain plaintext on the thread lane, matching the existing channel
lane: `roomId`, `roomName`, `roomType`, `siteId`, `userCount`, `lastMsgAt`,
`lastMsgId`, `timestamp`, `eventTimestamp`, `mentions`, `mentionAll`, and on
edits/deletes the message id and actor account. No new field is exposed that
the channel lane does not already expose.

## 6. Client contract

Opening a thread:

```
subscribe chat.room.{roomID}.thread.{parentMessageId}.event
```

using the same `crossSite`-derived local/global prefix the client already uses
to choose between `chat.room.{roomID}.event` and
`chat.local.room.{roomID}.event`. No new locality concept.

Closing the thread: unsubscribe. That is the whole lifecycle — no RPC, no
server state, no TTL, no heartbeat, nothing to clean up if the client
disconnects.

Payloads are the existing `RoomEvent` / `EditRoomEvent` / `DeleteRoomEvent`
shapes, with the encryption and mention-stripping rules of §5. No new event
type and no new model struct.

## 7. Duplicates and ordering

**A follower who opens the thread receives every event twice** — once on
`chat.user.{account}.event.room`, once on the thread lane. A NATS publish
cannot exclude subscribers, so this is structural, not a defect to engineer
around.

It costs nothing to handle because clients must already deduplicate:
broadcast-worker consumes MESSAGES-CANONICAL with `AckExplicitPolicy`
(`pkg/stream/consumer.go:38`), so any redelivery re-runs the whole fan-out.
At-least-once was already the contract.

**Client rule: every message insert is an upsert keyed by `message.id`, never
an append.** The two copies of one reply differ in encoding — one plaintext,
one encrypted — so the client must key on id, not on payload equality. Whichever
arrives second overwrites the first with identical decoded content.

`thread_metadata_updated` is unchanged and still room-wide. With the pane open
the viewer now gets replies live, so refetch-on-badge becomes the closed-pane
fallback rather than the primary path. The client should refetch only when
`replyMessageId` is absent from its cache.

Ordering across the two lanes is not guaranteed. Clients already order by
`eventTimestamp` (the canonical publish time) in preference to `timestamp`;
that rule is unchanged and sufficient.

## 8. Testing

TDD per CLAUDE.md §4 — tests first, confirmed red, then implementation.

**`pkg/subject/subject_test.go`** — table-driven `ThreadEventTargets` over the
`crossSite` (nil / true / false) × `RoomRouteMode` (global / dual / local) ×
grace-window (inside / outside) matrix, mirroring the existing
`RoomEventTargets` cases, asserting subject strings and ordering (local before
global).

**`broadcast-worker/handler_test.go`**, for each of the three handlers:

- thread lane receives the event, on the correct subject(s)
- per-account lane still receives its existing payload, byte-for-byte unchanged
- `encrypt=true`: thread-lane copy has `encryptedMessage` set and `message`
  nil (edits: `encryptedNewContent` set, `newContent` empty); per-account copy
  still plaintext
- `encrypt=false`: thread-lane copy is plaintext
- `Mentions` / `MentionAll` present and identical on both copies
- DM and BotDM rooms publish **nothing** to the thread lane
- a thread-lane publish error is logged and swallowed: the handler returns nil
  and the per-account fan-out still happened
- a thread-lane publish error does **not** mark the canonical message terminal
  (no `MarkTerminalFromContext`), so redelivery accounting is unaffected
- thread-lane deliveries are recorded under the `thread_lane` room kind, and no
  fan-out recipient count is recorded for it

Every existing thread test must pass untouched. That is the regression proof
that this change is purely additive.

## 9. Documentation

Required in the same PR by CLAUDE.md §5:

- `docs/client-api.md` — new subject in the subject-patterns table; delivery
  notes on `new_thread_message`, `message_edited`, `message_deleted`
  describing the viewer lane and the subscribe-on-open lifecycle.
- `docs/client-api/events.md` and `docs/client-api/request-reply.md` — the
  derived views, updated to match.

No `pkg/model` struct changes, so no model-doc churn beyond the delivery
description.

## 10. Rollout and risk

Additive and independently deployable. Deploying broadcast-worker with the new
publish before any client subscribes produces publishes with no subscribers,
which NATS discards — no cost, no behavior change. Clients can adopt the
subscription whenever they ship.

Reverting is a broadcast-worker rollback; clients subscribed to the thread lane
simply stop receiving on it and fall back to the `thread_metadata_updated`
refetch path they use today.

**Residual risks:**

- A deployment running `ENCRYPTION_ENABLED=false` puts thread content plaintext
  on an openly-subscribable lane (§5).
- A client that appends rather than upserts will show duplicate replies to
  followers who open the thread (§7). This is the one client-side change that
  is mandatory rather than optional.

## 11. Out of scope

- Any durable follow/unfollow API, and exposing the follower set to clients
  (`thread_rooms.replyAccounts` remains server-only).
- Unread, badge, and notification behavior for viewers — unchanged; a viewer
  accrues nothing.
- `thread_message_read` on the thread lane. It is already published room-wide,
  so viewers receive it today; no change needed.
- The `ThreadSubscription` / `replyAccounts` split (two collections written
  non-atomically, so a client's "am I following" signal and the actual fan-out
  set can diverge). Real, pre-existing, and unaffected by this change — the
  viewer lane does not consult either.
