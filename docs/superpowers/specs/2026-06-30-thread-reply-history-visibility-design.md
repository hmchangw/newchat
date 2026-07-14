# Thread-Reply History Visibility Gate + PR #245 Follow-ups

**Date:** 2026-06-30
**Branch:** `claude/broadcast-worker-thread-visibility-d4p1g2`
**Status:** Approved design

## Background

PR #245 (`feat: real-time thread reply fan-out + reply-count badge pipeline`, merged)
delivered thread-reply broadcast through `broadcast-worker` and the upstream
`message-worker` persistence/subscription side. Review left several follow-ups,
and a correctness gap remains: @-mentioned users with **restricted room history**
(a non-nil `HistorySharedSince` that starts *after* a thread's parent message)
currently still receive that thread's reply events and get thread subscriptions
created — even though they are not allowed to see the parent message.

`notification-worker` already enforces the analogous gate (`isRestricted` in
`notification-worker/handler.go`). This work brings `broadcast-worker` and
`message-worker` in line with that convention and clears the remaining PR #245
follow-ups.

## Visibility rule (authoritative)

Mirror `notification-worker`'s `isRestricted`. A user is **allowed** to receive /
subscribe to a thread reply when, for their room subscription:

- `HistorySharedSince == nil` → allowed (unrestricted member), **or**
- `ParentMessage.CreatedAt >= HistorySharedSince` → allowed (joined at/before the parent).

A restricted user is **excluded** when `HistorySharedSince` is set **and** the
parent message's `CreatedAt` is `nil`/unknown **or** strictly before
`HistorySharedSince`.

**Non-members are also excluded.** A mentioned account with **no room
subscription** in the target room is not a member and must not receive the thread
event or get a thread subscription. Because the history-window lookup queries the
`subscriptions` collection, a non-member is simply absent from the returned map —
key-presence *is* membership. The caller excludes any mentioned account missing
from the map, then applies the window check to those present.

`HistorySharedSince` lives on the room `Subscription` document (`subscriptions`
collection), keyed by `(roomId, u.account)`. The parent timestamp travels on the
reply event as `Message.ThreadParentMessageCreatedAt`.

Shared unexported helper (one copy per service that needs it, signature identical):

```go
// mentionVisible reports whether a mentioned user whose room subscription carries
// historySharedSince may see a thread reply whose parent was created at parentCreatedAt.
// Mirrors notification-worker.isRestricted (inverted to "visible"): nil window = full
// access; a set window with a missing/older parent timestamp = no access.
func mentionVisible(historySharedSince, parentCreatedAt *time.Time) bool {
    if historySharedSince == nil {
        return true
    }
    if parentCreatedAt == nil {
        return false
    }
    return !parentCreatedAt.Before(*historySharedSince)
}
```

The gate applies **only to @-mention-driven recipients**. Existing thread
followers (`thread_rooms.replyAccounts`: parent author + prior repliers) are never
filtered — they already participate in the thread.

The same gate applies to **all three** channel thread paths — create, edit, and
delete — so a restricted or non-member user @-mentioned in edited/deleted reply
content is excluded from the edit/delete fan-out exactly as they are from create.
The edit/delete paths reach the parent timestamp because the canonical
`EventUpdated`/`EventDeleted` events carry `ThreadParentMessageCreatedAt` (see Part
6); the create path already carries it (message-worker resolves it from
`messages_by_id`).

## Part 1 — broadcast-worker: gate mention fan-out

**Store (`broadcast-worker/store.go` + `store_mongo.go`):**

```go
GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)
```

Filters `subscriptions` by `{roomId, "u.account": {$in: accounts}}`, projects
`{"u.account": 1, "historySharedSince": 1, "_id": 0}`, returns
`account → *HistorySharedSince` for **members only** — an account with no
subscription is absent from the map (key-presence encodes membership). Empty
`accounts` short-circuits to an empty map (no query).

**Handler (`broadcast-worker/handler.go`, `handleThreadCreated`):**

- **Channel branch:** resolve `parsed.Accounts`, fetch their history windows via
  `GetHistorySharedSince`, then keep an account only when it is present in the map
  (a member) **and** `mentionVisible(hss, msg.ThreadParentMessageCreatedAt)` holds;
  non-members (absent from the map) and members who joined after the parent are
  dropped. Pass the **allowed** mentions to `channelThreadFanOut(parentMsgID,
  sender, allowedMentions)`. Followers are merged inside `channelThreadFanOut` and
  remain unfiltered.
  - `resolved.Participants` (the event's informational `mentions` payload) is left
    unchanged — it reflects who the message mentioned, independent of who receives it.
- **DM branch:** DMs are two-party and carry no restricted-history semantics →
  delivery unchanged (still `publishDMEvents` to all members). See Part 2 for the
  hasMention change on this branch.

## Part 2 — broadcast-worker: mark thread subscription hasMention (not room sub)

The DM branch of `handleThreadCreated` currently calls
`store.SetSubscriptionMentions` which sets `hasMention=true` on the **room**
`subscriptions` doc — incorrectly badging the whole DM room for a thread-only
reply (flagged in PR #245 review). Redirect it to the **thread** subscription.

**Store (`broadcast-worker/store.go` + `store_mongo.go`):**

```go
SetThreadSubscriptionMentions(ctx context.Context, parentMessageID string, accounts []string) error
```

`UpdateMany` on `thread_subscriptions` `{parentMessageId, userAccount: {$in:
accounts}}` with `$set {hasMention: true}`. No upsert: if the thread subscription
does not exist yet (race with message-worker), this no-ops and `message-worker`'s
`MarkThreadSubscriptionMention` (the durable owner) still sets it. Idempotent.

Requires adding a `thread_subscriptions` collection handle (`threadSubCol`) to
`mongoStore` and wiring it in `main.go`. It also requires a backing index: the
collection's only existing index is message-worker's unique
`(threadRoomId, userAccount)`, which cannot serve a `parentMessageId`-led filter,
so broadcast-worker's `EnsureIndexes` adds a non-unique
`(parentMessageId, userAccount)` index (idempotent, co-exists with the unique one).

**Handler:** DM branch replaces the `SetSubscriptionMentions` call with
`SetThreadSubscriptionMentions(parentMsgID, resolved.Accounts)`. No history gate is
applied on the DM path: `SetThreadSubscriptionMentions` is a no-upsert `UpdateMany`
that only flips rows that already exist, and thread-subscription rows are created —
and history-gated — upstream by message-worker's `markThreadMentions`. A restricted
or non-member mentionee therefore has no row to flip, so passing the raw
`resolved.Accounts` is equivalent to gating and avoids a redundant Mongo round-trip
(DMs also carry no restricted-history semantics). The channel branch's gate, by
contrast, controls live *delivery* to mentioned non-followers and is essential.
Channel thread-sub `hasMention` remains message-worker's responsibility.

## Part 3 — message-worker: batch the thread-message writes

`SaveThreadMessage` and `saveThreadMessageEncrypted` currently issue 2–3 sequential
plain `Query(...).Exec()` calls. Combine them into a single
`gocql.UnloggedBatch` — identical to `SaveMessage`/`saveMessageEncrypted`:

- `messages_by_id` INSERT + `thread_messages_by_thread` INSERT, plus the
  conditional `messages_by_room` TShow INSERT, all added to one batch and run via
  `ExecuteBatch`.
- `countAndSetParentTcount` continues to run **after** the batch commits (it COUNTs
  the `thread_messages_by_thread` partition just written).
- UnloggedBatch (not Logged) for the same reason `SaveMessage` uses it: each INSERT
  is idempotent on its primary key, and JetStream redelivery re-runs the whole
  thing safely. No atomicity guarantee is needed or claimed.

Behavior, column lists, encrypted-NULL binding, and idempotency are unchanged; only
the transport (3 round-trips → 1 batch) changes.

## Part 4 — message-worker: gate thread-subscription/replyAccounts for mentions

**ThreadStore (`message-worker/store.go` + `store_mongo.go`):**

```go
GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)
```

Backed by a new `subscriptions` collection handle on `threadStoreMongo` (added in
`newThreadStoreMongo`). Same query/projection/semantics as the broadcast-worker
method.

**Handler (`message-worker/handler.go`, `markThreadMentions`):**

Before the per-mention work, fetch history windows for the non-sender, non-`@all`
mentioned accounts. For each mentioned account that is **either** a non-member
(absent from the map) **or** fails `mentionVisible(hss,
msg.ThreadParentMessageCreatedAt)`:

- skip `MarkThreadSubscriptionMention` (no thread subscription created),
- skip `publishThreadSubInboxIfRemote` (no cross-site mention copy),
- skip adding the account to `thread_rooms.replyAccounts`.

Allowed (member + in-window) mentions proceed exactly as today. The migration short-circuit
(`isMigration`) is preserved. Note: this gates only the **mention** path;
parent-author and replier subscriptions (`handleFirstThreadReply` /
`handleSubsequentThreadReply`) are unaffected — they are participants, not mentions.

## Part 5 — docs/client-api.md: document eventTimestamp

`RoomEvent`, `EditRoomEvent`, and `ReactRoomEvent` already carry
`eventTimestamp int64` (`json:"eventTimestamp,omitempty"`) and the handler
populates them, but the field is undocumented for three events. Add an
`eventTimestamp` row (wording mirrored from the existing `message_deleted` row:
"Milliseconds since Unix epoch (UTC). When message-worker published the canonical
event. Omitted for legacy events.") to the field tables for:

- `new_message` (RoomEvent),
- `message_edited` (EditRoomEvent),
- `message_reacted` (ReactRoomEvent),

and add the field to each event's JSON example. Documentation-only; no schema or
subject change, so no further client-api obligations.

## Part 6 — extend the mention gate to thread edit/delete

`handleThreadUpdated` and `handleThreadDeleted` merge the edited/deleted reply's
`@`-mentions into the channel fan-out ungated, while `handleThreadCreated` now
gates them. This leaks edit/delete events to restricted or non-member users who are
newly mentioned in the edited/deleted content. Close the gap symmetrically.

**history-service (`internal/service/messages.go`):** the canonical `EventUpdated`
and `EventDeleted` `model.Message` literals set `ThreadParentMessageID` + `TShow`
but omit the parent timestamp. Add `ThreadParentMessageCreatedAt:
msg.ThreadParentCreatedAt` to both (the value is already in scope on the fetched
`msg`). For a non-thread message `msg.ThreadParentCreatedAt` is nil → harmless.
These are internal `MESSAGES_CANONICAL` events, so no `docs/client-api.md` change.

**broadcast-worker (`handler.go`):** in the channel branch of both
`handleThreadUpdated` and `handleThreadDeleted`, gate the parsed mentions before
fan-out — identical to create:

```go
parsed := mention.Parse(msg.Content)
allowedMentions, err := h.allowedThreadMentions(ctx, room.ID, parsed.Accounts, msg.ThreadParentMessageCreatedAt)
if err != nil {
    return err
}
fanOut, err := h.channelThreadFanOut(ctx, parentMsgID, msg.UserAccount, allowedMentions)
```

Followers (`replyAccounts`) remain unfiltered — they already exclude restricted
users (message-worker never adds them). No new store method or query is introduced.

## Testing & process

TDD (Red→Green→Refactor) for every code change:

- `mentionVisible` helper: unit table tests (nil window, set window before/after
  parent, nil parent + set window).
- broadcast-worker handler: table tests with regenerated mock store asserting the
  gated fan-out set and the thread-sub hasMention redirect; `GetHistorySharedSince`
  / `SetThreadSubscriptionMentions` covered by Mongo integration tests
  (`pkg/testutil`).
- message-worker handler: table tests asserting restricted mentions are skipped;
  `GetHistorySharedSince` Mongo integration test; batch refactor covered by the
  existing Cassandra idempotency integration tests (assert unchanged tcount
  behavior + both-table writes).
- Regenerate mocks (`make generate SERVICE=broadcast-worker`, `... message-worker`)
  after interface changes.
- Gates: `make lint`, `make test`, relevant `make test-integration`, `make sast`.

All work on `claude/broadcast-worker-thread-visibility-d4p1g2`. `docs/client-api.md`
updated in the same change set (Part 5).

## Part 7 — PR review follow-ups: parent fetch + always-on parent-sender fan-out

Three review comments on this PR supersede parts of the original design. Delivered
as a **separate commit** on the same branch.

**Comment 1 — parent `CreatedAt` no longer on the canonical event.** `main`'s #399
dropped `Message.ThreadParentMessageCreatedAt` from the canonical create event, so
the Part 1/6 gate that read `msg.ThreadParentMessageCreatedAt` would see `nil` and
silently over-exclude every restricted mentionee. Fix: `broadcast-worker` resolves
the parent authoritatively via a server-to-server NATS request to `history-service`'s
`GetMessageByID` (`subject.MsgGet(account, roomID, siteID)`), reading the parent's
`CreatedAt` (for the gate) and `Sender.Account` (Comment 3). The reply always
pre-exists its parent, so the fetch is race-free. New `broadcast-worker/parent_fetcher.go`
(`ParentFetcher` interface + `historyParentFetcher`), mirroring
`message-gatekeeper/fetcher_history.go`. A fetch error is returned so the worker
NAKs and JetStream redelivers.

**Comment 2 — `SetThreadSubscriptionMentions` is redundant.** `message-worker`'s
`markThreadMentions` already creates the thread-subscription row with
`hasMention=true` (history-gated) for mentioned users. `broadcast-worker`'s
no-upsert `SetThreadSubscriptionMentions` only flipped rows message-worker had
already created, so it was pure redundancy. **Removed** entirely — the Store method,
its `thread_subscriptions` collection handle, the `(parentMessageId, userAccount)`
index, and the DM-branch call. This supersedes **Part 2**. DM thread-reply mention
badges are owned solely by message-worker.

**Comment 3 — race: parent author dropped from fan-out.** The fan-out must *always*
include `threadMsg.Sender + parentMsg.Sender + replyAccounts + mentionedUsers`.
`replyAccounts` (`thread_rooms`) is written by message-worker on a separate,
unordered consumer and may be empty on the first reply, and it never necessarily
contains the parent author (who started the thread but may not have replied). So the
parent author could be silently dropped from a reply to their own thread. Fix, in
**both** `broadcast-worker` and `notification-worker`:

- `broadcast-worker.threadFanOutAccounts` adds `parentSenderAccount` (from the
  Comment 1 fetch) directly, deduped, bots excluded — alongside the reply sender.
- `notification-worker` fetches the parent the same way (new
  `notification-worker/parent_fetcher.go`) and treats the parent author as an
  implicit thread follower (`follows = true`), so they are notified even before
  `thread_rooms` exists. The parent author is never excluded by the restriction gate
  — they were present when they authored the parent. `notification-worker` now also
  sources `parentCreatedAt` from this authoritative fetch instead of
  `thread_rooms.threadParentCreatedAt` (which was `nil` on the first-reply race and
  drove conservative over-suppression); `ThreadRoomInfo.ParentCreatedAt` is removed.

Both fetchers decode a narrow `{createdAt, sender.account}` projection (not the full
`cassandra.Message`, whose marshal-only `Reactions` map sonic can't decode).

## Out of scope / accepted limitations

- DM thread-reply **delivery** is not history-gated (DMs have no restricted-history
  semantics).
- The `notification-worker` copy of the rule is not refactored into a shared
  package — each service keeps a small local `mentionVisible` to avoid cross-service
  coupling churn; the logic is identical and intentionally mirrored.
- Edit/delete thread fan-out uses the current follower set (PR #245's accepted
  limitation) for the follower portion; the @-mention portion is history-gated the
  same as create (Part 6).
- Per Part 7, both `broadcast-worker` and `notification-worker` issue one extra
  `history-service` request per channel thread reply. Accepted: the parent is a
  cached hot-path read and the request is race-free correctness-critical.
