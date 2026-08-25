# Thread delivery during a Cassandra outage

Status: approved design, not yet implemented
Branch: `claude/thread-messages-cassandra-down-fje1p9`

## Problem

A channel thread reply (`tshow=false`) is never delivered while Cassandra is down. It is
NAK'd, redelivered six times over roughly 33–66 s (jittered), and then dropped by JetStream. The
sender is told nothing — `msg.send` is fire-and-forget in `chat-frontend`, so the message
simply never appears, for anyone, with no error.

The chain:

1. `message-gatekeeper.resolveThreadParent` (`handler.go:419`) asks history-service for the
   parent via `GetMessageByID`, which reads `messages_by_id`. Cassandra down → the fetch
   fails → it soft-fails (`handler.go:506-513`) and publishes the canonical event without
   `ThreadParentMessageCreatedAt` or `ThreadParentSenderAccount`.
2. `broadcast-worker.channelThreadFanOut` (`handler.go:1352-1361`) sees both fields absent
   and calls `FetchParent` — the same history-service path, the same dead Cassandra. The
   error propagates through `handleThreadCreated` (`handler.go:303`).
3. `jsretry.Settle` (`main.go:394`) treats it as transient — history-service collapses a
   Cassandra read failure to `code=internal`, which is not `errcode.Permanent` — so it NAKs
   on `LowLatencyBackoff` `{200ms, 1s, 5s, 30s}` against `MaxDeliver=6`
   (`pkg/stream/consumer.go:18`). The seventh attempt never comes.
4. `notification-worker` does the same fetch (`handler.go:157-163`) on `DefaultBackoff`, so
   thread push notifications die the same way ~12.6 min in.

The gatekeeper's soft-fail comment states the intent: *"each consumer falls back to a store
it owns, so a Cassandra outage never blocks the send path."* That intent is not realized —
every consumer's fallback is another Cassandra read through the same service.

**Unaffected:** non-thread channel messages, `tshow=true` thread replies (they take the room
path), and DM/BotDM thread delivery. DM thread *notifications* do fail, since
`isThreadOnlyReply` gates the parent fetch regardless of room type.

## Non-goals

- **message-worker durability.** Messages dropped after `MaxDeliver` exhaustion on the
  Cassandra write path are [PR #307](https://github.com/hmchangw/newchat/pull/307)'s subject.
  This design has no file overlap with it.
- **bot-message-worker.** Carries the same write-side defect; it is #307's follow-up 2.
- **Reusing #307's `histdegrade` marker as the client signal.** The two track different
  failures: `incompleteSince` is set by history *write* failures, while this bug is caused
  by a *read* failure. They coincide in a full outage but diverge in a partial one, where
  the marker would make the client restrict threads that work.

## Design

### 1. broadcast-worker — resolve the parent from Mongo

`GetThreadFollowers` already reads the `thread_rooms` document keyed by `parentMessageId`
(`store_mongo.go:162-181`). That same document carries `threadParentCreatedAt`. Widening
the projection costs no extra round trip.

- Replace `GetThreadFollowers(ctx, parentMessageID) (map[string]struct{}, error)` with a
  method returning both the followers and the parent's `createdAt`, projecting
  `{replyAccounts, threadParentCreatedAt}`.
- `channelThreadFanOut` resolution order becomes: **event fields → `thread_rooms` →
  `FetchParent`**. The history round trip survives only as a last resort.
- **The parent author needs no separate resolution.** `handleFirstThreadReply` seeds them
  into `replyAccounts` via `AddReplyAccounts` (`message-worker/handler.go:368-371`), so
  `followers` already contains them and `threadFanOutAccounts` includes them without their
  identity being known independently.
- When no source resolves, fan out to sender + followers and **drop the history-gated
  mentionees**. Excluding an unverifiable mentionee is fail-closed; the message still
  reaches them through history after recovery.

**A zero `threadParentCreatedAt` must be treated as unknown, never as the epoch.**
`model.ThreadRoom.ThreadParentCreatedAt` is a non-pointer `time.Time` and
`message-worker/handler.go:317-321` leaves it zero when the createdAt is nil. That path is
unreachable today, but reading a zero value as a real timestamp would admit every mentionee
past their `historySharedSince` window — a delivery fix turning into a history leak.

### 2. notification-worker — the same fallback

`mongoThreadFollowers.Lookup` (`threads.go:33-56`) queries the same collection by the same
key. Add `ParentCreatedAt` to `ThreadRoomInfo` and widen the projection identically.
`handler.go:145-165` prefers the event fields, then the thread room, then `Parent.FetchParent`.
Unresolvable → followers keep their notifications, mention-only recipients are dropped.

Note the deliberate reversal here: `threads.go:13-15` records that the parent's createdAt
*"is no longer read here — it comes authoritatively from history-service"*. That was correct
when history-service was assumed reachable. It is the wrong default when it is not.

### 3. message-gatekeeper — reject an unresolvable thread start

The residual after §1 is narrow but real: a thread **first replied to during the outage** has
no `thread_rooms` document, because message-worker creates it only after the Cassandra read
at `handler.go:149`. Every reply to such a thread — not just the first — would degrade to
sender-only for the whole outage.

Rather than deliver into a void, refuse it:

- `Store` gains `ThreadRoomExists(ctx, parentMessageID) (bool, error)`. The gatekeeper's
  store wires only `subscriptions` and `rooms` today (`store_mongo.go:26-34`); add
  `thread_rooms`.
- **Failure path only.** The check runs solely when `resolveThreadParent` has already failed,
  so steady state is unchanged.
- Document present → delivery will work via §1; publish as normal.
- Document absent → return `errcode.Unavailable` with a new
  `MessageThreadStartUnavailable Reason = "thread_start_unavailable"` in
  `pkg/errcode/codes_message.go`.

Replies to existing threads are unaffected. The sender keeps their text instead of watching
a message disappear.

§3 does not make §1's degrade redundant. The gatekeeper only guards the *send* path, so the
fail-closed fan-out remains the backstop for events that never pass through it: replies
already in MESSAGES-CANONICAL when the outage began, and the `message_edited` /
`message_deleted` canonical events, which reach `handleThreadUpdated` / `handleThreadDeleted`
(`broadcast-worker/handler.go:527`, `:581`) without gatekeeper-carried parent fields at all.

**Docs obligation:** the new error case goes in `docs/client-api.md`'s `msg.send` error table
(`:6360-6373`) and both derived views (`docs/client-api/request-reply.md`,
`docs/client-api/events.md`) in the same PR. The success schema is untouched.

### 4. Shared

- Promote `message-gatekeeper`'s `quoteFetchErrIsTerminal` (`handler.go:574`) into
  `pkg/errcode` so the gatekeeper, broadcast-worker and notification-worker classify
  transient-vs-terminal from one definition rather than three copies.
- A counter for degraded thread fan-outs, labelled by reason. Only one reason is
  emittable — `fetch_failed`. An earlier draft also specified `no_thread_room`, but
  that label is structurally unreachable at the record site (the parent fetch either
  errors, or succeeds and resolves the timestamp), and §3's gatekeeper refusal stops
  the no-thread-room case reaching this code for creates at all. The drop today is already counted as `terminal_reason=max_deliver` by
  `pkg/natsmetrics/metrics.go:487-489`, but only under `event_type=created` —
  indistinguishable from any other message.

### 5. chat-frontend, PR 1 — make sends observable

`api/sendMessage/index.ts` calls `publish` and nothing subscribes to the reply. The
gatekeeper *does* publish its reply to `chat.user.{account}.response.{requestId}`
(`handler.go:212`, `:260-270`) — the client never listens.

**Every `msg.send` rejection is therefore invisible today**: `not_subscribed`,
`large_room_post_restricted`, an invalid message ID, content over 20 KiB. All are replied to
with a typed envelope, and all vanish. There is no optimistic rendering either, so the user
sees a message that simply never arrives.

`sendMessage` subscribes to `userResponse(account, requestId)` with a timeout and surfaces
the typed error, reusing the correlation machinery already in `api/_transport/asyncJob.ts`
(`requestWithAsyncResult`). This is the enabling change for §6, and it fixes the broader
silent failure on its own.

This alters send behaviour for **all** messages, not only thread replies, and warrants its
own review attention — hence its own PR.

### 6. chat-frontend, PR 2 — forbid the send

- A degraded context, set from either source: a history read failing with
  `unavailable`/`internal` (`api/fetchMessageHistory` is a real request/reply, so it already
  surfaces errors), or a `thread_start_unavailable` rejection from §3. Cleared on the next
  successful history load or send, and on a TTL.
- The history-read source is proactive — it fires at room open, before the user can compose.
  It is also *precisely* our condition rather than a proxy: it fails through the same
  `GetMessageByID` path in the same service against the same Cassandra.
- `MessageRow.jsx:139` already branches on `tcount`. While degraded, disable "reply in
  thread" on messages with `tcount === 0` — exactly the messages whose thread does not yet
  exist. `tcount > 0` means `thread_rooms` exists, so §1 covers it.
- `ThreadMessageInput` handles a rejection arriving mid-compose, for an outage that starts
  while the panel is open.

`tcount` reads `0` for migrated threads too (`docs/client-api.md:5786`), so the client will
occasionally restrict a thread that would have worked. That fails safe and only during an
outage.

## Testing

TDD throughout, per CLAUDE.md §4 — tests first, confirmed red, then implementation.

| Layer | Coverage |
|---|---|
| broadcast-worker unit | Table-driven across the three resolution sources; fail-closed mention drop; zero-timestamp treated as unknown; parent author arriving via `followers` |
| broadcast-worker integration | testcontainers Mongo; `thread_rooms` fallback with the parent fetcher erroring — the case with no test today |
| notification-worker | Mirror of both |
| message-gatekeeper unit | Rejects only when `thread_rooms` is absent; existing threads still publish; the check never runs on the success path |
| `pkg/errcode` | The promoted predicate, over every category |
| vitest | `sendMessage` reply correlation and timeout; degraded context transitions; thread-start disabled on `tcount === 0` while degraded |
| `scripts/threadOutage.smoke.mjs` | Live stack following `liveStack.smoke.mjs`; stop Cassandra, assert a pre-existing thread still delivers and a new thread start is refused |

## Risks

- **Rejecting thread starts is a product behaviour change** during outages. It must be called
  out in the PR description.
- **Send observability changes behaviour for every message**, not just thread replies.
- **Over-restriction on migrated threads** via the `tcount === 0` heuristic. Fails safe.

## Follow-ups

1. Correct #307's design note. Its premise — *"broadcast-worker had already delivered it to
   online clients and search-sync-worker had already indexed it — neither touches
   Cassandra"* — is false for channel thread replies, which reach Cassandra transitively
   through history-service. The divergence it is built around ("on screen, findable in
   search, gone from history") does not hold for them; they are absent from all three.
2. `bot-message-worker` carries the write-side defect (#307's own follow-up 2).
3. If the new-thread residual proves painful in practice, the shape to reach for is a
   per-room capped ring of recent message coordinates in Valkey — one key per *active room*,
   not one per message. Both broadcast-worker and notification-worker already hold a cluster
   client. Build it on evidence, not speculation.
