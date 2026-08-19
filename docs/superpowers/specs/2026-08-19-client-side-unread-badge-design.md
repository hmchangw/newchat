# Client-Side Unread Badge

**Date:** 2026-08-19
**Status:** Proposed
**Supersedes (partially):** `2026-05-18-unread-badge-rpc-source-design.md`
**Related:** `2026-08-02-thread-unread-badge-design.md`, `2026-08-10-badge-cache-invalidation-design.md`

## 1. Problem

The in-app unread badge is recomputed server-side on the message hot path.

`useUnreadCount` (`chat-frontend/src/context/RoomEventsContext/useUnreadCount.js`)
fires `subscription.count {unread:true}`:

- immediately on mount and on every `readSeq` bump (every committed `markRoomRead`), and
- 500ms-debounced on every `msgRecvSeq` bump — i.e. on **every message received in any room**.

Each call lands in `UserService.CountSubscriptions` → `unreadRooms()`
(`user-service/service/subscriptions.go:574`), which:

1. reads every active subscription for the account (Mongo, with a `$lookup` for room baselines), then
2. fans out a `GetRoomsMeta` RPC **per remote site** for cross-site rooms.

`BADGE_COUNT_CACHE_FIRST` defaults to `false` (`user-service/config/config.go:70`),
so the Valkey badge set does not serve this path today. The
2026-08-10 badge-cache design names the same gap in its own problem statement:
*"Desktop count never uses the cache. Every desktop badge refresh pays the
expensive path."*

Net: for an account in busy rooms, cost scales with **messages received**, and
each unit of that cost is a Mongo aggregation plus a cross-site RPC fan-out.

## 2. Why this reverses a prior decision

`2026-05-18-unread-badge-rpc-source-design.md` deliberately moved the badge
*away* from client derivation (`selectUnreadTotal`) and onto this RPC. That
decision was correct then and is worth restating accurately before reversing it.

**What has changed:** at the time, the real user-service *was not in this repo*.
The RPC was served by `mock-user-service`, which returned a hardcoded `42`. The
decision was about **where the contract lives** — sourcing the number from the
backend rather than deriving it — and the compute cost was not observable,
because there was no compute. Since then the real `user-service` landed, thread-aware
counting was added (2026-08-02), and a Valkey cache was introduced specifically
because the count became expensive (2026-08-10). The cost dimension that motivates
this document did not exist when the original call was made.

**What has not changed:** the two real bugs that decision fixed. Both are
requirements on this design, not casualties of it:

- **Self-send inflation.** Sending advances `Room.lastMsgAt` but not the sender's
  `lastSeenAt`, so a naive `lastMsgAt > lastSeenAt` fold counts the room you are
  sitting in. §4.3 makes this structurally impossible rather than merely patched.
- **Read-race.** A refetch racing an uncommitted `markRoomRead` could latch a
  stale count with no resync. The fold has no such race — it reads local state
  that the optimistic read already updated — and §4.5's reconcile is not
  triggered by reads at all.

## 3. What makes a client-side fold viable

Three properties of the current client, all verified:

1. **The full input set is already in memory.** `fetchSidebarBuckets`
   (`src/api/fetchSidebarBuckets/index.ts`) drains *every* page of all three
   buckets — not a first page. Each row carries `lastSeenAt`, `room.lastMsgAt`,
   `muted`, and `threadUnread`. Cross-site rows arrive already enriched by
   `subscription.list`, so no client-side federation is needed.
2. **The full delta signal already arrives.** `useRoomSubscriptions.js:568` opens
   a room-event subscription for every channel room, plus
   `chat.user.{account}.event.room` for DMs and thread replies. No message
   reaches the account unseen.
3. **Room reads already sync across devices.** `subscription.update` with
   `action:"read"` (`room-service/handler.go:1433`) is published to the account
   subject carrying the full `Subscription` — including `threadUnread`.

In short, `unreadRooms()` recomputes server-side, per message, a number the
client can fold from state it already holds.

## 4. Design

### 4.1 Badge semantics

Must match the server rule in `unreadRooms()` exactly. A room counts **once**
iff all hold:

- the subscription is active, and
- `muted` is falsy, and
- `room.lastMsgAt > lastSeenAt` (a null `lastSeenAt` with a set `lastMsgAt` counts
  as unread) **or** `threadUnread` is non-empty.

Not capped. The server's `subscription.count` is uncapped for display
(`BADGE_COUNT_CAP` applies to the push path only); `UnreadBadge.jsx` keeps its
existing `99+` render cap.

### 4.2 Component: the selector

`selectUnreadRoomIds(state)` — a pure selector in
`src/context/RoomEventsContext/reducer.js`, folding `state.summaries` ∪
`state.subscriptions` by the §4.1 rule and returning IDs (not a number) so it is
diffable in tests and reusable for a future mention-accent variant.

This reintroduces a selector the 2026-05-18 change deleted, under a different
rule: the old `selectUnreadTotal` was message-only and mute-blind. Name it
distinctly to avoid implying a revert.

### 4.3 Component: active-room suppression

The fold treats `state.activeRoomId` as read regardless of its stored
`lastSeenAt`.

This is the structural fix for self-send inflation, and it also eliminates the
residual flicker the 2026-05-18 doc accepted as unavoidable ("an active-room
message still bumps `msgRecvSeq` … the subsequent `readSeq` refetch corrects it
within ~the same second. The clean elimination is server-side … out of repo").
Client-side the room you are looking at is simply never counted, so there is
nothing to flicker.

`scheduleMarkActiveRead` keeps firing on self-send exactly as today — the server
`lastSeenAt` write is still required for other devices and for the reconcile.

### 4.4 Component: thread-unread in client state

`threadUnread` is not tracked client-side today: `fanThreadReply`
(`useRoomSubscriptions.js:266`) forwards thread replies to an optional
ThreadEvents consumer and no-ops when the thread panel is closed.

Maintain `state.subscriptions[roomId].threadUnread` from four sources:

| Source | Effect |
|---|---|
| Bootstrap — `sub.threadUnread` from `subscription.list` | seed (already on the wire) |
| `subscription.update` `action:"read"` — full `Subscription` | replace |
| `new_thread_message` on `chat.user.{account}.event.room` | add `threadParentMessageId` |
| Local `thread.read` / `thread.read.all` RPC resolves | optimistic remove / clear |

The `new_thread_message` fan-out is per-subscriber, so it arrives only for
threads the user follows — matching the server's `ThreadUnread` producer.

### 4.5 Component: reconcile

Replace the hot-path RPC with a low-frequency reconcile.

**Removed triggers:** the `msgRecvSeq` debounced fetch and the `readSeq` fetch.

The badge is the **only** consumer of both counters — they exist solely to
trigger it (`reducer.js:65,72,588,641,833`; nothing outside
`useUnreadCount` reads them). So they become dead code and are deleted along
with the `ROOM_READ_SYNCED` action that exists only to bump `readSeq`,
following the 2026-05-18 change's own precedent of deleting the derivation it
replaced. `markRoomRead`'s `Promise<boolean>` contract is unchanged — the RPC
must still fire, and callers may still sequence on it.

**Added triggers:** mount, `visibilitychange` → visible, window focus, and a slow
interval (5 min) while the document is visible.

**Reconcile is a drift *detector*, not the displayed value.** The badge always
renders the fold — one source of truth, so a corrected number can never silently
diverge from the room state behind it. Reconcile calls `subscription.count`
and compares:

- equal → no action (the overwhelmingly common case);
- unequal → log, then re-run `fetchSidebarBuckets` to resync the underlying
  subscription state, from which the fold re-derives.

Cheap detection, rare expensive correction. Comparing totals can mask
compensating errors (one room wrongly counted, another wrongly missed); accepted
— this is a backstop for rare dropped events, not a correctness primitive.

### 4.6 Data flow

```
bootstrap ──> fetchSidebarBuckets ──┐
                                    ├──> state.summaries + state.subscriptions ──> selectUnreadRoomIds ──> UnreadBadge
live events ────────────────────────┘         ▲                                             │
  new_message / new_thread_message            │                                             └──> navigator.setAppBadge (§7)
  subscription.update (read/mute/added/…)     │
                                              │
reconcile (focus / visible / 5min) ──> subscription.count ──> equal? ──no──> refetch buckets
```

### 4.7 Error handling

- **Reconcile RPC failure:** keep the folded value and retry on the next trigger.
  Note this fixes a live bug — the current hook's `.catch()` sets the badge to
  `0` on any transport error, so a transient failure silently blanks a non-zero
  badge.
- **Bucket refetch failure:** `fetchSidebarBuckets` already degrades per bucket
  via `Promise.allSettled`; the fold keeps serving whatever state survived.
- **Divergence source of truth:** the server. Reconcile never edits the number
  directly, only the state it is folded from.

## 5. What this does not fix

Two drift sources survive by construction, which is why §4.5 exists:

1. **Silent reconnect.** `NatsContext.jsx:196` connects with default nats.ws
   reconnect and no `status()` monitoring anywhere in the codebase. On a
   transient blip nats.js reconnects internally: `nc.closed()` never resolves,
   `connected` never flips, `MainApp` never unmounts, bootstrap never re-runs.
   Core-NATS events published during the gap are dropped, not replayed.
2. **Thread-read has no per-user event.** `thread.read` fans out
   `thread_message_read` only when the room-wide *floor* changes, and
   `thread.read.all` emits nothing to clients at all
   (`docs/client-api.md:5937`). Clearing a thread on another device is invisible
   here. See §8.

## 6. Testing

TDD per CLAUDE.md §4 — tests first, confirmed failing, then implementation.

**Selector** (table-driven): unread by message; unread by thread only; muted
excluded; active room excluded; null `lastSeenAt` with set `lastMsgAt` counts;
null `lastMsgAt` does not; each room counted once when both message- and
thread-unread; empty state → 0.

**Reducer:** `threadUnread` seeded from bootstrap; replaced wholesale by
`subscription.update action:"read"`; appended by `new_thread_message` (and
deduped on redelivery); cleared optimistically on local thread read; preserved
by unrelated actions.

**Hook:** folds without any RPC on message receipt (assert `subscription.count`
is *not* called — this is the core regression guard); reconciles on mount,
focus, and visibilitychange; equal count → no bucket refetch; unequal → exactly
one refetch; RPC failure preserves the previous value rather than zeroing.

**Component:** `UnreadBadge` hides at 0, renders the count, caps at `99+`.

## 7. Scope note: "taskbar"

Today's badge is a header pill (`AppHeader/UnreadBadge`). The same folded number
can drive an OS-level badge via `navigator.setAppBadge()` / Electron
`setBadgeCount` — a one-line consumer of the selector, listed here so the
boundary is explicit.

That only holds **while the app is running**. An OS badge that must be correct
with the app closed is inherently the push path (`badge.count.batch` →
`notification-worker`) and cannot move client-side. This design does not touch
that path; `CountSubscriptions` remains, it merely stops being called per
message.

## 8. Follow-up (separate change)

Closing §5.2 at the source: emit a client event on `thread.read` and
`thread.read.all` to the reader's account subject. Exploration findings:

- **Origin-side only.** `inbox-worker/handler.go:257,266,329` establish the
  convention that core-NATS `chat.user.{account}.event.…` publishes at the origin
  site are routed to the user's home site by the supercluster gateway — which is
  why inbox-worker deliberately does not republish client events. No federation
  or inbox-worker change is needed.
- **No extra read.** `UpdateSubscriptionThreadRead` (`room-service/store.go:227`)
  already returns the resulting `threadUnread` array;
  `messageThreadRead` currently discards it. The handler already holds exactly
  what the client needs.
- **`thread.read.all` wants one account-scoped event**, not per-room:
  `ClearSubscriptionThreadUnreadForAccount` clears every affected subscription in
  one op and does not report which rooms it touched.
- **Envelope:** add lean event types rather than reading back a full
  `Subscription`, following the `SubscriptionRemovedEvent` precedent
  (`pkg/model/event.go:499`).
- **Doc obligation:** touching a server→client event struct requires
  `docs/client-api.md` plus both derived views in the same PR.

Sequencing: this design ships first and delivers the full load reduction on its
own. The event work then lets the §4.5 reconcile interval stretch from
"correctness backstop for a common user action" to "rare-drop backstop".

## 9. Out of scope

- The push badge path (`badge.count.batch`, `notification-worker`) — unchanged.
- Flipping `BADGE_COUNT_CACHE_FIRST` — orthogonal, and less urgent once the
  desktop path is off the hot loop.
- Mention-accent styling on the badge. The 2026-05-18 change dropped it because
  the RPC carries no mention data; the fold restores `hasMention` locally, so it
  becomes possible again. Not included — separate UX decision.
