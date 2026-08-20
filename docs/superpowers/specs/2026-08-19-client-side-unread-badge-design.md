# Client-Side Unread Badge

**Date:** 2026-08-19
**Status:** Proposed
**Supersedes (partially):** `2026-05-18-unread-badge-rpc-source-design.md`
**Related:** `2026-08-02-thread-unread-badge-design.md`, `2026-08-10-badge-cache-invalidation-design.md`

**Target surface:** the OS taskbar / dock badge. The OS wiring lives in the
desktop shell, which is **not in this repo**. `chat-frontend` implements the
badge *logic* to taskbar semantics — visibility-aware, push-consistent, single
valued — and renders it in the existing header pill. The shell consumes the same
number. Every rule below is written for the taskbar meaning of the badge even
though nothing in this repo calls an OS API; §7 states the shell's side of the
contract.

## 1. Problem

The unread badge is recomputed server-side on the message hot path.

`useUnreadCount` (`chat-frontend/src/context/RoomEventsContext/useUnreadCount.js`)
fires `subscription.count {unread:true}`:

- immediately on mount and on every `readSeq` bump (every committed `markRoomRead`), and
- 500ms-debounced on every `msgRecvSeq` bump — i.e. on **every message received in any room**.

Each call lands in `UserService.CountSubscriptions` → `unreadRooms()`
(`user-service/service/subscriptions.go:574`), which:

1. reads every active subscription for the account (Mongo, with a `$lookup` for room baselines), then
2. fans out a `GetRoomsMeta` RPC **per remote site** for cross-site rooms.

`BADGE_COUNT_CACHE_FIRST` defaults to `false` (`user-service/config/config.go:70`),
so the Valkey badge set does not serve this path today. The 2026-08-10
badge-cache design names the same gap in its own problem statement: *"Desktop
count never uses the cache. Every desktop badge refresh pays the expensive
path."*

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
because there was no compute. Since then the real `user-service` landed,
thread-aware counting was added (2026-08-02), and a Valkey cache was introduced
specifically because the count became expensive (2026-08-10). The cost dimension
that motivates this document did not exist when the original call was made.

**What has not changed:** the two real bugs that decision fixed. Both are
requirements on this design, not casualties of it:

- **Self-send inflation.** Sending advances `Room.lastMsgAt` but not the sender's
  `lastSeenAt`, so a naive `lastMsgAt > lastSeenAt` fold counts the room you are
  sitting in. §4.3 makes this structurally impossible rather than merely patched.
- **Read-race.** A refetch racing an uncommitted `markRoomRead` could latch a
  stale count with no resync. The fold has no such race — it reads local state
  that the optimistic read already updated — and §4.6's reconcile is not
  triggered by reads at all.

## 3. What makes a client-side fold viable

Three properties of the current client, all verified:

1. **The full input set is already in memory.** `fetchSidebarBuckets`
   (`src/api/fetchSidebarBuckets/index.ts`) drains *every* page of all three
   buckets — not a first page. Each row carries `lastSeenAt`, `room.lastMsgAt`,
   `muted`, and `threadUnread`. Cross-site rows arrive already enriched by
   `subscription.list`, so no client-side federation is needed.

   **This was not true when first written.** `subscription.list` filtered on
   `open != false` while `unreadRooms()` did not
   (`user-service/mongorepo/subscriptions.go`), so a room the user had closed
   was counted by the server and invisible to the client — an unclosable gap
   that made every reconcile mismatch and every resync fail to converge. Fixed
   by adding the same `open` filter to `activeSubscriptionFilter`, which backs
   both the total and unread counts. **Any future divergence between what
   `subscription.list` returns and what `activeSubscriptionFilter` selects
   breaks this design in the same way**; the two must be changed together.
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

Matches the server rule in `unreadRooms()`, with one deliberate deviation noted
below. A room counts **once** iff all hold:

- the subscription is active, and
- `muted` is falsy, and
- `room.lastMsgAt > lastSeenAt` (a null `lastSeenAt` with a set `lastMsgAt`
  counts as unread) **or** `threadUnread` is non-empty, and
- it is not the *visibly* active room — **for the message test only**.

The last condition is the deviation: the server has no concept of an open window
and does count the active room. The two converge because `scheduleMarkActiveRead`
advances `lastSeenAt` server-side, after which the server stops counting it too —
but they disagree during the trailing-debounce window, which §4.6 must tolerate.

Suppression must NOT extend to the thread test. Sitting in a room's main feed is
not reading its threads, the server counts an unread thread regardless, and this
client has no thread-read RPC to converge with — so suppressing it would hold the
fold permanently below the server and spin the reconcile forever.

**Cap.** The count is capped by a parameter, not hardcoded. Push counts are
capped at `BADGE_COUNT_CAP` (default 10, rendered "9+") in `pkg/model/push.go:14`;
`subscription.count` is uncapped. Since the OS badge has two writers (§7), the
cap is what keeps them consistent — but there is no OS badge in this repo yet,
and capping the header pill today would discard information for no benefit. So:
`selectUnreadRoomIds` returns the uncapped ID set, the cap is applied at the
consumer, and the in-repo default is uncapped (`UnreadBadge.jsx` keeps its `99+`
render cap). Source the cap from `lib/runtimeConfig.js` following the existing
`window.__APP_CONFIG__` / `VITE_*` pattern, so the shell can set it to
`BADGE_COUNT_CAP` without a code change. Do not hardcode `10` in the frontend.

### 4.2 Component: the selector

`selectUnreadRoomIds(state)` — a pure selector in
`src/context/RoomEventsContext/reducer.js`, folding `state.summaries` ∪
`state.subscriptions` by the §4.1 rule and returning IDs (not a number) so it is
diffable in tests and reusable for a future mention-accent variant.

This reintroduces a selector the 2026-05-18 change deleted, under a different
rule: the old `selectUnreadTotal` was message-only and mute-blind. Name it
distinctly to avoid implying a revert.

### 4.3 Component: visibility-gated active-room suppression

The fold treats `state.activeRoomId` as read **only while
`document.visibilityState === 'visible'`**. A hidden window has no active room
for badge purposes.

Unconditional suppression would be correct for a header pill (nobody sees the
pill of a hidden window) and wrong for a taskbar badge: leave the app minimized
with a room open, and that room's messages would never badge — the exact case
the taskbar badge exists to serve.

With the gate, this remains the structural fix for self-send inflation (you
cannot send to a room while the window is hidden), and it still eliminates the
residual flicker the 2026-05-18 doc accepted as unavoidable ("an active-room
message still bumps `msgRecvSeq` … the subsequent `readSeq` refetch corrects it
within ~the same second. The clean elimination is server-side … out of repo").
Client-side, the room you are actually looking at is never counted.

### 4.4 Component: visibility-gated mark-read (ships first, independently)

`scheduleMarkActiveRead` (`useRoomSubscriptions.js:157`) marks the active room
read on every incoming message with **no visibility gate** — there is no
`visibilityState` / `document.hidden` check anywhere in `chat-frontend`.

So a minimized app with a room open silently marks that room's arriving messages
read server-side. Under a header pill this is invisible. Under a taskbar badge it
means those messages never badge and the user misses them outright — and because
the mark-read is a real server write, §4.6's reconcile cannot detect it: server
and client agree, and both are wrong.

Gate the mark-read (and its trailing timer) on `visibilityState === 'visible'`.
On becoming visible again, run the normal active-room read path once so
reopening the window still clears the room.

**This is a pre-existing bug that the new surface exposes, independent of the
rest of this design.** It ships as its own change, ahead of everything else.

### 4.5 Component: thread-unread in client state

`threadUnread` is not tracked client-side today: `fanThreadReply`
(`useRoomSubscriptions.js:266`) forwards thread replies to an optional
ThreadEvents consumer and no-ops when the thread panel is closed.

Maintain `state.subscriptions[roomId].threadUnread` from four sources:

| Source | Effect |
|---|---|
| Bootstrap / resync — `sub.threadUnread` from `subscription.list` | seed (already on the wire) |
| `subscription.update` `action:"read"` — full `Subscription` | merge (see below) |
| A thread reply from another account, via `fanThreadReply` | add `threadParentMessageId` |

**It only grows locally.** An earlier draft also listed optimistic removal on
`thread.read` / `thread.read.all`; those cases were written and then deleted,
because **this client has no thread-read operation at all** — no `api/` op, no
subject builder. Nothing could dispatch them. `threadUnread` therefore shrinks
only on a resync, and the thread panel marking threads read is separate work.
The badge dispatch happens *before* `fanThreadReply`'s handler short-circuit, so
a closed thread panel does not suppress it.

The `new_thread_message` fan-out is per-subscriber, so it arrives only for
threads the user follows — matching the server's `ThreadUnread` producer.

**The read event merges rather than replaces.** Reading a room does not clear
its threads server-side (`docs/client-api.md:2126` — room-level read and
thread-level read are separate), so `threadUnread` must survive a
`SUBSCRIPTION_UPSERTED` from a read event. `threadUnread` is `omitempty`, so a
replace would also drop the local list whenever the server's copy is empty. The
existing merge in the reducer is therefore already correct here; the only case
where local and server can legitimately diverge is a thread read on another
device, which is §5.2's gap and §4.6's job.

### 4.6 Component: reconcile

Replace the hot-path RPC with a low-frequency reconcile.

**Removed triggers:** the `msgRecvSeq` debounced fetch and the `readSeq` fetch.

The badge is the **only** consumer of both counters — they exist solely to
trigger it (`reducer.js:65,72,588,641,833`; nothing outside `useUnreadCount`
reads them). So they become dead code and are deleted along with the
`ROOM_READ_SYNCED` action that exists only to bump `readSeq`, following the
2026-05-18 change's own precedent of deleting the derivation it replaced.
`markRoomRead`'s `Promise<boolean>` contract is unchanged — the RPC must still
fire, and callers may still sequence on it.

**Added triggers — time-based first.** A taskbar badge is looked at precisely
when the app is *not* focused, so a focus-triggered reconcile would run only when
the badge stops mattering and never during the hours it matters most. Triggers:

1. a periodic interval (5 min) that runs **regardless of visibility** — the primary trigger;
2. `visibilitychange` → visible, and window focus — additional, not sufficient alone.

**Not on mount.** An earlier draft listed mount as a third trigger; it was
dropped during implementation because the bucket bootstrap *is* an
authoritative pull, so reconciling against it immediately only duplicates it
(and mismatches spuriously while the bootstrap is still in flight).

**Resync path.** `useRoomSubscriptions` exposes `resync()` — the sidebar
bootstrap, extracted into a callable and held in a ref so the published
identity stays stable while pointing at the current login's connection. It is
reused rather than badge-specific: a resync also picks up rooms whose
membership events the client missed.

The fold itself keeps working while hidden (the websocket stays up and events
keep arriving), so this is drift *detection*, not correctness. Hidden-window
timer throttling is a shell obligation — see §7.

**Reconcile is a drift detector, not the displayed value.** The badge always
renders the fold — one source of truth, so a corrected number can never silently
diverge from the room state behind it. Reconcile calls `subscription.count` and
compares:

- equal → no action (the overwhelmingly common case);
- unequal → log, then re-run `fetchSidebarBuckets` to resync the underlying
  subscription state, from which the fold re-derives.

Cheap detection, rare expensive correction. Comparing totals can mask
compensating errors (one room wrongly counted, another wrongly missed); accepted
— this is a backstop for rare dropped events, not a correctness primitive.

Compare **uncapped** values. Comparing capped numbers would hide all drift above
the cap (11 vs 40 both read as 10).

**Tolerate the active-room window.** Per §4.1, between a message arriving in the
visibly-active room and its trailing `markRoomRead` committing, the client
suppresses that room and the server still counts it — a legitimate off-by-one
that is not drift. Requiring **two consecutive mismatches** before refetching
covers it: that window is ~500ms wide against a 5-minute reconcile interval, so
two reconciles both landing inside one is not a realistic event. An earlier
draft also gated the reconcile on "no mark-read pending"; that was dropped
because it means plumbing `useRoomSubscriptions`' timer state into the badge
hook for no measurable gain. (When the window is hidden, §4.4 stops the
mark-read and §4.3 stops the suppression, so both sides count the room and the
two agree.)

### 4.7 Data flow

```
bootstrap ──> fetchSidebarBuckets ──┐
                                    ├─> state.summaries + state.subscriptions
live events ────────────────────────┘                │
  new_message / new_thread_message                   ▼
  subscription.update (read/mute/added/…)   selectUnreadRoomIds  <── document.visibilityState
                                                     │
                                        ┌────────────┴────────────┐
                                        ▼                         ▼
                                  UnreadBadge (pill)     shell → OS badge (§7)
                                                              (cap applied)

reconcile (5min, any visibility │ focus │ visible │ mount)
    └─> subscription.count ──> equal (uncapped)? ──no──> refetch buckets
```

### 4.8 Error handling

- **Reconcile RPC failure:** keep the folded value and retry on the next trigger.
  This fixes a live bug — the current hook's `.catch()` sets the badge to `0` on
  any transport error, so a transient failure silently blanks a non-zero badge.
  On a taskbar badge that reads as "all caught up" and is worse than stale.
- **Bucket refetch failure:** `fetchSidebarBuckets` already degrades per bucket
  via `Promise.allSettled`; the fold keeps serving whatever state survived.
- **Divergence source of truth:** the server. Reconcile never edits the number
  directly, only the state it is folded from.

## 5. What this does not fix

Two drift sources survive by construction, which is why §4.6 exists:

1. **Silent reconnect.** `NatsContext.jsx:196` connects with default nats.ws
   reconnect and no `status()` monitoring anywhere in the codebase. On a
   transient blip nats.js reconnects internally: `nc.closed()` never resolves,
   `connected` never flips, `MainApp` never unmounts, bootstrap never re-runs.
   Core-NATS events published during the gap are dropped, not replayed.
2. **Thread-read has no per-user event.** `thread.read` fans out
   `thread_message_read` only when the room-wide *floor* changes, and
   `thread.read.all` emits nothing to clients at all (`docs/client-api.md:5937`).
   Clearing a thread on another device is invisible here. See §8.

## 6. Testing

TDD per CLAUDE.md §4 — tests first, confirmed failing, then implementation.

**Selector** (table-driven): unread by message; unread by thread only; muted
excluded; visibly-active room excluded; **hidden-window active room included**;
null `lastSeenAt` with set `lastMsgAt` counts; null `lastMsgAt` does not; each
room counted once when both message- and thread-unread; empty state → 0.

**Mark-read gate (§4.4):** no `message.read` RPC fires for an incoming
active-room message while `visibilityState === 'hidden'`; the pending trailing
timer does not fire across a visible→hidden transition; becoming visible again
marks the active room read exactly once.

**Reducer:** `threadUnread` seeded from bootstrap; replaced wholesale by
`subscription.update action:"read"`; appended by `new_thread_message` (and
deduped on redelivery); cleared optimistically on local thread read; preserved by
unrelated actions.

**Hook:** folds without any RPC on message receipt (assert `subscription.count`
is *not* called — the core regression guard); the periodic reconcile fires while
`hidden` (use fake timers); reconciles on mount, focus, and visibilitychange;
equal count → no bucket refetch; a single mismatch → no refetch; two consecutive
mismatches → exactly one refetch; RPC failure preserves the previous value
rather than zeroing.

**Cap:** an uncapped selector result is capped at the configured value by the
consumer; the default configuration is uncapped; reconcile compares uncapped.

**Component:** `UnreadBadge` hides at 0, renders the count, caps at `99+`.

## 7. Taskbar semantics and the shell contract

`chat-frontend` implements the logic; the desktop shell owns the OS surface.
Obligations that the logic here depends on, recorded so they are not discovered
later:

- **Single writer, with precedence.** Push already carries `UnreadCounts` on
  `PushNotificationEvent` (`pkg/model/push.go:17`) *"so the push gateway and
  clients (desktop app icon, mobile) can render an up-to-date badge"*. The OS
  badge therefore already has a writer. While the app is running with a live NATS
  connection, **the fold wins**: the shell must overwrite push-set values, and a
  push arriving while running must not clobber the folded number. With multiple
  windows, one process owns the badge; renderers report their number to it rather
  than writing directly.
- **Cap consistency.** The shell applies `BADGE_COUNT_CAP` (§4.1) so the number
  does not jump between an uncapped `47` while running and a capped `9+` after a
  push.
- **Background throttling.** The §4.6 interval must keep firing while the window
  is hidden or minimized. Browsers throttle timers hard in hidden documents;
  Electron needs `backgroundThrottling: false` on the window.
- **Quit handoff.** Whatever value is on the icon at quit persists until a push
  overwrites it. Clearing on quit is wrong (says zero when there are unread
  rooms); leaving a stale uncapped number is also wrong. Write a final
  push-consistent capped value on quit and let push correct from there.
- **Platform matrix.** macOS dock and Linux (Unity) take a number directly.
  Windows has no numeric taskbar badge — only `setOverlayIcon(image, …)`, so the
  number must be rasterized and realistically fits one digit plus `+`. This is an
  independent argument for the cap.

**App closed is not in scope for the fold.** An OS badge that must be correct
while the app is not running is inherently the push path (`badge.count.batch` →
`notification-worker`). This design does not touch it; `CountSubscriptions`
remains, it merely stops being called per message.

## 8. Follow-up (separate change)

Closing §5.2 at the source: emit a client event on `thread.read` and
`thread.read.all` to the reader's account subject. Exploration findings:

- **Origin-side only.** `inbox-worker/handler.go:257,266,329` establish the
  convention that core-NATS `chat.user.{account}.event.…` publishes at the origin
  site are routed to the user's home site by the supercluster gateway — which is
  why inbox-worker deliberately does not republish client events. No federation
  or inbox-worker change is needed.
- **No extra read.** `UpdateSubscriptionThreadRead` (`room-service/store.go:227`)
  already returns the resulting `threadUnread` array; `messageThreadRead`
  currently discards it. The handler already holds exactly what the client needs.
- **`thread.read.all` wants one account-scoped event**, not per-room:
  `ClearSubscriptionThreadUnreadForAccount` clears every affected subscription in
  one op and does not report which rooms it touched.
- **Envelope:** add lean event types rather than reading back a full
  `Subscription`, following the `SubscriptionRemovedEvent` precedent
  (`pkg/model/event.go:499`).
- **Doc obligation:** touching a server→client event struct requires
  `docs/client-api.md` plus both derived views in the same PR.

## 9. Sequencing

1. **§4.4 mark-read visibility gate** — a live bug, independent of everything
   else, ships on its own.
2. **§4.2–4.3, 4.5–4.8** — the fold, thread-unread tracking, and the reconcile.
   Delivers the full load reduction.
3. **§8 server events** — lets the §4.6 interval stretch from "correctness
   backstop for a common user action" to "rare-drop backstop".
4. **Shell wiring (§7)** — out of this repo; unblocked once step 2 lands.

## 10. Out of scope

- The push badge path (`badge.count.batch`, `notification-worker`) — unchanged.
- The OS badge call itself and the platform matrix — desktop shell, out of repo.
- Flipping `BADGE_COUNT_CACHE_FIRST` — complementary, not an alternative, and
  worth doing on its own schedule. It reduces the *per-call* cost of
  `subscription.count` (SCARD instead of the Mongo aggregation) but leaves the
  *call volume* untouched: one NATS request/reply and one user-service goroutine
  per message per connection either way. This design removes the calls. Volume is
  the dominant term — per §7 of `docs/nats-traffic-estimation.md` the badge
  refetch is roughly 1:1 with message deliveries (~19k per connection per day at
  the modeled F=100/D=5), against a modeled `R_sub` of 10/day/connection for all
  subscription R/R combined. That model line almost certainly predates the
  message-driven badge refetch (added 2026-05-18) and should be revisited
  separately.
- Mention-accent styling on the badge. The 2026-05-18 change dropped it because
  the RPC carries no mention data; the fold restores `hasMention` locally, so it
  becomes possible again. Not included — separate UX decision.
