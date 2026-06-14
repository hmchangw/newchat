# Thread Reply Notifications — Out of Scope for PR #245

PR #245 ("feat: real-time thread reply fan-out (broadcast-worker) + reply-count badge pipeline")
implements thread reply fan-out in **broadcast-worker** and the reply-count badge pipeline in
**message-worker** and **history-service**.

**notification-worker was intentionally left unchanged.** A separate engineer owns that service.

> ⚠️ **Regression introduced by PR #245:** This PR publishes `EventThreadReplyAdded` events to
> `MESSAGES_CANONICAL` (subject `chat.msg.canonical.<siteID>.thread.reply`). The current
> notification-worker handler has no event-type guard, so every thread reply now fires a
> `"new_message"` push notification to **all room members** with a nearly empty `Message` body
> (`Content=""`, `UserID=""`). The sender-exclusion guard (`User.ID == senderID`) never fires
> because `senderID` is `""` on these events. **Priority #1 below is now a regression fix,
> not just a future improvement.**

## What needs to be built in notification-worker

### 1. Filter to EventCreated only

The current handler fans out a `"new_message"` notification for every event type it receives
(EventCreated, EventUpdated, EventDeleted, EventThreadReplyAdded, …). It should return early for
anything that is not `EventCreated`.

```go
if evt.Event != model.EventCreated {
    return nil
}
```

### 2. Route thread replies to thread subscribers only

Thread-only replies have `ThreadParentMessageID != ""` and `TShow == false`. They are invisible
in the main room and should notify only the thread's subscribers — not all room members.
Replies sent with the "Also send to channel" option (`TShow == true`) are NOT thread-only: they
appear in the room timeline and are treated as channel messages for notification fan-out
(`isThreadOnlyReply` in notification-worker/handler.go; covered by
TestHandle_ThreadReply_TShow_TreatedAsChannelMessage).

This requires:

- A `ThreadSubscriberLookup` interface backed by the `thread_subscriptions` MongoDB collection
  (same collection that broadcast-worker uses via its store).
- A `fanOutToThreadSubscribers` function that lists subscribers, excludes the sender, and
  publishes `notifData` to each.
- Wiring the lookup into `NewHandler` and `main.go`.

### 3. Notify @-mentioned non-subscribers

When a thread reply @-mentions a user who is not yet a thread subscriber, that user should still
receive a notification. The resolved `Mentions []model.Participant` slice is **not** available on
`EventCreated` (message-gatekeeper publishes it before mention resolution). It is available on the
`EventThreadReplyAdded` event published by message-worker after saving to Cassandra.

The correct approach:
- Handle `EventThreadReplyAdded` in `HandleMessage` in addition to `EventCreated`.
- For `EventCreated` thread replies: notify thread subscribers (no resolved Mentions yet).
- For `EventThreadReplyAdded`: notify only @-mentioned accounts that are **not** already thread
  subscribers (they were notified on EventCreated). Skip `"all"` — it is not a real account.

### Key files to read before starting

| File | Why |
|------|-----|
| `notification-worker/handler.go` | Current handler — in-code TODOs at HandleMessage |
| `broadcast-worker/handler.go` | Reference implementation for thread subscriber fan-out |
| `broadcast-worker/store.go` | `ThreadSubscriptions` store interface shape |
| `pkg/model/event.go` | `EventCreated`, `EventThreadReplyAdded`, `MessageEvent.Mentions` |
| `pkg/subject/subject.go` | `subject.Notification(account)` builder |
