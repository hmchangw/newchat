# unread-worker deployment

Applies the room-level MongoDB state derived from MESSAGES-CANONICAL:
`rooms.lastMsgAt`/`lastMsgId`/`lastMentionAllAt`, the sender's subscription
`lastSeenAt`, and the `hasMention` badge.

broadcast-worker keeps one MongoDB write of its own — the room-list preview,
buffered and drained best-effort, never awaited by a handler. It touches only the
`preview*` fields of the room document, which carry their own `previewAsOf`
watermark; nothing it writes overlaps what this service owns. It stayed there
because sealing a preview needs the resolved users, mention participants and
decoded attachments that the fan-out assembles anyway and this service, whose
whole contract is that it reads no MongoDB, deliberately does not have.

The one thing the two halves no longer guarantee each other is that
`previewForMsgId` and `lastMsgId` move together. history-service serves a stored
preview only while they agree, so a window where they do not costs that room one
Cassandra walk, which then warms the document back. Both services coalesce with
`msgbucket.NewerRow`, so they cannot disagree about which message is a room's
newest at a same-millisecond tie — only about when each has flushed.

## Deploy order

**Deploy unread-worker BEFORE rolling broadcast-worker to the release that
removes the room-pointer, lastSeenAt and mention writes.** In that order the old
broadcast-worker keeps writing until the new worker is live, and the overlap is
harmless — the writes are idempotent and additive. The reverse order leaves a window in which nobody writes to MongoDB
for messages sent during that window, and every one of those three writes is lost
for it, not just mention badges:

- **Mention badges** (`hasMention`) raised in that window are lost — the client
  never gets a reconnect-restorable `@` badge for those messages.
- **`rooms.lastMsgAt`** stalls at its pre-window value. Any unread computation of
  the form `lastMsgAt > lastSeenAt` reports the room as READ for the duration of
  the gap, even though new messages arrived — the unread (bold room name) badge
  goes silently missing, not just delayed.
- **Sender `lastSeenAt` advances** are lost. The sender's own subscription is not
  advanced to their message's `createdAt`, so `room-service`'s read-floor
  computation (`MinSubscriptionLastSeenByRoomID`) can undercount the floor for
  that room if a `messageRead` call lands inside the gap.

Rollback is an ordinary image rollback of broadcast-worker; the previous image
still writes.

## Tuning

| Variable | Meaning |
|---|---|
| `FLUSH_INTERVAL` | Coalescing window (default 250ms). Larger = fewer writes, more held messages. |
| `CONSUMER_MAX_ACK_PENDING` | Must exceed `FLUSH_INTERVAL` x peak message rate. Also the MongoDB-outage buffer: once full, JetStream stops delivering to this consumer and broadcast fan-out is unaffected. |
| `CONSUMER_ACK_WAIT` | Must exceed `FLUSH_INTERVAL` plus write latency. |

The consumer runs with `MaxDeliver=-1` so a MongoDB outage retries until it
recovers. Server-rejected documents are Ack-dropped with an ERROR log rather
than retried forever.

The health endpoint deliberately checks only NATS. Adding MongoDB would make
Kubernetes restart the pod during exactly the outage this service is built to
ride out, discarding the held batch each time.
