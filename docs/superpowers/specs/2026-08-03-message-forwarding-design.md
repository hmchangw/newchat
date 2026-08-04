# Message Forwarding Design

Date: 2026-08-03
Status: Approved (pending implementation plan)

## Goal

Let a user forward an existing chat message into one or more other rooms. The forwarded message renders in each destination room as a new message carrying an optional user comment plus a `forwardedMessage` snapshot of the source — visible to every member of the destination room through real-time delivery, push notifications, and persisted history. Mirrors the `quotedParentMessage` feature (2026-04-27 design) with the deltas below.

## Scope & decided semantics

- **Cross-room by definition.** The source message lives in a different room (or the same room — not forbidden) from the destination. The forwarder must be subscribed to the source room and inside its access window; authorization falls out of fetching the source via the existing `msg.get` RPC on the *source* room's subject.
- **Forward with optional comment.** The new message may carry both user-typed `content` and the snapshot, or the snapshot alone (`content` empty). The non-empty-content validation rule is skipped when forwarding.
- **Multi-room = client fan-out.** The client sends one `msg.send` publish per destination room, each with its own client-generated message ID and `requestId`. Per-room success/failure falls out naturally as N independent replies — no new RPC, no server-side fan-out.
- **Source may be a thread message; destination is always the main timeline.** A forward request combined with `threadParentMessageId` is rejected. The source's thread identity is derived server-side from the fetched message — the client never supplies a source threadId.
- **Text-only snapshot.** Forwarding is rejected when the source has attachments, has a card, or is a system message. The new (forwarding) message itself must not carry attachments either.
- **Chain depth stays 1.** Forwarding a message that is itself a forward captures that message's *own* content (its comment) only — the nested `forwardedMessage` snapshot is dropped, and the snapshot's sender is the immediate message's sender. Forwarding a forward whose own comment is empty is rejected (nothing forwardable). A source's `quotedParentMessage` is likewise not copied into the snapshot.
- **Hard-fail policy.** Any failure to fetch or accept the source (not found, forbidden, RPC error, timeout, attachment/card/system rejection) fails that send; the error is replied to the client and the message is NOT published to MESSAGES_CANONICAL. No placeholder snapshot, no `Unverified` marker, no message-worker re-projection — a forward without its content is meaningless, and hard-fail removes the entire cross-room re-projection problem.
- **Snapshot is immutable and self-contained.** Captured at forward time; later edits or deletion of the source do not touch it. It is never access-window-redacted on read (see Read path).
- **Client body override.** An optional `forwardedContent` on the send request replaces `ForwardedMessage.Msg` — for forwarding a selected excerpt rather than the whole message. The source is still fetched and every rule above still runs against it; only the body is substituted, and it is applied *after* the reject rules so an override can replace a real body but never manufacture one for a source that had none. Deliberately carries **no provenance marker**: once stored, a client-supplied body is indistinguishable from the fetched one, under the source author's identity. Everything else on the snapshot stays server-derived.
- **Notification marks forwards.** Push payload gains a `forwarded` flag so clients can render "forwarded a message" even when `content` is empty.
- **Search indexes only the comment.** The snapshot is never indexed — this is already the status quo (search-sync-worker maps only `Content`); zero search changes.

## Non-goals

- `PreviewMessage.forwardSource` (the reserved `TODO(#106)` in `pkg/model/message.go`) — separate follow-up.
- Bot-originated forwards — `bot-message-worker` is untouched; the field is never set on its path.
- Forwarding into threads, forwarding attachments/cards, editing the snapshot after the fact.
- No feature flag; gated by clients sending the new request fields.
- No data migration beyond additive DDL.

## Architecture

```
client ──(SendMessageRequest{forwardedMessageId, forwardedRoomId})──> MESSAGES stream
        (one publish per destination room; per-room replies)             │
                                                                         ▼
                                              message-gatekeeper
                                              ├─ existing validation (UUID, sub, …)
                                              ├─ forward-specific validation (mutual
                                              │   exclusions, no attachments on send)
                                              ├─ NATS request to history-service
                                              │   subject: chat.user.{account}.request
                                              │            .room.{forwardedRoomId}.{siteID}.msg.get
                                              │   ├─ success → reject if source has
                                              │   │   attachments/card/system type or is an
                                              │   │   empty-comment forward; else project to
                                              │   │   *cassandra.ForwardedMessage
                                              │   └─ ANY error → hard-fail: reply error, ack,
                                              │       do NOT publish
                                              └─ publish MessageEvent → MESSAGES_CANONICAL
                                                                         │
                      ┌──────────────────────────┬───────────────────────┴──────┐
                      ▼                          ▼                              ▼
                message-worker             broadcast-worker            notification-worker
                persists snapshot in       ships snapshot via          sets forwarded flag
                forwarded_message          embedded Message            on push payload
                column on insert           (zero changes)
```

The canonical event is the single source of truth. Once gatekeeper has built the snapshot, no downstream worker re-resolves it.

## Wire-format changes (`pkg/model`)

### `pkg/model/message.go`

```go
type SendMessageRequest struct {
    // ...existing fields...
    ForwardedMessageID string `json:"forwardedMessageId,omitempty"` // NEW: 17/20-char base62
    ForwardedRoomID    string `json:"forwardedRoomId,omitempty"`    // NEW: source room ID
}

type Message struct {
    // ...existing fields...
    ForwardedMessage *cassandra.ForwardedMessage `json:"forwardedMessage,omitempty" bson:"forwardedMessage,omitempty"` // NEW
}
```

`ForwardedMessageID` and `ForwardedRoomID` are required together; one without the other is `bad_request`.

### `pkg/model/cassandra/message.go` — new snapshot type

`ForwardedMessage` mirrors `QuotedParentMessage` minus the attachment pair and `TShow`:

```go
// ForwardedMessage maps to the Cassandra "ForwardedMessage" UDT.
type ForwardedMessage struct {
    MessageID             string        `json:"messageId"                       cql:"message_id"`
    RoomID                string        `json:"roomId"                          cql:"room_id"`
    Sender                Participant   `json:"sender"                          cql:"sender"`
    CreatedAt             time.Time     `json:"createdAt"                       cql:"created_at"`
    Msg                   string        `json:"msg,omitempty"                   cql:"msg"`
    Mentions              []Participant `json:"mentions,omitempty"              cql:"mentions"`
    MessageLink           string        `json:"messageLink,omitempty"           cql:"message_link"`
    ThreadParentID        string        `json:"threadParentId,omitempty"        cql:"thread_parent_id"`
    ThreadParentCreatedAt *time.Time    `json:"threadParentCreatedAt,omitempty" cql:"thread_parent_created_at"`
}
```

Embedded on the Cassandra row struct:

```go
ForwardedMessage *ForwardedMessage `json:"forwardedMessage,omitempty" cql:"forwarded_message"`
```

No `MessageEvent` changes — hard-fail means there is no unverified-marker analog of `QuotedParentUnverified`.

### `pkg/model/model_test.go` / `pkg/model/cassandra/message_test.go`

JSON round-trip cases for `Message` and the Cassandra row carrying a populated `ForwardedMessage`.

## Gatekeeper changes (`message-gatekeeper`)

### Validation (before fetch)

All `bad_request`, replied to the client, JetStream msg acked:

| Rule | Rationale |
|---|---|
| `forwardedMessageId` set XOR `forwardedRoomId` set | Both required together |
| `forwardedMessageId` fails `idgen.IsValidMessageID` | Same boundary check as quotes |
| Forward + `quotedParentMessageId` | A forward is not a quote |
| Forward + `threadParentMessageId` (or `tshow`) | Destination is main timeline only |
| Forward + attachments on the new message | Forward carries comment text only |
| Empty `content` is ALLOWED on a forward | Comment is optional |

### Fetch & snapshot (`fetcher_history.go`)

Extend the existing `ParentMessageFetcher` interface (same mock, same `historyParentFetcher` implementation) with:

```go
FetchForwardedSource(ctx context.Context, account, srcRoomID, siteID, messageID string) (*forwardedSourceProjection, error)
```

- Subject: `subject.MsgGet(account, srcRoomID, siteID)` — the *source* room. history-service's existing `findMessage` room check, subscription check, and access-window enforcement provide authorization for free. Cross-room reads are the point, so unlike quotes there is no same-room expectation.
- 2-second timeout, sonic codec, narrow projection struct (never decode the full `cassandra.Message` under sonic — the struct-keyed `Reactions` map breaks its decoder). The projection must additionally expose enough to enforce the text-only and chaining rules: `attachments` (presence only), `card` (presence only), `type`, `forwardedMessage` (presence only), plus the snapshot fields (`msg`, `sender`, `mentions`, `createdAt`, `roomId`, `threadParentId`, `threadParentCreatedAt`).
- Post-fetch rejections (all `bad_request`): source has attachments; source has a card; source is a system message; source is a forward with empty `msg`.
- Snapshot projection: copy `MessageID`, `RoomID` (from the reply), `Sender`, `CreatedAt`, `Msg`, `Mentions`, `ThreadParentID`, `ThreadParentCreatedAt`; build `MessageLink` from the injected `chatBaseURL` as `{base}/{roomID}/{messageID}` (existing `messageLink` helper).
- **Hard-fail**: every fetch error — typed errcode (not_found / forbidden / …) or transport (timeout, no responders, unmarshal) — fails the send. No `quoteFetchErrIsTerminal`-style tiering, no placeholder. Transport errors are wrapped into a client-facing typed error (`errcode.Unavailable`-class) so the JetStream message is acked and replied, not endlessly redelivered.

### `handler.go`

A `resolveForwardSnapshot` helper alongside `resolveQuoteSnapshot`; the canonical `model.Message` gains `ForwardedMessage: forwardSnapshot`. Everything else (dedup ID, reply marshaling, ack/nak discipline) is unchanged.

### Sonic

`pretouch.go` covers the new nested type transitively via `Message`. Extend `sonic_wire_test.go` with the forward fields.

### Tests

Handler table-driven cases: every validation row above; happy path (snapshot embedded on canonical event, reply carries it); fetch error → reply error + not published; chaining source (forward-of-forward projects comment only); empty-comment source rejected. Fetcher tests against in-process NATS: success projection, errcode envelope, no responder/timeout.

## Cassandra schema

`docs/cassandra_message_model.md` is the single source of truth; updated in this PR together with all mirrors.

### New UDT

```cql
CREATE TYPE IF NOT EXISTS chat."ForwardedMessage" (
  created_at               TIMESTAMP,
  mentions                 SET<FROZEN<"Participant">>,
  message_id               TEXT,
  message_link             TEXT,
  msg                      TEXT,
  room_id                  TEXT,
  sender                   FROZEN<"Participant">,
  thread_parent_id         TEXT,
  thread_parent_created_at TIMESTAMP
);
```

No `attachments` column — rejected at forward time, by design.

### New column — three tables only

```cql
ALTER TABLE chat.messages_by_room        ADD forwarded_message FROZEN<"ForwardedMessage">;
ALTER TABLE chat.messages_by_id          ADD forwarded_message FROZEN<"ForwardedMessage">;
ALTER TABLE chat.pinned_messages_by_room ADD forwarded_message FROZEN<"ForwardedMessage">;
```

`thread_messages_by_thread` is deliberately excluded: forwards never land in threads. Revisit only if forwarding into threads is ever allowed.

### Mirrors updated in the same PR

- `docker-local/cassandra/init/` — new `NN-udt-forwarded_message.cql` + the three table files.
- `history-service/internal/cassrepo/integration_test.go` inline UDT/DDL.
- `history-service/docker-local/docker-compose.yml` DDL.

### Production migration

`CREATE TYPE` + the three `ALTER TABLE ADD` statements. Additive, online-safe, no backfill; old rows read back NULL.

## Encryption at rest (`pkg/atrest` + `cassrepo/write.go`)

The snapshot's `msg` body joins the encrypted bundle, mirroring the quote treatment:

- `EncryptedFields` gains `ForwardedContent *ForwardedEncrypted` with `ForwardedEncrypted{ Msg string }` (no attachments field).
- `SplitForEncryption` moves `ForwardedMessage.Msg` into the bundle; `StripEncryptedFields` blanks it on the stored UDT; `ApplyDecryptedFields` restores it on read.
- `cassrepo/write.go` edit path: `blankForwardedBody` mirror of `blankQuotedBody`; `readEncryptedFields` SELECTs and promotes legacy-plaintext forward bodies; the encrypted-edit UPDATE statements bind `forwarded_message = ?` (not nulled — metadata survives edits, exactly like `quoted_parent_message`).

## message-worker

- `buildCassandraMessage`: assign `cm.ForwardedMessage = msg.ForwardedMessage` (no clone/re-encode needed — no attachments).
- `SaveMessage` + `saveMessageEncrypted`: bind `forwarded_message` in both `messages_by_room` and `messages_by_id` INSERTs. Encrypted variant strips the body into `enc_payload` via the atrest changes.
- `SaveThreadMessage` / `saveThreadMessageEncrypted`: untouched (gatekeeper guarantees no forward reaches the thread path).
- No re-projection hook — hard-fail means every persisted snapshot's *identity* (sender, room, timestamps, thread identity) is server-derived and trusted. The body is not: see "Client body override" below.
- Tests: handler table extension (snapshot pointer reaches the store); integration round-trip on both tables, plaintext and encrypted.

## What is NOT changed

- `broadcast-worker` — the snapshot rides the embedded `Message` in `ClientMessage` automatically, like quotes.
- `bot-message-worker` — bots don't forward.
- `search-sync-worker` / `search-service` / `pkg/searchindex` — `MessageFields` already carries only `Content`; the snapshot is never indexed.
- `history-service` request handlers — the existing `msg.get` RPC serves the fetch as-is.

## history-service read path

- **No redaction of the forwarded snapshot.** The reader's access window in the destination room governs the forward message itself (standard row-level rules); the source room's window was enforced once at forward time, and the snapshot is self-contained thereafter. `quoteInaccessible` logic explicitly does NOT apply to `forwardedMessage`.
- `cassrepo` SELECT column lists (`messages_by_room.go`, `pin.go`, `messages_by_id` reads) add `forwarded_message`; `thread_messages.go` untouched.
- No attachment decode for the snapshot (it has none).
- **Pins**: pinned forwards persist the column; pre-access-window pin stubbing clears `ForwardedMessage` to `nil`, same as quotes.
- Edit/delete of the forwarding message behave as for any message; the snapshot metadata survives edits (see encryption section).

## notification-worker

`pkg/model/push.go` — `PushNotificationData` gains:

```go
Forwarded bool `json:"forwarded,omitempty"`
```

Set when `msg.ForwardedMessage != nil`. `Body` remains `msg.Content` (possibly empty — the client renders "forwarded a message" from the flag). `NotificationEvent` embeds the full `Message`, so the snapshot rides along unchanged.

## Client API docs (same PR)

- `docs/client-api.md`: msg.send request rows (`forwardedMessageId`, `forwardedRoomId`), success-response row (`forwardedMessage`), new `##### ForwardedMessage` schema table (linked type), JSON example, error-table rows for every rejection above (including the invalid-ID `bad_request`, which the quote docs missed), and the Message-schema row in §3.2.
- `docs/client-api/request-reply.md` and `docs/client-api/events.md` (ClientMessage table): matching rows.
- `docs/cassandra_message_model.md`: UDT block + three table columns + encryption-section mention of the forwarded body.

## Error enumeration (client-visible)

| Failure | Code |
|---|---|
| `forwardedMessageId`/`forwardedRoomId` inconsistency, invalid ID, forward+quote, forward+thread, attachments on send | `bad_request` |
| Source not found / source room mismatch | `not_found` (from history-service) |
| Forwarder not subscribed to source room / outside access window | `forbidden` (from history-service) |
| Source has attachments / card / system type; forward-of-forward with empty comment | `bad_request` |
| history-service unreachable / timeout | `unavailable`-class typed error (hard-fail, acked + replied) |

## Deploy order

1. Run `CREATE TYPE` + three `ALTER TABLE ADD` against each site's Cassandra keyspace.
2. Deploy `history-service` (reads/edits tolerate the new column; serves it once present).
3. Deploy `message-worker` (binds the new column; NULL when absent).
4. Deploy `notification-worker` (flag).
5. Deploy `message-gatekeeper` (starts accepting forward requests).

**Multi-site ordering:** complete steps 1–4 on every site before deploying step 5 (`message-gatekeeper`) to any site — a gatekeeper emitting forwards while a remote site still runs an old `message-worker` would persist federated copies without the snapshot (old binaries silently drop the unknown `forwardedMessage` field), causing silent cross-site divergence.

Old binaries after the migration are unaffected — gocql tolerates extra columns/UDTs.

## TDD ordering

Per CLAUDE.md, every change lands Red → Green → Refactor → Commit:

1. `cassandra.ForwardedMessage` struct + row field + round-trip tests + `cassandra_message_model.md` + local-dev DDL + history-service inline DDL mirrors.
2. `pkg/model` request/message fields + round-trip tests.
3. `pkg/atrest` `ForwardedContent` split/strip/apply + tests.
4. `message-gatekeeper` validation rows + `resolveForwardSnapshot` + handler tests (mock fetcher).
5. `message-gatekeeper` `FetchForwardedSource` projection + fetcher tests (in-process NATS) + sonic wire test.
6. `message-worker` binds + handler tests + integration round-trip (plaintext + encrypted).
7. `history-service` cassrepo SELECT/edit-path additions + pin stubbing + integration tests.
8. `notification-worker` flag + tests.
9. Docs (`client-api.md` + derived views).

## Observability

Forward failures surface through the existing `"process message failed"` ERROR log in `HandleJetStreamMsg` (with `error`, `account`, `roomID`); the wrapped error names the tripped rule. No new metrics.
