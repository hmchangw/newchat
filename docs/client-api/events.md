> Request/Reply and Events views of the chat client API — see also [client-api.md](../client-api.md).

<!-- last synced: client-api.md @ 117da0c -->

# Chat — Server-to-Client Events

This document catalogs every server-pushed event a client may receive.
Each event type is defined **once** here; request/reply RPCs cross-link to this file
via `**Emits:** … → events.md#<anchor>`.

For request payloads and reply schemas see [request-reply.md](request-reply.md).
For connection, auth, and error details see [../client-api.md](../client-api.md).

---

## Table of contents

1. [AsyncJobResult — async completion](#asyncjobresult--async-completion)
2. [subscription.update — membership / state changes](#subscriptionupdate--membership--state-changes)
3. [room.key — room encryption key delivery](#roomkey--room-encryption-key-delivery)
4. [Room events — per-room live events](#room-events--per-room-live-events)
   - [new_message (RoomEvent)](#new_message-roomevent)
   - [message_edited (EditRoomEvent)](#message_edited-editroomevent)
   - [message_deleted (DeleteRoomEvent)](#message_deleted-deleteroomevent)
   - [message_pinned / message_unpinned (PinStateRoomEvent)](#message_pinned--message_unpinned-pinstateroomevent)
   - [message_reacted (ReactRoomEvent)](#message_reacted-reactroomevent)
   - [thread_metadata_updated (ThreadMetadataUpdatedEvent)](#thread_metadata_updated-threadmetadataupdatedevent)
   - [message_read (MessageReadEvent)](#message_read-messagereadevent)
   - [thread_message_read](#thread_message_read)
   - [room_renamed (RoomRenamedRoomEvent)](#room_renamed-roomrenamedroomevent)
   - [room_restricted (RoomRestrictedRoomEvent)](#room_restricted-roomrestrictedroomevent)
5. [member — room membership events](#member--room-membership-events)
   - [member_added (MemberAddEvent)](#member_added-memberaddevent)
   - [member_left / member_removed (MemberRemoveEvent)](#member_left--member_removed-memberremoveevent)
6. [notification — reaction notification](#notification--reaction-notification)
7. [Presence events](#presence-events)

---

## Subject patterns

| Subject | Events delivered |
|---|---|
| `chat.user.{account}.response.{requestID}` | AsyncJobResult (one-shot async job completion) |
| `chat.user.{account}.event.subscription.update` | SubscriptionUpdateEvent / SubscriptionRemovedEvent |
| `chat.user.{account}.event.room.key` | RoomKeyEvent |
| `chat.room.{roomID}.event` | new_message, message_edited, message_deleted, message_pinned/unpinned, message_reacted, thread_metadata_updated, message_read, thread_message_read, room_renamed, room_restricted |
| `chat.user.{account}.event.room` | same event types as above, per-user fan-out for DM/botDM rooms |
| `chat.room.{roomID}.event.member` | member_added, member_left / member_removed |
| `chat.user.{account}.notification` | NotificationEvent (reaction only) |
| `chat.user.presence.state.{account}` | PresenceState |

---

## AsyncJobResult — async completion

**Subject:** `chat.user.{requesterAccount}.response.{requestID}`

Delivered when an async room-worker job completes. The client must already be subscribed
to `chat.user.{account}.>` to receive it. Triggered by Create Room, Add Members, Remove
Member, Update Member Role (see
[request-reply.md](request-reply.md)).

| Field | Type | Notes |
|---|---|---|
| `requestId` | string | Echoes the `X-Request-ID` header value from the original request. |
| `operation` | string | One of `"room.create"`, `"room.member.add"`, `"room.member.remove"`, `"room.member.remove_org"`, `"room.member.role_update"`, `"room.rename"`. |
| `status` | string | `"ok"` or `"error"`. |
| `roomId` | string | Optional. The affected room. |
| `error` | string | Optional. User-safe message; present only when `status="error"`. |
| `code` | string | Optional. Errcode category (`bad_request`, `not_found`, `forbidden`, `conflict`, `internal`, …). Present only when `status="error"`. |
| `reason` | string | Optional. Domain reason (e.g. `not_room_member`, `max_room_size_reached`). Present only when `status="error"` and a reason was attached server-side. |
| `timestamp` | number | Epoch ms (UTC). |

```json
{
  "requestId": "01970a4f-8c2d-7c9a-abcd-e0123456789f",
  "operation": "room.member.add",
  "status": "ok",
  "roomId": "01970a4f8c2d7c9aQ",
  "timestamp": 1746518400456
}
```

---

## subscription.update — membership / state changes

**Subject:** `chat.user.{account}.event.subscription.update`

Emitted when a user's membership or subscription state changes. Clients update their
local sidebar cache from this event.

Two shapes exist — discriminated by `action`:

### `added` / `role_updated` / `mute_toggled` / `favorite_toggled` / `read` (SubscriptionUpdateEvent)

| Field | Type | Notes |
|---|---|---|
| `userId` | string | The affected user's internal user ID. Omitted on the org-removal path. |
| `subscription` | [Subscription](../client-api.md#subscription) | Full Subscription record for `added` / `role_updated` / `mute_toggled` / `favorite_toggled` / `read`. On `read`, `hasMention` and `hasGroupMention` are both `false` — reading the room clears both. |
| `action` | string | `"added"`, `"role_updated"`, `"mute_toggled"`, `"favorite_toggled"`, or `"read"`. |
| `roomName` | string | Per-subscriber display label. On `added`: channel name / DM counterpart's display name / bot app name. On `role_updated`: the channel name. Omitted on `mute_toggled` / `favorite_toggled` / `read`. |
| `timestamp` | number | Epoch ms (UTC). |

```json
{
  "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "subscription": {
    "id": "01970a4f8c2d7c9a01970a4f8c2d7c9b",
    "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "bob", "isBot": false },
    "roomId": "01970a4f8c2d7c9aQ",
    "roomType": "channel",
    "siteId": "siteA",
    "roles": ["member"],
    "joinedAt": "2026-05-06T08:01:23Z"
  },
  "action": "added",
  "roomName": "engineering-announcements",
  "timestamp": 1746518483000
}
```

### `removed` (SubscriptionRemovedEvent)

Uses a lean `RemovedSubscriptionRef` instead of the full Subscription, so no zero-valued
fields are sent.

| Field | Type | Notes |
|---|---|---|
| `userId` | string | The removed user's internal user ID. Omitted on the org-removal path (only `subscription.u.account` is set). |
| `subscription` | [RemovedSubscriptionRef](#removedsubscriptionref) | Lean ref — see below. |
| `action` | string | Always `"removed"`. |
| `timestamp` | number | Epoch ms (UTC). |

#### RemovedSubscriptionRef

| Field | Type | Notes |
|---|---|---|
| `roomId` | string | The room the user lost. |
| `roomType` | string | `"channel"`, `"dm"`, `"botDM"`, or `"discussion"`. |
| `u` | [SubscriptionUser](../client-api.md#subscriptionuser) | The removed user. On org removals only `account` is guaranteed. |

```json
{
  "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "subscription": {
    "roomId": "01970a4f8c2d7c9aQ",
    "roomType": "channel",
    "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "bob", "isBot": false }
  },
  "action": "removed",
  "timestamp": 1746518483000
}
```

**Triggered by:** Add Members (`added`), Remove Member (`removed`), Update Member Role
(`role_updated`), Toggle Mute (`mute_toggled`), Toggle Favorite (`favorite_toggled`),
Mark Messages Read (`read`) — see [request-reply.md](request-reply.md).

---

## room.key — room encryption key delivery

**Subject:** `chat.user.{account}.event.room.key`

Delivers the AES-256-GCM room key to channel members. Fired at create, add, and remove.
DM/botDM rooms are never encrypted and emit no key event.

| Field | Type | Notes |
|---|---|---|
| `roomId` | string | The room the key belongs to. |
| `version` | integer | Room-key version. Incremented on each rotation (member remove). |
| `privateKey` | string | Base64-encoded 32-byte room secret — used directly as the AES-256-GCM key. |
| `timestamp` | number | Epoch ms (UTC). |

```json
{
  "roomId": "01970a4f8c2d7c9aQ",
  "version": 1,
  "privateKey": "<base64-encoded 32-byte room secret>",
  "timestamp": 1747000000000
}
```

**When fired:**

- **Create Room (channel):** one event per initial enrolled member.
- **Add Members (channel):** one event per newly-subscribed account; existing members receive no duplicate.
- **Remove Member (channel):** the key is rotated; every surviving member receives a new event with `version` incremented. The removed account stops receiving events.

**Initial key bootstrap on (re)connect:** live events fire only when keys change.
The initial key set is delivered via `room.privateKey` / `room.keyVersion` on each
enriched [Subscription](../client-api.md#subscriptionroom) returned by
`subscription.list` / `subscription.getChannels` / etc.

**On-demand fetch:** if a client holds no key for `(roomId, version)` (e.g. reconnected
after the live event was delivered), call
`chat.user.{account}.request.room.{roomID}.{siteID}.key.get` — see
[request-reply.md § Room Encryption Key Get](request-reply.md#room-encryption-key-get).

**Client decryption:** `AES-GCM-Decrypt(privateKey, nonce, ciphertext, aad=empty)`.
`encryptedMessage` decrypts to a UTF-8-encoded JSON `ClientMessage`;
`encryptedNewContent` (edit) decrypts to a plain UTF-8 content string.
Retain past versions for history scrolling (server grace window: at least 24h).

---

## Room events — per-room live events

### Subjects

| Room type | Subject |
|---|---|
| Channel | `chat.room.{roomID}.event` |
| DM / botDM | `chat.user.{account}.event.room` — published per non-bot member |

The `type` field discriminates the event. All payloads carry `type`, `roomId`,
`siteId`, and `timestamp`.

---

### new_message (RoomEvent)

The live fan-out for a newly created message (plain send, thread reply, quoted send, or
system message). Triggered by [Send Message](request-reply.md#send-message).

**botDM rooms receive no `new_message` fan-out** — `broadcast-worker` skips `botDM`
room types; bots consume messages through a separate backend path.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"new_message"`. |
| `roomId` | string | |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `eventTimestamp` | number | Optional. Epoch ms (UTC). Canonical event time. |
| `roomName` | string | |
| `roomType` | string | `"channel"`, `"dm"`, etc. |
| `siteId` | string | |
| `userCount` | number | |
| `lastMsgAt` | string | RFC 3339. |
| `lastMsgId` | string | The new message's ID. |
| `mentions` | [Participant](../client-api.md#participant)[] | Optional. |
| `mentionAll` | boolean | Optional. `true` if `@all` or `@here` was used. |
| `hasMention` | boolean | Optional. Per-recipient flag — present only on DM events. |
| `message` | [ClientMessage](#clientmessage) | Optional. Set for unencrypted rooms. |
| `encryptedMessage` | [EncryptedMessage](../client-api.md#encryptedmessage) | Optional. Set for encrypted channel rooms. Decrypt with room key for `version`. |

#### ClientMessage

The broadcast message payload (distinct from the history Message schema which is the
Cassandra projection).

| Field | Type | Notes |
|---|---|---|
| `id` | string | Message ID. |
| `roomId` | string | |
| `userId` | string | Sender's internal user ID. |
| `userAccount` | string | Sender's account. |
| `userDisplayName` | string | Optional. Render-ready sender name. |
| `content` | string | The message body. |
| `sender` | [Participant](../client-api.md#participant) | Optional. Enriched sender identity. |
| `attachments` | [Attachment](../client-api.md#attachment)[] | Optional. Decoded attachment objects (same shape as history). |
| `card` | [MessageCard](../client-api.md#messagecard) | Optional. |
| `cardAction` | [MessageCardAction](../client-api.md#messagecardaction) | Optional. |
| `mentions` | [Participant](../client-api.md#participant)[] | Optional. |
| `createdAt` | string | RFC 3339. |
| `editedAt` | string | Optional. RFC 3339. |
| `updatedAt` | string | Optional. RFC 3339. |
| `threadParentMessageId` | string | Optional. Set for a thread reply. |
| `threadParentMessageCreatedAt` | string | Optional. RFC 3339. Server-resolved best-effort; absent when unresolved at send time. |
| `tshow` | boolean | Optional. Whether a thread reply is also shown in the parent room. |
| `type` | string | Optional. System-message type. |
| `sysMsgData` | string | Optional. Base64-encoded raw JSON payload for system messages. |
| `quotedParentMessage` | [QuotedParentMessage](../client-api.md#quotedparentmessage) | Optional. |
| `pinnedAt` | string | Optional. RFC 3339. |
| `pinnedBy` | [Participant](../client-api.md#participant) | Optional. |

Channel example (encrypted):

```json
{
  "type": "new_message",
  "roomId": "01970a4f8c2d7c9aQ",
  "timestamp": 1746518100123,
  "eventTimestamp": 1746518100100,
  "roomName": "engineering-announcements",
  "roomType": "channel",
  "siteId": "siteA",
  "userCount": 12,
  "lastMsgAt": "2026-05-06T07:55:00Z",
  "lastMsgId": "01970a4f8c2d7c9aQRST",
  "encryptedMessage": {
    "version": 3,
    "nonce": "<base64-12-bytes>",
    "ciphertext": "<base64-content-plus-16-byte-tag>"
  }
}
```

DM example (plaintext):

```json
{
  "type": "new_message",
  "roomId": "alice___bob",
  "timestamp": 1746518100123,
  "eventTimestamp": 1746518100100,
  "roomName": "alice, bob",
  "roomType": "dm",
  "siteId": "siteA",
  "userCount": 2,
  "lastMsgAt": "2026-05-06T07:55:00Z",
  "lastMsgId": "01970a4f8c2d7c9aQRST",
  "hasMention": false,
  "message": {
    "id": "01970a4f8c2d7c9aQRST",
    "roomId": "alice___bob",
    "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
    "userAccount": "alice",
    "content": "morning team",
    "createdAt": "2026-05-06T07:55:00Z",
    "sender": {
      "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
      "account": "alice",
      "chineseName": "愛麗絲",
      "engName": "Alice"
    }
  }
}
```

---

### message_edited (EditRoomEvent)

Flat event — no zero-valued `RoomEvent` base fields. Triggered by
[Edit Message](request-reply.md#edit-message).

**Subjects:** channel rooms → `chat.room.{roomID}.event`; DM/botDM rooms →
`chat.user.{recipient}.event.room` per non-bot member.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_edited"`. |
| `roomId` | string | |
| `siteId` | string | |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `eventTimestamp` | number | Optional. Epoch ms (UTC). Canonical event time. |
| `messageId` | string | The edited message's ID. |
| `newContent` | string | Optional. New plaintext content. Present for DMs and unencrypted channels. |
| `encryptedNewContent` | [EncryptedMessage](../client-api.md#encryptedmessage) | Optional. For encrypted channel rooms. Decrypt with the room key to obtain the new content string. |
| `editedBy` | string | The sender's account. |
| `editedAt` | string | RFC 3339 timestamp. Domain time of the edit. |
| `updatedAt` | string | RFC 3339 timestamp. |

```json
{
  "type": "message_edited",
  "roomId": "01970a4f8c2d7c9aQ",
  "siteId": "siteA",
  "timestamp": 1746518700123,
  "messageId": "01970a4f8c2d7c9aQRST",
  "newContent": "morning team — updated",
  "editedBy": "alice",
  "editedAt": "2026-05-06T08:05:00Z",
  "updatedAt": "2026-05-06T08:05:00Z"
}
```

---

### message_deleted (DeleteRoomEvent)

Flat event. Triggered by [Delete Message](request-reply.md#delete-message).

**Subjects:**

- Top-level channel message → `chat.room.{roomID}.event`.
- Thread reply (`tshow=false`) in a channel → `chat.user.{recipient}.event.room` per thread subscriber.
- Thread reply (`tshow=true`) in a channel → `chat.room.{roomID}.event`.
- DM/botDM → `chat.user.{recipient}.event.room` per non-bot member.

Thread-reply deletes **additionally** emit a
[`thread_metadata_updated`](#thread_metadata_updated-threadmetadataupdatedevent) event.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_deleted"`. |
| `roomId` | string | |
| `siteId` | string | |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `eventTimestamp` | number | Optional. Epoch ms (UTC). Canonical event time. Omitted for legacy events. |
| `messageId` | string | The deleted message's ID. |
| `deletedBy` | string | The sender's account. |
| `deletedAt` | string | RFC 3339 timestamp. Domain time of the delete. |
| `updatedAt` | string | RFC 3339 timestamp. |

```json
{
  "type": "message_deleted",
  "roomId": "01970a4f8c2d7c9aQ",
  "siteId": "siteA",
  "timestamp": 1746518800123,
  "messageId": "01970a4f8c2d7c9aQRST",
  "deletedBy": "alice",
  "deletedAt": "2026-05-06T08:06:40Z",
  "updatedAt": "2026-05-06T08:06:40Z"
}
```

---

### message_pinned / message_unpinned (PinStateRoomEvent)

Flat event. Same struct for both pin and unpin; `type` and `pinned` discriminate.
Triggered by [Pin Message](request-reply.md#pin-message) and
[Unpin Message](request-reply.md#unpin-message).

**Subjects:** channel rooms → `chat.room.{roomID}.event`; DM/botDM rooms →
`chat.user.{account}.event.room` per non-bot member.

Not published when the request hits an already-pinned (pin) or already-unpinned (unpin)
message (idempotent short-circuit).

| Field | Type | Notes |
|---|---|---|
| `type` | string | `"message_pinned"` or `"message_unpinned"`. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `eventTimestamp` | number | Optional. Epoch ms (UTC). Canonical event time. |
| `roomId` | string | |
| `siteId` | string | Originating site. |
| `messageId` | string | The pinned/unpinned message's ID. |
| `pinned` | boolean | Resulting pin state. `true` for `message_pinned`, `false` for `message_unpinned`. |
| `by` | [Participant](../client-api.md#participant) | Optional. Actor who performed the pin/unpin. |
| `at` | string | RFC 3339. Domain time of the change. |

```json
{
  "type": "message_pinned",
  "timestamp": 1746518900123,
  "eventTimestamp": 1746518900100,
  "roomId": "01970a4f8c2d7c9aQ",
  "siteId": "site1",
  "messageId": "01970a4f8c2d7c9aQRST",
  "pinned": true,
  "by": { "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" },
  "at": "2026-05-06T08:01:40Z"
}
```

---

### message_reacted (ReactRoomEvent)

Live reaction toggle event. Triggered by [React to Message](request-reply.md#react-to-message).

**Subjects:** channel rooms → `chat.room.{roomID}.event`; DM rooms →
`chat.user.{account}.event.room` per non-bot member.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_reacted"`. |
| `roomId` | string | |
| `siteId` | string | |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `eventTimestamp` | number | Optional. Epoch ms (UTC). Canonical event time. |
| `messageId` | string | The reacted-to message's ID. |
| `shortcode` | string | The bare reaction shortcode. |
| `action` | string | `"added"` or `"removed"`. |
| `actor` | [Participant](../client-api.md#participant) | The user whose toggle produced this event. Full Participant — includes display names; for a bot actor, `displayName` is the app's name (falls back to composed name if no app matches). |
| `reactedAt` | string (RFC 3339) | Domain time of the toggle. |
| `updatedAt` | string (RFC 3339) | Mirrors `reactedAt`. |

To merge into the history-derived `reactions` map: append or remove one entry under
`reactions[shortcode]` keyed on `actor.account`.

```json
{
  "type": "message_reacted",
  "roomId": "01970a4f8c2d7c9aQ",
  "siteId": "site-a",
  "timestamp": 1746518900123,
  "messageId": "01970a4f8c2d7c9aQRST",
  "shortcode": "acme_party",
  "action": "added",
  "actor": {
    "userId": "u-alice",
    "account": "alice",
    "siteId": "site-a",
    "engName": "Alice"
  },
  "reactedAt": "2026-05-06T11:28:20Z",
  "updatedAt": "2026-05-06T11:28:20Z"
}
```

---

### thread_metadata_updated (ThreadMetadataUpdatedEvent)

Pushed whenever a thread reply is **created** or **deleted**, so clients can update the
reply-count badge on the parent message without reloading the thread.

Published in **addition** to the `new_message` or `message_deleted` event — handle each
independently.

**Subjects:** channel rooms → `chat.room.{roomID}.event`; DM/botDM rooms →
`chat.user.{account}.event.room` per non-bot member.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"thread_metadata_updated"`. |
| `roomId` | string | The room the thread lives in. |
| `siteId` | string | |
| `timestamp` | number | Epoch ms (UTC). When broadcast-worker published this event. |
| `eventTimestamp` | number | Optional. Epoch ms (UTC). When message-worker published the canonical event. Prefer over `timestamp` for ordering. |
| `parentMessageId` | string | The thread parent message's ID. Use to locate the message in your cache and update its badge. |
| `replyMessageId` | string | The reply that was added or deleted. |
| `newTcount` | number | Authoritative reply count for the parent message, capped at 99 (99 means "99 or more"). Apply directly — do not delta. |
| `newThreadLastMsgAt` | string (ISO 8601) | Optional. Timestamp of the most recent surviving thread reply. Absent when `newTcount` is 0. |
| `action` | string | `"reply_added"` or `"reply_deleted"`. |

```json
{
  "type": "thread_metadata_updated",
  "roomId": "01970a4f8c2d7c9aQ",
  "siteId": "siteA",
  "parentMessageId": "01970a4f8c2d7c9aQRST",
  "newTcount": 4,
  "newThreadLastMsgAt": "2026-06-18T10:00:00Z",
  "action": "reply_added",
  "replyMessageId": "01970a4f8c2d7c9aUVWX",
  "timestamp": 1746518100123
}
```

**Client handling:** apply `newTcount` directly (not as a delta). When `eventTimestamp`
is present, prefer the event with the larger `eventTimestamp` for out-of-order handling.

---

### message_read (MessageReadEvent)

Published only when the room's read floor (`Room.MinUserLastSeenAt`) advances. Triggered
by [Mark Messages Read](request-reply.md#mark-messages-read).

<!-- union-merge note: the "channel rooms" and "DM rooms" subjects are described
separately in client-api.md §3.1 and §3.2; both are included here. -->

**Subjects:**
- Channel rooms → `chat.room.{roomID}.event` — one event to all subscribers.
- DM rooms → `chat.user.{account}.event.room` — one event per subscriber.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_read"`. |
| `roomId` | string | The room whose floor advanced. |
| `minUserLastSeenAt` | string | Optional. RFC 3339 UTC. The new read floor. **Omitted** when the floor is null (a member is still fully unread). |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |

```json
{
  "type": "message_read",
  "roomId": "Rb3kQ2",
  "minUserLastSeenAt": "2026-06-09T10:30:00Z",
  "timestamp": 1749465000123
}
```

---

### thread_message_read

Published only when a thread's read floor (`thread_rooms.minUserLastSeenAt`) advances.
Triggered by [Mark Thread as Read](request-reply.md#mark-thread-as-read). Best-effort — a
publish failure does not fail the RPC; never fires when the floor is unchanged or the
thread room is missing.

**Subjects — routed by the *parent* room's type:**
- Channel parent → `chat.room.{roomID}.event` — one event to every client subscribed to
  the parent room.
- DM parent → `chat.user.{account}.event.room` — one event per subscriber.
- botDM / other parent types → no fan-out (the floor is always null).

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"thread_message_read"`. |
| `roomId` | string | The **parent** room (for client routing/scoping). |
| `threadRoomId` | string | The thread room whose floor advanced. |
| `minUserLastSeenAt` | string | Optional. RFC3339 UTC timestamp of the new read floor. **Omitted** when the floor is null. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |

```json
{
  "type": "thread_message_read",
  "roomId": "Rb3kQ2",
  "threadRoomId": "Tx9aLm",
  "minUserLastSeenAt": "2026-06-09T10:30:00Z",
  "timestamp": 1749465000123
}
```

---

### room_renamed (RoomRenamedRoomEvent)

Flat event — no zero-valued `RoomEvent` base fields. Triggered by
[Rename Room](request-reply.md#rename-room).

**Subject:** `chat.room.{roomID}.event` — all room members on all sites.

> No separate `subscription.update` fires for renames. Clients drive their local
> subscription `name` update off this single room-scoped event.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"room_renamed"`. |
| `roomId` | string | The renamed room. |
| `siteId` | string | Home site of the room. |
| `timestamp` | number | Publish time, milliseconds since Unix epoch (UTC). |
| `newName` | string | The new room name. |
| `byAccount` | string | The account that performed the rename. |
| `renamedAt` | string | ISO-8601 timestamp of when the rename was applied. |

```json
{
  "type": "room_renamed",
  "roomId": "01970a4f8c2d7c9aQ",
  "siteId": "siteA",
  "timestamp": 1746518483000,
  "newName": "engineering-general",
  "byAccount": "alice",
  "renamedAt": "2026-05-06T08:01:23Z"
}
```

---

### room_restricted (RoomRestrictedRoomEvent)

Flat event. Emitted when a channel's `restricted` / `externalAccess` flags change.
This is a **server-internal admin RPC** — not a client-callable request. Clients receive
the event on the same room stream they already subscribe to.

**Subject:** `chat.room.{roomID}.event`.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"room_restricted"`. |
| `roomId` | string | The room whose flags changed. |
| `siteId` | string | Home site of the room. |
| `timestamp` | number | Publish time (UTC ms). |
| `restricted` | boolean | The new restricted state. |
| `externalAccess` | boolean | The new external-access state. |
| `ownerAccount` | string | Optional. Omitted unless this was an unrestricted→restricted transition with a designated owner. |
| `byAccount` | string | The admin who made the change. |
| `changedAt` | string | ISO-8601 timestamp of when the change was applied. |

---

## member — room membership events

**Subject:** `chat.room.{roomID}.event.member`

### member_added (MemberAddEvent)

Published once when at least one new account or org was added. Triggered by
[Add Members](request-reply.md#add-members) and indirectly by
[Create Room](request-reply.md#create-room).

A `members_added` system message also flows through the message pipeline as a
`new_message` room event.

> **No-op:** if every requested account is already a member (or the add only upgrades an
> existing org member to individual membership), no `member_added` event fires.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"member_added"`. |
| `roomId` | string | |
| `roomName` | string | |
| `roomType` | string | `"channel"`, `"dm"`, `"botDM"`, or `"discussion"`. Omitted when empty. |
| `accounts` | string[] | The newly added accounts. |
| `siteId` | string | The room's home site. |
| `requesterAccount` | string | The account that initiated the add. Omitted when empty. |
| `joinedAt` | number | Epoch ms (UTC). |
| `historySharedSince` | number | Optional. Epoch ms (UTC); present when prior history is shared with new members. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |

---

### member_left / member_removed (MemberRemoveEvent)

Triggered by [Remove Member](request-reply.md#remove-member).

A `member_left` / `member_removed` system message also flows through the message pipeline
as a `new_message` room event.

| Field | Type | Notes |
|---|---|---|
| `type` | string | `"member_left"` (self-leave) or `"member_removed"` (forced or org removal). |
| `roomId` | string | |
| `accounts` | string[] | The removed accounts. |
| `siteId` | string | The room's home site. |
| `orgId` | string | Present only on org removals. Omitted otherwise. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |

---

## notification — reaction notification

**Subject:** `chat.user.{account}.notification`

Sent to the **message author only** when someone reacts to their message with an `"added"` action
and the actor is not the author. Not emitted for reaction removals.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"reaction"`. |
| `roomId` | string | The room containing the reacted-to message. |
| `roomType` | string | Room type: `"channel"`, `"dm"`, or `"botDM"`. |
| `message` | [Message](../client-api.md#message-schema) | The full reacted-to message (same shape as history reads — `omitempty` fields like `tshow`/`threadParentMessageId` are absent, not `false`/`""`, when unset). |
| `reactionDelta` | [ReactionDelta](#reactiondelta) | The single-reaction delta that triggered the notification. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |

### ReactionDelta

| Field | Type | Notes |
|---|---|---|
| `shortcode` | string | The emoji shortcode reacted with. |
| `action` | string | Always `"added"` here (notification only fires on add). |
| `actor` | [Participant](../client-api.md#participant) | The user who reacted. `displayName` is populated (`CombineWithFallback(engName, chineseName, account)`); for a bot account (`.bot` suffix) it's the app's display name instead, falling back to the composed name if no app matches. |

---

## Presence events

### Live state

**Subject:** `chat.user.presence.state.{account}`

The owning home site publishes the user's effective status on every change. Subscribe
before the §7.6 batch query to avoid missing a transition.

#### PresenceState

| Field | Type | Notes |
|---|---|---|
| `account` | string | The user. |
| `siteId` | string | The user's home site. |
| `status` | string | Effective status: `"online"` / `"away"` / `"busy"` / `"dnd"` / `"brb"` / `"offline"` / `"in-call"`. `in-call` is set by an external Teams presence-sync signal (suppresses notifications; not settable as a manual status). `dnd` and `brb` are manual-only; `dnd` suppresses push notifications, `brb` resolves to `away` for display and remains push-eligible. |
| `timestamp` | number | Millis since Unix epoch (UTC) of the change. |

```json
{ "account": "bob", "siteId": "site-b", "status": "away", "timestamp": 1746518105000 }
```

### Publish events (fire-and-forget, client → server)

These are **not** server-to-client pushes — they are client publishes that update
server-side presence state. Documented here for completeness; they emit no reply events.

| Subject | Purpose |
|---|---|
| `chat.user.{account}.event.presence.{siteID}.hello` | Register a new connection (bring user online). |
| `chat.user.{account}.event.presence.{siteID}.ping` | Keep connection alive (~30 s interval). |
| `chat.user.{account}.event.presence.{siteID}.activity` | Report active/inactive flip. |
| `chat.user.{account}.event.presence.{siteID}.bye` | Best-effort disconnect (beforeunload). |

For payload details see [../client-api.md §8](../client-api.md#8-presence).
