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
3. [settings.update — user settings sync](#settingsupdate--user-settings-sync)
4. [room.key — room encryption key delivery](#roomkey--room-encryption-key-delivery)
5. [Room events — per-room live events](#room-events--per-room-live-events)
   - [new_message (RoomEvent)](#new_message-roomevent)
   - [new_thread_message (RoomEvent)](#new_thread_message-roomevent)
   - [Thread view subject](#thread-view-subject)
   - [message_edited (EditRoomEvent)](#message_edited-editroomevent)
   - [message_deleted (DeleteRoomEvent)](#message_deleted-deleteroomevent)
   - [message_pinned / message_unpinned (PinStateRoomEvent)](#message_pinned--message_unpinned-pinstateroomevent)
   - [message_reacted (ReactRoomEvent)](#message_reacted-reactroomevent)
   - [thread_metadata_updated (ThreadMetadataUpdatedEvent)](#thread_metadata_updated-threadmetadataupdatedevent)
   - [message_read (MessageReadEvent)](#message_read-messagereadevent)
   - [thread_message_read](#thread_message_read)
   - [room_renamed (RoomRenamedRoomEvent)](#room_renamed-roomrenamedroomevent)
   - [room_restricted (RoomRestrictedRoomEvent)](#room_restricted-roomrestrictedroomevent)
6. [member — room membership events](#member--room-membership-events)
   - [member_added (MemberAddEvent)](#member_added-memberaddevent)
   - [member_left / member_removed (MemberRemoveEvent)](#member_left--member_removed-memberremoveevent)
7. [notification — reaction notification](#notification--reaction-notification)
8. [Presence events](#presence-events)

---

## Subject patterns

| Subject | Events delivered |
|---|---|
| `chat.user.{account}.response.{requestID}` | AsyncJobResult (one-shot async job completion) |
| `chat.user.{account}.event.subscription.update` | SubscriptionUpdateEvent / SubscriptionRemovedEvent |
| `chat.user.{account}.event.settings.update` | SettingsUpdateEvent |
| `chat.user.{account}.event.chatlist.update` | ChatlistUpdateEvent |
| `chat.user.{account}.event.room.key` | RoomKeyEvent |
| `chat.room.{roomID}.event` | new_message, message_edited, message_deleted, message_pinned/unpinned, message_reacted, thread_metadata_updated, message_read, thread_message_read, room_renamed, room_restricted |
| `chat.user.{account}.event.room` | same event types as above (per-user fan-out for DM/botDM rooms); **plus `new_thread_message`** — channel thread replies fan out per-subscriber on this subject, not the room subject |
| `chat.room.{roomID}.thread.{parentMessageID}.event` | new_thread_message, message_edited, message_deleted — the [thread view subject](#thread-view-subject), subscribed to only while a thread panel is open |
| `chat.room.{roomID}.event.member` (or `chat.local.room.{roomID}.event.member` for same-site rooms, by `crossSite`) | member_added, member_left / member_removed |
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
local sidebar cache from this event. Bot members receive it like any member; for a
bot recipient the `{account}` token is **encoded** (dots→underscores, e.g.
`weather.bot` → `weather_bot`), matching the token its NATS JWT is scoped to (the
same transform as `room.key` delivery).

Two shapes exist — discriminated by `action`:

### `added` / `role_updated` / `mute_toggled` / `favorite_toggled` / `opened` / `read` (SubscriptionUpdateEvent)

| Field | Type | Notes |
|---|---|---|
| `userId` | string | The affected user's internal user ID. Omitted on the org-removal path. |
| `subscription` | [Subscription](../client-api.md#subscription) | Full Subscription record for `added` / `role_updated` / `mute_toggled` / `favorite_toggled` / `section_moved` / `opened` / `read`. On `added` it additionally embeds a populated `room` object ([SubscriptionRoom](../client-api.md#subscriptionroom)) — `previewMessage` always omitted; `privateKey`/`keyVersion` present only for encrypted channel rooms — so clients can render the sidebar entry and store the room key from this single event. On `section_moved`, `sectionId` / `sectionOrder` carry the new placement (both absent = removed from its section). On `read`, `hasMention` and `hasGroupMention` are both `false` — reading the room clears both. |
| `action` | string | `"added"`, `"role_updated"`, `"mute_toggled"`, `"favorite_toggled"`, `"section_moved"`, `"opened"`, or `"read"`. |
| `roomName` | string | Per-subscriber display label. On `added`: channel name / DM counterpart's display name / bot app name. On `role_updated`: the channel name. Omitted on `mute_toggled` / `favorite_toggled` / `section_moved` / `opened` / `read`. |
| `hrInfo` | [CounterpartHRInfo](../client-api.md#counterparthrinfo) | `{account, chineseName, engName}` — the DM counterpart's HR record, so a newly created DM renders from this event alone. Sent on `added` `dm` / `botDM` when the counterpart account does **not** end in `.bot`; on a self-DM it carries the recipient's own record. Both name fields are `omitempty`. Omitted on `channel` / `discussion` rooms and on a lookup miss. |
| `appInfo` | [CounterpartAppInfo](../client-api.md#counterpartappinfo) | `{id, name, assistantName}` — the counterpart's app record, sent on `added` `botDM` when the counterpart account ends in `.bot`. `name` is empty when the app document has none, and `roomName` then falls back to the bot account. Mutually exclusive with `hrInfo`; omitted on a lookup miss. |
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
    "joinedAt": "2026-05-06T08:01:23Z",
    "room": {
      "siteId": "siteA",
      "name": "engineering-announcements",
      "crossSite": false,
      "userCount": 12,
      "appCount": 1,
      "lastMsgAt": "2026-05-06T07:59:01Z",
      "lastMsgId": "01970a4f8c2d7c9aM123",
      "lastMentionAllAt": "2026-05-05T11:00:00Z",
      "minUserLastSeenAt": "2026-05-04T09:30:00Z",
      "privateKey": "<base64-encoded 32-byte room secret>",
      "keyVersion": 0
    }
  },
  "action": "added",
  "roomName": "engineering-announcements",
  "timestamp": 1778054483000
}
```

A newly created **DM** carries the counterpart's `hrInfo`; a **botDM** carries `appInfo`
on the human member's copy (the bot's copy carries the human's `hrInfo`):

```json
{
  "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "subscription": {
    "id": "01970a4f8c2d7c9a01970a4f8c2d7c9c",
    "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice", "isBot": false },
    "roomId": "01970a4f8c2d7c9a01970a4f8c2d7c9b",
    "roomType": "dm",
    "siteId": "siteA",
    "roles": null,
    "name": "bob",
    "joinedAt": "2026-05-06T08:01:23Z",
    "room": { "siteId": "siteA", "crossSite": false, "userCount": 2 }
  },
  "action": "added",
  "roomName": "Bob Chan 陳大文",
  "hrInfo": { "account": "bob", "chineseName": "陳大文", "engName": "Bob Chan" },
  "timestamp": 1778054483000
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
  "timestamp": 1778054483000
}
```

**Triggered by:** Add Members (`added`), Remove Member (`removed`), Update Member Role
(`role_updated`), Toggle Mute (`mute_toggled`), Toggle Favorite (`favorite_toggled`),
Open Room (`opened`), Mark Messages Read (`read`) — see [request-reply.md](request-reply.md).

---

## settings.update — user settings sync

**Subject:** `chat.user.{account}.event.settings.update`

Published by user-service after every successful
[settings.set](request-reply.md#settingsset),
[settings.priorityContacts.add](request-reply.md#settingsprioritycontactsadd),
or [settings.priorityContacts.remove](request-reply.md#settingsprioritycontactsremove)
— ephemeral core-NATS fan-out to the caller's own connected devices, so other
logged-in clients sync live. Best-effort: a fan-out failure does not fail the
triggering call. `settings.priorityContacts.get` is a pure read and never
publishes this event. Exception: a `settings.priorityContacts.add` for a
contact already present AND the list already at the 30-entry cap skips the
publish too (the write misses, the re-read finds the contact already there,
and the call returns early); a duplicate add under the cap still publishes.

The payload carries the **full post-update settings** — receivers replace their
local copy, they never merge deltas. A field absent from `settings` was never
set by the user, and **absent means the client applies its own default** (the
server never injects defaults).

### Schema (SettingsUpdateEvent)

| Field | Type | Notes |
|---|---|---|
| `timestamp` | number | Publish time, Unix ms. |
| `settings` | UserSettings | Full post-update settings; all ten fields optional. |

UserSettings — every field optional, present only when explicitly set:

| Field | Type |
|---|---|
| `fullWidth` | boolean |
| `themePreference` | string (`system`\|`light`\|`dark`) |
| `translateMessageInto` | string |
| `messagePreviewEnabled` | boolean |
| `muteAllNotifications` | boolean |
| `alwaysAllowPriorityNotifications` | boolean |
| `showPreviewsInNotifications` | boolean |
| `showNotificationsInCall` | boolean |
| `initialChatScrollPosition` | string (`lastRead`\|`newest`) |
| `priorityContacts` | string[] |

`priorityContacts` here is the **raw list of contact accounts** (`[]string`),
not the enriched `PriorityContactItem[]` shape returned by
[settings.priorityContacts.get](request-reply.md#settingsprioritycontactsget).
This is deliberate: the fanout mirrors what's stored, not a display-ready
projection. A device that needs display names (engName/chineseName/app name)
re-issues `settings.priorityContacts.get` rather than reading them off this
event.

```json
{
  "timestamp": 1737000000000,
  "settings": { "fullWidth": false, "translateMessageInto": "ja", "muteAllNotifications": true }
}
```

---

## chatlist.update — chatlist section sync

**Subject:** `chat.user.{account}.event.chatlist.update`

Published by user-service after every successful chatlist section-definition
mutation ([Chatlist Sections](../client-api.md#chatlist-sections)) — ephemeral
core-NATS fan-out to the caller's own connected devices, the same delivery pattern
as settings.update. Best-effort. Note this carries only the section **definitions**,
never per-chat membership — a chat moving section fires a `subscription.update`
(`action: "section_moved"`) instead, so `chatlist.update` stays O(sections).

The payload carries the **full post-update state** — receivers replace their local
copy, they never merge deltas.

### Schema (ChatlistUpdateEvent)

| Field | Type | Notes |
|---|---|---|
| `timestamp` | number | Publish time, Unix ms (the state's high-water mark). |
| `chatlist` | [ChatlistState](../client-api.md#chatliststate) | Full post-update section definitions. |

```json
{
  "timestamp": 1737000000000,
  "chatlist": {
    "sectionOrder": ["favorites", "apps", "teams", "work", "chats"],
    "sections": [
      { "id": "favorites", "name": "Favorites", "builtIn": true, "sortMode": "mostRecent" },
      { "id": "work", "name": "Work", "builtIn": false, "sortMode": "custom" }
    ],
    "lastUpdatedAt": 1737000000000
  }
}
```

---

## room.key — room encryption key delivery

**Subject:** `chat.user.{account}.event.room.key`

Delivers the AES-256-GCM room key to channel members on **key rotation** (member remove), **bots included** — bots receive it on their **encoded** per-user subject (a dotted `.bot` account maps to a single NATS subject token, the form its JWT is scoped to). Bots also receive `subscription.update` on that same encoded subject (a bot can log into the chat frontend). The **initial** key no longer arrives on this subject: create and add deliver it inline on the `added` `subscription.update` (`subscription.room.privateKey` / `keyVersion`).
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

- **Remove Member (channel):** the key is rotated; every surviving member receives a new event with `version` incremented. The removed account stops receiving events.
- **Create Room / Add Members no longer fire this event.** The initial key rides the `added` `subscription.update` (`subscription.room.privateKey` / `keyVersion`), delivered to each newly-subscribed member — bots on their encoded per-user subject (see §5 in the canonical doc).

**Initial key bootstrap on (re)connect:** live events fire only on rotation.
The initial key set is delivered via `room.privateKey` / `room.keyVersion` on each
enriched [Subscription](../client-api.md#subscriptionroom) returned by
`subscription.list` / `subscription.getChannels` / etc., and on the `added`
`subscription.update` for rooms joined mid-session.

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

The live fan-out for a newly created non-thread message (plain send, quoted send, or
system message). Triggered by [Send Message](request-reply.md#send-message). Thread
replies publish [`new_thread_message`](#new_thread_message-roomevent) instead.

**botDM rooms fan out to the human member, not the bot** — `broadcast-worker` publishes the
`RoomEvent` to each non-bot member and skips the bot account (`isBot`); the bot side consumes
messages through a separate backend path.

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
| `type` | string | Optional. Message type — a server-set system type (`room_created`, etc.) or the client-settable `"important"`. Absent for a normal message. |
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

### new_thread_message (RoomEvent)

The live fan-out for a newly created thread reply. Same [RoomEvent](#new_message-roomevent)
shape as `new_message` (see field table + `ClientMessage` above) — only `type` differs.
Triggered by [Send Message](request-reply.md#send-message) when `threadParentMessageId`
is set. Thread edits/deletes still publish `message_edited` / `message_deleted`; only the
create event gets the distinct type.

**Delivery differs from `new_message`.** A channel thread reply is **not** published room-wide on
`chat.room.{roomID}.event`; it fans out **per-subscriber** on `chat.user.{account}.event.room` to the
reply sender, the parent-message author, thread followers (anyone who has replied in the thread), and
history-gated @-mentioned accounts. DM/botDM thread replies fan out **per member** on the same
`chat.user.{account}.event.room` subject — the bot account is skipped (`isBot`), same as an ordinary
`new_message` in a botDM.

A channel thread reply is **additionally** published on the [thread view subject](#thread-view-subject),
which serves clients that have the thread panel open without following the thread.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"new_thread_message"`. |

Channel example:

```json
{
  "type": "new_thread_message",
  "roomId": "01970a4f8c2d7c9aQ",
  "timestamp": 1746518100123,
  "eventTimestamp": 1746518100100,
  "roomName": "engineering-announcements",
  "roomType": "channel",
  "siteId": "siteA",
  "userCount": 12,
  "lastMsgAt": "2026-05-06T07:55:00Z",
  "lastMsgId": "01970a4f8c2d7c9aQRST",
  "message": {
    "id": "01970a4f8c2d7c9aQRST",
    "roomId": "01970a4f8c2d7c9aQ",
    "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
    "userAccount": "alice",
    "content": "replying in thread",
    "createdAt": "2026-05-06T07:55:00Z",
    "threadParentMessageId": "01970a4f8c2d7c9aPARENT",
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

### Thread view subject

**Subjects:** `chat.room.{roomID}.thread.{parentMessageID}.event`, or
`chat.local.room.{roomID}.thread.{parentMessageID}.event` when the room's `crossSite` is
explicitly `false`. Resolve the namespace exactly as for `chat.room.{roomID}.event`; a room
that has just flipped local→global publishes to both for the transition grace window.

Channel rooms only — DM and botDM thread replies already reach every member.

The per-subscriber fan-out above reaches only thread followers, so a client that opens a thread
panel without following the thread would see nothing until it refetched. `broadcast-worker`
publishes the same three events a second time here, and a client subscribes for exactly as long
as the panel is open:

| Type | Payload |
|---|---|
| `new_thread_message` | [RoomEvent](#new_thread_message-roomevent) |
| `message_edited` | [EditRoomEvent](#message_edited-editroomevent) |
| `message_deleted` | [DeleteRoomEvent](#message_deleted-deleteroomevent) |

**Encrypted here, plaintext on the per-subscriber lane.** In an encrypted channel the
per-subscriber copy carries a plaintext `message` / `newContent` because
`chat.user.{account}.event.room` is scoped to one account; this subject is in the room
namespace, so its copy carries `encryptedMessage` / `encryptedNewContent` — decrypt with the
room key as for `chat.room.{roomID}.event`. Unencrypted channels send identical plaintext on
both. `message_deleted` has no body and is never encrypted. If sealing fails nothing is
published here, rather than a plaintext body reaching the room namespace.

**Client handling.** Subscribe *before* calling `msg.thread`, or a reply published in the gap is
lost. Unsubscribe on panel close, on a switch to another parent, and on teardown.

A follower with the panel open receives every event twice, and the copies are identical: suppress a
`new_thread_message` whose ID is already rendered, but apply `message_edited` / `message_deleted`
unconditionally — deduplicating those by message ID drops a later edit of an already-seen reply,
and both are idempotent anyway.

Process one thread's events in arrival order. In an encrypted room a plaintext `message_deleted`
resolves faster than a preceding `new_thread_message` that must be decrypted first, so a
concurrent handler can apply the delete to a reply it has not inserted yet and then render that
reply as live.

Delivery here is best-effort and never retried — the panel's next open refetches, and the
per-subscriber lane is unaffected.

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
| `threadParentMessageId` | string | Optional. Set when the edited message is a thread reply — lets the client tell a thread-reply edit from a top-level one. Omitted for top-level messages. |
| `tshow` | boolean | Optional. For a thread reply, whether it is also shown in the main room timeline. Omitted when `false`. |
| `previewMessage` | [PreviewMessage](../client-api.md#previewmessage) | Optional. The room's current preview after this edit (same resolution as `subscription.list`). **Omitted** for hidden thread-reply edits (`threadParentMessageId` set with `tshow` not true), when the room has no eligible message, or on a read error. |

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
  "updatedAt": "2026-05-06T08:05:00Z",
  "previewMessage": {
    "messageId": "01970a4f8c2d7c9aQRST",
    "sender": { "account": "alice", "displayName": "Alice" },
    "content": "morning team — updated",
    "createdAt": "2026-05-06T08:00:00Z"
  }
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
| `threadParentMessageId` | string | Optional. Set when the deleted message is a thread reply — lets the client tell a thread-reply delete from a top-level one. Omitted for top-level messages. |
| `tshow` | boolean | Optional. For a thread reply, whether it is also shown in the main room timeline. Omitted when `false`. |
| `previewMessage` | [PreviewMessage](../client-api.md#previewmessage) | Optional. The room's current preview after this delete (same resolution as `subscription.list`). **Omitted** for hidden thread-reply deletes (`threadParentMessageId` set with `tshow` not true), when the room has no eligible message left (e.g. the deleted message was the last one), or on a read error. |

```json
{
  "type": "message_deleted",
  "roomId": "01970a4f8c2d7c9aQ",
  "siteId": "siteA",
  "timestamp": 1746518800123,
  "messageId": "01970a4f8c2d7c9aQRST",
  "deletedBy": "alice",
  "deletedAt": "2026-05-06T08:06:40Z",
  "updatedAt": "2026-05-06T08:06:40Z",
  "previewMessage": {
    "messageId": "01970a4f8c2d7c9aQPRE",
    "sender": { "account": "bob", "displayName": "Bob" },
    "content": "the previous message, now the newest",
    "createdAt": "2026-05-06T07:59:00Z"
  }
}
```

When the deleted message was the room's last eligible message, `previewMessage` is **omitted**.

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
| `newTcount` | number | Authoritative exact reply count for the parent message. Apply directly — do not delta. |
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
  "timestamp": 1778054483000,
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

This is a **state update, not a message**: apply it to the local subscription and render
nothing. No system message accompanies it, so a restriction change never appears in the room
timeline and never notifies.

**Subject:** `chat.room.{roomID}.event` — or `chat.local.room.{roomID}.event` for a
same-site room, depending on the deployment's room-subject routing mode (see
[client-api.md §Subscriptions](../client-api.md#2-nats-subjects)). Subscribe to whichever
subject you already use for that room's messages.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"room_restricted"`. |
| `roomId` | string | The room whose flags changed. |
| `siteId` | string | Home site of the room. |
| `timestamp` | number | Publish time (UTC ms). |
| `restricted` | boolean | The new restricted state. |
| `externalAccess` | boolean | The new external-access state. |
| `ownerAccount` | string | Optional. The account designated as sole owner by this call. Present on any restricting call that named one — including an owner rotation on an already-restricted room; omitted when none was sent. |
| `byAccount` | string | The admin who made the change. |
| `changedAt` | string | ISO-8601 timestamp of when the change was applied. |

```json
{
  "type": "room_restricted",
  "roomId": "01970a4f8c2d7c9aQ",
  "siteId": "siteA",
  "timestamp": 1778054483000,
  "restricted": true,
  "externalAccess": true,
  "ownerAccount": "alice",
  "byAccount": "p_admin",
  "changedAt": "2026-08-03T09:41:23Z"
}
```

---

## member — room membership events

**Subject:** `chat.room.{roomID}.event.member` — routed on the room's namespace by its
`crossSite` flag (like `chat.room.{roomID}.event`): `chat.local.room.{roomID}.event.member`
when `crossSite: false` (same-site), `chat.room.{roomID}.event.member` when `true`/unknown.

### member_added (MemberAddEvent)

Published once whenever the room's member list actually changes: a new account joins, a
genuinely new org is added, or an existing org member is upgraded to an individual membership
(see the no-op note below for what does **not** fire). Triggered by
[Add Members](request-reply.md#add-members) and indirectly by
[Create Room](request-reply.md#create-room).

The event carries no separate account list — member identities are in `members`. When new members
actually join (or a new org is added), a `members_added` system message also flows through the
message pipeline as a `new_message` room event; a pure org→individual upgrade posts no such message.

> **No-op:** when the request changes nothing — every requested account already subscribed, no org
> member upgraded to an individual membership, and every requested org already present — no
> `member_added` event fires. In particular, re-adding an already-present org is a no-op. An
> org→individual upgrade is **not** a no-op: `member_added` fires with that individual in `members`
> (no `members_added` system message, since no one newly joined).

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"member_added"`. |
| `roomId` | string | |
| `roomName` | string | |
| `roomType` | string | `"channel"`, `"dm"`, `"botDM"`, or `"discussion"`. Omitted when empty. |
| `members` | [RoomMemberEntry](../client-api.md#roommemberentry)[] | The requested entities in member.list display shape (the [RoomMemberEntry](../client-api.md#roommemberentry) payload only — no membership `id`/`rid`/`ts` envelope): one org entry per requested org first (`orgName`, `orgCode`, `memberCount`, `orgDescription`), then one individual entry per requested user that was newly subscribed **or** upgraded to an individual membership (`engName`, `chineseName`, `sectName`, `employeeId`). Unlike [List Members](request-reply.md#list-members) (`enrich: true`), individual entries here omit `isOwner` (new members are never owners) and `name` (bot display name). Accounts joined only via org expansion are **not** listed individually — they are represented by their org entry, mirroring `member.list`. |
| `siteId` | string | The room's home site. |
| `requesterAccount` | string | The account that initiated the add. Omitted when empty. |
| `joinedAt` | number | Epoch ms (UTC). |
| `historySharedSince` | number | Optional. Epoch ms (UTC); the new members' history boundary, present when their history is restricted. For `history.mode: "none"` adds it is the add time or the requester's own boundary, whichever is later; for a share-all add by a requester whose own history is capped it is the requester's inherited boundary. Absent = unrestricted. |
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
| `status` | string | Effective status: `"online"` / `"away"` / `"busy"` / `"offline"` / `"in-call"`. `in-call` is set by an external Teams presence-sync signal (suppresses notifications; not settable as a manual status). |
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
