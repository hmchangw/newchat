# NATS Subject Naming Design

> Looking for the **client integrator reference**? See [`client-api.md`](./client-api.md). This document is the authoritative subject-naming spec used by backend developers.

## Overview

This document defines the complete NATS subject naming scheme for the chat system. Each site runs its own independent NATS server — subjects are scoped per-site at the infrastructure level, not by embedding `siteID` at a fixed position. Where a subject must indicate a room's home site (e.g., message sends, invite requests), `{siteID}` appears after `{roomID}`. Clients (web & mobile) connect to NATS core subjects only — no JetStream consumers on the client side. On reconnect, clients use request/reply to catch up on missed data (message history, subscription lists, etc.).

## Subject Hierarchy

All subjects are dot-delimited and organized into four namespaces:

| Prefix | Scope | Description |
|--------|-------|-------------|
| `chat.user.{account}.*` | Per-user | Events, streams, and requests scoped to a single user |
| `chat.room.{roomID}.*` | Per-room | Events and streams scoped to a single room |
| `fanout.{siteID}.*` | Backend | Internal message fan-out (JetStream only) |
| `outbox.{siteID}.*` | Backend | Cross-site federation (JetStream only) |

## Client Subscription Model

### 1. User Wildcard (always subscribed)

On connect, every client subscribes to `chat.user.{account}.>`. This single wildcard captures all personal events:

| Subject | Direction | Publisher | Purpose |
|---------|-----------|-----------|---------|
| `chat.user.{account}.stream.msg` | Server → Client | broadcast-worker | DM message delivery |
| `chat.user.{account}.notification` | Server → Client | _(removed — see PUSH_NOTIFICATIONS stream below)_ | _(deprecated)_ |
| `chat.user.{account}.event.subscription.update` | Server → Client | room-worker, inbox-worker | Room added/removed from user's list |
| `chat.user.{account}.event.room.metadata.update` | Server → Client | room-worker | Room metadata changed (for rooms in sidebar) |
| `chat.user.{account}.response.{requestID}` | Server → Client | various services | Response to a client request |

### 2. Per-Room Subjects (subscribed for each room in sidebar)

For each room displayed in the client's sidebar, the client subscribes to:

| Subject | Direction | Publisher | Purpose |
|---------|-----------|-----------|---------|
| `chat.room.{roomID}.stream.msg` | Server → Client | broadcast-worker | Group room message delivery |
| `chat.room.{roomID}.event.metadata.update` | Server → Client | broadcast-worker | Room metadata: name, user count, lastMessageAt |
| `chat.room.{roomID}.event.typing` | Server → Client | room-service (relay) | Typing indicators for the active room |

Clients subscribe to `chat.room.{roomID}.event.typing` only for the **currently opened room** (not all sidebar rooms) to minimize traffic. When the user switches rooms, the client unsubscribes from the old room's typing subject and subscribes to the new one.

### 3. Per-User Presence (subscribed per visible user)

For each user visible in the UI (room member list, DM list, etc.), the client subscribes to:

| Subject | Direction | Publisher | Purpose |
|---------|-----------|-----------|---------|
| `chat.user.{account}.event.presence` | Server → Client | Presence service (future) / Client heartbeat | Online/offline/away status |

Clients dynamically subscribe/unsubscribe to presence subjects as users appear/disappear from the viewport.

### 4. Sidebar Badge Model

Badges use two separate mechanisms: **unread** badges derive from room metadata events, while **mention** badges derive from the message stream itself.

#### Unread Badge (Bold Room Name)

broadcast-worker publishes `RoomMetadataUpdateEvent` to `chat.room.{roomID}.event.metadata.update` containing `lastMessageAt`. The client compares this against the user's locally cached `lastSeenAt` timestamp for each room.

| Badge | Source | Client Logic |
|-------|--------|-------------|
| **Bold room name** (unread) | `lastMessageAt` in `RoomMetadataUpdateEvent` | `lastMessageAt > lastSeenAt` |

#### Mention Badge (`@`) — Hybrid: Client-Derived Online + Server-Tracked Reconnect

Mention badges use a hybrid approach: client-side detection while online, server-tracked state for reconnect.

**While online (client-derived):**

Clients already subscribe to `chat.room.{roomID}.stream.msg` for every sidebar room and `chat.user.{account}.stream.msg` for DMs. When a message arrives, the client checks the `mentionedUserIDs` field in the `Message` payload:

1. If the logged-in user's ID is in `mentionedUserIDs` → show `@` badge
2. If `"all"` or `"here"` is in `mentionedUserIDs` → show `@` badge
3. Otherwise → no mention badge for this message

The client maintains a local per-room `hasMention` flag. It is set when a matching mention arrives and cleared when the user opens the room (advancing `lastSeenAt`).

This requires zero additional subjects, zero extra publishes, and zero write amplification — the data is already in the message payload the client receives.

**On reconnect (server-tracked):**

When offline, clients miss messages on non-active sidebar rooms. To restore mention badge state on reconnect, the server tracks mention counts per-user-per-room:

1. **broadcast-worker** — when processing a message with non-empty `mentionedUserIDs`, atomically increments `mentionCountSinceLastSeen` on the `Subscription` record in MongoDB for each mentioned user (expanding `"all"`/`"here"` to the full member list)
2. **Subscription list response** (`chat.user.{account}.request.rooms.list`) — includes `mentionCountSinceLastSeen` per room, allowing the client to restore `@` badges without fetching message history for every sidebar room
3. **Mark as read** — when the user opens a room, the client sends a read-position update (advancing `lastSeenAt`); the server resets `mentionCountSinceLastSeen` to `0` for that user+room

#### Desktop Banner Notifications (Mobile Push)

notification-worker publishes a `PushNotificationEvent` to `chat.server.notification.push.{siteID}.send` (captured by the `PUSH_NOTIFICATIONS_{siteID}` JetStream stream) for each eligible recipient. The push service consumes this stream and delivers the notification to the recipient's mobile device. The old per-user NATS core subject `chat.user.{account}.notification` is no longer used.

#### Reconnect Badge Restoration

On reconnect:
1. Client fetches subscription list via `chat.user.{account}.request.rooms.list` — response includes `lastSeenAt` and `mentionCountSinceLastSeen` per room
2. For each sidebar room: if `mentionCountSinceLastSeen > 0`, show `@` badge
3. Client re-subscribes to room streams and resumes client-side mention detection
4. For the active room, client fetches message history and can highlight individual mentioned messages using `mentionedUserIDs` in each message

### 5. Client Publishes

Clients publish exclusively to subjects under their own `chat.user.{account}.>` namespace — no exceptions:

| Subject | Direction | Consumer | Purpose |
|---------|-----------|----------|---------|
| `chat.user.{account}.room.{roomID}.{siteID}.msg.send` | Client → Server | message-worker (MESSAGES stream) | Send a message to a room |
| `chat.user.{account}.request.room.{roomID}.{siteID}.member.invite` | Client → Server | room-service (validates), room-worker (ROOMS stream) | Invite a member to a room |
| `chat.user.{account}.room.{roomID}.typing` | Client → Server | room-service (relay) | Typing indicator |

#### Typing Indicator Flow

Clients never publish directly to room-scoped subjects. For typing indicators:

1. Client publishes to `chat.user.{account}.room.{roomID}.typing` (user-scoped)
2. room-service subscribes to `chat.user.*.room.*.typing` and relays to `chat.room.{roomID}.event.typing`
3. Clients with that room actively opened receive the typing event via their room subscription

This keeps all client publishes under the user namespace, simplifying auth permissions and maintaining consistent security boundaries. Room-scoped subjects (`chat.room.*`) are server-published only — clients can subscribe but never publish to them.

## Request/Reply Subjects

All request subjects fall under the user's publish namespace. Responses are delivered to `chat.user.{account}.response.{requestID}`, which the client receives via its user wildcard subscription:

| Subject | Responder (Queue Group) | Purpose |
|---------|------------------------|---------|
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.history` | history-service | Fetch message history for a room |
| `chat.user.{account}.request.room.{roomID}.{siteID}.member.invite` | room-service | Invite a member to a room |
| `chat.user.{account}.request.rooms.create` | room-service | Create a new room |
| `chat.user.{account}.request.rooms.list` | room-service | List user's rooms |
| `chat.user.{account}.request.rooms.get.{roomID}` | room-service | Get room details by ID |

### Reconnect Flow

1. Client detects reconnect event from NATS connection
2. Client subscribes to `chat.user.{account}.>` (user wildcard)
3. Client calls `chat.user.{account}.request.rooms.list` to get current room list — response includes `lastSeenAt` and `mentionCountSinceLastSeen` per room
4. Client restores badges: bold room name if `lastMessageAt > lastSeenAt`; `@` badge if `mentionCountSinceLastSeen > 0`
5. Client subscribes to room subjects for each sidebar room
6. For the **currently active room**, client calls `chat.user.{account}.request.room.{roomID}.{siteID}.msg.history` with the last known message timestamp to fetch missed messages
7. Client resumes receiving real-time events and client-side mention detection

## Backend-Only Subjects (JetStream)

These subjects are used exclusively by backend services via JetStream. Clients never interact with them.

### MESSAGES Stream (`MESSAGES_{siteID}`)

| Subject Pattern | Publisher | Consumer | Purpose |
|-----------------|-----------|----------|---------|
| `chat.user.{account}.room.{roomID}.{siteID}.msg.send` | Client | message-worker | User message submissions |

Stream wildcard: `chat.user.*.room.*.{siteID}.msg.>`

### FANOUT Stream (`FANOUT_{siteID}`)

| Subject Pattern | Publisher | Consumer | Purpose |
|-----------------|-----------|----------|---------|
| `fanout.{siteID}.{roomID}` | message-worker | broadcast-worker, notification-worker | Stored message ready for delivery |

Stream wildcard: `fanout.{siteID}.>`

Deduplication: message-worker sets the `Nats-Msg-Id` header to the message ID on each publish. JetStream uses this for server-side dedup, keeping `msgID` out of the subject and bounding subject cardinality to the number of rooms rather than the number of messages.

### ROOMS Stream (`ROOMS_{siteID}`)

| Subject Pattern | Publisher | Consumer | Purpose |
|-----------------|-----------|----------|---------|
| `chat.user.{account}.request.room.{roomID}.{siteID}.member.invite` | room-service | room-worker | Member invitation (after authorization) |

Stream wildcard: `chat.user.*.request.room.*.{siteID}.member.>`

### OUTBOX Stream (`OUTBOX_{siteID}`)

| Subject Pattern | Publisher | Consumer | Purpose |
|-----------------|-----------|----------|---------|
| `outbox.{siteID}.to.{destSiteID}.{eventType}` | room-worker, broadcast-worker | Remote site's INBOX | Cross-site outbound events |

Stream wildcard: `outbox.{siteID}.>`

### PUSH_NOTIFICATIONS Stream (`PUSH_NOTIFICATIONS_{siteID}`)

| Subject Pattern | Publisher | Consumer | Purpose |
|-----------------|-----------|----------|---------|
| `chat.server.notification.push.{siteID}.send` | notification-worker | push service | Per-recipient mobile push event |

Stream wildcard: `chat.server.notification.push.{siteID}.>` (wildcard accommodates future `.silent`, `.priority` siblings)

This is a server-only, backend stream. Clients never interact with it.

### INBOX Stream (`INBOX_{siteID}`)

Sourced from remote sites' OUTBOX streams. Processed by `inbox-worker`.

## Auth Permissions (NATS JWT)

The auth-service issues per-user JWTs with these permissions:

| Type | Pattern | Rationale |
|------|---------|-----------|
| Pub.Allow | `chat.user.{account}.>` | User can publish messages, requests, typing under own namespace |
| Pub.Allow | `_INBOX.>` | Required for NATS core request/reply pattern |
| Sub.Allow | `chat.user.{account}.>` | User receives own events, responses, notifications |
| Sub.Allow | `chat.room.>` | User can subscribe to any room's message stream and events |
| Sub.Allow | `_INBOX.>` | Required for NATS core request/reply pattern |

All client publishes — message sends, member invites, room CRUD requests, typing indicators, and request/reply — fall under `chat.user.{account}.>`. No additional publish permissions are needed. Room-scoped subjects (`chat.room.*`) are server-published only; clients can subscribe but never publish to them.

## Subject Builders (`pkg/subject`)

### Specific Subject Builders

| Function | Subject |
|----------|---------|
| `MsgSend(account, roomID, siteID)` | `chat.user.{account}.room.{roomID}.{siteID}.msg.send` |
| `UserResponse(account, requestID)` | `chat.user.{account}.response.{requestID}` |
| `RoomMetadataUpdate(roomID)` | `chat.room.{roomID}.event.metadata.update` |
| `RoomMsgStream(roomID)` | `chat.room.{roomID}.stream.msg` |
| `UserRoomUpdate(account)` | `chat.user.{account}.event.room.update` |
| `UserMsgStream(account)` | `chat.user.{account}.stream.msg` |
| `MemberInvite(account, roomID, siteID)` | `chat.user.{account}.request.room.{roomID}.{siteID}.member.invite` |
| `MsgHistory(account, roomID, siteID)` | `chat.user.{account}.request.room.{roomID}.{siteID}.msg.history` |
| `SubscriptionUpdate(account)` | `chat.user.{account}.event.subscription.update` |
| `RoomMetadataChanged(account)` | `chat.user.{account}.event.room.metadata.update` |
| `Notification(account)` | `chat.user.{account}.notification` _(deprecated; use `PushNotification(siteID)` for mobile push)_ |
| `RoomsCreate(account)` | `chat.user.{account}.request.rooms.create` |
| `RoomsList(account)` | `chat.user.{account}.request.rooms.list` |
| `RoomsGet(account, roomID)` | `chat.user.{account}.request.rooms.get.{roomID}` |
| `Outbox(siteID, destSiteID, eventType)` | `outbox.{siteID}.to.{destSiteID}.{eventType}` |
| `Fanout(siteID, roomID)` | `fanout.{siteID}.{roomID}` |

### New Builders (to be added)

| Function | Subject | Purpose |
|----------|---------|---------|
| `UserTyping(account, roomID)` | `chat.user.{account}.room.{roomID}.typing` | Client publishes typing indicator |
| `RoomTyping(roomID)` | `chat.room.{roomID}.event.typing` | room-service relays typing to room |
| `UserPresence(account)` | `chat.user.{account}.event.presence` | User presence status |
| `UserWildcard(account)` | `chat.user.{account}.>` | Client subscribes to all personal events |

### Wildcard Patterns (Service Subscriptions)

| Function | Pattern | Used By |
|----------|---------|---------|
| `MsgSendWildcard(siteID)` | `chat.user.*.room.*.{siteID}.msg.send` | MESSAGES stream |
| `MemberInviteWildcard(siteID)` | `chat.user.*.request.room.*.{siteID}.member.>` | ROOMS stream, room-service |
| `MsgHistoryWildcard(siteID)` | `chat.user.*.request.room.*.{siteID}.msg.history` | history-service |
| `RoomsCreateWildcard()` | `chat.user.*.request.rooms.create` | room-service |
| `RoomsListWildcard()` | `chat.user.*.request.rooms.list` | room-service |
| `RoomsGetWildcard()` | `chat.user.*.request.rooms.get.*` | room-service |
| `FanoutWildcard(siteID)` | `fanout.{siteID}.>` | FANOUT stream |
| `OutboxWildcard(siteID)` | `outbox.{siteID}.>` | OUTBOX stream |

## Visual: Complete Message Flow

```
Client A (sender)                    NATS                         Client B (receiver)
    |                                  |                               |
    |--- pub: chat.user.A             |                               |
    |        .room.R1.siteX           |                               |
    |        .msg.send -------------->|                               |
    |                                  |                               |
    |                          [MESSAGES stream]                       |
    |                                  |                               |
    |                          message-worker                          |
    |                    (store msg + resolve mentions                  |
    |                     + publish to fanout)                          |
    |                                  |                               |
    |                          [FANOUT stream]                         |
    |                                  |                               |
    |                         broadcast-worker                         |
    |                                  |                               |
    |<-- sub: chat.user.A             |--- pub: chat.room.R1           |
    |        .response.{reqID} -------|        .stream.msg ---------->|
    |                                  |                               |
    |                                  |--- pub: chat.room.R1          |
    |                                  |        .event.metadata        |
    |                                  |        .update --------------->|
    |                                  |   (lastMessageAt)             |
    |                                  |                               |
    |                        notification-worker                       |
    |                                  |                               |
    |                                  |--- pub: chat.server.          |
    |                                  |    notification.push.         |
    |                                  |    {siteID}.send              |
    |                                  |   (PUSH_NOTIFICATIONS stream) |
    |                                  |                               |
    |--- pub: chat.user.A             |                               |
    |        .room.R1.typing -------->|                               |
    |                                  |                               |
    |                          room-service (relay)                    |
    |                                  |                               |
    |                                  |--- pub: chat.room.R1          |
    |                                  |        .event.typing -------->|
```

## Visual: Client Reconnect Flow

```
Client                              NATS                        Services
  |                                   |                             |
  |-- (reconnect detected) --------->|                             |
  |                                   |                             |
  |-- sub: chat.user.{account}.> ---->|                             |
  |                                   |                             |
  |-- req: chat.user.{account}        |                             |
  |    .request.rooms.list --------->|-----> room-service          |
  |<-- resp: [rooms + lastSeenAt    -|<-----                       |
  |    + mentionCountSinceLastSeen]  |                             |
  |                                   |                             |
  |-- (restore badges:               |                             |
  |    bold if lastMessageAt >       |                             |
  |    lastSeenAt; @ badge if        |                             |
  |    mentionCount > 0)             |                             |
  |                                   |                             |
  |-- sub: chat.room.R1             |                             |
  |        .stream.msg ------------->|                             |
  |-- sub: chat.room.R1             |                             |
  |        .event.* ---------------->|                             |
  |-- sub: chat.room.R2             |                             |
  |        .stream.msg ------------->|                             |
  |-- sub: chat.room.R2             |                             |
  |        .event.* ---------------->|                             |
  |                                   |                             |
  |-- req: msg.history (active room)->|-----> history-service       |
  |<-- resp: [missed messages] ------|<-----                       |
  |                                   |                             |
  |-- (resume real-time + mention -->|                             |
  |    detection from msg stream)    |                             |
```
