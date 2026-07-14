# Distributed Multi-Site Chat System — Technical Specification

## 1. Overview

A distributed multi-site chat system where users send messages in rooms with real-time delivery, federated across independent sites. The system is built as event-driven microservices in Go, using NATS JetStream for async event processing and NATS request/reply for synchronous operations.

Each site runs independently with its own NATS, MongoDB, and Cassandra. Cross-site events use the Outbox/Inbox pattern.

---

## 2. Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Go 1.24 |
| Messaging | NATS + JetStream |
| Operational DB | MongoDB (rooms, subscriptions, messages) |
| History DB | Cassandra (message history / time-series) |
| Auth | NATS callout service with JWT + NKeys |
| Config | Environment variables via `caarlos0/env` |
| Observability | OpenTelemetry (tracing), Prometheus (metrics), `log/slog` (logging) |
| Testing | `go.uber.org/mock`, `stretchr/testify`, `testcontainers-go` |
| Containers | Docker multi-stage builds, Docker Compose |
| CI/CD | Azure Pipelines |

---

## 3. Architecture

### 3.1 Core Message Flow

```
Client A (sender)
    |
    |--- pub: chat.user.A.room.R1.siteX.msg.send
    |
    v
[MESSAGES stream] --> message-worker
                        |
                        | validate subscription, persist to MongoDB + Cassandra
                        | reply to sender, publish to FANOUT
                        v
                    [FANOUT stream]
                        |
                        +--> broadcast-worker
                        |      | look up room, publish RoomMetadataUpdateEvent
                        |      | group: publish to chat.room.{roomID}.stream.msg
                        |      | DM: publish to chat.user.{account}.stream.msg per member
                        |      v
                        |    Clients receive messages
                        |
                        +--> notification-worker
                               | look up members, exclude sender
                               | publish NotificationEvent to chat.user.{account}.notification
                               v
                             Desktop banner notifications
```

### 3.2 Multi-Site Federation

```
Site A                              Site B
  |                                   |
  | room-worker publishes             |
  | OutboxEvent to OUTBOX_siteA       |
  |                                   |
  | [OUTBOX_siteA] ---(sourcing)---> [INBOX_siteB]
  |                                   |
  |                              inbox-worker processes:
  |                                member_added -> create local subscription
  |                                room_sync -> upsert room metadata
```

### 3.3 User Login & NATS Connection (Auth Callout)

```
Client (browser/mobile)
    |
    | 1. Authenticate with SSO provider (OAuth/OIDC)
    |    -> receives SSO token
    |
    | 2. Connect to NATS with SSO token in ConnectOptions.Token
    |
    v
NATS Server
    |
    | 3. NATS triggers auth_callout for unauthenticated connection
    |
    v
auth-service (callout handler)
    |
    | 4. Extract token from AuthorizationRequest.ConnectOptions.Token
    | 5. Verify token with SSO provider (TokenVerifier.Verify)
    | 6. On success: build UserClaims JWT with scoped permissions
    |      Pub.Allow: chat.user.{account}.>, _INBOX.>
    |      Sub.Allow: chat.user.{account}.>, chat.room.>, _INBOX.>
    |      Expires: 2 hours
    | 7. Sign JWT with account signing key
    | 8. Return signed JWT to NATS server
    |
    v
NATS Server
    |
    | 9. NATS accepts connection with scoped permissions
    |
    v
Client (connected)
    |
    | 10. Subscribe to chat.user.{account}.> (personal wildcard)
    | 11. Subscribe to chat.room.{roomID}.* for each sidebar room
    | 12. Ready for real-time messaging
```

### 3.4 Room Invitation Flow

```
Client
  |--- req: member.invite
  v
room-service
  | validate inviter is owner
  | check room capacity
  | publish to ROOMS stream
  v
[ROOMS stream] --> room-worker
                     | create subscription
                     | increment room user count
                     | publish SubscriptionUpdateEvent to invitee
                     | publish RoomMetadataUpdateEvent to all members
                     | if cross-site: publish OutboxEvent
```

---

## 4. Domain Models

### 4.1 Core Entities

**User**
| Field | Type | JSON | BSON |
|-------|------|------|------|
| ID | string | `id` | `_id` |
| Name | string | `name` | `name` |
| SiteID | string | `siteId` | `siteId` |

**Room**
| Field | Type | JSON | BSON |
|-------|------|------|------|
| ID | string | `id` | `_id` |
| Name | string | `name` | `name` |
| Type | RoomType | `type` | `type` |
| SiteID | string | `siteId` | `siteId` |
| UserCount | int | `userCount` | `userCount` |
| CreatedAt | time.Time | `createdAt` | `createdAt` |
| UpdatedAt | time.Time | `updatedAt` | `updatedAt` |

Room types: `"group"`, `"dm"`

**Message**
| Field | Type | JSON |
|-------|------|------|
| ID | string | `id` |
| RoomID | string | `roomId` |
| UserID | string | `userId` |
| Content | string | `content` |
| CreatedAt | time.Time | `createdAt` |

**Subscription**
| Field | Type | JSON | BSON |
|-------|------|------|------|
| ID | string | `id` | `_id` |
| UserID | string | `userId` | `userId` |
| RoomID | string | `roomId` | `roomId` |
| SiteID | string | `siteId` | `siteId` |
| Role | Role | `role` | `role` |
| HistorySharedSince | time.Time | `historySharedSince` | `historySharedSince` |
| JoinedAt | time.Time | `joinedAt` | `joinedAt` |

Roles: `"owner"`, `"member"`

### 4.2 Event Types

**MessageEvent** — Published to FANOUT after message persistence
| Field | Type |
|-------|------|
| Message | Message |
| RoomID | string |
| SiteID | string |

**RoomMetadataUpdateEvent** — Published on room state changes
| Field | Type |
|-------|------|
| RoomID | string |
| Name | string |
| UserCount | int |
| LastMessageAt | time.Time |
| UpdatedAt | time.Time |

**SubscriptionUpdateEvent** — Published when user's room list changes
| Field | Type |
|-------|------|
| UserID | string |
| Subscription | Subscription |
| Action | string (`"added"` / `"removed"`) |

**NotificationEvent** — Published for desktop banner notifications
| Field | Type |
|-------|------|
| Type | string (`"new_message"`) |
| RoomID | string |
| Message | Message |

**OutboxEvent** — Published for cross-site federation
| Field | Type |
|-------|------|
| Type | string (`"member_added"` / `"room_sync"`) |
| SiteID | string |
| DestSiteID | string |
| Payload | []byte (JSON-encoded inner event) |

### 4.3 Request/Response Types

**CreateRoomRequest**: `name`, `type`, `createdBy`, `siteId`, `members` (optional)

**InviteMemberRequest**: `inviterId`, `inviteeId`, `roomId`, `siteId`

**SendMessageRequest**: `roomId`, `content`, `requestId`

**HistoryRequest**: `roomId`, `before` (cursor), `limit`

**HistoryResponse**: `messages` ([]Message), `hasMore` (bool)

**Error envelope** (every transport — NATS reply, HTTP, AsyncJobResult): owned by `pkg/errcode`; shape `{error, code, reason?, metadata?}`. See `docs/error-handling.md` and `docs/client-api.md` §6.

---

## 5. JetStream Streams

| Stream | Subject Pattern | Publisher | Consumer |
|--------|----------------|-----------|----------|
| `MESSAGES_{siteID}` | `chat.user.*.room.*.{siteID}.msg.>` | Client | message-worker |
| `FANOUT_{siteID}` | `fanout.{siteID}.>` | message-worker | broadcast-worker, notification-worker |
| `ROOMS_{siteID}` | `chat.user.*.request.room.*.{siteID}.member.>` | room-service | room-worker |
| `OUTBOX_{siteID}` | `outbox.{siteID}.>` | room-worker, broadcast-worker | Remote INBOX |
| `INBOX_{siteID}` | *(sourced from remote OUTBOX)* | Remote sites | inbox-worker |

**Deduplication**: message-worker sets `Nats-Msg-Id` header to message ID on FANOUT publish.

**Consumer naming**: Each service uses its own durable consumer name (e.g., `"broadcast-worker"`, `"notification-worker"`), allowing multiple services to independently consume from the same stream.

---

## 6. NATS Subject Naming

### 6.1 Client Subjects

**User wildcard** (always subscribed): `chat.user.{account}.>`

| Subject | Publisher | Purpose |
|---------|-----------|---------|
| `chat.user.{account}.stream.msg` | broadcast-worker | DM message delivery |
| `chat.user.{account}.notification` | notification-worker | Desktop banner notification |
| `chat.user.{account}.event.subscription.update` | room-worker, inbox-worker | Room added/removed |
| `chat.user.{account}.event.room.metadata.update` | room-worker | Room metadata changed |
| `chat.user.{account}.response.{requestID}` | various | Request/reply response |

**Per-room subjects** (subscribed per sidebar room):

| Subject | Publisher | Purpose |
|---------|-----------|---------|
| `chat.room.{roomID}.stream.msg` | broadcast-worker | Group room message delivery |
| `chat.room.{roomID}.event.metadata.update` | broadcast-worker | Room metadata updates |
| `chat.room.{roomID}.event.typing` | room-service (relay) | Typing indicators |

### 6.2 Client Publishes

All client publishes are under `chat.user.{account}.>`:

| Subject | Purpose |
|---------|---------|
| `chat.user.{account}.room.{roomID}.{siteID}.msg.send` | Send message |
| `chat.user.{account}.request.room.{roomID}.{siteID}.member.invite` | Invite member |
| `chat.user.{account}.room.{roomID}.typing` | Typing indicator |

### 6.3 Request/Reply Subjects

| Subject | Responder | Purpose |
|---------|-----------|---------|
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.history` | history-service | Fetch message history |
| `chat.user.{account}.request.rooms.create` | room-service | Create room |
| `chat.user.{account}.request.rooms.list` | room-service | List user's rooms |
| `chat.user.{account}.request.rooms.get.{roomID}` | room-service | Get room details |

### 6.4 Backend-Only Subjects

| Subject | Publisher | Consumer | Stream |
|---------|-----------|----------|--------|
| `fanout.{siteID}.{roomID}.{msgID}` | message-worker | broadcast-worker, notification-worker | FANOUT |
| `outbox.{siteID}.to.{destSiteID}.{eventType}` | room-worker | Remote INBOX | OUTBOX |

---

## 7. Services

### 7.1 Auth Service (`auth-service/`)

**Purpose**: NATS auth_callout service. Verifies SSO tokens and issues scoped NATS user JWTs.

**Flow**: Client connects to NATS with SSO token -> auth-service verifies token -> issues JWT with user-scoped pub/sub permissions.

**JWT Permissions**:
| Type | Pattern |
|------|---------|
| Pub.Allow | `chat.user.{account}.>`, `_INBOX.>` |
| Sub.Allow | `chat.user.{account}.>`, `chat.room.>`, `_INBOX.>` |

**Dependencies**: NATS
**Key Interface**: `TokenVerifier` — `Verify(token string) (account string, err error)`
**Config**: `NATS_URL`, `NATS_CREDS`, `AUTH_SCOPED_SIGNING_KEY`, `AUTH_ACCOUNT_PUB_KEY` (required)

### 7.2 Message Worker (`message-worker/`)

**Purpose**: JetStream consumer that processes incoming messages from `MESSAGES_{siteID}`.

**Flow**:
1. Consume message from MESSAGES stream
2. Parse userID, roomID, siteID from subject
3. Validate sender has subscription to room
4. Generate UUID, persist to Cassandra (time-series) + update room in MongoDB
5. Reply to sender via `chat.user.{account}.response.{requestID}`
6. Publish `MessageEvent` to `fanout.{siteID}.{roomID}.{msgID}`

**Dependencies**: NATS + JetStream, MongoDB, Cassandra
**Key Interface**: `MessageStore` — `GetSubscription`, `SaveMessage`, `UpdateRoomLastMessage`
**Consumer**: Durable `"message-worker"` on `MESSAGES_{siteID}`
**Config**: `NATS_URL`, `SITE_ID`, `MONGO_URI`, `MONGO_DB`, `CASSANDRA_HOSTS`, `CASSANDRA_KEYSPACE`

### 7.3 Broadcast Worker (`broadcast-worker/`)

**Purpose**: Consumes `MessageEvent` from `FANOUT_{siteID}` and distributes messages to room/user streams.

**Flow**:
1. Unmarshal `MessageEvent`
2. Look up room in MongoDB
3. Publish `RoomMetadataUpdateEvent` to `chat.room.{roomID}.event.metadata.update`
4. **Group room**: Publish message to `chat.room.{roomID}.stream.msg`
5. **DM room**: Look up the other member (exclude sender), publish to `chat.user.{account}.stream.msg`

**Dependencies**: NATS + JetStream, MongoDB
**Key Interface**: `RoomLookup` — `GetRoom`, `ListSubscriptions`
**Consumer**: Durable `"broadcast-worker"` on `FANOUT_{siteID}`
**Design**: Metadata always published before message fan-out.

### 7.4 Notification Worker (`notification-worker/`)

**Purpose**: Consumes `MessageEvent` from `FANOUT_{siteID}` and sends `NotificationEvent` to each room member except the sender.

**Flow**:
1. Unmarshal `MessageEvent`
2. Look up room members via `ListSubscriptions`
3. For each member where `userID != sender`: publish `NotificationEvent` to `chat.user.{account}.notification`

**Dependencies**: NATS + JetStream, MongoDB
**Key Interface**: `MemberLookup` — `ListSubscriptions`
**Consumer**: Durable `"notification-worker"` on `FANOUT_{siteID}` (independent from broadcast-worker)
**Design**: Sender exclusion. Partial failure tolerance: continues notifying remaining members on individual publish failure.

### 7.5 Room Service (`room-service/`)

**Purpose**: Handles room CRUD via NATS request/reply and authorizes member invitations.

**Room CRUD**:
- **Create**: Generate room, auto-create owner subscription, return room
- **List**: Return all rooms
- **Get**: Return room by ID

**Invite Authorization**:
1. Verify inviter has `owner` role on room
2. Check room is below `maxRoomSize`
3. Publish approved invite to `ROOMS` JetStream stream

**Dependencies**: NATS + JetStream, MongoDB
**Key Interface**: `RoomStore` — `CreateRoom`, `GetRoom`, `ListRooms`, `GetSubscription`, `CreateSubscription`
**Config**: `NATS_URL`, `SITE_ID`, `MONGO_URI`, `MONGO_DB`, `MAX_ROOM_SIZE` (default 1000)

### 7.6 Room Worker (`room-worker/`)

**Purpose**: Processes approved member invitations from `ROOMS_{siteID}` JetStream stream.

**Flow**:
1. Unmarshal `InviteMemberRequest`
2. Create `Subscription` document (role: member)
3. Increment room user count
4. If cross-site invite: publish `OutboxEvent` (type: `member_added`)
5. Publish `SubscriptionUpdateEvent` to invitee
6. Publish `RoomMetadataUpdateEvent` to all existing room members

**Dependencies**: NATS + JetStream, MongoDB
**Key Interface**: `SubscriptionStore` — `CreateSubscription`, `ListByRoom`, `IncrementUserCount`, `GetRoom`
**Consumer**: Durable `"room-worker"` on `ROOMS_{siteID}`

### 7.7 Inbox Worker (`inbox-worker/`)

**Purpose**: Consumes cross-site `OutboxEvent` messages from `INBOX_{siteID}` and processes them locally.

**Event Handlers**:
- **`member_added`**: Unmarshal `InviteMemberRequest` from payload, create local `Subscription`, publish `SubscriptionUpdateEvent`
- **`room_sync`**: Unmarshal `Room` from payload, upsert room metadata in MongoDB
- **Unknown types**: Log and skip (ack without error to prevent infinite redelivery)

**Dependencies**: NATS + JetStream, MongoDB
**Key Interface**: `InboxStore` — `CreateSubscription`, `UpsertRoom`
**Consumer**: Durable `"inbox-worker"` on `INBOX_{siteID}`

### 7.8 History Service (`history-service/`)

**Purpose**: NATS request/reply service for paginated message history.

**Flow**:
1. Parse userID, roomID from subject
2. Verify user has subscription to room (MongoDB)
3. Use `historySharedSince` from subscription as lower bound
4. Query Cassandra for messages (descending by `createdAt`, with limit)
5. Fetch `limit+1` to determine `hasMore`
6. Return `HistoryResponse`

**Dependencies**: NATS, MongoDB (subscriptions), Cassandra (messages)
**Key Interface**: `HistoryStore` — `GetSubscription`, `ListMessages`
**Config**: `NATS_URL`, `SITE_ID`, `MONGO_URI`, `MONGO_DB`, `CASSANDRA_HOSTS`, `CASSANDRA_KEYSPACE`

---

## 8. Shared Packages (`pkg/`)

| Package | Purpose |
|---------|---------|
| `pkg/model` | All domain structs with `json` + `bson` tags |
| `pkg/subject` | NATS subject builder functions and wildcard patterns |
| `pkg/natsutil` | `ReplyJSON`, `MarshalResponse`, `HeaderCarrier` (OTel) — success-reply mechanics only |
| `pkg/errcode` | `Code`/`Reason` types, `Error` (the wire envelope, leak-safe), named constructors (`BadRequest`, `NotFound`, …), `Classify` boundary, `Parse` for remote replies. Adapters: `errnats.Reply` (NATS) and `errhttp.Write` (Gin). Test helper: `errtest.AssertCode`/`AssertReason`. See `docs/error-handling.md`. |
| `pkg/stream` | JetStream `StreamConfig` builders for all 5 streams |
| `pkg/mongoutil` | `Connect`, `Disconnect` wrappers |
| `pkg/cassutil` | `Connect`, `Close` wrappers (LocalQuorum, 10s timeout) |
| `pkg/otelutil` | `InitTracer` (OTLP gRPC), `InitMeter` (Prometheus) |
| `pkg/shutdown` | `Wait` — signal-based graceful shutdown with timeout |

---

## 9. Data Storage

### 9.1 MongoDB Collections

| Collection | Primary Key | Purpose | Used By |
|------------|-------------|---------|---------|
| `rooms` | `_id` (UUID) | Room metadata | room-service, room-worker, broadcast-worker, inbox-worker |
| `subscriptions` | `_id` (UUID) | User-room membership | message-worker, broadcast-worker, notification-worker, room-service, room-worker, inbox-worker, history-service |

**Subscription indexes**: `{userId, roomId}` (unique), `{roomId}` (for member lookups)

### 9.2 Cassandra Tables

| Table | Partition Key | Clustering Key | Purpose |
|-------|--------------|----------------|---------|
| `messages` | `room_id` | `created_at DESC` | Time-series message history |

**Schema**:
```cql
CREATE TABLE messages (
    room_id text,
    created_at timestamp,
    id text,
    user_id text,
    content text,
    PRIMARY KEY (room_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
```

---

## 10. Client Behavior

### 10.1 Connection & Subscriptions

1. Connect to NATS with SSO token (auth-service issues JWT)
2. Subscribe to `chat.user.{account}.>` (personal wildcard)
3. For each sidebar room: subscribe to `chat.room.{roomID}.stream.msg` and `chat.room.{roomID}.event.metadata.update`
4. For active room only: subscribe to `chat.room.{roomID}.event.typing`

### 10.2 Badge Model

**Unread badge** (bold room name): Client compares `lastMessageAt` from `RoomMetadataUpdateEvent` against locally cached `lastSeenAt`.

**Mention badge** (`@`):
- **Online**: Client checks `mentionedUserIDs` field in received messages
- **Reconnect**: Server tracks `mentionCountSinceLastSeen` per user-per-room on `Subscription`; returned in room list response

### 10.3 Reconnect Flow

1. Re-subscribe to `chat.user.{account}.>`
2. Request room list (includes `lastSeenAt`, `mentionCountSinceLastSeen`)
3. Restore badges
4. Re-subscribe to room subjects
5. Fetch message history for active room
6. Resume real-time + client-side mention detection

---

## 11. Auth Permissions

The auth-service issues per-user NATS JWTs:

| Type | Pattern | Rationale |
|------|---------|-----------|
| Pub.Allow | `chat.user.{account}.>` | All client publishes under own namespace |
| Pub.Allow | `_INBOX.>` | NATS request/reply pattern |
| Sub.Allow | `chat.user.{account}.>` | Personal events, responses, notifications |
| Sub.Allow | `chat.room.>` | Room message streams and events |
| Sub.Allow | `_INBOX.>` | NATS request/reply pattern |

Room-scoped subjects (`chat.room.*`) are server-published only — clients subscribe but never publish.

---

## 12. Graceful Shutdown

All services use `pkg/shutdown.Wait` with a 25-second timeout (below Kubernetes' 30-second `terminationGracePeriodSeconds`).

**JetStream consumers**: `cctx.Drain()` (processes buffered messages) -> wait on `cctx.Closed()` channel -> `nc.Drain()` -> disconnect databases.

**Non-consumer services**: `nc.Drain()` -> disconnect databases.

---

## 13. Infrastructure

### 13.1 Docker

- Multi-stage builds: `golang:1.24-alpine` builder, `alpine:3.21` runtime
- Build context: repo root (for `pkg/` and `go.mod` access)
- Per-service Dockerfiles at `<service>/Dockerfile`

### 13.2 CI/CD (Azure Pipelines)

**Validate stage** (all branches + PRs):
1. `golangci-lint run ./...` (includes `go vet`, `staticcheck`, `errcheck`, `goimports`, etc.)
2. Test shared packages with coverage
3. Test all services with coverage
4. Build all services

**Build stage** (main branch only):
- Matrix build of all 8 service Docker images
- Push to container registry with `BuildId` + `latest` tags

### 13.3 Local Development

Per-service `docker-compose.yml` files in `build/<service>/` include only required dependencies:
- All services: NATS (with JetStream + HTTP monitoring)
- Most services: MongoDB
- message-worker, history-service: MongoDB + Cassandra

---

## 14. Key Design Decisions

1. **Event-driven over synchronous**: Services communicate via JetStream streams, enabling independent scaling and fault isolation. Only CRUD operations use request/reply.

2. **Interface-driven testing**: Every service defines consumer-side interfaces (`MessageStore`, `RoomLookup`, `MemberLookup`, etc.) with in-memory implementations for unit testing. No real databases in unit tests.

3. **Publisher injection**: All handlers accept a `publish` function or `Publisher` interface rather than depending on `*nats.Conn` directly, making handlers fully testable without NATS.

4. **Dual-consumer FANOUT**: Both broadcast-worker and notification-worker consume from the same `FANOUT_{siteID}` stream via separate durable consumers, ensuring each independently processes every message.

5. **Room-type routing**: Group rooms use a shared room stream (`chat.room.{roomID}.stream.msg`). DM rooms fan out to individual user streams (`chat.user.{account}.stream.msg`), since DM participants subscribe to their personal stream.

6. **Metadata-first publishing**: `RoomMetadataUpdateEvent` is published before message fan-out, ensuring clients see updated metadata before or alongside new messages.

7. **Partial failure tolerance**: DM fan-out and notification delivery continue to remaining members when individual publishes fail.

8. **HistorySharedSince**: Users only see messages from after they joined a room. The history-service uses subscription's `historySharedSince` as the lower bound for queries.

9. **Cross-site Outbox/Inbox**: Local events go to `OUTBOX_{siteID}`, remote sites source them into their `INBOX_{siteID}` via JetStream cross-account sourcing. The inbox-worker processes them locally.

10. **Client namespace isolation**: All client publishes are under `chat.user.{account}.>`. Room-scoped subjects are server-published only. This simplifies auth permissions to a single wildcard per user.
