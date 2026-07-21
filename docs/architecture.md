# System Architecture

A distributed, **multi-site federated** chat system written in Go 1.25. Each
site runs independently with its own NATS, MongoDB, and Cassandra. Real-time
delivery and async processing flow through **NATS JetStream**; synchronous
operations use **NATS request/reply**. Cross-site events use the
**Outbox/Inbox** pattern.

This document is the architectural map. For wire-level contracts see
[`client-api.md`](./client-api.md); for the full subject hierarchy see
[`nats-subject-naming.md`](./nats-subject-naming.md).

---

## 1. Multi-Site Federation (Context)

Each site is a self-contained deployment. Clients connect to their home site's
NATS over WebSocket. Cross-site events flow OUTBOX → (JetStream source) → INBOX.

```mermaid
flowchart LR
    subgraph SiteA["🌐 Site A (independent deployment)"]
        direction TB
        NA[("NATS + JetStream")]
        SVCA["Services<br/>(workers + RPC)"]
        DBA[("MongoDB")]
        CASA[("Cassandra")]
        SVCA --- NA
        SVCA --- DBA
        SVCA --- CASA
    end

    subgraph SiteB["🌐 Site B (independent deployment)"]
        direction TB
        NB[("NATS + JetStream")]
        SVCB["Services<br/>(workers + RPC)"]
        DBB[("MongoDB")]
        CASB[("Cassandra")]
        SVCB --- NB
        SVCB --- DBB
        SVCB --- CASB
    end

    CA["Clients (Site A)<br/>web / mobile"] -->|WebSocket| NA
    CB["Clients (Site B)<br/>web / mobile"] -->|WebSocket| NB

    NA -. "OUTBOX_A → INBOX_B<br/>(JetStream cross-site source)" .-> NB
    NB -. "OUTBOX_B → INBOX_A" .-> NA

    Portal["portal-service<br/>(site directory)"] -.-> CA
    Portal -.-> CB
```

---

## 2. Component Architecture (single site)

Three planes: **edge/HTTP** (auth, portal, upload), **request/reply RPC**
services, and **JetStream workers**. All real-time traffic crosses the NATS bus.

```mermaid
flowchart TB
    Client["🖥️ chat-frontend<br/>(React 19 + nats.ws)"]

    %% Edge / HTTP plane
    subgraph Edge["Edge — HTTP (Gin)"]
        Auth["auth-service<br/>OIDC → NATS JWT/NKeys"]
        Portal["portal-service<br/>site directory cache"]
        Upload["upload-service<br/>image uploads"]
    end

    %% NATS bus
    NATS{{"NATS + JetStream<br/>(message bus)"}}

    %% RPC plane (request/reply)
    subgraph RPC["Request/Reply Services"]
        RoomSvc["room-service<br/>room CRUD, invite auth, typing relay"]
        UserSvc["user-service<br/>users, subscriptions, apps"]
        HistSvc["history-service<br/>msg history, edit/delete/react"]
        Presence["user-presence-service<br/>online status"]
        Search["search-service<br/>messages/rooms/users search"]
    end

    %% JetStream workers
    subgraph Workers["JetStream Workers (event-driven)"]
        Gatekeeper["message-gatekeeper<br/>validate → canonical"]
        MsgWorker["message-worker<br/>persist + resolve mentions + fanout"]
        Broadcast["broadcast-worker<br/>deliver to members + metadata"]
        Notif["notification-worker<br/>push notifications"]
        RoomWorker["room-worker<br/>apply invites, sub events"]
        Inbox["inbox-worker<br/>consume remote OUTBOX"]
        SearchSync["search-sync-worker<br/>index canonical msgs"]
    end

    %% Data stores
    Mongo[("MongoDB<br/>rooms, subs, messages")]
    Cass[("Cassandra<br/>message history")]
    ES[("Elasticsearch<br/>search index")]
    Valkey[("Valkey<br/>caches, presence")]
    MinIO[("MinIO / Drive<br/>image blobs")]

    %% Client edges
    Client -->|HTTPS login| Auth
    Client -->|HTTPS| Portal
    Client -->|HTTPS multipart| Upload
    Client <-->|WebSocket pub/sub + req/reply| NATS

    %% RPC over NATS
    NATS <--> RoomSvc
    NATS <--> UserSvc
    NATS <--> HistSvc
    NATS <--> Presence
    NATS <--> Search

    %% Workers on NATS
    NATS <--> Gatekeeper
    NATS <--> MsgWorker
    NATS <--> Broadcast
    NATS <--> Notif
    NATS <--> RoomWorker
    NATS <--> Inbox
    NATS <--> SearchSync

    %% Data store edges
    RoomSvc --- Mongo
    UserSvc --- Mongo
    HistSvc --- Cass
    HistSvc --- Mongo
    MsgWorker --- Cass
    Broadcast --- Mongo
    Broadcast --- Valkey
    Notif --- Mongo
    Notif --- Valkey
    RoomWorker --- Mongo
    Inbox --- Mongo
    Gatekeeper --- Valkey
    Presence --- Valkey
    Presence --- Mongo
    Search --- ES
    SearchSync --- ES
    Upload --- MinIO
    Upload --- Mongo
```

---

## 3. Message Send Flow (happy path)

A user sends a message; it is validated, persisted, broadcast to room members,
notified, and indexed — each step on its own JetStream stream.

```mermaid
sequenceDiagram
    actor Sender
    participant NATS as NATS / JetStream
    participant GK as message-gatekeeper
    participant MW as message-worker
    participant BW as broadcast-worker
    participant NW as notification-worker
    participant SS as search-sync-worker
    participant Cass as Cassandra
    actor Members as Room members

    Sender->>NATS: pub chat.user.{acct}.room.{room}.{site}.msg.send
    Note over NATS: MESSAGES stream
    NATS->>GK: deliver
    GK->>GK: validate (membership, size, mentions)
    GK->>NATS: pub chat.msg.canonical.{site}.created
    Note over NATS: MESSAGES_CANONICAL stream<br/>(single source of truth)

    NATS->>MW: canonical.created
    MW->>Cass: persist message
    MW->>NATS: pub fanout.{site}.{room}
    Note over NATS: FANOUT stream

    NATS->>BW: fanout.{site}.{room}
    BW->>Members: pub chat.room.{room}.stream.msg
    BW->>Members: pub chat.room.{room}.event.metadata.update
    BW-->>NATS: pub outbox.{site}.to.{dest}.* (remote members)

    NATS->>NW: fanout / canonical.created
    NW->>NATS: pub chat.server.notification.push.{site}.send
    Note over NATS: PUSH_NOTIFICATION stream → push service

    NATS->>SS: canonical.created
    SS->>SS: index message in Elasticsearch
```

---

## 4. JetStream Stream Topology

Streams are named `<STREAM>_<siteID>` (see `pkg/stream/stream.go`). Stream
creation is opt-in (`BOOTSTRAP_STREAMS`) — owned by ops/IaC in production.

```mermaid
flowchart LR
    Client(["Client"])

    MESSAGES[["MESSAGES_{site}<br/>chat.user.*.room.*.{site}.msg.&gt;"]]
    CANON[["MESSAGES_CANONICAL_{site}<br/>chat.msg.canonical.{site}.&gt;"]]
    FANOUT[["FANOUT_{site}<br/>fanout.{site}.&gt;"]]
    ROOMS[["ROOMS_{site}<br/>member invites"]]
    PUSH[["PUSH_NOTIFICATION_{site}<br/>push.{site}.&gt;"]]
    OUTBOX[["OUTBOX_{site}<br/>outbox.{site}.&gt;"]]
    INBOX[["INBOX_{site}<br/>chat.inbox.{site}.*"]]

    Client -->|msg.send| MESSAGES
    MESSAGES --> GK[message-gatekeeper]
    GK -->|canonical.created| CANON

    CANON --> MW[message-worker]
    CANON --> BW[broadcast-worker]
    CANON --> NW[notification-worker]
    CANON --> SS[search-sync-worker]

    MW -->|fanout| FANOUT
    FANOUT --> BW
    FANOUT --> NW

    Client -->|member.invite| ROOMS
    ROOMS --> RW[room-worker]

    NW -->|push.send| PUSH

    BW -->|cross-site| OUTBOX
    RW -->|cross-site| OUTBOX
    OUTBOX -. "JetStream source<br/>(remote site)" .-> INBOX
    INBOX --> IW[inbox-worker]

    HS[history-service] -->|edited/deleted/reacted| CANON
```

---

## 5. Authentication Flow

`auth-service` bridges the org's OIDC IdP to NATS. It validates the SSO token
and mints a short-lived NATS user JWT signed with the account NKey, scoping
pub/sub permissions to the user's own `chat.user.{account}.>` namespace.

```mermaid
sequenceDiagram
    actor User
    participant FE as chat-frontend
    participant IdP as OIDC IdP (Keycloak)
    participant Auth as auth-service
    participant NATS as NATS

    User->>FE: open app
    FE->>IdP: OIDC login (PKCE)
    IdP-->>FE: id_token
    FE->>Auth: POST /auth (Bearer id_token)
    Auth->>IdP: validate token (JWKS)
    Auth->>Auth: build scoped permissions<br/>sign NATS JWT with account NKey
    Auth-->>FE: { natsJwt }
    FE->>NATS: connect (JWT + NKey)
    Note over NATS: pub/sub limited to<br/>chat.user.{account}.> + chat.room.> (sub)
```

---

## 6. Data Stores & Responsibilities

| Store | Used for | Owners |
|-------|----------|--------|
| **MongoDB** | Operational data: rooms, subscriptions, room members, users, apps | room-service, user-service, room-worker, broadcast-worker, notification-worker, inbox-worker |
| **Cassandra** | Message history (time-series, bucketed by `(room_id, bucket)`) | message-worker (write), history-service (read) |
| **Elasticsearch** | Full-text search index (messages, rooms, users) | search-sync-worker (write), search-service (read) |
| **Valkey** (cluster) | Subscription/room-meta caches, presence | message-gatekeeper, broadcast-worker, notification-worker, user-presence-service |
| **MinIO / Drive** | Uploaded image blobs | upload-service |

---

## 7. Tech Stack

| Concern | Choice |
|---------|--------|
| Language | Go 1.25 |
| Messaging | NATS + JetStream |
| Operational DB | MongoDB (`mongo-driver/v2`) |
| History DB | Cassandra (`gocql`) |
| Search | Elasticsearch |
| Cache / presence | Valkey (cluster mode) |
| Object storage | MinIO / Drive |
| Auth | NATS callout — OIDC → JWT + NKeys |
| HTTP server / client | Gin / Resty |
| Config | env vars via `caarlos0/env` |
| Observability | `flywindy/o11y` SDK (OpenTelemetry traces, Prometheus metrics, `log/slog` logs) |
| Frontend | React 19 + `nats.ws` + `oidc-client-ts` |
```

## 8. Observability

All three signals come from the `flywindy/o11y` SDK. Each service initializes it
once in `main` via `pkg/obs.Init`, which installs the SDK's providers as the
OpenTelemetry globals and the SDK logger as the `slog` default.

- **Traces** — context propagates across process hops: NATS via the
  `o11y/nats` facade (publish injects W3C `traceparent`, consume extracts it)
  and HTTP via the `o11y/gin` server middleware. Datastore spans
  (Mongo / Cassandra / Valkey / MinIO / Elasticsearch) come from the `pkg/*`
  connect helpers, wired with `WithObservability(sdk)`. A single message send is
  one trace spanning `message-gatekeeper → message-worker → broadcast-worker`
  with datastore spans as children.
- **Metrics** — each service exposes a Prometheus endpoint on `:2112`
  (`OTEL_EXPORTER_PROMETHEUS_PORT`) carrying SDK runtime + instrumentation
  metrics. Services that keep app-level counters (e.g. `search-service`) expose
  those on their own listener.
- **Logs** — `log/slog` JSON through the SDK logger; lines emitted under an
  active span carry `traceId` / `spanId` for correlation.
- **Export & backend** — OTLP (`OTEL_EXPORTER_OTLP_ENDPOINT`) to the o11y
  monitor stack (Alloy / OTel Collector → Tempo, Loki, Prometheus), viewed in
  Grafana. `OTEL_SERVICE_NAME` is required per service (the SDK fails fast
  without it).
