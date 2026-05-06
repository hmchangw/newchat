# Client API Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce `docs/client-api.md` — a single integrator-facing reference covering every client-callable API the chat backend exposes (HTTP `POST /auth`, NATS request/reply methods on room-service / history-service / search-service, the message-send publish flow), with subject, request body, success response, error response, and the server-pushed events triggered on success and error paths for each method.

**Architecture:** A hand-written Markdown document. Every method uses the same fixed block layout. Schemas and examples are derived from the actual handler code and Go request/response types (the source of truth). A maintenance rule in `CLAUDE.md` requires updates to the doc in the same PR that changes a client-facing handler.

**Tech Stack:** Markdown, plus `jq` for validating JSON examples.

**Spec:** [`docs/superpowers/specs/2026-05-06-client-api-doc-design.md`](../specs/2026-05-06-client-api-doc-design.md)

---

## Background for the implementer

You're writing one Markdown file. The repo is a Go monorepo with a chat backend. Clients call the backend over NATS (request/reply and publish-with-async-reply) and over a single HTTP endpoint. You don't write Go code in this plan — you read Go code to derive accurate JSON schemas and examples for the doc.

**Key files you will reference repeatedly:**

- Subject builders: `pkg/subject/subject.go` — every subject in the doc must be reproducible by a function in this file.
- Error envelope: `pkg/model/error.go` — `ErrorResponse{ Error string }`.
- Shared models: `pkg/model/` — request/response types used by room-service, search-service, message-gatekeeper.
- History-service models: `history-service/internal/models/message.go` — request/response types used only by history-service.
- Auth-service handler: `auth-service/handler.go` — HTTP endpoint and request/response types.

**Per-method block format** (the canonical layout — every block in §2.2, §3, and §4 looks like this):

````markdown
### <Method name>

**Subject:** `chat.user.{account}.request.…`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

#### Request body

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| ...   | ...  | ...      | ...   |

```json
{
  "...": "..."
}
```

#### Success response

| Field | Type | Notes |
|-------|------|-------|
| ...   | ...  | ...   |

```json
{
  "...": "..."
}
```

#### Error response

See [Error envelope](#error-envelope-reference).

```json
{ "error": "<example reason>" }
```

#### Triggered events — success path

For each event:

**Subject:** `<full subject>`
**Recipients:** `<who receives it>`

| Field | Type | Notes |
|-------|------|-------|
| ...   | ...  | ...   |

```json
{
  "...": "..."
}
```

If none: `None — reply only.`

#### Triggered events — error path

`None — error returned only via the reply subject.` (or list events if any)
````

**Conventions for field tables:**

- `Type` uses JSON-level types: `string`, `number`, `boolean`, `object`, `string[]`, `array<Object>`. Not Go types like `int64` or `time.Time`.
- For Go `time.Time` fields → JSON type `string` with note `"RFC 3339 timestamp"`.
- For Go `int64` Unix-millis fields → JSON type `number` with note `"milliseconds since Unix epoch (UTC)"`.
- `Required`: `yes` / `no`. For `no`, document what omitting means (default value or "field absent" semantics).
- Read `json:"…"` struct tags for field names. Tags with `,omitempty` mean the field is optional in responses (omitted when zero); for requests, treat `,omitempty` as `Required: no`.

**Example conventions:**

- Stable example accounts: `alice` (the requester), `bob` and `carol` (other users).
- Stable site ID: `siteA`.
- Channel room ID example: `01970a4f8c2d7c9aQ` (17-char base62).
- DM room ID example: `alice___bob` (sorted concat of two accounts).
- Message ID example: `01970a4f8c2d7c9aQRST` (20-char base62).
- Request ID example: `01970a4f-8c2d-7c9a-abcd-e0123456789f` (36-char hyphenated UUIDv7).
- Pretty-print JSON with 2-space indentation.

---

## Task 1: Scaffold the doc with section skeleton

**Files:**
- Create: `docs/client-api.md`

- [ ] **Step 1: Create the file with the full section skeleton**

Create `docs/client-api.md` with exactly this content:

````markdown
# Chat Backend — Client API Reference

This document is the integrator-facing reference for the chat backend.
It covers every API a client (web, mobile, third-party) can call:

- **HTTP** — the single `POST /auth` endpoint that exchanges an SSO token
  for a NATS user JWT.
- **NATS request/reply** — RPC-style methods exposed by `room-service`,
  `history-service`, and `search-service`.
- **NATS publish + async reply** — the message-send flow handled by
  `message-gatekeeper`.

For each method, this doc lists the subject, the request body schema and
example, the success response schema and example, the error response, and
the server-pushed events the client will receive on the success and error
paths.

## Table of contents

1. [Overview](#1-overview)
2. [Connection & Auth](#2-connection--auth)
   - [2.1 NATS connection](#21-nats-connection)
   - [2.2 HTTP — POST /auth](#22-http--post-auth)
3. [Request/Reply Methods](#3-requestreply-methods)
   - [3.1 room-service](#31-room-service)
   - [3.2 history-service](#32-history-service)
   - [3.3 search-service](#33-search-service)
4. [Message Send](#4-message-send)
5. [Error envelope reference](#5-error-envelope-reference)

---

## 1. Overview

_(filled in by Task 2)_

---

## 2. Connection & Auth

### 2.1 NATS connection

_(filled in by Task 3)_

### 2.2 HTTP — POST /auth

_(filled in by Task 4)_

---

## 3. Request/Reply Methods

### 3.1 room-service

_(filled in by Tasks 6–13)_

### 3.2 history-service

_(filled in by Tasks 14–21)_

### 3.3 search-service

_(filled in by Tasks 22–23)_

---

## 4. Message Send

_(filled in by Task 24)_

---

## 5. Error envelope reference

_(filled in by Task 5)_
````

- [ ] **Step 2: Verify markdown renders**

Run:

```bash
test -s docs/client-api.md && echo "OK: file exists and is non-empty"
```

Expected: `OK: file exists and is non-empty`.

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md
git commit -m "docs: scaffold client-api.md skeleton"
```

---

## Task 2: Section 1 — Overview

**Files:**
- Modify: `docs/client-api.md` — replace the `_(filled in by Task 2)_` placeholder under `## 1. Overview`.

- [ ] **Step 1: Replace the placeholder with this content**

Edit `docs/client-api.md`. Replace the line `_(filled in by Task 2)_` directly under `## 1. Overview` with:

````markdown
This doc covers the public client-facing API surface only.

**Out of scope (documented elsewhere or backend-internal):**

- Backend-only JetStream subjects (MESSAGES, MESSAGES_CANONICAL, FANOUT, OUTBOX, INBOX, ROOMS streams). See [`docs/nats-subject-naming.md`](./nats-subject-naming.md).
- Server-pushed events not triggered by a specific client RPC (federation arrivals, presence, room-key rotation, cross-site member events).
- Server-to-server subjects (`chat.server.request.…`).

### Subject placeholders

Subjects in this doc use these placeholders:

| Placeholder | Meaning |
|-------------|---------|
| `{account}` | The user's NATS account (preferred username from SSO claims, e.g. `alice`). |
| `{roomID}`  | A room ID (see "ID formats" below). |
| `{siteID}`  | The site that owns the room (each site runs its own NATS). |
| `{requestID}` | A 36-char hyphenated UUIDv7 generated by the client for the message-send async-reply pattern. |

### Encoding

All NATS payloads are JSON. All HTTP request/response bodies are JSON.

### ID formats

| ID kind | Format | Length | Notes |
|---------|--------|--------|-------|
| Account | Lowercase string | variable | SSO-derived; appears as `{account}` in subjects. |
| Channel room ID | base62 | 17 chars | Generated server-side at room creation. |
| DM room ID | sorted concat of two accounts | ~`len(a)+3+len(b)` | Deterministic; same two users always produce the same ID. |
| Message ID | base62 | 17 or 20 chars | New messages are 20-char; 17-char accepted for legacy/federated messages. |
| Request ID | hyphenated UUIDv7 | 36 chars | Both inbound `X-Request-ID` headers and the `requestId` payload field for `msg.send`. |

### Request-ID propagation

Clients **may** include an `X-Request-ID` NATS message header on outbound requests. If present, the server uses it for log correlation; if absent, the server generates one. The header value must be a valid hyphenated UUID (v4 or v7, case-insensitive).

The `msg.send` flow is different — see [§4](#4-message-send): the client puts the request ID in the JSON payload (`requestId` field), and the server replies on `chat.user.{account}.response.{requestID}`.

### Reply patterns

- **Standard NATS request/reply** — the NATS client library auto-generates a reply subject under `_INBOX.>` and routes the reply back to the caller. Used by every method in §3.
- **Async reply on `chat.user.{account}.response.{requestID}`** — used only by `msg.send` (§4). The client publishes (no synchronous reply expected on `_INBOX.>`); the server reads `requestId` from the payload and publishes the reply to `chat.user.{account}.response.{requestID}`. The client must already be subscribed to `chat.user.{account}.>` (the user wildcard) to receive it.

### Timestamps

All event payloads carry a top-level `timestamp` field that is **milliseconds since the Unix epoch in UTC**. Domain timestamps inside payloads (e.g. `Message.createdAt`) are RFC 3339 strings.
````

- [ ] **Step 2: Verify the placeholder is replaced**

Run:

```bash
grep -c "_(filled in by Task 2)_" docs/client-api.md
```

Expected: `0`.

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): write Overview section"
```

---

## Task 3: Section 2.1 — NATS connection

**Files:**
- Modify: `docs/client-api.md` — replace placeholder under `### 2.1 NATS connection`.

**Source of truth to read first:**
- `auth-service/handler.go:166-180` (`signNATSJWT`) — the exact pub/sub permissions the JWT grants.

- [ ] **Step 1: Replace the placeholder with this content**

Replace `_(filled in by Task 3)_` directly under `### 2.1 NATS connection` with:

````markdown
A client connects to NATS using a user NKey pair plus a signed JWT obtained from the auth-service (§2.2). The JWT scopes the client's permissions to:

| Permission | Subject pattern | Why |
|------------|-----------------|-----|
| Publish    | `chat.user.{account}.>` | The client may publish only under its own user namespace. All RPC requests, the message-send subject, and any client-emitted event fall here. |
| Publish    | `_INBOX.>`              | Required for the standard NATS request/reply pattern (the auto-generated reply inbox). |
| Subscribe  | `chat.user.{account}.>` | Receives all responses, notifications, and per-user events. |
| Subscribe  | `chat.room.>`           | Subscribes to per-room message streams and room events for any room the user belongs to. |
| Subscribe  | `_INBOX.>`              | Required to receive replies to client-issued requests. |

**Recommended baseline subscriptions on connect:**

- `chat.user.{account}.>` — captures every personal event including async replies, notifications, and subscription updates.
- `chat.room.{roomID}.stream.msg` for each room in the user's sidebar — receives new messages.

The exact event subjects a client may receive as a result of an RPC are listed under each method's "Triggered events" sections in §2.2, §3, and §4.
````

- [ ] **Step 2: Verify the placeholder is replaced**

Run:

```bash
grep -c "_(filled in by Task 3)_" docs/client-api.md
```

Expected: `0`.

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): write NATS connection section"
```

---

## Task 4: Section 2.2 — HTTP POST /auth

**Files:**
- Modify: `docs/client-api.md` — replace placeholder under `### 2.2 HTTP — POST /auth`.

**Source of truth to read first:**
- `auth-service/handler.go:24-47` (request and response struct definitions).
- `auth-service/handler.go:71-129` (`HandleAuth`) — happy path, error statuses, sanitized error messages.
- `auth-service/handler.go:133-162` (`handleDevAuth`) — dev-mode behavior.
- `auth-service/routes.go` — the route registration.

- [ ] **Step 1: Replace the placeholder with this content**

Replace `_(filled in by Task 4)_` directly under `### 2.2 HTTP — POST /auth` with:

````markdown
**Endpoint:** `POST /auth`
**Reply:** synchronous HTTP response

Exchanges an SSO token for a signed NATS user JWT. The returned JWT is what the client uses to connect to NATS (see §2.1).

#### Request body

| Field           | Type   | Required | Notes |
|-----------------|--------|----------|-------|
| `ssoToken`      | string | yes      | OIDC-issued SSO token. |
| `natsPublicKey` | string | yes      | The client's NATS user public NKey (must pass `nkeys.IsValidPublicUserKey`). |

```json
{
  "ssoToken": "<sso-token>",
  "natsPublicKey": "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4"
}
```

#### Success response

`HTTP 200`

| Field               | Type   | Notes |
|---------------------|--------|-------|
| `natsJwt`           | string | Signed NATS user JWT. Use as the user JWT when connecting to NATS. |
| `user.email`        | string | OIDC email claim. |
| `user.account`      | string | The `{account}` value used in every NATS subject. Derived from `preferred_username` (falls back to `name`). |
| `user.employeeId`   | string | Parsed from the SSO `description` claim. |
| `user.engName`      | string | Parsed from the SSO `description` claim. |
| `user.chineseName`  | string | Parsed from the SSO `description` claim. |
| `user.deptName`     | string | OIDC dept-name claim. |
| `user.deptId`       | string | OIDC dept-id claim. |

```json
{
  "natsJwt": "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...",
  "user": {
    "email": "alice@example.com",
    "account": "alice",
    "employeeId": "E12345",
    "engName": "Alice",
    "chineseName": "愛麗絲",
    "deptName": "Engineering",
    "deptId": "ENG"
  }
}
```

#### Error response

See [Error envelope](#5-error-envelope-reference). HTTP statuses:

| Status | Meaning | Example body |
|--------|---------|--------------|
| 400    | Missing or malformed fields, or invalid `natsPublicKey`. | `{ "error": "ssoToken and natsPublicKey are required" }` |
| 401    | SSO token expired or invalid. | `{ "error": "SSO token has expired, please re-login" }` |
| 500    | Server-side JWT signing failure. | `{ "error": "failed to generate NATS token" }` |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

#### Dev mode

When the auth-service is started with `DEV_MODE=true`, the request body schema is `{ "account": string, "natsPublicKey": string }` (no SSO token; the supplied account is trusted). This is local-development only and is **not** part of the production contract.
````

- [ ] **Step 2: Verify JSON examples parse**

Run:

```bash
jq -e . <<'EOF'
{
  "ssoToken": "<sso-token>",
  "natsPublicKey": "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4"
}
EOF
```

Expected: the same JSON echoed back; exit code 0.

```bash
jq -e . <<'EOF'
{
  "natsJwt": "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...",
  "user": {
    "email": "alice@example.com",
    "account": "alice",
    "employeeId": "E12345",
    "engName": "Alice",
    "chineseName": "愛麗絲",
    "deptName": "Engineering",
    "deptId": "ENG"
  }
}
EOF
```

Expected: exit code 0.

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): write POST /auth section"
```

---

## Task 5: Section 5 — Error envelope reference

(Done early so later method blocks can link to it.)

**Files:**
- Modify: `docs/client-api.md` — replace placeholder under `## 5. Error envelope reference`.

**Source of truth to read first:**
- `pkg/model/error.go` — the `ErrorResponse` struct.
- Search the codebase for `natsutil.ReplyError` usage to confirm the envelope shape.

- [ ] **Step 1: Replace the placeholder with this content**

Replace `_(filled in by Task 5)_` directly under `## 5. Error envelope reference` with:

````markdown
Every error response — over NATS reply subjects and HTTP — uses the same envelope:

```json
{ "error": "<human-readable reason>" }
```

| Field   | Type   | Notes |
|---------|--------|-------|
| `error` | string | Human-readable, sanitized at the service boundary. Do not parse or pattern-match against the text. |

**NATS errors** are sent on the standard reply subject (`_INBOX.>` for §3 methods, `chat.user.{account}.response.{requestID}` for §4) via `natsutil.ReplyError`. The reply body is the JSON object above.

**HTTP errors** (auth-service §2.2) use the same shape with an HTTP status code in the response line.

Clients should rely on the presence/absence of the `error` field — and on context (HTTP status, or whether a reply parses as a success-shape) — rather than on the error text.
````

- [ ] **Step 2: Verify the placeholder is replaced**

Run:

```bash
grep -c "_(filled in by Task 5)_" docs/client-api.md
```

Expected: `0`.

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): write error envelope reference"
```

---

## Task 6: §3.1 room-service — Create Room (worked example)

**This task is the canonical example.** Subsequent room-service tasks (7–13) follow the same recipe; the recipe template is repeated in each task with the right source-of-truth files. Read this task in full first.

**Files:**
- Modify: `docs/client-api.md` — append a method block under `### 3.1 room-service`, replacing `_(filled in by Tasks 6–13)_` with the first method block.

**Source of truth to read first:**
- `pkg/subject/subject.go:168` (`RoomsCreate`) — subject template.
- `pkg/model/room.go:30-37` (`CreateRoomRequest`) — request struct.
- `pkg/model/room.go:14-28` (`Room`) — response struct (the handler returns the created `Room`).
- `room-service/handler.go:78-88` (`natsCreateRoom`) and `:112-167` (`handleCreateRoom`) — what the handler does and what events it triggers.
- The handler **only** creates the owner subscription synchronously (`store.CreateSubscription`); it does not publish any NATS events. Member adds happen via a separate RPC. So the success-path triggered-event list is empty.

- [ ] **Step 1: Insert the Create Room method block**

Replace `_(filled in by Tasks 6–13)_` directly under `### 3.1 room-service` with:

````markdown
#### Create Room

**Subject:** `chat.user.{account}.request.rooms.create`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

##### Request body

| Field              | Type   | Required | Notes |
|--------------------|--------|----------|-------|
| `name`             | string | yes      | Room name. |
| `type`             | string | yes      | One of `channel`, `dm`, `botDM`, `discussion`. |
| `createdBy`        | string | yes      | Internal user ID of the creator. |
| `createdByAccount` | string | yes      | Account name of the creator. Used for the owner subscription. |
| `siteId`           | string | yes      | The site that will own this room. |
| `members`          | string[] | no     | Required exactly **one** entry when `type=dm` (the other user's ID); ignored otherwise. |

```json
{
  "name": "engineering-announcements",
  "type": "channel",
  "createdBy": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "createdByAccount": "alice",
  "siteId": "siteA"
}
```

##### Success response

The created `Room` object.

| Field               | Type   | Notes |
|---------------------|--------|-------|
| `id`                | string | Room ID. 17-char base62 for channels; sorted concat of two accounts for DMs. |
| `name`              | string |       |
| `type`              | string | Same values as request. |
| `createdBy`         | string |       |
| `siteId`            | string |       |
| `userCount`         | number | `1` immediately after creation (the owner). |
| `lastMsgAt`         | string | Optional. RFC 3339 timestamp; absent until first message. |
| `lastMsgId`         | string | Empty until first message. |
| `lastMentionAllAt`  | string | Optional. RFC 3339 timestamp. |
| `minUserLastSeenAt` | string | Optional. RFC 3339 timestamp. |
| `createdAt`         | string | RFC 3339 timestamp. |
| `updatedAt`         | string | RFC 3339 timestamp. |
| `restricted`        | boolean | Optional. |

```json
{
  "id": "01970a4f8c2d7c9aQ",
  "name": "engineering-announcements",
  "type": "channel",
  "createdBy": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "siteId": "siteA",
  "userCount": 1,
  "lastMsgId": "",
  "createdAt": "2026-05-06T08:00:00Z",
  "updatedAt": "2026-05-06T08:00:00Z"
}
```

##### Error response

See [Error envelope](#5-error-envelope-reference).

```json
{ "error": "DM requires exactly one other member, got 0" }
```

##### Triggered events — success path

`None — reply only.` Member additions are a separate RPC (Add Members); creating a room only enrolls the owner.

##### Triggered events — error path

`None — error returned only via the reply subject.`

---
````

- [ ] **Step 2: Verify both example payloads parse**

```bash
jq -e . <<'EOF'
{
  "name": "engineering-announcements",
  "type": "channel",
  "createdBy": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "createdByAccount": "alice",
  "siteId": "siteA"
}
EOF

jq -e . <<'EOF'
{
  "id": "01970a4f8c2d7c9aQ",
  "name": "engineering-announcements",
  "type": "channel",
  "createdBy": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "siteId": "siteA",
  "userCount": 1,
  "lastMsgId": "",
  "createdAt": "2026-05-06T08:00:00Z",
  "updatedAt": "2026-05-06T08:00:00Z"
}
EOF
```

Both expected: exit code 0.

- [ ] **Step 3: Verify field names match the Go struct**

Confirm every JSON key in the request example appears as a `json:"…"` tag in `pkg/model/room.go` `CreateRoomRequest`, and every key in the response example is a tag on `Room`:

```bash
grep -E 'json:"[a-zA-Z]+' /home/user/chat/pkg/model/room.go | head -40
```

Cross-check by eye against the example. Any mismatch must be fixed before commit.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): document Create Room"
```

---

## Tasks 7–13: §3.1 room-service — remaining methods

Each task below uses the same recipe as Task 6:

1. Read the listed source-of-truth files.
2. Append a method block (using the canonical block format) under `### 3.1 room-service`, after the previous method, separated by `---`.
3. Validate JSON examples with `jq`.
4. Cross-check field names against the Go structs.
5. Commit.

The "Reply subject" line is `auto-generated `_INBOX.>` (NATS request/reply)` for every method in this section.

### Task 7: List Rooms

**Source of truth:** `pkg/subject/subject.go:172` (`RoomsList`); `pkg/model/room.go:39-41` (`ListRoomsResponse`); `room-service/handler.go:90-98` (`natsListRooms`).

- [ ] **Step 1: Append method block**

````markdown
#### List Rooms

**Subject:** `chat.user.{account}.request.rooms.list`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

##### Request body

Empty. Send `{}` or no payload.

```json
{}
```

##### Success response

| Field   | Type           | Notes |
|---------|----------------|-------|
| `rooms` | array<Room>    | All rooms the requester is subscribed to. See Create Room for the `Room` schema. |

```json
{
  "rooms": [
    {
      "id": "01970a4f8c2d7c9aQ",
      "name": "engineering-announcements",
      "type": "channel",
      "createdBy": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
      "siteId": "siteA",
      "userCount": 12,
      "lastMsgAt": "2026-05-06T07:55:00Z",
      "lastMsgId": "01970a4f8c2d7c9aQRST",
      "createdAt": "2026-05-01T10:00:00Z",
      "updatedAt": "2026-05-06T07:55:00Z"
    }
  ]
}
```

##### Error response

See [Error envelope](#5-error-envelope-reference).

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---
````

- [ ] **Step 2: Validate JSON, cross-check fields, commit**

```bash
jq -e . <<'EOF'
{ "rooms": [{ "id": "01970a4f8c2d7c9aQ", "name": "engineering-announcements", "type": "channel", "createdBy": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "siteId": "siteA", "userCount": 12, "lastMsgAt": "2026-05-06T07:55:00Z", "lastMsgId": "01970a4f8c2d7c9aQRST", "createdAt": "2026-05-01T10:00:00Z", "updatedAt": "2026-05-06T07:55:00Z" }] }
EOF
git add docs/client-api.md
git commit -m "docs(client-api): document List Rooms"
```

### Task 8: Get Room

**Source of truth:** `pkg/subject/subject.go:176` (`RoomsGet`); `pkg/model/room.go:14-28` (`Room`); `room-service/handler.go:100-110` (`natsGetRoom`). Note the room ID is **embedded in the subject**, not in the body.

- [ ] **Step 1: Append method block**

````markdown
#### Get Room

**Subject:** `chat.user.{account}.request.rooms.get.{roomID}`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

The room ID is the last subject segment — there is no request body.

##### Request body

Empty. Send `{}` or no payload.

```json
{}
```

##### Success response

A single `Room` object. See [Create Room](#create-room) for the `Room` schema.

```json
{
  "id": "01970a4f8c2d7c9aQ",
  "name": "engineering-announcements",
  "type": "channel",
  "createdBy": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "siteId": "siteA",
  "userCount": 12,
  "lastMsgAt": "2026-05-06T07:55:00Z",
  "lastMsgId": "01970a4f8c2d7c9aQRST",
  "createdAt": "2026-05-01T10:00:00Z",
  "updatedAt": "2026-05-06T07:55:00Z"
}
```

##### Error response

See [Error envelope](#5-error-envelope-reference).

```json
{ "error": "room not found" }
```

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---
````

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Get Room"
```

### Task 9: Add Members

**Source of truth:** `pkg/subject/subject.go:315-317` (`MemberAdd`); `pkg/model/member.go:30-40` (`AddMembersRequest`); `room-service/handler.go:430-498` (`natsAddMembers` / `handleAddMembers`); `pkg/model/event.go:165-171` (`AsyncJobResult` — what the requester receives once room-worker processes the bulk add); `pkg/model/event.go:32-37` (`SubscriptionUpdateEvent` — what each newly added member receives). Read the handler carefully: it returns `200 OK` synchronously after authorization, and the actual member adds happen asynchronously in `room-worker`. The async result lands on the requester's response subject.

- [ ] **Step 1: Append method block** — list the `200 OK ack` reply, then under "Triggered events — success path" list:
  - `chat.user.{account}.response.{requestID}` → the `AsyncJobResult` (delivered to the **requester** when the async job finishes).
  - `chat.user.{newMember}.event.subscription.update` → the `SubscriptionUpdateEvent` (delivered to **each newly added member**).

Write the block following the canonical format. The request example must include `roomId`, `users`, `orgs`, `channels` (array of `{roomId, siteId}`), and `history.mode`. The success-path event examples must include `requestId`, `job: "add_members"`, `success: true`.

Field-by-field tables for both events. Both events carry a `timestamp` in milliseconds.

- [ ] **Step 2: Validate JSON, cross-check fields, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Add Members"
```

### Task 10: Remove Member

**Source of truth:** `pkg/subject/subject.go:72-74` (`MemberRemove`); `pkg/model/member.go:63-70` (`RemoveMemberRequest`); `room-service/handler.go:170-181, 254-340` (`NatsHandleRemoveMember` / `handleRemoveMember`); `pkg/model/event.go:165-171` (`AsyncJobResult`); `pkg/model/event.go:32-37` (`SubscriptionUpdateEvent` for the removed user with `action: "removed"`).

- [ ] **Step 1: Append method block** including:
  - Request fields: `roomId` (also derivable from subject), `account` OR `orgId` (exactly one), `requester` (server-set; document but mark "set by server"), `timestamp` (server-set).
  - Triggered events — success path: `AsyncJobResult` on the requester's response subject (`job: "remove_member"` or `"remove_org"`); `SubscriptionUpdateEvent` with `action: "removed"` to the removed account(s).

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Remove Member"
```

### Task 11: Update Member Role

**Source of truth:** `pkg/subject/subject.go:68-70` (`MemberRoleUpdate`); `pkg/model/event.go:39-45` (`UpdateRoleRequest`); `room-service/handler.go:340-410` (`natsUpdateRole` / `handleUpdateRole`); `pkg/model/event.go:165-171` (`AsyncJobResult` with `job: "role_update"`).

- [ ] **Step 1: Append method block** including:
  - Request: `roomId`, `account`, `newRole` (e.g. `owner`, `moderator`, `member`).
  - Success-path event: `AsyncJobResult` on the requester's response subject.

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Update Member Role"
```

### Task 12: List Members

**Source of truth:** `pkg/subject/subject.go:76-78` (`MemberList`); `pkg/model/member.go:97-105` (`ListRoomMembersRequest`, `ListRoomMembersResponse`, `RoomMember`, `RoomMemberEntry`); `room-service/handler.go:183-192, 220-252` (`natsListMembers` / `handleListMembers`).

- [ ] **Step 1: Append method block** including:
  - Request fields: `limit` (optional, must be > 0), `offset` (optional, must be >= 0), `enrich` (optional, when true populates `engName`, `chineseName`, `isOwner`, `sectName`, `memberCount`).
  - Response: `members[]` with each member's `id`, `rid`, `ts`, and `member` sub-object (`id`, `type` of `individual`/`org`, `account`, plus enriched fields when requested).
  - No triggered events — pure read.

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document List Members"
```

### Task 13: List Org Members

**Source of truth:** `pkg/subject/subject.go:204-211` (`OrgMembers`); `pkg/model/member.go:111-121` (`OrgMember`, `ListOrgMembersResponse`); `room-service/handler.go:194-218` (`natsListOrgMembers` / `handleListOrgMembers`). The org ID is in the subject — no body needed.

- [ ] **Step 1: Append method block** including:
  - Subject template: `chat.user.{account}.request.orgs.{orgID}.members`.
  - Empty request body.
  - Response: `members[]` with each entry `{id, account, engName, chineseName, siteId}`.
  - Error example: `{ "error": "invalid org" }` (from the `errInvalidOrg` sentinel).
  - No triggered events.

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document List Org Members"
```

After Task 13, the `### 3.1 room-service` section is complete. Confirm by running:

```bash
grep -A 1 "^### 3.1 room-service" docs/client-api.md | head -3
grep -c "_(filled in by Tasks 6–13)_" docs/client-api.md
```

Expected: the second command returns `0`.

---

## Tasks 14–21: §3.2 history-service — methods

For every method in this section the "Reply subject" line is `auto-generated `_INBOX.>` (NATS request/reply)`.

History-service request/response types live in `history-service/internal/models/message.go`. The shared `Message` type is `pkg/model/cassandra.Message` (alias-imported as `Message` in the history-service models package). Read `pkg/model/cassandra/message.go` for the full `Message` schema before Task 14.

When you first need to describe a `Message` in the doc, write the full field table once under §3.2 with the heading "Message schema (used in history-service responses)" — placed before the first method block — and link to it from each subsequent method that returns messages. This avoids repeating ~20 fields per method.

### Task 14: Load History

**Source of truth:** `pkg/subject/subject.go:280` (`MsgHistoryPattern`); `history-service/internal/models/message.go:12-19` (`LoadHistoryRequest`, `LoadHistoryResponse`); `history-service/internal/service/messages.go:23-…` (`LoadHistory`). Read the handler to confirm: it does NOT publish any events on success — pure read.

- [ ] **Step 1: First, write the Message schema block under `### 3.2 history-service`** (replacing the placeholder), then append the Load History method block:

````markdown
#### Message schema

Used by the responses of every history-service method that returns messages.

| Field | Type | Notes |
|-------|------|-------|
| `id`  | string | Message ID. 17- or 20-char base62. |
| `roomId` | string | |
| `userId` | string | |
| `userAccount` | string | |
| `content` | string | |
| `mentions` | array<Participant> | Optional. Each `{userId?, account, siteId?, chineseName, engName}`. |
| `createdAt` | string | RFC 3339. |
| `threadParentMessageId` | string | Optional. Set when this message is a thread reply. |
| `threadParentMessageCreatedAt` | string | Optional. RFC 3339. |
| `tshow` | boolean | Optional. Whether the reply is also shown in the parent room. |
| `type` | string | Optional. System-message type when set (e.g. `"member_added"`); regular messages omit it. |
| `sysMsgData` | string (base64) | Optional. Raw JSON payload for system messages. |
| `quotedParentMessage` | object | Optional. Embedded snapshot of the quoted parent message. |

#### Load History

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.history`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

##### Request body

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `before` | number | no | Milliseconds since Unix epoch (UTC). Returns messages with `createdAt < before`. Omit (or `null`) for "now". |
| `limit`  | number | yes | Maximum number of messages to return. |

```json
{
  "before": 1746518400000,
  "limit": 50
}
```

##### Success response

| Field | Type | Notes |
|-------|------|-------|
| `messages` | array<Message> | Most-recent first. See [Message schema](#message-schema). |

```json
{
  "messages": [
    {
      "id": "01970a4f8c2d7c9aQRST",
      "roomId": "01970a4f8c2d7c9aQ",
      "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
      "userAccount": "alice",
      "content": "morning team",
      "createdAt": "2026-05-06T07:55:00Z"
    }
  ]
}
```

##### Error response

See [Error envelope](#5-error-envelope-reference).

```json
{ "error": "not subscribed to room" }
```

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---
````

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Message schema and Load History"
```

### Task 15: Load Next Messages

**Source of truth:** `pkg/subject/subject.go:284` (`MsgNextPattern`); `history-service/internal/models/message.go:21-31` (`LoadNextMessagesRequest`, `LoadNextMessagesResponse`).

- [ ] **Step 1: Append block** with request `{after?: number, limit: number, cursor: string}` and response `{messages: [], nextCursor?: string, hasNext: boolean}`. No triggered events.

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Load Next Messages"
```

### Task 16: Load Surrounding Messages

**Source of truth:** `pkg/subject/subject.go:288` (`MsgSurroundingPattern`); `history-service/internal/models/message.go:33-42` (`LoadSurroundingMessagesRequest`, `LoadSurroundingMessagesResponse`).

- [ ] **Step 1: Append block** with request `{messageId: string, limit: number}` and response `{messages: [], moreBefore: boolean, moreAfter: boolean}`. No triggered events.

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Load Surrounding Messages"
```

### Task 17: Get Message By ID

**Source of truth:** `pkg/subject/subject.go:295` (`MsgGetPattern`); `history-service/internal/models/message.go:44-46` (`GetMessageByIDRequest`); response is a single `Message`.

- [ ] **Step 1: Append block** with request `{messageId: string}` and response: a single Message object. No triggered events.

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Get Message By ID"
```

### Task 18: Edit Message

**Source of truth:** `pkg/subject/subject.go:301` (`MsgEditPattern`); `history-service/internal/models/message.go:48-56` (`EditMessageRequest`, `EditMessageResponse`); `history-service/internal/service/messages.go` — find the `EditMessage` handler and look for the `publisher.Publish` call to see the room-event subject and payload (`chat.room.{roomID}.event` with a `RoomEvent` carrying the updated `Message`). Confirm whether broadcast/notification fan-out is also published — read the related publish call in the same handler.

- [ ] **Step 1: Append block** including:
  - Request `{messageId: string, newMsg: string}`, response `{messageId: string, editedAt: number}`.
  - Triggered events — success path: list each event the handler publishes (subject + recipients + full payload schema + example). At minimum, expect a `chat.room.{roomID}.event` carrying the updated message; verify by reading the handler.
  - Triggered events — error path: `None — error returned only via the reply subject.`

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Edit Message"
```

### Task 19: Delete Message

**Source of truth:** `pkg/subject/subject.go:307` (`MsgDeletePattern`); `history-service/internal/models/message.go:58-65` (`DeleteMessageRequest`, `DeleteMessageResponse`); `history-service/internal/service/messages.go` — find the `DeleteMessage` handler and document the room-event publish.

- [ ] **Step 1: Append block** with request `{messageId: string}`, response `{messageId: string, deletedAt: number}`, and the success-path event mirroring what the handler publishes.

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Delete Message"
```

### Task 20: Get Thread Messages

**Source of truth:** `pkg/subject/subject.go:311` (`MsgThreadPattern`); `history-service/internal/models/message.go:67-77` (`GetThreadMessagesRequest`, `GetThreadMessagesResponse`).

- [ ] **Step 1: Append block** with request `{threadMessageId: string, cursor?: string, limit: number}` and response `{messages: [], nextCursor?: string, hasNext: boolean}`. No triggered events.

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Get Thread Messages"
```

### Task 21: Get Thread Parent Messages

**Source of truth:** `pkg/subject/subject.go:327` (`MsgThreadParentPattern`); the request/response types are at the bottom of `history-service/internal/models/message.go` (read past line 78). The endpoint lists parent messages of threads the user has subscribed to.

- [ ] **Step 1: Read the request/response struct definitions, then append the block** following the canonical format. No triggered events.

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Get Thread Parent Messages"
```

After Task 21, confirm:

```bash
grep -c "_(filled in by Tasks 14–21)_" docs/client-api.md
```

Expected: `0`.

---

## Tasks 22–23: §3.3 search-service

### Task 22: Search Messages

**Source of truth:** `pkg/subject/subject.go:338` (`SearchMessages`); `pkg/model/search.go:10-35` (`SearchMessagesRequest`, `SearchMessagesResponse`, `MessageSearchHit`); `search-service/handler.go:59-…` (`searchMessages`).

- [ ] **Step 1: Append method block** under `### 3.3 search-service` (replacing the placeholder). Document:
  - Request: `searchText` (required), `roomIds[]` (optional — empty means global), `size`, `offset` (both optional).
  - Response: `total`, `results[]` of `MessageSearchHit`.
  - No triggered events.

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Search Messages"
```

### Task 23: Search Rooms

**Source of truth:** `pkg/subject/subject.go:343` (`SearchRooms`); `pkg/model/search.go:41-63` (`SearchRoomsRequest`, `SearchRoomsResponse`, `RoomSearchHit`).

- [ ] **Step 1: Append block** documenting:
  - Request: `searchText` (required), `scope` (optional: `all` (default) / `channel` / `dm`; `app` reserved/rejected), `size`, `offset`.
  - Response: `total`, `results[]` of `RoomSearchHit`.
  - No triggered events.

- [ ] **Step 2: Validate, commit**

```bash
git add docs/client-api.md && git commit -m "docs(client-api): document Search Rooms"
```

After Task 23:

```bash
grep -c "_(filled in by Tasks 22–23)_" docs/client-api.md
```

Expected: `0`.

---

## Task 24: §4 — Send Message (publish + async reply)

**Files:**
- Modify: `docs/client-api.md` — replace placeholder under `## 4. Message Send`.

**Source of truth to read first:**

- `pkg/subject/subject.go:36-38` (`MsgSend`) — subject template.
- `pkg/subject/subject.go:48-50` (`UserResponse`) — async reply subject.
- `pkg/model/message.go:25-33` (`SendMessageRequest`) — request schema. Note `requestId` is the value the server uses for the reply subject.
- `message-gatekeeper/handler.go:36-104` — the gatekeeper's flow: parse subject, validate request, publish canonical event, send reply. Read carefully to understand the success reply payload (the gatekeeper acknowledges with the canonical message; check the exact reply struct or whether it returns the same `SendMessageRequest` echoed back — read the handler).
- The gatekeeper publishes to `chat.msg.canonical.{siteID}.created` (backend-only, not exposed to client). Downstream events the client receives:
  - `chat.room.{roomID}.event` (broadcast-worker fans out a `RoomEvent` to the room) — see `pkg/model/event.go:120-145` for `RoomEvent`.
  - `chat.user.{account}.notification` (notification-worker, only when applicable) — see `pkg/model/event.go:70-75` for `NotificationEvent`.
  - For DMs, `chat.user.{account}.stream.msg` — verify by reading `broadcast-worker/handler.go`.
- For the error path: the gatekeeper replies on `chat.user.{account}.response.{requestID}` with the standard error envelope when validation fails (see `message-gatekeeper/handler.go:104` (`respSubj := subject.UserResponse(account, req.RequestID)`)).

- [ ] **Step 1: Read the gatekeeper handler thoroughly and verify the actual reply payload structure**

```bash
sed -n '50,200p' /home/user/chat/message-gatekeeper/handler.go
```

Note the exact JSON the gatekeeper publishes to `chat.user.{account}.response.{requestID}` on success.

- [ ] **Step 2: Replace the placeholder with the Send Message block**

Use the canonical block format. Include three request example payloads side-by-side under "Request body":

1. **Plain message** — only `id`, `content`, `requestId`.
2. **Thread reply** — adds `threadParentMessageId` and `threadParentMessageCreatedAt`.
3. **Quoted message** — adds `quotedParentMessageId`.

The "Reply subject" line is exactly:

```
**Reply subject:** `chat.user.{account}.response.{requestID}` — the client must subscribe to `chat.user.{account}.>` (the user wildcard) to receive it. The `{requestID}` value is the `requestId` field from the request body.
```

Under "Triggered events — success path", document:

- `chat.room.{roomID}.event` with the full `RoomEvent` schema and an example. Recipients: every client subscribed to the room.
- `chat.user.{recipient}.notification` with the full `NotificationEvent` schema and an example. Recipients: clients with applicable mentions / DM recipients.

Under "Triggered events — error path":

- `chat.user.{account}.response.{requestID}` with the error envelope. Recipient: the sender only.

- [ ] **Step 3: Validate every JSON example with `jq`** (request × 3, success reply, room event, notification event, error reply).

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): document Send Message (plain, thread, quoted)"
```

---

## Task 25: Cross-link from README and nats-subject-naming.md

**Files:**
- Modify: `README.md`
- Modify: `docs/nats-subject-naming.md`

- [ ] **Step 1: Add a link from README.md**

Find the existing "Documentation" section in `README.md` (or, if absent, add one near the top). Add a bullet:

```markdown
- [Client API Reference](./docs/client-api.md) — integrator-facing reference for HTTP and NATS APIs.
```

If the README has no docs section, append at the end (before any trailing badges or boilerplate):

```markdown
## Documentation

- [Client API Reference](./docs/client-api.md) — integrator-facing reference for HTTP and NATS APIs.
- [NATS Subject Naming](./docs/nats-subject-naming.md) — full subject hierarchy (backend developer reference).
- [Cassandra Message Model](./docs/cassandra_message_model.md) — message-history schema.
```

- [ ] **Step 2: Add a back-link from nats-subject-naming.md**

At the very top of `docs/nats-subject-naming.md`, immediately after the `# NATS Subject Naming Design` heading, add:

```markdown
> Looking for the **client integrator reference**? See [`client-api.md`](./client-api.md). This document is the authoritative subject-naming spec used by backend developers.
```

- [ ] **Step 3: Verify edits**

```bash
grep -c "client-api.md" README.md
grep -c "client-api.md" docs/nats-subject-naming.md
```

Expected: at least `1` for each.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/nats-subject-naming.md
git commit -m "docs: cross-link client-api.md from README and subject-naming doc"
```

---

## Task 26: Add maintenance rule to CLAUDE.md

**Files:**
- Modify: `CLAUDE.md` — add a bullet under "Section 5: Workflow Guardrails" → "Before Committing".

- [ ] **Step 1: Find the right insertion point**

Open `CLAUDE.md`. In Section 5, locate the "Before Committing" subsection. The current bullets begin with "Run `make generate` first…". Add a new bullet at the end of that list:

```markdown
- If your changes touch a client-facing handler (any handler registered with `nc.QueueSubscribe` or `natsrouter.Register` whose subject begins with `chat.user.{account}.request.…` or `chat.user.{account}.room.{roomID}.{siteID}.msg.send`, or any HTTP route in `auth-service`), update `docs/client-api.md` in the same PR to reflect the new request/response schema, error cases, and triggered events.
```

- [ ] **Step 2: Verify**

```bash
grep -n "client-api.md" CLAUDE.md
```

Expected: at least one match in Section 5.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(claude): require client-api.md updates when client handlers change"
```

---

## Task 27: Final spot-check verification

This task validates the finished doc end-to-end.

- [ ] **Step 1: All placeholders are gone**

```bash
grep -nE "_\(filled in by Task" docs/client-api.md
```

Expected: no output (exit code 1).

- [ ] **Step 2: Every NATS subject in the doc has a corresponding subject builder**

Extract every backtick-quoted subject from the doc that starts with `chat.` and verify each pattern matches a function in `pkg/subject/subject.go`:

```bash
grep -oE "chat\.[a-zA-Z0-9_.{}*]+" docs/client-api.md | sort -u
grep -oE 'chat\.[a-zA-Z0-9_.%{}]+' /home/user/chat/pkg/subject/subject.go | sort -u
```

By eye, confirm every distinct subject pattern in the doc is producible from a builder. Treat `{account}`, `{roomID}`, `{siteID}`, `{requestID}`, `{orgID}` placeholders as matching the builder's `%s` slots.

- [ ] **Step 3: Every example JSON parses**

```bash
awk '/^```json$/{flag=1; buf=""; next} /^```$/{ if(flag){ print buf | "jq -e . > /dev/null && echo OK || echo FAIL"; close("jq -e . > /dev/null && echo OK || echo FAIL")}; flag=0; next } flag{buf = buf "\n" $0}' docs/client-api.md
```

Expected: every line says `OK`. Investigate any `FAIL`.

- [ ] **Step 4: Spot-check three method blocks live**

Pick three random method blocks (e.g. List Rooms, Search Messages, Edit Message). For each, manually:

1. Stand up the local stack (`docker compose -f deploy/docker-compose.yml up -d` if applicable per service).
2. Send the documented request via `nats req` (or the `tools/nats-debug` web UI) using the documented payload.
3. Confirm the reply matches the documented success-response schema.

Document any mismatch. Stop and fix the doc (or fix the handler if a documented behavior is the desired contract). This step is the manual quality gate the spec calls for.

- [ ] **Step 5: Commit any fixes**

If Steps 3–4 surface any documentation defects, edit `docs/client-api.md` and commit:

```bash
git add docs/client-api.md
git commit -m "docs(client-api): fix discrepancies found in spot-check"
```

If no fixes needed, skip this step.

---

## Done

After Task 27, the doc is complete:

- `docs/client-api.md` covers HTTP `POST /auth`, all 18 NATS request/reply methods, and the message-send publish flow, each with subject, request body, success/error responses, and triggered events.
- README and `nats-subject-naming.md` link to it.
- `CLAUDE.md` requires future PRs that change client handlers to update the doc.

Final push:

```bash
git push -u origin claude/document-api-methods-X8q0D
```
