# Chat Backend — Client API Reference

> [!IMPORTANT]
> **Changelog — centralized error codes (current release).** All client-facing
> errors — over NATS sync replies, JetStream async results (`model.AsyncJobResult`),
> and HTTP — now use the same envelope: `{ "error": <message>, "code": <category>,
> "reason"?: <domain-code>, "metadata"?: {…} }`. `code` is **always present** and
> drives HTTP status (see §6); `reason` is the optional domain code the client
> branches on (`reason ?? code`). Three notable behavior changes:
>
> 1. **`POST /auth` 500** now returns `{ "code": "internal", "error": "internal error" }` —
>    the real signing-failure cause is logged server-side and never sent to the client.
> 2. **room-service DM-exists** flipped from the legacy error-shaped envelope
>    `{ "error": "dm already exists", "roomId": … }` to a SUCCESS reply
>    `{ "status": "exists", "roomId": … }`. Clients must route on `status === "exists"`.
>    A frontend predicate `isDMExistsReply` accepts BOTH shapes during the rollout
>    window; the legacy fallback is removed in a follow-up.
> 3. **message-gatekeeper `not_subscribed`** now carries `code: forbidden` + `reason: not_subscribed`
>    (and `large_room_post_restricted` is `forbidden` + that reason) instead of bare error strings.

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
   - [2.3 HTTP — Protected image upload/download](#23-http--protected-image-uploaddownload)
3. [Request/Reply Methods](#3-requestreply-methods)
   - [3.0 Shared schemas](#30-shared-schemas)
   - [3.1 room-service](#31-room-service)
     - [Create Room](#create-room) · [Add Members](#add-members) · [Remove Member](#remove-member) · [Update Member Role](#update-member-role) · [Rename Room](#rename-room)
     - [List Members](#list-members) · [Get Member Statuses](#get-member-statuses) · [Get Mentionable Subscriptions](#get-mentionable-subscriptions) · [List Org Members](#list-org-members)
     - [Mark Messages Read](#mark-messages-read) · [Mark Thread as Read](#mark-thread-as-read) · [Read Message Receipts](#read-message-receipts) · [Toggle Mute](#toggle-mute) · [Toggle Favorite](#toggle-favorite)
     - [Get Room App Tabs](#get-room-app-tabs) · [Get Room App Command Menu](#get-room-app-command-menu)
   - [3.2 history-service](#32-history-service)
     - [Load History](#load-history) · [Load Next Messages](#load-next-messages) · [Load Surrounding Messages](#load-surrounding-messages) · [Get Message By ID](#get-message-by-id)
     - [Edit Message](#edit-message) · [Delete Message](#delete-message) · [Pin Message](#pin-message) · [Unpin Message](#unpin-message) · [List Pinned Messages](#list-pinned-messages) · [React to Message](#react-to-message)
     - [Get Thread Messages](#get-thread-messages) · [Get Thread Parent Messages](#get-thread-parent-messages)
   - [3.3 search-service](#33-search-service)
     - [`search.messages`](#searchmessages--full-text-message-search) · [Search Rooms](#search-rooms) · [Search Apps](#search-apps) · [Search Users](#search-users)
4. [Message Send](#4-message-send)
5. [Room Encryption](#5-room-encryption)
6. [Error envelope reference](#6-error-envelope-reference)
7. [Presence](#7-presence)

---

## 1. Overview

This doc covers the public client-facing API surface only.

**Out of scope (backend-internal — clients never see these):**

- Backend-only JetStream subjects (MESSAGES, MESSAGES_CANONICAL, OUTBOX, INBOX, ROOMS streams).
- Server-to-server subjects (`chat.server.request.…`).

Room-encryption key events that clients consume are documented under the RPC that triggers them (Create Room, Add Members, Remove Member) and in [§5 Room Encryption](#5-room-encryption). Multi-site federation is transparent to clients: a cross-site action delivers the **same** events on the same `chat.user.{account}.…` / `chat.room.…` subjects as a same-site action, so this doc does not distinguish them.

### Subject placeholders

Subjects in this doc use these placeholders:

| Placeholder | Meaning |
|---|---|
| `{account}` | The user's NATS account (preferred username from SSO claims, e.g. `alice`). |
| `{roomID}` | A room ID (see "ID formats" below). |
| `{siteID}` | The site that owns the room (each site runs its own NATS). |
| `{requestID}` | A 36-char hyphenated UUIDv7 generated by the client for the message-send async-reply pattern. |

### Encoding

All NATS payloads are JSON. All HTTP request/response bodies are JSON.

### ID formats

| ID kind | Format | Length | Notes |
|---|---|---|---|
| Account | Lowercase string | variable | SSO-derived; appears as `{account}` in subjects. |
| Channel room ID | base62 | 17 chars | Generated server-side at room creation. |
| DM room ID | sorted concat of two accounts | ~`len(a)+3+len(b)` | Deterministic; same two users always produce the same ID. |
| Message ID | base62 | 17 or 20 chars | New messages are 20-char; 17-char accepted for legacy/federated messages. |
| Request ID | hyphenated UUIDv7 | 36 chars | Both inbound `X-Request-ID` headers and the `requestId` payload field for `msg.send`. |

### Request-ID propagation

Clients **may** include an `X-Request-ID` NATS message header on outbound requests. If present, the server uses it for log correlation; if absent, the server generates one. The header value must be a valid hyphenated UUID (v4 or v7, case-insensitive).

The `msg.send` flow is different — see [§4](#4-message-send): the client puts the request ID in the JSON payload (`requestId` field), and the server replies on `chat.user.{account}.response.{requestID}`.

### Debug tracing (`X-Debug`)

Clients **may** include an optional `X-Debug` NATS header to turn on verbose, server-side, per-request tracing for a **single** request — useful when diagnosing "what happened to my message?". It changes neither the request schema, the reply, nor any triggered event; output is written only to the server logs, joinable by `request_id`.

| Value | Effect |
|-------|--------|
| _(absent)_ / `0` / `false` / `off` | Off — the default. Any unrecognized value is also treated as off. |
| `flow` | Cross-service path + timing breadcrumbs (how far the request got, where latency was spent). |
| `debug` (also `1` / `true` / `on`) | Adds in-service decision detail. |
| `trace` | Adds per-item / per-recipient detail (most verbose). |

Each rung includes the ones below it. The header propagates across every service the request touches. It is **best-effort and rate-limited** per server instance: under load, verbose output for some flagged requests may be dropped (the request itself is unaffected). Treat it as a diagnostic aid, not a guaranteed log.

### Reply patterns

- **Standard NATS request/reply** — the NATS client library auto-generates a reply subject under `_INBOX.>` and routes the reply back to the caller. Used by every method in §3.
- **Async reply on `chat.user.{account}.response.{requestID}`** — used only by `msg.send` (§4). The client publishes (no synchronous reply expected on `_INBOX.>`); the server reads `requestId` from the payload and publishes the reply to `chat.user.{account}.response.{requestID}`. The client must already be subscribed to `chat.user.{account}.>` (the user wildcard) to receive it.

### Timestamps

All event payloads carry a top-level `timestamp` field that is **milliseconds since the Unix epoch in UTC** — abbreviated `Epoch ms (UTC)` in the field tables below. Domain timestamps inside payloads (e.g. `Message.createdAt`) are RFC 3339 strings.

---

## 2. Connection & Auth

### 2.1 NATS connection

A client connects to NATS using a user NKey pair plus a signed JWT obtained from the auth-service (§2.2). The JWT scopes the client's permissions to:

| Permission | Subject pattern | Why |
|---|---|---|
| Publish | `chat.user.{account}.>` | The client may publish only under its own user namespace. All RPC requests, the message-send subject, and any client-emitted event fall here. |
| Publish | `_INBOX.>` | Required for the standard NATS request/reply pattern (the auto-generated reply inbox). |
| Subscribe | `chat.user.{account}.>` | Receives all responses, notifications, and per-user events. |
| Subscribe | `chat.room.>` | Subscribes to per-room message streams and room events for any room the user belongs to. |
| Subscribe | `_INBOX.>` | Required to receive replies to client-issued requests. |

**Recommended baseline subscriptions on connect:**

- `chat.user.{account}.>` — captures every personal event including async replies, per-user room events (DM messages, edits, deletes), room-key events, and subscription updates.
- `chat.room.{roomID}.event` for each channel room in the user's sidebar — receives new messages plus edit/delete events for that channel.

The exact event subjects a client may receive as a result of an RPC are listed under each method's "Triggered events" sections in §2.2, §3, and §4.

### 2.2 HTTP — POST /auth

**Endpoint:** `POST /auth`
**Reply:** synchronous HTTP response

Exchanges an SSO token for a signed NATS user JWT. The returned JWT is what the client uses to connect to NATS (see §2.1).

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `ssoToken` | string | yes | OIDC-issued SSO token. |
| `natsPublicKey` | string | yes | The client's NATS user public NKey (must pass `nkeys.IsValidPublicUserKey`). |

```json
{
  "ssoToken": "<sso-token>",
  "natsPublicKey": "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4"
}
```

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `natsJwt` | string | Signed NATS user JWT. Use as the user JWT when connecting to NATS. |
| `user.email` | string | OIDC email claim. |
| `user.account` | string | The `{account}` value used in every NATS subject. Derived from `preferred_username` (falls back to `name`). |
| `user.employeeId` | string | Parsed from the SSO `description` claim. |
| `user.engName` | string | Parsed from the SSO `description` claim. |
| `user.chineseName` | string | Parsed from the SSO `description` claim. |
| `user.deptName` | string | OIDC dept-name claim. |
| `user.deptId` | string | OIDC dept-id claim. |

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

See [Error envelope](#6-error-envelope-reference). HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "ssoToken and natsPublicKey are required" }` |
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "invalid natsPublicKey format" }` |
| 401 | `unauthenticated` | `sso_token_expired` | `{ "code": "unauthenticated", "reason": "sso_token_expired", "error": "SSO token has expired, please re-login" }` |
| 401 | `unauthenticated` | `invalid_sso_token` | `{ "code": "unauthenticated", "reason": "invalid_sso_token", "error": "invalid SSO token" }` |
| 500 | `internal` | — | `{ "code": "internal", "error": "internal error" }` — the real cause is logged server-side and never sent to the client. |

The returned `natsJwt` has a server-configured lifetime (default 2h). Clients should re-call `POST /auth` to refresh before it expires.

> **Background renewal.** The web client also calls `POST /auth` periodically to
> renew the NATS user JWT before it expires (at ~80% of the token's lifetime,
> jittered). It obtains a fresh SSO access token in the background via the OIDC
> refresh token (silent renew) and re-mints with the **same** `natsPublicKey`,
> so the request/response schema is identical to the initial login call. When
> silent renewal fails (the SSO session has ended), the client performs a
> graceful re-login redirect instead.

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

### 2.3 HTTP — Protected image upload/download

Two HTTP endpoints on `upload-service` for protected inline images, proxied
to/from an internal Drive. Both require the `ssoToken` header (validated via
OIDC) and that the caller is a member (has a subscription) of `:roomId`. Errors
use the standard [§6](#6-error-envelope-reference) envelope `{ code, reason?, error }`.

#### POST /api/v1/rooms/:roomId/upload/images

**Endpoint:** `POST /api/v1/rooms/:roomId/upload/images`
**Reply:** synchronous HTTP response

Uploads one or more images for a room on behalf of the authenticated user. Each
file is validated independently and the response reports per-file
success/failure in a single `200` (partial success).

#### Request

`Content-Type: multipart/form-data`

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `ssoToken` | header | string | yes | OIDC-issued SSO token; identifies the uploader. |
| `roomId` | path | string | yes | Target room ID; the caller must be a member. |
| `images` | form file | file[] | yes | One or more images (`.png`/`.jpeg`/`.jpg`/`.heic`), each ≤ `MAX_IMAGE_SIZE_BYTES` (default 25 MiB); at most `MAX_FILES` (default 10). Repeat the field once per file. |

#### Success response

`HTTP 200` — one `results` entry per submitted file (successes and failures together).

| Field | Type | Notes |
|---|---|---|
| `results` | [UploadResult](#uploadresult)[] | Per-file outcome. |

##### UploadResult

| Field | Type | Notes |
|---|---|---|
| `name` | string | The file name. |
| `status` | string | `Success` for an uploaded file, `failure` for a rejected one. |
| `error` | string | Present on failure: `file size exceeds limit`, `file has an invalid file type`, or `failed to open file`. |
| `relativePath` | string | Present on success: path to download the image via the GET endpoint below, including the `drive_host` query param. |

```json
{
  "results": [
    { "name": "pic1.png", "status": "Success", "relativePath": "api/v1/rooms/abc123/image/img-xyz?drive_host=https://drive.example.com" },
    { "name": "big.exe", "status": "failure", "error": "file has an invalid file type" }
  ]
}
```

#### Error response

A whole-request failure (not a per-file rejection) uses the
[§6](#6-error-envelope-reference) envelope. HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "too many files" }` — also `roomId is required`, `request must be multipart/form-data`. |
| 401 | `unauthenticated` | `invalid_sso_token` / `sso_token_expired` / `missing_fields` | `{ "code": "unauthenticated", "reason": "invalid_sso_token", "error": "invalid sso token" }` |
| 403 | `forbidden` | `not_room_member` | `{ "code": "forbidden", "reason": "not_room_member", "error": "user alice is not in room abc123" }` |
| 404 | `not_found` | — | `{ "code": "not_found", "error": "room not found" }` |
| 500 | `internal` | — | `{ "code": "internal", "error": "internal error" }` — user missing in context, no email on the account, or a Drive/store fault; real cause logged server-side only. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

#### GET /api/v1/rooms/:roomId/image/:fileId

**Endpoint:** `GET /api/v1/rooms/:roomId/image/:fileId`
**Reply:** synchronous HTTP response (raw image bytes, not JSON)

Downloads a protected image. The service proxies the bytes from Drive: it
fetches a signed URL, streams the body, and pipes it straight back. Typically
called with the `relativePath` returned by the upload endpoint.

#### Request

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `ssoToken` | header | string | yes | OIDC-issued SSO token. |
| `roomId` | path | string | yes | Room the image belongs to; the caller must be a member. |
| `fileId` | path | string | yes | Drive file ID (from the upload response). |
| `drive_host` | query | string | yes | Drive base URL (from the upload response). |

#### Success response

`HTTP 200` — raw image binary streamed directly (not JSON), with the upstream
`Content-Type` (defaulting to `application/octet-stream`).

#### Error response

See [Error envelope](#6-error-envelope-reference). HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "drive_host is required" }` — also `roomId is required`, `fileId is required`. |
| 401 | `unauthenticated` | `invalid_sso_token` / `sso_token_expired` / `missing_fields` | `{ "code": "unauthenticated", "reason": "invalid_sso_token", "error": "invalid sso token" }` |
| 403 | `forbidden` | `not_room_member` | `{ "code": "forbidden", "reason": "not_room_member", "error": "user alice is not in room abc123" }` |
| 500 | `internal` | — | `{ "code": "internal", "error": "internal error" }` — user missing in context. |
| 503 | `unavailable` | — | `{ "code": "unavailable", "error": "failed to retrieve image" }` — Drive signer/download failure. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

## 3. Request/Reply Methods

### 3.0 Shared schemas

Reusable payload types referenced by name throughout §3–§5. Each RPC links
here instead of repeating the table.

#### Participant

Actor / sender identity embedded in room and message events.

| Field | Type | Notes |
|---|---|---|
| `userId` | string | Optional. Internal user ID. |
| `account` | string | The user's account name. |
| `siteId` | string | Optional. The user's home site. |
| `chineseName` | string | Optional. Chinese display name — omitted when unset. |
| `engName` | string | Optional. English display name — omitted when unset. |
| `displayName` | string | Optional. Server-composed render-ready name. |

#### SubscriptionUser

The `u` field on a [Subscription](#subscription) / [RemovedSubscriptionRef](#removedsubscriptionref).

| Field | Type | Notes |
|---|---|---|
| `id` | string | Internal user ID. |
| `account` | string | The user's account name. On org-removal paths only `account` is guaranteed. |
| `isBot` | boolean | True when the user is a bot. |

#### ChannelRef

Reference to another channel whose members are copied or added in bulk.

| Field | Type | Notes |
|---|---|---|
| `roomId` | string | The source channel's ID. |
| `siteId` | string | The source channel's home site. |

#### EncryptedMessage

Room ciphertext envelope (`roomcrypto.EncryptedMessage`). See [§5 Room Encryption](#5-room-encryption).

| Field | Type | Notes |
|---|---|---|
| `version` | integer | Room-key version used to seal the payload. |
| `nonce` | string | Base64-encoded nonce. |
| `ciphertext` | string | Base64-encoded ciphertext. |

#### Subscription

A user's membership record for one room, embedded in `subscription.update`
events on `added` / `role_updated` / `mute_toggled` / `favorite_toggled`. The
ID serializes as `id` (not `_id`) and the user under `u` (not `user`). The
first group is always present; the rest are optional (omitted when empty/unset).

| Field | Type | Notes |
|---|---|---|
| `id` | string | Subscription ID. |
| `u` | [SubscriptionUser](#subscriptionuser) | The subscribed user. |
| `roomId` | string | The room. |
| `siteId` | string | The room's home site. |
| `roomType` | string | `"channel"`, `"dm"`, `"botDM"`, or `"discussion"`. |
| `name` | string | The room's display name. |
| `roles` | string[] | The user's roles in the room (e.g. `["member"]`, `["owner"]`). |
| `joinedAt` | RFC3339 timestamp | When the user joined. |
| `hasMention` | boolean | Whether the user has an unread mention. |
| `alert` | boolean | Whether the room has an unread alert for the user. |
| `muted` | boolean | Whether the user muted the room. |
| `favorite` | boolean | Whether the user favorited the room. |
| `isSubscribed` | boolean | Optional. Whether the user is actively subscribed. |
| `historySharedSince` | RFC3339 timestamp | Optional. Boundary before which prior history is shared. |
| `lastSeenAt` | RFC3339 timestamp | Optional. The user's last-seen time in the room. |
| `threadUnread` | string[] | Optional. Thread room IDs with unread replies. |
| `restricted` | boolean | Optional. Denormalized room restricted flag. |
| `externalAccess` | boolean | Optional. Denormalized room external-access flag. |

#### HrInfo

HR display names.

| Field | Type | Notes |
|---|---|---|
| `engName` | string | English display name. |
| `chineseName` | string | Chinese display name. |

#### AppAssistant

An app's assistant (bot) subdocument. Only `name` is always present; the other
fields appear per endpoint.

| Field | Type | Notes |
|---|---|---|
| `name` | string | Assistant/bot name. |
| `enabled` | boolean | Optional. Whether the assistant is enabled. |
| `settingsUrl` | string | Optional. Assistant settings URL. |

#### AsyncJobResult

Delivered on `chat.user.{requesterAccount}.response.{requestID}` when an
async-job RPC finishes. Shared by Create Room, Add Members, Remove Member,
and Rename Room.

| Field | Type | Notes |
|---|---|---|
| `requestId` | string | Echoes the `X-Request-ID` value from the original request. |
| `operation` | string | One of `"room.create"`, `"room.member.add"`, `"room.member.remove"`, `"room.member.remove_org"`, `"room.rename"`. |
| `status` | string | `"ok"` or `"error"`. |
| `roomId` | string | Optional. The affected room. |
| `error` | string | Optional. User-safe message; present only when `status="error"`. |
| `code` | string | Optional. The errcode category (`bad_request`, `not_found`, `forbidden`, `conflict`, `internal`, …) — same closed set as sync replies (see §6). Present only when `status="error"`. |
| `reason` | string | Optional. Domain reason from `pkg/errcode/codes_room.go` (e.g. `not_room_member`, `max_room_size_reached`) when the frontend needs to distinguish cases. Present only when `status="error"` and a reason was attached server-side. |
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

### 3.1 room-service

| RPC subject | Method |
|---|---|
| `chat.user.{account}.request.room.{siteID}.create` | [Create Room](#create-room) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.member.add` | [Add Members](#add-members) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.member.remove` | [Remove Member](#remove-member) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.member.role-update` | [Update Member Role](#update-member-role) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.room.rename` | [Rename Room](#rename-room) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.member.list` | [List Members](#list-members) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.member.statuses` | [Get Member Statuses](#get-member-statuses) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.subscription.mentionable` | [Get Mentionable Subscriptions](#get-mentionable-subscriptions) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.message.read` | [Mark Messages Read](#mark-messages-read) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.message.thread.read` | [Mark Thread as Read](#mark-thread-as-read) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.mute.toggle` | [Toggle Mute](#toggle-mute) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.favorite.toggle` | [Toggle Favorite](#toggle-favorite) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.message.read-receipt` | [Read Message Receipts](#read-message-receipts) |
| `chat.user.{account}.request.orgs.{orgID}.{siteID}.members` | [List Org Members](#list-org-members) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.app.tabs` | [Get Room App Tabs](#get-room-app-tabs) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.app.cmd-menu` | [Get Room App Command Menu](#get-room-app-command-menu) |

#### Create Room

**Subject:** `chat.user.{account}.request.room.{siteID}.create`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This is an **async-job RPC**: the synchronous reply only confirms acceptance. The room is created asynchronously in `room-worker`, which publishes the events under "Triggered events" below. The client **must** set an `X-Request-ID` NATS header — Create Room rejects requests without it, and the header value is echoed as the `requestId` on the `AsyncJobResult` event.

The room **type is inferred server-side** from the payload shape — the client does not send it:

- `name` set → `channel`
- `name` empty + exactly one entry in `users` → `dm` (or `botDM` if that user is a bot)

The creator's account and the site come from the subject (`chat.user.{account}.request.room.{siteID}.create`); the client does not pass them in the body.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | channels | Channel name (≤ 100 chars). Required to create a channel; leave empty for a DM/botDM. |
| `users` | string[] | no | Internal user IDs (or accounts) to enroll. For a DM, exactly one entry (the other user). Channel creates reject a bot account here with `"bots cannot be added to a channel"`, and any account with no matching user document with `user "<account>": user not found`. |
| `orgs` | string[] | no | `channel` only. Org IDs to enroll (expanded server-side to all org members). Any entry matching zero users is rejected with `org "<orgId>": invalid org`. |
| `channels` | array<ChannelRef> | no | `channel` only. Other channels whose members are copied in. Each entry is `{ "roomId": string, "siteId": string }`. |

```json
{
  "name": "engineering-announcements",
  "users": ["bob"],
  "orgs": ["org-eng"],
  "channels": []
}
```

##### Success response

**Not** the full room — the room object is never returned; it is created asynchronously.

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"accepted"`. |
| `roomId` | string | The new room's ID. Channel: 17-char base62. DM/botDM: sorted concat of the two account IDs. |
| `roomType` | string | `channel`, `dm`, or `botDM`. |

```json
{ "status": "accepted", "roomId": "01970a4f8c2d7c9aQ", "roomType": "channel" }
```

**DM already exists.** When the client asks to create a DM/botDM that already exists, the reply is a SUCCESS reply carrying the existing room ID (open-or-create contract — the client opens the existing room):

```json
{ "status": "exists", "roomId": "<existing room id>" }
```

> [!WARNING]
> **Contract change (breaking):** prior to the centralized error-codes migration this case returned the *error*-shaped envelope `{ "error": "dm already exists", "roomId": "<existing room id>" }`. Clients on the new release must route on `status === "exists"`. During the rollout window, the frontend predicate `isDMExistsReply` accepts BOTH shapes; see the migration changelog at the top of this file.

##### Error response

See [Error envelope](#6-error-envelope-reference). Returned synchronously on validation/authorization failure:

- `"X-Request-ID header is required …"` (`bad_request`, reason `request_id_required`) — the `X-Request-ID` header is absent or not a valid hyphenated UUID
- `"request must include at least one of users, orgs, channels, or name"`
- `"channel name is required"` / `"channel name must be at most 100 characters"`
- `"cannot create a DM with yourself"`
- `"bots cannot be added to a channel"` / `"bot not available"` (botDM target whose assistant is disabled)
- `user "<account>": user not found` / `org "<orgId>": invalid org` (each wrapped with the offending account/org ID)
- `"user is missing required name fields"`
- `"exceeds maximum capacity (N): would create M members"`

```json
{ "code": "bad_request", "error": "channel name is required" }
```

##### Triggered events — success path

The creator (from the subject) plus any members supplied via `users` / `orgs` / `channels` are enrolled asynchronously. The following events fire:

**1. `chat.user.{account}.response.{requestID}`** — an `AsyncJobResult` to the requester when the job finishes (requires the `X-Request-ID` header). See the [AsyncJobResult schema](#asyncjobresult). `operation` is `"room.create"`.

**2. `chat.user.{account}.event.subscription.update`** — one per enrolled member (including the owner), `action: "added"`. See the [subscription.update schema](#subscriptionupdate-event) under Add Members.

**3. `chat.user.{account}.event.room.key`** — **channel rooms only:** one `RoomKeyEvent` per enrolled local member. DM/botDM rooms are not encrypted and emit no key event. See [§5 Room Encryption](#5-room-encryption).

For **channel** rooms, the first messages (`type: "room_created"`, then `type: "members_added"` when initial members were enrolled) flow through the normal message pipeline and arrive as `new_message` room events (see [§4](#4-message-send)).

##### Triggered events — error path

`None for synchronous rejections.` If the async job fails after `accepted`, the requester receives an `AsyncJobResult` with `status: "error"`.

---

#### Add Members

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.add`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This is an **async-job RPC**: the synchronous reply only confirms acceptance. The actual member adds run asynchronously in `room-worker`, which publishes the events listed under "Triggered events" below. To receive the `AsyncJobResult` event, the client **must** set an `X-Request-ID` NATS header on the original request (see [Request-ID propagation](#request-id-propagation)).

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `roomId` | string | no | Optional; the server derives the room ID from the subject and ignores any non-matching value. |
| `users` | string[] | no | Internal user IDs (or accounts) to add directly. |
| `orgs` | string[] | no | Org IDs to add (expanded server-side to all org members). |
| `channels` | array<ChannelRef> | no | Other channels to add as bulk sources. Each entry is `{ "roomId": string, "siteId": string }`. |
| `history.mode` | string | no | `"none"` (default) or `"all"` — controls whether new members see history before they joined. |

The fields `requesterId`, `requesterAccount`, and `timestamp` on the Go `AddMembersRequest` are server-set — the client should omit them.

```json
{
  "users": ["bob"],
  "orgs": [],
  "channels": [
    { "roomId": "01970a4f8c2d7c9aP", "siteId": "siteA" }
  ],
  "history": { "mode": "all" }
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"accepted"`. Confirms the request passed authorization and was queued for processing. |

```json
{ "status": "accepted" }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Returned synchronously when validation or authorization fails (e.g. requester not in room, room is full, room is restricted and requester is not owner, or a `users` entry is a bot — rejected with `"bots cannot be added to a channel"`). Any `orgs` entry that matches zero users (no user with `sectId == orgId` or `deptId == orgId`) is rejected with `org "<orgId>": invalid org`, and any `users` entry that has no matching user document is rejected with `user "<account>": user not found` (each wrapped with the offending account/org ID) — in both cases the request is not queued and no members are added.

```json
{ "code": "conflict", "reason": "max_room_size_reached", "error": "room is at maximum capacity" }
```

##### Triggered events — success path

**1. `chat.user.{requesterAccount}.response.{requestID}`** — an [`AsyncJobResult`](#asyncjobresult) delivered to the **requester** when the bulk add finishes. Only published if the client set `X-Request-ID` on the original request. `operation` is `"room.member.add"`.

Error example (e.g. requester not in room):

```json
{
  "requestId": "01970a4f-8c2d-7c9a-abcd-e0123456789f",
  "operation": "room.member.add",
  "status": "error",
  "error": "only room members can list members",
  "code": "forbidden",
  "reason": "not_room_member",
  "timestamp": 1746518400456
}
```

**2. `chat.user.{newMember}.event.subscription.update`** — one event per **newly subscribed** member (not the requester, not existing members, not org→individual upgrades).

###### `subscription.update` event

Shared by Add Members, Remove Member, and Update Member Role.

| Field | Type | Notes |
|---|---|---|
| `userId` | string | The affected user's internal user ID. Omitted on the org-removal path (only `subscription.u.account` is set there). |
| `subscription` | [Subscription](#subscription) | For `added` / `role_updated`: the full Subscription record. For `removed`: a [RemovedSubscriptionRef](#removedsubscriptionref) lean ref (see Remove Member). |
| `action` | string | `"added"`, `"removed"`, `"role_updated"`, `"mute_toggled"`, or `"favorite_toggled"`. |
| `timestamp` | number | Epoch ms (UTC). |

On `added` / `role_updated` / `mute_toggled` / `favorite_toggled` the embedded `Subscription` serializes its ID as `id` (not `_id`) and the user under `u` (not `user`). Non-`omitempty` fields (`id`, `u`, `roomId`, `siteId`, `roles`, `name`, `roomType`, `joinedAt`, `hasMention`, `alert`, `muted`, `favorite`) are always present. `removed` events use a dedicated lean payload (`SubscriptionRemovedEvent`) whose `subscription` carries **only** `roomId`, `roomType`, and `u` — no zero-valued `Subscription` fields are sent.

```json
{
  "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "subscription": {
    "id": "01970a4f8c2d7c9a01970a4f8c2d7c9b",
    "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "bob" },
    "roomId": "01970a4f8c2d7c9aQ",
    "roomType": "channel",
    "siteId": "siteA",
    "roles": ["member"],
    "joinedAt": "2026-05-06T08:01:23Z"
  },
  "action": "added",
  "timestamp": 1746518483000
}
```

**3. `chat.user.{newMember}.event.room.key`** — a `RoomKeyEvent` per newly-subscribed account (channels). Existing members do not receive a duplicate. See [§5 Room Encryption](#5-room-encryption).

**4. `chat.room.{roomID}.event.member`** — a `MemberAddEvent` (`type: "member_added"`) published once when at least one new account or org was added. Delivered to clients subscribed to `chat.room.>` for the room.

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
| `historySharedSince` | number | Optional. Epoch ms (UTC); present when prior history is shared with the new members. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |

A `members_added` system message also flows through the message pipeline and arrives as a `new_message` room event.

> [!NOTE]
> **No-op:** if every requested account is already a member (and no new orgs), or the add only upgrades an existing org member to an individual membership, the requester still gets an `AsyncJobResult` with `status: "ok"` but **no** `subscription.update` / `room.key` / `member_added` events follow.

##### Triggered events — error path

`None for synchronous rejections.` If the async job fails after `accepted`, the requester receives the `AsyncJobResult` with `status: "error"`.

---

#### Remove Member

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.remove`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This is an **async-job RPC**: the synchronous reply only confirms acceptance. The actual member removal runs asynchronously in `room-worker`, which publishes the events listed under "Triggered events" below. To receive the `AsyncJobResult` event, the client **must** set an `X-Request-ID` NATS header on the original request (see [Request-ID propagation](#request-id-propagation)).

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `account` | string | no | Remove a single user. Mutually exclusive with `orgId`. |
| `orgId` | string | no | Remove all users in this org. Mutually exclusive with `account`. |
| `roomId` | string | no | Server derives from subject; non-matching values are rejected. |

Exactly one of `account` or `orgId` must be set. The fields `requester`, `roomType`, and `timestamp` on the Go `RemoveMemberRequest` are server-set — the client should omit them.

```json
{ "account": "bob" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"accepted"`. Confirms the request passed authorization and was queued for processing. |

```json
{ "status": "accepted" }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Returned synchronously when validation or authorization fails (e.g. neither or both of `account`/`orgId` set, requester is not an owner, target is the last member, or org member cannot leave individually).

```json
{ "code": "bad_request", "error": "exactly one of account or orgId must be set" }
```

##### Triggered events — success path

**1. `chat.user.{requesterAccount}.response.{requestID}`** — an [`AsyncJobResult`](#asyncjobresult) to the requester when the removal finishes (requires `X-Request-ID`). `operation` is `"room.member.remove"` for single-account removal, `"room.member.remove_org"` for org removal.

**2. `chat.user.{removedAccount}.event.subscription.update`** — one per removed account, `action: "removed"`. The payload is a dedicated `SubscriptionRemovedEvent` (not the full `SubscriptionUpdateEvent`): its `subscription` carries **only** `roomId`, `roomType`, and `u` so no zero-valued `Subscription` fields are sent. On the **org-removal** path `userId` is omitted (only `subscription.u.account` is set).

| Field | Type | Notes |
|---|---|---|
| `userId` | string | The removed user's internal user ID. Omitted on the org-removal path (only `subscription.u.account` is set there). |
| `subscription` | [RemovedSubscriptionRef](#removedsubscriptionref) | Lean ref — no full `Subscription` payload. |
| `action` | string | Always `"removed"`. |
| `timestamp` | number | Epoch ms (UTC). |

###### RemovedSubscriptionRef

| Field | Type | Notes |
|---|---|---|
| `roomId` | string | The room the user lost. |
| `roomType` | string | `"channel"`, `"dm"`, `"botDM"`, or `"discussion"`. |
| `u` | [SubscriptionUser](#subscriptionuser) | The removed user. On org removals only `account` is guaranteed. |

```json
{
  "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "subscription": {
    "roomId": "01970a4f8c2d7c9aQ",
    "roomType": "channel",
    "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "bob" }
  },
  "action": "removed",
  "timestamp": 1746518483000
}
```

**3. `chat.user.{survivor}.event.room.key`** — on a channel removal the room key is **rotated**; every surviving member receives a new `RoomKeyEvent` with an incremented `version`. The removed account stops receiving key events. See [§5 Room Encryption](#5-room-encryption).

**4. `chat.room.{roomID}.event.member`** — a `MemberRemoveEvent` (`type: "member_left"` for a self-leave, `"member_removed"` for a forced removal or org removal). Delivered to clients subscribed to `chat.room.>`.

| Field | Type | Notes |
|---|---|---|
| `type` | string | `"member_left"` (self-leave) or `"member_removed"` (forced removal or org removal). |
| `roomId` | string | |
| `accounts` | string[] | The removed accounts. |
| `siteId` | string | The room's home site. |
| `orgId` | string | Present only on org removals. Omitted otherwise. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |

A `member_left` / `member_removed` system message also flows through the message pipeline as a `new_message` room event.

> [!NOTE]
> **No-op:** removals that change no membership (e.g. an org member who still has an individual membership, or an org removal where every member is covered by another membership) still return an `AsyncJobResult` `ok` with no follow-on events.

##### Triggered events — error path

`None for synchronous rejections.` If the async job fails after `accepted`, the requester receives the `AsyncJobResult` with `status: "error"`.

---

#### Update Member Role

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.role-update`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This is a **synchronous RPC**. `room-service` validates the request, applies the role change atomically, emits the event below, and only then returns the reply. There is no async job, no `AsyncJobResult`, and no `chat.user.{requesterAccount}.response.{requestID}` event for this RPC.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `roomId` | string | no | Server derives from subject; non-matching values are rejected. |
| `account` | string | yes | The account of the user whose role is being changed. |
| `newRole` | string | yes | Either `"owner"` (promote) or `"member"` (demote). |

The `timestamp` field on the Go `UpdateRoleRequest` is server-set — the client should omit it.

```json
{ "account": "bob", "newRole": "owner" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"ok"`. Confirms the role change was applied. |

```json
{ "status": "ok" }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Returned synchronously when validation, authorization, or the role mutation fails. Common errors include:

- Requester is not an owner of the room.
- Target account is not a member of the room.
- `newRole` is neither `"owner"` nor `"member"`.
- Promote attempt when the target is already an owner.
- Demote attempt when the target is not an owner.
- Last-owner guard: an owner cannot demote themselves if they are the only owner.
- Promote attempt on an org-only member (individual subscription required).

```json
{ "code": "forbidden", "error": "only owners can update roles" }
```

##### Triggered events — success path

**`chat.user.{targetAccount}.event.subscription.update`** — emitted once for the user whose role changed, `action: "role_updated"`. Delivered to the target user only (not the requester, not other members). See the [subscription.update schema](#subscriptionupdate-event); the embedded `Subscription` reflects the updated `roles`. No `AsyncJobResult` and no room-key event fire for role updates.

```json
{
  "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "subscription": {
    "id": "01970a4f8c2d7c9a01970a4f8c2d7c9b",
    "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "bob" },
    "roomId": "01970a4f8c2d7c9aQ",
    "roomType": "channel",
    "siteId": "siteA",
    "roles": ["member", "owner"],
    "joinedAt": "2026-05-06T08:01:23Z"
  },
  "action": "role_updated",
  "timestamp": 1746518483000
}
```

##### Triggered events — error path

When the reply is an error envelope, no events follow. All validation and the role mutation happen synchronously in `room-service`, so any failure is surfaced directly in the error reply — there is no deferred worker step and no separate failure event.

---

#### Rename Room

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.room.rename`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

This is an **async-job RPC**: the synchronous reply only confirms acceptance. The actual rename runs asynchronously in `room-worker`, which publishes an `AsyncJobResult` on `chat.user.{requesterAccount}.response.{requestID}` when the job finishes. To receive this event the client **must** set an `X-Request-ID` NATS header on the original request (see [Request-ID propagation](#request-id-propagation)).

Only channel rooms may be renamed. This RPC may be called by a **platform admin** (`model.UserRoleAdmin`) or by a room member who holds the **room owner** (`model.RoleOwner`) role. Platform admins bypass the room-membership check entirely.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `newName` | string | yes | New room name. 1–100 characters after trimming whitespace. |

The room ID and requesting account are taken from the subject — clients do not include them in the body.

```json
{ "newName": "engineering-general" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"accepted"`. Confirms the request passed authorization and was queued for processing. |
| `requestId` | string | The 36-char hyphenated UUID from the required `X-Request-ID` header. Clients MUST supply a valid hyphenated UUID; missing or invalid header is rejected with the listed validation error. |

```json
{ "status": "accepted", "requestId": "01970a4f-8c2d-7c9a-abcd-e0123456789f" }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Returned synchronously when validation or authorization fails. Common errors:

- `"invalid name"` — `newName` is empty after trimming, or exceeds 100 characters.
- `"room not found"` — no room matches the subject `{roomID}`.
- `"rename is only allowed in channel rooms"` — the room is a DM, botDM, or discussion.
- `"only owners or platform admins can rename a channel"` — the requester is not a platform admin and does not hold the `owner` role in the room.
- `"invalid request payload"` — body is malformed (returned as `bad_request` by the router; previously surfaced as an internal error).
- `"X-Request-ID header is required …"` — `bad_request` (reason `request_id_required`); the `X-Request-ID` header is absent or not a valid hyphenated UUID.

```json
{
  "error": "rename is only allowed in channel rooms",
  "code": "bad_request",
  "reason": "non_channel_operation"
}
```

##### Triggered events — success path

**1. `chat.room.{roomID}.event`** — a `RoomRenamedRoomEvent` fanned out by `broadcast-worker` to every client subscribed to the room.

Recipients: all room members on all sites.

The event uses a **dedicated flat struct** (`type: "room_renamed"`) — mirroring the convention of `EditRoomEvent` / `DeleteRoomEvent` — so the wire payload carries no zero-valued `RoomEvent` base fields (no `userCount`, `lastMsgAt`, `message`, `mentions`, etc. on a rename).

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"room_renamed"`. |
| `roomId` | string | The renamed room. |
| `siteId` | string | Home site of the room. |
| `timestamp` | number | Publish time, milliseconds since Unix epoch (UTC). |
| `newName` | string | The new room name. |
| `byAccount` | string | The account that performed the rename (room owner or platform admin). |
| `renamedAt` | string | ISO-8601 timestamp of when the rename was applied (the source sys message's `createdAt`). |

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

> [!NOTE]
> **No per-subscription `subscription.update` event is published for the rename.** Clients drive their local subscription `name` update off this single room-scoped event.

**2. `chat.user.{requesterAccount}.response.{requestID}`** — an [`AsyncJobResult`](#asyncjobresult) to the requester when the rename finishes (requires `X-Request-ID`). `operation` is `"room.rename"`. `status` is `"ok"` on success or `"error"` if the async job fails.

**3. Outbox events** — one event per remote site that has federated members. Delivered via the `OUTBOX_{siteID}` → `INBOX_{remoteSiteID}` pipeline; remote `inbox-worker` mirrors the rename.

##### Triggered events — error path

When the synchronous reply is an error envelope, the request was rejected before publishing to the worker — no events follow.

---

> [!NOTE]
> **Server-internal — not a client RPC.** The "Set Room Restricted" RPC (formerly "Set Room Visibility") is admin-only and lives outside the client API surface. It is a **synchronous** server-to-server NATS request/reply on `chat.server.request.room.{siteID}.restricted`. Admin tooling sends a `RoomRestrictedRequest` (`pkg/model/room.go`) carrying:
>
> - `roomId` — channel room to mutate
> - `account` — the admin caller (used for the sys-message authorship + audit log)
> - `restricted` — whether the room is members-only; on the `false → true` transition `ownerAccount` is required and that account is promoted to sole owner
> - `externalAccess` — whether the room is reachable from outside the company network (e.g. internet-side / off-VPN clients). This is a network-access gate, NOT a cross-site federation flag
> - `ownerAccount` — required on the unrestricted-to-restricted transition
>
> room-service does the Mongo writes, fans out an `OutboxRoomRestricted` event per remote federated site, and replies `{"status":"ok","requestId":"…"}` once the work is committed. No `AsyncJobResult` is emitted — the reply *is* the result.
>
> Clients learn about the change via a **`RoomRestrictedRoomEvent`** (`type: "room_restricted"`) on the same `chat.room.{roomID}.event` stream they already subscribe to for chat messages. Like `RoomRenamedRoomEvent`, it's a flat struct with no zero-valued envelope fields:
>
> | Field | Type | Notes |
> |---|---|---|
> | `type` | string | Always `"room_restricted"`. |
> | `roomId` | string | The room whose flags changed. |
> | `siteId` | string | Home site of the room. |
> | `timestamp` | number | Publish time (UTC ms). |
> | `restricted` | bool | The new restricted state. |
> | `externalAccess` | bool | The new external-access state. |
> | `ownerAccount` | string | Omitted unless this was an unrestricted→restricted transition with a designated owner. |
> | `byAccount` | string | The admin who made the change. |
> | `changedAt` | string | ISO-8601 timestamp of when the change was applied. |

---

#### List Members

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.list`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `limit` | number | no | If set, must be `> 0`. Caps the number of members returned. |
| `offset` | number | no | If set, must be `>= 0`. For pagination. |
| `enrich` | boolean | no | When `true`, populates the display fields (`engName`, `chineseName`, `name`, `isOwner`, `orgName`, `memberCount`) on each entry. Omitted-or-`false` returns the lean record only. |

```json
{ "limit": 50, "enrich": true }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `members` | [RoomMember](#roommember)[] | One entry per individual or org membership. |

###### RoomMember

| Field | Type | Notes |
|---|---|---|
| `id` | string | Membership record ID (UUIDv7 hex). |
| `rid` | string | Room ID (note JSON key is `rid`, not `roomId`). |
| `ts` | string | RFC 3339 timestamp of when the membership was created. |
| `member` | [RoomMemberEntry](#roommemberentry) | The member identity. |

###### RoomMemberEntry

| Field | Type | Notes |
|---|---|---|
| `id` | string | The user account or org ID. |
| `type` | string | `"individual"` or `"org"`. |
| `account` | string | Optional. Account name (set for individuals). |
| `engName` | string | Optional. Populated only when request had `enrich: true`. |
| `chineseName` | string | Optional. Populated only when `enrich: true`. |
| `name` | string | Optional. Bot/app display name from `apps.name` when the member's account ends with `.bot`. Mutually exclusive with `engName`/`chineseName`. |
| `isOwner` | boolean | Optional. Populated only when `enrich: true`. |
| `orgName` | string | Optional. Org's display name (dept name preferred, sect name fallback). Populated only when `enrich: true` and entry is an org. |
| `memberCount` | number | Optional. Populated only when `enrich: true` and entry is an org. |

```json
{
  "members": [
    {
      "id": "01970a4f8c2d7c9a01970a4f8c2d7c9b",
      "rid": "01970a4f8c2d7c9aQ",
      "ts": "2026-05-01T10:00:00Z",
      "member": {
        "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
        "type": "individual",
        "account": "alice",
        "engName": "Alice",
        "chineseName": "愛麗絲",
        "isOwner": true
      }
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors: `"only room members can perform this action"` (requester has no subscription in the room), `"limit must be > 0"`, `"offset must be >= 0"`.

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Member Statuses

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.statuses`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `limit` | number | no | When omitted, the server uses `min(3, room.userCount)` (so a 2-member room returns 2 rows, an empty room returns an empty list). When supplied, must be `> 0` and `<= room.userCount`. |

```json
{ "limit": 5 }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `members` | array<MemberStatus> | One entry per room subscription, projected from the joined `users` document. |

`MemberStatus`:

| Field | Type | Notes |
|---|---|---|
| `account` | string | The user's account. |
| `engName` | string | English display name. |
| `chineseName` | string | Chinese display name. |
| `statusIsShow` | boolean | Whether the user has chosen to surface their status text. |
| `statusText` | string | Free-form presence text (e.g. `"available"`, `"in a meeting"`). Empty for users who have never set a status. |

```json
{
  "members": [
    {
      "account": "alice",
      "engName": "Alice Wang",
      "chineseName": "愛麗絲",
      "statusIsShow": true,
      "statusText": "available"
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- `"only room members can perform this action"` — caller has no subscription in the room (sentinel reused across membership-gated RPCs).
- `"limit must be > 0 and <= room user count"` — limit was `0`, negative, or larger than the room's current `userCount`.

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Mentionable Subscriptions

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.subscription.mentionable`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`**.

Used by the message composer's `@…` mention autocomplete. Returns subscriptions discriminated as `user` or `app`. The caller is always excluded from the result set. Platform-admin / webhook accounts (those whose `account` is prefixed `p_`) are also excluded — they are not `@`-mentionable.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `limit` | number | no | When omitted, the server uses `min(3, room.userCount + room.appCount)` (small rooms cap automatically, empty rooms return an empty list). When supplied, must be `> 0` and `<= room.userCount + room.appCount`. |
| `filter` | string | no | Defaults to `""` (matches everything). Treated as a literal substring; regex metacharacters are escaped server-side. Matched case-insensitively against a dash-joined keyword built from `account`, `engName`, `chineseName`, `app.name`, and `app.assistant.name`. |

```json
{ "limit": 10, "filter": "ali" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `subscriptions` | array<MentionableSubscription> | At most `limit` rows in arbitrary order. |

`MentionableSubscription` (discriminated by `optionType`):

| Field | Type | Notes |
|---|---|---|
| `optionType` | string | `"user"` for human accounts; `"app"` for assistant bots (`.bot` suffix). |
| `userId` | string | The subscription's `u._id` (the user's `_id`). |
| `account` | string | The user/bot account. |
| `siteId` | string | User's home site for `"user"` rows; **empty string** for `"app"` rows. |
| `hrInfo` | [HrInfo](#hrinfo) | Present **only for `"user"` rows**. |
| `app` | [MentionableApp](#mentionableapp) | Present **only for `"app"` rows**. |

###### MentionableApp

| Field | Type | Notes |
|---|---|---|
| `name` | string | App display name. |
| `assistant` | [AppAssistant](#appassistant) | The app's assistant subdocument (only `name` is set here). |

```json
{
  "subscriptions": [
    {
      "optionType": "user",
      "userId": "u-alice",
      "account": "alice",
      "siteId": "site-a",
      "hrInfo": { "engName": "Alice Wang", "chineseName": "愛麗絲" }
    },
    {
      "optionType": "app",
      "userId": "u-helper",
      "account": "helper.bot",
      "siteId": "",
      "app": { "name": "Helper", "assistant": { "name": "helper.bot" } }
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- `"only room members can perform this action"` — caller has no subscription in the room.
- `"limit must be > 0 and <= room user count + app count"` — limit was `0`, negative, or larger than the room's combined user + app population.

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Mark Messages Read

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.message.read`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This is a **synchronous RPC** — `room-service` performs all writes inline before replying. The handler validates room membership, recomputes the per-subscription `alert` flag, persists the new `lastSeenAt` and `alert` on the user's `Subscription`, and optionally recomputes `Room.MinUserLastSeenAt`.

##### Request body

The subject already carries `account` and `roomID`, so no body fields are required. Clients may send `{}` or omit the body entirely; any body content is ignored by the handler.

##### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"accepted"`. Confirms the read receipt was applied. |

```json
{ "status": "accepted" }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- `"only room members can perform this action"` — the user has no subscription in the room (sentinel reused across membership-gated RPCs).
- A malformed subject surfaces as a generic `"internal error"` (the specific reason is sanitized away). Not normally reachable — the wildcard subscription guarantees a well-formed subject.

```json
{ "code": "forbidden", "reason": "not_room_member", "error": "only room members can perform this action" }
```

##### Behaviour notes

- **Alert recomputation:** new `alert = oldSub.alert && len(oldSub.threadUnread) > 0`. Reading the room clears the alert when there are no unread thread mentions; it stays set when thread-level unreads remain.
- **No `JoinedAt` fallback for the early-return:** if `subscription.lastSeenAt` is null (the user was invited but has never opened the room), the handler does **not** treat `joinedAt` as a synthetic read position — being invited isn't reading. The room-floor recompute runs in this case so a member who has just read for the first time is reflected in the floor.
- **Room-floor recompute (`Room.MinUserLastSeenAt`):** the room's read floor (surfaced as `minUserLastSeenAt` in history responses) is a **strict "everyone has read" marker**: `MIN(lastSeenAt)` across **all** of the room's subscriptions, set **only when every subscription has a usable `lastSeenAt`**. If **any** member has never read the room (no/zero `lastSeenAt` — e.g. invited but never opened), the floor is `$unset` (null). Bots are counted like any other member, so a **botDM room — where the bot never reads — always has a null floor**. Reading a room can advance the floor (or, if this was the last unread member, raise it from null to a value).
- **Recompute trigger & a known gap:** the floor is recomputed only on this Mark Read path, and only when the caller was not already past `room.lastMsgAt` (the early-return above). Adding a member does not itself recompute the floor, so a newly-invited, never-read member will not flip an existing non-null floor to null until the next recompute is triggered (e.g. that member reads, or another member reads while the room has content).
- **Read-floor fan-out:** when (and only when) the recompute above changes `Room.MinUserLastSeenAt`, the server publishes a `message_read` room event carrying the new floor, so peers can advance read-receipt / unread UI live. Fan-out is best-effort (a publish failure does not fail the RPC) and never fires on the early-return paths or when the floor is unchanged. No system message is written.

##### Triggered events — success path

Emitted **only when the room read floor (`Room.MinUserLastSeenAt`) changes** (best-effort, core NATS):

- **Channel rooms — `chat.room.{roomID}.event`** — a single `message_read` event to every client subscribed to the room.
- **DM rooms — `chat.user.{account}.event.room`** — one `message_read` event per subscriber.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_read"`. |
| `roomId` | string | The room whose floor advanced. |
| `minUserLastSeenAt` | string | Optional. RFC3339 UTC timestamp of the new read floor. **Omitted** when the floor is null (a member is still fully unread). |
| `timestamp` | number | Event publish time, UTC milliseconds since Unix epoch. |

```json
{
  "type": "message_read",
  "roomId": "Rb3kQ2",
  "minUserLastSeenAt": "2026-06-09T10:30:00Z",
  "timestamp": 1749465000123
}
```

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Mark Thread as Read

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.message.thread.read`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

A **synchronous RPC** that clears a single thread's unread state for the caller. `room-service` validates room membership and thread-subscription existence, removes the threadId from the user's `Subscription.ThreadUnread`, recomputes the per-subscription `alert` flag, refreshes the `ThreadSubscription` (`lastSeenAt`, `updatedAt`, `hasMention=false`), and — for cross-site users — publishes a `thread_read` event to the user's home-site outbox so the destination `inbox-worker` can mirror both updates.

##### Request body

| Field | Type | Notes |
|---|---|---|
| `threadId` | string | Required. The thread's parent message ID. Empty / missing → `threadId is required`. |

```json
{ "threadId": "01970a4f8c2d7c9aQRST" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"accepted"`. |

```json
{ "status": "accepted" }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- `"only room members can perform this action"` — the caller has no subscription in the room (sentinel reused across membership-gated RPCs).
- `"thread subscription not found"` — the caller has no `ThreadSubscription` for the supplied `threadId` in the supplied room. Also returned when the thread exists but belongs to a different room than the one in the subject.
- `"threadId is required"` — body is missing `threadId` or sends an empty string.
- `"invalid message-thread-read subject: …"` — the subject is malformed.

##### Behaviour notes

- **Alert recomputation:** `alert = oldSub.alert && len(newThreadUnread) > 0`. A thread-read can only clear an alert, never set one. When the post-removal `threadUnread` is empty, `alert` becomes false. This computation runs atomically inside the MongoDB aggregation pipeline on the handler's site — not derived client-side.
- **Concurrent local writes:** the room-`Subscription` update and the `ThreadSubscription` update run in parallel inside an `errgroup`. Both must succeed before the handler proceeds.
- **Cross-site federation:** if the user's home site differs from the handler's site, a `thread_read` event is published to `outbox.{handlerSite}.to.{userSite}.thread_read` with payload `{account, roomId, threadRoomId, parentMessageId, newThreadUnread, alert, lastSeenAt, timestamp}` (timestamps as `int64` UnixMilli). The destination `inbox-worker` applies the supplied `newThreadUnread`+`alert` to the local Subscription cache and applies `lastSeenAt`+`updatedAt`+`hasMention=false` to the local ThreadSubscription with an `$lt` order-safety guard so out-of-order delivery cannot regress the thread's read position.
- **Defensive `roomId` filter:** the thread-subscription lookup additionally enforces that the supplied `threadId` belongs to the room named in the subject. Mismatches return `thread subscription not found` (rather than silently clearing an unrelated thread).
- **No system message, no fan-out events:** thread reads are silent; only the requester receives the `accepted` reply.

##### Triggered events — success path

`None — reply only.` (Cross-site users may observe a delayed cache update on their home site via the outbox/inbox flow above; this is treated as cache convergence rather than a client-visible event for this RPC.)

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Toggle Mute

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.mute.toggle`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Synchronous RPC. `room-service` flips `Subscription.muted` for the requester in a single atomic Mongo `FindOneAndUpdate`, replies with the resulting value, and fans out a `subscription.update` event to the user's other client sessions.

Idempotency: this is a toggle, not a set — every successful call flips the bit. Clients must debounce the user-visible action; redelivery of the same RPC will flip back.

##### Request body

The subject already carries `account` and `roomID`, so no body fields are required. Clients may send `{}` or omit the body entirely; any body content is ignored.

##### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"ok"`. |
| `muted` | boolean | The resulting value of `Subscription.muted` after the flip. |

```json
{ "status": "ok", "muted": true }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- `"only room members can perform this action"` — the user has no subscription in the room (sentinel reused across membership-gated RPCs).
- `"invalid mute-toggle subject: …"` — the subject is malformed.

##### Triggered events — success path

**`chat.user.{account}.event.subscription.update`** — emitted once for the requester so other client sessions reconcile.

| Field | Type | Notes |
|---|---|---|
| `userId` | string | The requester's internal user ID. |
| `subscription` | [Subscription](#subscription) | The Subscription record with the updated `muted`. |
| `action` | string | `"mute_toggled"`. |
| `timestamp` | number | Epoch ms (UTC). |

##### Behaviour notes

- **Notification delivery:** `notification-worker` respects `muted` flags when deciding whether to send mobile push notifications (see [Notification fan-out](#notification-fan-out-mobile-push-only) below).

---

#### Toggle Favorite

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.favorite.toggle`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Synchronous RPC. `room-service` flips `Subscription.favorite` for the requester in a single atomic Mongo `FindOneAndUpdate`, replies with the resulting value, and fans out a `subscription.update` event to the user's other client sessions. Used by the client to render the per-user "favorited" sidebar section; backend treats the flag as a render hint only — no downstream behaviour (notifications, routing, retention) is gated on it.

Idempotency: this is a toggle, not a set — every successful call flips the bit. Clients must debounce the user-visible action; redelivery of the same RPC will flip back.

##### Request body

The subject already carries `account` and `roomID`, so no body fields are required. Clients may send `{}` or omit the body entirely; any body content is ignored.

##### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"ok"`. |
| `favorite` | boolean | The resulting value of `Subscription.favorite` after the flip. |

```json
{ "status": "ok", "favorite": true }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- `"only room members can list members"` — the user has no subscription in the room (sentinel reused across membership-gated RPCs).
- `"invalid favorite-toggle subject: …"` — the subject is malformed.

##### Triggered events — success path

**`chat.user.{account}.event.subscription.update`** — emitted once for the requester so other client sessions reconcile.

| Field | Type | Notes |
|---|---|---|
| `userId` | string | The requester's internal user ID. |
| `subscription` | [Subscription](#subscription) | The Subscription record with the updated `favorite`. |
| `action` | string | `"favorite_toggled"`. |
| `timestamp` | number | Epoch ms (UTC). |

##### Cross-site behaviour

When the requester's home site differs from the room's site, `room-service` additionally publishes a `subscription_favorite_toggled` OutboxEvent to `outbox.{roomSite}.to.{userSite}.subscription_favorite_toggled`. `inbox-worker` on the user's home site mirrors the flip onto the local `Subscription` document. Missing-subscription on the home site (e.g., a federation race) is a silent no-op — no NACK, no redelivery loop.

---

#### Read Message Receipts

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.message.read-receipt`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

A **synchronous, sender-only** RPC. Returns the list of users on the local site whose `subscription.lastSeenAt` is at or after the target message's `createdAt`. Only the message author may call it. The author is excluded from the result.

##### Request body

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Required. The message whose readers to enumerate. |

```json
{ "messageId": "01970a4f8c2d7c9aQRST" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `readers` | array<ReadReceiptEntry> | Empty array when no subscription has read past `message.createdAt`. |

`ReadReceiptEntry`:

| Field | Type | Notes |
|---|---|---|
| `userId` | string | Internal user ID (`users._id`). |
| `account` | string | Account name. |
| `chineseName` | string | |
| `engName` | string | |

```json
{
  "readers": [
    {
      "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
      "account": "bob",
      "chineseName": "鮑勃",
      "engName": "Bob"
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- `"only room members can perform this action"` — the requester has no subscription in the room.
- `"message not found"` — no message matches `messageId`.
- `"message does not belong to this room"` — `messageId` exists but its `roomId` differs from the subject roomID.
- `"only the message sender can view read receipts"` — requester is not the author of `messageId`.
- A malformed subject surfaces as a generic `"internal error"` (the specific reason is sanitized away). Not normally reachable — the wildcard subscription guarantees a well-formed subject.
- `"invalid request: messageId is required"` — empty `messageId`.

```json
{ "code": "forbidden", "error": "only the message sender can view read receipts" }
```

##### Behaviour notes

- **Local-site only.** The query reads same-site `subscriptions`; cross-site read state is not surfaced by this RPC.
- **Sender excluded.** The author is filtered out at the database layer regardless of their own `lastSeenAt`.
- **Cap.** Result is capped at `MAX_ROOM_SIZE`; rooms larger than this cap will truncate silently.
- **No writes.** This RPC does not mutate any subscription, room, or message. Use the `message.read` RPC to advance `lastSeenAt`.

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### List Org Members

**Subject:** `chat.user.{account}.request.orgs.{orgID}.{siteID}.members`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` selects which site's user directory to query for org membership. Each site has its own `users` collection, so the returned membership set is per-site. (When the caller is composing for a specific room, this is typically the room's origin `siteID` — the same value used for `member.list` — but the endpoint itself is org-scoped, not room-scoped.)

The org ID is the third-from-last subject segment — there is no request body. `orgID` matches a user's `sectId` OR `deptId`; the response includes every user whose either field equals `orgID`. This mirrors the dept-aware org membership pipelines on the server side (a room may be added by sect-level or dept-level org and either form resolves through this endpoint).

##### Request body

Empty. Send `{}` or no payload.

```json
{}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `members` | array<OrgMember> | All individuals in the org. |

`OrgMember`:

| Field | Type | Notes |
|---|---|---|
| `id` | string | Internal user ID. |
| `account` | string | Account name. |
| `engName` | string | |
| `chineseName` | string | |
| `siteId` | string | The user's home site. |

```json
{
  "members": [
    {
      "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
      "account": "alice",
      "engName": "Alice",
      "chineseName": "愛麗絲",
      "siteId": "siteA"
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

```json
{ "code": "bad_request", "error": "invalid org" }
```

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Room App Tabs

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.app.tabs`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

##### Request body

Empty body (`{}` is tolerated). All inputs come from the subject.

##### Success response

| Field | Type | Notes |
|---|---|---|
| `apps` | [RoomApp](#roomapp)[] | Apps whose `channelTab.enabled` AND `channelTab.default` are both true, sorted by `channelTab.name asc`. Empty by default in DM/botDM rooms. |

###### RoomApp

| Field | Type | Notes |
|---|---|---|
| `id` | string | `apps._id`. |
| `name` | string | `apps.channelTab.name`. |
| `tabUrl` | string | Computed: `SITE_URL`'s scheme/host/path-prefix + `apps.channelTab.url.default`'s path; `${roomId}` and `${siteId}` are substituted. Apps whose template URL is empty or unparseable are silently skipped. |
| `assistant` | [AppAssistant](#appassistant) | Optional. `apps.assistant` subdocument if set. |
| `avatarUrl` | string | Optional. `apps.avatarUrl` if set. |

```json
{
  "apps": [
    {
      "id": "app-weather",
      "name": "Weather",
      "tabUrl": "https://site-a.example.com/apps/weather?room=01970a4f8c2d7c9aQ",
      "assistant": { "enabled": true, "name": "weather.bot" },
      "avatarUrl": "https://site-a.example.com/avatars/weather.png"
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors: `"not authorized to access this room's apps"` (caller is neither a room member nor a platform admin on the room's site), `"response payload exceeds maximum size"` (rare: response would exceed the NATS server's `max_payload`).

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Room App Command Menu

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.app.cmd-menu`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's origin `siteID`.

##### Request body

Empty body (`{}` is tolerated).

##### Success response

| Field | Type | Notes |
|---|---|---|
| `appAssistants` | [RoomAppAssistant](#roomappassistant)[] | One entry per bot currently subscribed in the room whose owning app has `assistant.enabled=true`. Sorted by `name asc`. |

###### RoomAppAssistant

| Field | Type | Notes |
|---|---|---|
| `appName` | string | `apps.name`. |
| `name` | string | `apps.assistant.name` (the bot account). |
| `cmdBlocks` | [CmdBlock](#cmdblock)[] | Optional. Active command-menu blocks from `bot_cmd_menu` joined by name. Omitted/nil if no active menu exists for the bot. |

###### CmdBlock

Recursive command-menu block: a block renders directly (`text` + `actionType` + `payload`), opens a `modal`, or groups nested `blocks`.

| Field | Type | Notes |
|---|---|---|
| `text` | string | Optional. Display text. |
| `actionType` | string | Optional. The action type. |
| `description` | string | Optional. Block description. |
| `payload` | string | Optional. Action payload. |
| `modal` | [CmdModal](#cmdmodal) | Optional. Modal opened by this block. |
| `blocks` | [CmdBlock](#cmdblock)[] | Optional. Nested child blocks. |

###### CmdModal

| Field | Type | Notes |
|---|---|---|
| `command` | string | Optional. Slash-style command the modal invokes. |
| `param` | string | Optional. Command parameter. |

```json
{
  "appAssistants": [
    {
      "appName": "Helper",
      "name": "helper.bot",
      "cmdBlocks": [
        {
          "text": "Create ticket",
          "actionType": "modal",
          "modal": { "command": "/ticket", "param": "new" }
        }
      ]
    }
  ]
}
```

##### Error response

Same envelope and sentinels as Get Room App Tabs.

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

### 3.2 history-service

| RPC subject | Method |
|---|---|
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.history` | [Load History](#load-history) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.next` | [Load Next Messages](#load-next-messages) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.surrounding` | [Load Surrounding Messages](#load-surrounding-messages) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get` | [Get Message By ID](#get-message-by-id) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.edit` | [Edit Message](#edit-message) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.delete` | [Delete Message](#delete-message) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.pin` | [Pin Message](#pin-message) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.unpin` | [Unpin Message](#unpin-message) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.pinned.list` | [List Pinned Messages](#list-pinned-messages) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.react` | [React to Message](#react-to-message) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread` | [Get Thread Messages](#get-thread-messages) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread.parent` | [Get Thread Parent Messages](#get-thread-parent-messages) |

#### Common request fields (read RPCs)

The paginated read RPCs (Load History, Load Next, Load Surrounding, Get Thread Messages) accept these shared optional fields in addition to their own:

| Field | Type | Notes |
|---|---|---|
| `limit` | number | Page size. Defaults when `0`/omitted: **20** for Load History, Load Next, and Get Thread Messages; **50** for Load Surrounding. Capped at **100**. |
| `meta` | [RoomMeta](#roommeta) | Optional. Room time hints — see below. Not accepted by Get Thread Messages. |

###### RoomMeta

| Field | Type | Notes |
|---|---|---|
| `lastMsgAt` | number | Optional. Room's most-recent-message time, UTC ms. |
| `createdAt` | number | Optional. Room's creation time, UTC ms. |

**What to pass for `meta`:** the server needs the room's `lastMsgAt` and `createdAt` to pick the Cassandra time-bucket window to scan. `meta` lets the client supply the values it already holds so the server can skip a MongoDB lookup:

- `meta.lastMsgAt` — the room's most-recent-message time, as the client knows it from the room summary (the same `lastMsgAt` carried on `RoomEvent`s / the sidebar). Use the room's last-activity timestamp; for an empty room use its `createdAt`.
- `meta.createdAt` — the room's creation time from the room summary.

Both are **hints, not authority**: the server sanitizes them (ignores values that are negative, in the future, or mutually inconsistent) and falls back to a MongoDB fetch when a value is missing or fails sanitization. A client that does not have these values should omit `meta` entirely — correctness is unaffected, only an extra lookup is incurred.

Common error envelopes across these RPCs (see §6 for the full shape):

| `code` | When |
|---|---|
| `forbidden` | `"not subscribed to room"`, or (for access-restricted readers) `"message is outside access window"` / `"thread is outside access window"`. |
| `not_found` | `"room not found"`, `"message not found"`. |
| `bad_request` | `"invalid pagination cursor"` (malformed `cursor` value), other request-validation failures. |
| `internal` | `"internal error"` — bubbled from store/publisher failures; real cause logged server-side, not sent. |

history-service does not currently emit a domain `reason` — clients branch on `code` (and the human-readable `error` only for display, never logic).

#### Message schema

Used by every history-service method that returns messages. Mirrors the Cassandra `Message` row.

| Field | Type | Notes |
|---|---|---|
| `roomId` | string | |
| `createdAt` | string | RFC 3339 timestamp. |
| `messageId` | string | 17- or 20-char base62. |
| `sender` | [MessageParticipant](#messageparticipant) | The message author. |
| `msg` | string | The message body. |
| `mentions` | [MessageParticipant](#messageparticipant)[] | Optional. |
| `attachments` | string[] | Optional. Each entry is base64-encoded bytes. |
| `file` | [MessageFile](#messagefile) | Optional. |
| `card` | [MessageCard](#messagecard) | Optional. |
| `cardAction` | [MessageCardAction](#messagecardaction) | Optional. |
| `tshow` | boolean | Optional. Whether a thread reply is also shown in the parent room. |
| `tcount` | number | Optional. Number of replies on a thread parent. |
| `threadParentId` | string | Optional. Set when this message is a thread reply. |
| `threadParentCreatedAt` | string | Optional. RFC 3339. |
| `quotedParentMessage` | [QuotedParentMessage](#quotedparentmessage) | Optional. Embedded snapshot of the quoted message. |
| `visibleTo` | string | Optional. Visibility scope. |
| `reactions` | map<emoji, [ReactionUser](#reactionuser)[]> | Optional. Omitted when absent; `{}` when present but empty. |
| `deleted` | boolean | Optional. `true` for tombstoned messages. |
| `type` | string | Optional. System-message type when set; regular messages omit it. Known values: `"room_created"`, `"members_added"`, `"member_removed"`, `"member_left"`, `"room_renamed"`, `"room_restricted"`. For all six, `msg` is populated with a server-rendered human-readable body and `sender.account` is the responsible actor (the requester for adds/removes-by-other / room-creates / renames / restricted changes, the leaving user for self-leave). |
| `sysMsgData` | string | Optional. Base64-encoded raw JSON payload for system messages. |
| `siteId` | string | Optional. The site that owns the message. |
| `editedAt` | string | Optional. RFC 3339. Set after an edit. |
| `updatedAt` | string | Optional. RFC 3339. Mirrors `editedAt` for edits, set on delete to record the deletion time. |
| `threadRoomId` | string | Optional. The thread room ID when this is a thread message. |
| `pinnedAt` | string | Optional. RFC 3339. With the `messages_by_room` `pinned_at` mirror, room-timeline history loads now return this on pinned rows too (previously only `pin.list` and point lookups carried it). |
| `pinnedBy` | [MessageParticipant](#messageparticipant) | Optional. |

##### MessageParticipant

The author/mention/pinner embedded in a message. Distinct from the event-actor
[Participant](#participant) (which is `userId`-keyed) — this is the Cassandra
message projection, keyed by `id`.

| Field | Type | Notes |
|---|---|---|
| `id` | string | Internal user (or app) ID. |
| `engName` | string | Optional. |
| `companyName` | string | Optional. |
| `appId` | string | Optional. Set for bot messages. |
| `appName` | string | Optional. |
| `isBot` | boolean | Optional. |
| `account` | string | Optional. |

##### MessageFile

| Field | Type | Notes |
|---|---|---|
| `id` | string | File ID. |
| `name` | string | File name. |
| `type` | string | MIME type. |

##### MessageCard

| Field | Type | Notes |
|---|---|---|
| `template` | string | Card template name. |
| `data` | string | Optional. Base64-encoded card payload. |

##### MessageCardAction

| Field | Type | Notes |
|---|---|---|
| `verb` | string | The action verb. |
| `text` | string | Optional. Button/label text. |
| `cardId` | string | Optional. Target card ID. |
| `displayText` | string | Optional. Text shown after the action runs. |
| `hideExecLog` | boolean | Optional. Suppress the execution log entry. |
| `cardTmId` | string | Optional. Card template ID. |
| `data` | string | Optional. Base64-encoded action payload. |

##### QuotedParentMessage

Embedded snapshot of the quoted message at the time of quoting.

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | |
| `roomId` | string | |
| `sender` | [MessageParticipant](#messageparticipant) | |
| `createdAt` | string | RFC 3339. |
| `msg` | string | Optional. Body snapshot. |
| `mentions` | [MessageParticipant](#messageparticipant)[] | Optional. |
| `attachments` | string[] | Optional. |
| `messageLink` | string | Optional. |
| `threadParentId` | string | Optional. Set if the quoted message itself is a thread reply. |
| `threadParentCreatedAt` | string | Optional. RFC 3339. |

When the reader is in a restricted access window and the quoted parent falls outside it, the embedded snapshot is redacted to `{ "msg": "This message is unavailable" }` — all other quote fields are dropped.

##### ReactionUser

`reactions` is keyed by emoji; each value is the list of users who reacted with that emoji. The inner record is intentionally minimal — the FE composes any further presentation it needs from these two fields:

| Field | Type | Notes |
|---|---|---|
| `account` | string | The reactor's stable account identifier. Used for "is this me?" / "remove my reaction" checks. |
| `displayName` | string | Server-composed via `displayfmt.CombineWithFallback(engName, chineseName, account)` — same helper used by system-message formatters. |

Example:

```json
"reactions": {
  "❤️": [{"account": "bob",   "displayName": "Bob 鲍勃"}],
  "👍": [{"account": "alice", "displayName": "Alice 爱丽丝"}, {"account": "carol", "displayName": "Carol 卡罗尔"}]
}
```

Each emoji's user array is sorted by `reactedAt` ascending — FIFO, oldest reaction first, newest last. This matches the legacy MongoDB insertion-order behaviour. Same-millisecond ties break by `account` ASC. Outer JSON object key order (across emojis) is unspecified — FE applies its own emoji ordering.

Live reaction events (`MessageReactedPayload`) carry a single-actor delta (`{shortcode, action: "added"|"removed", actor: Participant, reactedAt}`) including the actor's full `Participant`; clients merge a delta into history-derived state by appending or removing one entry under `reactions[shortcode]` keyed on `actor.account`.

#### Load History

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.history`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `before` | number | no | Epoch ms (UTC). Returns messages with `createdAt < before`. Omit (or `null`) for "now". |
| `limit` | number | no | Page size — see [Common request fields](#common-request-fields-read-rpcs) (default 20, max 100). |

```json
{
  "before": 1746518400000,
  "limit": 50
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | array<Message> | Most-recent first. See [Message schema](#message-schema). |
| `minUserLastSeenAt` | number | Optional. UTC milliseconds since Unix epoch. The room's **strict read floor** — `MIN(lastSeenAt)` across all subscribers, present **only when every member has read** the room. Absent (null) when any member has not read yet (so botDM rooms, where the bot never reads, never set it), when the most recent read is already past `room.lastMsgAt` (recompute is skipped), or when the value cannot be retrieved (best-effort; messages still load). See the Message Read RPC for how this floor is recomputed. |

```json
{
  "messages": [
    {
      "roomId": "01970a4f8c2d7c9aQ",
      "createdAt": "2026-05-06T07:55:00Z",
      "messageId": "01970a4f8c2d7c9aQRST",
      "sender": {
        "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
        "account": "alice",
        "engName": "Alice"
      },
      "msg": "morning team"
    }
  ],
  "minUserLastSeenAt": 1746518100000
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

```json
{ "code": "forbidden", "error": "not subscribed to room" }
```

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Load Next Messages

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.next`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Fetches messages newer than a cursor — the forward-pagination counterpart to Load History.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `after` | number | no | Epoch ms (UTC). Returns messages with `createdAt > after`. Omit for "no lower bound". |
| `limit` | number | no | Page size — see [Common request fields](#common-request-fields-read-rpcs) (default 20, max 100). |
| `cursor` | string | yes | Pagination cursor returned by a previous response. Use empty string for the first page. |

```json
{
  "after": 1746518400000,
  "limit": 50,
  "cursor": ""
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | array<Message> | Oldest-first within the page. See [Message schema](#message-schema). |
| `nextCursor` | string | Optional. Opaque cursor to pass to the next call. Empty when `hasNext=false`. |
| `hasNext` | boolean | `true` if more messages exist beyond this page. |

```json
{
  "messages": [
    {
      "roomId": "01970a4f8c2d7c9aQ",
      "createdAt": "2026-05-06T07:55:00Z",
      "messageId": "01970a4f8c2d7c9aQRST",
      "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" },
      "msg": "morning team"
    }
  ],
  "nextCursor": "eyJ0cyI6MTc0NjUxODQwMDAwMH0=",
  "hasNext": true
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Load Surrounding Messages

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.surrounding`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Fetches messages around a target message — useful for "jump to this message" navigation. Returns up to `limit` messages total, centered on `messageId`.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `messageId` | string | yes | The central message to center the window on. |
| `limit` | number | no | Window size including the central message — see [Common request fields](#common-request-fields-read-rpcs) (default 50, max 100). |

```json
{
  "messageId": "01970a4f8c2d7c9aQRST",
  "limit": 50
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | array<Message> | Window of messages centered on `messageId`, oldest-first. See [Message schema](#message-schema). |
| `moreBefore` | boolean | `true` if more messages exist before the window. |
| `moreAfter` | boolean | `true` if more messages exist after the window. |

```json
{
  "messages": [
    {
      "roomId": "01970a4f8c2d7c9aQ",
      "createdAt": "2026-05-06T07:55:00Z",
      "messageId": "01970a4f8c2d7c9aQRST",
      "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" },
      "msg": "morning team"
    }
  ],
  "moreBefore": true,
  "moreAfter": false
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Message By ID

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `messageId` | string | yes | The message to fetch. |

```json
{ "messageId": "01970a4f8c2d7c9aQRST" }
```

##### Success response

A single `Message` object. See [Message schema](#message-schema).

```json
{
  "roomId": "01970a4f8c2d7c9aQ",
  "createdAt": "2026-05-06T07:55:00Z",
  "messageId": "01970a4f8c2d7c9aQRST",
  "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" },
  "msg": "morning team"
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

```json
{ "code": "not_found", "error": "message not found" }
```

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Edit Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.edit`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Only the original sender may edit a message.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `messageId` | string | yes | The message to edit. |
| `newMsg` | string | yes | The new content. Must be non-empty and within the size limit. |

```json
{
  "messageId": "01970a4f8c2d7c9aQRST",
  "newMsg": "morning team — updated"
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Echoes the input. |
| `editedAt` | number | Epoch ms (UTC). |

```json
{
  "messageId": "01970a4f8c2d7c9aQRST",
  "editedAt": 1746518700000
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Errors:

| `code` | `error` | When |
|---|---|---|
| `forbidden` | `only the sender can edit` | Caller is not the message author. |
| `not_found` | `message not found` | `messageId` does not exist (or is outside the access window). |
| `bad_request` | `newMsg must not be empty` | Empty `newMsg`. |
| `bad_request` | `newMsg exceeds maximum size` | `newMsg` exceeds the configured cap. |
| `internal` | `internal error` | Store/publish failure; real cause logged server-side. |

##### Triggered events — success path

An `EditRoomEvent` is fanned out by `broadcast-worker`. The subject depends on room type:

- **Channel rooms — `chat.room.{roomID}.event`** — one publish to the room stream.
- **DM/botDM rooms — `chat.user.{recipient}.event.room`** — published once per non-bot member.

The payload is flat (no zero-valued room fields):

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_edited"`. |
| `roomId` | string | |
| `siteId` | string | |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `messageId` | string | The edited message's ID. |
| `newContent` | string | Optional. New plaintext content. Present for DMs and unencrypted channels. Omitted for encrypted channels — see `encryptedNewContent`. |
| `encryptedNewContent` | [EncryptedMessage](#encryptedmessage) | Optional. For encrypted channel rooms. Omitted otherwise. |
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

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Delete Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.delete`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Soft-deletes a message (sets `deleted=true` on the row; row is preserved for audit). Only the original sender may delete. Idempotent — re-deleting an already-deleted message returns success without re-publishing the event.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `messageId` | string | yes | The message to delete. |

```json
{ "messageId": "01970a4f8c2d7c9aQRST" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Echoes the input. |
| `deletedAt` | number | Epoch ms (UTC). For an already-deleted message, this is the original deletion time. |

```json
{
  "messageId": "01970a4f8c2d7c9aQRST",
  "deletedAt": 1746518800000
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Errors:

| `code` | `error` | When |
|---|---|---|
| `forbidden` | `only the sender can delete` | Caller is not the message author. |
| `not_found` | `message not found` | `messageId` does not exist (or is outside the access window). |
| `internal` | `internal error` | Store/publish failure; real cause logged server-side. |

##### Triggered events — success path

A `DeleteRoomEvent` is fanned out by `broadcast-worker` (not published when the request hits an already-deleted message or loses a concurrent-delete CAS). The subject and recipients depend on message type:

- **Top-level channel message — `chat.room.{roomID}.event`** — one publish to the room stream; all room subscribers receive it.
- **Thread reply (TShow=false) in a channel** — `chat.user.{recipient}.event.room` — published once per thread subscriber (followers + @-mentioned accounts). Non-subscribers do not receive this event.
- **Thread reply (TShow=true) in a channel** — `chat.room.{roomID}.event` — visible in the main channel, so the full room stream receives it.
- **DM/botDM message — `chat.user.{recipient}.event.room`** — published once per non-bot member.

The payload is flat:

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_deleted"`. |
| `roomId` | string | |
| `siteId` | string | |
| `timestamp` | number | Milliseconds since Unix epoch (UTC). When broadcast-worker published this event. |
| `eventTimestamp` | number | Milliseconds since Unix epoch (UTC). When message-worker published the canonical event. Omitted for legacy events. |
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

**Thread-reply deletes additionally emit a `ThreadMetadataUpdatedEvent`** (see [§4.1 Thread Metadata Event](#41-thread-metadata-event)) to update the parent message's reply-count badge. The `DeleteRoomEvent` and `ThreadMetadataUpdatedEvent` are published independently; clients must handle each on its own.

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Pin Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.pin`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

Pins a message in the room. Idempotent — pinning an already-pinned message succeeds and echoes the existing `pinnedAt` without re-publishing the canonical event.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `messageId` | string | yes | The message to pin. |

```json
{ "messageId": "01970a4f8c2d7c9aQRST" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Echoes the input. |
| `pinnedAt` | number | Epoch ms (UTC). For an already-pinned message, this is the original pin time. |

```json
{
  "messageId": "01970a4f8c2d7c9aQRST",
  "pinnedAt": 1746518900000
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

| Code | Reason | Message | Cause |
|---|---|---|---|
| `forbidden` | `pin_disabled` | `"pinning is disabled"` | Global kill-switch (`PIN_ENABLED=false`) is off. |
| `forbidden` | `not_subscribed` | `"not subscribed to room"` | Caller has no subscription to the room. |
| `forbidden` | `pin_room_too_large` | `"room is too large to pin"` | Room member count exceeds the configured `LARGE_ROOM_THRESHOLD`. Owners, admins, and bot accounts are exempt. |
| `forbidden` | `pin_limit_reached` | `"room pin limit reached"` | Room already has `MAX_PINNED_PER_ROOM` pinned messages (default 10). Hard cap — no role-based bypass. Unpin an existing message to free a slot. |
| `not_found` | — | `"message not found"` | Message does not exist, belongs to a different room, or has been deleted. |
| `internal` | — | `"internal error"` | Mongo/Cassandra read or write failed (subscription lookup, room user count, pinned-messages count, message lookup, or pin write). Specific cause appears in the server log. |

##### Triggered events — success path

**Channel rooms:** `chat.room.{roomID}.event` — `PinStateRoomEvent`. Recipients: every client subscribed to the room.
**DM / BotDM rooms:** `chat.user.{account}.room.event` — `PinStateRoomEvent`. Recipients: each non-bot DM participant.

Not published when the request hits an already-pinned message (idempotent short-circuit).

Pin and unpin share the same flat `PinStateRoomEvent` payload; `type` discriminates and `pinned` carries the resulting state.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_pinned"`. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `eventTimestamp` | number | Epoch ms (UTC). Canonical event time. Omitted when zero. |
| `roomId` | string | |
| `siteId` | string | Originating site. |
| `messageId` | string | The pinned message's ID. |
| `pinned` | boolean | Resulting pin state. Always `true` for `message_pinned`. |
| `by` | [Participant](#participant) | Optional — omitted if no actor was recorded. The actor who pinned. `chineseName` / `engName` are omitted when unset. |
| `at` | string | RFC 3339. Domain time of the pin. |

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

##### Triggered events — error path

`None — error returned only via the reply subject.`

##### Backend side effects (internal — not client-subscribable)

On success, the service publishes a `MessageEvent` to **`chat.msg.canonical.{siteID}.pinned`** (JetStream, `MESSAGES_CANONICAL_{siteID}` stream). This is an internal canonical subject consumed by backend workers (broadcast-worker, search-sync-worker, etc.) and is **not** part of any client subscription pattern. Documented here only so backend service authors know the payload shape. Not published when the request hits an already-pinned message (idempotent short-circuit) or a soft-deleted message (the handler returns `not_found` before publishing).

| Field | Type | Notes |
|---|---|---|
| `event` | string | Always `"pinned"`. |
| `siteId` | string | The originating site. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `message` | [Message](#message-schema) | `id`, `roomId`, `userId`, `userAccount`, `createdAt`, `pinnedAt`, `pinnedBy` populated. |

```json
{
  "event": "pinned",
  "siteId": "site1",
  "timestamp": 1746518900123,
  "message": {
    "id": "01970a4f8c2d7c9aQRST",
    "roomId": "01970a4f8c2d7c9aQ",
    "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
    "userAccount": "alice",
    "createdAt": "2026-05-01T10:00:00Z",
    "pinnedAt": "2026-05-06T08:01:40Z",
    "pinnedBy": { "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" }
  }
}
```

---

#### Unpin Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.unpin`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

Unpins a message in the room. Idempotent — unpinning a message that is not pinned succeeds as a no-op without publishing the canonical event.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `messageId` | string | yes | The message to unpin. |

```json
{ "messageId": "01970a4f8c2d7c9aQRST" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Echoes the input. |

```json
{ "messageId": "01970a4f8c2d7c9aQRST" }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

| Code | Reason | Message | Cause |
|---|---|---|---|
| `forbidden` | `pin_disabled` | `"pinning is disabled"` | Global kill-switch (`PIN_ENABLED=false`) is off. |
| `forbidden` | `not_subscribed` | `"not subscribed to room"` | Caller has no subscription to the room. |
| `forbidden` | `pin_room_too_large` | `"room is too large to pin"` | Room member count exceeds the configured `LARGE_ROOM_THRESHOLD`. Owners, admins, and bot accounts are exempt. |
| `not_found` | — | `"message not found"` | Message does not exist or belongs to a different room. Unlike pin, **soft-deleted messages are still unpinnable** — a pinned message that was later deleted retains its slot in `pinned_messages_by_room`, and unpin is the only way to free it. |
| `internal` | — | `"internal error"` | Mongo/Cassandra read or write failed (subscription lookup, room user count, message lookup, or unpin write). Specific cause appears in the server log. |

##### Triggered events — success path

**Channel rooms:** `chat.room.{roomID}.event` — `PinStateRoomEvent`. Recipients: every client subscribed to the room.
**DM / BotDM rooms:** `chat.user.{account}.room.event` — `PinStateRoomEvent`. Recipients: each non-bot DM participant.

Not published when the request hits an already-unpinned message (idempotent short-circuit).

Same flat `PinStateRoomEvent` payload as [Pin Message](#pin-message); `type` discriminates and `pinned` carries the resulting state.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_unpinned"`. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `eventTimestamp` | number | Epoch ms (UTC). Canonical event time. Omitted when zero. |
| `roomId` | string | |
| `siteId` | string | Originating site. |
| `messageId` | string | The unpinned message's ID. |
| `pinned` | boolean | Resulting pin state. Always `false` for `message_unpinned`. |
| `by` | [Participant](#participant) | Optional — omitted if no actor was recorded. The actor recorded on the pin. `chineseName` / `engName` are omitted when unset. |
| `at` | string | RFC 3339. Stamped by history-service when it processes the unpin RPC (the canonical unpin event clears the pin timestamp, so this is the only unpin time on the wire). |

```json
{
  "type": "message_unpinned",
  "timestamp": 1746518950123,
  "eventTimestamp": 1746518950100,
  "roomId": "01970a4f8c2d7c9aQ",
  "siteId": "site1",
  "messageId": "01970a4f8c2d7c9aQRST",
  "pinned": false,
  "by": { "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" },
  "at": "2026-05-06T08:02:30Z"
}
```

##### Triggered events — error path

`None — error returned only via the reply subject.`

##### Backend side effects (internal — not client-subscribable)

On success, the service publishes a `MessageEvent` to **`chat.msg.canonical.{siteID}.unpinned`** (JetStream, `MESSAGES_CANONICAL_{siteID}` stream). This is an internal canonical subject consumed by backend workers and is **not** part of any client subscription pattern. Documented here only so backend service authors know the payload shape. Not published when the request hits an already-unpinned message.

| Field | Type | Notes |
|---|---|---|
| `event` | string | Always `"unpinned"`. |
| `siteId` | string | The originating site. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `message` | [Message](#message-schema) | `id`, `roomId`, `userId`, `userAccount`, `createdAt`, `pinnedBy` populated (no `pinnedAt` — the message is now unpinned). |

```json
{
  "event": "unpinned",
  "siteId": "site1",
  "timestamp": 1746518950123,
  "message": {
    "id": "01970a4f8c2d7c9aQRST",
    "roomId": "01970a4f8c2d7c9aQ",
    "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
    "userAccount": "alice",
    "createdAt": "2026-05-01T10:00:00Z",
    "pinnedBy": { "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" }
  }
}
```

---

#### List Pinned Messages

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.pinned.list`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

Returns pinned messages in a room, ordered most-recently-pinned first. Only subscription access is required — the global pin kill-switch and the large-room override do **not** apply to listing (existing pins remain listable even when new pinning is disabled).

The response is cursor-paginated (`cursor`/`limit` in the request, `nextCursor`/`hasNext` in the response). Because pins are capped at `MAX_PINNED_PER_ROOM` (default 10), most callers will see `hasNext=false` on the first page.

**Access window — redacted stubs:** If the caller's subscription has a `historySharedSince` lower bound (partial history access), pins whose underlying message was created before that timestamp are returned as **redacted stubs**. The following fields are cleared: `msg` (replaced with `"This message is unavailable"`), `mentions`, `attachments`, `file`, `card`, `cardAction`, `quotedParentMessage`, `reactions`, `type`, `sysMsgData`. **All other Message fields remain populated** — identifiers, `sender`, `createdAt`, `pinnedAt`, `pinnedBy`, plus any thread/edit metadata — so the frontend can render a placeholder in place. **The row count is the same for every caller.** A quoted parent inside a still-visible pin is redacted by the same mechanism as elsewhere (see Load History).

**Timestamps:** Each returned message carries both `createdAt` (the underlying message's true creation time) and `pinnedAt` (when it was pinned). No second round-trip is needed to obtain the original creation timestamp.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `cursor` | string | no | Pagination cursor returned by a previous response. Omit (or empty string) for the first page. |
| `limit` | number | yes | Maximum number of pinned messages to return in this page. Server clamps to a sane range. |

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | array<Message> | Pinned messages in the page, most-recently-pinned first. See [Message schema](#message-schema). Pre-access pins appear as redacted stubs (see "Access window" above). |
| `nextCursor` | string | Opaque token to fetch the next page. Empty when there are no more pages. |
| `hasNext` | boolean | `true` while more pages exist; `false` on the final page. |

```json
{
  "messages": [
    {
      "roomId": "01970a4f8c2d7c9aQ",
      "messageId": "01970a4f8c2d7c9aQRST",
      "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" },
      "msg": "morning team",
      "createdAt": "2026-05-06T07:55:12Z",
      "pinnedAt": "2026-05-06T08:01:40Z",
      "pinnedBy": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" }
    },
    {
      "roomId": "01970a4f8c2d7c9aQ",
      "messageId": "01970a4f8c2d7c9aQOLD",
      "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "bob" },
      "msg": "This message is unavailable",
      "createdAt": "2025-12-01T10:00:00Z",
      "pinnedAt": "2025-12-02T09:00:00Z",
      "pinnedBy": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" }
    }
  ],
  "nextCursor": "",
  "hasNext": false
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

| Code | Reason | Message | Cause |
|---|---|---|---|
| `forbidden` | `not_subscribed` | `"not subscribed to room"` | Caller has no subscription to the room. |
| `bad_request` | — | `"invalid pagination cursor"` | The `cursor` value is not a valid base64 page-state token. |
| `internal` | — | `"internal error"` | Mongo/Cassandra read failed (subscription lookup or pinned-messages page). Specific cause appears in the server log. |

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### React to Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.react`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

Toggles a reaction on a message. Any subscribed room member may react — the server decides add vs remove by checking whether the calling user is already in the message's reactor map for that shortcode. Reactions can always be _removed_ from a soft-deleted message (so users can clean up after a delete), but cannot be _added_ to one.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `messageId` | string | yes | The message to react to. |
| `shortcode` | string | yes | The bare reaction shortcode without surrounding colons (e.g. `acme_party`). Must match `^[a-z0-9_+-]{1,32}$` after NFC normalisation. The server resolves the shortcode against the `custom_emojis` collection for the site; an unregistered shortcode returns `"invalid reaction shortcode"`. |

```json
{
  "messageId": "01970a4f8c2d7c9aQRST",
  "shortcode": "acme_party"
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Echoes the input. |
| `shortcode` | string | The canonical NFC-normalised form of the input. ASCII shortcodes (`acme_party`, etc.) are byte-identical to the request; Unicode codepoints in alternate encodings (NFD, ZWJ variants) would collapse to NFC. Clients should treat this — not their request value — as the storage key. |
| `action` | string | `"added"` or `"removed"` — which side of the toggle the server applied. |
| `reactedAt` | number | Epoch ms (UTC). |

```json
{
  "messageId": "01970a4f8c2d7c9aQRST",
  "shortcode": "acme_party",
  "action": "added",
  "reactedAt": 1746518900000
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors: `"messageId is required"`, `"shortcode is required"`, `"invalid reaction shortcode"` (format or unknown custom emoji), `"message not found"` (also returned when attempting to _add_ a reaction to a soft-deleted message), `"not subscribed to room"`, `"failed to add reaction"`, `"failed to remove reaction"`.

##### Triggered events — success path

**`chat.room.{roomID}.event`** — `ReactRoomEvent`. Recipients: every client subscribed to the room (channel rooms); for DMs, each non-bot member receives the event on `chat.user.{account}.event.room`.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_reacted"`. |
| `roomId` | string | |
| `siteId` | string | |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `messageId` | string | The reacted-to message's ID. |
| `shortcode` | string | The bare reaction shortcode. |
| `action` | string | `"added"` or `"removed"`. |
| `actor` | [Participant](#participant) | The user whose toggle produced this event. |
| `reactedAt` | string (RFC 3339) | Domain time of the toggle. |
| `updatedAt` | string (RFC 3339) | Mirrors `reactedAt`. |

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

**`chat.user.{account}.notification`** — `NotificationEvent` with `type: "reaction"`. Recipients: the message author only, and only when the toggle is an `"added"` action and the actor is not the author. The notification carries the full reacted-to `message` plus the `reactionDelta` so the client can render which emoji was added, by whom, and on which message without a separate fetch.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"reaction"`. |
| `roomId` | string | The room containing the reacted-to message. |
| `message` | [Message](#message-schema) | The full reacted-to message. |
| `reactionDelta` | [ReactionDelta](#reactiondelta) | The single-reaction delta that triggered the notification. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |

###### ReactionDelta

| Field | Type | Notes |
|---|---|---|
| `shortcode` | string | The emoji shortcode reacted with. |
| `action` | string | Always `"added"` here (the notification only fires on add). |
| `actor` | [Participant](#participant) | The user who reacted. |

To reconcile this delta with the grouped per-message `reactions` map returned by history endpoints (`map<emoji, [{account, displayName}]>`), clients append or remove one entry under `reactions[shortcode]` keyed on `actor.account`. See the "Message schema" section for the history-side shape.

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Thread Messages

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Returns the replies in a thread. The thread parent's `messageId` is supplied in the request.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `threadMessageId` | string | yes | The top-level thread message ID. Must be a thread parent — not a reply. |
| `cursor` | string | no | Pagination cursor returned by a previous response. Omit for the first page. |
| `limit` | number | no | Page size — see [Common request fields](#common-request-fields-read-rpcs) (default 20, max 100). |

```json
{
  "threadMessageId": "01970a4f8c2d7c9aQRST",
  "limit": 50
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | array<Message> | Replies in the thread, oldest-first within the page. See [Message schema](#message-schema). |
| `nextCursor` | string | Optional. Opaque cursor for the next page. |
| `hasNext` | boolean | `true` if more replies exist beyond this page. |

```json
{
  "messages": [
    {
      "roomId": "01970a4f8c2d7c9aQ",
      "createdAt": "2026-05-06T08:00:00Z",
      "messageId": "01970a4f8c2d7c9aQUVW",
      "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9b", "account": "bob" },
      "msg": "good morning",
      "threadParentId": "01970a4f8c2d7c9aQRST",
      "threadParentCreatedAt": "2026-05-06T07:55:00Z"
    }
  ],
  "hasNext": false
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Thread Parent Messages

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread.parent`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Lists the parent messages of threads the user has subscribed to (or all threads, depending on filter). Use this to drive a "Threads" tab in the client.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `filter` | string | yes | One of `"all"`, `"following"` (only threads the user is subscribed to), or `"unread"` (only threads with unread replies). |
| `offset` | number | yes | For pagination. `0` for the first page. |
| `limit` | number | yes | Maximum number of thread parents to return. |

```json
{
  "filter": "following",
  "offset": 0,
  "limit": 50
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `parentMessages` | array<Message> | Thread parent messages, ordered by most-recent reply activity. See [Message schema](#message-schema). |
| `total` | number | Raw count before access filtering. Use for pagination math only — `parentMessages.length` may be smaller. |

```json
{
  "parentMessages": [
    {
      "roomId": "01970a4f8c2d7c9aQ",
      "createdAt": "2026-05-06T07:55:00Z",
      "messageId": "01970a4f8c2d7c9aQRST",
      "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" },
      "msg": "let's discuss the rollout",
      "tcount": 3
    }
  ],
  "total": 42
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

### 3.3 search-service

| RPC subject | Method |
|---|---|
| `chat.user.{account}.request.search.{siteID}.messages` | [`search.messages`](#searchmessages--full-text-message-search) |
| `chat.user.{account}.request.search.{siteID}.rooms` | [Search Rooms](#search-rooms) |
| `chat.user.{account}.request.search.{siteID}.apps` | [Search Apps](#search-apps) |
| `chat.user.{account}.request.search.{siteID}.users` | [Search Users](#search-users) |

#### `search.messages` — full-text message search

> [!WARNING]
> **Breaking change (v2):** The response shape has changed from `{total, results}` to `{messages, total}`. The `results` field no longer exists. The per-hit type is now `SearchMessage` (an enriched projection) instead of the former `MessageSearchHit`. Update all clients before deploying this version.

**Subject:** `chat.user.{account}.request.search.{siteID}.messages`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

**Auth:** the `{account}` in the subject is the authenticated identity. `{siteID}` is the requester's home site — the supercluster routes the request to the search-service running on that site. The search is automatically scoped to rooms the user is a member of — results never include messages from rooms the user cannot access.

**Cross-site results:** results may include messages from rooms hosted on remote sites (the index is searched across the local and remote clusters). The `siteId` on each `SearchMessage` identifies the originating site; there is no client opt-in/opt-out.

##### Request body

```json
{
  "query": "hello world",
  "roomIds": ["r1", "r2"],
  "size": 25,
  "offset": 0
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | **yes** | Full-text query. Empty string is rejected. |
| `roomIds` | string[] | no | Scope the search to these rooms. Omit for global search across all accessible rooms. Unknown room IDs and rooms the user cannot access are silently excluded (enforced by the ES terms-lookup + restricted-rooms floor). |
| `size` | integer | no | Page size. Default `25`, capped at `100`. |
| `offset` | integer | no | Page offset. Default `0`. |

##### Success response

```json
{
  "messages": [
    {
      "messageId": "m1",
      "roomId": "r1",
      "siteId": "site-a",
      "userAccount": "alice",
      "content": "hello world (edited)",
      "createdAt": "2026-04-01T12:00:00Z",
      "editedAt": "2026-04-01T12:05:00Z",
      "updatedAt": "2026-04-01T12:06:00Z",
      "threadParentMessageId": "p0",
      "threadParentMessageCreatedAt": "2026-04-01T11:58:00Z"
    }
  ],
  "total": 42
}
```

| Field | Type | Notes |
|---|---|---|
| `messages` | SearchMessage[] | Per-hit projection. Always an array (empty `[]` when no results). |
| `total` | integer | Total matching hits (may exceed `messages.length` when paginating). |

**`SearchMessage` fields** (all sourced directly from the ES message index — no Mongo round-trip):

| Field | Type | Omitted when |
|---|---|---|
| `messageId` | string | — |
| `roomId` | string | — |
| `siteId` | string | — |
| `userAccount` | string | — |
| `content` | string | — |
| `createdAt` | RFC3339 timestamp | — |
| `editedAt` | RFC3339 timestamp (nullable) | omitted when the message has never been edited |
| `updatedAt` | RFC3339 timestamp (nullable) | omitted when the message has never been mutated server-side (edit, soft-delete, etc.) |
| `threadParentMessageId` | string | omitted when not a thread reply |
| `threadParentMessageCreatedAt` | RFC3339 timestamp (nullable) | omitted when not a thread reply |

Display fields (user name, room name) are intentionally NOT carried in the response. Clients resolve them via the `user-service` lookups (`user.{siteID}.profile.getByName`) or their own subscription cache.

##### Error response

See [Error envelope](#6-error-envelope-reference).

| Code | Reason |
|---|---|
| `bad_request` | `query` is empty; or `size`/`offset` is negative. |
| `internal` | ES backend failure or cache failure with no ES fallback. Raw errors are never leaked to the client. |

**Access control for `roomIds`:**
- Rooms in neither the user's subscription set nor the restricted-rooms map are silently excluded.
- Restricted rooms (those with an HSS floor) are included only for messages posted after the user's `historySharedSince` boundary.

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Search Rooms

**Subject:** `chat.user.{account}.request.search.{siteID}.rooms`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

`{siteID}` is the requester's home site; the supercluster routes the request to that site's search-service. Full-text search across rooms the requester is subscribed to. Results are served directly from the spotlight ES index (one document per `(account, room)` pair), in ES relevance order.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | **yes** | Case-insensitive prefix/substring match on room name. Whitespace-only is rejected. |
| `roomType` | string | no | `"all"` (default), `"channel"`, or `"dm"`. The value `"app"` and any other value are rejected with `bad_request`. |
| `size` | number | no | Page size. Default `25`, capped at `100`. |
| `offset` | number | no | Pagination offset. Default `0`. |

```json
{
  "query": "engineering",
  "roomType": "channel",
  "size": 20
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `rooms` | array<SearchRoom> | Page of room results. Empty slice when no matches, never null. |

`SearchRoom` (projection of the spotlight ES document):

| Field | Type | Notes |
|---|---|---|
| `roomId` | string | The room's ID. |
| `name` | string | The room's display name. |
| `roomType` | string | `"channel"`, `"dm"`, or omitted for other types. |
| `siteId` | string | The room's home site. |

```json
{
  "rooms": [
    {
      "roomId": "01970a4f8c2d7c9aQ",
      "name": "engineering-announcements",
      "roomType": "channel",
      "siteId": "site-a"
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

| Code | Reason |
|---|---|
| `bad_request` | `query` is missing, empty, or whitespace-only; or `roomType` is `"app"` or an unrecognized value; or `size`/`offset` is negative. |
| `internal` | Elasticsearch backend failure (transient or permanent). The raw error is never leaked to the client. |

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Search Apps

**Subject:** `chat.user.{account}.request.search.{siteID}.apps`

**Auth:** the `{account}` in the subject is the authenticated identity (enforced by the NATS auth callout). `{siteID}` is the requester's home site; the supercluster routes the request to that site's search-service.

**Current behavior (prototype):** results are matched by `query` (and optional `assistantEnabled`) only. The response is **not** yet subscription-scoped — every app whose name matches the query is returned.

**Planned behavior:** the response will be scoped to apps the caller has subscribed to once the pipeline's `$lookup` access guard against the `subscriptions` collection is enabled. See `TODO(searchApps-pipeline)` in `search-service/query_apps.go`.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | **yes** | Case-insensitive substring match on `app.name`. Whitespace-only is rejected. |
| `assistantEnabled` | boolean (nullable) | no | When set, strict equality on `app.assistant.enabled`. Omit for no filter. |
| `size` | integer | no | Page size. Default `25`, capped at `100`. |
| `offset` | integer | no | Page offset. Default `0`. |

```json
{
  "query": "weather",
  "assistantEnabled": true,
  "size": 25,
  "offset": 0
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `apps` | [App](#app)[] | Matching apps. Empty array when none. |

###### App

| Field | Type | Notes |
|---|---|---|
| `id` | string | App ID. |
| `name` | string | App name. |
| `description` | string | Optional. App description. |
| `avatarUrl` | string | Optional. App avatar URL. |
| `assistant` | [AppAssistant](#appassistant) | Optional. The app's assistant subdocument. |
| `channelTab` | [AppChannelTab](#appchanneltab) | Optional. Channel-tab embedding config. |
| `sponsors` | [AppSponsor](#appsponsor)[] | Optional. App sponsors. |

###### AppChannelTab

| Field | Type | Notes |
|---|---|---|
| `enabled` | boolean | Whether the tab is enabled. |
| `default` | boolean | Whether the tab appears by default in every channel. |
| `name` | string | Tab name. |
| `url` | [AppChannelTabURL](#appchanneltaburl) | Tab URL template. |

###### AppChannelTabURL

| Field | Type | Notes |
|---|---|---|
| `default` | string | Canonical URL template with literal `${roomId}` / `${siteId}` placeholders. |

###### AppSponsor

| Field | Type | Notes |
|---|---|---|
| `name` | string | Sponsor name. |
| `phone` | string | Sponsor phone number. |

```json
{
  "apps": [
    {
      "id": "a1",
      "name": "Weather",
      "description": "Local forecasts",
      "avatarUrl": "https://site-a.example.com/avatars/weather.png",
      "assistant": { "enabled": true, "name": "weather.bot", "settingsUrl": "https://site-a.example.com/apps/weather/settings" },
      "channelTab": { "enabled": true, "default": false, "name": "Weather", "url": { "default": "https://site-a.example.com/apps/weather?room=${roomId}&site=${siteId}" } },
      "sponsors": [{ "name": "Acme Corp", "phone": "+1-555-0100" }]
    }
  ]
}
```

##### Error response

| Category | Reason |
|---|---|
| `bad_request` | Validation failures (`query` missing/blank, negative `size`/`offset`). |
| `internal` | Backend failure (transient or permanent). The raw error is never leaked to the client. |

These are documentation categories. The wire error envelope shape — `{ "error": "<human-readable message>", "code": "<category>", "reason"?: "<domain code>", "metadata"?: {…} }` — is the same across every endpoint and is defined canonically in §6 (Error envelope reference).

---

#### Search Users

**Subject:** `chat.user.{account}.request.search.{siteID}.users`
**Reply subject:** auto-generated `_INBOX.>` (NATS request/reply)

`{siteID}` is the requester's home site; the supercluster routes the request to that site's search-service. Proxy search for users via the third-party HR endpoint. The `{account}` in the subject is the authenticated identity (enforced by the NATS auth callout) and is used for logging/metrics only — company-scoping is enforced by the third-party endpoint.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | **yes** | Search term forwarded to the third-party HR endpoint. Whitespace-only is rejected. |
| `offset` | integer | no | Page offset forwarded to the HR endpoint. Default `0`. Must be non-negative. |
| `limit` | integer | no | Page size forwarded to the HR endpoint. Default `25`, capped at `100`. Must be non-negative. |

```json
{
  "query": "alice",
  "offset": 0,
  "limit": 25
}
```

##### Success response

A top-level JSON array of [SearchUser](#searchuser) objects (no wrapping envelope).

###### SearchUser

| Field | Type | Notes |
|---|---|---|
| `account` | string | The user's account name. |
| `engName` | string | Optional. English display name. |
| `chineseName` | string | Optional. Chinese display name. |

Additional legacy fields may be present, mirroring the `GET /api/v3/users` response shape.

```json
[
  {
    "account": "alice",
    "engName": "Alice Wang",
    "chineseName": "愛麗絲王"
  }
]
```

##### Error response

| Code | Reason |
|---|---|
| `bad_request` | `query` is missing, empty, or whitespace-only. |
| `internal` | Third-party HR endpoint unavailable or returned a non-2xx status. The raw third-party error is never forwarded to the caller. |

---

## 4. Message Send

### Send Message

**Subject:** `chat.user.{account}.room.{roomID}.{siteID}.msg.send`
**Reply subject:** `chat.user.{account}.response.{requestID}` — the client must subscribe to `chat.user.{account}.>` (the user wildcard) to receive it. The `{requestID}` value is the `requestId` field from the request body.

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This RPC uses the **publish + async-reply** pattern, not the standard NATS request/reply. The client publishes to the `msg.send` subject (no `_INBOX.>` reply expected). `message-gatekeeper` validates the request, publishes the canonical message to `MESSAGES_CANONICAL`, and replies to `chat.user.{account}.response.{requestID}` with the persisted `Message` (or an error envelope on failure).

The same subject and request body cover three send variants: plain message, thread reply, and quoted message. The variant is determined by which optional fields are set.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `id` | string | yes | The message's ID. Must be 20-char base62. The client generates this and uses it for client-side optimistic rendering. |
| `content` | string | yes | The message body. Must be non-empty and ≤ 20 KiB. |
| `requestId` | string | yes | A 36-char hyphenated UUID (v4 or v7) the client generates. **Validated** — an empty or malformed `requestId` is rejected with no message published. The async reply is delivered to `chat.user.{account}.response.{requestId}`. |
| `threadParentMessageId` | string | no | Set when posting a thread reply. Must be a valid 20-char base62 message ID. Pair with `threadParentMessageCreatedAt`. |
| `threadParentMessageCreatedAt` | number | no | Required when `threadParentMessageId` is set. Epoch ms (UTC). |
| `tshow` | boolean | no | The "Also send to channel" option. Only meaningful on a thread reply (`threadParentMessageId` set): the reply is persisted into the parent room's channel timeline as well as the thread (dual-write into `messages_by_room` in addition to `thread_messages_by_thread` + `messages_by_id`), and is surfaced with `tshow: true` on the persisted message. On a non-thread send the flag is **ignored and normalized to `false`** — the request is not rejected. |
| `quotedParentMessageId` | string | no | Set when posting a quoted message. The gatekeeper fetches the parent and embeds a snapshot in the persisted message; the client does not send the snapshot itself. |

##### Plain message

```json
{
  "id": "01970a4f8c2d7c9aQRST",
  "content": "morning team",
  "requestId": "01970a4f-8c2d-7c9a-abcd-e0123456789f"
}
```

##### Thread reply

```json
{
  "id": "01970a4f8c2d7c9aQUVW",
  "content": "good morning",
  "requestId": "01970a4f-8c2d-7c9a-abcd-e0123456789a",
  "threadParentMessageId": "01970a4f8c2d7c9aQRST",
  "threadParentMessageCreatedAt": 1746518100000
}
```

##### Quoted message

```json
{
  "id": "01970a4f8c2d7c9aQXYZ",
  "content": "agreed — adding context",
  "requestId": "01970a4f-8c2d-7c9a-abcd-e0123456789b",
  "quotedParentMessageId": "01970a4f8c2d7c9aQRST"
}
```

#### Success response

Delivered on `chat.user.{account}.response.{requestId}`. The body is the persisted `Message` as the gatekeeper builds it. Only the fields below are populated — the same shape for plain, thread, and quoted sends (the optional fields appear only for their variant):

| Field | Type | Notes |
|---|---|---|
| `id` | string | Echoes the request `id`. |
| `roomId` | string | Derived from the request subject. |
| `userId` | string | Sender's internal user ID (resolved from the sender's subscription). |
| `userAccount` | string | Sender's account. |
| `userDisplayName` | string | Render-ready sender name composed server-side (falls back to `userAccount`). |
| `content` | string | The message body, exactly as sent. |
| `createdAt` | string | RFC 3339. Server-assigned send time (UTC). |
| `threadParentMessageId` | string | Present only for a thread reply (echoes the request). |
| `threadParentMessageCreatedAt` | string | Present only for a thread reply. RFC 3339. |
| `tshow` | boolean | Present only when the request set `tshow: true` on a thread reply (absent when the flag was normalized away on a non-thread send). |
| `quotedParentMessage` | [QuotedParentMessage](#quotedparentmessage) | Present only for a quoted send — the server-fetched snapshot of the quoted parent. |

The gatekeeper does **not** populate `mentions`, `editedAt`/`updatedAt`, `type`, or `sysMsgData` on this reply (all `omitempty`, so they are absent). Mention resolution and the enriched `sender` happen in the broadcast fan-out event ([§4 triggered events](#triggered-events--success-path)), not in this reply.

```json
{
  "id": "01970a4f8c2d7c9aQRST",
  "roomId": "01970a4f8c2d7c9aQ",
  "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "userAccount": "alice",
  "userDisplayName": "Alice 愛麗絲",
  "content": "morning team",
  "createdAt": "2026-05-06T07:55:00Z"
}
```

#### Error response

Delivered on `chat.user.{account}.response.{requestId}`. See [Error envelope](#6-error-envelope-reference). Errors:

| Wire `error` | `code` | `reason` | Cause |
|---|---|---|---|
| `invalid requestId "…": must be a hyphenated UUID` | `bad_request` | — | Empty/malformed `requestId`. (Reachable only when `requestId` is non-empty but malformed; an empty `requestId` leaves no reply subject, so the client just times out.) |
| `invalid message ID "…": must be a 20-char base62 string` | `bad_request` | — | `id` is not valid base62. |
| `invalid thread parent message ID "…": …` | `bad_request` | — | `threadParentMessageId` is not a valid message ID. |
| `content must not be empty` | `bad_request` | — | Empty `content`. |
| `content exceeds maximum size of 20480 bytes` | `bad_request` | — | `content` > 20 KiB. |
| `validate thread parent fields: threadParentMessageCreatedAt is required when threadParentMessageId is set` | `bad_request` | — | Missing thread-parent timestamp. |
| `not subscribed` | `forbidden` | `not_subscribed` | Sender is not a member of the room. |
| `posting is restricted to owners and admins in this room` | `forbidden` | `large_room_post_restricted` | Non-owner/admin/bot posting a top-level message in a room above the large-room threshold (thread replies are exempt). |
| `quoted parent {id} not found` | `not_found` | — | The quoted message lookup failed (deleted, cross-room, …). |
| `quoted parent {id} thread context mismatch: …` | `bad_request` | — | A quoted message must be in the same thread context (main-room or the same thread) as the new message. |

**Delivery guarantee:** every validation/authorization failure — including a `siteID` mismatch and a malformed `msg.send` subject — is replied to the client on the response subject and the JetStream message is acked (not retried). The error reply requires a routable response subject, so it can only be sent when the `{account}` segment is recoverable from the subject and the payload carries a valid hyphenated-UUID `requestId`; if neither is recoverable (a truly malformed subject or missing/invalid `requestId`) no reply is possible and the client falls back to a request timeout. **Only infrastructure failures** (store/publish errors) are nak'd and **redelivered by JetStream** — these produce no immediate reply.

```json
{ "code": "bad_request", "error": "content must not be empty" }
```

#### Triggered events — success path

After a successful send, `broadcast-worker` fans out a `RoomEvent`. The subject depends on room type. **`botDM` rooms receive no `new_message` fan-out at all:** `broadcast-worker` only handles `channel` and `dm` room types, so a `botDM` falls through to the default branch and is skipped — the human participant in a `botDM` does **not** receive a `new_message` room event from this pipeline. (Bot integrations consume `botDM` messages through a separate backend path.)

**1. For channel rooms — `chat.room.{roomID}.event`** (`publishChannelEvent`)

A `RoomEvent`. Recipients: every client subscribed to the room (which includes the sender, since the sender is also a member).

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"new_message"`. |
| `roomId` | string | |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `roomName` | string | |
| `roomType` | string | `channel`, `dm`, etc. |
| `siteId` | string | |
| `userCount` | number | |
| `lastMsgAt` | string | RFC 3339. |
| `lastMsgId` | string | The new message's ID. |
| `mentions` | [Participant](#participant)[] | Optional. |
| `mentionAll` | boolean | Optional. `true` if the message mentioned `@all` or `@here`. |
| `hasMention` | boolean | Optional. Per-recipient flag (DM event only). Always absent on channel events. |
| `message` | [ClientMessage](#clientmessage) | Optional. Set for unencrypted rooms. |
| `encryptedMessage` | [EncryptedMessage](#encryptedmessage) | Optional. Set for encrypted (channel) rooms. Clients decrypt with the room key for `version`. |

###### ClientMessage

The canonical broadcast message (distinct from the history [Message schema](#message-schema), which is the Cassandra projection): it uses `content`/`userId`/`userAccount` and carries an enriched `sender` [Participant](#participant).

| Field | Type | Notes |
|---|---|---|
| `id` | string | Message ID. |
| `roomId` | string | The room. |
| `userId` | string | Sender's internal user ID. |
| `userAccount` | string | Sender's account. |
| `userDisplayName` | string | Optional. Render-ready sender name. |
| `content` | string | The message body. |
| `sender` | [Participant](#participant) | Optional. Enriched sender identity. |
| `attachments` | string[] | Optional. Each entry is base64-encoded bytes. |
| `file` | [MessageFile](#messagefile) | Optional. |
| `card` | [MessageCard](#messagecard) | Optional. |
| `cardAction` | [MessageCardAction](#messagecardaction) | Optional. |
| `mentions` | [Participant](#participant)[] | Optional. |
| `createdAt` | string | RFC 3339. |
| `editedAt` | string | Optional. RFC 3339. |
| `updatedAt` | string | Optional. RFC 3339. |
| `threadParentMessageId` | string | Optional. Set for a thread reply. |
| `threadParentMessageCreatedAt` | string | Optional. RFC 3339. |
| `tshow` | boolean | Optional. Whether a thread reply is also shown in the parent room. |
| `type` | string | Optional. System-message type when set. |
| `sysMsgData` | string | Optional. Base64-encoded raw JSON payload for system messages. |
| `quotedParentMessage` | [QuotedParentMessage](#quotedparentmessage) | Optional. |
| `pinnedAt` | string | Optional. RFC 3339. |
| `pinnedBy` | [Participant](#participant) | Optional. |

```json
{
  "type": "new_message",
  "roomId": "01970a4f8c2d7c9aQ",
  "timestamp": 1746518100123,
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

**2. For DM rooms — `chat.user.{recipient}.event.room`** (`publishDMEvents`)

A `RoomEvent` (same struct as above) published once per DM participant. Recipients: each user subscribed to the DM. The `hasMention` field is set per-recipient. DM events carry `message` (plaintext); they do not use `encryptedMessage`.

```json
{
  "type": "new_message",
  "roomId": "alice___bob",
  "timestamp": 1746518100123,
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

**Thread replies additionally emit a `ThreadMetadataUpdatedEvent`** (see [§4.1 Thread Metadata Event](#41-thread-metadata-event)) to update the parent message's reply-count badge. This event is published to all room members (not only thread subscribers) so every client can show the correct badge without subscribing to the thread.

#### Triggered events — error path

When validation fails, the gatekeeper publishes the error envelope to `chat.user.{account}.response.{requestId}` and **no downstream events are emitted**. The client should display the error and offer a retry.

```json
{ "code": "bad_request", "error": "content must not be empty" }
```

#### Notification fan-out (mobile push only)

`notification-worker` no longer publishes `chat.user.{account}.notification`
on core NATS. Mobile pushes are emitted on the server-only JetStream subject
`chat.server.notification.push.{siteID}.send` and forwarded by the internal
push-notification service. Desktop banners are computed client-side from the
broadcast-worker room-event stream — no server-side desktop publish exists.

The worker filters recipients per message:

- Skips the sender.
- Skips members with `muted: true` on their subscription.
- Skips members whose `historySharedSince` postdates the message (for a
  thread-only reply the parent's `createdAt` is used instead).
- For a thread reply with `tshow: false`, skips non-followers who are not
  mentioned.
- In rooms with more than `LARGE_ROOM_THRESHOLD` members (default 500),
  pushes only to mentioned recipients (`@user`, `@all`, `@here`).
- Bots never receive a mobile push.
- Presence-busy / in-call recipients are not pushed; everyone else
  (online, offline, away, missing) receives one.

---

## 4.1 Thread Metadata Event

### ThreadMetadataUpdatedEvent

Pushed by `broadcast-worker` whenever a thread reply is **created** (`action: "reply_added"`) or **deleted** (`action: "reply_deleted"`). Its purpose is to let clients update the reply-count badge on the parent message without reloading the thread.

#### Subjects

| Room type | Subject |
|-----------|---------|
| Channel | `chat.room.{roomID}.event` |
| DM / botDM | `chat.user.{account}.event.room` — published once per non-bot member |

#### Payload

| Field | Type | Notes |
|-------|------|-------|
| `type` | string | Always `"thread_metadata_updated"`. |
| `roomId` | string | The room the thread lives in. |
| `siteId` | string | |
| `parentMessageId` | string | The thread parent message's ID. Clients use this to locate the message in their cache and update its badge. |
| `newTcount` | number | Authoritative post-CAS reply count for the parent message. Replaces any locally-computed count — do not delta. |
| `action` | string | `"reply_added"` or `"reply_deleted"`. |
| `replyMessageId` | string | The reply that was added or deleted. |
| `timestamp` | number | Milliseconds since Unix epoch (UTC). When broadcast-worker published this event. |
| `eventTimestamp` | number | Milliseconds since Unix epoch (UTC). When message-worker published the canonical event. Omitted for legacy events. |

```json
{
  "type": "thread_metadata_updated",
  "roomId": "01970a4f8c2d7c9aQ",
  "siteId": "siteA",
  "parentMessageId": "01970a4f8c2d7c9aQRST",
  "newTcount": 4,
  "action": "reply_added",
  "replyMessageId": "01970a4f8c2d7c9aUVWX",
  "timestamp": 1746518100123
}
```

#### When it fires

- **Reply added (`action: "reply_added"`):** fired when a new thread reply is successfully persisted (triggered by a `Send Message` RPC with `threadParentId` set). Published in addition to the per-subscriber `new_message` `RoomEvent` that carries the reply content.
- **Reply deleted (`action: "reply_deleted"`):** fired when a thread reply is soft-deleted (triggered by a `Delete Message` RPC). Published in addition to the `DeleteRoomEvent` that carries the delete notification.

#### Client handling

Apply `newTcount` directly to the parent message's badge — do not compute a delta. Events for the same parent may arrive out of order due to JetStream redelivery; always prefer the event with the larger `timestamp` for badge state.

---

## 5. Room Encryption

Channel messages can be end-to-end encrypted. The key material reaches clients as `RoomKeyEvent`s, which are triggered by the Create Room / Add Members / Remove Member RPCs (see their "Triggered events" sections). This section describes the event payload and how a client uses it to decrypt.

Each **channel** room has a 32-byte secret generated server-side at create time (`crypto/rand`). The secret is distributed to channel members and used directly as an AES-256-GCM key — no key derivation step. DM and botDM rooms are **not** encrypted: their messages fan out to per-user subjects that only the recipient can subscribe to, so they carry no room key, emit no `RoomKeyEvent`, and always broadcast plaintext `message` (no `encryptedMessage`).

#### Subject

```text
chat.user.{account}.event.room.key
```

Clients are already authorized for `chat.user.{theirAccount}.>` and receive key events on this subject without additional setup.

#### Payload (`RoomKeyEvent`)

| Field | Type | Notes |
|---|---|---|
| `roomId` | string | The room the key belongs to. |
| `version` | integer | Room-key version. |
| `privateKey` | string | Base64-encoded 32-byte room secret, used directly as the AES-256-GCM key. |
| `timestamp` | number | Epoch ms (UTC). |

```json
{
  "roomId": "<room id>",
  "version": 0,
  "privateKey": "<base64-encoded 32-byte room secret>",
  "timestamp": 1747000000000
}
```

`[]byte` fields marshal to standard base64 in JSON. The `privateKey` is the 32-byte room secret used directly as the AES-256-GCM key; no public key field is transmitted.

#### Client behavior

1. On every `RoomKeyEvent`, store the key under `(roomId, version) → privateKey`.
2. To decrypt an incoming `encryptedMessage` payload:
   - Look up `privateKey` for `(roomId, encryptedMessage.version)`.
   - Use the 32-byte `privateKey` directly as the AES-256-GCM key (no key derivation step).
   - Decrypt: `AES-GCM-Decrypt(privateKey, nonce, ciphertext, aad=empty)`. The ciphertext already includes the 16-byte GCM tag at the end (Go `cipher.AEAD.Seal` format).
   - **The cipher is identical for both event kinds (same `roomcrypto` AES-256-GCM seal); only the plaintext payload differs**, because each event encrypts exactly what the client needs:
     - **`encryptedMessage`** (new message) decrypts to a UTF-8-encoded JSON `ClientMessage` — a brand-new message the client has never seen, so the whole object (sender, timestamps, thread/quote fields) is sealed.
     - **`encryptedNewContent`** (edit) decrypts to a plain UTF-8 content **string** — the client already has the original message rendered, and an edit only replaces its `content`, so just the new body is sealed (the surrounding message metadata is unchanged and already known).
3. Retain past versions to support history scrolling. The server retains the previous version in its store for at least `ROOM_KEY_GRACE_PERIOD` (default 24h); after that, server-side decryption of old messages may not be possible, but clients holding old keys can still decrypt locally.

#### When clients receive `RoomKeyEvent`s

- **Room creation (all room types):** sent to every initial member.
- **Add member (channels only):** sent to each newly-added account; existing members do not receive a duplicate event.
- **Remove member (channels only):** the server rotates the room key. Surviving members receive a new `RoomKeyEvent` with an incremented `version`. The removed account stops receiving events for the room.

Removed members keep prior keys for decrypting historical messages but cannot decrypt anything published after the rotation.

**Initial key bootstrap on (re)connect:** live `RoomKeyEvent`s fire only when keys change. The initial set of keys for rooms the client is already subscribed to will be delivered as part of the `subscription.get*` RPC family (see user-service — to be documented). Until that extension lands, clients receive keys only via live events.

### Requesting a missing key

If a client receives an `encryptedMessage` whose `(roomId, version)` it
doesn't hold — e.g. it reconnected after the original `RoomKeyEvent` was
delivered, or the message references an older version it never stored —
it can fetch the key on demand from `room-service`.

#### Subject

```text
chat.user.{account}.request.room.{roomID}.{siteID}.key.get
```

`{siteID}` is the room's origin siteID (the same value carried on the
inbound message event). Clients are already authorized for
`chat.user.{theirAccount}.>` and need no additional grant.

#### Request payload (`RoomKeyGetRequest`)

| Field | Type | Notes |
|---|---|---|
| `version` | integer | Optional (nullable). When omitted or `null`, the server returns the **current** key for the room. |

```json
{ "version": 3 }
```

#### Success reply (`RoomKeyGetResponse`)

| Field | Type | Notes |
|---|---|---|
| `roomId` | string | The room the key belongs to. |
| `version` | integer | Room-key version returned. |
| `privateKey` | string | Base64-encoded 32-byte room secret. |

```json
{
  "roomId": "<room id>",
  "version": 3,
  "privateKey": "<base64-encoded 32-byte room secret>"
}
```

Same shape as `RoomKeyEvent` minus `timestamp`, so a client can feed the
reply through the same caching path it uses for live events.

#### Errors

| Condition | Error envelope `error` text | Notes |
|---|---|---|
| Requester is not a member of the room | `only room members can list members` | Surfaces the existing `errNotRoomMember` sentinel. |
| Key not held (rolled past grace window, or never existed) | `room key not available` | Includes "explicit version not in the previous-key slot". |
| Malformed request body | `invalid request: …` | |
| Internal failure | `internal error` | |

#### Use as complement to live events

This RPC complements — it does not replace — live `RoomKeyEvent`s on
`chat.user.{account}.event.room.key`. Live events remain the primary
delivery channel at room create / add-member / rotation. Clients
should call `key.get` only when a received message cannot be decrypted
with the keys they already hold, and back off after a failure so a
chatty channel does not stampede the server for a key that is
permanently gone.

---

## 6. Error envelope reference

Every error response — NATS reply subjects, JetStream async results, and HTTP — uses the same envelope:

```json
{
  "error": "<human-readable, user-safe message>",
  "code": "<one of 8 generic categories>",
  "reason": "<optional, domain-specific machine code>",
  "metadata": { "<key>": "<value>" }
}
```

| Field | Type | Notes |
|---|---|---|
| `error` | string | Human-readable, user-safe (never carries an internal cause). Do not parse or pattern-match against the text. |
| `code` | string | **Always present.** One of the 8 categories below. Drives HTTP status. |
| `reason` | string (optional) | Domain-specific machine code (e.g. `max_room_size_reached`, `not_subscribed`). When present, the client should branch on `reason ?? code`. |
| `metadata` | object (optional) | Free-form `string→string` map for structured detail (e.g. `{ "limit": "500" }`). |

### Generic `code` values (always present) → HTTP status

| `code` | HTTP | When |
|---|---|---|
| `bad_request` | 400 | Malformed/invalid input or unsupported parameters. |
| `unauthenticated` | 401 | Missing/expired/invalid credentials. |
| `forbidden` | 403 | Authenticated but not permitted. |
| `not_found` | 404 | Target resource does not exist. |
| `conflict` | 409 | State conflict (duplicate, capacity exceeded, last-owner removal, …). |
| `too_many_requests` | 429 | Per-caller rate limit / quota exceeded. |
| `unavailable` | 503 | Transient server saturation/timeout (admission, expand timeout). |
| `internal` | 500 | Unclassified server-side fault. The real cause is logged server-side only and never sent to the client. |

> [!IMPORTANT]
> **Malformed request bodies.** Any room request/reply RPC whose payload is not valid JSON for its schema is rejected uniformly with `code: bad_request` and the message `"invalid request payload"` — the transport layer rejects it before the handler runs. Treat this as a generic encoding error; do not pattern-match the message text.

### `reason` catalog (present today)

| `reason` | Typical `code` | Emitted by |
|---|---|---|
| `max_room_size_reached` | conflict | room-service create/add (room capacity exceeded) |
| `not_room_member` | forbidden | room-service / room-worker (actor not a member) |
| `not_room_owner` | forbidden | room-service role/admin paths |
| `last_owner_cannot_leave` | conflict | room-service leave |
| `bot_in_channel` | bad_request | room-service member-add (bot in a channel room) |
| `bot_not_available` | not_found | room-service member-add (unknown bot) |
| `user_not_found` | not_found | room-service / room-worker (account does not resolve to a user) |
| `invalid_org` | bad_request | room-service create/add (orgId does not resolve to any users) |
| `self_dm` | bad_request | room-service create (DM to yourself) |
| `last_member_cannot_remove` | conflict | room-service remove-member (would empty the room) |
| `target_not_member` | bad_request | room-service role-update (target is not a room member) |
| `already_owner` | conflict | room-service role-update (promote a current owner) |
| `cannot_demote_last_owner` | conflict | room-service role-update (demote the last owner) |
| `promote_requires_individual` | bad_request | room-service role-update (only individual members can be owners) |
| `large_room_post_restricted` | forbidden | message-gatekeeper (non-owner/admin posting in a large room) |
| `not_subscribed` | forbidden | message-gatekeeper / history-service (caller not subscribed) |
| `outside_access_window` | forbidden | history-service (subscribed but message predates HSS) |
| `pin_disabled` | forbidden | history-service pin/unpin/list (kill-switch `PIN_ENABLED=false`) |
| `pin_limit_reached` | forbidden | history-service pin (room at `MAX_PINNED_PER_ROOM` hard cap) |
| `pin_room_too_large` | forbidden | history-service pin/unpin (non-owner/admin/bot in a room above `LARGE_ROOM_THRESHOLD`) |
| `sso_token_expired` | unauthenticated | auth-service `POST /auth` |
| `invalid_sso_token` | unauthenticated | auth-service `POST /auth` |
| `invalid_request` | bad_request | auth-service (body parse / required field missing) |
| `invalid_nkey` | bad_request | auth-service (natsPublicKey format) |
| `missing_fields` | bad_request | auth-service (ssoToken/account/natsPublicKey missing) |

### Where envelopes are sent

- **NATS sync replies** — on the reply subject for §3/§4 RPCs.
- **JetStream async results** — `model.AsyncJobResult` carries the same `code` + `reason` fields when `status == "error"`, so a failed async job is surfaced the same way as a sync error.
- **HTTP** — auth-service `POST /auth` (§2.2) and upload-service's image endpoints (§2.3) write the envelope as the response body with the matching HTTP status from the table above.

### Client branching guidance

Compute the trigger as `reason ?? code` and branch on that. Use `code` for generic copy ("you don't have permission", "service unavailable, try again"), `reason` for endpoint-specific UX (open the "room is full" dialog on `max_room_size_reached`; redirect to re-login on `sso_token_expired`/`invalid_sso_token`; surface "join the room first" on `not_subscribed`). Never branch on the `error` text — message wording can change without notice.

---

## 7. Presence

Served by **user-presence-service**. Tracks each user's effective presence —
`online`, `away`, `busy`, `offline` — derived from live connections plus an
optional manual override. Each site owns the presence of its local users; a
user's home site is the site they connect to. Cross-site is transparent: for
**live state** (§7.7) a watcher subscribes to the global per-user subject and
the NATS gateway routes it; for **batch queries** (§7.6) the watcher sends a
single request to its **own local site**, which resolves each account's home
site and fans out to peer sites server-side.

**Connection model.** A client mints one **client-generated** `connId` (e.g.
`crypto.randomUUID()`) per connection — browser tab / SharedWorker / socket —
when it comes up, reuses it for that connection's `hello`/`ping`/`activity`/`bye`,
and discards it on close. The server never assigns it. Writes carry the user's
**home `{siteID}`** so they route to the presence service that owns the user's
state regardless of which site the client is connected to; `{account}` is
JWT-pinned to the caller, so a client can only ever write its own presence.

`online` vs `away` is derived from per-connection activity the client reports
via `activity` (§7.3); the server has at least one **active** connection ⇒
`online`, all connections **inactive** ⇒ `away`, none ⇒ `offline`.

**Effective status resolution.** The server evaluates this precedence ladder
top-down; the **first** matching rule wins (so a stale manual override never
keeps a fully-disconnected user "present"):

1. **No live connections → `offline`** — beats any manual override.
2. Manual `appear_offline` → `offline`; manual `away` → `away`.
3. Manual `online` → `online`; manual `busy` → `busy`.
4. Any other manual override → that status.
5. All live connections inactive → `away`.
6. Otherwise → `online`.

### 7.1 Hello — initialize a connection (publish, fire-and-forget)

**Subject:** `chat.user.{account}.event.presence.{siteID}.hello`

Sent once when a connection comes up, to register it (and bring the user
`online`). No reply. A `ping` for an unseen `connId` also creates it, so a
dropped `hello` self-heals on the next ping.

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `connId` | string | yes | Client-generated per connection (e.g. a UUID per browser/SharedWorker). |
| `timestamp` | number | no | Millis since Unix epoch (UTC). |

```json
{ "connId": "1f0a-uuid", "timestamp": 1746518100000 }
```

### 7.2 Ping — liveness (publish, fire-and-forget)

**Subject:** `chat.user.{account}.event.presence.{siteID}.ping`

Published per connection roughly every **30 s** (no reply) to refresh liveness.
A connection is considered live for ~**45 s** after its last ping; miss that
window and the sweeper decays it toward `offline`. A ping does **not** change
activity — use `activity` (§7.3) for that. Pinging a `connId` the server has not
seen before creates it (offline→online), so an initial `hello` is optional.

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `connId` | string | yes | The connection being refreshed. |
| `timestamp` | number | no | Millis since Unix epoch (UTC). |

```json
{ "connId": "1f0a-uuid", "timestamp": 1746518100000 }
```

### 7.3 Activity — active / inactive (publish, fire-and-forget)

**Subject:** `chat.user.{account}.event.presence.{siteID}.activity`

Published when the client's own idle detection (mouse/keyboard) flips a
connection between active and inactive — not on every ping. The server
aggregates across the user's connections: all inactive ⇒ `away`, any active ⇒
`online`.

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `connId` | string | yes | The connection whose activity changed. |
| `away` | boolean | yes | `true` marks the connection inactive, `false` active. |
| `timestamp` | number | no | Millis since Unix epoch (UTC). |

```json
{ "connId": "1f0a-uuid", "away": true, "timestamp": 1746518103000 }
```

### 7.4 Disconnect (publish, best-effort)

**Subject:** `chat.user.{account}.event.presence.{siteID}.bye`

Sent best-effort on tab close (`beforeunload`) for instant offline instead of
waiting for the liveness window to lapse. The sweeper is the backstop when it
does not arrive.

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `connId` | string | yes | The connection being closed. |
| `timestamp` | number | no | Millis since Unix epoch (UTC). |

### 7.5 Set / clear manual override (request/reply)

**Subject:** `chat.user.{account}.request.presence.{siteID}.manual.set`
**Reply:** standard `_INBOX.>`.

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `status` | string | yes | One of `online`, `away`, `busy`, `appear_offline`, or `""` to clear. Any other value → `bad_request`. |
| `timestamp` | number | no | Millis since Unix epoch (UTC). |

**Success reply:**

| Field | Type | Notes |
|-------|------|-------|
| `account` | string | Echoes the caller. |
| `status` | string | The override just set (or `""` when cleared). |
| `setAt` | number | Server-assigned millis (UTC) the override was set. |
| `effective` | string | The resolved effective status after applying the override. |

```json
{ "account": "alice", "status": "busy", "setAt": 1746518100000, "effective": "busy" }
```

### 7.6 Batch query — initial state (request/reply)

**Subject:** `chat.user.presence.{siteID}.query.batch` — addressed to **your own
local site**. You do **not** need to know or group accounts by their home site.
**Reply:** standard `_INBOX.>`.

Send one request with all the accounts you want, regardless of which site they
live on. The local site resolves each account's home site, serves locally-homed
accounts from its own store, and fans out **server-to-server in parallel** to
the peer sites that own the rest (via the internal
`chat.server.request.presence.{siteID}.query.batch` RPC), then aggregates. At
most **100 accounts** per request (configurable server-side); more →
`bad_request`.

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `accounts` | string[] | yes | ≤ 100 accounts, any mix of home sites. |

**Success reply:**

| Field | Type | Notes |
|-------|------|-------|
| `states` | [PresenceState](#presencestate)[] | One per requested account, in request order. |
| `timestamp` | number | Reply time, millis (UTC). |

The query is
best-effort for display: an account that is unknown to the directory, or whose
home site does not respond within the per-peer timeout, reports `offline` (its
`siteId` is still the resolved home site when known) rather than failing the
whole request. Only a failure of the local site's own store surfaces as an error.

### 7.7 Live state (subscribe)

**Subject:** `chat.user.presence.state.{account}` — the owning site publishes a
user's effective status (a [PresenceState](#presencestate)) here on every
change. The subject omits `siteID`: it is a global per-user event, so you
subscribe knowing only the account, without first resolving the user's home site
(cross-site delivery is routed by the gateway). The home site is still reported
in the `siteId` payload field.

```json
{ "account": "bob", "siteId": "site-b", "status": "away", "timestamp": 1746518105000 }
```

#### PresenceState

| Field | Type | Notes |
|-------|------|-------|
| `account` | string | The user. |
| `siteId` | string | The user's home site. |
| `status` | string | Effective status: `online` / `away` / `busy` / `offline`. |
| `timestamp` | number | Millis since Unix epoch (UTC) of the change. |

**Subscribe before you snapshot.** To avoid missing a transition between the
snapshot and the subscription, subscribe to the state subject(s) **first**, then
send the §7.6 batch query for current values.
