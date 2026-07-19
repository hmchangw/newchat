> Request/Reply and Events views of the chat client API — see also [client-api.md](../client-api.md).

<!-- last synced: client-api.md @ 117da0c -->

# Chat — Request/Reply Methods & Publish Operations

This document covers all client-initiated interactions:

- **Request/reply** — client publishes to `…request.…`, awaits a reply on `_INBOX.>`.
- **Publish operations** — client publishes with no synchronous reply (Send Message,
  presence lifecycle).

For the event payloads these operations trigger, see [events.md](events.md).
For connection, auth, shared schemas, and error reference, see [../client-api.md](../client-api.md).

---

## Table of contents

1. [HTTP — Connection & Auth](#http--connection--auth)
   - [POST /api/v1/auth](#post-apiv1auth)
   - [GET /api/userInfo](#get-apiuserinfo)
   - [GET /api/settings](#get-apisettings)
   - [POST /api/v1/file/setCookie](#post-apiv1filesetcookie)
   - [POST /api/v1/file/rooms/:roomId/upload/images](#post-apiv1fileroomsroomiduploadimages)
   - [POST /api/v1/file/rooms/:roomId/upload/file](#post-apiv1fileroomsroomiduploadfile)
   - [GET /api/v1/file/rooms/:roomId/file/:fileId](#get-apiv1fileroomsroomidfilefileid)
   - [GET /api/v1/file-upload/:fileId/:fileName](#get-apiv1file-uploadfileidfilename)
   - [Media Service — avatar endpoints](#media-service--avatar-endpoints)
   - [Media Service — emoji endpoints](#media-service--emoji-endpoints)
2. [HTTP — Botplatform Service](#http--botplatform-service)
3. [HTTP — Admin Service](#http--admin-service)
4. [room-service (§3.1)](#room-service)
5. [history-service (§3.2)](#history-service)
6. [search-service (§3.3)](#search-service)
7. [user-service (§3.4)](#user-service)
8. [media-service (§3.5)](#media-service)
9. [Publish operations](#publish-operations)
   - [Send Message](#send-message)
   - [Room Encryption Key Get](#room-encryption-key-get)
   - [Presence publishes](#presence-publishes)

---

## HTTP — Connection & Auth

### POST /api/v1/auth

**Endpoint:** `POST /api/v1/auth`
**Reply:** synchronous HTTP response

Exchanges an SSO token for a signed NATS user JWT. See
[../client-api.md §2.2](../client-api.md#22-http--post-apiv1auth) for the full schema, examples,
and error table.

**Emits:** `None — HTTP-only.`

---

### GET /api/userInfo

**Endpoint:** `GET /api/userInfo?account={account}`
**Reply:** synchronous HTTP response

Site discovery — called once per login before POST /api/v1/auth. Returns the home site's NATS
and auth-service connection coordinates. See
[../client-api.md §2.3](../client-api.md#23-http--get-apiuserinfo-portal-service).

**Emits:** `None — HTTP-only.`

---

### GET /api/settings

**Endpoint:** `GET /api/settings`
**Reply:** synchronous HTTP response

Deployment-level frontend configuration — the backend API generation to target (`apiVersion`)
and the OTEL telemetry base URL (`otelBaseUrl`). See
[../client-api.md §2.5](../client-api.md#25-http--get-apisettings-portal-service).

**Emits:** `None — HTTP-only.`

---

### POST /api/v1/file/setCookie

**Endpoint:** `POST /api/v1/file/setCookie`
**Reply:** synchronous HTTP response

Exchanges the `ssoToken` header for an `ssoToken` cookie so the browser can load
protected files via `<img src>` (which cannot send headers). Token is validated before
the cookie is issued. Credentialed request; caller's `Origin` must be in the server's
`CORS_ALLOWED_ORIGINS` allowlist. See
[../client-api.md §2.4](../client-api.md#post-apiv1filesetcookie).

**Emits:** `None — HTTP-only.`

---

### POST /api/v1/file/rooms/:roomId/upload/images

**Endpoint:** `POST /api/v1/file/rooms/:roomId/upload/images`
**Reply:** synchronous HTTP response

Uploads one or more protected inline images. `Content-Type: multipart/form-data`.
`ssoToken` header required; caller must be a room member. Returns per-file
success/failure in one `200`. See
[../client-api.md §2.4](../client-api.md#post-apiv1fileroomsroomiduploadimages).

**Emits:** `None — HTTP-only.`

---

### POST /api/v1/file/rooms/:roomId/upload/file

**Endpoint:** `POST /api/v1/file/rooms/:roomId/upload/file`
**Reply:** synchronous HTTP response

Uploads a single file (image/audio/video/document) and returns a render-ready
[Attachment](../client-api.md#attachment) for the client to embed in a `msg.send`
(§4) — pure HTTP, does **not** itself publish a message. `Content-Type:
multipart/form-data`. `ssoToken` header required; caller must be a room member. See
[../client-api.md §2.4](../client-api.md#post-apiv1fileroomsroomiduploadfile).

**Emits:** `None — HTTP-only.`

---

### GET /api/v1/file/rooms/:roomId/file/:fileId

**Endpoint:** `GET /api/v1/file/rooms/:roomId/file/:fileId`
**Reply:** synchronous HTTP response (raw file bytes, any type)

Downloads a protected file (image/audio/video/document). `ssoToken` required (header,
or the `ssoToken` cookie from `POST /api/v1/file/setCookie` for browser `<img>` downloads;
header wins); caller must be a room member. `drive_host` query param required.
Called with the `relativePath` (image upload) or `titleLink` (file upload)
returned by the upload endpoints. See
[../client-api.md §2.4](../client-api.md#get-apiv1fileroomsroomidfilefileid).

**Emits:** `None — HTTP-only.`

---

### GET /api/v1/file-upload/:fileId/:fileName

**Endpoint:** `GET /api/v1/file-upload/:fileId/:fileName`
**Reply:** synchronous HTTP response (raw file bytes, not JSON)

Downloads a previously-uploaded file by `fileId` (resolved via the `uploads`
collection, streamed from MinIO/S3); `fileName` is cosmetic. `ssoToken` required
(header, or the `ssoToken` cookie from `POST /api/v1/file/setCookie` for browser `<img>`
downloads; header wins); caller must be a member of the file's room. See
[../client-api.md §2.4](../client-api.md#get-apiv1file-uploadfileidfilename).

**Emits:** `None — HTTP-only.`

---

### Media Service — avatar endpoints

Public HTTP endpoints served by `media-service` (no `ssoToken`/auth required).
Full decision logic, redirect/caching rules, and the `PUT` upload contract are in
[../client-api.md §7](../client-api.md#7-media-service).

| Endpoint | Reply | Purpose |
|---|---|---|
| `GET /api/v1/avatar/:accountName` | synchronous HTTP (redirect, image bytes, or default SVG) | User/bot avatar; frontend also uses this for DM/botDM room avatars. |
| `GET /api/v1/avatar/room/:roomID` | synchronous HTTP (image bytes or default SVG) | Channel/discussion room avatar. |
| `PUT /api/v1/avatar/bot/:botName` | synchronous HTTP | Upload a bot's custom avatar. ⚠️ Unauthenticated — must be network-restricted. |

**Emits:** `None — HTTP-only.`

---

### Media Service — emoji endpoints

Public HTTP endpoints served by `media-service` (no `ssoToken`/auth required).
Full decision logic, limits, and response schemas are in
[../client-api.md §7](../client-api.md#7-media-service).

| Endpoint | Reply | Purpose |
|---|---|---|
| `GET /api/v1/emoji/:shortcode` | synchronous HTTP (image bytes, `304`, `307`, or `404`) | Serve a custom emoji image. Defaults to this cluster's site; optional lowercase `?siteid=` names a site — known remote `307`-redirects, unknown `404`. No generated default (unlike avatars). Cache-bust with `?v={etag}`. |
| `PUT /api/v1/emoji/:shortcode` | synchronous HTTP | Upload (upsert) a custom emoji — PNG/JPEG/GIF, env-capped size/dimensions. Always writes to this cluster's site. ⚠️ Unauthenticated; optional `?uploader={account}` is audit-only. |

**Emits:** `None — HTTP-only.`

---

## HTTP — Botplatform Service

Password login and session-token validation for bot/admin accounts, served by
`botplatform-service`. Any user may authenticate against any cluster (no home-site
gate). Full schemas, examples, and error tables are in
[../client-api.md §10](../client-api.md#10-botplatform-service); the portal-fronted
login is [../client-api.md §2.5](../client-api.md#25-http--post-apiv1login-portal-service).

| Endpoint | Reply | Purpose |
|---|---|---|
| `POST /api/v1/login` (portal-service) | synchronous HTTP | Web/mobile/desktop/admin password login; portal forwards to botplatform and returns a merged identity + home-site URL bundle (§2.5). |
| `POST /api/v1/login` (botplatform, bot SDK direct) | synchronous HTTP | Direct bot-SDK login; legacy `{userId, authToken, me}` shape (§10.1). |
| `POST /api/v1/auth/validate` | synchronous HTTP | Server-to-server session-token validation; returns the principal (§10.2). |

**Emits:** `None — HTTP-only.`

---

## HTTP — Admin Service

Account-management REST endpoints served by `admin-service`. All `/v1/admin/…`
routes require an admin session token (`Authorization: Bearer <authToken>`,
`admin` role + matching `siteId`). Full schemas, examples, and error tables are in
[../client-api.md §9](../client-api.md#9-admin-service).

| Endpoint | Reply | Purpose |
|---|---|---|
| `GET /v1/admin/users` | synchronous HTTP | List/search users (§9.1). |
| `POST /v1/admin/users` | synchronous HTTP | Create a user (§9.2). |
| `GET /v1/admin/users/:account` | synchronous HTTP | Get a user by account (§9.3). |
| `PATCH /v1/admin/users/:account` | synchronous HTTP | Update a user by account (§9.4). |
| `POST /v1/admin/users/:account/password` | synchronous HTTP | Admin set/reset a user's password by account (§9.5). |
| `GET /v1/admin/sessions?account=<account>` | synchronous HTTP | List an account's active sessions (§9.6). |
| `DELETE /v1/admin/sessions?account=<account>` | synchronous HTTP | Revoke all of an account's sessions (§9.7). |
| `DELETE /v1/admin/sessions/:sessionId?account=<account>` | synchronous HTTP | Revoke a single session (§9.8). |
| `GET /v1/admin/audit` | synchronous HTTP | List the audit log (§9.9). |

**Emits:** `None — HTTP-only.`

---

## room-service

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
| `chat.user.{account}.request.room.{roomID}.{siteID}.teams.call` | [Start Teams Room Call](#start-teams-room-call) |
| `chat.user.{account}.request.teams.{siteID}.call.user` | [Start Teams User Call](#start-teams-user-call) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.teams.meeting` | [Start Teams Meeting](#start-teams-meeting) |

All room-service methods: `{siteID}` must be the room's **origin siteID** (the site
that owns the room), not the caller's own site.

---

### Create Room

**Subject:** `chat.user.{account}.request.room.{siteID}.create`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Async-job RPC — sync reply only confirms acceptance; room is created by `room-worker`.
Room type is inferred server-side from the payload shape (`name` set → channel;
`name` empty + one `users` entry → DM/botDM). Creator account comes from the subject.
`X-Request-ID` header is **required** (rejected without it).

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | channels | Channel name (≤ 100 chars). Required for channel; empty for DM/botDM/self-DM. |
| `users` | string[] | no | Internal user IDs or accounts to enroll. For a DM, exactly one entry. For a **self-DM** (note-to-self), the caller themselves — required, an otherwise-empty request is rejected as empty. Bots are rejected in channels. |
| `orgs` | string[] | no | `channel` only. Org IDs expanded server-side to all members. |
| `channels` | [ChannelRef](../client-api.md#channelref)[] | no | `channel` only. Source channels whose members are copied in. |

Room type is inferred: `name` set → channel; `name` empty + one `users` entry →
DM/botDM; `name` empty + `users` is just the caller → **self-DM**, a
single-member `dm` room, one-per-user.

#### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | `"accepted"` (new room) or `"exists"` (DM/self-DM already existed). |
| `roomId` | string | The room ID. Channel: 17-char base62. DM/botDM: sorted concat of the two internal user IDs. Self-DM: the requester's own user ID concatenated with itself. |
| `roomType` | string | `"channel"`, `"dm"`, or `"botDM"`. |

```json
{ "status": "accepted", "roomId": "01970a4f8c2d7c9aQ", "roomType": "channel" }
```

DM/self-DM already exists: `{ "status": "exists", "roomId": "<existing room id>" }`

#### Errors

- `"X-Request-ID header is required"` (`bad_request`, `request_id_required`)
- `"channel name is required"` / channel name > 100 chars
- `"bots cannot be added to a channel"` / `"bot not available"`
- `user "<account>": user not found` / `org "<orgId>": invalid org`
- `"exceeds maximum capacity (N): would create M members"`

**Emits:** [`AsyncJobResult`](events.md#asyncjobresult--async-completion) (`operation: "room.create"`), [`subscription.update`](events.md#subscriptionupdate--membership--state-changes) (`action: "added"` — one per enrolled member), [`room.key`](events.md#roomkey--room-encryption-key-delivery) (channel rooms — one per enrolled local member), `new_message` system messages (`room_created`, `members_added`) → [events.md](events.md#new_message-roomevent)

---

### Add Members

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.add`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Async-job RPC. `X-Request-ID` recommended (required to receive `AsyncJobResult`).

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `roomId` | string | no | Optional echo; server derives from subject. |
| `users` | string[] | no | Internal user IDs or accounts to add. |
| `orgs` | string[] | no | Org IDs to add (expanded to all members). |
| `channels` | [ChannelRef](../client-api.md#channelref)[] | no | Bulk source channels. |
| `history.mode` | string | no | `"none"` (default) or `"all"` — controls history visibility for new members. |

#### Success response

`{ "status": "accepted" }`

#### Errors

Synchronous: requester not in room, room full, restricted + not owner, bots in channel,
user/org not found.

```json
{ "code": "conflict", "reason": "max_room_size_reached", "error": "room is at maximum capacity" }
```

**Emits:** [`AsyncJobResult`](events.md#asyncjobresult--async-completion) (`operation: "room.member.add"`), [`subscription.update`](events.md#subscriptionupdate--membership--state-changes) (`action: "added"` — one per newly subscribed member), [`room.key`](events.md#roomkey--room-encryption-key-delivery) (channel rooms), [`member_added`](events.md#member_added-memberaddevent) (on `chat.room.{roomID}.event.member`), `new_message` system message (`members_added`) → [events.md](events.md#new_message-roomevent)

---

### Remove Member

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.remove`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Async-job RPC. `X-Request-ID` recommended.

#### Request body

Exactly one of `account` or `orgId` must be set.

| Field | Type | Required | Notes |
|---|---|---|---|
| `account` | string | one-of | Remove a single user. |
| `orgId` | string | one-of | Remove all users in this org. |
| `roomId` | string | no | Server derives from subject. |

#### Success response

`{ "status": "accepted" }`

#### Errors

Synchronous: neither/both of `account`/`orgId` set; requester not an owner; target is
last member; org member cannot leave individually.

**Emits:** [`AsyncJobResult`](events.md#asyncjobresult--async-completion) (`operation: "room.member.remove"` or `"room.member.remove_org"`), [`subscription.update`](events.md#subscriptionupdate--membership--state-changes) (`action: "removed"` — one per removed account), [`room.key`](events.md#roomkey--room-encryption-key-delivery) (channel rooms — key rotated; surviving members receive new event), [`member_left` / `member_removed`](events.md#member_left--member_removed-memberremoveevent) (on `chat.room.{roomID}.event.member`), `new_message` system message → [events.md](events.md#new_message-roomevent)

---

### Update Member Role

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.role-update`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

**Synchronous RPC** — role change is applied inline before reply. No `AsyncJobResult`.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `account` | string | yes | Account of the user whose role is changing. |
| `newRole` | string | yes | `"owner"` (promote) or `"member"` (demote). |
| `roomId` | string | no | Server derives from subject. |

#### Success response

`{ "status": "ok" }`

#### Errors

Not an owner; target not a member; invalid `newRole`; already owner (promote); not owner (demote); last-owner guard; org-only member cannot be promoted.

**Emits:** [`subscription.update`](events.md#subscriptionupdate--membership--state-changes) (`action: "role_updated"` — to the target user only) → [events.md](events.md#subscriptionupdate--membership--state-changes)

---

### Rename Room

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.room.rename`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Async-job RPC. `X-Request-ID` header **required**. Caller must be a room owner or
platform admin. Channel rooms only.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `newName` | string | yes | New room name. 1–100 characters after trimming whitespace. |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"accepted"`. |
| `requestId` | string | Echoes the `X-Request-ID` header. |

#### Errors

- `"invalid name"` — empty after trimming or > 100 chars
- `"room not found"` — no room matches subject `{roomID}`
- `"rename is only allowed in channel rooms"`
- `"only owners or platform admins can rename a channel"`
- `"X-Request-ID header is required"` (`bad_request`, `request_id_required`)

**Emits:** [`room_renamed`](events.md#room_renamed-roomrenamedroomevent) (on `chat.room.{roomID}.event`), [`AsyncJobResult`](events.md#asyncjobresult--async-completion) (`operation: "room.rename"`) → [events.md](events.md)

---

### List Members

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.list`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `limit` | number | no | Caps returned members; must be > 0 if set. |
| `offset` | number | no | Pagination; must be ≥ 0 if set. |
| `enrich` | boolean | no | When `true`, populates display fields on each entry. |

#### Success response

`{ "members": RoomMember[] }` — see `RoomMember` / `RoomMemberEntry` schemas in
[../client-api.md §3.1](../client-api.md#list-members).

#### Errors

`"only room members can perform this action"`, `"limit must be > 0"`,
`"offset must be >= 0"`.

**Emits:** None — reply only.

---

### Get Member Statuses

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.statuses`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `limit` | number | no | Upper bound on returned rows. Default: `min(3, room.userCount)`. Must be > 0 and ≤ room.userCount. |

#### Success response

`{ "members": MemberStatus[] }` — members with non-empty `statusText` only.
See `MemberStatus` schema in [../client-api.md §3.1](../client-api.md#get-member-statuses).

#### Errors

`"only room members can perform this action"`, `"limit must be > 0 and <= room user count"`.

**Emits:** None — reply only.

---

### Get Mentionable Subscriptions

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.subscription.mentionable`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Used by the message composer's `@…` mention autocomplete. Caller and platform-admin
accounts are excluded. Returns `user` and `app` rows.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `limit` | number | no | Default: `min(3, room.userCount + room.appCount)`. Must be > 0 and ≤ combined count. |
| `filter` | string | no | Literal substring, case-insensitive. Matched against account, names, app name. |

#### Success response

`{ "subscriptions": MentionableSubscription[] }` — see `MentionableSubscription` /
`MentionableApp` schemas in [../client-api.md §3.1](../client-api.md#get-mentionable-subscriptions).

#### Errors

`"only room members can perform this action"`, `"limit must be > 0 and <= room user count + app count"` (fires only for a non-positive limit; an over-cap positive limit is clamped).

**Emits:** None — reply only.

---

### Mark Messages Read

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.message.read`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Synchronous RPC. Advances the caller's `lastSeenAt`, recomputes the per-subscription
`alert` flag. No request body required.

#### Success response

`{ "status": "accepted" }`

#### Errors

`"only room members can perform this action"` (`forbidden`, `not_room_member`).

**Emits:** [`subscription.update`](events.md#subscriptionupdate--membership--state-changes) (`action: "read"` — to the reader, non-bot only; not fired on early-return paths), [`message_read`](events.md#message_read-messagereadevent) (floor advance events — only when `Room.MinUserLastSeenAt` changes) → [events.md](events.md)

---

### Mark Thread as Read

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.message.thread.read`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Synchronous RPC. Clears one thread's unread state for the caller.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `threadId` | string | Required. The thread's parent message ID. |

#### Success response

`{ "status": "accepted" }`

#### Errors

`"only room members can perform this action"`, `"thread subscription not found"`,
`"threadId is required"`.

**Emits:** [`thread_message_read`](events.md#thread_message_read) (only when the thread's
read floor `minUserLastSeenAt` changes; routed by the **parent** room's type) →
[events.md](events.md). Cross-site users may additionally observe a delayed cache update
via the internal cross-site inbox flow (not a client-visible event).

---

### Toggle Mute

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.mute.toggle`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Synchronous RPC. Flips `Subscription.muted`. Every successful call toggles the bit —
clients must debounce. No request body required.

#### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"ok"`. |
| `muted` | boolean | Resulting value of `Subscription.muted`. |

#### Errors

`"only room members can perform this action"`, `"invalid mute-toggle subject: …"`.

**Emits:** [`subscription.update`](events.md#subscriptionupdate--membership--state-changes) (`action: "mute_toggled"` — to the requester for other sessions) → [events.md](events.md)

---

### Toggle Favorite

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.favorite.toggle`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Synchronous RPC. Flips `Subscription.favorite`. Every successful call toggles the bit —
clients must debounce. No request body required.

#### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"ok"`. |
| `favorite` | boolean | Resulting value of `Subscription.favorite`. |

#### Errors

`"only room members can list members"`, `"invalid favorite-toggle subject: …"`.

**Emits:** [`subscription.update`](events.md#subscriptionupdate--membership--state-changes) (`action: "favorite_toggled"` — to the requester for other sessions) → [events.md](events.md)

---

### Read Message Receipts

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.message.read-receipt`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Synchronous, sender-only RPC. Returns local-site users whose `subscription.lastSeenAt`
is at or after the target message's `createdAt`. The message author is excluded from
results.

For a **thread-only reply** (a threaded reply not mirrored to the channel, i.e. not
`tshow`), readers are resolved from **thread** read-state (`thread_subscriptions.lastSeenAt`)
instead of the room's — the reply never appears in the channel, so a member's channel
read-position is not evidence they saw it. Channel messages and `tshow` replies use room
read-state.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Required. |

#### Success response

`{ "readers": ReadReceiptEntry[] }` — see `ReadReceiptEntry` schema in
[../client-api.md §3.1](../client-api.md#read-message-receipts).

#### Errors

`"only room members can perform this action"`, `"message not found"`,
`"message does not belong to this room"`, `"only the message sender can view read receipts"`,
`"invalid request: messageId is required"`, `"read receipts are temporarily unavailable"`
(`unavailable`, `read_receipts_unavailable` — history service unreachable), `"message is
outside access window"` (`forbidden`, `outside_access_window` — predates the requester's
`historySharedSince`).

**Emits:** None — reply only.

---

### List Org Members

**Subject:** `chat.user.{account}.request.orgs.{orgID}.{siteID}.members`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Returns all users whose `sectId` OR `deptId` equals `{orgID}` on the given site.
No request body.

#### Success response

`{ "members": OrgMember[] }` — see `OrgMember` schema in
[../client-api.md §3.1](../client-api.md#list-org-members).

#### Errors

`{ "code": "bad_request", "error": "invalid org" }`

**Emits:** None — reply only.

---

### Get Room App Tabs

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.app.tabs`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Empty body. Returns apps with `channelTab.enabled=true` AND `channelTab.default=true`,
sorted by `channelTab.name asc`.

#### Success response

`{ "apps": RoomApp[] }` — see `RoomApp` / `AppAssistant` schemas in
[../client-api.md §3.1](../client-api.md#get-room-app-tabs).

#### Errors

`"not authorized to access this room's apps"`, `"response payload exceeds maximum size"`.

**Emits:** None — reply only.

---

### Get Room App Command Menu

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.app.cmd-menu`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Empty body. Returns one entry per bot subscribed in the room whose owning app has
`assistant.enabled=true`.

#### Success response

`{ "appAssistants": RoomAppAssistant[] }` — see `RoomAppAssistant` / `CmdBlock` /
`CmdModal` schemas in [../client-api.md §3.1](../client-api.md#get-room-app-command-menu).

#### Errors

Same envelope and sentinels as Get Room App Tabs.

**Emits:** None — reply only.

---

### Start Teams Room Call

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.teams.call`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

External client label: `POST /api/v1/calls/room`. Builds a Teams deep-link call to every
other room member (caller excluded). No Graph API call — link derived from member list.
Empty body.

#### Success response

`{ "joinUrl": "https://teams.microsoft.com/l/call/0/0?users=<comma-joined emails>" }`

#### Errors

| Reason | Code | When |
|---|---|---|
| — | `unauthenticated` | Requester account missing from subject. |
| — | `bad_request` | `roomId` empty (malformed subject). |
| `not_room_member` | `forbidden` | Caller not a member. |
| `target_not_member` | `not_found` | No other callable members in the room. |
| `max_room_size_reached` | `conflict` | More than `ROOM_MEMBERS_CALL_LIMIT` (20) other members. |

**Emits:** None — reply only.

---

### Start Teams User Call

**Subject:** `chat.user.{account}.request.teams.{siteID}.call.user`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

External client label: `POST /api/v1/calls/user`. Builds a Teams 1:1 call deep-link.
No Graph API call.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `accountName` | string | yes | Target user's account. |

#### Success response

`{ "joinUrl": "https://teams.microsoft.com/l/call/0/0?users=<email>" }`

#### Errors

| Reason | Code | When |
|---|---|---|
| — | `unauthenticated` | Requester account missing from subject. |
| — | `bad_request` | `accountName` empty. |

**Emits:** None — reply only.

---

### Start Teams Meeting

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.teams.meeting`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

External client label: `POST /api/v1/meetings`. Creates a Teams `onlineMeeting` via
Graph API. **Idempotent per room** — repeated calls for the same room return the same
meeting. The system message is published only on first creation.

#### Request body

Optional; `roomId` field optional echo.

#### Success response

| Field | Type | Notes |
|---|---|---|
| `id` | string | The Graph `onlineMeeting` ID. |
| `joinUrl` | string | The meeting's join web URL. |

#### Errors

| Reason | Code | When |
|---|---|---|
| — | `unauthenticated` | Requester account missing from subject. |
| — | `bad_request` | `roomId` empty. |
| `not_room_member` | `forbidden` | Caller not a member. |
| `max_room_size_reached` | `conflict` | More than `ROOM_MEMBERS_LIMIT` (500) members. |
| — | `internal` | Teams not configured, or Graph create failed. |

**Emits:** On first creation, a `teams_meet_started` system message → [events.md § new_message](events.md#new_message-roomevent). No event on idempotent repeat calls.

---

## history-service

| RPC subject | Method |
|---|---|
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.history` | [Load History](#load-history) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.next` | [Load Next Messages](#load-next-messages) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.surrounding` | [Load Surrounding Messages](#load-surrounding-messages) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get` | [Get Message By ID](#get-message-by-id) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get.ids` | [Get Messages By IDs](#get-messages-by-ids) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.edit` | [Edit Message](#edit-message) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.delete` | [Delete Message](#delete-message) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.pin` | [Pin Message](#pin-message) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.unpin` | [Unpin Message](#unpin-message) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.pinned.list` | [List Pinned Messages](#list-pinned-messages) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.react` | [React to Message](#react-to-message) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread` | [Get Thread Messages](#get-thread-messages) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread.parent` | [Get Thread Parent Messages](#get-thread-parent-messages) |

**Common request fields (read RPCs):** `limit` (default 20 for Load History / Load Next /
Get Thread, 50 for Load Surrounding; max 100) and `meta` (room time hints to skip a Mongo
lookup; `{ lastMsgAt, createdAt }`). See [../client-api.md §3.2](../client-api.md#32-history-service).

**Common errors:** `forbidden` (`not subscribed to room`), `not_found` (`room/message not
found`), `bad_request` (invalid pagination cursor), `internal`. A reply that would exceed
the transport's max payload returns `internal`/`response_too_large` instead of the success
body (most likely with a high `limit`) — retry with a smaller `limit`.

Message schema: see [../client-api.md § Message schema](../client-api.md#message-schema).

---

### Load History

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.history`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

#### Request body

| Field | Type | Notes |
|---|---|---|
| `before` | number | Epoch ms (UTC). Returns messages with `createdAt < before`. Omit for "now". |
| `limit` | number | Page size (default 20, max 100). |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | Message[] | Most-recent first. |
| `minUserLastSeenAt` | number | Optional. UTC ms. The room's strict read floor — present only when every member has read. |

**Emits:** None — reply only.

---

### Load Next Messages

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.next`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Forward-pagination counterpart to Load History.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `after` | number | Epoch ms (UTC). Returns messages with `createdAt > after`. Omit for no lower bound. |
| `limit` | number | Page size (default 20, max 100). |
| `cursor` | string | Required. Pagination cursor; empty string for first page. |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | Message[] | Oldest-first. |
| `nextCursor` | string | Optional. Opaque cursor for next page. |
| `hasNext` | boolean | `true` if more messages exist. |

**Emits:** None — reply only.

---

### Load Surrounding Messages

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.surrounding`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

#### Request body

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Required. The central message to center the window on. |
| `limit` | number | Window size including the central message (default 50, max 100). |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | Message[] | Window centered on `messageId`, oldest-first. |
| `moreBefore` | boolean | `true` if more messages exist before the window. |
| `moreAfter` | boolean | `true` if more messages exist after the window. |

**Emits:** None — reply only.

---

### Get Message By ID

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

#### Request body

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Required. |

#### Success response

A single `Message` object.

**Emits:** None — reply only.

---

### Get Messages By IDs

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get.ids`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

All IDs must belong to the same room. IDs not found or outside access window are silently
omitted.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `messageIds` | string[] | Required. Non-empty; max 100. |

#### Success response

`{ "messages": Message[] }` — in request order; missing IDs are absent.

#### Errors

`"messageIds must not be empty"`, `"too many messageIds"`, `"not subscribed to room"`.

**Emits:** None — reply only.

---

### Edit Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.edit`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Only the original sender may edit.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Required. |
| `newMsg` | string | Required. Must be non-empty and within size limit. |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Echoes input. |
| `editedAt` | number | Epoch ms (UTC). |

#### Errors

`"only the sender can edit"` (`forbidden`), `"message not found"` (`not_found`),
`"newMsg must not be empty"`, `"newMsg exceeds maximum size"`.

**Emits:** [`message_edited`](events.md#message_edited-editroomevent) → [events.md](events.md)

---

### Delete Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.delete`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Soft-delete (row preserved for audit). Only the original sender may delete. Idempotent.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Required. |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Echoes input. |
| `deletedAt` | number | Epoch ms (UTC). For already-deleted: original deletion time. |

#### Errors

`"only the sender can delete"` (`forbidden`), `"message not found"` (`not_found`).

**Emits:** [`message_deleted`](events.md#message_deleted-deleteroomevent), [`thread_metadata_updated`](events.md#thread_metadata_updated-threadmetadataupdatedevent) (thread replies only) → [events.md](events.md)

---

### Pin Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.pin`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Idempotent — pinning an already-pinned message returns success without re-publishing.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Required. |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Echoes input. |
| `pinnedAt` | number | Epoch ms (UTC). For already-pinned: original pin time. |

#### Errors

| Code | Reason | Cause |
|---|---|---|
| `forbidden` | `pin_disabled` | Kill-switch `PIN_ENABLED=false`. |
| `forbidden` | `not_subscribed` | Caller not subscribed. |
| `forbidden` | `pin_room_too_large` | Exceeds `LARGE_ROOM_THRESHOLD`; owners/admins/bots exempt. |
| `forbidden` | `pin_limit_reached` | Room at `MAX_PINNED_PER_ROOM` (default 10) hard cap. |
| `not_found` | — | Message not found, wrong room, or deleted. |
| `internal` | — | Mongo/Cassandra failure. |

**Emits:** [`message_pinned`](events.md#message_pinned--message_unpinned-pinstateroomevent) → [events.md](events.md)

---

### Unpin Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.unpin`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Idempotent. Soft-deleted messages are still unpinnable.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Required. |

#### Success response

`{ "messageId": "<id>" }`

#### Errors

Same table as Pin Message except no `pin_limit_reached` error; also `not_found` for
messages that don't exist or belong to a different room.

**Emits:** [`message_unpinned`](events.md#message_pinned--message_unpinned-pinstateroomevent) → [events.md](events.md)

---

### List Pinned Messages

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.pinned.list`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Most-recently-pinned first. Kill-switch and large-room override do **not** apply to
listing. Caller with a `historySharedSince` lower bound receives redacted stubs for pins
whose underlying message predates their access window.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `cursor` | string | Omit for first page. |
| `limit` | number | Required. Max per page. |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | Message[] | Pinned messages, most-recently-pinned first. Pre-access pins appear as redacted stubs. |
| `nextCursor` | string | Opaque token. Empty when no more pages. |
| `hasNext` | boolean | |

**Emits:** None — reply only.

---

### React to Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.react`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Toggle — server decides add vs remove by checking the calling user's existing reactor
state. Can always **remove** from a soft-deleted message; cannot **add** to one.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Required. |
| `shortcode` | string | Required. Bare shortcode without colons (`thumbsup`, `acme_party`). Must match `^[a-z0-9_+-]{1,32}$` after NFC normalisation. Format-only validation — no registration check; clients offer only picker-sourced shortcodes (standard set + local [`emoji.list`](#emojilist)). |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | Echoes input. |
| `shortcode` | string | Canonical NFC-normalised form. |
| `action` | string | `"added"` or `"removed"`. |
| `reactedAt` | number | Epoch ms (UTC). |

#### Errors

`"messageId is required"`, `"shortcode is required"`, `"invalid reaction shortcode"`
(malformed format), `"message not found"`, `"not subscribed to room"`.

**Emits:** [`message_reacted`](events.md#message_reacted-reactroomevent) (channel `chat.room.{roomID}.event`; DM `chat.user.{account}.event.room` per non-bot member), [`notification`](events.md#notification--reaction-notification) (to message author on add only) → [events.md](events.md)

---

### Get Thread Messages

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

#### Request body

| Field | Type | Notes |
|---|---|---|
| `threadMessageId` | string | Required. The top-level thread message ID (must be a thread parent, not a reply). |
| `cursor` | string | Optional. Pagination cursor; omit for first page. |
| `limit` | number | Page size (default 20, max 100). |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | Message[] | Replies, oldest-first. |
| `nextCursor` | string | Optional. Opaque cursor for next page. |
| `hasNext` | boolean | |
| `parentMessage` | Message | Optional. Thread-parent message. Present when accessible. |
| `minUserLastSeenAt` | number | Optional. UTC ms. Thread room read floor — present only when every subscriber has read. |

**Emits:** None — reply only.

---

### Get Thread Parent Messages

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread.parent`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Lists parent messages of threads the user has subscribed to (or all threads, depending
on filter). Drives a "Threads" tab in the client.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `filter` | string | Required. `"all"`, `"following"`, or `"unread"`. |
| `offset` | number | Required. `0` for first page. |
| `limit` | number | Required. Max thread parents to return. |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `parentMessages` | Message[] | Ordered by most-recent reply activity. |
| `total` | number | Raw count before access filtering. |

**Emits:** None — reply only.

---

## search-service

| RPC subject | Method |
|---|---|
| `chat.user.{account}.request.search.{siteID}.messages` | [search.messages](#searchmessages) |
| `chat.user.{account}.request.search.{siteID}.rooms` | [Search Rooms](#search-rooms) |
| `chat.user.{account}.request.search.{siteID}.apps` | [Search Apps](#search-apps) |
| `chat.user.{account}.request.search.{siteID}.users` | [Search Users](#search-users) |

`{siteID}` is the requester's home site. The supercluster routes the request to the
search-service on that site.

---

### search.messages

**Subject:** `chat.user.{account}.request.search.{siteID}.messages`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Full-text message search. Auto-scoped to rooms the user is a member of. May include
messages from remote sites.

> **Breaking change (v2):** Response changed from `{total, results}` to `{messages, total}`.
> The `results` field no longer exists.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | yes | Full-text query. Empty string rejected. |
| `roomIds` | string[] | no | Scope to specific rooms. Unknown or inaccessible rooms silently excluded. |
| `size` | integer | no | Page size. Default 25, max 100. |
| `offset` | integer | no | Page offset. Default 0. |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | SearchMessage[] | Per-hit projection. |
| `total` | integer | Total matching hits (may exceed `messages.length`). |

`SearchMessage` fields: `messageId`, `roomId`, `siteId`, `userAccount`, `content`,
`createdAt`, `editedAt` (nullable), `updatedAt` (nullable), `threadParentMessageId`
(omitted when not a reply), `threadParentMessageCreatedAt` (omitted when not a reply).
All sourced from ES — no Mongo round-trip.

#### Errors

`"query is empty"` (`bad_request`), `"internal"` (ES backend failure).

**Emits:** None — reply only.

---

### Search Rooms

**Subject:** `chat.user.{account}.request.search.{siteID}.rooms`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Full-text search across rooms the caller is subscribed to. Results from the spotlight
ES index.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | yes | Case-insensitive prefix/substring on room name. Whitespace-only rejected. |
| `roomType` | string | no | `"all"` (default), `"channel"`, or `"dm"`. `"app"` and unknown values rejected. |
| `size` | number | no | Default 25, max 100. |
| `offset` | number | no | Default 0. |

#### Success response

`{ "rooms": SearchRoom[] }` where `SearchRoom` has: `roomId`, `name`, `roomType`, `siteId`.

**Emits:** None — reply only.

---

### Search Apps

**Subject:** `chat.user.{account}.request.search.{siteID}.apps`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Current behavior (prototype): not yet subscription-scoped — every matching app is
returned. Planned behavior: scoped to apps the caller has subscribed to.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | yes | Case-insensitive substring on `app.name`. Whitespace-only rejected. |
| `assistantEnabled` | boolean (nullable) | no | Filter by `app.assistant.enabled`. |
| `size` | integer | no | Default 25, max 100. |
| `offset` | integer | no | Default 0. |

#### Success response

`{ "apps": App[] }` — see `App` / `AppChannelTab` / `AppSponsor` schemas in
[../client-api.md §3.3](../client-api.md#search-apps).

**Emits:** None — reply only.

---

### Search Users

**Subject:** `chat.user.{account}.request.search.{siteID}.users`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Proxy search via third-party HR endpoint. Company-scoping enforced by the HR endpoint.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | yes | Forwarded to HR endpoint. Whitespace-only rejected. |
| `offset` | integer | no | Default 0, non-negative. |
| `limit` | integer | no | Default 25, max 100, non-negative. |

#### Success response

Top-level JSON array of `SearchUser` (`account`, `engName`?, `chineseName`?).

**Emits:** None — reply only.

---

## user-service

All user-service subjects: `chat.user.{account}.request.user.{siteID}.<area>.<action>`,
except `me` — a single-token self-lookup (`chat.user.{account}.request.user.{siteID}.me`).
No client-facing events are emitted.

| RPC subject | Method |
|---|---|
| `chat.user.{account}.request.user.{siteID}.me` | [me](#me) |
| `chat.user.{account}.request.user.{siteID}.status.getByName` | [status.getByName](#statusgetbyname) |
| `chat.user.{account}.request.user.{siteID}.profile.getByName` | [profile.getByName](#profilegetbyname) |
| `chat.user.{account}.request.user.{siteID}.status.set` | [status.set](#statusset) |
| `chat.user.{account}.request.user.{siteID}.subscription.list` | [subscription.list](#subscriptionlist) |
| `chat.user.{account}.request.user.{siteID}.subscription.getChannels` | [subscription.getChannels](#subscriptiongetchannels) |
| `chat.user.{account}.request.user.{siteID}.subscription.getDM` | [subscription.getDM](#subscriptiongetdm) |
| `chat.user.{account}.request.user.{siteID}.subscription.getByRoomID` | [subscription.getByRoomID](#subscriptiongetbyroomid) |
| `chat.user.{account}.request.user.{siteID}.subscription.count` | [subscription.count](#subscriptioncount) |
| `chat.user.{account}.request.user.{siteID}.subscription.setAppSubscription` | [subscription.setAppSubscription](#subscriptionsetappsubscription) |
| `chat.user.{account}.request.user.{siteID}.apps.list` | [apps.list](#appslist) |
| `chat.user.{account}.request.user.{siteID}.apps.categories` | [apps.categories](#appscategories) |
| `chat.user.{account}.request.user.{siteID}.thread.list` | [List User Threads](#list-user-threads) |
| `chat.user.{account}.request.user.{siteID}.thread.unread.summary` | [Get Thread Unread Summary](#get-thread-unread-summary) |
| `chat.user.{account}.request.user.{siteID}.thread.read.all` | [Clear All Thread Unread](#clear-all-thread-unread) |

---

### me

**Subject:** `chat.user.{account}.request.user.{siteID}.me`

Returns the **calling** user's own status view plus effective presence
(resolved from user-presence-service; degrades to `offline` on lookup failure
rather than erroring).

#### Request body

None. Any payload is ignored.

#### Success response

`{ "account", "statusText", "statusIsShow", "chineseName"?, "engName"?, "presence" }`

`chineseName`/`engName` are **omitted** when the user record has no value (never
empty strings). `presence` is one of `online` / `away` / `busy` / `offline` /
`in-call`; `offline` when unknown or degraded.

#### Errors

`not_found` (user not found), `internal` (user-status read failed — the only
`internal` source). A failed presence lookup never errors; it degrades to
`presence: "offline"` in a success response.

**Emits:** None.

---

### status.getByName

**Subject:** `chat.user.{account}.request.user.{siteID}.status.getByName`

Fetches status and display-name fields for a named user.

#### Request body

`{ "name": "alice" }`

#### Success response

`{ "account", "statusText", "statusIsShow", "chineseName"?, "engName"? }`

#### Errors

`not_found` (user not found), `internal`.

**Emits:** None.

---

### profile.getByName

**Subject:** `chat.user.{account}.request.user.{siteID}.profile.getByName`

Identical to `status.getByName` by design — same request, same response, same errors.

**Emits:** None.

---

### status.set

**Subject:** `chat.user.{account}.request.user.{siteID}.status.set`

Sets the caller's status. Returns the updated status view.

#### Request body

`{ "text": "Working from home", "isShow": true }`

`text` max 512 bytes; empty string clears.

#### Success response

Same shape as `status.getByName`.

#### Errors

`"status text too long"` (`bad_request`), `"user not found"` (`not_found`).

**Emits:** None client-facing. (A server-side cross-site federation update fires
internally but is not delivered to clients.)

---

### subscription.list

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.list`

Returns the user's sidebar subscriptions. **Room-info-enriched** — see
[../client-api.md §3.4 Enrichment behavior](../client-api.md#enrichment-behavior).

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `type` | string | yes | `"current"` (active rooms), `"rooms"` (DM+channel), `"apps"` (botDM). |
| `favorite` | boolean | no | Filter to favorited only; also pins the self-DM first. |
| `updatedWithinDays` | number | no | `rooms`-type only. Filters to rooms whose `lastMsgAt` is within N days. Non-negative. |
| `includeLastMessage` | boolean | no | Embed each room's `lastMessage`. Omitted ⇒ include (default); `false` ⇒ skip the last-message resolve. |
| `offset` | integer | no | Zero-based index of first record. Negative ⇒ `0`. Default `0`. |
| `limit` | integer | no | Page size. Omitted/≤0 ⇒ `SUBSCRIPTION_DEFAULT_LIMIT` (default `40`); capped at `MAX_SUBSCRIPTION_LIMIT` (default `1000`). |

#### Success response

| Field | Type | Notes |
|---|---|---|
| `subscriptions` | Subscription[] | One page of room-info-enriched records, ordered by `lastMsgAt` desc. |
| `hasMore` | boolean | `true` when another page follows. Advance `offset` by your `limit` for the next page. |

Per-room-type fields: channel rows add `name` (channel name); DM rows add `hrInfo`;
botDM rows add `app` (AppSubscription). See
[../client-api.md §3.4](../client-api.md#subscriptionlist) for the full schema + example.

**Emits:** None.

---

### subscription.getChannels

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.getChannels`

Returns channel subscriptions containing the caller and specified accounts. Room-info-enriched.
Exactly one of `membersContain` or `accountNames` must be set.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `membersContain` | string | Return channels containing this single account. |
| `accountNames` | string[] | Return channels where ALL accounts (+ caller) are members. Max 100. Bot accounts ignored. |
| `offset` | integer | Zero-based index of first record. Negative ⇒ `0`. Default `0`. |
| `limit` | integer | Page size. Omitted/≤0 ⇒ `SUBSCRIPTION_DEFAULT_LIMIT` (default `40`); capped at `MAX_SUBSCRIPTION_LIMIT` (default `1000`). |

#### Success response

Same paginated shape as `subscription.list` — `{ "subscriptions": [...], "hasMore": <bool> }`.

#### Errors

Both/neither fields set (`bad_request`), too many `accountNames` (> 100).

**Emits:** None.

---

### subscription.getDM

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.getDM`

Returns the DM subscription with a named counterpart. Room-info-enriched.

#### Request body

`{ "accountName": "bob" }` — must not be a bot (`.bot` suffix) or platform (`p_` prefix) account.

#### Success response

`{ "subscription": Subscription + hrInfo }` — `hrInfo` is the DM counterpart's HR record.

#### Errors

`"accountName required"`, `"invalid DM target"` (`bad_request`, `invalid_dm_target`),
`"dm not found"` (`not_found`, `subscription_not_found`).

**Emits:** None.

---

### subscription.getByRoomID

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.getByRoomID`

Returns the caller's subscription for one room as a 0-or-1-element list. Absence is
normal — **not** an error. Room-info-enriched.

#### Request body

`{ "roomId": "alice_bob" }`

#### Success response

`{ "subscriptions": Subscription[], "total": 0|1 }`

#### Errors

`"roomId required"` (`bad_request`).

**Emits:** None.

---

### subscription.count

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.count`

Returns the count of active subscriptions (non-muted DM/channel; non-muted + subscribed
botDM). Soft-deleted rooms excluded.

#### Request body

`{ "unread": true }` — when `true`, returns active rooms with unread messages or unread
followed threads (at most +1 per room; muted excluded).

#### Success response

`{ "count": 5 }`

**Emits:** None.

---

### subscription.setAppSubscription

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.setAppSubscription`

PUT-like idempotent endpoint to subscribe or unsubscribe from a bot app.

#### Request body

`{ "appId": "calendar-app", "subscribed": true }`

#### Success response

`{ "success": true }`

#### Errors

`"appId required"`, `"app not found"` (`not_found`, `app_not_found`),
`"app has no assistant"` (`bad_request`, `app_disabled`).

**Emits:** None.

---

### apps.list

**Subject:** `chat.user.{account}.request.user.{siteID}.apps.list`

Returns a page of apps, each annotated with `isSubscribed`. Sorted by name.

#### Request body

`{ "limit": 20, "offset": 0 }` (optional, defaults apply).

#### Success response

`{ "apps": AppListItem[], "hasMore": boolean }` where `AppListItem` is an `App` record
plus `isSubscribed: boolean`, and `hasMore` signals another page follows (offset-based;
advance `offset` by your `limit`). See [../client-api.md §3.4](../client-api.md#appslist).

**Emits:** None.

---

### apps.categories

**Subject:** `chat.user.{account}.request.user.{siteID}.apps.categories`

Returns the full fab-domain → site mapping used to group apps in the UI,
sorted by `name` ascending (rows sharing a `name` are ordered by `id`, so
ordering is deterministic across calls). Global, slow-changing reference
data, populated out-of-band; an unpopulated site returns `{ "categories": [] }`.

#### Request body

None — send an empty payload.

#### Success response

`{ "categories": AppCategory[] }` where `AppCategory` is `{ "id", "name", "siteId" }`
(`id` is the hex form of the Mongo ObjectID, exposed under the `id` key per the
API-wide `_id`→`id` convention), sorted by `name`; always an array (`[]` when
empty, never `null`). See [../client-api.md §3.4](../client-api.md#appscategories).

#### Errors

`internal` (Mongo read failed).

**Emits:** None.

---

### List User Threads

**Subject:** `chat.user.{account}.request.user.{siteID}.thread.list`

`{siteID}` is the **caller's own home site**. Returns the user's thread subscriptions
across **all** federation sites as one globally-ordered "thread inbox" (newest activity
first) — `user-service` fans the query out per-site and merges the results.

#### Request body

| Field | Type | Notes |
|---|---|---|
| `cursor` | string | Optional. Opaque cursor from a previous `nextCursor`; omit for the first page. |
| `limit` | number | Optional. Page size (default 20, max 100). |

#### Success response

`{ "items": ThreadListItem[], "nextCursor"?: string, "hasNext": boolean, "unavailableSites"?: string[] }`
— see `ThreadListItem` schema in
[../client-api.md §3.4](../client-api.md#list-user-threads). Sites that fail to respond
are listed in `unavailableSites` rather than failing the request.

#### Errors

A malformed `cursor` returns `bad_request`.

**Emits:** None.

---

### Get Thread Unread Summary

**Subject:** `chat.user.{account}.request.user.{siteID}.thread.unread.summary`

`{siteID}` is the **caller's own home site**. Returns aggregated unread status across all
of the user's thread subscriptions, site by site, merged into one response.

#### Request body

Empty object: `{}`.

#### Success response

`{ "unread": boolean, "unreadDirectMessage": boolean, "unreadMention": boolean,
"lastMessageAt"?: number, "unavailableSites"?: string[] }` — see
[../client-api.md §3.4](../client-api.md#get-thread-unread-summary). Per-site RPC
failures degrade into `unavailableSites` rather than erroring.

#### Errors

`internal` — local thread-subscription read failed.

**Emits:** None.

---

### Clear All Thread Unread

**Subject:** `chat.user.{account}.request.user.{siteID}.thread.read.all`

`{siteID}` is the **caller's own home site**. Clears the unread status of all of the
user's threads across every site — the "mark all threads read" action. `user-service`
asks each owning site's `room-service` to clear that user's thread-subscription read
state and room-subscription thread-unread state.

#### Request body

Empty object: `{}`.

#### Success response

`{ "clearedThreads": number, "unavailableSites"?: string[] }` — see
[../client-api.md §3.4](../client-api.md#clear-all-thread-unread). A bulk dismiss:
clears only the requester's own read state; does not advance thread read floors or emit
`thread_message_read`. Per-site RPC failures degrade into `unavailableSites`.

#### Errors

`internal` — local thread-subscription read failed.

**Emits:** None.

---

## media-service

| RPC subject | Method |
|---|---|
| `chat.user.{account}.request.emoji.{siteID}.list` | [emoji.list](#emojilist) |
| `chat.user.{account}.request.emoji.{siteID}.delete` | [emoji.delete](#emojidelete) |

`{siteID}` is the target site whose emoji set you want — in v1 the FE fetches only its
**local** site's list (non-local shortcodes are not rendered). The supercluster routes
the request to that site's media-service.

---

### emoji.list

**Subject:** `chat.user.{account}.request.emoji.{siteID}.list`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Lists the site's custom emoji, sorted by `shortcode`.

#### Request body

Empty. Send `{}` or no payload.

#### Success response

| Field | Type | Notes |
|---|---|---|
| `emojis` | EmojiEntry[] | `[]` when the site has none. |

`EmojiEntry`: `{ "shortcode", "imageUrl", "contentType", "etag", "updatedAt" }` —
`imageUrl` is the bare relative serve path `/api/v1/emoji/{shortcode}` (resolve against
the media-service base URL of the site the list came from; cache-bust with `?v={etag}`);
`updatedAt` is an RFC 3339 timestamp (UTC).
See [../client-api.md §3.5](../client-api.md#emojientry) for the full schema + example.

#### Errors

`internal` — store failure.

**Emits:** None — reply only.

---

### emoji.delete

**Subject:** `chat.user.{account}.request.emoji.{siteID}.delete`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

Deletes a custom emoji. Any authenticated user may delete (v1). Disabled by default —
gated by media-service's `EMOJI_DELETE_ENABLED` (default `false`).

#### Request body

`{ "shortcode": "acme_party" }`

#### Success response

`{ "shortcode": "acme_party", "deleted": true }` — `shortcode` is the canonical (NFC) form.

#### Errors

Malformed/missing `shortcode` (`bad_request`), no such emoji on this site (`not_found`),
kill-switch off (`forbidden`, reason `emoji_delete_disabled`), store failure (`internal`).

**Emits:** None — reply only.

---

## Publish operations

### Send Message

**Subject:** `chat.user.{account}.room.{roomID}.{siteID}.msg.send`
**Async reply:** `chat.user.{account}.response.{requestID}` (subscribe to `chat.user.{account}.>` to receive it)

Publish + async-reply pattern — no `_INBOX.>` reply. Covers plain message, thread reply,
and quoted message; variant determined by optional fields.

`{siteID}` must be the room's origin siteID.

**botDM rooms receive no `new_message` fan-out** — `broadcast-worker` skips botDM types.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `id` | string | yes | 20-char base62 client-generated message ID. |
| `content` | string | yes* | Message body, ≤ 20 KiB. *Required unless `attachments` is present. |
| `requestId` | string | yes | 36-char hyphenated UUID (v4 or v7). Async reply delivered to `…response.{requestId}`. |
| `attachments` | string[] | no | Optional. Each entry is base64-encoded JSON of one [Attachment](../client-api.md#attachment) from the upload endpoint. Max 1 entry, ≤ 8 KiB total; returned decoded as `Attachment[]` in message payloads. |
| `threadParentMessageId` | string | no | Thread reply: the parent's message ID (20-char base62). |
| `tshow` | boolean | no | "Also send to channel". Only meaningful on a thread reply; ignored on non-thread sends. |
| `quotedParentMessageId` | string | no | Quoted message: the parent's message ID. Server fetches and embeds the authoritative snapshot from message history. On a *transient* history outage the live copy gets a `"Content temporarily unavailable"` placeholder, re-projected to the authoritative snapshot (or dropped) before the durable write — the placeholder never persists. A genuinely missing/forbidden parent is still rejected. |

#### Async success response

Delivered on `chat.user.{account}.response.{requestId}`.

| Field | Type | Notes |
|---|---|---|
| `id` | string | Echoes request `id`. |
| `roomId` | string | From subject. |
| `userId` | string | Sender's internal user ID. |
| `userAccount` | string | Sender's account. |
| `userDisplayName` | string | Render-ready sender name. |
| `content` | string | Message body as sent. |
| `createdAt` | string | RFC 3339. Server-assigned send time. |
| `threadParentMessageId` | string | Present only for a thread reply. |
| `threadParentMessageCreatedAt` | string | Optional. RFC 3339. Server-resolved best-effort; absent when unresolved at send time. |
| `tshow` | boolean | Present only when `tshow: true` on a thread reply. |
| `quotedParentMessage` | [QuotedParentMessage](../client-api.md#quotedparentmessage) | Present only for a quoted send. |

#### Async error response

Same subject. See [../client-api.md §4](../client-api.md#4-message-send) for the full
error table. Key errors:

- `"invalid requestId"` (`bad_request`)
- `"content must not be empty"` / `"content exceeds maximum size of 20480 bytes"`
- `"not subscribed"` (`forbidden`, `not_subscribed`)
- `"posting is restricted to owners and admins in this room"` (`forbidden`, `large_room_post_restricted`)

**Emits:** [`new_message`](events.md#new_message-roomevent) `RoomEvent` (channel: `chat.room.{roomID}.event`; DM: `chat.user.{recipient}.event.room` per non-bot member), [`thread_metadata_updated`](events.md#thread_metadata_updated-threadmetadataupdatedevent) (thread replies only) → [events.md](events.md)

---

### Room Encryption Key Get

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.key.get`
**Reply:** auto-generated `_INBOX.>` (NATS request/reply)

On-demand key fetch when a received message cannot be decrypted with held keys.
Call only when needed; back off after failure (permanently-gone keys won't reappear).

#### Request body (`RoomKeyGetRequest`)

`{ "version": 3 }` — when omitted or `null`, returns the current key.

#### Success response (`RoomKeyGetResponse`)

| Field | Type | Notes |
|---|---|---|
| `roomId` | string | |
| `version` | integer | |
| `privateKey` | string | Base64-encoded 32-byte room secret. Same caching path as live `RoomKeyEvent`. |

#### Errors

`"only room members can list members"` (not a member), `"room key not available"` (past grace window or never existed).

---

### Presence publishes

Client → server publishes (no reply). Payload and subject details in
[../client-api.md §8](../client-api.md#8-presence).

| Subject | Sent when |
|---|---|
| `chat.user.{account}.event.presence.{siteID}.hello` | Connection comes up — registers the connection, brings user online. |
| `chat.user.{account}.event.presence.{siteID}.ping` | Every ~30 s — keeps connection live. |
| `chat.user.{account}.event.presence.{siteID}.activity` | Client idle detection flips active/inactive. |
| `chat.user.{account}.event.presence.{siteID}.bye` | Best-effort on tab close (beforeunload). |

Presence request/reply methods:

| Subject | Method |
|---|---|
| `chat.user.{account}.request.presence.{siteID}.manual.set` | [Set / clear manual override](../client-api.md#85-set--clear-manual-override-requestreply) |
| `chat.user.presence.{siteID}.query.batch` | [Batch query initial presence state](../client-api.md#86-batch-query--initial-state-requestreply) |
