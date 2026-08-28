# Chat Backend — Client API Reference

> [!IMPORTANT]
> **Changelog — centralized error codes (current release).** All client-facing
> errors — over NATS sync replies, JetStream async results (`model.AsyncJobResult`),
> and HTTP — now use the same envelope: `{ "error": <message>, "code": <category>,
> "reason"?: <domain-code>, "metadata"?: {…} }`. `code` is **always present** and
> drives HTTP status (see §6); `reason` is the optional domain code the client
> branches on (`reason ?? code`). Three notable behavior changes:
>
> 1. **`POST /api/v1/auth` 500** now returns `{ "code": "internal", "error": "internal error" }` —
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

- **HTTP** — the single `POST /api/v1/auth` endpoint that exchanges an SSO token
  for a NATS user JWT.
- **NATS request/reply** — RPC-style methods exposed by `room-service`,
  `history-service`, and `search-service`.
- **NATS publish + async reply** — the message-send flow handled by
  `message-gatekeeper`.

For each method, this doc lists the subject, the request body schema and
example, the success response schema and example, the error response, and
the server-pushed events the client will receive on the success and error
paths.

> Frontend-oriented split views: [request-reply](client-api/request-reply.md) · [events](client-api/events.md). This document remains canonical.

## Table of contents

1. [Overview](#1-overview)
2. [Connection & Auth](#2-connection--auth)
   - [2.1 NATS connection](#21-nats-connection)
   - [2.2 HTTP — POST /api/v1/auth](#22-http--post-apiv1auth)
   - [2.3 HTTP — GET /api/userInfo (portal-service)](#23-http--get-apiuserinfo-portal-service)
   - [2.4 HTTP — Protected image upload/download](#24-http--protected-image-uploaddownload)
   - [2.5 HTTP — GET /api/settings (portal-service)](#25-http--get-apisettings-portal-service)
3. [Request/Reply Methods](#3-requestreply-methods)
   - [3.0 Shared schemas](#30-shared-schemas)
   - [3.1 room-service](#31-room-service)
     - [Create Room](#create-room) · [Add Members](#add-members) · [Remove Member](#remove-member) · [Update Member Role](#update-member-role) · [Rename Room](#rename-room)
     - [List Members](#list-members) · [Get Member Statuses](#get-member-statuses) · [Get Mentionable Subscriptions](#get-mentionable-subscriptions) · [List Org Members](#list-org-members)
     - [Mark Messages Read](#mark-messages-read) · [Mark Thread as Read](#mark-thread-as-read) · [Read Message Receipts](#read-message-receipts) · [Toggle Mute](#toggle-mute) · [Toggle Favorite](#toggle-favorite) · [Open Room](#open-room)
     - [Get Room App Tabs](#get-room-app-tabs) · [Get Room App Command Menu](#get-room-app-command-menu)
   - [3.2 history-service](#32-history-service)
     - [Load History](#load-history) · [Load Next Messages](#load-next-messages) · [Load Surrounding Messages](#load-surrounding-messages) · [Get Message By ID](#get-message-by-id) · [Get Messages By IDs](#get-messages-by-ids)
     - [Edit Message](#edit-message) · [Delete Message](#delete-message) · [Pin Message](#pin-message) · [Unpin Message](#unpin-message) · [List Pinned Messages](#list-pinned-messages) · [React to Message](#react-to-message)
     - [Get Thread Messages](#get-thread-messages) · [Get Thread Parent Messages](#get-thread-parent-messages)
   - [3.3 search-service](#33-search-service)
     - [`search.messages`](#searchmessages--full-text-message-search) · [Search Rooms](#search-rooms) · [Search Apps](#search-apps) · [Search Users](#search-users) · [Search Orgs](#search-orgs)
   - [3.4 user-service](#34-user-service)
     - [`me`](#me) · [`status.getByName`](#statusgetbyname) · [`profile.getByName`](#profilegetbyname) · [`status.set`](#statusset) · [`subscription.list`](#subscriptionlist) · [`subscription.getChannels`](#subscriptiongetchannels)
     - [`subscription.getDM`](#subscriptiongetdm) · [`subscription.getByRoomID`](#subscriptiongetbyroomid) · [`subscription.count`](#subscriptioncount) · [`subscription.setAppSubscription`](#subscriptionsetappsubscription) · [`apps.list`](#appslist) · [`apps.categories`](#appscategories) · [`settings.get`](#settingsget) · [`settings.set`](#settingsset) · [`settings.priorityContacts.get`](#settingsprioritycontactsget) · [`settings.priorityContacts.add`](#settingsprioritycontactsadd) · [`settings.priorityContacts.remove`](#settingsprioritycontactsremove)
     - [Chatlist Sections](#chatlist-sections)
     - [`sso.set`](#ssoset) · [`sso.refresh`](#ssorefresh)
   - [3.5 media-service](#35-media-service)
     - [`emoji.list`](#emojilist--list-a-sites-custom-emoji) · [`emoji.delete`](#emojidelete--delete-a-custom-emoji)
   - [3.6 translation-service](#36-translation-service)
     - [Translate Text](#translate-text)
4. [Message Send](#4-message-send)
   - [4.1 Thread Metadata Event](#41-thread-metadata-event)
   - [4.2 Thread View Subject](#42-thread-view-subject)
5. [Room Encryption](#5-room-encryption)
6. [Error envelope reference](#6-error-envelope-reference)
7. [Media Service](#7-media-service)
   - [GET /api/v1/avatar/:accountName](#get-apiv1avataraccountname)
   - [GET /api/v1/avatar/room/:roomID](#get-apiv1avatarroomroomid)
   - [PUT /api/v1/avatar/bot/:botName](#put-apiv1avatarbotbotname)
   - [GET /api/v1/emoji/:shortcode](#get-apiv1emojishortcode)
   - [PUT /api/v1/emoji/:shortcode](#put-apiv1emojishortcode)
8. [Presence](#8-presence)
9. [Admin Service](#9-admin-service)
    - [9.17 POST /v1/admin/client-updates](#917-http--post-v1adminclient-updates)
10. [Botplatform Service](#10-botplatform-service)
    - [10.1 POST /api/v1/login](#101-http--post-apiv1login-bot-sdk-direct) · [10.2 POST /api/v1/auth/validate](#102-http--post-apiv1authvalidate)
11. [tcard-service](#11-tcard-service)
    - [11.1 GET card template](#111-http--get-apiv1cardspathcardversiontemplatejson) · [11.2 POST /api/v1/cards/validate (admin)](#112-http--post-apiv1cardsvalidate-admin)
12. [Client Update Service](#12-client-update-service)
    - [POST /api/v1/version](#post-apiv1version) · [GET /api/v1/version/:fileName](#get-apiv1versionfilename)
13. [User Service HTTP API](#13-user-service-http-api)
    - [13.1 Authentication](#131-authentication) · [13.2 GET /api/v1/subscriptions](#132-http--get-apiv1subscriptions)

---

## 1. Overview

This doc covers the public client-facing API surface only.

**Out of scope (backend-internal — clients never see these):**

- Backend-only JetStream subjects (MESSAGES, MESSAGES-CANONICAL, INBOX, ROOMS, OUTBOX streams).
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

- **Standard NATS request/reply** — the NATS client library auto-generates a reply subject under `chat.user.{account}.>` (from the connection's `inboxPrefix`, see §2.1) and routes the reply back to the caller. Used by every method in §3.
- **Async reply on `chat.user.{account}.response.{requestID}`** — used only by `msg.send` (§4). The client publishes and expects **no synchronous request/reply response**; the server reads `requestId` from the payload and publishes the reply to `chat.user.{account}.response.{requestID}`. The client must hold a subscription matching that subject to receive it — the `chat.user.{account}.>` wildcard is one way, but a narrower subscription works and is what the frontend uses.

### Timestamps

All event payloads carry a top-level `timestamp` field that is **milliseconds since the Unix epoch in UTC** — abbreviated `Epoch ms (UTC)` in the field tables below. Domain timestamps inside payloads (e.g. `Message.createdAt`) are RFC 3339 strings.

---

## 2. Connection & Auth

### 2.1 NATS connection

Login is a three-step sequence: portal userInfo lookup (§2.3) resolves the user's home site → auth (§2.2) mints a NATS user JWT → the client connects to the resolved `natsUrl`. The auth-service base URL, the NATS WebSocket URL, and the user's `siteId` are not static client config — they come from the portal lookup. The JWT scopes the client's permissions to:

| Permission | Subject pattern | Why |
|---|---|---|
| Publish | `chat.user.{account}.>` | The client may publish only under its own user namespace. All RPC requests, the message-send subject, and any client-emitted event fall here. |
| Publish | `chat.user.presence.*.query.batch` | Batch presence-state queries. Read-only for the state broadcast (`chat.user.presence.state.*`) — this subject is deliberately named `query` so it cannot match the state pub-rule. |
| Subscribe | `chat.user.{account}.>` | Receives all responses, notifications, and per-user events. Also carries request/reply replies: the client sets `inboxPrefix` to `chat.user.{account}` at connect time, using the `user.account` value from §2.2 **verbatim**. That value is the JWT's own tag value, which is what the grant is evaluated against — re-normalising it client-side diverges on non-ASCII accounts and has every reply denied. Per-account, so no client can read or forge another user's replies. |
| Subscribe | `chat.room.>` | Subscribes to per-room message streams and room events for cross-site rooms (`crossSite: true`) the user belongs to. |
| Subscribe | `chat.local.room.>` | Subscribes to per-room message streams and room events for same-site rooms (`crossSite: false`) the user belongs to. |
| Subscribe | `chat.user.presence.state.*` | Read anyone's live presence state broadcast. |

Permissions and connection limits come from the auth-service account's scoped signing key template on the NATS side — they are not inlined per JWT. The user JWT carries only an `account:{account}` tag; the scope template on the server substitutes `{{tag(account)}}` at connect time to produce the grants above.

**Recommended baseline subscriptions on connect:**

- `chat.user.{account}.>` — one option, capturing every personal event including async replies, per-user room events (DM messages, edits, deletes), room-key events, subscription updates, and settings updates. Note it also matches this client's own request/reply replies, because `inboxPrefix` is `chat.user.{account}`, and NATS delivers a copy to every matching subscription — so a handler here must ignore subjects it does not recognise. Subscribing to the specific event subjects instead avoids that overlap; the frontend does so.
- the room-event subject for each channel room in the user's sidebar — receives new messages plus edit/delete events for that channel. Pick the subject by the room's `crossSite` flag (from `subscription.list`): `chat.room.{roomID}.event` when `crossSite: true`, `chat.local.room.{roomID}.event` when `crossSite: false`. **Absent/unknown `crossSite` defaults to the global `chat.room.{roomID}.event`** (fail-safe — a global room misrouted to the local subject would silently miss cross-site delivery).

The exact event subjects a client may receive as a result of an RPC are listed under each method's "Triggered events" sections in §2.2, §3, and §4.

### 2.2 HTTP — POST /api/v1/auth

**Endpoint:** `POST /api/v1/auth`
**Reply:** synchronous HTTP response

Exchanges either an OIDC SSO token (humans) or a botplatform session token (bots/admins) for a signed NATS user JWT. The returned JWT is what the client uses to connect to NATS (see §2.1). The server auto-routes by which token field is present — exactly one of `ssoToken` / `authToken` must be supplied.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `ssoToken` | string | one-of | OIDC-issued SSO token. Set this for the SSO (human) path; leave `authToken` empty. |
| `authToken` | string | one-of | Botplatform session token from §10.1 / §10.2. Set this for the bot/admin path; leave `ssoToken` empty. |
| `natsPublicKey` | string | yes | The client's NATS user public NKey (must pass `nkeys.IsValidPublicUserKey`). |

Exactly one of `ssoToken` / `authToken` must be set: both → `400 ambiguous_token`; neither → `400 missing_token`. The scope of the returned JWT is derived server-side from the principal's roles (admin > bot > user); the client never declares a role.

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
| `user.account` | string | The `{account}` value used in every NATS subject. Derived from the token's `preferred_username` claim. |
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
| 400 | `bad_request` | `missing_fields` | `{ "code": "bad_request", "reason": "missing_fields", "error": "natsPublicKey is required" }` |
| 400 | `bad_request` | `invalid_nkey` | `{ "code": "bad_request", "reason": "invalid_nkey", "error": "invalid natsPublicKey format" }` |
| 400 | `bad_request` | `ambiguous_token` | `{ "code": "bad_request", "reason": "ambiguous_token", "error": "set exactly one of ssoToken / authToken" }` |
| 400 | `bad_request` | `missing_token` | `{ "code": "bad_request", "reason": "missing_token", "error": "set exactly one of ssoToken / authToken" }` |
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "account must be a single NATS subject token (no '.', '*', '>' or whitespace)" }` — the account becomes a NATS subject token, so separator/wildcard/whitespace characters are refused. |
| 401 | `unauthenticated` | `sso_token_expired` | `{ "code": "unauthenticated", "reason": "sso_token_expired", "error": "SSO token has expired, please re-login" }` |
| 401 | `unauthenticated` | `invalid_sso_token` | `{ "code": "unauthenticated", "reason": "invalid_sso_token", "error": "invalid SSO token" }` |
| 401 | `unauthenticated` | `invalid_token` | `{ "code": "unauthenticated", "reason": "invalid_token", "error": "invalid session token" }` — botplatform session token failed validation. Same envelope every service returns for a rejected session token. |
| 503 | `unavailable` | `upstream_unavailable` | `{ "code": "unavailable", "reason": "upstream_unavailable", "error": "botplatform unavailable" }` — auth-service cannot reach botplatform to validate a session token. |
| 500 | `internal` | — | `{ "code": "internal", "error": "internal error" }` — the real cause is logged server-side and never sent to the client. |

The returned `natsJwt` has a server-configured lifetime (default 2h). Clients should re-call `POST /api/v1/auth` to refresh before it expires.

> **Background renewal.** The web client also calls `POST /api/v1/auth` periodically to
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

### 2.3 HTTP — GET /api/userInfo (portal-service)

**Endpoint:** `GET /api/userInfo?account={account}`
**Reply:** synchronous HTTP response

Site discovery — called once per login, **before** §2.2. Looks the account up in the portal's in-memory directory, which is **users-primary**: every account in the `users` collection (the canonical user record) is loaded, left-joined against the HR-owned `hr_employee` collection (refreshed daily) for enrichment fields. A cache hit therefore means the account is provisioned in `users` — bot/admin accounts resolve too, just without `hr_employee` enrichment — and returns the home site's connection coordinates. The client then calls `POST {baseUrl}/api/v1/auth` (§2.2) and connects to `natsUrl` (§2.1). JWT renewal does **not** re-query the portal — site assignment is stable within a session.

**Discovery only — no token is validated here.** The endpoint serves non-secret directory data keyed by `account`. The client supplies the account directly: derived from the SSO token's `preferred_username` claim in production, or the dev login form in dev mode. The authoritative check is auth-service (§2.2), which validates the SSO token before minting a JWT — an account that resolves here still cannot obtain a NATS JWT or connect without a valid token at that step.

#### Request

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `account` | query | string | yes | The account to resolve. Becomes the `{account}` in every NATS subject, so separator/wildcard/whitespace characters (`.`, `*`, `>`, whitespace) are refused. |

```http
GET /api/userInfo?account=alice
```

#### Success response

`HTTP 200`. The response shape is **role-aware**:

- For regular SSO users (account NOT carrying `bot` or `admin` in `users.roles`): the existing rich employee shape below.
- For **bot/admin** accounts: a minimal 4-field shape that omits `employeeId` and only carries the URL bundle the SDK / admin UI needs to bootstrap.

##### Rich shape (SSO users — default)

| Field | Type | Notes |
|---|---|---|
| `account` | string | The `{account}` used in every NATS subject. |
| `employeeId` | string | From the portal directory; informational. |
| `baseUrl` | string | Home-site unified backend origin — the Traefik gateway (`:7777`), fronting auth-service, upload-service, and media-service under `/api/v1/*`. The client calls `POST {baseUrl}/api/v1/auth` next. Portal login (§2.5) is portal-direct and is NOT routed through this gateway. |
| `natsUrl` | string | WebSocket URL of the home site's NATS. |
| `siteId` | string | The user's home site; scopes site-suffixed NATS subjects. |

```json
{
  "account": "alice",
  "employeeId": "E12345",
  "baseUrl": "https://site-a.example.com",
  "natsUrl": "wss://nats.site-a.example.com",
  "siteId": "site-a"
}
```

##### Minimal shape (bot/admin accounts)

When `users.roles` contains `bot` or `admin`, the response omits `employeeId`. All four remaining fields are exactly as documented above.

```json
{
  "account": "p_admin",
  "baseUrl": "https://site-a.example.com",
  "natsUrl": "wss://nats.site-a.example.com",
  "siteId": "site-a"
}
```

Note: bot account names that contain `.` (e.g. `name.shortcode.bot`) cannot be served through this endpoint — the existing single-NATS-token validator refuses dotted accounts. Bot SDKs do not call this endpoint; they hit botplatform `/api/v1/login` directly (§10.1).

#### Error response

See [Error envelope](#6-error-envelope-reference). HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | `missing_fields` | `{ "code": "bad_request", "reason": "missing_fields", "error": "account is required" }` |
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "account must be a single NATS subject token (no '.', '*', '>' or whitespace)" }` — same account rule as §2.2. |
| 403 | `forbidden` | `account_not_ready` | `{ "code": "forbidden", "reason": "account_not_ready", "error": "account not ready for chat" }` — the account is absent from the portal's `users` directory cache. `hr_employee` is an enrichment left-join, not a gate — a human account missing only its `hr_employee` row still resolves. |
| 500 | `internal` | — | `{ "code": "internal", "error": "internal error" }` |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

### 2.5 HTTP — POST /api/v1/login (portal-service)

**Endpoint:** `POST /api/v1/login`
**Reply:** synchronous HTTP response

Password login for bot/admin accounts via portal-service, called by web / mobile / desktop / admin-UI clients. Portal looks up the user's home site, forwards `{username, password}` to botplatform `/api/v1/login` (§10.1) over the cluster-internal endpoint, and returns a merged 7-field response so the client has both the session token and the home-site URL bundle in one round trip. **Regular SSO users do not use this endpoint** — they use the existing SSO flow (§2.3 + §2.2).

Bot SDKs do not call portal — they hit botplatform `/api/v1/login` directly (§10.1).

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `username` | string | yes | Account name (matches `users.account`). Must hold the `bot` or `admin` role; SSO-only users get a uniform `401 invalid_credentials`. |
| `password` | string | yes | Plaintext password. Verified server-side using `bcrypt(sha256_hex(plaintext))` per the legacy recipe. |

```json
{
  "username": "p_admin",
  "password": "<secret>"
}
```

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `userId` | string | Canonical 17-char user identifier from botplatform. |
| `authToken` | string | 43-char base64url session token. Use as the `authToken` field in `POST /api/v1/auth` (§2.2) to obtain a NATS JWT. |
| `account` | string | The `{account}` used in every NATS subject; same as `username`. |
| `siteId` | string | The user's home site; informational. |
| `baseUrl` | string | Home-site unified backend origin — the Traefik gateway (`:7777`), fronting auth-service, upload-service, and media-service under `/api/v1/*`. The client calls `POST {baseUrl}/api/v1/auth` next. Portal login (§2.5) is portal-direct and is NOT routed through this gateway. |
| `natsUrl` | string | Home-site NATS WebSocket URL. |
| `requirePasswordChange` | boolean | First-login flag from the user doc. When `true`, the client should route to a password-update page before normal app use. The session token is still valid. |

```json
{
  "userId": "abcdef1234567890x",
  "authToken": "<43-char base64url>",
  "account": "p_admin",
  "siteId": "site-a",
  "baseUrl": "https://site-a.example.com",
  "natsUrl": "wss://nats.site-a.example.com",
  "requirePasswordChange": false
}
```

#### Error response

See [Error envelope](#6-error-envelope-reference). HTTP statuses:

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `missing_fields` | `username` or `password` empty. |
| 401 | `unauthenticated` | `invalid_credentials` | Uniform rejection covering unknown account, wrong password, AND SSO-only accounts attempting password login. Body is byte-identical across the three arms to prevent enumeration. |
| 403 | `forbidden` | `bot_login_disabled` | Account holds a bot role and portal's `BOT_LOGIN_ENABLED` flag is `false`. Direct the user to the bot developer console instead. |
| 500 | `internal` | `site_unknown` | The user's `siteId` is missing from `PORTAL_SITE_URLS`. Server misconfiguration. |
| 503 | `unavailable` | `upstream_unavailable` | Portal cannot reach the home-site botplatform. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

### 2.4 HTTP — Protected file/image upload/download

HTTP endpoints on `upload-service` for protected file uploads and downloads,
proxied to/from an internal Drive.

**Auth — either credential is accepted:**

| Caller | Credential |
|---|---|
| SSO user | An OIDC-validated `ssoToken`, sent as the `ssoToken` header, or (for browser `<img>` downloads that cannot set headers) as an `ssoToken` cookie obtained from `POST /api/v1/file/setCookie` below. The header takes precedence over the cookie. |
| Bot / admin | A botplatform session token (§10.1), sent as `x-user-id` + `x-auth-token`. |

Selection rules: sending **both** an `ssoToken` header and an `x-auth-token` header is
`400 ambiguous_token`. An `x-auth-token` header takes precedence over an `ssoToken`
**cookie** — the cookie is ambient state a browser attaches automatically, the header is
an explicit act, so only two explicit headers are treated as a conflict.

`POST /api/v1/file/setCookie` is the one exception: it is SSO-only and returns `400` to a
session-token caller, because it exists solely to mirror an `ssoToken` into a cookie for
browser downloads. Bots send headers on every request and need no cookie.

Room-scoped endpoints also require that the caller is a member (has a subscription) of
`:roomId` — this applies identically to bots, which hold real subscription rows.
Cross-origin browsers are served credentialed CORS headers only when their `Origin` is in
the server's `CORS_ALLOWED_ORIGINS` allowlist. Errors use the standard
[§6](#6-error-envelope-reference) envelope `{ code, reason?, error }`.

**Errors common to every endpoint below** (in addition to each endpoint's own):

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | `ambiguous_token` | `{ "code": "bad_request", "reason": "ambiguous_token", "error": "set exactly one of ssoToken / x-auth-token" }` |
| 401 | `unauthenticated` | `invalid_token` | `{ "code": "unauthenticated", "reason": "invalid_token", "error": "invalid session token" }` — missing, unknown, or mismatched session credential. The three cases are deliberately indistinguishable. |
| 503 | `unavailable` | `upstream_unavailable` | `{ "code": "unavailable", "reason": "upstream_unavailable", "error": "botplatform unavailable" }` — the session token could not be validated. Distinct from `401`: the credential was not rejected, it could not be checked. |

#### POST /api/v1/file/setCookie

**Endpoint:** `POST /api/v1/file/setCookie`
**Reply:** synchronous HTTP response

Exchanges the `ssoToken` header for an `ssoToken` cookie so the browser can then load
protected files via `<img src>` (which cannot send headers). The token is validated
before the cookie is issued. Call this once after login. The cookie is a session cookie
and the token can expire, so when downloads start returning `401`, re-call this endpoint
with a **fresh** valid `ssoToken` — this endpoint re-authenticates on every call and
cannot refresh the cookie from an already-expired token.

#### Request

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `ssoToken` | header | string | yes | OIDC-issued SSO token; validated before the cookie is set. **This endpoint is SSO-only** — a session-token caller gets `400`. |

Cross-origin callers must send the request with credentials (e.g. `fetch(..., { credentials: "include" })`) and be served from an origin in `CORS_ALLOWED_ORIGINS`.

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `success` | boolean | Always `true` on a 200. |

Response header:

```
Set-Cookie: ssoToken=<token>; Path=/; HttpOnly; Secure; SameSite=None
```

`Partitioned` is env-controlled server-side (`SETCOOKIE_PARTITIONED`, default `false` — omitted) and is added only when enabled.

```json
{ "success": true }
```

#### Error response

Uses the [§6](#6-error-envelope-reference) envelope. HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "setCookie requires an ssoToken; session-token callers send credentials as headers" }` — this endpoint is SSO-only. |
| 401 | `unauthenticated` | `invalid_sso_token` / `sso_token_expired` / `missing_fields` | `{ "code": "unauthenticated", "reason": "invalid_sso_token", "error": "invalid sso token" }` |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

#### POST /api/v1/file/rooms/:roomId/upload/images

**Endpoint:** `POST /api/v1/file/rooms/:roomId/upload/images`
**Reply:** synchronous HTTP response

Uploads one or more images for a room on behalf of the authenticated user. Each
file is validated independently and the response reports per-file
success/failure in a single `200` (partial success).

#### Request

`Content-Type: multipart/form-data`

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `ssoToken` | header | string | conditional | OIDC-issued SSO token; identifies the uploader. Required unless the session-token pair below is sent. |
| `x-user-id` + `x-auth-token` | header | string | conditional | Botplatform session token (§10.1); identifies a bot/admin uploader. Required unless `ssoToken` is sent. |
| `roomId` | path | string | yes | Target room ID; the caller must be a member. |
| `images` | form file | file[] | yes | One or more images (`.png`/`.jpeg`/`.jpg`/`.heic`), each ≤ `MAX_IMAGE_SIZE_BYTES` (default 25 MiB); at most `MAX_IMAGES` (default 10). Repeat the field once per file. |

#### Success response

`HTTP 200` — one `results` entry per submitted file (successes and failures together).

| Field | Type | Notes |
|---|---|---|
| `results` | [UploadResult](#uploadresult)[] | Per-file outcome. |

##### UploadResult

| Field | Type | Notes |
|---|---|---|
| `name` | string | The file name. |
| `status` | string | `success` for an uploaded file, `failure` for a rejected one. |
| `error` | string | Present on failure: `file size exceeds limit`, `file has an invalid file type`, or `failed to open file`. |
| `relativePath` | string | Present on success: path to download the file via the GET endpoint below, including the `drive_host` query param. |

```json
{
  "results": [
    { "name": "pic1.png", "status": "success", "relativePath": "api/v1/file/rooms/abc123/file/img-xyz?drive_host=https://drive.example.com" },
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

#### POST /api/v1/file/rooms/:roomId/upload/file

**Endpoint:** `POST /api/v1/file/rooms/:roomId/upload/file`
**Reply:** synchronous HTTP response

Uploads a single file (image/audio/video/document) for a room on behalf of the
authenticated user and stores it in Drive. Returns a render-ready
[Attachment](#attachment) the client uses to compose a `msg.send` (§4). This is a
pure-HTTP endpoint — it does **not** publish a message.

#### Request

`Content-Type: multipart/form-data`

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `ssoToken` | header | string | conditional | OIDC-issued SSO token; identifies the uploader. Required unless the session-token pair below is sent. |
| `x-user-id` + `x-auth-token` | header | string | conditional | Botplatform session token (§10.1); identifies a bot/admin uploader. Required unless `ssoToken` is sent. |
| `roomId` | path | string | yes | Target room ID; the caller must be a member. |
| `file` | form file | file | yes | The single file, ≤ `FILE_UPLOAD_MAX_FILE_SIZE` (default 100 MiB). At most `MAX_ATTACHMENTS` (default 1) parts may be sent under this field; more is rejected with `too many files`. Its MIME type must pass the server's allow/deny lists (`FILE_UPLOAD_MEDIA_TYPE_WHITELIST`/`BLACKLIST`; `image/svg+xml` is blocked by default). |
| `description` | form field | string | no | Optional attachment description. |

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `success` | boolean | Always `true` on a 200. |
| `attachments` | [Attachment](#attachment)[] | One-element array describing the uploaded file. |

```json
{
  "success": true,
  "attachments": [
    {
      "id": "drive-file-1",
      "title": "report.pdf",
      "type": "file",
      "description": "Q2 report",
      "titleLink": "api/v1/file/rooms/abc123/file/drive-file-1?drive_host=https://drive.example.com",
      "titleLinkDownload": true
    }
  ]
}
```

#### Error response

Uses the [§6](#6-error-envelope-reference) envelope. HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "file type is not allowed" }` — also `roomId is required`, `request must be multipart/form-data`, `file is required`, `too many files`, `file size exceeds limit`. |
| 401 | `unauthenticated` | `invalid_sso_token` / `sso_token_expired` / `missing_fields` | `{ "code": "unauthenticated", "reason": "invalid_sso_token", "error": "invalid sso token" }` |
| 403 | `forbidden` | `not_room_member` | `{ "code": "forbidden", "reason": "not_room_member", "error": "user alice is not in room abc123" }` |
| 404 | `not_found` | — | `{ "code": "not_found", "error": "room not found" }` |
| 500 | `internal` | — | `{ "code": "internal", "error": "internal error" }` — user missing in context, no email on the account, or a read fault; real cause logged server-side only. |
| 503 | `unavailable` | — | `{ "code": "unavailable", "error": "drive upload failed" }` |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

#### GET /api/v1/file/rooms/:roomId/file/:fileId

**Endpoint:** `GET /api/v1/file/rooms/:roomId/file/:fileId`
**Reply:** synchronous HTTP response (raw file bytes, not JSON)

Downloads a protected file (any type — image/audio/video/document). The service
proxies the bytes from Drive: it fetches a signed URL, streams the body, and
pipes it straight back. Typically called with the `relativePath` (image upload)
or `titleLink` (file upload) returned by the upload endpoints.

#### Request

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `ssoToken` | header/cookie | string | conditional | OIDC-issued SSO token. Sent as the `ssoToken` header, or as the `ssoToken` cookie from `POST /api/v1/file/setCookie` (browser `<img>` downloads); header wins. Required unless the session-token pair below is sent. |
| `x-user-id` + `x-auth-token` | header | string | conditional | Botplatform session token (§10.1), for bot/admin callers. Required unless `ssoToken` is sent. |
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
| 503 | `unavailable` | — | `{ "code": "unavailable", "error": "failed to retrieve file" }` — Drive signer/download failure. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

#### GET /api/v3/rooms/:roomId/protected-image/:fileId

**Endpoint:** `GET /api/v3/rooms/:roomId/protected-image/:fileId`
**Reply:** synchronous HTTP response (raw file bytes, not JSON)

Backward-compatible download for inline images referenced by **legacy message
data** (produced by a prior system version). Behaves exactly like
`GET /api/v1/file/rooms/:roomId/file/:fileId` above, except the bytes are proxied
from a **separate (legacy) Drive backend** with its own credentials. The client
calls this with the old-style path preserved in legacy messages.

#### Request

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `ssoToken` | header/cookie | string | conditional | OIDC-issued SSO token. Sent as the `ssoToken` header, or as the `ssoToken` cookie from `POST /api/v1/file/setCookie` (browser `<img>` downloads); header wins. Required unless the session-token pair below is sent. |
| `x-user-id` + `x-auth-token` | header | string | conditional | Botplatform session token (§10.1), for bot/admin callers. Required unless `ssoToken` is sent. |
| `roomId` | path | string | yes | Room the image belongs to; the caller must be a member. |
| `fileId` | path | string | yes | Legacy Drive file ID (from the original message data). |
| `drive_host` | query | string | yes | Legacy Drive base URL carried in the legacy message data. |

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
| 503 | `unavailable` | — | `{ "code": "unavailable", "error": "failed to retrieve file" }` — Drive signer/download failure. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

#### GET /api/v1/file-upload/:fileId/:fileName

**Endpoint:** `GET /api/v1/file-upload/:fileId/:fileName`
**Reply:** synchronous HTTP response (raw file bytes, not JSON)

Downloads a previously-uploaded file. Metadata is resolved from the `uploads`
collection by `fileId`; the bytes are streamed straight from the MinIO/S3 bucket.
The response is always served as an attachment.

#### Request

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `ssoToken` | header/cookie | string | conditional | OIDC-issued SSO token. Sent as the `ssoToken` header, or as the `ssoToken` cookie from `POST /api/v1/file/setCookie` (browser `<img>` downloads); header wins. Required unless the session-token pair below is sent. |
| `x-user-id` + `x-auth-token` | header | string | conditional | Botplatform session token (§10.1), for bot/admin callers. Required unless `ssoToken` is sent. |
| `fileId` | path | string | yes | Upload ID (the `uploads._id`); used for the metadata lookup. |
| `fileName` | path | string | yes | Cosmetic — accepted but ignored; the served filename comes from the stored metadata. |

The caller must be a member of the room the file belongs to (`uploads.rid`).

#### Success response

`HTTP 200` — raw file binary streamed directly (not JSON), with these headers:

| Header | Value |
|---|---|
| `Content-Type` | the upload's `type` (defaults to `application/octet-stream`). |
| `Content-Length` | the upload's `size`. |
| `Content-Disposition` | `attachment; filename*=UTF-8''<percent-encoded name>`. |
| `Content-Security-Policy` | `default-src 'none'`. |
| `Cache-Control` | `private, max-age=<FILE_DOWNLOAD_CACHE_MAX_AGE_SECONDS>` (default `31536000`). `private` because the response is authorization-gated. |

#### Error response

See [Error envelope](#6-error-envelope-reference). HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "fileId is required" }` |
| 401 | `unauthenticated` | `invalid_sso_token` / `sso_token_expired` / `missing_fields` | `{ "code": "unauthenticated", "reason": "invalid_sso_token", "error": "invalid sso token" }` |
| 403 | `forbidden` | `not_room_member` | `{ "code": "forbidden", "reason": "not_room_member", "error": "user alice is not in room r1" }` |
| 404 | `not_found` | — | `{ "code": "not_found", "error": "file not found" }` |
| 500 | `internal` | — | `{ "code": "internal", "error": "internal error" }` — user missing in context. |
| 503 | `unavailable` | — | `{ "code": "unavailable", "error": "failed to retrieve file" }` — S3 GetObject/Stat failure. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

### 2.5 HTTP — GET /api/settings (portal-service)

**Endpoint:** `GET /api/settings`
**Reply:** synchronous HTTP response

Deployment-level frontend configuration, served from the portal's environment. Called before login — same discovery-tier trust as §2.3: no input, no token validated. Both values are static per deployment and required at portal startup (the OTEL URL is additionally validated and normalized there), so the endpoint has no runtime error path.

#### Request

No parameters.

```http
GET /api/settings
```

#### Success response

`HTTP 200`, served with `Cache-Control: no-cache` (deployment config — revalidate, don't cache).

| Field | Type | Notes |
|---|---|---|
| `apiVersion` | string | Backend API generation the client should target (e.g. `v2`). Opaque string — compare, don't parse. |
| `otelBaseUrl` | string | Base URL for client OTEL telemetry, never with a trailing slash. The client appends `/trace` and `/log`. |

```json
{
  "apiVersion": "v2",
  "otelBaseUrl": "https://otel.example.com/v1"
}
```

#### Error response

The handler has no error path (both values are validated at portal startup). The only possible non-200 is a framework-level `500` from panic recovery, which returns an **empty body** — the [error envelope](#6-error-envelope-reference) does not apply here.

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

## 3. Request/Reply Methods

### 3.0 Shared schemas

Reusable payload types referenced by name throughout §3–§5. Each RPC links
here instead of repeating the table.

> **Platform-admin pseudo-account prefix.** The `p_admin` prefix used
> throughout (e.g. `p_admin{siteID}`) is configurable per deployment via
> the `ADMIN_ACCT_PREFIX` env var (default `p_admin`) and MUST be set to
> the same value in every service.

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

#### Attachment

Render-ready descriptor for an uploaded file. Returned by the upload endpoint
([§2.3](#23-http--protected-image-uploaddownload)), carried (base64-encoded JSON)
into `msg.send` (§4), and returned decoded as objects in message payloads. Media
fields are present only for the matching MIME family.

Attachments stored in the pre-migration format are converted server-side on
every read, so clients always receive the schema below. A converted attachment
carries `id`, `title`, `type`, `titleLink`, `titleLinkDownload` and `fileType`
(plus `description` when present); its `titleLink` points at
`api/v1/file-upload/{fileId}/{fileName}`, `titleLinkDownload` is `true` like on
any other attachment, and the media fields are absent.

| Field | Type | Notes |
|---|---|---|
| `id` | string | Drive file ID. |
| `title` | string | File name. |
| `type` | string | Always `"file"`. |
| `description` | string | Optional. |
| `titleLink` | string | Relative download URL (the GET image endpoint). |
| `titleLinkDownload` | boolean | Always `true`. |
| `fileType` | string | Optional. Canonical lowercased MIME type, present on every attachment family. |
| `imageUrl` | string | Image only. Same as `titleLink`. |
| `imageType` | string | Image only. MIME type. |
| `imageSize` | number | Image only. Bytes. |
| `imageDimensions` | [ImageDimensions](#imagedimensions) | Image only. Pixel size. |
| `imagePreview` | string | Image only. Base64 32×32 blurred JPEG. **No longer produced** — the upload endpoints stopped generating it. Present only on attachments uploaded before that change, which keep returning it from history, search and delivery. |
| `audioUrl` / `audioType` / `audioSize` | string / string / number | Audio only. |
| `videoUrl` / `videoType` / `videoSize` | string / string / number | Video only. |

#### ImageDimensions

| Field | Type | Notes |
|---|---|---|
| `width` | number | Pixels. |
| `height` | number | Pixels. |

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
events on `added` / `role_updated` / `mute_toggled` / `favorite_toggled` /
`opened` and returned (enriched) by the user-service subscription endpoints. The ID
serializes as `id` (not `_id`) and the user under `u` (not `user`). The first
group is always present; the rest are optional (omitted when empty/unset).

`name` is the **subscription's** display name and depends on the room type:
the channel name for channels, the counterpart's account for DMs, the app's
display name for botDMs. It is never overwritten by the room's canonical name.

All room-derived properties live under the nested `room` object
([SubscriptionRoom](#subscriptionroom)), populated at read time by the
user-service endpoints via room-service's `GetRoomsInfo` enrichment. On
`subscription.update` events, `room` is populated **only** on `action:
"added"` (built by room-worker at publish time, `previewMessage` always
omitted, `privateKey`/`keyVersion` present only for encrypted channel rooms);
it is absent on every other action.

| Field | Type | Notes |
|---|---|---|
| `id` | string | Subscription ID. |
| `u` | [SubscriptionUser](#subscriptionuser) | The subscribed user. |
| `roomId` | string | The room. |
| `siteId` | string | The room's home site. |
| `roomType` | string | `"channel"`, `"dm"`, `"botDM"`, or `"discussion"`. |
| `name` | string | Display name per room type (see above). |
| `roles` | string[] | The user's roles in the room (e.g. `["member"]`, `["owner"]`). |
| `joinedAt` | RFC3339 timestamp | When the user joined. |
| `hasMention` | boolean | Whether the user has an unread mention. Authoritative subscription state maintained by the write path (set when the user is @-mentioned, cleared on read); **not** modified by read enrichment. For a mentionee homed on another site, `broadcast-worker` also relays a cross-site `subscription_mention` event so the badge lands on the site that serves their `subscription.list`. |
| `hasUnread` | boolean | Whether the room has unread messages — computed at read time by comparing the room's `lastMsgAt` to the subscription's `lastSeenAt` (not persisted). |
| `hasGroupMention` | boolean | Whether the room has an unread @all/@channel mention — computed at read time by comparing the room's `lastMentionAllAt` to the subscription's `lastSeenAt` (not persisted). |
| `alert` | boolean | Whether the room has an unread alert for the user. Authoritative subscription state maintained by the write path (set on new message, cleared on read receipt); **not** modified by read enrichment. |
| `muted` | boolean | Whether the user muted the room. |
| `favorite` | boolean | Whether the user favorited the room. |
| `sectionId` | string | Optional. The custom chatlist section this room is in; absent = no custom section (the client derives a built-in placement). Set/cleared via [Move Chat to Section](#move-chat-to-section). |
| `sectionOrder` | number | Optional. Manual fractional position within `sectionId`'s section; honored only when that section's `sortMode == "custom"`. |
| `open` | boolean | Sidebar-visibility flag; false hides the room from subscription.list. |
| `isSubscribed` | boolean | Optional. Whether the user is actively subscribed. |
| `historySharedSince` | RFC3339 timestamp | Optional. Boundary before which prior history is shared. |
| `lastSeenAt` | RFC3339 timestamp | Optional. The user's last-seen time in the room. |
| `threadUnread` | string[] | Optional. Parent message IDs of threads with unread replies. |
| `restricted` | boolean | Optional. Denormalized room restricted flag. |
| `externalAccess` | boolean | Optional. Denormalized room external-access flag. |
| `room` | [SubscriptionRoom](#subscriptionroom) | Optional. Room-derived view. Populated at read time by the user-service endpoints, and at publish time by room-worker on `added` `subscription.update` events (no user-service read needed); absent on every other event action. |
| `favoriteUpdatedAt` | RFC3339 timestamp | Optional. Last time the user toggled favorite on the room (also bumped on un-favorite). |
| `muteUpdatedAt` | RFC3339 timestamp | Optional. Last time the subscription's mute state changed. |
| `rolesUpdatedAt` | RFC3339 timestamp | Optional. Last time the subscription's roles changed. |
| `nameUpdatedAt` | RFC3339 timestamp | Optional. Last time the subscription's room name changed. |
| `restrictUpdatedAt` | RFC3339 timestamp | Optional. Last time the room's restrict / external-access visibility changed. |

#### SubscriptionRoom

The room-derived view nested on an enriched [Subscription](#subscription).
On the list endpoints, **local** rooms are populated from the Mongo `$lookup`
baseline (room metadata plus the E2E key) with no RPC and **cross-site** rooms
from room-service's `GetRoomsInfo` RPC; if that RPC fails or the room isn't
found, the `room` object is **omitted entirely** — the subscription still
carries its own top-level `siteId`. On `added` `subscription.update` events the
same view is built by room-worker at publish time from the room's home-site
document (`previewMessage` always omitted there). All fields are optional
(omitted when zero/unset).

| Field | Type | Notes |
|---|---|---|
| `siteId` | string | The room's home site. |
| `crossSite` | bool | Tri-state. `true` → the room's real-time events are on `chat.room.{roomId}.>` (cross-gateway); `false` → CONFIRMED same-site, on `chat.local.room.{roomId}.>` (site-local). Omitted when the room's locality is unknown/unclassified (a pre-existing room the server hasn't classified yet) — clients MUST treat a missing value as `true` (global), never as `false`. |
| `name` | string | The room's canonical name (may differ from the subscription `name`). |
| `userCount` | number | Member count — human members, including QA `p_` test accounts (ordinary users). |
| `appCount` | number | App count — `.bot` bots plus the `p_admin` platform-admin pseudo-account. |
| `lastMsgAt` | RFC3339 timestamp | The room's last-message time. |
| `lastMsgId` | string | Last message ID. |
| `lastMentionAllAt` | RFC3339 timestamp | The last room-wide mention time. |
| `minUserLastSeenAt` | RFC3339 timestamp | The room-wide read floor — the oldest `lastSeenAt` across the room's members ("everyone has read up to here"). Omitted when the floor is unset (a member is still fully unread). |
| `privateKey` | string | Base64-encoded room E2E private key — initial key bootstrap for room members (see [§5](#5-room-encryption)). Present only for encrypted (channel) rooms whose key the caller's site holds; omitted otherwise. |
| `keyVersion` | number | Version of `privateKey`. |
| `previewMessage` | [PreviewMessage](#previewmessage) | Optional. The room's latest eligible message, stored server-side. Served from the room document when one is stored; when not, it is resolved from message history and that result is stored, so the room serves it from the document on subsequent reads. Its `content` is truncated to a short preview (see [PreviewMessage](#previewmessage)). Omitted when the room has no eligible message at all, when that site's enrichment degraded, when read-time resolution degrades for that room (a per-room read failure; the rest of the batch is unaffected), or when the request set `includeLastMessage: false` — best-effort, never fails the list. |

##### PreviewMessage

A room's most-recent **eligible** message, composed and stored when that message is
delivered, and enriched for room-list rendering. Eligible = not soft-deleted and not a
system message (quoted replies are normal content and ARE eligible) — an ineligible newer
message leaves the stored preview in place, so the room keeps showing its last real
content; a room with only ineligible messages omits `previewMessage`. A message carrying a
`visibleTo` marker **is** eligible and is previewed with the marker attached: the backend
does not filter previews on `visibleTo`, so the client honours the scope.

Editing or deleting a message also updates the stored preview: an edit to the previewed
message refreshes it, and deleting it moves the preview back to the previous eligible
message, or removes it when the recompute confirms the room has no eligible message
left. A recompute that cannot complete does not leave the pre-edit or deleted content in
place: the event omits `previewMessage`, and the stored preview stops being served, so the
next read resolves the room's preview from message history instead.

###### Reacting to a preview change

`message_edited` and `message_deleted` both carry the room's preview after the mutation.
Take `previewMessage` whenever it is present — it is the room's current preview, whichever
message the mutation touched.

When it is **omitted**, compare the event's `messageId` against the `messageId` of the
preview being displayed. They differ ⇒ the mutation did not touch the preview, so leave it
alone. They match ⇒ the displayed preview describes the mutated message and must stop being
shown; what to do next differs by event:

| Event | `previewMessage` omitted, `messageId` matches | Why |
|---|---|---|
| `message_deleted` | Clear the preview | The room has no eligible message left, or the recompute could not confirm one — either way the deleted content must not stay on the row. |
| `message_edited` | Re-read the room (`rooms.get` / `subscription.list`) | An edit never empties a room, so this is only a recompute that did not complete; the message still exists and the next read resolves it. |

There is no separate "preview cleared" flag: an omitted `previewMessage` plus a matching
`messageId` is the signal. A client that ignores it keeps rendering the pre-edit or deleted
content until its next room-list read, even though the server has already stopped serving
that preview.

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | |
| `sender` | [Participant](#participant) | `chineseName` is the sender's company name; `displayName` is the composed render-ready name (a bot sender's is its app name). |
| `content` | string | Message content snippet, capped at 500 runes (longer bodies are truncated). On `subscription.list` (both transports) it is truncated further to a short preview — 50 characters by default, whole characters only, no ellipsis appended. |
| `createdAt` | string | RFC 3339 timestamp. |
| `attachments` | [Attachment](#attachment)[] | Optional. Omitted when the message has none. At most 10 — the room list renders a count, not the set. |
| `mentions` | [Participant](#participant)[] | Optional. Mentioned users as wire Participants. Omitted when none. At most 20. |
| `visibleTo` | string | Optional. The message's opaque visibility marker, surfaced verbatim; the backend does not filter the preview on it. |

#### AppSubscription

The nested `app` object carried on **botDM** subscription rows in
`subscription.list`. The botDM's base `Subscription.name` is the **app's display
name**; the `app` object also carries its own `name`. All app fields are optional
(omitted when unset).

| Field | Type | Notes |
|---|---|---|
| `appId` | string | The app's ID. |
| `name` | string | The app's display name. |
| `description` | string | App description. |
| `assistant` | [AppAssistant](#appassistant) | The app's assistant subdocument. |
| `appViewUrl` | map<string, string> | App-view URLs keyed by view name. |
| `reportUrl` | string | App report URL. |
| `forumUrl` | string | App forum URL. |
| `userManualUrl` | string | App user-manual URL. |
| `version` | string | App version. |
| `sponsors` | [AppSponsor](#appsponsor)[] | App sponsors. |

#### HrInfo

HR display names.

| Field | Type | Notes |
|---|---|---|
| `engName` | string | English display name. |
| `chineseName` | string | Chinese display name. |

#### SubscriptionHRInfo

The `hrInfo` field on a [DMSubscription](#subscription) — the DM counterpart's HR record.

| Field | Type | Notes |
|---|---|---|
| `account` | string | Counterpart's account. |
| `name` | string | Counterpart's native (Chinese) name. |
| `engName` | string | Counterpart's English name. |

#### AppAssistant

An app's assistant (bot) subdocument. `name` and `enabled` are always present; `username` and `settingsUrl` are optional.

| Field | Type | Notes |
|---|---|---|
| `name` | string | Assistant/bot name (bot account). |
| `enabled` | boolean | Whether the assistant is enabled. |
| `username` | string | Optional. Assistant display username. |
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

#### PriorityContactItem

One row of a [priorityContacts.get](#settingsprioritycontactsget) /
[priorityContacts.add](#settingsprioritycontactsadd) /
[priorityContacts.remove](#settingsprioritycontactsremove) response. `type`
selects which nested field is present — `user` for `"user"`, `app` for
`"bot"` — so `user` and `app` are mutually exclusive with each other. Both are
omitted when the account no longer resolves (no surviving user document, or a
deleted app) — the row still carries `account` and `type`.

| Field | Type | Notes |
|---|---|---|
| `account` | string | The contact's account. |
| `type` | string | `"user"` or `"bot"`. |
| `user` | [PriorityContactUser](#prioritycontactuser) | Optional. Present only when `type == "user"` and a user document still exists for the account — a deactivated user whose document survives still renders full enrichment; only an account with no document at all degrades to `account` + `type`. |
| `app` | [PriorityContactApp](#prioritycontactapp) | Optional. Present only when `type == "bot"` and the account still resolves to an app. |

#### PriorityContactUser

HR-directory display fields for a `"user"`-type [PriorityContactItem](#prioritycontactitem).

| Field | Type | Notes |
|---|---|---|
| `engName` | string | English display name. |
| `chineseName` | string | Chinese display name. |
| `employeeId` | string | Employee ID. |
| `sectName` | string | Section/department name. |

#### PriorityContactApp

App display fields for a `"bot"`-type [PriorityContactItem](#prioritycontactitem).

| Field | Type | Notes |
|---|---|---|
| `name` | string | App display name. |

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
| `chat.user.{account}.request.room.{roomID}.{siteID}.open` | [Open Room](#open-room) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.message.read-receipt` | [Read Message Receipts](#read-message-receipts) |
| `chat.user.{account}.request.orgs.{orgID}.{siteID}.members` | [List Org Members](#list-org-members) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.app.tabs` | [Get Room App Tabs](#get-room-app-tabs) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.app.cmd-menu` | [Get Room App Command Menu](#get-room-app-command-menu) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.teams.call` | [Start Teams Room Call](#start-teams-room-call) |
| `chat.user.{account}.request.teams.{siteID}.call.user` | [Start Teams User Call](#start-teams-user-call) |
| `chat.user.{account}.request.room.{roomID}.{siteID}.teams.meeting` | [Start Teams Meeting](#start-teams-meeting) |

#### Create Room

**Subject:** `chat.user.{account}.request.room.{siteID}.create`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This is an **async-job RPC**: the synchronous reply only confirms acceptance. The room is created asynchronously in `room-worker`, which publishes the events under "Triggered events" below. The client **must** set an `X-Request-ID` NATS header — Create Room rejects requests without it, and the header value is echoed as the `requestId` on the `AsyncJobResult` event.

The room **type is inferred server-side** from the payload shape — the client does not send it:

- `name` set → `channel`
- `name` empty + exactly one entry in `users` → `dm` (or `botDM` if that user is a `.bot` bot or the `p_admin` platform-admin pseudo-account; a QA `p_` account is an ordinary user, so it yields a regular `dm`)
- `name` empty + `users` is just the caller (e.g. `[caller]` or empty) → **self-DM** (note-to-self): a single-member `dm` room, created through the same async path as any other room. The subscription is **favorited**, and it is **one-per-user** — a repeat create returns the existing room with `status: "exists"`.

The creator's account and the site come from the subject (`chat.user.{account}.request.room.{siteID}.create`); the client does not pass them in the body.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | channels | Channel name (≤ 100 chars). Required to create a channel; leave empty for a DM/botDM. |
| `users` | string[] | no | Internal user IDs (or accounts) to enroll. For a DM, exactly one entry (the other user); for a **self-DM**, the caller themselves (it must be present — an otherwise-empty request is rejected as empty). Channel creates reject a bot account here with `"bots cannot be added to a channel"`, and any account with no matching user document with `user "<account>": user not found`. |
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
| `roomId` | string | The new room's ID. Channel: 17-char base62. DM/botDM: sorted concat of the two internal user IDs. Self-DM: the requester's own user ID concatenated with itself (deterministic). |
| `roomType` | string | `channel`, `dm`, or `botDM`. |

```json
{ "status": "accepted", "roomId": "01970a4f8c2d7c9aQ", "roomType": "channel" }
```

**DM already exists.** When the client asks to create a DM/botDM that already exists — or repeats a **self-DM** create (one self-DM per user) — the reply is a SUCCESS reply carrying the existing room ID (open-or-create contract — the client opens the existing room):

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
- `"bots cannot be added to a channel"` / `"bot not available"` (botDM target whose assistant is disabled)
- `user "<account>": user not found` / `org "<orgId>": invalid org` (each wrapped with the offending account/org ID)
- `"user is missing required name fields"` — rejected only when BOTH `engName` and `chineseName` are empty (either one alone is sufficient)
- `"exceeds maximum capacity (N): would create M members"`

```json
{ "code": "bad_request", "error": "channel name is required" }
```

##### Triggered events — success path

The creator (from the subject) plus any members supplied via `users` / `orgs` / `channels` are enrolled asynchronously. The following events fire:

**1. `chat.user.{account}.response.{requestID}`** — an `AsyncJobResult` to the requester when the job finishes (requires the `X-Request-ID` header). See the [AsyncJobResult schema](#asyncjobresult). `operation` is `"room.create"`.

**2. `chat.user.{account}.event.subscription.update`** — one per enrolled member (including the owner), `action: "added"`. See the [subscription.update schema](#subscriptionupdate-event) under Add Members. The embedded `subscription.room` carries the room view — for **channel** rooms including the E2E key (`room.privateKey` / `room.keyVersion`); DM/botDM rooms are not encrypted and carry no key fields. No separate `room.key` event fires on create — see [§5 Room Encryption](#5-room-encryption).

For **channel** rooms, the first messages (`type: "room_created"`, then `type: "members_added"` when initial members were enrolled) flow through the normal message pipeline and arrive as `new_message` room events (see [§4](#4-message-send)).

##### Triggered events — error path

`None for synchronous rejections.` If the async job fails after `accepted`, the requester receives an `AsyncJobResult` with `status: "error"`.

---

#### Add Members

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.add`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This is an **async-job RPC**: the synchronous reply only confirms acceptance. The actual member adds run asynchronously in `room-worker`, which publishes the events listed under "Triggered events" below. To receive the `AsyncJobResult` event, the client **must** set an `X-Request-ID` NATS header on the original request (see [Request-ID propagation](#request-id-propagation)).

Platform admins (`model.UserRoleAdmin`, same site) bypass the room owner/member check — an admin need not be a room member to add members.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `roomId` | string | no | Optional; the server derives the room ID from the subject and ignores any non-matching value. |
| `users` | string[] | no | Internal user IDs (or accounts) to add directly. May include `.bot` bot accounts: each listed bot must resolve to an app with an **enabled assistant**, else the request is rejected (see Error response); a bot whose home site differs from the room's is allowed (cross-site bot membership). Bots join as plain members, count toward the room's `appCount` (not `userCount` or the capacity cap), and — because a bot can log into the chat frontend — receive the `subscription.update` event (with the room key inline under `subscription.room`) on their encoded per-user subject (`chat.user.{encodedAccount}.…`, dots→underscores; see [§5](#5-room-encryption)). The `p_admin` platform-admin pseudo-account may also be listed; it is admitted **without** app/assistant validation (it has no app) and, like a bot, counts toward `appCount`. Plain `p_` QA test accounts are **ordinary users** — they count toward `userCount`, are subject to the capacity cap, and behave like any human member. |
| `orgs` | string[] | no | Org IDs to add (expanded server-side to all org members). |
| `channels` | array<ChannelRef> | no | Other channels to add as bulk sources. Each entry is `{ "roomId": string, "siteId": string }`. |
| `history.mode` | string | no | `"none"` or `"all"` — controls whether new members see history before they joined. **When omitted or empty, the server treats it as `"all"`** (share-all). `"all"` is capped by the **requester's own** `historySharedSince`: when the adder's history is restricted, the new members inherit the adder's boundary instead of unrestricted history (members can never see more history than whoever added them). `"none"` restricts new members to messages from the add time onward (never earlier than the adder's own boundary — the later of the two wins). Any other value is rejected with `history.mode must be "none" or "all"` (`bad_request`). |

The fields `requesterId`, `requesterAccount`, `timestamp`, and `historySharedSince` on the Go `AddMembersRequest` are server-set — the client should omit them (any client-supplied `historySharedSince` is overwritten).

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

See [Error envelope](#6-error-envelope-reference). Returned synchronously when validation or authorization fails (e.g. requester not in room, room is full, room is restricted and requester is not owner, unrecognized `history.mode`). A `users` entry that is a bot is rejected with `"bot not available"` (`bot_not_available`) when it has no app record or its assistant is disabled; a bot whose home site differs from the room's site is admitted (cross-site bot membership is allowed). Any `orgs` entry that matches zero users (no user with `sectId == orgId` or `deptId == orgId`) is rejected with `org "<orgId>": invalid org`, and any `users` entry that has no matching user document is rejected with `user "<account>": user not found` (each wrapped with the offending account/org ID) — in both cases the request is not queued and no members are added. Bots resolved from `channels` / `orgs` expansion are silently filtered (only explicitly listed bots are added).

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

**2. `chat.user.{newMember}.event.subscription.update`** — one event per **newly subscribed** member (not the requester, not existing members, not org→individual upgrades). Bot members receive it too, delivered on their **encoded** per-user subject (`chat.user.{encodedAccount}.event.subscription.update`, dots→underscores — the same transform as their NATS JWT scope and `room.key` delivery), since a bot can log into the chat frontend.

###### `subscription.update` event

Shared by Add Members, Remove Member, and Update Member Role.

| Field | Type | Notes |
|---|---|---|
| `userId` | string | The affected user's internal user ID. Omitted on the org-removal path (only `subscription.u.account` is set there). |
| `subscription` | [Subscription](#subscription) | For `added` / `role_updated`: the full Subscription record. On `added` it additionally embeds a populated `room` object ([SubscriptionRoom](#subscriptionroom)) — `previewMessage` always omitted; `privateKey`/`keyVersion` present only for encrypted channel rooms. For `removed`: a [RemovedSubscriptionRef](#removedsubscriptionref) lean ref (see Remove Member). |
| `action` | string | `"added"`, `"removed"`, `"role_updated"`, `"mute_toggled"`, `"favorite_toggled"`, `"opened"`, or `"read"`. |
| `roomName` | string | Per-subscriber display label, set only where the server already has the name. On `added`: `channel` → room name; `dm` → counterpart's display name (`engName` + `chineseName`, falling back to account); `botDM` → the bot's app name. On `role_updated`: the channel name. Omitted (`omitempty`) on `mute_toggled` / `favorite_toggled` / `opened` / `read`, and absent on `removed`. |
| `hrInfo` | [CounterpartHRInfo](#counterparthrinfo) | The DM counterpart's HR record, so the client can render the new sidebar row without a `subscription.list` refetch. Sent on `added` `dm` / `botDM` events when the counterpart account does **not** end in `.bot` — i.e. to both sides of a `dm`, and to the bot's own copy of a `botDM`. On a self-DM (note-to-self) the counterpart is the recipient, so the event carries their own record. Omitted on `channel` / `discussion` rooms and when the user lookup missed. |
| `appInfo` | [CounterpartAppInfo](#counterpartappinfo) | The counterpart's app record, sent on `added` `botDM` events when the counterpart account ends in `.bot` — i.e. to the human member. Mutually exclusive with `hrInfo`; omitted when the app lookup missed. |
| `timestamp` | number | Epoch ms (UTC). |

On `added` / `role_updated` / `mute_toggled` / `favorite_toggled` / `opened` the embedded `Subscription` serializes its ID as `id` (not `_id`) and the user under `u` (not `user`). Non-`omitempty` fields (`id`, `u`, `roomId`, `siteId`, `roles`, `name`, `roomType`, `joinedAt`, `hasMention`, `alert`, `muted`, `favorite`, `open`) are always present — and the envelope's `roomName` is `omitempty`: set on `added` / `role_updated`, omitted on `mute_toggled` / `favorite_toggled` / `opened` / `read`. On `added` the nested `room` object matches a `subscription.list` row (minus `previewMessage`), so clients can render the sidebar entry — and store the room key — from this single event. `removed` events use a dedicated lean payload (`SubscriptionRemovedEvent`) whose `subscription` carries **only** `roomId`, `roomType`, and `u` — no zero-valued `Subscription` fields are sent.

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

On a newly created **DM** the event additionally carries the counterpart's `hrInfo` — everything the client needs to render the sidebar row on its own:

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

For a **botDM**, the human member's event carries `appInfo` instead (the bot's own copy of the event carries the human's `hrInfo`):

```json
{
  "action": "added",
  "roomName": "Helper Bot",
  "appInfo": { "id": "01970a4f8c2d7c9aA1", "name": "Helper Bot", "assistantName": "helper.bot" },
  "timestamp": 1778054483000
}
```

**3.** ~~`chat.user.{newMember}.event.room.key`~~ — **no longer fired on add.** The room key is delivered inline on the `added` event above (`subscription.room.privateKey` / `keyVersion`); `room.key` events now fire only on key rotation (member removal). See [§5 Room Encryption](#5-room-encryption).

**4. `chat.room.{roomID}.event.member` / `chat.local.room.{roomID}.event.member`** — a `MemberAddEvent` (`type: "member_added"`) published once whenever the room's member list actually changes: a new account joins, a genuinely new org is added, or an existing org member is upgraded to an individual membership (see the no-op note below for what does **not** fire). Routed on the room's namespace exactly like `chat.room.{roomID}.event` — pick the subject by the room's `crossSite` flag (`chat.local.room.{roomID}.event.member` when `crossSite: false`, `chat.room.{roomID}.event.member` when `crossSite: true`/unknown). Delivered to clients subscribed to the room on that namespace.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"member_added"`. |
| `roomId` | string | |
| `roomName` | string | |
| `roomType` | string | `"channel"`, `"dm"`, `"botDM"`, or `"discussion"`. Omitted when empty. |
| `members` | [RoomMemberEntry](#roommemberentry)[] | The requested entities in member.list display shape (the [RoomMemberEntry](#roommemberentry) payload only — no membership `id`/`rid`/`ts` envelope): one org entry per requested org first (`orgName`, `orgCode`, `memberCount`, `orgDescription`), then one individual entry per requested user that was newly subscribed **or** upgraded to an individual membership (`engName`, `chineseName`, `sectName`, `employeeId`). Unlike [List Members](#list-members) (`enrich: true`), individual entries here omit `isOwner` (new members are never owners) and `name` (bot display name). Accounts joined only via org expansion are **not** listed individually — they are represented by their org entry, mirroring `member.list`. |
| `siteId` | string | The room's home site. |
| `requesterAccount` | string | The account that initiated the add. Omitted when empty. |
| `joinedAt` | number | Epoch ms (UTC). |
| `historySharedSince` | number | Optional. Epoch ms (UTC); the new members' history boundary, present when their history is restricted. For `history.mode: "none"` adds it is the add time or the requester's own boundary, whichever is later; for a share-all add by a requester whose own history is capped it is the requester's inherited boundary. Absent = unrestricted. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |

The event carries no separate account list — member identities are in `members`. When new members actually join (or a new org is added), a `members_added` system message also flows through the message pipeline and arrives as a `new_message` room event; a pure org→individual upgrade posts no such message.

The cross-site INBOX copy of this event additionally carries `accounts` and `lastMsgAt` (the room's activity position, epoch ms). Both are server-internal federation fields, stripped from the client-facing copy documented above — clients never receive them.

> [!NOTE]
> **No-op:** when the request changes nothing — every requested account already subscribed, no org member upgraded to an individual membership, and every requested org already present — the requester still gets an `AsyncJobResult` with `status: "ok"` but **no** `subscription.update` / `member_added` events follow. In particular, **re-adding an already-present org is a no-op**. An **org→individual upgrade** (an existing org member added individually) is **not** a no-op: `member_added` fires with that individual in `members`, but no `members_added` system message is posted (no one newly joined).

###### CounterpartHRInfo

| Field | Type | Notes |
|---|---|---|
| `account` | string | Counterpart's account. |
| `chineseName` | string | Counterpart's native (Chinese) name. Omitted when empty. |
| `engName` | string | Counterpart's English name. Omitted when empty. |

> Both name fields are `omitempty`, so a directory record with neither yields `{"account": "..."}` alone — and `roomName` then falls back to the account.
>
> Same wire shape as the search hit's [MessageHRInfo](#messagehrinfo). The key is `chineseName` here, whereas [SubscriptionHRInfo](#subscriptionhrinfo) — the `hrInfo` nested on a `subscription.list` DM row — still uses the legacy `name`.
>
> **This divergence is deliberate.** PR #165 scoped the `chineseName` rekey to "search response only; other payloads untouched", deliberately leaving `subscription.list` on the legacy key. New and reshaped surfaces take `chineseName`; existing ones are not rekeyed. A client must therefore **not** reuse one `hrInfo` parser across the event and the list — it would silently drop the name on one of them.

###### CounterpartAppInfo

| Field | Type | Notes |
|---|---|---|
| `id` | string | App ID. |
| `name` | string | App display name. Empty string when the app document has no name — `roomName` then falls back to the bot account. |
| `assistantName` | string | The bot account the app answers on. |

##### Triggered events — error path

`None for synchronous rejections.` If the async job fails after `accepted`, the requester receives the `AsyncJobResult` with `status: "error"`.

---

#### Remove Member

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.remove`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This is an **async-job RPC**: the synchronous reply only confirms acceptance. The actual member removal runs asynchronously in `room-worker`, which publishes the events listed under "Triggered events" below. To receive the `AsyncJobResult` event, the client **must** set an `X-Request-ID` NATS header on the original request (see [Request-ID propagation](#request-id-propagation)).

Platform admins (`model.UserRoleAdmin`, same site) bypass the room owner/member check — an admin need not be a room member to remove members.

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

See [Error envelope](#6-error-envelope-reference). Returned synchronously when validation or authorization fails (e.g. neither or both of `account`/`orgId` set, requester is not an owner, target is the last member, or org member cannot leave individually). The last-member guard counts **human** members only: removing the last human is rejected even when bot members remain, while removing a bot never trips the guard.

```json
{ "code": "bad_request", "error": "exactly one of account or orgId must be set" }
```

##### Triggered events — success path

**1. `chat.user.{requesterAccount}.response.{requestID}`** — an [`AsyncJobResult`](#asyncjobresult) to the requester when the removal finishes (requires `X-Request-ID`). `operation` is `"room.member.remove"` for single-account removal, `"room.member.remove_org"` for org removal.

**2. `chat.user.{removedAccount}.event.subscription.update`** — one per removed account, `action: "removed"`. Bot members receive it too, on their **encoded** per-user subject (`chat.user.{encodedAccount}.…`, dots→underscores), since a bot can log into the chat frontend. The payload is a dedicated `SubscriptionRemovedEvent` (not the full `SubscriptionUpdateEvent`): its `subscription` carries **only** `roomId`, `roomType`, and `u` so no zero-valued `Subscription` fields are sent. On the **org-removal** path `userId` is omitted (only `subscription.u.account` is set).

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
    "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "bob", "isBot": false }
  },
  "action": "removed",
  "timestamp": 1778054483000
}
```

**3. `chat.user.{survivor}.event.room.key`** — on a channel removal the room key is **rotated**; every surviving member receives a new `RoomKeyEvent` with an incremented `version`. The removed account stops receiving key events. See [§5 Room Encryption](#5-room-encryption).

**4. `chat.room.{roomID}.event.member` / `chat.local.room.{roomID}.event.member`** — a `MemberRemoveEvent` (`type: "member_left"` for a self-leave, `"member_removed"` for a forced removal or org removal). Routed on the room's namespace by its `crossSite` flag, exactly like the add event above (`chat.local.room.{roomID}.event.member` when `crossSite: false`).

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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This is a **synchronous RPC**. `room-service` validates the request, applies the role change atomically, emits the event below, and only then returns the reply. There is no async job, no `AsyncJobResult`, and no `chat.user.{requesterAccount}.response.{requestID}` event for this RPC.

Platform admins (`model.UserRoleAdmin`, same site) bypass the room owner/member check — an admin need not be a room member to update roles.

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
- Promote attempt on a bot account — rejected with `"bots cannot be room owners"` (demoting a bot stays allowed).
- Promote attempt on an org-only member (individual subscription required).

```json
{ "code": "forbidden", "error": "only owners can update roles" }
```

##### Triggered events — success path

**`chat.user.{targetAccount}.event.subscription.update`** — emitted once for the user whose role changed, `action: "role_updated"`. Delivered to the target user only (not the requester, not other members). See the [subscription.update schema](#subscriptionupdate-event); the embedded `Subscription` reflects the updated `roles`. No `AsyncJobResult` and no room-key event fire for role updates.

**Cross-site federation:** when the target user's home site differs from the room's site, `room-service` emits an `OutboxEvent` on the OUTBOX stream and `outbox-worker` forwards the cross-site `role_updated` event (at-least-once) to `chat.inbox.{userSite}.external.role_updated`, where `inbox-worker` applies the updated `roles` to the local `Subscription` (guarded by `rolesUpdatedAt`).

```json
{
  "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "subscription": {
    "id": "01970a4f8c2d7c9a01970a4f8c2d7c9b",
    "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "bob", "isBot": false },
    "roomId": "01970a4f8c2d7c9aQ",
    "roomType": "channel",
    "siteId": "siteA",
    "roles": ["member", "owner"],
    "joinedAt": "2026-05-06T08:01:23Z"
  },
  "action": "role_updated",
  "roomName": "engineering-announcements",
  "timestamp": 1778054483000
}
```

##### Triggered events — error path

When the reply is an error envelope, no events follow. All validation and the role mutation happen synchronously in `room-service`, so any failure is surfaced directly in the error reply — there is no deferred worker step and no separate failure event.

---

#### Rename Room

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.room.rename`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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

**1. `chat.room.{roomID}.event`** — a `RoomRenamedRoomEvent` published by `room-worker` to every client subscribed to the room.

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
  "timestamp": 1778054483000,
  "newName": "engineering-general",
  "byAccount": "alice",
  "renamedAt": "2026-05-06T08:01:23Z"
}
```

> [!NOTE]
> **No per-subscription `subscription.update` event is published for the rename.** Clients drive their local subscription `name` update off this single room-scoped event.

**2. `chat.user.{requesterAccount}.response.{requestID}`** — an [`AsyncJobResult`](#asyncjobresult) to the requester when the rename finishes (requires `X-Request-ID`). `operation` is `"room.rename"`. `status` is `"ok"` on success or `"error"` if the async job fails.

**3. Cross-site inbox events** — one event per remote site that has federated members. Published directly to `chat.inbox.{remoteSiteID}.external.room_renamed` (the destination's `INBOX-{remoteSiteID}` stream); remote `inbox-worker` mirrors the rename.

##### Triggered events — error path

When the synchronous reply is an error envelope, the request was rejected before publishing to the worker — no events follow.

---

> [!NOTE]
> **Server-internal — not a client RPC.** The "Set Room Restricted" RPC (formerly "Set Room Visibility") is admin-only and lives outside the client API surface. It is a **synchronous** server-to-server NATS request/reply on `chat.server.request.room.{siteID}.restricted`. Admin tooling sends a `RoomRestrictedRequest` (`pkg/model/room.go`) carrying:
>
> - `roomId` — channel room to mutate
> - `account` — the admin caller; carried as `byAccount` on the room event and recorded in room-service's log line
> - `restricted` — whether the room is members-only
> - `externalAccess` — whether the room is reachable from outside the company network (e.g. internet-side / off-VPN clients). This is a network-access gate, NOT a cross-site federation flag
> - `ownerAccount` — **required** on the `false → true` transition. Whenever it is supplied together with `restricted: true` — transition or not — that account is promoted to **sole** owner and every other member is reset to plain member, so an already-restricted room can have its owner rotated. Omit it to change the flags without touching anyone's roles
>
> room-service does the Mongo writes, emits one `OutboxEvent` on the OUTBOX stream per remote federated site, and replies `{"status":"ok","requestId":"…"}` once the work is committed. `outbox-worker` forwards the cross-site `room_restricted` event (at-least-once) to each remote site's `chat.inbox.{remoteSiteID}.external.room_restricted`. No `AsyncJobResult` is emitted — the reply *is* the result.
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
> | `ownerAccount` | string | The account designated sole owner by this call. Present on any restricting call that named one, including an owner rotation on an already-restricted room; omitted when none was sent. |
> | `byAccount` | string | The admin who made the change. |
> | `changedAt` | string | ISO-8601 timestamp of when the change was applied. |

---

#### List Members

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.member.list`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `limit` | number | no | If set, must be `> 0`. Caps the number of members returned. |
| `offset` | number | no | If set, must be `>= 0`. For pagination. |
| `enrich` | boolean | no | When `true`, populates the display fields (`engName`, `chineseName`, `name`, `isOwner`, `sectName`, `employeeId`, `orgName`, `orgCode`, `memberCount`, `orgDescription`) on each entry. Omitted-or-`false` returns the lean record only. |

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
| `sectName` | string | Optional. The member's section name. Populated only when `enrich: true` and entry is an individual. |
| `employeeId` | string | Optional. The member's employee ID. Populated only when `enrich: true` and entry is an individual. |
| `name` | string | Optional. Bot/app display name from `apps.name` when the member's account ends with `.bot`. Mutually exclusive with `engName`/`chineseName`. |
| `isOwner` | boolean | Optional. Populated only when `enrich: true`. |
| `orgName` | string | Optional. Org's display name (dept name preferred, sect name fallback), combined with the TC name when present. Populated only when `enrich: true` and entry is an org. |
| `orgCode` | string | Optional. Org's plain section/department name (dept-first), without the TC-name combination `orgName` applies and with no orgID fallback. Populated only when `enrich: true` and entry is an org. |
| `memberCount` | number | Optional. Populated only when `enrich: true` and entry is an org. |
| `orgDescription` | string | Optional. Org's description, from the same org unit shown in `orgName` (dept-first); omitted when empty. Populated only when `enrich: true` and entry is an org. |

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
        "isOwner": true,
        "sectName": "Cardiology",
        "employeeId": "E10293"
      }
    },
    {
      "id": "01970a4f8c2d7c9a01970a4f8c2d7c9c",
      "rid": "01970a4f8c2d7c9aQ",
      "ts": "2026-05-01T10:05:00Z",
      "member": {
        "id": "DEPT-100",
        "type": "org",
        "orgName": "Cardiology Department",
        "orgCode": "Cardiology Department",
        "orgDescription": "Inpatient & outpatient cardiac care",
        "memberCount": 42
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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `limit` | number | no | Upper bound on returned rows. When omitted, the server uses `min(3, room.userCount)` (an empty room returns an empty list); when supplied, must be `> 0` and `<= room.userCount`. Fewer rows may come back — members with an empty `statusText` or with `statusIsShow=false` are omitted (see `members`). |

```json
{ "limit": 5 }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `members` | array<MemberStatus> | One entry per room subscription **with a non-empty `statusText` and `statusIsShow=true`**, projected from the joined `users` document. Members without a status set, or who have opted out of surfacing it (`statusIsShow=false`), are omitted. |

`MemberStatus`:

| Field | Type | Notes |
|---|---|---|
| `account` | string | The user's account. |
| `engName` | string | English display name. |
| `chineseName` | string | Chinese display name. |
| `statusIsShow` | boolean | Whether the user has chosen to surface their status text. Always `true` — members with `statusIsShow=false` are omitted from `members`. |
| `statusText` | string | Free-form presence text (e.g. `"available"`, `"in a meeting"`); always non-empty — members without a status are omitted from `members`. |

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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`**.

Used by the message composer's `@…` mention autocomplete. Returns subscriptions discriminated as `user` or `app`. The caller is always excluded from the result set. The `p_admin` platform-admin pseudo-account is also excluded — it is not `@`-mentionable. Plain `p_` QA test accounts are ordinary users and remain mentionable.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `limit` | number | no | When omitted, the server uses `min(3, room.userCount + room.appCount)` (small rooms cap automatically, empty rooms return an empty list). When supplied, must be `> 0`; a value larger than `room.userCount + room.appCount` is clamped to that cap (not rejected). |
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
- `"limit must be > 0 and <= room user count + app count"` — limit was `0` or negative. (A positive limit larger than the room's combined user + app population is clamped to that cap, not rejected.)

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Mark Messages Read

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.message.read`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This is a **synchronous RPC** — `room-service` performs all writes inline before replying. The handler validates room membership, persists the new `lastSeenAt` on the user's `Subscription` (clearing `alert` and `hasMention`), and optionally recomputes `Room.MinUserLastSeenAt`.

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

- **Alert cleared:** reading the room clears the subscription's alert flag.
- **No `JoinedAt` fallback for the early-return:** if `subscription.lastSeenAt` is null (the user was invited but has never opened the room), the handler does **not** treat `joinedAt` as a synthetic read position — being invited isn't reading. The room-floor recompute runs in this case so a member who has just read for the first time is reflected in the floor.
- **Room-floor recompute (`Room.MinUserLastSeenAt`):** the room's read floor (surfaced as `minUserLastSeenAt` in history responses) is a **strict "everyone has read" marker**: `MIN(lastSeenAt)` across the room's **human** subscriptions — bots (`u.isBot`) and the `p_admin` platform-admin pseudo-account are excluded — set **only when every counted subscription has a usable `lastSeenAt`**. If **any** counted member has never read the room (no/zero `lastSeenAt` — e.g. invited but never opened), the floor is `$unset` (null). Because bots and the admin pseudo-account are excluded, a **botDM room resolves to the human's `lastSeenAt`** rather than being forced null by a never-reading bot; plain `p_` QA accounts are ordinary members and do count. Reading a room can advance the floor (or, if this was the last unread member, raise it from null to a value).
- **Recompute trigger & a known gap:** the floor is recomputed only on this Mark Read path, and only when the caller was not already past `room.lastMsgAt` (the early-return above). Adding a member does not itself recompute the floor, so a newly-invited, never-read member will not flip an existing non-null floor to null until the next recompute is triggered (e.g. that member reads, or another member reads while the room has content).
- **Read-floor fan-out:** when (and only when) the recompute above changes `Room.MinUserLastSeenAt`, the server publishes a `message_read` room event carrying the new floor, so peers can advance read-receipt / unread UI live. Fan-out is best-effort (a publish failure does not fail the RPC) and never fires on the early-return paths or when the floor is unchanged. No system message is written.

##### Triggered events — success path

**1. `chat.user.{account}.event.subscription.update`** — emitted once for the reader (non-bot only) on the success path (best-effort, core NATS), `action: "read"`. See the [subscription.update schema](#subscriptionupdate-event). The embedded `Subscription` carries the updated `lastSeenAt` and `alert`, plus the read-cleared derived flags `hasMention: false` and `hasGroupMention: false` (reading the room clears both). Not fired on the early-return paths (empty room or reader already past `lastMsgAt`).

**2. Floor advance events** — emitted **only when the room read floor (`Room.MinUserLastSeenAt`) changes** (best-effort, core NATS):

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

**3. Cross-site federation** — when the reader's home site differs from the room's site, `room-service` emits an `OutboxEvent` on the OUTBOX stream and `outbox-worker` forwards the cross-site `subscription_read` event (at-least-once) to `chat.inbox.{userSite}.external.subscription_read`, where `inbox-worker` applies `lastSeenAt`/`alert` to the local `Subscription` (guarded by `lastSeenAt`).

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Mark Thread as Read

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.message.thread.read`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

A **synchronous RPC** that clears a single thread's unread state for the caller. `room-service` validates room membership, then — when the caller follows the thread — concurrently refreshes the `ThreadSubscription` (`lastSeenAt`, `updatedAt`, `hasMention=false`) and `$pull`s the thread's parent message ID out of the caller's `Subscription.threadUnread`, and — for cross-site users — emits an `OutboxEvent` on the OUTBOX stream; `outbox-worker` forwards the cross-site `thread_read` event to the user's home site (at-least-once) so the destination `inbox-worker` can mirror both updates. If the caller has no `ThreadSubscription` for the thread (i.e. does not follow it), there is nothing to clear: the RPC performs no writes and returns `accepted`.

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
- `"threadId is required"` — body is missing `threadId` or sends an empty string.
- `"invalid message-thread-read subject: …"` — the subject is malformed.

##### Behaviour notes

- **Local write:** the RPC advances the caller's `ThreadSubscription` read state (`lastSeenAt`, `updatedAt`, `hasMention=false`) and removes the thread's parent message ID from `Subscription.threadUnread` via `$pull` (the field is `$unset`, not stored empty, once the array drains). Room-level alert/unread is untouched — that's cleared by [Mark Messages Read](#mark-messages-read).
- **Cross-site federation:** if the user's home site differs from the handler's site, the handler emits an `OutboxEvent` on the OUTBOX stream and `outbox-worker` forwards the cross-site `thread_read` event (at-least-once) to `chat.inbox.{userSite}.external.thread_read` with payload `{account, roomId, threadRoomId, lastSeenAt, parentMessageId, timestamp}` (timestamps as `int64` UnixMilli). The destination `inbox-worker` applies `lastSeenAt`+`updatedAt`+`hasMention=false` to the local ThreadSubscription with an `$lt` order-safety guard, and — only when that guard matches — `$pull`s `parentMessageId` from the local Subscription's `threadUnread`. The per-ID pull commutes with concurrent `thread_unread_added` merges for other threads, so out-of-order delivery can neither regress the read position nor lose or resurrect unrelated unread IDs.
- **Not following the thread:** if the caller holds no `ThreadSubscription` for the supplied `threadId` in the room, the RPC is an idempotent no-op — it runs no `ThreadSubscription` write, no cross-site federation, and no floor recompute, and returns `{ "status": "accepted" }`.
- **Defensive `roomId` filter:** the thread-subscription lookup additionally enforces that the supplied `threadId` belongs to the room named in the subject. A thread that belongs to a different room matches no `ThreadSubscription` and so is treated as the not-following no-op above (rather than clearing an unrelated thread).
- **Thread-room read-floor recompute:** after the `ThreadSubscription` write succeeds, `room-service` recomputes `thread_rooms.minUserLastSeenAt` = `MIN(lastSeenAt)` across all `thread_subscriptions` for the thread room. The floor is set only when every subscriber has a usable `lastSeenAt`; otherwise it is cleared. The recompute is best-effort — a failure is logged but does not fail the RPC. The stored value is also available via [Get Thread Messages](#get-thread-messages).
- **Read-floor fan-out:** when (and only when) the recompute above changes `thread_rooms.minUserLastSeenAt`, the server publishes a `thread_message_read` event (routed by the **parent** room's type) carrying the new floor, so peers can advance thread read-receipt UI live. Best-effort (a publish failure does not fail the RPC); never fires when the floor is unchanged or the thread room is missing.
- **No system message:** thread reads are silent; only the requester receives the `accepted` reply.

##### Triggered events — success path

**Floor advance events** — emitted **only when the thread read floor (`thread_rooms.minUserLastSeenAt`) changes** (best-effort, core NATS, routed by the **parent** room's type):

- **Channel parent — `chat.room.{roomID}.event`** — a single `thread_message_read` event to every client subscribed to the parent room.
- **DM parent — `chat.user.{account}.event.room`** — one `thread_message_read` event per subscriber.
- **botDM / other parent types** — no fan-out (the floor is always null).

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"thread_message_read"`. |
| `roomId` | string | The **parent** room (for client routing/scoping). |
| `threadRoomId` | string | The thread room whose floor advanced. |
| `minUserLastSeenAt` | string | Optional. RFC3339 UTC timestamp of the new read floor. **Omitted** when the floor is null (a member is still fully unread). |
| `timestamp` | number | Event publish time, UTC milliseconds since Unix epoch. |

```json
{
  "type": "thread_message_read",
  "roomId": "Rb3kQ2",
  "threadRoomId": "Tx9aLm",
  "minUserLastSeenAt": "2026-06-09T10:30:00Z",
  "timestamp": 1749465000123
}
```

(Cross-site users may additionally observe a delayed cache update on their home site via the cross-site inbox flow above; this is treated as cache convergence rather than a client-visible event for this RPC.)

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Toggle Mute

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.mute.toggle`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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
- **Cross-site federation:** when the requester's home site differs from the room's site, `room-service` emits an `OutboxEvent` on the OUTBOX stream and `outbox-worker` forwards the cross-site `subscription_mute_toggled` event (at-least-once) to `chat.inbox.{userSite}.external.subscription_mute_toggled`, where `inbox-worker` applies `muted` to the local `Subscription` (guarded by `muteUpdatedAt`).

---

#### Toggle Favorite

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.favorite.toggle`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Synchronous RPC. `room-service` flips `Subscription.favorite` for the requester in a single atomic Mongo `FindOneAndUpdate`, replies with the resulting value, and fans out a `subscription.update` event to the user's other client sessions. Used by the client to render the per-user "favorited" sidebar section; backend treats the flag as a render hint only — no downstream behaviour (notifications, routing, retention) is gated on it.

Favorites membership is also manually orderable via [Move Chat to Section](#move-chat-to-section) (`sectionId: "favorites"`) — the two RPCs stay in sync: toggling favorite **off** also clears `sectionId`/`sectionOrder` when they were `"favorites"` (toggling **on** leaves any existing `sectionId` alone); moving a chat **into** `"favorites"` sets `favorite: true`, moving it to any other section (or removing it) sets `favorite: false`. Clients keep reading membership from `favorite`, unchanged.

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

When the requester's home site differs from the room's site, `room-service` emits an `OutboxEvent` on the OUTBOX stream and `outbox-worker` forwards the cross-site `subscription_favorite_toggled` event (at-least-once) to `chat.inbox.{userSite}.external.subscription_favorite_toggled`. `inbox-worker` on the user's home site mirrors the flip onto the local `Subscription` document. Missing-subscription on the home site (e.g., a federation race) is a silent no-op — no NACK, no redelivery loop.

---

#### Move Chat to Section

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.chat.move`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Synchronous RPC. Assigns a chat to a custom chatlist section and sets its manual order, or removes it from its section. Membership + order live on the **subscription** (`sectionId` / `sectionOrder`) — the exact storage + fanout model as `favorite.toggle`, so the per-user section overlay never carries per-chat data (see [Chatlist Sections](#chatlist-sections)). `room-service` writes one subscription, replies with the result, and fans out a `subscription.update` (`action: "section_moved"`).

**Order key.** `sectionOrder` is a `float64`. `afterRoomId` places the chat just after that room (midpoint between its order and the next member's); `beforeRoomId` places it just before that room (top-insertion when the target is the section head); omit both to append (last + 1). `afterRoomId` and `beforeRoomId` are mutually exclusive — supplying both is rejected. A single move is one subscription write. When repeated inserts into one gap exhaust float precision, `room-service` transparently re-spaces that section's members and retries — the client sees nothing.

##### Request body

| Field | Type | Notes |
|---|---|---|
| `sectionId` | string \| null | The custom section to move the chat into, or `"favorites"`. `null` (explicit JSON `null`) **or omitting the field entirely** both remove it from its section (falls back to a derived built-in, and clears `favorite` if it was set) — the two are indistinguishable at the wire layer and handled identically. The other built-in ids (`apps`/`teams`/`chats`) are rejected — their membership is derived, not user-set. `favorites` is the one built-in target: moving in sets `Subscription.favorite = true` (mirroring [Toggle Favorite](#toggle-favorite)); moving to any other section sets it `false`. |
| `afterRoomId` | string | Optional. Place the chat just after this room within the section. Omit to append at the end. Mutually exclusive with `beforeRoomId`. |
| `beforeRoomId` | string | Optional. Place the chat just before this room within the section (top-insertion when it is the section head). Mutually exclusive with `afterRoomId`. |

##### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"ok"`. |
| `sectionId` | string | Omitted on a remove. The section the chat is now in. |
| `sectionOrder` | number | Omitted on a remove. The chat's new manual position. |

```json
{ "status": "ok", "sectionId": "0192...uuid", "sectionOrder": 3.5 }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- reason `chatlist_builtin_target` — `sectionId` is a built-in section other than `favorites`.
- reason `chatlist_section_not_found` — `sectionId` is empty.
- `"only room members can list members"` — the user has no subscription in the room.

##### Triggered events — success path

**`chat.user.{account}.event.subscription.update`** — `action: "section_moved"`; `subscription` carries the updated `sectionId` / `sectionOrder`. Cross-site, the `subscription_section_moved` inbox event mirrors it onto the user's home-site subscription (HWM-guarded by `sectionUpdatedAt`), the same lane `favorite_toggled` uses.

---

#### Open Room

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.open`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Synchronous RPC. `room-service` sets `Subscription.open` to `true` for the requester in a single atomic Mongo `FindOneAndUpdate`, replies with the resulting value, and fans out a `subscription.update` event to the user's other client sessions. Used by the client to reveal a room in the sidebar.

Idempotency: this is a set, not a toggle. Every successful call sets `open` to `true`; redelivery of the same RPC is a no-op.

##### Request body

The subject already carries `account` and `roomID`, so no body fields are required. Clients may send `{}` or omit the body entirely; any body content is ignored.

##### Success response

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"ok"`. |
| `open` | boolean | Always `true`. |

```json
{ "status": "ok", "open": true }
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors:

- `"only room members can list members"` — the user has no subscription in the room (sentinel reused across membership-gated RPCs).
- `"invalid open-room subject: …"` — the subject is malformed.

##### Triggered events — success path

**`chat.user.{account}.event.subscription.update`** — emitted once for the requester so other client sessions reconcile.

| Field | Type | Notes |
|---|---|---|
| `userId` | string | The requester's internal user ID. |
| `subscription` | [Subscription](#subscription) | The Subscription record with the updated `open`. |
| `action` | string | `"opened"`. |
| `timestamp` | number | Epoch ms (UTC). |

##### Cross-site behaviour

When the requester's home site differs from the room's site, `room-service` emits an `OutboxEvent` on the OUTBOX stream and `outbox-worker` forwards the cross-site `subscription_opened` event (at-least-once) to `chat.inbox.{userSite}.external.subscription_opened`. `inbox-worker` on the user's home site mirrors the change onto the local `Subscription` document, setting `open` to `true`. This mirror is idempotent and eventually consistent: if the home-site subscription doesn't exist yet (e.g., `subscription_opened` races ahead of the originating `member_added`), the event is redelivered until the subscription exists.

---

#### Read Message Receipts

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.message.read-receipt`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

A **synchronous, sender-only** RPC. Returns the list of users on the local site whose `subscription.lastSeenAt` is at or after the target message's `createdAt`. Only the message author may call it. The author is excluded from the result.

For a **thread-only reply** (a threaded reply not mirrored to the channel, i.e. not `tshow`), readers are resolved from **thread** read-state (`thread_subscriptions.lastSeenAt`) rather than the room's: the reply never appears in the channel, so a member's channel read-position is not evidence they saw it. Channel messages and `tshow` replies (which do appear in the channel) continue to use room read-state. This keeps the receipt consistent with the thread's `minUserLastSeenAt`.

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
- `"read receipts are temporarily unavailable"` (`code: "unavailable"`, `reason: "read_receipts_unavailable"`) — the message-history service used to resolve the target message is unreachable. This affects only read receipts; other room operations are unaffected. Clients should show a transient message and allow a retry.
- `"message is outside access window"` (`code: "forbidden"`, `reason: "outside_access_window"`) — the message predates the requester's history-shared-since for the room (e.g. they left and rejoined). Read receipts are resolved via the history service, which enforces the same access window as message reads, so a sender cannot view receipts for a message they can no longer access.

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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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
| `tabUrl` | string | Computed from `apps.channelTab.url.default`, which carries the full URL (no server-side base-URL rewrite). Substituted template variables: `${roomId}`, `${siteId}`, `${roomType}` (legacy room-type vocabulary: `channel`/`discussion` → `p`, `dm`/`botDM` → `d`, unknown → `p`), `${roomOrigin}` (legacy origin URL of the room's home site from `LEGACY_ROOM_ORIGINS`; empty string when the site is unconfigured). Apps whose template URL is empty, unparseable, or not an absolute http(s) URL are silently skipped. |
| `assistant` | [AppAssistant](#appassistant) | Optional. `apps.assistant` subdocument if set. |

```json
{
  "apps": [
    {
      "id": "app-weather",
      "name": "Weather",
      "tabUrl": "https://template-a.com/apps/weather?room=01970a4f8c2d7c9aQ&roomType=p&roomOrigin=https://legacy.site-a.com",
      "assistant": { "enabled": true, "name": "weather.bot" }
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). Common errors: `"not authorized to access this room's apps"` (caller is not a room member, or the room does not exist), `"response payload exceeds maximum size"` (rare: response would exceed the NATS server's `max_payload`).

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Room App Command Menu

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.app.cmd-menu`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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

> **Note on `External client label`:** each Teams RPC below lists an HTTP-style
> label (e.g. `POST /api/v1/calls/room`). That label is the path the **edge
> gateway exposes to external/mobile clients**; the gateway translates it to the
> NATS RPC shown under **Subject**. This service implements **only** the NATS RPC
> (request/reply over `chat.user.{account}.>`) — it does not serve an HTTP endpoint.

#### Start Teams Room Call

Builds a Microsoft Teams deep link for a call to every other member of the room (the caller is excluded). No Graph API call — the link is built from the member list, deriving each member's email as `account@TEAMS_EMAIL_DOMAIN`.

External client label: `POST /api/v1/calls/room`.

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.teams.call`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's origin `siteID`.
- The requester account is taken from the subject, not from a token.

##### Request body

Empty body is accepted (the room is the subject's `{roomID}`).

| Field | Type | Required | Notes |
|---|---|---|---|
| `roomId` | string | no | Optional echo of the room; the authoritative room is the subject's `{roomID}`. |

##### Success response

| Field | Type | Notes |
|---|---|---|
| `joinUrl` | string | A `https://teams.microsoft.com/l/call/0/0?users=<comma-joined emails>` deep link. |

```json
{ "joinUrl": "https://teams.microsoft.com/l/call/0/0?users=bob%40corp.com%2Ccarol%40corp.com" }
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

| Reason | Code | When |
|---|---|---|
| — | `unauthenticated` | Requester account missing from the subject. |
| — | `bad_request` | `roomId` empty (subject malformed). |
| `not_room_member` | `forbidden` | Caller is not a member of the room. |
| `target_not_member` | `not_found` | No other callable members in the room. |
| `max_room_size_reached` | `conflict` | More than `ROOM_MEMBERS_CALL_LIMIT` (20) other members. |

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Start Teams User Call

Builds a Microsoft Teams 1:1 call deep link for a single target account. No Graph API call. The target email is derived as `accountName@TEAMS_EMAIL_DOMAIN`.

External client label: `POST /api/v1/calls/user`.

**Subject:** `chat.user.{account}.request.teams.{siteID}.call.user`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- The requester account is taken from the subject, not from a token.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `accountName` | string | yes | The target user's account. |

```json
{ "accountName": "bob" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `joinUrl` | string | A `https://teams.microsoft.com/l/call/0/0?users=<email>` deep link. |

```json
{ "joinUrl": "https://teams.microsoft.com/l/call/0/0?users=bob%40corp.com" }
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

| Reason | Code | When |
|---|---|---|
| — | `unauthenticated` | Requester account missing from the subject. |
| — | `bad_request` | `accountName` empty. |

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Start Teams Meeting

Creates a Microsoft Teams `onlineMeeting` via the Graph API and returns its join URL. **Idempotent per room, including under concurrency:** the meeting is created via Graph's `createOrGet` endpoint keyed on a stable per-room `externalId`, and a first-class `teams_meetings` record with a unique key on `(roomId, siteId)` guards local state. Repeated or concurrent calls for the same room return the same meeting and publish exactly one `teams_meet_started` system message. The organizer and attendees are resolved to their Azure AD object IDs via the app-only `User.Read.All` Service Principal that creates the meeting; the organizer object ID scopes Graph's `createOrGet` and attendees are added by object ID. An attendee that cannot be resolved is omitted; an unresolvable organizer fails the request.

> Graph client details (config env vars, app-only auth, the `createOrGet` idempotency key, the production application-access-policy requirement, and how to test without real credentials) are documented in [`docs/msgraph-client.md`](msgraph-client.md).

External client label: `POST /api/v1/meetings`.

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.teams.meeting`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's origin `siteID`.
- The requester account is taken from the subject, not from a token; it becomes the meeting organizer.

##### Request body

Empty body is accepted (the room is the subject's `{roomID}`).

| Field | Type | Required | Notes |
|---|---|---|---|
| `roomId` | string | no | Optional echo of the room; the authoritative room is the subject's `{roomID}`. |

##### Success response

| Field | Type | Notes |
|---|---|---|
| `id` | string | The Graph `onlineMeeting` ID. |
| `joinUrl` | string | The meeting's join web URL. |

```json
{ "id": "MSpkYzE3...", "joinUrl": "https://teams.microsoft.com/l/meetup-join/..." }
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

| Reason | Code | When |
|---|---|---|
| — | `unauthenticated` | Requester account missing from the subject. |
| — | `bad_request` | `roomId` empty (subject malformed). |
| `not_room_member` | `forbidden` | Caller is not a member of the room. |
| `max_room_size_reached` | `conflict` | Room has more than `ROOM_MEMBERS_LIMIT` (500) members. |
| — | `internal` | Teams meetings not configured (Azure app credentials unset), the organizer could not be resolved to an Azure object ID, or the Graph create failed. |

##### Triggered events — success path

On first creation, a `teams_meet_started` system message is published on the canonical message path (`chat.msg.canonical.{siteID}.created`), persisted by `message-worker`, and fanned out to room members like other system messages. Its `sysMsgData` carries `{ "meetingId": "...", "joinUrl": "..." }`.
On idempotent repeat calls that return cached meeting details, no additional system message is published.

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
| `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get.ids` | [Get Messages By IDs](#get-messages-by-ids) |
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
| `lastMsgAt` | number | Optional. Room's most-recent-message time, UTC ms. Supplying a valid value is what lets the server skip its MongoDB lookup. |
| `createdAt` | number | Optional. Room's creation time, UTC ms. A refinement only — narrows the scan's lower bound; its absence never forces a lookup. |

**What to pass for `meta`:** the server uses the room's `lastMsgAt` to pick the Cassandra time-bucket window to scan (and, when given, `createdAt` as the scan's lower bound). `meta` lets the client supply the values it already holds so the server can skip a MongoDB lookup:

- `meta.lastMsgAt` — the room's most-recent-message time, as the client knows it from the room summary (the same `lastMsgAt` carried on `RoomEvent`s / the sidebar). **A valid `lastMsgAt` alone is sufficient to skip the lookup.** Use the room's last-activity timestamp; for an empty room use its `createdAt`.
- `meta.createdAt` — the room's creation time from the room summary. Optional refinement: when present it floors the scan at the room's creation time instead of the server's default history window; when omitted the server still skips the lookup and simply uses that default floor.

Both are **hints, not authority**: the server sanitizes each (values that are negative or in the future are ignored) and falls back to a MongoDB fetch when `lastMsgAt` is missing or fails sanitization — or when both are supplied but mutually inconsistent (a `createdAt` later than `lastMsgAt`), which triggers a re-fetch to resolve the inconsistency. A client that does not have `lastMsgAt` should omit `meta` entirely — correctness is unaffected, only an extra lookup is incurred.

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
| `attachments` | [Attachment](#attachment)[] | Optional. Decoded attachment objects (history-service decodes the stored blobs on read). |
| `card` | [MessageCard](#messagecard) | Optional. |
| `cardAction` | [MessageCardAction](#messagecardaction) | Optional. |
| `tshow` | boolean | Optional. Whether a thread reply is also shown in the parent room. |
| `tcount` | number | Optional. Exact number of non-deleted replies on a thread parent. |
| `threadLastMsgAt` | string (ISO 8601) | Optional. Timestamp of the most recent reply in the thread. Absent if no replies or not a thread parent. |
| `threadParentId` | string | Optional. Set when this message is a thread reply. |
| `threadParentCreatedAt` | string | Optional. RFC 3339. |
| `quotedParentMessage` | [QuotedParentMessage](#quotedparentmessage) | Optional. Embedded snapshot of the quoted message. |
| `visibleTo` | string | Optional. Opaque client-set visibility marker (set on `msg.send`). Stored and surfaced verbatim; the backend never filters delivery, reads, or previews on it — the client interprets the scope. |
| `reactions` | map<emoji, [ReactionUser](#reactionuser)[]> | Optional. Omitted when absent; `{}` when present but empty. |
| `deleted` | boolean | Optional. `true` for tombstoned messages. |
| `type` | string | Optional. System-message type when set; regular messages omit it. Known values: `"room_created"`, `"members_added"`, `"member_removed"`, `"member_left"`, `"room_renamed"`. For all five, `msg` is populated with a server-rendered human-readable body and `sender.account` is the responsible actor (the requester for adds/removes-by-other / room-creates / renames, the leaving user for self-leave). `"room_restricted"` also appears on historical messages: it is no longer produced — a restriction change emits a [room event](client-api/events.md#room_restricted-roomrestrictedroomevent) instead — but rows written before that change remain readable. |
| `sysMsgData` | string | Optional. Base64-encoded JSON payload for system messages; shape depends on `type` (see [System-message `sysMsgData` payloads](#system-message-sysmsgdata-payloads)). |
| `siteId` | string | Optional. The site that owns the message. |
| `editedAt` | string | Optional. RFC 3339. Set after an edit. |
| `updatedAt` | string | Optional. RFC 3339. Mirrors `editedAt` for edits, set on delete to record the deletion time. |
| `threadRoomId` | string | Optional. The thread room ID when this is a thread message. |
| `pinnedAt` | string | Optional. RFC 3339. With the `messages_by_room` `pinned_at` mirror, room-timeline history loads now return this on pinned rows too (previously only `pin.list` and point lookups carried it). |
| `pinnedBy` | [MessageParticipant](#messageparticipant) | Optional. |
| `truncated` | boolean | Optional. `true` when the server blanked this row to make the page fit — either because the row alone exceeded the transport's `max_payload`, or because it shares a `createdAt` millisecond with such a row and the whole group had to be returned together. `msg`, `mentions`, `attachments`, `card`, `cardAction`, `quotedParentMessage`, `reactions`, `sysMsgData`, `encPayload` and `encMeta` are cleared; identifiers, `sender`, `createdAt` and `type` are retained so a placeholder can be rendered. Absent on every ordinary row. |

##### System-message `sysMsgData` payloads

`sysMsgData` is base64-encoded JSON whose shape depends on `type`.

`members_added` (also emitted on room creation):

| field | type | description |
|-------|------|-------------|
| `individuals` | string[] | Accounts of the individuals in the request (direct + channel-expanded, deduped; excludes organization members and the requester). May include accounts already in the room. Empty `[]`, never `null`. The "n people" count is `individuals.length`; clients may render it as a clickable list. |
| `orgs` | string[] | Organization IDs in the request (direct + channel-expanded, deduped). Empty `[]`, never `null`. The "m organizations" count is `orgs.length`; clients may render it as a clickable list. |
| `channels` | [ChannelRef](#channelref)[] | Source channels whose members were copied in (provenance). Empty `[]`, never `null`. |
| `addedUsersCount` | number | New subscriptions created by the operation, including organization-expanded members; may differ from `individuals.length`. |

```json
{ "individuals": ["alice", "bob"], "orgs": ["eng"], "channels": [], "addedUsersCount": 12 }
```

`member_removed`:

| field | type | description |
|-------|------|-------------|
| `user` | [SysMsgUser](#sysmsguser) | Set when an individual was removed. |
| `orgId` | string | Set when an organization was removed. |
| `sectName` | string | Display name of the removed organization (set with `orgId`). |
| `removedUsersCount` | number | Number of underlying accounts whose subscription was removed. |

```json
{ "user": { "account": "bob", "engName": "Bob", "chineseName": "鮑勃" }, "removedUsersCount": 1 }
```

`member_left` — `{ "user": SysMsgUser }` for the user who left:

```json
{ "user": { "account": "bob", "engName": "Bob", "chineseName": "鮑勃" } }
```

##### SysMsgUser

A user referenced by a system-message payload.

| field | type | description |
|-------|------|-------------|
| `account` | string | The user's account. |
| `engName` | string | English display name; may be empty. |
| `chineseName` | string | Chinese display name; may be empty. |

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
| `botUsername` | string | Optional. Username of the bot the action targets — the client sets it on a tcard_execute event so the server can route the callback. |

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
| `attachments` | [Attachment](#attachment)[] | Optional. Decoded attachment objects. |
| `messageLink` | string | Optional. |
| `threadParentId` | string | Optional. Set if the quoted message itself is a thread reply. |
| `threadParentCreatedAt` | string | Optional. RFC 3339. |
| `tshow` | boolean | Optional. Mirrors the quoted message's own `tshow` — set when the quoted message is a thread reply also shown in its parent channel room. |

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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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
| `hasNext` | boolean | `true` if older messages may exist beyond this page — fetch the next page with `before` = the oldest returned message's `createdAt`. `false` once the caller's history boundary (room start, history floor, or access window) is reached. Conservative: occasionally `true` when nothing older remains; the following fetch then returns an empty page with `hasNext=false`. An empty page always has `hasNext=false`. |
| `minUserLastSeenAt` | number | Optional. UTC milliseconds since Unix epoch. The room's **strict read floor** — `MIN(lastSeenAt)` across all subscribers, present **only when every member has read** the room. Omitted (the key is absent, never `null`) when any member has not read yet (so botDM rooms, where the bot never reads, never set it), when the most recent read is already past `room.lastMsgAt` (recompute is skipped), or when the value cannot be retrieved (best-effort; messages still load). See the Message Read RPC for how this floor is recomputed. |
| `sizeLimited` | boolean | Optional. `true` when rows were dropped to keep the reply inside the transport's `max_payload`. A short page alone does not mean this — the history walk returns one too — so branch on this flag, never on `len(messages) < limit`. Absent when nothing was dropped, including when a row was merely blanked (`truncated`). See **A page may be shorter than `limit`** for how to pick the next `limit`. |

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
  "hasNext": true,
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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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
| `minUserLastSeenAt` | number | Optional. UTC milliseconds since Unix epoch. The room's **strict read floor** — `MIN(lastSeenAt)` across all subscribers, present **only when every member has read** the room. Omitted (the key is absent, never `null`) when any member has not read yet (so botDM rooms, where the bot never reads, never set it), when the most recent read is already past `room.lastMsgAt` (recompute is skipped), or when the value cannot be retrieved (best-effort; messages still load). See the Message Read RPC for how this floor is recomputed. |

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
  "hasNext": true,
  "minUserLastSeenAt": 1746518100000
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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

Fetches messages around a pivot — useful for "jump to this message" navigation and "resume at
last-read position". The pivot is **exactly one of** `messageId` or `timestamp`. Returns up to
`limit` messages total.

- **`messageId` mode** — the window is centered on that message, which is **included** in the
  middle of the result; `limit` covers the central message plus the context around it.
- **`timestamp` mode** — the pivot is a UTC-millis coordinate with **no central message**. The
  result is the messages **at or before** the pivot (`createdAt <= timestamp`) followed by the
  messages **after** it (`createdAt > timestamp`), split evenly (before gets the larger half on an
  odd `limit`). A message whose `createdAt` equals `timestamp` exactly is included as the newest of
  the "at or before" group (e.g. anchoring a subscription `lastSeenAt`).

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `messageId` | string | conditional | The central message to center the window on. Provide **exactly one** of `messageId` / `timestamp`. |
| `timestamp` | number | conditional | UTC milliseconds since Unix epoch. Pivot coordinate; must be `> 0`. Provide **exactly one** of `messageId` / `timestamp`. |
| `limit` | number | no | Window size (includes the central message in `messageId` mode) — see [Common request fields](#common-request-fields-read-rpcs) (default 50, max 100). |

```json
{
  "messageId": "01970a4f8c2d7c9aQRST",
  "limit": 50
}
```

```json
{
  "timestamp": 1746518100000,
  "limit": 50
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | array<Message> | Window of messages, oldest-first. In `messageId` mode, centered on `messageId`; in `timestamp` mode, the "at or before" group followed by the "after" group with no distinct central message. See [Message schema](#message-schema). |
| `moreBefore` | boolean | `true` if more messages exist before the window. |
| `moreAfter` | boolean | `true` if more messages exist after the window. |
| `minUserLastSeenAt` | number | Optional. UTC milliseconds since Unix epoch. The room's **strict read floor** — `MIN(lastSeenAt)` across all subscribers, present **only when every member has read** the room. Omitted (the key is absent, never `null`) when any member has not read yet (so botDM rooms, where the bot never reads, never set it), when the most recent read is already past `room.lastMsgAt` (recompute is skipped), or when the value cannot be retrieved (best-effort; messages still load). See the Message Read RPC for how this floor is recomputed. |
| `sizeLimited` | boolean | Optional. `true` when the window was narrowed to keep the reply inside the transport's `max_payload`. Branch on this, never on the window being shorter than `limit`. Absent when nothing was dropped, including when a row was merely blanked (`truncated`). See **A page may be shorter than `limit`** for how to pick the next `limit`. |

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
  "moreAfter": false,
  "minUserLastSeenAt": 1746518100000
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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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

#### Get Messages By IDs

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get.ids`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.
- All requested IDs must belong to the same room (the room is identified by `{roomID}` in the subject).

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `messageIds` | string[] | yes | IDs of the messages to fetch. Must be non-empty; maximum 100. |

```json
{ "messageIds": ["01970a4f8c2d7c9aQRST", "01970a4f8c2d7c9aABCD"] }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `messages` | [Message](#message-schema)[] | Results in the same order as the input `messageIds`. IDs not found in the store or outside the caller's access window are silently omitted. |

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
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

| Condition | `code` | `error` |
|---|---|---|
| Empty `messageIds` | `bad_request` | `messageIds must not be empty` |
| `messageIds` length exceeds 100 | `bad_request` | `too many messageIds` |
| Caller not subscribed to the room | `forbidden` | `not subscribed to room` |
| Store failure | `internal` | `internal error` |

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Edit Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.edit`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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
| `eventTimestamp` | number | Milliseconds since Unix epoch (UTC). When message-worker published the canonical event. Omitted for legacy events. |
| `messageId` | string | The edited message's ID. |
| `newContent` | string | Optional. New plaintext content. Present for DMs and unencrypted channels. Omitted for encrypted channels — see `encryptedNewContent`. |
| `encryptedNewContent` | [EncryptedMessage](#encryptedmessage) | Optional. For encrypted channel rooms. Omitted otherwise. |
| `mentions` | [Participant](#participant)[] | Optional. `@`-mentions resolved from the edited content, so an edit that adds a mention renders like a fresh message. Omitted when none. |
| `mentionAll` | boolean | Optional. `true` if the edited content mentions `@all` (or `@here`), so an edit that adds or removes `@all` conveys it like a fresh message. Omitted when `false`. |
| `editedBy` | string | The sender's account. |
| `editedAt` | string | RFC 3339 timestamp. Domain time of the edit. |
| `updatedAt` | string | RFC 3339 timestamp. |
| `threadParentMessageId` | string | Optional. Set when the edited message is a thread reply — its presence lets the client tell a thread-reply edit from a top-level one. Omitted for top-level messages. |
| `tshow` | boolean | Optional. For a thread reply, whether it is also shown in the main room timeline. Omitted when `false`. |
| `previewMessage` | [PreviewMessage](#previewmessage) | Optional. The room's current preview after this edit (same resolution as `subscription.list`; `content` carries the 500-rune snippet, which list rows truncate further). **Omitted** for hidden thread-reply edits (`threadParentMessageId` set with `tshow` not true — not shown in the room timeline), or when the recompute could not complete. An edit never empties a room, so unlike `message_deleted` an omission here never means "no eligible message left". See [Reacting to a preview change](#reacting-to-a-preview-change). |

```json
{
  "type": "message_edited",
  "roomId": "01970a4f8c2d7c9aQ",
  "siteId": "siteA",
  "timestamp": 1746518700123,
  "eventTimestamp": 1746518700100,
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

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Delete Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.delete`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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
- **Thread reply (TShow=false) in a channel** — `chat.user.{recipient}.event.room` — published once per thread subscriber (followers + @-mentioned accounts). Non-subscribers do not receive this event. Also published on `chat.room.{roomID}.thread.{parentMessageID}.event` for clients with the thread panel open — see [§4.2 Thread View Subject](#42-thread-view-subject).
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
| `threadParentMessageId` | string | Optional. Set when the deleted message is a thread reply — its presence lets the client tell a thread-reply delete from a top-level one. Omitted for top-level messages. |
| `tshow` | boolean | Optional. For a thread reply, whether it is also shown in the main room timeline. Omitted when `false`. |
| `previewMessage` | [PreviewMessage](#previewmessage) | Optional. The room's current preview after this delete (same resolution as `subscription.list`; `content` carries the 500-rune snippet, which list rows truncate further). **Omitted** for hidden thread-reply deletes (`threadParentMessageId` set with `tshow` not true — not shown in the room timeline), when the room has no eligible message left (e.g. the deleted message was the last one), or when the recompute could not complete. See [Reacting to a preview change](#reacting-to-a-preview-change). |

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

When the deleted message was the room's last eligible message, `previewMessage` is **omitted** entirely. An omission is not "leave the preview as it is" — if the deleted `messageId` matches the preview being displayed, the client must clear it, or the deleted content stays on the room row until the next read. See [Reacting to a preview change](#reacting-to-a-preview-change).

**Thread-reply deletes additionally emit a `ThreadMetadataUpdatedEvent`** (see [§4.1 Thread Metadata Event](#41-thread-metadata-event)) to update the parent message's reply-count badge. The `DeleteRoomEvent` and `ThreadMetadataUpdatedEvent` are published independently; clients must handle each on its own.

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Pin Message

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.pin`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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
**DM / BotDM rooms:** `chat.user.{account}.event.room` — `PinStateRoomEvent`. Recipients: each non-bot DM participant.

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

On success, the service publishes a `MessageEvent` to **`chat.msg.canonical.{siteID}.pinned`** (JetStream, `MESSAGES-CANONICAL-{siteID}` stream). This is an internal canonical subject consumed by backend workers (broadcast-worker, search-sync-worker, etc.) and is **not** part of any client subscription pattern. Documented here only so backend service authors know the payload shape. Not published when the request hits an already-pinned message (idempotent short-circuit) or a soft-deleted message (the handler returns `not_found` before publishing).

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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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
**DM / BotDM rooms:** `chat.user.{account}.event.room` — `PinStateRoomEvent`. Recipients: each non-bot DM participant.

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

On success, the service publishes a `MessageEvent` to **`chat.msg.canonical.{siteID}.unpinned`** (JetStream, `MESSAGES-CANONICAL-{siteID}` stream). This is an internal canonical subject consumed by backend workers and is **not** part of any client subscription pattern. Documented here only so backend service authors know the payload shape. Not published when the request hits an already-unpinned message.

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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Returns pinned messages in a room, ordered most-recently-pinned first. Only subscription access is required — the global pin kill-switch and the large-room override do **not** apply to listing (existing pins remain listable even when new pinning is disabled).

The response is cursor-paginated (`cursor`/`limit` in the request, `nextCursor`/`hasNext` in the response). Because pins are capped at `MAX_PINNED_PER_ROOM` (default 10), most callers will see `hasNext=false` on the first page.

**Access window — redacted stubs:** If the caller's subscription has a `historySharedSince` lower bound (partial history access), pins whose underlying message was created before that timestamp are returned as **redacted stubs**. A thread reply is stubbed on a second condition too: it was sent to the thread only (`tshow=false`) and its thread parent predates the boundary, or that parent time was never recorded. A reply that was also sent to the channel (`tshow=true`) is judged on its own `createdAt` alone, so it stays readable exactly when the channel timeline shows it. The following fields are cleared: `msg` (replaced with `"This message is unavailable"`), `mentions`, `attachments`, `card`, `cardAction`, `quotedParentMessage`, `reactions`, `type`, `sysMsgData`. **All other Message fields remain populated** — identifiers, `sender`, `createdAt`, `pinnedAt`, `pinnedBy`, plus any thread/edit metadata — so the frontend can render a placeholder in place. **The row count is the same for every caller.** A quoted parent inside a still-visible pin is redacted by the same mechanism as elsewhere (see Load History).

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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Toggles a reaction on a message. Any subscribed room member may react — the server decides add vs remove by checking whether the calling user is already in the message's reactor map for that shortcode. Reactions can always be _removed_ from a soft-deleted message (so users can clean up after a delete), but cannot be _added_ to one.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `messageId` | string | yes | The message to react to. |
| `shortcode` | string | yes | The reaction emoji — **any non-empty, visible textual or Unicode key** (e.g. a bare shortcode `thumbsup`, a colon-wrapped `:thumbsup:`, ASCII punctuation like `_`/`-`, a raw-unicode emoji `👍` / ZWJ sequence / flag, or a private-use glyph). The server applies **no support check**: it NFC-normalises the value and rejects only (a) a value over 64 bytes (a resource guard, ~20× any real emoji) or (b) a value with **no visible character** — empty, or made only of whitespace / control / zero-width / combining-mark code points. It does **not** check the character set, the standard-emoji set, or the site's registered custom emoji. The **FE decides renderability**. Note: reactions are stored as **opaque keys with no shortcode↔unicode aliasing**, so `thumbsup` and `👍` are two distinct reactions — the FE should send one consistent representation per emoji. Clients typically offer shortcodes from their picker (built-in standard set + the local site's [`emoji.list`](#emojilist--list-a-sites-custom-emoji)), but any emoji is accepted (e.g. a migrated one the picker doesn't list). |

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

See [Error envelope](#6-error-envelope-reference). Common errors: `"messageId is required"`, `"shortcode is required"`, `"reaction emoji too large"` (over 64 bytes post-NFC), `"reaction emoji is required"` (no visible character — whitespace / control / zero-width / combining-mark only; an *empty* shortcode returns `"shortcode is required"` instead, checked before canonicalization), `"message not found"` (also returned when attempting to _add_ a reaction to a soft-deleted message), `"not subscribed to room"`, `"failed to add reaction"`, `"failed to remove reaction"`.

##### Triggered events — success path

**`chat.room.{roomID}.event`** — `ReactRoomEvent`. Recipients: every client subscribed to the room (channel rooms); for DMs, each non-bot member receives the event on `chat.user.{account}.event.room`.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_reacted"`. |
| `roomId` | string | |
| `siteId` | string | |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `eventTimestamp` | number | Milliseconds since Unix epoch (UTC). When message-worker published the canonical event. Omitted for legacy events. |
| `messageId` | string | The reacted-to message's ID. |
| `shortcode` | string | The canonical NFC-normalised reaction key — a textual shortcode (`thumbsup`) or a raw-unicode emoji (`👍`); opaque, not format-validated. |
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
  "eventTimestamp": 1746518900100,
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
| `roomType` | string | Room type: `"channel"`, `"dm"`, or `"botDM"`. |
| `message` | [Message](#message-schema) | The full reacted-to message (same shape as history reads — `omitempty` fields like `tshow`/`threadParentMessageId` are absent, not `false`/`""`, when unset). |
| `reactionDelta` | [ReactionDelta](#reactiondelta) | The single-reaction delta that triggered the notification. |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |

###### ReactionDelta

| Field | Type | Notes |
|---|---|---|
| `shortcode` | string | The canonical NFC-normalised reaction key reacted with — a textual shortcode or a raw-unicode emoji; opaque, not format-validated. |
| `action` | string | Always `"added"` here (the notification only fires on add). |
| `actor` | [Participant](#participant) | The user who reacted. `displayName` is populated (`CombineWithFallback(engName, chineseName, account)`); for a bot account (`.bot` suffix) it's the app's display name instead, falling back to the composed name if no app matches. |

To reconcile this delta with the grouped per-message `reactions` map returned by history endpoints (`map<emoji, [{account, displayName}]>`), clients append or remove one entry under `reactions[shortcode]` keyed on `actor.account`. See the "Message schema" section for the history-side shape.

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Thread Messages

**Subject:** `chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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
| `messages` | array<[Message](#message-schema)> | Replies in the thread, oldest-first within the page. |
| `nextCursor` | string | Optional. Opaque cursor for the next page. |
| `hasNext` | boolean | `true` if more replies exist beyond this page. |
| `parentMessage` | [Message](#message-schema) | Optional. The thread-parent message. Present whenever the thread parent passes the access-window check. Absent only on error paths. The parent's `quotedParentMessage` is access-window-redacted by the same rules as replies. |
| `minUserLastSeenAt` | number | Optional. UTC milliseconds since Unix epoch. The thread room's **strict read floor** — `MIN(lastSeenAt)` across all thread subscribers, present **only when every subscriber has read**. Absent when any subscriber has not yet read (including bots, which never call Mark Thread as Read), or when the value cannot be retrieved (best-effort; thread messages still load). |

```json
{
  "parentMessage": {
    "roomId": "01970a4f8c2d7c9aQ",
    "createdAt": "2026-05-06T07:55:00Z",
    "messageId": "01970a4f8c2d7c9aQRST",
    "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" },
    "msg": "anyone have thoughts on the new design?",
    "tcount": 1
  },
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
  "hasNext": false,
  "minUserLastSeenAt": 1746518100000
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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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
| `chat.user.{account}.request.search.{siteID}.orgs` | [Search Orgs](#search-orgs) |

#### `search.messages` — full-text message search

> [!WARNING]
> **Breaking change (v2):** The response shape has changed from `{total, results}` to `{messages, total}`. The `results` field no longer exists. The per-hit type is now `SearchMessage` (an enriched projection) instead of the former `MessageSearchHit`. Update all clients before deploying this version.

**Subject:** `chat.user.{account}.request.search.{siteID}.messages`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

**Auth:** the `{account}` in the subject is the authenticated identity. `{siteID}` is the requester's home site — the supercluster routes the request to the search-service running on that site. The search is automatically scoped to rooms the user is a member of — results never include messages from rooms the user cannot access.

**Cross-site results:** results may include messages from rooms hosted on remote sites (the index is searched across the local and remote clusters). The `siteId` on each `SearchMessage` identifies the originating site; there is no client opt-in/opt-out.

**Searched fields:** one query matches message text (`content`), attachment text (`attachmentText` — every attachment's file name and description pooled into one field), and tcard data (`cardData`, the card's data document indexed verbatim as text). All terms of the query must match within a single one of these fields (`multi_match` with `AND`) — a query mixing a word from the message text with a word from a filename matches neither field and returns no hit; a query mixing a filename word with a description word DOES match, since both live in `attachmentText`.

**System messages are never returned.** Server-generated room chrome (`type` of `room_created`, `members_added`, `member_removed`, `member_left`, `room_renamed`, `room_restricted`, `teams_meet_started` — see [`Message.type`](#message-schema)) is excluded from the search index, so it can never appear in `messages`. A client-set `type: "important"` message is normal user content and remains searchable.

**Teams-migrated messages** are excluded when the server's `SHOW_TEAMS_ROOM` env is `false` (the default); included when `true`, **or** when the requesting account is listed in `SHOW_TEAMS_ROOM_ACCOUNTS` (a comma-separated per-account allowlist). Reversible read-time filter on the indexed `origin` field, no data change.

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
      "threadParentMessageCreatedAt": "2026-04-01T11:58:00Z",
      "attachments": [
        {
          "id": "file-abc",
          "title": "q3-report.pdf",
          "type": "file",
          "description": "Quarterly numbers",
          "titleLink": "api/v1/file/rooms/r1/file/file-abc?drive_host=https://drive.example.com",
          "titleLinkDownload": true,
          "fileType": "application/pdf"
        }
      ],
      "card": {
        "template": "expense-approval-v1",
        "data": "eyJhbW91bnQiOjQyfQ=="
      },
      "tshow": true,
      "sender": {
        "account": "alice",
        "hr": { "account": "alice", "chineseName": "王愛麗", "engName": "Alice Wong" }
      },
      "room": {
        "id": "r1",
        "name": "Bob Chan",
        "type": "dm",
        "hrInfo": { "account": "bob", "chineseName": "陳大文", "engName": "Bob Chan" }
      }
    }
  ],
  "total": 42
}
```

| Field | Type | Notes |
|---|---|---|
| `messages` | SearchMessage[] | Per-hit projection. Always an array (empty `[]` when no results). |
| `total` | integer | Total matching hits (may exceed `messages.length` when paginating). |

**`SearchMessage` fields** (base fields from the ES message index; `room`/`sender` resolved server-side):

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
| `attachments` | [Attachment](#attachment)[] | omitted when the message has no attachments |
| `card` | [MessageCard](#messagecard) | omitted when the message carries no tcard |
| `tshow` | boolean | omitted when false — set on a thread reply that is also shown in the parent channel timeline |
| `sender` | [MessageSender](#messagesender) | present on every hit; `account` always set, `hr`/`appInfo` best-effort |
| `room` | [MessageRoom](#messageroom) | present on every hit; `id` always set, `name`/`type`/`hrInfo`/`appInfo` best-effort |

`attachments` and `card` are the message's payloads mirrored as-is from the index (same wire shape as history reads — decoded `Attachment` objects; `card.data` is base64-encoded bytes), so the client can render a hit (file row, tcard) without a follow-up history-service load.

`room` and `sender` are resolved server-side and are **best-effort**: individual fields are omitted when they cannot be resolved (e.g. the caller has no subscription for the room, or a federated channel's origin site is unreachable). The base message fields are always present.

##### MessageSender

| Field | Type | Notes |
|---|---|---|
| `account` | string | sender's account |
| `hr` | [MessageHRInfo](#messagehrinfo) | human senders only; omitted when the users lookup missed |
| `appInfo` | [MessageAppInfo](#messageappinfo) | bot senders only (`isSubscribed` never set here); omitted when the apps lookup missed |

##### MessageRoom

| Field | Type | Notes |
|---|---|---|
| `id` | string | roomId |
| `name` | string | app name (`botDM`) / counterpart display name (`dm`) / canonical room name (`channel`, `discussion`). Omitted when unresolved. |
| `type` | string | `channel` \| `dm` \| `botDM` \| `discussion`. Omitted when the caller has no subscription for the room. |
| `hrInfo` | [MessageHRInfo](#messagehrinfo) | present **only for `dm` rooms** |
| `appInfo` | [MessageAppInfo](#messageappinfo) | present **only for `botDM` rooms**; `isSubscribed` always set here |

##### MessageHRInfo

| Field | Type | Notes |
|---|---|---|
| `account` | string | HR-directory account |
| `chineseName` | string | omitted when empty |
| `engName` | string | omitted when empty |

##### MessageAppInfo

| Field | Type | Notes |
|---|---|---|
| `id` | string | app document id |
| `name` | string | app display name |
| `assistantName` | string | bot account (`assistant.name`) |
| `isSubscribed` | boolean | `room.appInfo` only — the caller's subscription state for the bot (explicit `true`/`false`); absent on `sender.appInfo` |

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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

`{siteID}` is the requester's home site; the supercluster routes the request to that site's search-service. Full-text search across rooms the requester is subscribed to. Results are served directly from the spotlight ES index (one document per `(account, room)` pair), in ES relevance order.

**Teams-migrated rooms** are excluded when the server's `SHOW_TEAMS_ROOM` env is `false` (the default); included when `true`, **or** when the requesting account is listed in `SHOW_TEAMS_ROOM_ACCOUNTS` (a comma-separated per-account allowlist). Reversible read-time filter on the indexed `origin` field, no data change.

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
| `appViewUrl` | map<string, string> | Optional. App-view URLs keyed by view name (e.g. `{"main": "..."}`). |
| `reportUrl` | string | Optional. App report URL. |
| `forumUrl` | string | Optional. App forum URL. |
| `userManualUrl` | string | Optional. App user-manual URL. |
| `version` | string | Optional. App version. |
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
| `default` | string | Canonical URL template with literal `${roomId}`, `${siteId}`, `${roomType}`, `${roomOrigin}` placeholders. See the Get Room App Tabs `tabUrl` row for substitution semantics. |

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
      "assistant": { "enabled": true, "name": "weather.bot", "settingsUrl": "https://site-a.example.com/apps/weather/settings" },
      "channelTab": { "enabled": true, "default": false, "name": "Weather", "url": { "default": "https://site-a.example.com/apps/weather?room=${roomId}&site=${siteId}&roomType=${roomType}&roomOrigin=${roomOrigin}" } },
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
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

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

#### Search Orgs

**Subject:** `chat.user.{account}.request.search.{siteID}.orgs`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

`{siteID}` is the requester's home site; the supercluster routes the request to that site's search-service. Prefix search across the organization directory (sections and departments), served directly from the local spotlight-org ES index (one document per section, keyed by `sectId`, maintained by `search-sync-worker` from HR employee events). The directory is **company-wide**: results are the same for every caller. The `{account}` in the subject is the authenticated identity (enforced by the NATS auth callout) and is used for logging/metrics only — it does **not** scope the result set.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | **yes** | Case-insensitive prefix match on the organization fields (`sectName`, `sectTCName`, `deptName`, `deptTCName`, `divisionId`). Whitespace-only is rejected. |
| `size` | integer | no | Page size. Default `25`, capped at `100`. |
| `offset` | integer | no | Page offset. Default `0`. |

```json
{
  "query": "engineering",
  "size": 25,
  "offset": 0
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `orgs` | [SearchOrg](#searchorg)[] | Page of org results, in ES relevance order. Empty array when no matches, never null. |

###### SearchOrg

Projection of the spotlight-org ES document. Optional fields are omitted when the source document is partial.

| Field | Type | Notes |
|---|---|---|
| `sectId` | string | Section ID (the document key). |
| `sectName` | string | Optional. Section name. |
| `sectTCName` | string | Optional. Section name (Traditional Chinese). |
| `sectDescription` | string | Optional. Section description. |
| `deptId` | string | Optional. Department ID. |
| `deptName` | string | Optional. Department name. |
| `deptTCName` | string | Optional. Department name (Traditional Chinese). |
| `deptDescription` | string | Optional. Department description. |
| `divisionId` | string | Optional. Division ID. |

```json
{
  "orgs": [
    {
      "sectId": "S1",
      "sectName": "engineering-platform",
      "sectTCName": "平台",
      "deptId": "D1",
      "deptName": "technology",
      "deptTCName": "科技",
      "divisionId": "DIV1"
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

| Code | Reason |
|---|---|
| `bad_request` | `query` is missing, empty, or whitespace-only; or `size`/`offset` is negative. |
| `internal` | Elasticsearch backend failure (transient or permanent). The raw error is never leaked to the client. |

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

### 3.4 user-service

`user-service` exposes 19 NATS request/reply endpoints over **core NATS** (no JetStream consumers). Subjects follow the pattern `chat.user.{account}.request.user.{siteID}.<area>.<action>`, except `me`, which is a single-token self-lookup (`chat.user.{account}.request.user.{siteID}.me`).

> **Events:** [`settings.set`](#settingsset), [`settings.priorityContacts.add`](#settingsprioritycontactsadd), and [`settings.priorityContacts.remove`](#settingsprioritycontactsremove) each emit [`settings.update`](#settingsupdate-event) to the caller's other devices; [`settings.priorityContacts.get`](#settingsprioritycontactsget) is a pure read and emits nothing. No other endpoint emits a client-facing event. (`status.set` and the three settings-mutating endpoints above also trigger a server-side cross-site federation update, which is not delivered to clients.)

| RPC subject | Method |
|---|---|
| `chat.user.{account}.request.user.{siteID}.me` | [`me`](#me) |
| `chat.user.{account}.request.user.{siteID}.status.getByName` | [`status.getByName`](#statusgetbyname) |
| `chat.user.{account}.request.user.{siteID}.profile.getByName` | [`profile.getByName`](#profilegetbyname) |
| `chat.user.{account}.request.user.{siteID}.status.set` | [`status.set`](#statusset) |
| `chat.user.{account}.request.user.{siteID}.settings.get` | [`settings.get`](#settingsget) |
| `chat.user.{account}.request.user.{siteID}.settings.set` | [`settings.set`](#settingsset) |
| `chat.user.{account}.request.user.{siteID}.settings.priorityContacts.get` | [`settings.priorityContacts.get`](#settingsprioritycontactsget) |
| `chat.user.{account}.request.user.{siteID}.settings.priorityContacts.add` | [`settings.priorityContacts.add`](#settingsprioritycontactsadd) |
| `chat.user.{account}.request.user.{siteID}.settings.priorityContacts.remove` | [`settings.priorityContacts.remove`](#settingsprioritycontactsremove) |
| `chat.user.{account}.request.user.{siteID}.chatlist.get` | [`chatlist.get`](#chatlist-sections) |
| `chat.user.{account}.request.user.{siteID}.chatlist.section.create` | [`chatlist.section.create`](#chatlist-sections) |
| `chat.user.{account}.request.user.{siteID}.chatlist.section.rename` | [`chatlist.section.rename`](#chatlist-sections) |
| `chat.user.{account}.request.user.{siteID}.chatlist.section.delete` | [`chatlist.section.delete`](#chatlist-sections) |
| `chat.user.{account}.request.user.{siteID}.chatlist.section.reorder` | [`chatlist.section.reorder`](#chatlist-sections) |
| `chat.user.{account}.request.user.{siteID}.chatlist.section.setsortmode` | [`chatlist.section.setSortMode`](#chatlist-sections) |
| `chat.user.{account}.request.user.{siteID}.subscription.list` | [`subscription.list`](#subscriptionlist) |
| `chat.user.{account}.request.user.{siteID}.subscription.getChannels` | [`subscription.getChannels`](#subscriptiongetchannels) |
| `chat.user.{account}.request.user.{siteID}.subscription.getDM` | [`subscription.getDM`](#subscriptiongetdm) |
| `chat.user.{account}.request.user.{siteID}.subscription.getByRoomID` | [`subscription.getByRoomID`](#subscriptiongetbyroomid) |
| `chat.user.{account}.request.user.{siteID}.subscription.count` | [`subscription.count`](#subscriptioncount) |
| `chat.user.{account}.request.user.{siteID}.subscription.setAppSubscription` | [`subscription.setAppSubscription`](#subscriptionsetappsubscription) |
| `chat.user.{account}.request.user.{siteID}.apps.list` | [`apps.list`](#appslist) |
| `chat.user.{account}.request.user.{siteID}.apps.categories` | [`apps.categories`](#appscategories) |
| `chat.user.{account}.request.user.{siteID}.thread.list` | [List User Threads](#list-user-threads) |
| `chat.user.{account}.request.user.{siteID}.thread.unread.summary` | [Get Thread Unread Summary](#get-thread-unread-summary) |
| `chat.user.{account}.request.user.{siteID}.thread.read.all` | [Clear All Thread Unread](#clear-all-thread-unread) |
| `chat.user.{account}.request.user.{siteID}.sso.set` | [`sso.set`](#ssoset) |
| `chat.user.{account}.request.user.{siteID}.sso.refresh` | [`sso.refresh`](#ssorefresh) |

#### me

**Subject:** `chat.user.{account}.request.user.{siteID}.me`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Returns the **calling** user's own status view plus their effective presence. The
target is the `{account}` in the subject (the requester) — there is no request
body. Presence is resolved from `user-presence-service`; if that lookup fails,
`presence` degrades to `offline` (best-effort display data) rather than failing
the request.

##### Request body

None. Any payload is ignored.

##### Success response

| Field          | Type    | Notes |
|----------------|---------|-------|
| `account`      | string  | The calling user's account. |
| `statusText`   | string  | Current status message (empty if not set). |
| `statusIsShow` | boolean | Always present. Whether the status is displayed; `false` when never set. |
| `chineseName`  | string  | Optional — **omitted** when the user record has no Chinese name (never sent as an empty string). |
| `engName`      | string  | Optional — **omitted** when the user record has no English name (never sent as an empty string). |
| `presence`     | string  | Effective presence: one of `online`, `away`, `busy`, `offline`, `in-call`. `offline` when unknown or on a degraded presence lookup. |

```json
{
  "account": "alice",
  "statusText": "In a meeting",
  "statusIsShow": true,
  "chineseName": "愛麗絲",
  "engName": "Alice",
  "presence": "online"
}
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| User not found | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` |
| Internal failure | `internal` | — | The user-status read failed — the **only** source of `internal` on this endpoint. A failed presence lookup never errors; it degrades to `presence: "offline"` in a success response. |

---

#### status.getByName

**Subject:** `chat.user.{account}.request.user.{siteID}.status.getByName`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Fetches the status and display-name fields for a named user. The caller's `{account}` in the subject is the requester; the target user is identified by the request body.

##### Request body

| Field  | Type   | Required | Notes |
|--------|--------|----------|-------|
| `name` | string | yes      | Account name of the user whose status to fetch. Must be non-empty. |

```json
{ "name": "alice" }
```

##### Success response

| Field          | Type    | Notes |
|----------------|---------|-------|
| `account`      | string  | The user's account. |
| `statusText`   | string  | Current status message (empty if not set). |
| `statusIsShow` | boolean | Always present. Whether the status is displayed; `false` when never set. |
| `chineseName`  | string  | Optional. Display name in Chinese. |
| `engName`      | string  | Optional. English display name. |

```json
{
  "account": "alice",
  "statusText": "In a meeting",
  "statusIsShow": true,
  "chineseName": "愛麗絲",
  "engName": "Alice"
}
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| `name` missing or empty | `bad_request` | — | `{ "code": "bad_request", "error": "name required" }` — rejected before any lookup. |
| User not found | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` |
| Internal failure | `internal` | — | — |

---

#### profile.getByName

**Subject:** `chat.user.{account}.request.user.{siteID}.profile.getByName`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

The profile lookup for a named user. **Identical to [status.getByName](#statusgetbyname) by design** — same request body, same response fields, same error cases; it queries the same users collection. It is exposed as a separate subject.

##### Request body

| Field  | Type   | Required | Notes |
|--------|--------|----------|-------|
| `name` | string | yes      | Account name of the user whose profile to fetch. Must be non-empty. |

```json
{ "name": "alice" }
```

##### Success response

Same shape as `status.getByName`:

```json
{
  "account": "alice",
  "statusText": "In a meeting",
  "statusIsShow": true,
  "chineseName": "愛麗絲",
  "engName": "Alice"
}
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| `name` missing or empty | `bad_request` | — | `{ "code": "bad_request", "error": "name required" }` — rejected before any lookup. |
| User not found | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` |
| Internal failure | `internal` | — | — |

---

#### status.set

**Subject:** `chat.user.{account}.request.user.{siteID}.status.set`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Sets the calling user's status and returns the updated status view.

##### Request body

| Field    | Type    | Required | Notes |
|----------|---------|----------|-------|
| `text`   | string  | no       | Empty string clears the status. Maximum 512 bytes. |
| `isShow` | boolean | no       | Whether the status is displayed. |

```json
{ "text": "Working from home", "isShow": true }
```

##### Success response

Same shape as `status.getByName`:

```json
{
  "account": "alice",
  "statusText": "Working from home",
  "statusIsShow": true,
  "chineseName": "愛麗絲",
  "engName": "Alice"
}
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| `text` > 512 bytes | `bad_request` | — | `{ "code": "bad_request", "error": "status text too long" }` |
| No active user doc for the caller | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` — nothing is broadcast. |
| Internal failure | `internal` | — | — |

---

#### settings.get

**Subject:** `chat.user.{account}.request.user.{siteID}.settings.get`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Returns the calling user's stored settings sub-document — **exactly as stored** — plus the evaluated admin-managed `permissions`. The server never injects settings defaults: a field the user never set is absent from the reply, and **absent means the client applies its own default** (cross-client default consistency is client-owned by design). A user who never set anything gets `{ "permissions": … }` and nothing else.

##### Request body

None (empty payload).

##### Success response

The stored settings object plus `permissions`. All ten settings fields are optional and appear only when the user has explicitly set them; `permissions` is always present:

| Field | Type | Notes |
|-------|------|-------|
| `fullWidth` | boolean | Full-width layout. |
| `themePreference` | string | Theme: `"system"` \| `"light"` \| `"dark"`. |
| `translateMessageInto` | string | Target language tag for message translation, e.g. `"en-US"`; `""` means translation explicitly off. |
| `messagePreviewEnabled` | boolean | Show message previews in the sidebar list. |
| `muteAllNotifications` | boolean | Mute all notifications. Enforced server-side by `notification-worker`: push delivery is suppressed for this user unless pierced — see `alwaysAllowPriorityNotifications`. |
| `alwaysAllowPriorityNotifications` | boolean | Always allow priority-contact notifications. Enforced server-side: a message whose sender is in [`priorityContacts`](#settingsprioritycontactsadd) is pushed regardless of `muteAllNotifications` and regardless of a suppressing presence (`"busy"` / `"in-call"`) — in **any** room type, DM and channel alike, and for `.bot` senders as well as users. This setting is the only opt-in for that pierce; listing a priority contact without enabling it changes nothing. Per-room mute is not pierced. |
| `showPreviewsInNotifications` | boolean | Show previews in notifications. |
| `showNotificationsInCall` | boolean | Show notifications in call. Enforced server-side: when unset or `false`, push is suppressed while the user's presence is `"busy"` or `"in-call"`. A priority-contact pierce bypasses this — see `alwaysAllowPriorityNotifications`. This enforcement takes effect once presence reporting is enabled server-side; until then no status is treated as in-call, so pushes are delivered regardless of this setting. |
| `initialChatScrollPosition` | string | Where a chat opens: `"lastRead"` \| `"newest"`. |
| `priorityContacts` | string[] | Read-only here — raw contact accounts (not enriched), stored order. Written only by [`settings.priorityContacts.add`](#settingsprioritycontactsadd) / [`settings.priorityContacts.remove`](#settingsprioritycontactsremove), never by `settings.set`. |
| `permissions` | map<permission key, boolean> | Evaluated admin-managed permissions; every known key is always present (`false` when never granted, expired, not yet effective, or revoked). Admin-written, read-only here — `settings.set` cannot touch it. No client event is emitted when an admin changes a permission: call `settings.get` again (e.g. on reconnect) to pick one up. |

```json
{
  "fullWidth": true,
  "themePreference": "dark",
  "translateMessageInto": "en-US",
  "messagePreviewEnabled": true,
  "priorityContacts": ["alice", "helper.bot"],
  "permissions": { "external.image.view": true }
}
```

Never-set user:

```json
{ "permissions": { "external.image.view": false } }
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| No active user doc for the caller | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` |
| Any other failure | — | — | Collapses to the generic boundary error code — see [§6 Error envelope reference](#6-error-envelope-reference). |

---

#### settings.set

**Subject:** `chat.user.{account}.request.user.{siteID}.settings.set`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Partially updates the calling user's settings: **only the fields present in the request are written**; every unsent field keeps its stored value (or stays absent). At least one field is required. Returns the full post-update settings.

##### Request body

Any non-empty subset of the nine settings fields (same types as [`settings.get`](#settingsget)):

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `fullWidth` | boolean | no | |
| `themePreference` | string | no | One of `"system"`, `"light"`, `"dark"`; any other value is rejected. |
| `translateMessageInto` | string | no | Language-tag shape: hyphen-separated letter/digit subtags, leading subtag letters-only (e.g. `"en"`, `"en-US"`, `"zh-Hant-TW"`); or `""` to explicitly turn translation off. No value whitelist. |
| `messagePreviewEnabled` | boolean | no | |
| `muteAllNotifications` | boolean | no | |
| `alwaysAllowPriorityNotifications` | boolean | no | |
| `showPreviewsInNotifications` | boolean | no | |
| `showNotificationsInCall` | boolean | no | |
| `initialChatScrollPosition` | string | no | One of `"lastRead"`, `"newest"`; any other value is rejected. |

```json
{ "fullWidth": false, "translateMessageInto": "ja" }
```

##### Success response

The **full post-update settings** (the same ten settings fields as [`settings.get`](#settingsget), without `permissions`) — sent fields updated, previously stored fields retained:

```json
{
  "fullWidth": false,
  "translateMessageInto": "ja",
  "muteAllNotifications": true
}
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| No fields in the request | `bad_request` | — | `{ "code": "bad_request", "error": "no settings provided" }` |
| Malformed `translateMessageInto` | `bad_request` | — | `{ "code": "bad_request", "error": "invalid translateMessageInto language tag" }` |
| No active user doc for the caller | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` — nothing is written or published. |
| Any other failure | — | — | Collapses to the generic boundary error code — see [§6 Error envelope reference](#6-error-envelope-reference). |

<a id="settingsupdate-event"></a>
##### `settings.update` event

**Emits:** `chat.user.{account}.event.settings.update` after every successful set — ephemeral core-NATS fan-out to the caller's own connected devices (same delivery pattern as `subscription.update`), so other logged-in clients sync live. Best-effort: a fan-out failure does not fail the set.

The payload carries the **full post-update settings** (replace, don't merge):

| Field | Type | Notes |
|-------|------|-------|
| `timestamp` | number | Publish time, Unix ms. |
| `settings` | UserSettings | The full post-update settings — the same ten optional settings fields as [`settings.get`](#settingsget), without `permissions`. |

```json
{
  "timestamp": 1737000000000,
  "settings": { "fullWidth": false, "translateMessageInto": "ja", "muteAllNotifications": true }
}
```

---

#### settings.priorityContacts.get

**Subject:** `chat.user.{account}.request.user.{siteID}.settings.priorityContacts.get`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Returns the calling user's priority-contact list, enriched for display, in
stored order. Capped at 30 stored entries (enforced by the mutating RPCs, not
this one).

##### Request body

None (empty payload).

##### Success response

| Field | Type | Notes |
|-------|------|-------|
| `contacts` | [PriorityContactItem](#prioritycontactitem)[] | The full enriched list, stored order. |

```json
{
  "contacts": [
    {
      "account": "alice",
      "type": "user",
      "user": {
        "engName": "Alice",
        "chineseName": "愛麗絲",
        "employeeId": "E12345",
        "sectName": "Engineering"
      }
    },
    {
      "account": "helper.bot",
      "type": "bot",
      "app": { "name": "Helper Bot" }
    }
  ]
}
```

A contact whose account no longer resolves (deactivated user, deleted app)
still appears with only `account` and `type` — `user`/`app` are omitted.

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| No active user doc for the caller | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` |
| Any other failure | — | — | Collapses to the generic boundary error code — see [§6 Error envelope reference](#6-error-envelope-reference). |

---

#### settings.priorityContacts.add

**Subject:** `chat.user.{account}.request.user.{siteID}.settings.priorityContacts.add`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Adds one contact to the calling user's priority-contact list and returns the
full enriched list. **Idempotent**: re-adding a contact already on the list
succeeds and returns the unchanged list, even when the list is already at the
cap of 30.

##### Request body

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `contactAccount` | string | yes | The account to add. Must be non-empty and must not equal the caller's own account. |

```json
{ "contactAccount": "helper.bot" }
```

##### Success response

Same shape as [`settings.priorityContacts.get`](#settingsprioritycontactsget):

| Field | Type | Notes |
|-------|------|-------|
| `contacts` | [PriorityContactItem](#prioritycontactitem)[] | The full enriched list, stored order, after this mutation. |

```json
{
  "contacts": [
    {
      "account": "alice",
      "type": "user",
      "user": {
        "engName": "Alice",
        "chineseName": "愛麗絲",
        "employeeId": "E12345",
        "sectName": "Engineering"
      }
    },
    {
      "account": "helper.bot",
      "type": "bot",
      "app": { "name": "Helper Bot" }
    }
  ]
}
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| `contactAccount` missing or empty | `bad_request` | — | `{ "code": "bad_request", "error": "contactAccount is required" }` |
| `contactAccount` equals the caller's own account | `bad_request` | — | `{ "code": "bad_request", "error": "cannot add yourself as a priority contact" }` |
| `contactAccount` does not resolve | `not_found` | `priority_contact_not_found` | `{ "code": "not_found", "reason": "priority_contact_not_found", "error": "priority contact not found" }` — a user account must be ACTIVE; a `.bot` account only needs its app to exist (it need not be enabled). |
| List already at the 30-entry cap and `contactAccount` is not already on it | `forbidden` | `priority_contact_limit` | `{ "code": "forbidden", "reason": "priority_contact_limit", "error": "priority contact limit reached" }` |
| Write missed (cap or missing caller) but a concurrent `settings.priorityContacts.remove` changed the list before the disambiguating re-read | `conflict` | — | `{ "code": "conflict", "error": "priority contacts changed concurrently, retry" }` — retry the request. |
| No active user doc for the caller | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` |
| Any other failure | — | — | Collapses to the generic boundary error code — see [§6 Error envelope reference](#6-error-envelope-reference). |

**Emits:** [`settings.update`](#settingsupdate-event) to the caller's other devices, carrying the full post-update settings — including a duplicate add under the cap (the stored list is unchanged, but `settingsUpdatedAt` still bumps and both fanouts still fire). Only a duplicate add already at the 30-entry cap skips the publish. A server-side cross-site federation update also fires whenever this event does, not delivered to clients.

---

#### settings.priorityContacts.remove

**Subject:** `chat.user.{account}.request.user.{siteID}.settings.priorityContacts.remove`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Removes one contact from the calling user's priority-contact list and returns
the full enriched list. **Idempotent**: removing a contact not on the list
succeeds and returns the unchanged list. Unlike `add`, removing yourself is
allowed (no self-account check), and there is no existence check on
`contactAccount` — removing a since-deleted account is exactly the cleanup
case this permits.

##### Request body

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `contactAccount` | string | yes | The account to remove. Must be non-empty. |

```json
{ "contactAccount": "helper.bot" }
```

##### Success response

Same shape as [`settings.priorityContacts.get`](#settingsprioritycontactsget):

| Field | Type | Notes |
|-------|------|-------|
| `contacts` | [PriorityContactItem](#prioritycontactitem)[] | The full enriched list, stored order, after this mutation. |

```json
{
  "contacts": [
    {
      "account": "alice",
      "type": "user",
      "user": {
        "engName": "Alice",
        "chineseName": "愛麗絲",
        "employeeId": "E12345",
        "sectName": "Engineering"
      }
    },
    {
      "account": "helper.bot",
      "type": "bot",
      "app": { "name": "Helper Bot" }
    }
  ]
}
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| `contactAccount` missing or empty | `bad_request` | — | `{ "code": "bad_request", "error": "contactAccount is required" }` |
| No active user doc for the caller | `not_found` | — | `{ "code": "not_found", "error": "user not found" }` |
| Any other failure | — | — | Collapses to the generic boundary error code — see [§6 Error envelope reference](#6-error-envelope-reference). |

**Emits:** [`settings.update`](#settingsupdate-event) to the caller's other devices, carrying the full post-update settings. A server-side cross-site federation update also fires, not delivered to clients.

---

#### Chatlist Sections

The per-user chatlist is split into **sections**. A chat's section **membership**
and manual **order** live on its subscription (`sectionId` / `sectionOrder`, set via
[Move Chat to Section](#move-chat-to-section)); the section **definitions** (names,
display order, sort mode) live in a small per-user overlay owned by `user-service`.
So the overlay is O(number of sections), never O(number of chats) — a rename or
reorder fans a few-KB event no matter how many chats a section holds.

**Built-in sections** (membership derived client-side, never user-set): `favorites`
(from `subscription.favorite`), `apps` (from `isBot`), `teams` (from
`subscription.sectionId == "teams"`), `chats` (everything else — no `sectionId`, not
favorite, not a bot). All built-ins default to `sortMode: "mostRecent"`.

**sortMode**: `"custom"` (sort the section's chats by `subscription.sectionOrder`) or
`"mostRecent"` (sort by last message — the default and fallback).

##### Client read model

1. Load the paginated room list ([`subscription.list`](#subscriptionlist)) — each sub
   already carries `favorite`, `sectionId`, `sectionOrder`.
2. Load `chatlist.get` once — the section definitions (names, order, sortMode).
3. Group each sub: `favorites` (favorite) · `apps` (bot) · `teams` (`sectionId == "teams"`)
   · each custom section (`sectionId == <that id>`) · `chats` (no `sectionId`, not
   favorite/bot). A `sectionId` pointing at a section not in the definitions renders in
   **chats** (orphan tolerance — a deleted section leaves its members orphaned, no cascade).
4. Within a section: `sortMode == "custom"` → order by `sectionOrder`; else by last message.
5. Live updates: `subscription.update` (a chat's membership/order changed) and
   `chatlist.update` (a section def changed) each replace their own scope, guarded by
   their timestamp (last-write-wins, no deltas).

##### RPCs

| Subject action | Body | Notes |
|---|---|---|
| `chatlist.get` | — (no body) | Returns the [ChatlistState](#chatliststate). Defaults (the built-ins) when never customized. |
| `chatlist.section.create` | `{ "name": string, "sortMode"?: "custom"\|"mostRecent" }` | Adds a custom section above `chats`. `sortMode` defaults to `custom`. |
| `chatlist.section.rename` | `{ "sectionId": string, "name": string }` | Custom sections only. |
| `chatlist.section.delete` | `{ "sectionId": string }` | Custom sections only; members orphan to `chats`. |
| `chatlist.section.reorder` | `{ "sectionOrder": string[] }` | A permutation of **every** section id (built-in + custom). |
| `chatlist.section.setSortMode` | `{ "sectionId": string, "sortMode": "custom"\|"mostRecent" }` | Any section (a built-in may be flipped to `custom`). |

Every mutation returns the full post-update [ChatlistState](#chatliststate) and fans out
[`chatlist.update`](events.md#chatlistupdate--chatlist-section-sync) to the caller's other
devices (and cross-site, HWM-guarded by `chatlistUpdatedAt`).

**Section name validation** (create / rename): 1–50 characters after trimming, no
consecutive spaces, only letters/digits (any script), spaces, or `-_./()`. Names are
case-sensitive and unique across a user's custom sections. Errors carry a reason:
`chatlist_invalid_name`, `chatlist_duplicate_name`, `chatlist_section_not_found`,
`chatlist_builtin_immutable` (rename/delete a built-in), `chatlist_invalid_order`,
`chatlist_invalid_sort_mode`.

##### ChatlistState

| Field | Type | Notes |
|---|---|---|
| `sectionOrder` | string[] | Section display order — every section id, built-in + custom. |
| `sections` | ChatlistSection[] | The section definitions. |
| `lastUpdatedAt` | number | Unix-millis high-water mark (last-write-wins). |

**ChatlistSection**: `{ "id": string, "name": string, "builtIn": boolean, "sortMode": "custom"|"mostRecent" }`.
There is **no** member list on a section — membership rides the subscriptions.

---

#### subscription.list

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.list`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

> **Large pages:** the NATS reply is capped at 128 KB, so a full sidebar does not
> fit in one call — a page that overflows it comes back as `internal` /
> [`response_too_large`](#6-error-envelope-reference), which is the signal to
> switch rather than to retry. Clients fetching 100+ rows should use the HTTP form,
> [GET /api/v1/subscriptions](#132-http--get-apiv1subscriptions), which returns
> the same body with no payload ceiling.

Returns the user's sidebar subscriptions, optionally filtered by type, age, and favorite status. The reply is **room-info-enriched** — see "Enrichment" below.

##### Request body

| Field               | Type    | Required | Notes |
|---------------------|---------|----------|-------|
| `type`              | string  | yes      | One of `"current"` (active rooms), `"rooms"` (DM and channel subscriptions), `"apps"` (botDM rooms). |
| `favorite`          | boolean | no       | When `true`, filters to favorited subscriptions only **and** moves the self-DM to the front of the list. |
| `updatedWithinDays` | number  | no       | When set, filters **`rooms`-type** results to rooms **whose last message (`room.lastMsgAt`) is within the last N days** — room activity, not the subscription's update time. Cross-site rooms (no local `lastMsgAt`) fall outside the window. **Ignored for `current`** (always returns the full active set) and for `apps`. Omit for no age filter — the server applies no default; the client supplies any default it wants. Must be non-negative; a negative value is rejected with `bad_request`. |
| `includeLastMessage` | boolean | no      | Whether to embed each room's [`previewMessage`](#subscriptionroom). Omitted ⇒ include (backward-compatible default); `false` ⇒ skip the per-room last-message resolve (a client that renders no room-list snippet can send `false` to save the server-side work). |
| `offset`            | integer | no       | Zero-based index of the first record to return. Negative ⇒ `0`. Default `0`. |
| `limit`             | integer | no       | Page size. Omitted or ≤ 0 ⇒ the server default `SUBSCRIPTION_DEFAULT_LIMIT` (default `40`); values above `MAX_SUBSCRIPTION_LIMIT` (default `1000`) are capped to it. |

```json
{ "type": "current", "favorite": true, "offset": 0, "limit": 40 }
```

##### Success response

| Field           | Type              | Notes |
|-----------------|-------------------|-------|
| `subscriptions` | array<[Subscription](#subscription)> | One page of room-info-enriched subscription records. |
| `hasMore`       | boolean           | `true` when at least one more record follows this page (the server over-fetches `limit + 1` to decide). Request the next page by advancing `offset` by the `limit` you sent. |

`subscriptions` is one page of [Subscription](#subscription) records (full schema in §3.0), room-info-enriched per the behavior below. Ordered by the room's `lastMsgAt` descending (rooms with no messages fall back to the room's `createdAt`). In the `favorite` view the caller's self-DM is pinned first; otherwise favorites are **not** pinned by this ordering. Ordering freshness is bounded by a short server-side cache (default 15s): a room's position may lag its newest message by up to that window, while the returned records' `room` fields — and `updatedWithinDays`, `favorite` and open/closed membership — are always fresh: a row that stops matching the request's filters before its page is built is dropped from that page rather than returned in its stale state. The cache is per server instance, so consecutive pages of one `hasMore` drain may be served from orderings that differ within that window: treat multi-page drains as best-effort membership and dedupe rows by `roomId` (a row can appear on two pages or fall between them; a missed room reappears on its next event or refetch).

Results are **paginated** by `offset`/`limit` (offset-based): the server returns the requested window and `hasMore` signals whether another page follows. `limit` defaults to `SUBSCRIPTION_DEFAULT_LIMIT` (default `40`) when omitted and is capped at `MAX_SUBSCRIPTION_LIMIT` (default `1000`); omitting `offset`/`limit` yields the first page.

<a id="enrichment"></a>
**Enrichment behavior** (shared by `subscription.list`, `subscription.getChannels`, `subscription.getDM`, `subscription.getByRoomID`):
- Room-derived fields are returned under the nested `room` object ([SubscriptionRoom](#subscriptionroom)): **local** rows from the Mongo `$lookup` baseline (no RPC), **cross-site** rows from room-service's per-site `GetRoomsInfo` RPC. The subscription's own fields are never overwritten by room data.
- `alert` and `hasMention` are **subscription** state, not room state: they are returned as stored on the subscription (maintained by the write path — `broadcast-worker` sets `hasMention` when the user is @-mentioned, read receipts clear `alert`) and are **never** overwritten or recomputed by enrichment.
- `room.privateKey` / `room.keyVersion` deliver the room's current E2E key to the member when the room has one (the initial key bootstrap on (re)connect; see §5). Both fields are omitted for rooms with no key.
- **Room names carry no special meaning.** No endpoint filters, drops, or suppresses a subscription based on its room's name — the `Del-` prefix that legacy soft-deletes used is not interpreted anywhere, so a room named `Del-anything` is returned like any other. Enrichment only adds: every subscription a query matches is returned, so a `subscription.list` page is never shortened after pagination — it is short only when the query itself matched fewer rows than `limit`.
- **Local** rows carry the full room object (metadata + E2E key) from the `$lookup` baseline. **Cross-site** rows are fetched per remote site in parallel; if a site's RPC fails or a room isn't found, those rows are returned with **no `room` object** (the field is omitted) — the subscription still carries its own top-level `siteId`. `alert` and `hasMention` are unaffected (they come from the subscription, not the RPC).
- **Teams-migrated rooms** (`room.origin == "teams"`, server-side only — not sent on the wire): excluded from `subscription.list`/`subscription.count` when the server's `SHOW_TEAMS_ROOM` env is `false` (the default); included when `true`, **or** when the requesting account is listed in `SHOW_TEAMS_ROOM_ACCOUNTS` (a comma-separated per-account allowlist). Reversible read-time filter, no data change.

**Per-room-type record shape.** The kinds returned by `subscription.list` differ by row schema: `channel` and `dm` rows use the [Subscription](#subscription) schema (§3.0) — `dm` adds a top-level `hrInfo` — while `botDM` rows add a nested `app` object ([AppSubscription](#appsubscription), §3.0). All carry the nested [SubscriptionRoom](#subscriptionroom) (§3.0). Every field except the ones below is identical across the three types (`id`, `u`, `roomId`, `siteId`, `roles`, `joinedAt`, `muted`, `favorite`, `alert`, `hasMention`, `hasUnread`, `hasGroupMention`, the per-attribute `*UpdatedAt` timestamps, and the rest of `room`). `isSubscribed` is a **base [Subscription](#subscription) field** (boolean, optional — omitted unless stored `true`) shared by all three types, not a type-specific field. Type-specific fields:

| Field | `channel` | `dm` | `botDM` |
|---|---|---|---|
| `name` | Channel name. | Counterpart's account. | App display name (falls back to the bot account when the app record is unavailable). |
| `hrInfo` | absent | Counterpart's HR record ([SubscriptionHRInfo](#subscriptionhrinfo)) — returned in both `subscription.list` and `subscription.getDM`. | absent |
| `app` | absent | absent | Nested app-metadata object — see [AppSubscription](#appsubscription). |
| `room.name` | Canonical channel name. | Omitted (DM rooms have no canonical name). | Omitted (botDM rooms have no canonical name; the app name is the top-level `name`). |
| `room.appCount` | Bot/app count in the channel (omitted when 0). | omitted (0). | ≥ 1. |

The example below shows one record of each type in order (`channel`, `dm`, `botDM`):

```json
{
  "subscriptions": [
    {
      "id": "01970a4f8c2d7c9a01970a4f8c2d7c9b",
      "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice", "isBot": false },
      "roomId": "01970a4f8c2d7c9aQ",
      "siteId": "siteA",
      "roomType": "channel",
      "roles": ["member"],
      "name": "engineering-general",
      "joinedAt": "2026-05-06T08:01:23Z",
      "hasMention": false,
      "hasUnread": true,
      "hasGroupMention": false,
      "alert": true,
      "muted": false,
      "favorite": true,
      "favoriteUpdatedAt": "2026-05-15T09:00:00Z",
      "muteUpdatedAt": "2026-05-10T12:00:00Z",
      "rolesUpdatedAt": "2026-05-06T08:01:23Z",
      "nameUpdatedAt": "2026-05-06T08:01:23Z",
      "restrictUpdatedAt": "2026-05-06T08:01:23Z",
      "room": {
        "siteId": "siteA",
        "name": "engineering-general",
        "userCount": 42,
        "appCount": 2,
        "lastMsgAt": "2026-06-01T10:00:00Z",
        "lastMsgId": "01970a4f8c2d7c9aBB",
        "lastMentionAllAt": "2026-05-30T08:00:00Z",
        "minUserLastSeenAt": "2026-05-30T07:55:00Z",
        "privateKey": "bDM4dGZ5...base64...JjT0g9PQ==",
        "keyVersion": 3,
        "previewMessage": {
          "messageId": "01970a4f8c2d7c9aBB",
          "sender": { "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice", "chineseName": "愛麗絲", "displayName": "Alice 愛麗絲" },
          "content": "morning team",
          "createdAt": "2026-05-06T07:55:00Z"
        }
      }
    },
    {
      "id": "01970a4f8c2d7c9a01970a4f8c2d7c9c",
      "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice", "isBot": false },
      "roomId": "alice_bob",
      "siteId": "siteA",
      "roomType": "dm",
      "roles": ["member"],
      "name": "bob",
      "joinedAt": "2026-04-01T09:00:00Z",
      "hasMention": false,
      "hasUnread": false,
      "hasGroupMention": false,
      "alert": false,
      "muted": false,
      "favorite": false,
      "hrInfo": { "account": "bob", "name": "鮑伯", "engName": "Bob" },
      "room": {
        "siteId": "siteA",
        "lastMsgAt": "2026-05-20T15:30:00Z"
      }
    },
    {
      "id": "01970a4f8c2d7c9a01970a4f8c2d7c9d",
      "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice", "isBot": false },
      "roomId": "alice_helper.bot",
      "siteId": "siteA",
      "roomType": "botDM",
      "roles": ["member"],
      "name": "Helper",
      "isSubscribed": true,
      "joinedAt": "2026-03-15T11:00:00Z",
      "hasMention": false,
      "hasUnread": false,
      "hasGroupMention": false,
      "alert": false,
      "muted": false,
      "favorite": false,
      "app": {
        "appId": "app_helper",
        "name": "Helper",
        "description": "Your friendly helper bot",
        "assistant": { "name": "helper.bot", "enabled": true, "username": "Helper" },
        "appViewUrl": { "main": "https://apps.example.com/helper" },
        "version": "1.4.2",
        "sponsors": [{ "name": "Acme Corp", "phone": "+1-555-0100" }]
      },
      "room": {
        "siteId": "siteA",
        "userCount": 1,
        "appCount": 1,
        "lastMsgAt": "2026-05-01T08:00:00Z",
        "lastMsgId": "01970a4f8c2d7c9aDD"
      }
    }
  ],
  "hasMore": false
}
```

##### Error response

| Condition | `code` | Notes |
|-----------|--------|-------|
| Unknown `type` value | `bad_request` | `{ "code": "bad_request", "error": "unknown subscription type" }` |
| Negative `updatedWithinDays` | `bad_request` | `{ "code": "bad_request", "error": "updatedWithinDays must be non-negative" }` |
| Server exceeded its own handler budget | `unavailable` | Enrichment did not finish in time. Retryable — the server fails rather than return a page whose rooms are indistinguishable from deleted ones. |
| Reply exceeds the transport payload cap | `internal` (`response_too_large`) | Only over NATS. The HTTP form has no payload ceiling — see [GET /api/v1/subscriptions](#132-http--get-apiv1subscriptions). |
| Internal failure | `internal` | — |

---

#### subscription.getChannels

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.getChannels`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Returns the channel subscriptions for the calling user — rooms containing the requester AND all members listed in `accountNames` (exact match). Bot accounts are excluded from the membership check: accounts ending in `.bot` are ignored in the match even if listed. Exactly one of `membersContain` or `accountNames` must be provided. The reply is **room-info-enriched** (same behavior as `subscription.list`).

##### Request body

Exactly one of the two fields must be set. The requester's own account is implicitly included in the membership check — it does not need to be listed in `accountNames`.

| Field            | Type     | Required | Notes |
|------------------|----------|----------|-------|
| `membersContain` | string   | one-of   | Return channels that contain this single account as a member. |
| `accountNames`   | string[] | one-of   | Return channels where ALL of the given accounts (plus the requester) are members. Accounts ending in `.bot` are ignored in the match even if listed. |
| `offset`         | integer  | no       | Zero-based index of the first record. Negative ⇒ `0`. Default `0`. |
| `limit`          | integer  | no       | Page size. Omitted or ≤ 0 ⇒ `SUBSCRIPTION_DEFAULT_LIMIT` (default `40`); capped at `MAX_SUBSCRIPTION_LIMIT` (default `1000`). |

```json
{ "membersContain": "bob", "offset": 0, "limit": 40 }
```

##### Success response

Same paginated shape as `subscription.list` — `{ "subscriptions": [...], "hasMore": <bool> }`, where `hasMore` is `true` when another page follows (offset-based; advance `offset` by your `limit`) — with [enrichment](#enrichment) applied.

```json
{
  "subscriptions": [
    {
      "id": "01970a4f8c2d7c9a01970a4f8c2d7c9b",
      "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice", "isBot": false },
      "roomId": "01970a4f8c2d7c9aQ",
      "siteId": "siteA",
      "roomType": "channel",
      "roles": ["member"],
      "name": "engineering-general",
      "joinedAt": "2026-05-06T08:01:23Z",
      "hasMention": false,
      "hasUnread": true,
      "hasGroupMention": false,
      "alert": true,
      "muted": false,
      "favorite": true,
      "room": {
        "siteId": "siteA",
        "name": "engineering-general",
        "userCount": 42,
        "appCount": 2,
        "lastMsgAt": "2026-06-01T10:00:00Z",
        "lastMsgId": "01970a4f8c2d7c9aBB",
        "privateKey": "bDM4dGZ5...base64...JjT0g9PQ==",
        "keyVersion": 3
      }
    }
  ],
  "hasMore": false
}
```

##### Error response

| Condition | `code` | Notes |
|-----------|--------|-------|
| Both or neither field set | `bad_request` | `{ "code": "bad_request", "error": "exactly one of membersContain or accountNames is required" }` |
| Too many `accountNames` (> 100) | `bad_request` | `{ "code": "bad_request", "error": "too many accountNames" }` |
| Internal failure | `internal` | — |

---

#### subscription.getDM

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.getDM`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Returns the calling user's DM subscription with the named counterpart. The reply is **room-info-enriched** (same behavior as `subscription.list`). Any account is a valid DM target — an ordinary user, a bot, or the platform-admin pseudo-account — since all of them can log into the chat frontend and hold a DM subscription.

##### Request body

| Field         | Type   | Required | Notes |
|---------------|--------|----------|-------|
| `accountName` | string | yes      | The counterpart's account. Any account is valid — an ordinary user, a bot (`.bot` suffix), or the platform-admin pseudo-account. |

```json
{ "accountName": "bob" }
```

##### Success response

| Field          | Type           | Notes |
|----------------|----------------|-------|
| `subscription` | [DMSubscription](#subscription) | The enriched DM subscription. |

`DMSubscription` is a [Subscription](#subscription) with one additional field, `hrInfo`, nested inside the `subscription` object alongside the base fields:

| Field | Type | Notes |
|---|---|---|
| `hrInfo` | [SubscriptionHRInfo](#subscriptionhrinfo) | Optional. DM counterpart's HR record. Present when available. |

```json
{
  "subscription": {
    "id": "01970a4f8c2d7c9a01970a4f8c2d7c9c",
    "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice", "isBot": false },
    "roomId": "alice_bob",
    "siteId": "siteA",
    "roomType": "dm",
    "roles": ["member"],
    "name": "bob",
    "joinedAt": "2026-04-01T09:00:00Z",
    "alert": false,
    "hasMention": false,
    "hasUnread": false,
    "hasGroupMention": false,
    "muted": false,
    "favorite": false,
    "hrInfo": { "account": "bob", "name": "鮑伯", "engName": "Bob" },
    "room": {
      "siteId": "siteA",
      "lastMsgAt": "2026-05-20T15:30:00Z"
    }
  }
}
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| `accountName` empty | `bad_request` | — | `"accountName required"` |
| DM subscription not found | `not_found` | `subscription_not_found` | `"dm not found"` |
| Internal failure | `internal` | — | — |

---

#### subscription.getByRoomID

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.getByRoomID`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Returns the calling user's subscription for a single room (any room type) as a **0-or-1-element list**. When the caller isn't subscribed to that room, the reply is an empty list (`total: 0`) — absence is a normal result, **not** an error. A present subscription is **room-info-enriched** (same behavior as `subscription.list`).

##### Request body

| Field    | Type   | Required | Notes |
|----------|--------|----------|-------|
| `roomId` | string | yes      | The room whose subscription to fetch. |

```json
{ "roomId": "alice_bob" }
```

##### Success response

Same shape as `subscription.list` — a (here, at most one) list:

| Field           | Type           | Notes |
|-----------------|----------------|-------|
| `subscriptions` | [Subscription](#subscription)[] | The matching subscription, or empty when not subscribed. |
| `total`         | number         | `1` when found, `0` when not subscribed. |

```json
{
  "subscriptions": [
    {
      "id": "01970a4f8c2d7c9a01970a4f8c2d7c9c",
      "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice", "isBot": false },
      "roomId": "alice_bob",
      "siteId": "siteA",
      "roomType": "dm",
      "roles": ["member"],
      "name": "bob",
      "joinedAt": "2026-04-01T09:00:00Z",
      "alert": false,
      "hasMention": false,
      "hasUnread": false,
      "hasGroupMention": false,
      "muted": false,
      "favorite": false,
      "room": {
        "siteId": "siteA",
        "lastMsgAt": "2026-05-20T15:30:00Z",
        "lastMsgId": "01970a4f8c2d7c9aCC"
      }
    }
  ],
  "total": 1
}
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| `roomId` empty | `bad_request` | — | `"roomId required"` |
| Internal failure | `internal` | — | — |

---

#### subscription.count

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.count`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Returns the count of active subscriptions, optionally filtered to unread rooms only.

**Active set:** an active subscription is a non-muted, **open** DM or channel, **or** a botDM that is non-muted, open **and** subscribed (`isSubscribed: true`). Excluded from the count: unsubscribed botDMs, muted rooms of any type, and rooms the user has closed (`open: false`) — closed rooms are hidden from `subscription.list`, so counting them would put the badge and the list permanently out of step. A missing `open` field (legacy documents) counts as open. The count reads subscription state only — it consults no room document, and no room name is filtered.

##### Request body

| Field    | Type    | Required | Notes |
|----------|---------|----------|-------|
| `unread` | boolean | no       | When `true`, returns the number of active rooms with unread messages or unread followed threads. When `false` or absent, returns the total active-subscription count. |

```json
{ "unread": true }
```

##### Success response

| Field   | Type   | Notes |
|---------|--------|-------|
| `count` | number | The subscription count (total or unread depending on the request). |

```json
{ "count": 5 }
```

**Unread count behavior:** when `unread: true`, the service fetches the active subscriptions and splits them by site. **Local** subscriptions are counted directly from the room baseline carried on the `$lookup` (comparing the room's `lastMsgAt` against the subscription's `lastSeenAt`) — no RPC is made. Only **cross-site** subscriptions trigger a per-site `GetRoomsMeta` RPC, run in **parallel**. The count **degrades per-site** (matching `subscription.list` enrichment): if a cross-site RPC fails, that site's subscriptions are **skipped** — omitted from the count and logged as a warning — while local subscriptions and the sites that did respond still contribute. The result is a best-effort count that may under-report while a remote site is unreachable, rather than the full active-subscription total.

**Threads:** a room also counts as unread if it has at least one unread followed thread, even when its own messages are all read — at most **+1 per room** (existence, not a per-thread count). Muted rooms are excluded (as with room-level unread), and only rooms within the fetched active-subscription page are considered. Thread-unread state (`Subscription.ThreadUnread`, a list of unread thread parent-message IDs) is already carried on the subscription document fetched for the room-level pass — federated onto the account's home-replica sub for both local and cross-site rooms — so this phase needs no additional RPC and cannot degrade independently of the room-level pass.

**Caching:** when the server-side badge cache is enabled, the unread count may be served from the account's maintained unread-room set instead of being recomputed. The set is bumped on every message, and a read removes exactly the room read (a room with unread followed threads stays counted); mute, unmute, thread-read and membership changes invalidate it. The set is re-derived from MongoDB whenever it has gone unverified for longer than the server's badge marker TTL, which is therefore the upper bound on how stale this count can be. Same response schema and staleness bounds as the badge counts carried on push notifications (this count is not capped for display, unlike push counts).

##### Error response

| Condition | `code` | Notes |
|-----------|--------|-------|
| Internal failure | `internal` | — |

---

#### subscription.setAppSubscription

**Subject:** `chat.user.{account}.request.user.{siteID}.subscription.setAppSubscription`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

PUT-like idempotent endpoint to subscribe or unsubscribe the calling user from a bot app. The `subscribed` field is the **desired end-state**; calling with `subscribed: true` on an already-subscribed user is safe (re-enables the subscription and clears `muted`). Replaces the former `subscribeApp` / `unsubscribeApp` endpoints.

##### Request body

| Field        | Type    | Required | Notes |
|--------------|---------|----------|-------|
| `appId`      | string  | yes      | The ID of the app to subscribe/unsubscribe. |
| `subscribed` | boolean | yes      | `true` = subscribe; `false` = unsubscribe. |

```json
{ "appId": "calendar-app", "subscribed": true }
```

**Subscribe behavior:**
- If the user has no existing DM room with the bot, a new botDM room is created via room-service.
- If the user had a previous subscription (muted or deactivated), it is re-enabled and `muted` is cleared.

**Unsubscribe behavior:**
- Marks the subscription as unsubscribed and muted. The botDM room is not deleted.

##### Success response

| Field     | Type    | Notes |
|-----------|---------|-------|
| `success` | boolean | Always `true`. |

```json
{ "success": true }
```

##### Triggered events — success path

**`chat.user.{account}.event.subscription.update`** — emitted once for the requester (best-effort, core NATS) so the user's other devices reconcile the botDM without a refetch. Bot accounts receive it on their **encoded** per-user subject (dots→underscores), matching their NATS JWT scope.

- **Unsubscribe** (`subscribed: false` on an existing subscription) → `action: "removed"`. Payload is the dedicated `SubscriptionRemovedEvent` (`subscription` carries only `roomId`, `roomType: "botDM"`, and `u`). No event fires when there was no subscription to remove.
- **Reactivate** (`subscribed: true` on an existing, previously-unsubscribed subscription) → `action: "added"`, the same event the first-time subscribe path emits, so the FE re-adds the botDM it dropped on the prior `removed`. See the [subscription.update schema](#subscriptionupdate-event); the embedded `Subscription` reflects `isSubscribed: true` / `muted: false`, and `appInfo` carries the bot app's identity.
- **First-time subscribe** (`subscribed: true`, no existing DM room) → the `action: "added"` event is emitted by room-service as part of botDM room creation, not by this handler.

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| `appId` missing | `bad_request` | — | `"appId required"` |
| App not found | `not_found` | `app_not_found` | `"app not found"` |
| App has no assistant | `bad_request` | `app_disabled` | `"app has no assistant"` |
| room-service unreachable | `unavailable` | `no_responders` | `"create-dm rpc: no service responding"` — subscribing with no existing DM room creates a botDM room via room-service. Retryable. |
| room-service did not answer | `unavailable` | `upstream_timeout` | `"create-dm rpc: upstream did not respond in time"` — same path, request delivered but unanswered. Retryable. |
| Internal failure | `internal` | — | — |

---

#### apps.list

**Subject:** `chat.user.{account}.request.user.{siteID}.apps.list`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Returns a page of the apps known to the system, each annotated with whether the calling user is currently subscribed to the app's bot assistant. Sorted by app name.

##### Request body

Optional — an empty body returns the first page with defaults.

| Field    | Type   | Required | Notes |
|----------|--------|----------|-------|
| `limit`  | number | no       | Page size. Default `20`, max `100`. |
| `offset` | number | no       | Number of apps to skip. Default `0`. |

```json
{ "limit": 20, "offset": 0 }
```

##### Success response

| Field   | Type           | Notes |
|---------|----------------|-------|
| `apps`    | AppListItem[] | The requested page of apps. |
| `hasMore` | boolean       | `true` when at least one more app follows this page (the server over-fetches `limit + 1`). Advance `offset` by your `limit` for the next page. |

`AppListItem` is a flattened [App](#app) record plus `isSubscribed`:

| Field | Type | Notes |
|---|---|---|
| `id` | string | App ID. |
| `name` | string | App display name. |
| `description` | string | Optional. App description. |
| `appViewUrl` | map<string, string> | Optional. App-view URLs keyed by view name (e.g. `{"main": "..."}`). |
| `reportUrl` | string | Optional. App report URL. |
| `forumUrl` | string | Optional. App forum URL. |
| `userManualUrl` | string | Optional. App user-manual URL. |
| `version` | string | Optional. App version. |
| `assistant` | [AppAssistant](#appassistant) | Optional. The app's bot assistant. |
| `channelTab` | [AppChannelTab](#appchanneltab) | Optional. Channel-tab configuration. |
| `sponsors` | [AppSponsor](#appsponsor)[] | Optional. App sponsor list. |
| `isSubscribed` | boolean | Whether the calling user is subscribed to this app's bot. |

```json
{
  "apps": [
    {
      "id": "calendar-app",
      "name": "Calendar",
      "description": "Meeting and calendar integration",
      "appViewUrl": { "main": "https://apps.example.com/calendar" },
      "reportUrl": "https://apps.example.com/calendar/report",
      "forumUrl": "https://apps.example.com/calendar/forum",
      "userManualUrl": "https://apps.example.com/calendar/manual",
      "version": "2.1.0",
      "assistant": { "enabled": true, "name": "calendar.bot" },
      "isSubscribed": true
    }
  ],
  "hasMore": false
}
```

##### Error response

| Condition | `code` | Notes |
|-----------|--------|-------|
| Internal failure | `internal` | — |

---

#### apps.categories

**Subject:** `chat.user.{account}.request.user.{siteID}.apps.categories`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Returns the full fab-domain → site mapping used to group apps in the UI, sorted by `name` ascending (rows sharing a `name` are ordered by `id`, so ordering is deterministic across calls). Global, slow-changing reference data — no filtering, no pagination. The mapping is populated out-of-band (legacy migration); a site whose collection is unpopulated returns `{ "categories": [] }`.

##### Request body

None — send an empty payload.

##### Success response

| Field | Type | Notes |
|---|---|---|
| `categories` | [AppCategory](#appcategory)[] | All mappings, sorted by `name` ascending then `id` ascending. Always an array — `[]` when empty, never `null`. |

###### AppCategory

| Field | Type | Notes |
|---|---|---|
| `id` | string | Mapping ID — the hex form of the Mongo ObjectID, exposed under the `id` key per the API-wide `_id`→`id` convention. (Unlike `apps.list` ids, which are plain strings.) |
| `name` | string | Fab/domain name; the array is sorted by this field. |
| `siteId` | string | Site the fab/domain belongs to. |

```json
{
  "categories": [
    { "id": "64226446224a1b2c3d4e5f61", "name": "F14", "siteId": "00700000" },
    { "id": "64226446224a1b2c3d4e5f60", "name": "F22", "siteId": "00600000" }
  ]
}
```

##### Error response

| Condition | `code` | Notes |
|-----------|--------|-------|
| Internal failure | `internal` | — |

---

#### List User Threads

**Subject:** `chat.user.{account}.request.user.{siteID}.thread.list`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` is the **caller's own home site** — the site that holds the user's federated subscriptions and runs the aggregator.

Returns the user's thread subscriptions across **all sites** as one globally-ordered "thread inbox", newest activity first. Each item carries the thread's parent and last message plus the owning room's name/type. `user-service` fans out a per-site query to **every configured federation site** (`ALL_SITE_IDS`, including the local site), and each site's `history-service` answers for its own threads; the results are merged into one ordered page.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `cursor` | string | no | Opaque pagination cursor from a previous response's `nextCursor`. Omit for the first page. |
| `limit` | number | no | Page size. Default `20`, capped at `100`. |

```json
{
  "limit": 20
}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `items` | [ThreadListItem](#threadlistitem)[] | Thread subscriptions, ordered by last activity (newest first) across all sites. |
| `nextCursor` | string | Optional. Opaque cursor for the next page; absent on the last page. |
| `hasNext` | boolean | `true` if more threads exist beyond this page. |
| `unavailableSites` | string[] | Optional. Sites that failed to respond for this page; their threads may appear on a later page once they recover. |
| `sizeLimited` | boolean | Optional. `true` when rows were dropped to keep the reply inside the transport's `max_payload`. A short page alone does not mean this — a per-site limit or an unavailable peer also produces one — so branch on this flag, never on `len(items) < limit`. Absent when nothing was dropped, including when a row was merely blanked (`truncated`). See **A page may be shorter than `limit`** for how to pick the next `limit`. |

###### ThreadListItem

| Field | Type | Notes |
|---|---|---|
| `siteId` | string | The thread's owning site. |
| `roomId` | string | The room the thread belongs to. |
| `roomName` | string | Per-subscriber display label, sourced from the user's subscription: `channel` → room name; `dm` → counterpart account; `botDM` → app name. |
| `roomType` | string | The owning room's type (`channel`, `dm`, `botDM`, `discussion`). |
| `threadRoomId` | string | The thread room ID. |
| `parentMessageId` | string | The thread's parent (top-level) message ID. |
| `lastSeenAt` | number | Optional. UTC ms the user last read the thread; absent if never opened. |
| `hasMention` | boolean | The user was @-mentioned in the thread. |
| `unread` | boolean | `true` when `lastMsgAt` is newer than `lastSeenAt` (or the thread was never opened). |
| `lastMsgAt` | number | UTC ms of the thread's last activity — the global sort key. |
| `tcount` | number | Exact non-deleted reply count. Always present; `0` also covers threads whose count was never written — migrated threads, and briefly a just-created thread whose first reply has not yet been counted. During a mixed-version rollout, rows from a not-yet-upgraded site read `0` (their leaf omits the field), and the key is absent entirely behind a not-yet-upgraded aggregator. |
| `parentMessage` | [Message](#message-schema) | Optional. The hydrated parent message. |
| `lastMessage` | [Message](#message-schema) | Optional. The hydrated last reply. |
| `truncated` | boolean | Optional. `true` when the row's `parentMessage` and `lastMessage` were dropped because the row alone exceeded the transport's `max_payload`. Identifiers and `lastMsgAt` are kept so pagination still advances. |
| `hrInfo` | [SubscriptionHRInfo](#subscriptionhrinfo) | Optional. Present **only on `dm` rows** — the counterpart's HR record, resolved from `roomName`. Omitted when the directory lookup degrades. |

```json
{
  "items": [
    {
      "siteId": "site-a",
      "roomId": "01970a4f8c2d7c9aQ",
      "roomName": "rollout",
      "roomType": "channel",
      "threadRoomId": "01970a4f8c2d7c9aTHRD",
      "parentMessageId": "01970a4f8c2d7c9aQRST",
      "hasMention": true,
      "unread": true,
      "lastMsgAt": 1746518400000,
      "tcount": 3,
      "parentMessage": {
        "roomId": "01970a4f8c2d7c9aQ",
        "messageId": "01970a4f8c2d7c9aQRST",
        "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" },
        "msg": "let's discuss the rollout",
        "tcount": 3
      },
      "lastMessage": {
        "roomId": "01970a4f8c2d7c9aQ",
        "messageId": "01970a4f8c2d7c9aWXYZ",
        "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9b", "account": "bob" },
        "msg": "shipping it"
      }
    },
    {
      "siteId": "site-a",
      "roomId": "01970a4f8c2d7c9aDM",
      "roomName": "bob",
      "roomType": "dm",
      "threadRoomId": "01970a4f8c2d7c9aTHR2",
      "parentMessageId": "01970a4f8c2d7c9aPQRS",
      "lastSeenAt": 1746518200000,
      "hasMention": false,
      "unread": false,
      "lastMsgAt": 1746518100000,
      "tcount": 1,
      "parentMessage": {
        "roomId": "01970a4f8c2d7c9aDM",
        "messageId": "01970a4f8c2d7c9aPQRS",
        "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9b", "account": "bob" },
        "msg": "lunch?",
        "tcount": 1
      },
      "lastMessage": {
        "roomId": "01970a4f8c2d7c9aDM",
        "messageId": "01970a4f8c2d7c9aTUVW",
        "sender": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice" },
        "msg": "sure"
      },
      "hrInfo": { "account": "bob", "name": "鮑伯", "engName": "Bob" }
    }
  ],
  "nextCursor": "eyJsYXN0TXNnQXQiOjE3NDY1MTg0MDAwMDAsInRocmVhZFJvb21JZCI6IjAxOTcwYTRmOGMyZDdjOWFUSFJEIn0=",
  "hasNext": true
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference). A malformed `cursor` returns `bad_request`.

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Get Thread Unread Summary

**Subject:** `chat.user.{account}.request.user.{siteID}.thread.unread.summary`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` is the **caller's own home site** — the site that holds the user's federated subscriptions and runs the aggregator.

Returns the aggregated unread status of the user's thread subscriptions across every site the user participates in. `user-service` determines those sites from the user's local thread-subscription replica — joined against the user's subscriptions so a thread is only counted while the user still belongs to its room, and app (`botDM`) threads drop out once the user unsubscribes the app — queries each owning site's `room-service` for the threads' latest activity, and merges the results into one response. `unreadDirectMessage` is classified from the room type on that subscription. Sites that fail to respond are listed in `unavailableSites` rather than failing the request.

##### Request body

Empty object.

```json
{}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `unread` | boolean | Any thread has activity newer than the user's last-seen. |
| `unreadDirectMessage` | boolean | `unread` restricted to DM-room threads. |
| `unreadMention` | boolean | The user is @-mentioned in any unread-tracked thread. |
| `lastMessageAt` | number? | Optional. UnixMilli of the newest thread activity across sites; omitted when none. |
| `unavailableSites` | string[]? | Optional. Sites whose per-site lookup failed; omitted when all responded. |

```json
{
  "unread": true,
  "unreadDirectMessage": false,
  "unreadMention": true,
  "lastMessageAt": 1717000000000
}
```

##### Error response

| Condition | `code` | Notes |
|-----------|--------|-------|
| Internal failure | `internal` | Local thread-subscription read failed. Per-site RPC failures degrade into `unavailableSites` rather than erroring. |

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### Clear All Thread Unread

**Subject:** `chat.user.{account}.request.user.{siteID}.thread.read.all`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` is the **caller's own home site** — the site that holds the user's federated thread subscriptions and runs the aggregator.

Clears the unread status of **all** of the user's threads across every site the user participates in — the server side of a "mark all threads read" action. `user-service` reads the user's local thread-subscription replicas, determines the distinct owning sites, and asks each site's `room-service` to clear that user's thread-subscription read state (`lastSeenAt` advanced, `hasMention` cleared) and, concurrently, `$unset` `threadUnread` on every one of that user's subscriptions that still has unread threads. Sites that fail to respond are listed in `unavailableSites` rather than failing the request.

This is a bulk **dismiss**: it clears only the requesting user's own read state. It does **not** advance thread-room read floors or emit `thread_message_read` receipt events, so other participants' read-receipt UI is unaffected.

##### Request body

Empty object.

```json
{}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `unavailableSites` | string[] | Optional. Sites whose per-site clear failed; their threads may remain unread. Omitted when all responded. |

```json
{}
```

##### Error response

| Condition | `code` | Notes |
|-----------|--------|-------|
| Internal failure | `internal` | Local thread-subscription read failed. Per-site RPC failures degrade into `unavailableSites` rather than erroring. |

##### Triggered events — success path

`None — reply only. No thread_message_read receipt events are emitted (bulk dismiss).`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### sso.set

**Subject:** `chat.user.{account}.request.user.{siteID}.sso.set`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Store (upsert) the caller's own SSO token pair in the site-local MongoDB store. Self-service — the `{account}` subject token is the caller's NATS-JWT-authenticated identity; the frontend calls this on every login. The submitted `ssoToken` is verified against the site's OIDC issuer; its `preferred_username` must equal the caller. The stored expiry is derived server-side from the token's `exp` claim.

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `ssoToken` | string | yes | The SSO token (Keycloak access token) to store. |
| `refreshToken` | string | yes | The matching refresh token. |

```json
{ "ssoToken": "eyJhbGciOiJSUzI1NiIs...", "refreshToken": "eyJhbGciOiJSUzI1NiIs..." }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `success` | boolean | Always `true`. |

```json
{ "success": true }
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| `ssoToken` or `refreshToken` missing | `bad_request` | `missing_fields` | — |
| Token's `preferred_username` ≠ the caller | `bad_request` | — | — |
| Submitted token is expired | `unauthenticated` | `sso_token_expired` | — |
| Submitted token fails verification | `unauthenticated` | `invalid_sso_token` | — |
| SSO is not configured on this site | `unavailable` | `upstream_unavailable` | — |
| Internal failure | `internal` | — | — |

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### sso.refresh

**Subject:** `chat.user.{account}.request.user.{siteID}.sso.refresh`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

Return the caller's stored `ssoToken`, transparently refreshing it against the OIDC issuer when it is within the refresh window (default 1h) of expiry or already expired. Self-service — the `{account}` subject token is the caller's NATS-JWT-authenticated identity. The request body is empty.

##### Request body

None — the request body is empty.

```json
{}
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `ssoToken` | string | The stored or freshly-refreshed SSO token. |

```json
{ "ssoToken": "eyJhbGciOiJSUzI1NiIs..." }
```

##### Error response

| Condition | `code` | `reason` | Notes |
|-----------|--------|----------|-------|
| Refreshed token does not belong to the caller | `unauthenticated` | `sso_token_expired` | Stored refresh token minted a different identity; client re-logins (re-runs `sso.set`) |
| No token pair stored for the caller | `not_found` | `sso_token_not_found` | — |
| Token needed refreshing and the refresh failed (re-login) | `unauthenticated` | `sso_token_expired` | — |
| SSO is not configured on this site | `unavailable` | `upstream_unavailable` | — |
| Internal failure | `internal` | — | — |

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

### 3.5 media-service

| RPC subject | Method |
|---|---|
| `chat.user.{account}.request.emoji.{siteID}.list` | [`emoji.list`](#emojilist--list-a-sites-custom-emoji) |
| `chat.user.{account}.request.emoji.{siteID}.delete` | [`emoji.delete`](#emojidelete--delete-a-custom-emoji) |

#### `emoji.list` — list a site's custom emoji

**Subject:** `chat.user.{account}.request.emoji.{siteID}.list`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

**Auth:** the `{account}` in the subject is the authenticated identity. `{siteID}` is the **target site whose emoji set you want** — in v1 the FE fetches only its **local** site's list (non-local shortcodes are not rendered). The supercluster routes the request to that site's media-service.

##### Request body

Empty. Send `{}` or no payload.

##### Success response

| Field | Type | Notes |
|---|---|---|
| `emojis` | [EmojiEntry](#emojientry)[] | Sorted by `shortcode`. `[]` when the site has none. |

###### EmojiEntry

| Field | Type | Notes |
|---|---|---|
| `shortcode` | string | Bare shortcode (no colons), `^[a-z0-9_+-]{1,32}$`. |
| `imageUrl` | string | Bare relative serve path `/api/v1/emoji/{shortcode}` — resolve against the media-service base URL of the site the list came from (no `?siteid=`; the serve endpoint defaults to its own cluster's site). Append `?v={etag}` to cache-bust. |
| `contentType` | string | `image/png`, `image/jpeg`, or `image/gif` (GIFs may be animated). |
| `etag` | string | Storage ETag of the current image. |
| `updatedAt` | string (RFC 3339) | Time of the last upload (UTC). |

```json
{
  "emojis": [
    {
      "shortcode": "acme_party",
      "imageUrl": "/api/v1/emoji/acme_party",
      "contentType": "image/gif",
      "etag": "9a0364b9e99bb480dd25e1f0284c8555",
      "updatedAt": "2025-05-06T08:08:20Z"
    }
  ]
}
```

##### Error response

See [Error envelope](#6-error-envelope-reference).

| Code | Reason |
|---|---|
| `internal` | Store failure. |

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

#### `emoji.delete` — delete a custom emoji

**Subject:** `chat.user.{account}.request.emoji.{siteID}.delete`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

**Auth:** the `{account}` in the subject is the authenticated identity. Any authenticated user may delete (v1). `{siteID}` targets the owning site. Disabled by default — gated by media-service's `EMOJI_DELETE_ENABLED` (default `false`).

##### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `shortcode` | string | yes | Bare shortcode of the emoji to delete. |

```json
{ "shortcode": "acme_party" }
```

##### Success response

| Field | Type | Notes |
|---|---|---|
| `shortcode` | string | Canonical (NFC) form of the deleted shortcode. |
| `deleted` | boolean | Always `true` on success. |

```json
{ "shortcode": "acme_party", "deleted": true }
```

Existing reactions that reference the deleted shortcode are not rewritten. Reactions validate shortcode **format only** (no registration check), so a deleted shortcode technically remains reactable — the FE stops offering it once its emoji list refresh no longer includes it, and its image URL returns `404`.

##### Error response

See [Error envelope](#6-error-envelope-reference).

| Code | Reason |
|---|---|
| `bad_request` | `shortcode` is missing or fails `^[a-z0-9_+-]{1,32}$` after NFC. |
| `not_found` | No custom emoji with that shortcode on this site. |
| `forbidden` + reason `emoji_delete_disabled` | The delete RPC is switched off (EMOJI_DELETE_ENABLED=false, the default). |
| `internal` | Store failure. |

##### Triggered events — success path

`None — reply only.`

##### Triggered events — error path

`None — error returned only via the reply subject.`

---

### 3.6 translation-service

#### Translate Text

**Subject:** `chat.user.{account}.request.translate.{siteID}.text`
**Reply subject:** auto-generated `chat.user.{account}.>` (NATS request/reply)

- `{siteID}` is the caller's **own (local) site ID** — the local `translation-service` handles the request. Translation is stateless and not federated across sites, so unlike `msg.send` there is no origin-site rule: always use your own site.

Synchronous RPC. `translation-service` validates the request, calls the translation backend, and replies with a `TranslateResult` (the translated text) on success, or the standard [error envelope](#6-error-envelope-reference) on failure. Under handler saturation the router replies `unavailable` (`"service busy"`) so the client can retry.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `text` | string | yes | The text to translate. No length cap is enforced by the service. |
| `targetLang` | string | yes | Target language as a **BCP-47 tag** — send the user's [`settings.translateMessageInto`](#settingsget) value unchanged (no client-side conversion). See [Supported languages](#supported-languages). |

##### Supported languages

`targetLang` is a BCP-47 tag matched **case-insensitively** and tolerant of region/script subtags, so the value stored in `settings.translateMessageInto` is sent as-is. It resolves to one of five backend languages:

| Language | Example tags that resolve | Resolves to |
|---|---|---|
| English | `en`, `en-US`, `en-GB` | English |
| German | `de`, `de-DE` | German |
| Japanese | `ja`, `ja-JP` | Japanese |
| Traditional Chinese | `zh-Hant-TW`, `zh-Hant`, `zh-TW`, `zh-HK` | Traditional Chinese |
| Simplified Chinese | `zh-Hans-CN`, `zh-Hans`, `zh-CN`, `zh-SG` | Simplified Chinese |

Chinese resolves by script (`Hant`/`Hans`) or, absent a script, by region. A bare `zh` (no script or region) is ambiguous and rejected as `unsupported_lang`, as is `""` (translation off — the client should not send a request) and any language outside the five above (e.g. `fr`, `ko`). The result's `targetLang` echoes the tag you sent, not the resolved language.

> **Breaking change:** `targetLang` now requires a BCP-47 tag. The former backend short codes `zhTW` / `zhCN` are **no longer accepted** and return `unsupported_lang` — send the BCP-47 form (`zh-Hant-TW` / `zh-Hans-CN`, or the user's `settings.translateMessageInto`) instead. (`en` / `de` / `ja` are unaffected, being valid BCP-47 tags already.) Deploy this in lockstep with the client that sends the settings value.

#### Success response — `TranslateResult`

| Field | Type | Notes |
|---|---|---|
| `translatedText` | string | The translated text. |
| `targetLang` | string | Echoes the request `targetLang` (the BCP-47 tag the client sent), not the resolved backend language. |

```json
{
  "translatedText": "你好 世界",
  "targetLang": "zh-Hant-TW"
}
```

#### Error response

See [Error envelope](#6-error-envelope-reference). The reply carries the `{ code, reason?, error }` envelope:

| Code | Reason | When |
|---|---|---|
| `bad_request` | `empty_text` | `text` is empty. |
| `bad_request` | `unsupported_lang` | `targetLang` does not resolve to a supported language (outside the [Supported languages](#supported-languages) set, or a bare `zh` with no script/region). |
| `unavailable` | — | Handler saturation — the concurrency cap is full; retry. |
| `unavailable` | `upstream_unavailable` | The third-party translation backend returned a 5XX or was unreachable (transport failure). Clients should show a "translation service temporarily unavailable" message and allow a retry. |
| `too_many_requests` | `rate_limited` | The third-party translation backend rate-limited the request (HTTP 429). The service does **not** retry; clients should back off and retry later. |
| `internal` | — | Other translation backend failure (e.g. malformed stream, other 4XX, non-success returnCode). The raw cause is logged server-side, never returned. |

```json
{
  "code": "bad_request",
  "reason": "unsupported_lang",
  "error": "unsupported targetLang"
}
```

##### Triggered events

`None — the reply is the only output.`

---

## 4. Message Send

### Send Message

**Subject:** `chat.user.{account}.room.{roomID}.{siteID}.msg.send`
**Reply subject:** `chat.user.{account}.response.{requestID}` — the client must subscribe to `chat.user.{account}.>` (the user wildcard) to receive it. The `{requestID}` value is the `requestId` field from the request body.

- `{siteID}` must be the room's **origin `siteID`** (the site that owns the room), not the caller's own site.

This RPC uses the **publish + async-reply** pattern, not the standard NATS request/reply. The client publishes to the `msg.send` subject and expects no synchronous request/reply response. `message-gatekeeper` validates the request, publishes the canonical message to `MESSAGES-CANONICAL`, and replies to `chat.user.{account}.response.{requestID}` with the persisted `Message` (or an error envelope on failure).

The same subject and request body cover three send variants: plain message, thread reply, and quoted message. The variant is determined by which optional fields are set.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `id` | string | yes | The message's ID. Must be 20-char base62. The client generates this and uses it for client-side optimistic rendering. |
| `content` | string | yes* | The message body, ≤ 20 KiB. *Required unless `attachments` is present — a message with attachments may have empty `content`. |
| `requestId` | string | yes | A 36-char hyphenated UUID (v4 or v7) the client generates. **Validated** — an empty or malformed `requestId` is rejected with no message published. The async reply is delivered to `chat.user.{account}.response.{requestId}`. |
| `attachments` | string[] | no | Optional. Each entry is base64-encoded bytes — the JSON of one [Attachment](#attachment) from the upload endpoint ([§2.3](#23-http--protected-image-uploaddownload)). Max 1 entry, ≤ 8 KiB total. Stored opaquely and returned **decoded** (as `Attachment[]`) in message payloads. |
| `threadParentMessageId` | string | no | Set when posting a thread reply. Must be a valid 20-char base62 message ID. |
| `tshow` | boolean | no | The "Also send to channel" option. Only meaningful on a thread reply (`threadParentMessageId` set): the reply is persisted into the parent room's channel timeline as well as the thread (dual-write into `messages_by_room` in addition to `thread_messages_by_thread` + `messages_by_id`), and is surfaced with `tshow: true` on the persisted message. On a non-thread send the flag is **ignored and normalized to `false`** — the request is not rejected. |
| `quotedParentMessageId` | string | no | Set when posting a quoted message. The gatekeeper fetches the authoritative parent snapshot from message history and embeds it in the persisted message. If that fetch fails *transiently* (history briefly unavailable), the message is not dropped: the gatekeeper inserts a placeholder snapshot for live delivery (body `"Content temporarily unavailable"`), and `message-worker` re-projects the authoritative snapshot (or drops the quote) from history before the durable write, so the placeholder never persists. A genuinely missing/forbidden parent is still rejected. |
| `type` | string | no | Optional client-settable message type. The only accepted value is `"important"` (an important message — previews and notifies like a normal message). Any other value, including a system type (`room_created`, etc.), is rejected with `bad_request`. Omitted = a normal message. |
| `visibleTo` | string | no | Optional opaque visibility marker, ≤ 4096 bytes. Stored and surfaced verbatim (on history reads and the room-list preview); the server never interprets it or filters delivery, reads, or previews on it — the client interprets visibility. Oversize is rejected with `bad_request`. |

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
  "threadParentMessageId": "01970a4f8c2d7c9aQRST"
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

The client sends `quotedParentMessageId`; the server fetches and embeds the authoritative quoted-parent snapshot. During a transient history outage the server fills the live copy with a `"Content temporarily unavailable"` placeholder and re-projects the authoritative quote (or drops it) before persisting, so the placeholder never persists.

##### Thread reply quoting the thread-starter

A thread reply may quote the thread's own parent message (the message that started the thread) by setting both `threadParentMessageId` and `quotedParentMessageId` to the same ID. The quoted snapshot is embedded in the response like any other quote.

```json
{
  "id": "01970a4f8c2d7c9aQUV2",
  "content": "to your original point…",
  "requestId": "01970a4f-8c2d-7c9a-abcd-e0123456789c",
  "threadParentMessageId": "01970a4f8c2d7c9aQRST",
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
| `threadParentMessageCreatedAt` | string | Optional. RFC 3339. Server-resolved best-effort for a thread reply; absent when the parent's createdAt could not be resolved at send time. |
| `tshow` | boolean | Present only when the request set `tshow: true` on a thread reply (absent when the flag was normalized away on a non-thread send). |
| `quotedParentMessage` | [QuotedParentMessage](#quotedparentmessage) | Present only for a quoted send — the server-fetched snapshot of the quoted parent. |
| `visibleTo` | string | Present only when the request set `visibleTo` — echoed verbatim. |

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
| `invalid message type "…"` | `bad_request` | — | `type` is set to something other than `"important"` (e.g. a system type). |
| `content exceeds maximum size of 20480 bytes` | `bad_request` | — | `content` > 20 KiB. |
| `visibleTo exceeds maximum size of 4096 bytes` | `bad_request` | — | `visibleTo` > 4096 bytes. |
| `not subscribed` | `forbidden` | `not_subscribed` | Sender is not a member of the room. |
| `posting is restricted to owners and admins in this room` | `forbidden` | `large_room_post_restricted` | Non-owner/admin/bot posting a top-level message in a room above the large-room threshold (thread replies are exempt). |
| `quoted parent {id} not found` | `not_found` | — | The quoted message lookup failed (deleted, cross-room, …). |
| `quoted parent {id} thread context mismatch: …` | `bad_request` | — | A quoted message must be in the same thread context (main-room or the same thread) as the new message — except a `tshow: true` thread reply, which may also be quoted from its parent channel room. |

**Delivery guarantee:** every validation/authorization failure — including a `siteID` mismatch and a malformed `msg.send` subject — is replied to the client on the response subject and the JetStream message is acked (not retried). The error reply requires a routable response subject, so it can only be sent when the `{account}` segment is recoverable from the subject and the payload carries a valid hyphenated-UUID `requestId`; if neither is recoverable (a truly malformed subject or missing/invalid `requestId`) no reply is possible and the client falls back to a request timeout. **Only infrastructure failures** (store/publish errors) are nak'd and **redelivered by JetStream** — these produce no immediate reply.

```json
{ "code": "bad_request", "error": "content must not be empty" }
```

#### Triggered events — success path

After a successful send, `broadcast-worker` fans out a `RoomEvent`. The subject depends on room type. A thread reply (`threadParentMessageId` set) publishes `type: "new_thread_message"` instead of `"new_message"` — same `RoomEvent` shape but a **different delivery path** (per-subscriber on `chat.user.{account}.event.room`, not the room subject), see [events.md#new_thread_message-roomevent](client-api/events.md#new_thread_message-roomevent). **A `botDM` fans out to its human participant, not the bot:** `broadcast-worker` handles `botDM` via the same DM path (`publishDMEvents`) — it publishes the `RoomEvent` to each **non-bot** member on `chat.user.{account}.event.room` and skips the bot account (`isBot`). This applies to both an ordinary `new_message` and a thread reply's `new_thread_message`. (The bot side consumes messages through a separate backend path.)

**1. For channel rooms — `chat.room.{roomID}.event`** (`publishChannelEvent`)

A `RoomEvent`. Recipients: every client subscribed to the room (which includes the sender, since the sender is also a member).

| Field | Type | Notes |
|---|---|---|
| `type` | string | `"new_message"` for this room-wide channel path. A thread reply carries `"new_thread_message"` instead and is delivered per-subscriber (see [new_thread_message](client-api/events.md#new_thread_message-roomevent)). |
| `roomId` | string | |
| `timestamp` | number | Epoch ms (UTC). Event publish time. |
| `eventTimestamp` | number | Milliseconds since Unix epoch (UTC). When message-worker published the canonical event. Omitted for legacy events. |
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
| `attachments` | [Attachment](#attachment)[] | Optional. Decoded attachment objects (same shape as history). |
| `card` | [MessageCard](#messagecard) | Optional. |
| `cardAction` | [MessageCardAction](#messagecardaction) | Optional. |
| `mentions` | [Participant](#participant)[] | Optional. |
| `createdAt` | string | RFC 3339. |
| `editedAt` | string | Optional. RFC 3339. |
| `updatedAt` | string | Optional. RFC 3339. |
| `threadParentMessageId` | string | Optional. Set for a thread reply. |
| `threadParentMessageCreatedAt` | string | Optional. RFC 3339. Server-resolved best-effort; absent when unresolved at send time. |
| `tshow` | boolean | Optional. Whether a thread reply is also shown in the parent room. |
| `type` | string | Optional. System-message type when set. |
| `sysMsgData` | string | Optional. Base64-encoded JSON payload for system messages; shape depends on `type` (see [System-message `sysMsgData` payloads](#system-message-sysmsgdata-payloads)). |
| `quotedParentMessage` | [QuotedParentMessage](#quotedparentmessage) | Optional. |
| `pinnedAt` | string | Optional. RFC 3339. |
| `pinnedBy` | [Participant](#participant) | Optional. |
| `truncated` | boolean | Optional. `true` when the server blanked this row to make the page fit — either because the row alone exceeded the transport's `max_payload`, or because it shares a `createdAt` millisecond with such a row and the whole group had to be returned together. `msg`, `mentions`, `attachments`, `card`, `cardAction`, `quotedParentMessage`, `reactions`, `sysMsgData`, `encPayload` and `encMeta` are cleared; identifiers, `sender`, `createdAt` and `type` are retained so a placeholder can be rendered. Absent on every ordinary row. |

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

**2. For DM rooms — `chat.user.{recipient}.event.room`** (`publishDMEvents`)

A `RoomEvent` (same struct as above) published once per DM participant. Recipients: each user subscribed to the DM. The `hasMention` field is set per-recipient. DM events carry `message` (plaintext); they do not use `encryptedMessage`.

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

**Thread replies additionally emit a `ThreadMetadataUpdatedEvent`** (see [§4.1 Thread Metadata Event](#41-thread-metadata-event)) to update the parent message's reply-count badge. This event is published to all room members (not only thread subscribers) so every client can show the correct badge without subscribing to the thread.

**Channel thread replies are additionally published on the thread-scoped subject** (see [§4.2 Thread View Subject](#42-thread-view-subject)), so a client with the thread panel open receives the reply even when it does not follow the thread.

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
- Presence-busy / in-call recipients are not pushed unless they set
  `showNotificationsInCall`; everyone else (online, offline, away, missing)
  receives one.
- A sender in the recipient's `priorityContacts` bypasses `muteAllNotifications`
  and presence suppression alike, but only when the recipient enabled
  `alwaysAllowPriorityNotifications`. Per-room mute is never bypassed.

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
| `newTcount` | number | Authoritative exact reply count for the parent message. Replaces any locally-computed count — do not delta. |
| `newThreadLastMsgAt` | string (ISO 8601) | Optional. Timestamp of the most recent surviving thread reply. Absent when `newTcount` is 0 (all replies deleted). |
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
  "newThreadLastMsgAt": "2026-06-18T10:00:00Z",
  "action": "reply_added",
  "replyMessageId": "01970a4f8c2d7c9aUVWX",
  "timestamp": 1746518100123
}
```

#### When it fires

- **Reply added (`action: "reply_added"`):** fired when a new thread reply is successfully persisted (triggered by a `Send Message` RPC with `threadParentId` set). Published in addition to the per-subscriber `new_thread_message` `RoomEvent` that carries the reply content.
- **Reply deleted (`action: "reply_deleted"`):** fired when a thread reply is soft-deleted (triggered by a `Delete Message` RPC). Published in addition to the `DeleteRoomEvent` that carries the delete notification.

#### Client handling

Apply `newTcount` directly to the parent message's badge — do not compute a delta. Apply `newThreadLastMsgAt` to the parent message's thread-freshness timestamp (or clear it when absent). Events for the same parent may arrive out of order due to JetStream redelivery; when `eventTimestamp` is present, prefer the event with the larger `eventTimestamp`. Fall back to `timestamp` only for legacy events that omit `eventTimestamp`.

---

## 4.2 Thread View Subject

A **channel** thread reply fans out per-subscriber (see [`new_thread_message`](#send-message)), so a client that opens a thread panel without following the thread receives nothing until it refetches. `broadcast-worker` therefore publishes the same events a second time on a thread-scoped subject that a client subscribes to for exactly as long as the panel is open.

### Subjects

| Room `crossSite` | Subject |
|---|---|
| `true` / absent | `chat.room.{roomID}.thread.{parentMessageID}.event` |
| `false` | `chat.local.room.{roomID}.thread.{parentMessageID}.event` |

Resolve `crossSite` exactly as for the room's own `chat.room.{roomID}.event` subject: only an explicit `false` selects the local namespace. A room that has just flipped local→global publishes to both for the transition grace window.

Channel rooms only. DM and botDM thread replies already reach every member, so no thread subject is published for them.

### Events carried

| Type | Payload |
|---|---|
| `new_thread_message` | The `RoomEvent` of [Send Message](#send-message) |
| `message_edited` | The `EditRoomEvent` of [Edit Message](#edit-message) |
| `message_deleted` | The `DeleteRoomEvent` of [Delete Message](#delete-message) |

**The body is encrypted on this subject, unlike the per-subscriber copy.** In an encrypted channel the per-subscriber copy on `chat.user.{account}.event.room` carries a plaintext `message` / `newContent`, because that subject is scoped to one account. The thread subject sits in the room namespace, so its copy carries `encryptedMessage` / `encryptedNewContent` instead — decrypt with the room key exactly as for `chat.room.{roomID}.event`, see [§5 Room Encryption](#5-room-encryption). In an unencrypted channel both copies are identical and plaintext. `message_deleted` carries no body and is never encrypted.

If the body cannot be sealed, nothing is published on this subject — the lane fails closed rather than emitting a plaintext body into the room namespace.

### Client handling

- **Subscribe before fetching.** Open the subscription, then call [Get Thread Messages](#get-thread-messages), then merge. Fetching first leaves a window in which a reply is published before the subscription exists and is lost.
- **Unsubscribe when the panel closes**, when it switches to another parent, and on teardown. The subscription is the only thing that makes the server deliver here, so a leaked one keeps consuming events.
- **Deduplicate replies by message ID, and apply edits and deletes unconditionally.** A thread follower with the panel open receives every event twice — once per-subscriber, once here. The two are the **same logical event, not the same payload**: in an encrypted channel this lane carries `encryptedMessage` while the per-subscriber lane carries a plaintext `message`, so normalize to the decrypted body before comparing. Suppress a `new_thread_message` whose ID is already rendered; do **not** suppress `message_edited` / `message_deleted` on that basis, or a later edit of an already-seen reply is dropped. Both mutations are idempotent, so applying a duplicate is a no-op.
- **Let a decrypted body replace a placeholder, never the reverse.** If you rendered a placeholder for one lane's copy because the key had not arrived, the other lane's copy of the same ID may still be readable. Deduplicating on ID alone leaves whichever arrived first in place, which can pin a placeholder over a body you could have shown.
- **Reject an edit no newer than the one already applied.** Arrival order is not causal order: a redelivered older `message_edited` would otherwise overwrite a newer one. Compare `editedAt`, the domain edit time, and ignore an edit at or before the applied one.
- **Render a placeholder when the room key has not arrived.** An `encryptedMessage` you cannot open still carries `lastMsgId` and `lastMsgAt`; show a placeholder from those rather than dropping the event, or the panel is silently missing a reply.
- **Process events for one thread in arrival order.** In an encrypted room a plaintext `message_deleted` resolves faster than a preceding `new_thread_message` that must be decrypted first. A client that handles events concurrently can apply the delete to a reply it has not inserted yet, drop it, and then render the reply as live. Serialize the handler per thread — per thread, not globally, or a decrypt stalled on a key fetch for one thread delays the thread the user opens next.
- **Delivery is best-effort.** A publish failure on this subject is not retried; the panel's next open refetches. Followers' per-subscriber delivery is unaffected.

---

## 5. Room Encryption

Channel messages can be end-to-end encrypted. The key material reaches clients two ways: **inline** on the `added` `subscription.update` event (`subscription.room.privateKey` / `keyVersion` — Create Room and Add Members) and on `subscription.list`, and as live `RoomKeyEvent`s **on key rotation** (Remove Member — see its "Triggered events" section). This section describes the rotation event payload and how a client uses the key to decrypt.

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

1. Store every key under `(roomId, version) → privateKey`, whatever the delivery path: `subscription.room.privateKey`/`keyVersion` on an `added` `subscription.update` or a `subscription.list` row, or a live `RoomKeyEvent` (rotation).
2. To decrypt an incoming `encryptedMessage` payload:
   - Look up `privateKey` for `(roomId, encryptedMessage.version)`.
   - Use the 32-byte `privateKey` directly as the AES-256-GCM key (no key derivation step).
   - Decrypt: `AES-GCM-Decrypt(privateKey, nonce, ciphertext, aad=empty)`. The ciphertext already includes the 16-byte GCM tag at the end (Go `cipher.AEAD.Seal` format).
   - **The cipher is identical for both event kinds (same `roomcrypto` AES-256-GCM seal); only the plaintext payload differs**, because each event encrypts exactly what the client needs:
     - **`encryptedMessage`** (new message) decrypts to a UTF-8-encoded JSON `ClientMessage` — a brand-new message the client has never seen, so the whole object (sender, timestamps, thread/quote fields) is sealed.
     - **`encryptedNewContent`** (edit) decrypts to a plain UTF-8 content **string** — the client already has the original message rendered, and an edit only replaces its `content`, so just the new body is sealed (the surrounding message metadata is unchanged and already known).
3. Retain past versions to support history scrolling. The server retains the previous version in its store for at least `ROOM_KEY_GRACE_PERIOD` (default 24h); after that, server-side decryption of old messages may not be possible, but clients holding old keys can still decrypt locally.

#### When clients receive `RoomKeyEvent`s

- **Remove member (channels only):** the server rotates the room key. Surviving members receive a new `RoomKeyEvent` with an incremented `version`. The removed account stops receiving events for the room.
- **Room creation / Add member no longer fire `RoomKeyEvent`s.** The initial key reaches each newly-subscribed member inline on their `added` `subscription.update` (`subscription.room.privateKey` / `keyVersion`). DM/botDM rooms carry no key at all.
- **Bot members** are key-holders like any member. A bot's events land on its **encoded** per-user subject (a dotted `.bot` account maps to a single NATS subject token — the form its JWT is scoped to): the `added` `subscription.update` carrying the initial key, and rotation `RoomKeyEvent`s afterwards (a bot can log into the chat frontend).

Removed members keep prior keys for decrypting historical messages but cannot decrypt anything published after the rotation.

**Initial key bootstrap on (re)connect:** live `RoomKeyEvent`s fire only on rotation. The initial set of keys for rooms the client is already subscribed to is delivered by the user-service subscription endpoints as `room.privateKey` / `room.keyVersion` on each enriched subscription (see §3.4 and [SubscriptionRoom](#subscriptionroom)); a room joined mid-session delivers its key the same way on the `added` `subscription.update`. Live rotation events keep the client current after bootstrap.

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
| Key not held (rolled past grace window, or never existed) | `room key not available` | Includes "explicit version resolvable in neither the previous-key slot nor the retired-key archive". |
| Malformed request body | `invalid request: …` | |
| Internal failure | `internal error` | |

#### Use as complement to live events

This RPC complements — it does not replace — the primary delivery
channels: the key inline on the `added` `subscription.update` /
`subscription.list` (create, add, bootstrap) and live `RoomKeyEvent`s on
`chat.user.{account}.event.room.key` (rotation). Clients
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

> [!IMPORTANT]
> **Oversize replies.** If a reply would exceed the transport's maximum payload size, it is returned as `code: internal` with `reason: response_too_large` instead. This covers both a success body that is too large and an error envelope that is too large; either way the caller gets an answer rather than a timeout. This is most likely on large history reads (e.g. Load History / Load Next / Load Surrounding / Get Thread Messages with a high `limit`); the client should retry with a smaller `limit`. Branch on `reason` (`response_too_large`), not the message text. Which RPCs can raise it, and where the smaller-`limit` retry is the wrong move, is set out under **A page may be shorter than `limit`** below.


> [!IMPORTANT]
> **A page may be shorter than `limit`.** `limit` is a maximum, never a guarantee. Load History, Load Surrounding and Thread List size each page against the transport's `max_payload` and return fewer rows when the full page would not fit. The "more" flag is authoritative — `hasNext` for Load History and Thread List, `moreBefore` / `moreAfter` for Load Surrounding. Page until the flag clears; never treat a short page as the end of the collection, and never assume `len(items) < limit` means there is nothing more.
>
> **`sizeLimited` says a page was cut for bytes**, which a short page alone cannot: the history walk and per-site limits return short pages too. Branch on the flag, and set the next request's `limit` to the row count you just received — the server already sized that page to fit, so it converges in one round trip where halving takes several, and it stops the server re-reading the rows it dropped.
>
> Trimming is server-side and can be switched off per service (`PAGE_TRIMMING_ENABLED=false`). With it off these RPCs behave like the ones below: the page ships whole, an oversize one returns `internal` / `response_too_large`, and `sizeLimited` is never set.
>
> A single row too large to ship inside a page is returned blanked with `truncated: true` rather than dropped, so pagination always advances past it. A Thread List row too large to send has its `parentMessage` and `lastMessage` bodies dropped and is marked `truncated: true`; its identifiers and `lastMsgAt` are kept, so the cursor still advances.
>
> Load History and Load Surrounding can still return `internal` / `response_too_large` in one case: rows sharing a single `createdAt` millisecond are never split, because the resume bound is exclusive and a cut inside the millisecond would skip the rest of that group for good. If such a group exceeds the budget even with every row blanked, it is returned whole and the transport rejects the reply — a visible error in preference to a silent gap. Reaching this needs a full page of rows in one millisecond in one room against a `max_payload` far below any deployed value. Retrying with a smaller `limit` makes the reply deliverable but drops the rest of the group, so treat it as a server-side misconfiguration, not a client-recoverable error.
>
> Load Next, List Pinned and Get Thread Messages are **not** sized this way — their cursors are opaque and cannot be re-derived after trimming — so they still return `internal` / `response_too_large` on an oversize page. Retry those with a smaller `limit`.

### `reason` catalog (present today)

| `reason` | Typical `code` | Emitted by |
|---|---|---|
| `max_room_size_reached` | conflict | room-service create/add (room capacity exceeded) |
| `not_room_member` | forbidden | room-service / room-worker (actor not a member) |
| `not_room_owner` | forbidden | room-service role/admin paths |
| `last_owner_cannot_leave` | conflict | room-service leave |
| `bot_in_channel` | bad_request | room-service create-channel (bot listed in a channel create request) |
| `bot_not_available` | not_found | room-service member-add (bot with no app record or disabled assistant) and create-botDM (assistant disabled) |
| `bot_cannot_be_owner` | bad_request | room-service role-update (promote a bot to owner) |
| `user_not_found` | not_found | room-service / room-worker (account does not resolve to a user) |
| `invalid_org` | bad_request | room-service create/add (orgId does not resolve to any users) |
| `self_dm` | bad_request | room-service create (DM to yourself) |
| `last_member_cannot_remove` | conflict | room-service remove-member (would remove the last human member; bots don't count) |
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
| `sso_token_expired` | unauthenticated | auth-service `POST /api/v1/auth`; user-service `sso.set` (submitted token expired), `sso.refresh` (refresh failed, re-login required) |
| `invalid_sso_token` | unauthenticated | auth-service `POST /api/v1/auth`; user-service `sso.set` (submitted token fails verification) |
| `upstream_unavailable` | unavailable | auth-service `POST /api/v1/auth` (cannot reach botplatform); portal-service `GET /api/userInfo` (cannot reach home-site botplatform); user-service `sso.set`/`sso.refresh` (SSO not configured on this site); upload-service (§2.4) and media-service (§7) — a session token could not be validated against botplatform |
| `invalid_request` | bad_request | auth-service (body parse / required field missing) |
| `invalid_nkey` | bad_request | auth-service (natsPublicKey format) |
| `missing_fields` | bad_request | auth-service (ssoToken/account/natsPublicKey missing); portal-service `GET /api/userInfo` (account missing); user-service `sso.set` (ssoToken/refreshToken missing) |
| `account_not_ready` | forbidden | portal-service `GET /api/userInfo` (account absent from the HR directory cache, or not provisioned in the users collection) |
| `bot_login_disabled` | forbidden | portal-service `POST /api/v1/login` (§2.5) — portal is configured (`BOT_LOGIN_ENABLED=false`) to reject bot-account logins. Direct the user to the bot developer console. |
| `app_not_found` | not_found | user-service `subscription.setAppSubscription` (appId does not resolve to any app) |
| `app_disabled` | bad_request | user-service `subscription.setAppSubscription` (app exists but has no assistant) |
| `subscription_not_found` | not_found | user-service `subscription.getDM` (no DM subscription exists for the account pair) |
| `sso_token_not_found` | not_found | user-service `sso.refresh` (no token pair stored for the caller) |
| `priority_contact_limit` | forbidden | user-service `settings.priorityContacts.add` (list already at the 30-entry cap and the contact is not already on it) |
| `priority_contact_not_found` | not_found | user-service `settings.priorityContacts.add` (contact account does not resolve — inactive/unknown user, or app not found for a `.bot` account) |
| `response_too_large` | internal | any RPC whose reply would exceed the transport `max_payload` (most often large history reads — retry with a smaller `limit`) |
| `no_responders` | unavailable | any request/reply RPC whose upstream subject had no subscriber (`natsutil.RequestFailure`) — the upstream service is down, not yet started, or not routed to this site. Retry. |
| `upstream_timeout` | unavailable | any request/reply RPC delivered to the upstream but not answered within the caller's timeout (`natsutil.RequestFailure`). Retry. |
| `invalid_token` | unauthenticated | admin-service (every `Authorization: Bearer` route — §9.11 and all `/v1/admin/…`; token missing, unknown, or session not found), botplatform-service (session token failed validation, §10); upload-service (§2.4) and media-service (§7) — `x-user-id`/`x-auth-token` missing, unknown, or disagreeing with the session (deliberately indistinguishable) |
| `not_admin` | forbidden | admin-service (valid session, but caller does not hold the `admin` role or the session site does not match); media-service (§7) — avatar PUT by a session that is neither the named bot nor an admin, emoji PUT without the `admin` role, or either PUT with a session issued for another site |
| `ambiguous_token` | bad_request | auth-service `POST /api/v1/auth` (§2.2) — both `ssoToken` and `authToken` set; upload-service (§2.4) — both an `ssoToken` header and an `x-auth-token` header set |
| `account_exists` | conflict | admin-service `POST /v1/admin/users` (account already exists in the users collection) |
| `invalid_credentials` | unauthenticated | admin-service `POST /v1/login` (§9.10) (unknown account, wrong password, not admin, or deactivated — uniform response) |
| `old_password_mismatch` | unauthenticated | admin-service `POST /v1/password/change` (§9.11) (`oldPassword` does not match) |
| `unknown_permission` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13), `GET /v1/admin/permissions` (§9.14), `POST /v1/admin/permissions/resync` (§9.15) (permission key not recognized) |
| `invalid_subject_count` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (`subjectAccounts` empty), `POST /v1/admin/permissions/resync` (§9.15) (`accounts` empty) |
| `invalid_reason` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (`reason` over 1000 runes) |
| `missing_permission_fields` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (`granted` omitted, or `applicantAccount`/`approverAccount` empty) |
| `invalid_permission_window` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (grant only: malformed date, `effectiveFrom` after `expiresAt`, or `expiresAt` not in the future) |
| `unexpected_permission_window` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (revoke only: `effectiveFrom`/`expiresAt` present with a non-null value; an explicit `null` counts as absent) |
| `inactive_subject` | bad_request | admin-service `POST /v1/admin/permissions` (§9.13) (a subject account is deactivated) |
| `unknown_accounts` | not_found | admin-service `POST /v1/admin/permissions` (§9.13) (a subject, applicant, or approver account does not exist; the lookup is company-wide, not filtered by site) |
| `emoji_shortcode_reserved` | bad_request | media-service `PUT /api/v1/emoji/…` (shortcode collides with a built-in standard emoji) |
| `emoji_delete_disabled` | forbidden | media-service `emoji.delete` (kill-switch `EMOJI_DELETE_ENABLED=false`, the default) |

### Where envelopes are sent

- **NATS sync replies** — on the reply subject for §3/§4 RPCs.
- **JetStream async results** — `model.AsyncJobResult` carries the same `code` + `reason` fields when `status == "error"`, so a failed async job is surfaced the same way as a sync error.
- **HTTP** — auth-service `POST /api/v1/auth` (§2.2), portal-service `GET /api/userInfo` (§2.3), upload-service's file/image endpoints (§2.4), and media-service's avatar and emoji PUTs (§7) write the envelope as the response body with the matching HTTP status from the table above.

### Client branching guidance

Compute the trigger as `reason ?? code` and branch on that. Use `code` for generic copy ("you don't have permission", "service unavailable, try again"), `reason` for endpoint-specific UX (open the "room is full" dialog on `max_room_size_reached`; redirect to re-login on `sso_token_expired`/`invalid_sso_token`; surface "join the room first" on `not_subscribed`). Never branch on the `error` text — message wording can change without notice.

---

## 7. Media Service

HTTP endpoints served by `media-service` — the three GETs are public, the two PUTs require a session token (see each). GET image responses (streamed custom image and generated default SVG) set `X-Content-Type-Options: nosniff` and `Content-Security-Policy: default-src 'none'`; redirects do not, and the upload sets `nosniff` only.

**Bot detection:** an account takes the bot avatar path if it ends in `.bot` **or** is the `p_admin` platform-admin pseudo-account. Everything else — including plain `p_` QA test accounts — is a user.

**Default image:** when no custom image exists (and for users with no `employeeId`), the service returns a deterministic SVG "initials" avatar (`Content-Type: image/svg+xml`) generated on the fly — never stored. The SVG is cacheable: it carries a stable `ETag` and `Cache-Control: public, max-age=<cfg>`.

**Default-avatar toggle:** the generated default is gated by `DEFAULT_AVATAR_ENABLED` (default `true`). When disabled, every avatar endpoint returns `404 not_found` instead of synthesizing the SVG (for rooms, users with no `employeeId`, and bots), and the client MUST render its own fallback on image-load failure — the same `<img onerror>` contract as the employee-photo `404` case below. Both the SVG and the disabled `404` carry the same `Cache-Control: public, max-age=<cfg>`, so **a toggle change propagates to shared/browser caches within that window** (default 6h); purge those caches to apply it immediately.

**Frontend-default contract for user avatars:** after a `307` to the employee-photo host, a user who has an `employeeId` but no actual photo on that host receives a `404` from the external service. The client MUST render its own fallback on image-load failure (`<img onerror>`). The server-side default (initials SVG) only covers users with no `employeeId`, bots, and rooms.

### GET /api/v1/avatar/:accountName

**Auth:** public (no credentials required)

Resolves a user or bot avatar. The frontend also routes DM/botDM room avatars here by passing the counterpart account (the other user or bot).

#### Query parameters

| Parameter | Notes |
|---|---|
| `siteid` | Optional. Owning site ID hint for bots. When supplied the bot's home site is trusted directly — no DB lookup. Ignored for users (always resolved locally). |
| `fwd` | Internal loop-breaker. Set to `1` by the service on cross-cluster redirects. A handler that receives `fwd=1` resolves locally or falls back to the default SVG — it does not redirect again. Clients must not set this; it is reserved for server-to-server hops. |

#### Response

| Status | Condition | Notes |
|---|---|---|
| `307 Temporary Redirect` | User with a known `employeeId` | `Location: {EMPLOYEE_PHOTO_BASE_URL}/{employeeId}_120.JPG` |
| `304 Not Modified` | `If-None-Match` matches the stored `ETag` | Empty body. |
| `200 OK` | Bot with a custom image (local) | Streams the MinIO object. `Content-Type` as stored. `ETag` + `Cache-Control: public, max-age=<cfg>`. |
| `200 OK` | No custom image or user with no `employeeId` | Returns the generated default SVG (`Content-Type: image/svg+xml`). `ETag` + `Cache-Control: public, max-age=<cfg>`. |
| `307 Temporary Redirect` | Bot whose owning site is a remote cluster | `Location: {clusterBaseURL}/api/v1/avatar/{account}?fwd=1`. Single hop only. |

**Decision logic:**

1. If the account matches the bot pattern: resolve owning site from `?siteid=` hint (no DB) or from the bot's user record (`User.SiteID`). If no user record exists → default SVG. If the owning site is a remote cluster → `307` to that cluster (unless `fwd=1`, in which case → default SVG). If local: check `avatars` collection; custom image found → `304`/stream; else → default SVG.
2. Otherwise (user): look up `employeeId` locally (users are synced to every cluster). If found → `307` to employee-photo URL. If not found → default SVG.

```
GET /api/v1/avatar/alice          → 307 to employee-photo host
GET /api/v1/avatar/helper.bot     → 200 (custom image) or 200 (default SVG)
GET /api/v1/avatar/p_adminhk → 200 (custom image) or 200 (default SVG)
```

---

### GET /api/v1/avatar/room/:roomID

**Auth:** public (no credentials required)

Resolves a channel or discussion room avatar. For DM and botDM rooms the service returns the default SVG — the frontend should use [GET /api/v1/avatar/:accountName](#get-apiv1avataraccountname) for those.

#### Query parameters

| Parameter | Notes |
|---|---|
| `siteid` | Optional. Owning site ID hint. When supplied, skips the subscription lookup — a remote hint triggers an immediate redirect; a local hint goes straight to the `avatars` lookup. Trade-off: without the subscription the dm/botDM guard and the room `Name` are unavailable (the default's initial falls back to `roomID`). |
| `fwd` | Same loop-breaker semantics as Endpoint 1. Single cross-cluster hop only. |

#### Response

| Status | Condition | Notes |
|---|---|---|
| `304 Not Modified` | `If-None-Match` matches the stored `ETag` | Empty body. |
| `200 OK` | Local room with a custom image | Streams the MinIO object. `ETag` + `Cache-Control: public, max-age=<cfg>`. |
| `200 OK` | DM/botDM room, unknown room, or no custom image | Default SVG (`Content-Type: image/svg+xml`). Initial derived from room name when available, else from `roomID`. |
| `307 Temporary Redirect` | Room owned by a remote cluster | `Location: {clusterBaseURL}/api/v1/avatar/room/{roomID}?fwd=1`. Single hop only. |

**Decision logic:**

1. Owning site, room type, and room name are resolved from the `subscriptions` collection (or from the `?siteid=` hint). If no local subscription and no hint → not found → default SVG.
2. If room type is `dm` or `botDM` → default SVG (use Endpoint 1 for these).
3. If owning site is a remote cluster → `307` (unless `fwd=1` → default SVG).
4. Check `avatars` collection: custom image found → `304`/stream; else → default SVG.

```
GET /api/v1/avatar/room/01970a4f8c2d7c9aQ    → 200 (custom) or 200 (default SVG)
GET /api/v1/avatar/room/<dm-room-id>         → 200 (default SVG — use Endpoint 1 for DMs)
```

---

### PUT /api/v1/avatar/bot/:botName

**Auth:** `x-user-id` + `x-auth-token` (botplatform session token, §10.1). The session must
either **be** the bot named in the path (`account == :botName`) or hold the `admin` role;
anything else is `403 not_admin`. A bot manages its own avatar; an admin manages any bot's.

Uploads a custom PNG or JPEG avatar for a bot. The body is the raw image bytes; `Content-Type` declares the format.

#### Request

| | Notes |
|---|---|
| Path | `:botName` — bare bot account, matched exactly against the session's account. Do **not** prefix `@`. Must satisfy the bot pattern (ends in `.bot` or is the `p_admin` platform-admin pseudo-account). |
| Body | Raw image bytes (PNG or JPEG). SVG and non-image payloads are rejected. |
| `Content-Type` header | Advisory. Validation is by decoding the body — a valid PNG or JPEG is accepted regardless of the declared header; non-images are rejected. |
| Max size | `MAX_UPLOAD_BYTES` (default 1 MiB). |

The service decodes the image bytes to verify they are a valid PNG or JPEG — malformed bytes are rejected even if `Content-Type` is correct.

#### Response

| Status | Condition |
|---|---|
| `200 OK` | Upload accepted and stored. Body returns the new avatar metadata (below). |
| `400 Bad Request` | `botName` does not match the bot pattern; body does not decode as a PNG or JPEG; body exceeds `MAX_UPLOAD_BYTES`. A wrong `Content-Type` alone is not rejected — the header is advisory, as the request table above states. |
| `401 Unauthenticated` | `invalid_token` — missing, unknown, or mismatched session credential (the three are indistinguishable). |
| `403 Forbidden` | `not_admin` — a valid session that is neither the named bot nor an admin, or one issued for another site. |
| `404 Not Found` | No user record for `botName` — unknown bot. |
| `409 Conflict` | Bot is owned by a different cluster. Response body names the correct domain. |
| `503 Unavailable` | `upstream_unavailable` — the session token could not be validated against botplatform. |

##### Success response (`200`)

| Field | Type | Notes |
|---|---|---|
| `etag` | string | The stored object's ETag — use it to bust caches / set `If-None-Match` without a follow-up `GET`. |
| `contentType` | string | Detected image type (`image/png` or `image/jpeg`). |
| `size` | int | Stored object size in bytes. |
| `updatedAt` | string (RFC 3339) | When the avatar was stored. |

```json
{
  "etag": "\"9b2cf...\"",
  "contentType": "image/png",
  "size": 20480,
  "updatedAt": "2026-06-16T02:00:00Z"
}
```

On `409`, the response body carries a human-readable message indicating which cluster to re-upload to (e.g. `"bot is owned by site-b — upload to https://media-service-site-b"`). The client must re-issue the `PUT` to the correct domain.

On success, the custom image takes effect immediately: subsequent `GET /api/v1/avatar/:accountName` calls for that bot will stream it (or return `304`). A re-upload overwrites the previous image; there is no delete/reset in v1.

---

### GET /api/v1/emoji/:shortcode

Serves a custom emoji image. The path no longer carries siteID: v1's FE only ever fetches the local site's emoji list and never renders non-local shortcodes, so the endpoint defaults to this cluster's site. An optional lowercase `?siteid=` query param (matching the avatar endpoints' `?siteid=` hint) names a specific site — absent or equal to this cluster's site serves locally; a known remote site 307-redirects to its owning cluster. Clients normally just use the `imageUrl` returned by [`emoji.list`](#emojilist--list-a-sites-custom-emoji) as-is (it is a bare `/api/v1/emoji/{shortcode}` path, resolved against the base URL of the site the list came from); append `?v={etag}` to cache-bust after re-uploads.

#### Response

| Status | Condition | Notes |
|---|---|---|
| `200 OK` | Local and registered | Streams the stored image bytes (`image/png`, `image/jpeg`, or `image/gif`) with `ETag` / `Cache-Control` / `X-Content-Type-Options: nosniff`. |
| `304 Not Modified` | `If-None-Match` matches the current `ETag` | Empty body. |
| `307 Temporary Redirect` | `?siteid=` names a known remote cluster | Redirect to `{remote base}/api/v1/emoji/{shortcode}` — no query param on the target, so it resolves locally there. |
| `400 Bad Request` | Malformed shortcode | |
| `404 Not Found` | `?siteid=` names a site not in this cluster's domain map (unknown); or local and unregistered | Custom emoji have **no generated default** (unlike avatars). |

**Decision logic:**

- `?siteid=` absent or equal to this cluster's site → local lookup.
- `?siteid=` names a known remote site → `307` redirect to that cluster's media-service at `/api/v1/emoji/{shortcode}` (param dropped). The target always defaults to local, so there is no redirect loop and no `fwd` guard.
- `?siteid=` names a site not in this cluster's domain map → `404`.
- Local and registered → streams the stored image with `ETag` / `Cache-Control` / `X-Content-Type-Options: nosniff`; honours `If-None-Match` with `304`.
- Local and unknown → `404`. Custom emoji have **no generated default** (unlike avatars).
- Malformed shortcode → `400`.

---

### PUT /api/v1/emoji/:shortcode

Uploads (or replaces — PUT is an upsert) a custom emoji. The upload always writes to **this** cluster's site — there is no cross-cluster upload in v1, so there is no declared-intent check to fail.

**Auth:** `x-user-id` + `x-auth-token` (botplatform session token, §10.1), **admin role required**. A shortcode is a site-wide shared name that every user on the site renders, so uploading one is an admin operation — a bot cannot overwrite an emoji for everybody. A valid non-admin session is `403 not_admin`.

The `?uploader={account}` query parameter is **accepted and ignored**. The `createdBy` / `updatedBy` audit fields are taken from the authenticated session, which cannot be spoofed by a caller.

#### Request

Raw image bytes as the body. PNG, JPEG, or GIF (animated GIFs are stored and served verbatim). Limits (env-configurable): body ≤ `EMOJI_MAX_UPLOAD_BYTES` (default 256 KiB), width/height ≤ `EMOJI_MAX_DIMENSION` (default 512).

#### Response

| Status | Condition |
|---|---|
| `200 OK` | Upload accepted and stored. Body returns the new emoji metadata (below). |
| `400 Bad Request` | Malformed shortcode; body not a valid PNG/JPEG/GIF; body or dimensions over the limits. |
| `400 Bad Request` | Reason `emoji_shortcode_reserved` — shortcode collides with a built-in standard emoji (would be permanently shadowed). |
| `401 Unauthenticated` | `invalid_token` — missing, unknown, or mismatched session credential (the three are indistinguishable). |
| `403 Forbidden` | `not_admin` — a valid session without the `admin` role, or one issued for another site. |
| `503 Unavailable` | `upstream_unavailable` — the session token could not be validated against botplatform. |

##### Success response (`200`)

| Field | Type | Notes |
|---|---|---|
| `shortcode` | string | Canonical (NFC) shortcode. |
| `etag` | string | New storage ETag — use as the `?v=` cache-buster. |
| `contentType` | string | Detected type (`image/png`, `image/jpeg`, `image/gif`). |
| `size` | int | Stored size in bytes. |
| `updatedAt` | string (RFC 3339) | Time of this upload (UTC). |

```json
{
  "shortcode": "acme_party",
  "etag": "9a0364b9e99bb480dd25e1f0284c8555",
  "contentType": "image/gif",
  "size": 20480,
  "updatedAt": "2025-05-06T08:08:20Z"
}
```

A newly uploaded emoji is usable immediately — it appears in the next [`emoji.list`](#emojilist--list-a-sites-custom-emoji) fetch and its image serves right away; reactions validate shortcode format only, so no registration propagation delay applies.

## 8. Presence

Served by **user-presence-service**. Tracks each user's effective presence —
`online`, `away`, `busy`, `offline`, `in-call` — derived from live connections,
an optional manual override, and an external Teams "in a call" signal
(`in-call`, set by the presence sync; suppresses notifications, never settable
as a manual status). Each site owns the presence of its local users; a
user's home site is the site they connect to. Cross-site is transparent: for
**live state** (§8.7) a watcher subscribes to the global per-user subject and
the NATS gateway routes it; for **batch queries** (§8.6) the watcher sends a
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
via `activity` (§8.3); the server has at least one **active** connection ⇒
`online`, all connections **inactive** ⇒ `away`, none ⇒ `offline`.

**Effective status resolution.** The server evaluates this precedence ladder
top-down; the **first** matching rule wins (so a stale manual override never
keeps a fully-disconnected user "present"):

1. **No live connections → `offline`** — beats any manual override.
2. Manual `appear_offline` → `offline`; manual `away` → `away`.
3. External Teams `in-call` → `in-call`.
4. Manual `online` → `online`; manual `busy` → `busy`.
5. All live connections inactive → `away`.
6. Otherwise → `online`.

### 8.1 Hello — initialize a connection (publish, fire-and-forget)

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

### 8.2 Ping — liveness (publish, fire-and-forget)

**Subject:** `chat.user.{account}.event.presence.{siteID}.ping`

Published per connection roughly every **30 s** (no reply) to refresh liveness.
A connection is considered live for ~**45 s** after its last ping; miss that
window and the sweeper decays it toward `offline`. A ping does **not** change
activity — use `activity` (§8.3) for that. Pinging a `connId` the server has not
seen before creates it (offline→online), so an initial `hello` is optional.

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `connId` | string | yes | The connection being refreshed. |
| `timestamp` | number | no | Millis since Unix epoch (UTC). |

```json
{ "connId": "1f0a-uuid", "timestamp": 1746518100000 }
```

### 8.3 Activity — active / inactive (publish, fire-and-forget)

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

### 8.4 Disconnect (publish, best-effort)

**Subject:** `chat.user.{account}.event.presence.{siteID}.bye`

Sent best-effort on tab close (`beforeunload`) for instant offline instead of
waiting for the liveness window to lapse. The sweeper is the backstop when it
does not arrive.

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `connId` | string | yes | The connection being closed. |
| `timestamp` | number | no | Millis since Unix epoch (UTC). |

### 8.5 Set / clear manual override (request/reply)

**Subject:** `chat.user.{account}.request.presence.{siteID}.manual.set`
**Reply:** standard `chat.user.{account}.>`.

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

### 8.6 Batch query — initial state (request/reply)

**Subject:** `chat.user.presence.{siteID}.query.batch` — addressed to **your own
local site**. You do **not** need to know or group accounts by their home site.
**Reply:** standard `chat.user.{account}.>`.

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

### 8.7 Live state (subscribe)

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
| `status` | string | Effective status: `online` / `away` / `busy` / `offline` / `in-call`. |
| `timestamp` | number | Millis since Unix epoch (UTC) of the change. |

**Subscribe before you snapshot.** To avoid missing a transition between the
snapshot and the subscription, subscribe to the state subject(s) **first**, then
send the §8.6 batch query for current values.

---

## 9. Admin Service

HTTP REST endpoints served by `admin-service`. Admins obtain an `authToken` via admin-service's own `POST /v1/login` (§9.10, unauthenticated — see also botplatform-service or portal-service `POST /api/v1/login`, which validate against the same shared sessions collection). That token is required as `Authorization: Bearer <authToken>` on `POST /v1/password/change` (§9.11) and every `/v1/admin/…` route below. The token is validated by `requireAdmin` middleware, which checks that the session exists, the session holds the `admin` role, and the session's `siteId` matches the admin-service's configured `SITE_ID`. Callers that fail any of these checks receive `403 not_admin` (role/site mismatch) or `401 invalid_token` (missing or unknown token).

The `userView` returned by all user endpoints is a projected subset — the `services` / bcrypt field is never included.

### Common error table (admin-service)

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 401 | `unauthenticated` | `invalid_token` | Token missing, unknown, or session not found. |
| 403 | `forbidden` | `not_admin` | Valid session, but caller lacks the `admin` role or the session `siteId` does not match. |
| 404 | `not_found` | `user_not_found` | Target user ID not found (get, update, set-password). |
| 409 | `conflict` | `account_exists` | Account already exists (create only). |
| 400 | `bad_request` | `missing_fields` | Required fields absent or body not valid JSON. |
| 500 | `internal` | — | Server-side fault; cause is logged server-side only. |

### 9.1 List users

**Endpoint:** `GET /v1/admin/users`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Returns a paged list of users across every site — cross-site replicas included. Each row's `siteId` names its home site; mutating endpoints (§9.2–§9.5, §9.16) stay home-site-scoped, so rows homed elsewhere are read-only on this site.

#### Query parameters

| Parameter | Type | Notes |
|---|---|---|
| `q` | string | Optional free-text search. Empty returns all users. |
| `page` | integer | Page number, 1-based. Defaults to `1`. |
| `limit` | integer | Page size. Defaults to `20`. |

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `users` | [UserView](#userview)[] | Projected user records for this page. |
| `total` | integer | Total matching users (across all pages). |

```json
{
  "users": [
    {
      "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
      "account": "alice",
      "siteId": "site-a",
      "engName": "Alice",
      "chineseName": "愛麗絲",
      "roles": ["admin"],
      "active": true,
      "requirePasswordChange": false
    }
  ],
  "total": 1
}
```

### 9.2 Create user

**Endpoint:** `POST /v1/admin/users`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Creates a new user account. The `siteId` is always forced to the admin-service's configured `SITE_ID` — the caller cannot set it.

After the write commits, the account is fanned out to every other site so their copies converge, and onto the durable HR identity feed. Neither failure changes the status code; both surface as response fields.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `account` | string | yes | Login account name. Must be unique within the site. |
| `engName` | string | no | English display name. Recommended but not required. |
| `chineseName` | string | no | Chinese display name. |
| `password` | string | yes | Plaintext password; stored as bcrypt of SHA-256 hex. |
| `roles` | string[] | no | Initial roles, e.g. `["admin"]`. Defaults to empty. |
| `requirePasswordChange` | boolean | no | Whether the user must change password on first login. Defaults to `true` when omitted. |

```json
{
  "account": "bob",
  "engName": "Bob",
  "chineseName": "鮑勃",
  "password": "s3cr3t!",
  "roles": [],
  "requirePasswordChange": true
}
```

#### Success response

`HTTP 201` — the created [UserView](#userview), plus the fanout outcome.

| Field | Type | Notes |
|---|---|---|
| `syncFailures` | string[] | Remote site IDs whose account-snapshot publish was not acknowledged. Omitted (not `[]`) when every destination landed. Still `201` when present — the account exists on this site. The durable HR feed delivers **identity fields only**, so the listed sites lack this account's roles/status (or the whole account, when `hrSyncFailed` is also set) until healed by [§9.16 resync](#916-resync-user) or the next edit ([§9.4](#94-update-user)). |
| `hrSyncFailed` | boolean | `true` when the durable HR identity publish failed. Omitted when it landed. Still `201` when present. |

```json
{
  "id": "01970a4f8c2d7c9b01970a4f8c2d7c9b",
  "account": "bob",
  "siteId": "site-a",
  "engName": "Bob",
  "chineseName": "鮑勃",
  "roles": [],
  "active": true,
  "requirePasswordChange": true
}
```

Same `201` body with a partially-failed fanout — `site-b` did not acknowledge its snapshot and the HR identity publish did not land:

```json
{
  "id": "01970a4f8c2d7c9b01970a4f8c2d7c9b",
  "account": "bob",
  "siteId": "site-a",
  "engName": "Bob",
  "chineseName": "鮑勃",
  "roles": [],
  "active": true,
  "requirePasswordChange": true,
  "syncFailures": ["site-b"],
  "hrSyncFailed": true
}
```

### 9.3 Get user

**Endpoint:** `GET /v1/admin/users/:account`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Returns a single [UserView](#userview) by account. The account is resolved within the admin-service's configured site.

#### Success response

`HTTP 200` — a [UserView](#userview).

```json
{
  "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
  "account": "alice",
  "siteId": "site-a",
  "engName": "Alice",
  "chineseName": "愛麗絲",
  "roles": ["admin"],
  "active": true,
  "requirePasswordChange": false
}
```

### 9.4 Update user

**Endpoint:** `PATCH /v1/admin/users/:account`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Applies partial updates to a user. All fields are optional; omitting a field leaves it unchanged. When `active` is set to `false`, all active sessions for the user are revoked immediately.

After the write commits, the whole account snapshot is fanned out to every other site so their copies converge. A fanout failure does not change the status code; it surfaces as `syncFailures`.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `engName` | string | no | New English display name. |
| `chineseName` | string | no | New Chinese display name. |
| `roles` | string[] | no | Replaces the user's roles array. |
| `active` | boolean | no | Set to `false` to deactivate (all sessions revoked); `true` to reactivate. |

```json
{ "roles": ["admin"], "active": true }
```

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"ok"`. |
| `syncFailures` | string[] | Remote site IDs whose account-snapshot publish was not acknowledged. Omitted (not `[]`) when every destination landed. Still `200` when present — the update is stored on this site; the listed sites converge when healed by [§9.16 resync](#916-resync-user) or the next edit. |

```json
{ "status": "ok" }
```

Same `200` body when a destination did not acknowledge its snapshot:

```json
{ "status": "ok", "syncFailures": ["site-b"] }
```

### 9.5 Set password

**Endpoint:** `POST /v1/admin/users/:account/password`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Replaces the user's password and revokes all active sessions (forcing re-login).

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `password` | string | yes | New plaintext password. |
| `requirePasswordChange` | boolean | no | Whether to force a password change on next login. Defaults to `true` when omitted. |

```json
{ "password": "newS3cr3t!", "requirePasswordChange": false }
```

#### Success response

`HTTP 200`

```json
{ "status": "ok" }
```

### 9.6 List sessions

**Endpoint:** `GET /v1/admin/sessions?account=<account>`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Lists all active sessions for the given account (required `account` query parameter). Returns only the projected [SessionView](#sessionview) fields — roles are excluded from the response. A missing `account` query parameter returns `400 missing_fields`.

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `sessions` | [SessionView](#sessionview)[] | Active sessions for the account. |

```json
{
  "sessions": [
    {
      "id": "sess_01970a4f8c2d7c9a",
      "userId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
      "account": "bob",
      "siteId": "site-a",
      "issuedAt": 1746518400000
    }
  ]
}
```

### 9.7 Revoke all sessions

**Endpoint:** `DELETE /v1/admin/sessions?account=<account>`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Revokes all active sessions for the given account (required `account` query parameter) and appends a `session.revoke_all` audit entry. A missing `account` query parameter returns `400 missing_fields`.

#### Success response

`HTTP 200`

```json
{ "status": "ok" }
```

### 9.8 Revoke single session

**Endpoint:** `DELETE /v1/admin/sessions/:sessionId?account=<account>`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Revokes a single session scoped to the given account (required `account` query parameter) and appends a `session.revoke` audit entry. A missing `account` query parameter returns `400 missing_fields`.

#### Success response

`HTTP 200`

```json
{ "status": "ok" }
```

### 9.9 List audit log

**Endpoint:** `GET /v1/admin/audit`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Returns audit entries for the admin's site, newest-first, with optional filtering.

#### Query parameters

| Parameter | Type | Notes |
|---|---|---|
| `targetAccount` | string | Optional. Filter by the affected user's account. |
| `actor` | string | Optional. Filter by actor account. |
| `action` | string | Optional. Filter by action string (e.g. `user.create`, `session.revoke_all`, `permission.grant`, `permission.revoke`). |
| `page` | integer | Page number, 1-based. Defaults to `1`. |
| `limit` | integer | Page size. Defaults to `20`. |

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `entries` | [AuditEntry](#auditentry)[] | Matching entries, newest-first. |
| `total` | integer | Total matching entries across all pages. |

```json
{
  "entries": [
    {
      "id": "01970a4f8c2d7c9c",
      "actorUserId": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
      "actorAccount": "alice",
      "action": "user.create",
      "targetAccount": "bob",
      "details": { "account": "bob" },
      "siteId": "site-a",
      "timestamp": 1746518400000
    }
  ],
  "total": 1
}
```

### 9.10 Admin login

**Endpoint:** `POST /v1/login`
**Auth:** none — credentials are supplied in the request body.

Password login for platform-admin accounts, served directly by admin-service. Distinct from the SSO flow (§2.2/§2.3) and the bot/portal password login (§2.5/§10.1) — this endpoint is for the admin console only. Verifies the account holds the `admin` role, is not deactivated, and the password matches, then issues a session token in the same shared sessions collection used by `/v1/admin/…` and `/v1/password/change`. Unknown account, wrong password, non-admin account, and deactivated account all collapse to the same `401 invalid_credentials` response (checked in that order, deactivated last) so a caller cannot distinguish the cases by shape, reason, or timing.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `username` | string | yes | Login account name. |
| `password` | string | yes | Plaintext password. |

```json
{ "username": "alice", "password": "s3cr3t!" }
```

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `authToken` | string | Session token. Use as `Authorization: Bearer <authToken>` on `/v1/password/change` (§9.11) and every `/v1/admin/…` endpoint. |
| `account` | string | Login account name. |
| `siteId` | string | Site the session was issued for (the admin-service's configured `SITE_ID`). |
| `requirePasswordChange` | boolean | Whether the caller must change their password before continuing. When `true`, the client should route to §9.11 before normal use; the session token is still valid either way. |

```json
{
  "authToken": "sess_tok_9f8e7d6c5b4a",
  "account": "alice",
  "siteId": "site-a",
  "requirePasswordChange": false
}
```

#### Error response

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `missing_fields` | `username` or `password` empty, or body not valid JSON. |
| 401 | `unauthenticated` | `invalid_credentials` | Unknown account, wrong password, account lacks the `admin` role, or account is deactivated. Uniform response — cannot be used to enumerate accounts. |
| 500 | `internal` | — | Server-side fault; cause is logged server-side only. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

### 9.11 Change password (self-service)

**Endpoint:** `POST /v1/password/change`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Lets the logged-in admin change their own password. Verifies `oldPassword` against the stored hash, updates it to `newPassword`, clears `requirePasswordChange`, and best-effort revokes every other active session for the account — admin-console and chat-frontend sessions live in the same collection, so this signs the account out everywhere else. The caller's own session is preserved. Appends a `password_change_self` audit entry.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `oldPassword` | string | yes | Current plaintext password. |
| `newPassword` | string | yes | New plaintext password. |

```json
{ "oldPassword": "s3cr3t!", "newPassword": "newS3cr3t!" }
```

#### Success response

`HTTP 204` — no body.

#### Error response

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `missing_fields` | `oldPassword` or `newPassword` empty, or body not valid JSON. |
| 401 | `unauthenticated` | `invalid_token` | Bearer token missing, unknown, or session not found. |
| 401 | `unauthenticated` | `old_password_mismatch` | `oldPassword` does not match the stored password. |
| 403 | `forbidden` | `not_admin` | Valid session, but caller lacks the `admin` role or the session `siteId` does not match. |
| 500 | `internal` | — | Server-side fault; cause is logged server-side only. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

### 9.12 Set room on-duty

**Endpoint:** `POST /v1/admin/rooms/:roomId/onduty`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Toggles a channel room's on-duty state. On-duty staff work off the company network, so `onDuty: true` narrows who may change the roster (`restricted` — only owners may add members) and permits the connection from outside (`externalAccess`); `onDuty: false` clears both. No room or subscription field named `onDuty` exists — the parameter maps onto those two flags, which are owned by room-service.

**Nothing is displayed.** A restriction change publishes no system message, so no chat entry appears in the room and no notification is sent. Clients are still told: a flat `room_restricted` **room event** carries the new flags on the room's event subject, so open sessions refresh their state without a re-fetch and without rendering anything. No audit row is written — room-service's `processing room.restricted` log line, carrying actor, room, both flags and the designated owner, is the only durable server-side record.

Turning duty **on** designates `ownerAccount` as the room's owner: that account becomes the sole owner and every other member is reset to plain member. Turning duty **off** sends no owner, so roles are left exactly as they are.

Channel rooms only. The caller must also hold the platform `admin` user role, which room-service verifies independently of the session check.

The member floor and the require-an-owner rule apply **only to the unrestricted → restricted transition**. Calling with `onDuty: true` against a room that is *already* restricted rotates the owner — the new account is still validated as a member and still becomes sole owner — but the `RESTRICTED_ROOM_MIN_MEMBERS` floor (default 5) is not re-checked.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `onDuty` | boolean | yes | `true` sets `restricted` and `externalAccess`; `false` clears both. An absent field is rejected; an explicit `false` is accepted. |
| `ownerAccount` | string | when `onDuty` is `true` | Account that becomes the room's sole owner. Must be a member of the room. Surrounding whitespace is trimmed, and a whitespace-only value counts as absent. Ignored when `onDuty` is `false`. |

```json
{ "onDuty": true, "ownerAccount": "alice" }
```

#### Success response

`HTTP 200`

```json
{ "status": "ok" }
```

#### Errors

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `missing_fields` | `onDuty` absent, not a boolean, body not valid JSON, or `ownerAccount` absent/blank while `onDuty` is `true`. |
| 400 | `bad_request` | `non_channel_operation` | Target room is not a channel. |
| 400 | `bad_request` | — | `ownerAccount` is not a member of the room. |
| 401 | `unauthenticated` | `invalid_token` | Bearer token missing, unknown, or session not found. |
| 403 | `forbidden` | `not_admin` | Session lacks the `admin` role or its `siteId` does not match. |
| 403 | `forbidden` | — | Caller does not hold the platform `admin` user role (raised by room-service). |
| 404 | `not_found` | — | Room not found. |
| 409 | `conflict` | — | Room has fewer members than `RESTRICTED_ROOM_MIN_MEMBERS`. Only on the unrestricted → restricted transition. |
| 500 | `internal` | — | Server-side fault; cause is logged server-side only. |
| 503 | `unavailable` | — | room-service did not answer within `ROOM_RPC_TIMEOUT`, no responder was reachable, the client disconnected mid-call, room-service shed the request under load, or admin-service has no room-service client configured. The toggle is idempotent and may have applied — retry is safe. |

#### Triggered events — success path

[`room_restricted`](client-api/events.md#room_restricted-roomrestrictedroomevent) — a flat room event on the room's event subject carrying the new `restricted` / `externalAccess` values, the designated `ownerAccount`, and the acting admin. It is a state update, **not** a message: clients apply it to their local subscription and render nothing.

No system message is published, so nothing appears in the room timeline and no push notification is sent.

The event is published last, after every step that can still fail the call, so a non-2xx response means no event went out.

For a room with members homed on other sites, room-service still fans the state change out to each remote site's inbox so their subscription copies stay in step. That is a server-to-server sync, not a client-visible event.

#### Triggered events — error path

`None.`

### 9.13 Create / revoke permission grants

**Endpoint:** `POST /v1/admin/permissions`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Records a new row in the append-only `permission_grants` ledger for one or more subject accounts — either a grant (`granted: true`) or a revocation (`granted: false`) of the same permission key in one call. Unlike its siblings this is not a `/users/:account/...`-shaped endpoint, because `subjectAccounts` is a batch. The same write also materializes the new state on each subject's user document — that snapshot, not the ledger, is what [`settings.get`](#settingsget) and `currentlyGranted` (§9.14) read; the ledger is audit history and is never rewritten. One `admin_audit` entry (`action`: `permission.grant` or `permission.revoke`, `targetAccount`: the subject, `details`: `{"permission": "<permission key>"}`) is written per subject alongside the ledger rows.

After the write commits, the new state is fanned out to every other site so their copies converge. Large batches are split into chunks internally; that is transparent to callers — chunking never shows up in the request or the response, and only whole destinations appear in `syncFailures`.

Dates are plain `YYYY-MM-DD` strings, interpreted under a fixed UTC+8 rule — never the caller's timezone, never the server's local timezone. There is no `timezone` field, and there never will be one for this endpoint.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `permission` | string | yes | Permission key. The only key defined today is `external.image.view`. |
| `subjectAccounts` | string[] | yes | Accounts to grant or revoke for — non-empty, no fixed cap. Duplicates are silently deduplicated (first occurrence kept) and reported back in `duplicatesIgnored` — never rejected outright. |
| `granted` | boolean | yes | `true` records a grant, `false` records a revocation. Required — a body that omits `granted`, or sends an explicit `granted: null`, is rejected with `400 missing_permission_fields`, not silently treated as `false`/revoke. |
| `effectiveFrom` | string | no | Grant only — `YYYY-MM-DD`. Omitted means "effective immediately" (the write instant). Backdating is allowed. **Must be omitted when `granted` is `false`** — a revocation has no validity window. A JSON `null` is treated as absent (equivalent to omitting the field). |
| `expiresAt` | string | when `granted` is `true` | Grant only — `YYYY-MM-DD`, the last valid day (inclusive). Stored internally as the exclusive-end instant, UTC+8 midnight the *following* day. A value equal to today is valid (the grant expires tonight); any earlier date is rejected. **Must be omitted when `granted` is `false`** — there are no permanent grants and no dated revocations. A JSON `null` is treated as absent (equivalent to omitting the field). |
| `applicantAccount` | string | yes | Account of the person who requested the change (offline approval). Must exist; may be inactive. |
| `approverAccount` | string | yes | Account of the person who approved the change (offline approval). Must exist; may be inactive. |
| `reason` | string | no | Free text, ≤1000 runes (`utf8.RuneCountInString`) when present. Omitted or empty is accepted and stored as `""`. Retained permanently — this is an append-only ledger, not a mutable note. |

Grant:

```json
{
  "permission": "external.image.view",
  "subjectAccounts": ["alice", "bob"],
  "granted": true,
  "effectiveFrom": "2026-09-01",
  "expiresAt": "2026-12-31",
  "applicantAccount": "carol",
  "approverAccount": "dave",
  "reason": "On-call staff must review production line photos from outside the fab."
}
```

Revoke — `effectiveFrom`/`expiresAt` omitted (an explicit `null` is equivalent):

```json
{
  "permission": "external.image.view",
  "subjectAccounts": ["alice"],
  "granted": false,
  "applicantAccount": "carol",
  "approverAccount": "dave",
  "reason": "Project ended."
}
```

#### Success response

`HTTP 201`

| Field | Type | Notes |
|---|---|---|
| `created` | integer | Number of ledger rows written — the deduplicated subject count. |
| `duplicatesIgnored` | string[] | Subject accounts dropped as duplicates of an earlier entry in the same request. `[]`, never `null`, when there were none. |
| `syncFailures` | string[] | Remote site IDs whose cross-site publish was not acknowledged. Omitted (not `[]`) when every destination landed. Still `201` when present — the ledger rows and this site's state were written; the listed sites may keep serving the previous decision until healed by [§9.15 resync](#915-resync-permission-fanout). |

```json
{
  "created": 2,
  "duplicatesIgnored": [],
  "syncFailures": ["site-b"]
}
```

#### Errors

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `missing_fields` | Body is not valid JSON. |
| 400 | `bad_request` | `unknown_permission` | `permission` is not a recognized key. |
| 400 | `bad_request` | `invalid_subject_count` | `subjectAccounts` is empty. |
| 400 | `bad_request` | `invalid_reason` | `reason` exceeds 1000 runes. |
| 400 | `bad_request` | `missing_permission_fields` | `granted` is omitted, or `applicantAccount`/`approverAccount` is empty. |
| 400 | `bad_request` | `invalid_permission_window` | Grant only. `effectiveFrom`/`expiresAt` not `YYYY-MM-DD`, `effectiveFrom` after `expiresAt`, or `expiresAt`'s derived instant is not in the future (today is valid; yesterday or earlier is not). |
| 400 | `bad_request` | `unexpected_permission_window` | Revoke only. `effectiveFrom` or `expiresAt` present with a non-null value; an explicit `null` counts as absent and is accepted. |
| 404 | `not_found` | `unknown_accounts` | A subject, applicant, or approver account does not exist. Accounts are looked up company-wide — the lookup does not filter by `siteId`, so a subject may be homed at any site. Message names the offending accounts; `metadata.accounts` carries the same comma-joined list for programmatic display. |
| 400 | `bad_request` | `inactive_subject` | A subject account exists but is deactivated. `applicantAccount`/`approverAccount` are exempt — a departed staff member may still be recorded as applicant or approver. Message names the offending accounts; `metadata.accounts` carries the same comma-joined list. |
| 401 | `unauthenticated` | `invalid_token` | Token missing, unknown, or session not found. |
| 403 | `forbidden` | `not_admin` | Valid session, but caller lacks the `admin` role or the session `siteId` does not match. |
| 500 | `internal` | — | The batch insert is transactional — on failure, no ledger row was written and no audit entry exists. |

```bash
# Grant
curl -X POST https://admin.example.com/v1/admin/permissions \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "permission": "external.image.view",
    "subjectAccounts": ["alice", "bob"],
    "granted": true,
    "effectiveFrom": "2026-09-01",
    "expiresAt": "2026-12-31",
    "applicantAccount": "carol",
    "approverAccount": "dave",
    "reason": "On-call staff must review production line photos from outside the fab."
  }'
```

```bash
# Revoke
curl -X POST https://admin.example.com/v1/admin/permissions \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "permission": "external.image.view",
    "subjectAccounts": ["alice"],
    "granted": false,
    "applicantAccount": "carol",
    "approverAccount": "dave",
    "reason": "Project ended."
  }'
```

### 9.14 List permission grants

**Endpoint:** `GET /v1/admin/permissions`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Returns the permission ledger, newest-first, plus the current computed decision when the filters narrow to a single subject and permission. Backs both the terminal workflow (confirm state before writing, confirm again after) and the console's lookup pane. Company-wide — the ledger browse does not filter by `siteId`, so rows for subjects homed at any site are returned.

#### Query parameters

| Parameter | Type | Notes |
|---|---|---|
| `subjectAccount` | string | Optional. Filter to one subject account. Omitted lists every subject. |
| `permission` | string | Optional. Filter to one permission key. Omitted lists every permission. An unrecognized key returns `400 unknown_permission`. |
| `page` | integer | Page number, 1-based. Defaults to `1`. |
| `limit` | integer | Page size. Defaults to `20`, max `100`. |

`subjectAccount` and `permission` combine independently, four ways: neither given returns every row; `subjectAccount` alone returns one subject's full ledger; `permission` alone returns one permission key across every subject; both given narrows to one subject's ledger for one permission. `currentlyGranted` (below) is included only in the both-given case.

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `currentlyGranted` | boolean | Present only when BOTH `subjectAccount` and `permission` were supplied — a single decision is meaningless without both. The current decision, evaluated at read time from the subject's materialized user-document state — the same evaluation path as [`settings.get`](#settingsget), so the two can never disagree. Not derived from the ledger rows below (those are audit history). Deliberately not named `granted`, which is a per-row field meaning "what this record did", not "current state". |
| `entries` | [PermissionGrantView](#permissiongrantview)[] | The ledger, newest-first (`recordedAt` desc, `_id` desc tie-break). |
| `total` | integer | Total matching rows across all pages. |

```json
{
  "currentlyGranted": false,
  "entries": [
    {
      "id": "0199f2c3a4b5c6d90199f2c3a4b5c6d9",
      "permission": "external.image.view",
      "subjectAccount": "alice",
      "granted": false,
      "applicantAccount": "carol",
      "approverAccount": "dave",
      "reason": "Project ended.",
      "recordedBy": "p_admin_wang",
      "recordedAt": "2026-10-15T02:03:04Z"
    },
    {
      "id": "0199f2c3a4b5c6d70199f2c3a4b5c6d7",
      "permission": "external.image.view",
      "subjectAccount": "alice",
      "granted": true,
      "effectiveFrom": "2026-09-01",
      "expiresAt": "2026-12-31",
      "applicantAccount": "carol",
      "approverAccount": "dave",
      "reason": "On-call staff must review production line photos from outside the fab.",
      "recordedBy": "p_admin_wang",
      "recordedAt": "2026-09-01T01:00:00Z"
    }
  ],
  "total": 2
}
```

Note what this example demonstrates: the revoke row (newest, listed first) omits
`effectiveFrom`/`expiresAt` entirely, and `currentlyGranted` reads
`false` because the revoke is what the subject's materialized state now holds.

#### Errors

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `unknown_permission` | `permission` is not a recognized key. |
| 401 | `unauthenticated` | `invalid_token` | Token missing, unknown, or session not found. |
| 403 | `forbidden` | `not_admin` | Valid session, but caller lacks the `admin` role or the session `siteId` does not match. |
| 500 | `internal` | — | Server-side fault; cause is logged server-side only. |

```bash
# Lookup
curl -G https://admin.example.com/v1/admin/permissions \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  --data-urlencode "subjectAccount=alice" \
  --data-urlencode "permission=external.image.view"
```

### 9.15 Resync permission fanout

**Endpoint:** `POST /v1/admin/permissions/resync`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Re-delivers the **current** materialized state for the given accounts to every other site — the remediation for a `syncFailures` entry returned by §9.13. Re-delivery only: it reads each account's stored state and republishes it, and **writes nothing** — no ledger row, no `admin_audit` entry, no user-document update. Safe to call repeatedly: each delivered state carries its original write timestamp, and a destination ignores anything older than what it already holds — so a replay either rewrites the identical value or is discarded, and can never undo a newer decision.

No existence or active check is performed — this endpoint syncs state, it does not validate people. An account with no stored state for the requested permission (unknown account, or never granted this permission) is silently skipped: nothing is published for it and it is not reported as an error.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `permission` | string | yes | Permission key to resync. The only key defined today is `external.image.view`. |
| `accounts` | string[] | yes | Accounts to resync — non-empty, no fixed cap. Duplicates are silently deduplicated (first occurrence kept) and not reported. |

```json
{
  "permission": "external.image.view",
  "accounts": ["alice", "bob"]
}
```

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `syncFailures` | string[] | Remote site IDs whose republish was not acknowledged, deduplicated. Omitted (not `[]`) when every destination landed — the usual case; call again to retry. |

Everything landed:

```json
{}
```

One destination still unreachable:

```json
{ "syncFailures": ["site-b"] }
```

#### Errors

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `missing_fields` | Body is not valid JSON. |
| 400 | `bad_request` | `unknown_permission` | `permission` is not a recognized key. |
| 400 | `bad_request` | `invalid_subject_count` | `accounts` is empty. |
| 401 | `unauthenticated` | `invalid_token` | Token missing, unknown, or session not found. |
| 403 | `forbidden` | `not_admin` | Valid session, but caller lacks the `admin` role or the session `siteId` does not match. |
| 500 | `internal` | — | Failed to read the stored state; nothing was published. Cause is logged server-side only. |

```bash
# Resync the two accounts a failed grant left out of sync
curl -X POST https://admin.example.com/v1/admin/permissions/resync \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "permission": "external.image.view",
    "accounts": ["alice", "bob"]
  }'
```

### UserView

Projected user record returned by all admin user endpoints. The `services` / bcrypt field is never included.

| Field | Type | Notes |
|---|---|---|
| `id` | string | Internal user ID (32-char UUIDv7 hex). |
| `account` | string | Login account name. |
| `siteId` | string | Owning site ID. |
| `sectId` | string | Section (org unit) ID. Omitted when empty. |
| `sectName` | string | Section name. Omitted when empty. |
| `sectTCName` | string | Section traditional-Chinese name. Omitted when empty. |
| `sectDescription` | string | Section description. Omitted when empty. |
| `deptId` | string | Department ID. Omitted when empty. |
| `deptName` | string | Department name. Omitted when empty. |
| `deptTCName` | string | Department traditional-Chinese name. Omitted when empty. |
| `deptDescription` | string | Department description. Omitted when empty. |
| `engName` | string | English display name. Omitted when empty. |
| `chineseName` | string | Chinese display name. Omitted when empty. |
| `employeeId` | string | Employee ID. Omitted when empty. |
| `statusIsShow` | boolean | Whether the user's status text is visible. Always present. |
| `statusText` | string | Custom status text. Omitted when empty. |
| `roles` | string[] | Role tags, e.g. `["admin"]`. Omitted when empty. |
| `requirePasswordChange` | boolean | First-login password-change flag. Omitted when `false`. |
| `active` | boolean | Whether the account is active. Always present; a user doc with no stored flag reads as `true`. |

### SessionView

| Field | Type | Notes |
|---|---|---|
| `id` | string | Session ID. |
| `userId` | string | Internal user ID. |
| `account` | string | Account the session belongs to. |
| `siteId` | string | Site the session was issued for. |
| `issuedAt` | integer | Epoch ms (UTC) when the session was created. |

### AuditEntry

| Field | Type | Notes |
|---|---|---|
| `id` | string | Audit entry ID. |
| `actorUserId` | string | Internal user ID of the admin who performed the action. |
| `actorAccount` | string | Account of the admin. |
| `action` | string | Action string, e.g. `user.create`, `user.update`, `user.password.set`, `session.revoke_all`, `session.revoke`, `permission.grant`, `permission.revoke`. |
| `targetUserId` | string | Internal ID of the affected user. Omitted when not applicable. |
| `targetAccount` | string | Account of the affected user. Omitted when not applicable. |
| `details` | map<string, string> | Non-secret context for the action (e.g. `{"account":"bob"}`). Omitted when empty. Never contains passwords, hashes, or tokens. |
| `siteId` | string | Site the action was performed on. |
| `timestamp` | integer | Epoch ms (UTC) when the action occurred. |

### PermissionGrantView

| Field | Type | Notes |
|---|---|---|
| `id` | string | Ledger row ID (32-char UUIDv7 hex). |
| `permission` | string | Permission key this row applies to. |
| `subjectAccount` | string | The account this row grants or revokes the permission for. |
| `granted` | boolean | What this row did — `true` for a grant, `false` for a revocation. Historical rows are never rewritten; this is not "does the subject currently hold the permission" (see `currentlyGranted`, §9.14). |
| `effectiveFrom` | string | `YYYY-MM-DD`, decoded back from the stored instant at UTC+8. Omitted on revoke rows (no validity window). |
| `expiresAt` | string | `YYYY-MM-DD`, the inclusive last valid day (decoded from the stored exclusive-end instant at UTC+8, minus one day). Omitted on revoke rows. |
| `applicantAccount` | string | Who requested the change. |
| `approverAccount` | string | Who approved the change. |
| `reason` | string | Free-text justification, as submitted. |
| `recordedBy` | string | The admin account that recorded this row (from the session token). |
| `recordedAt` | string | RFC 3339. Server clock at write time (when the decision was recorded). Independent of the validity window — a grant can be back- or future-dated via `effectiveFrom`/`expiresAt`. |

### 9.16 Resync user

**Endpoint:** `POST /v1/admin/users/:account/resync`
**Auth:** `Authorization: Bearer <authToken>`, admin role + same-site required.

Re-delivers the account's current home-site state on both sync lanes: the durable HR identity bootstrap plus the `user_account_updated` snapshot to every remote site's INBOX. Re-delivery only — no user write and no audit entry — and idempotent on the receivers (the snapshot re-stamps the watermark with the same field values). Only home-site accounts qualify; a replica homed elsewhere returns `404 user_not_found`.

#### Request body

None.

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"ok"`. |
| `syncFailures` | string[] | Remote site IDs whose snapshot publish was not acknowledged. Omitted when every destination landed. The durable HR feed delivers identity fields only, so sites named here lack roles/status (or the whole account, when `hrSyncFailed` is also set) until a later resync or edit lands. |
| `hrSyncFailed` | boolean | `true` when the durable HR identity publish failed. Omitted when it landed. |

```json
{
  "status": "ok"
}
```

#### Errors

| HTTP | `code` | `reason` | When |
|---|---|---|---|
| 404 | `not_found` | `user_not_found` | No user with that account homed at this site (unknown account, or a cross-site replica). |
| 401 | `unauthenticated` | `invalid_token` | Missing/invalid session token. |
| 403 | `forbidden` | `not_admin` | Session lacks the admin role. |

### 9.17 HTTP — POST /v1/admin/client-updates

**Auth:** admin session (`Authorization: Bearer <session token>`), same as every `/v1/admin/…` route.

Publishes a client update artifact pair. `admin-service` relays both parts to
`client-update-service` under its own service-account credential; the browser
never holds that credential.

**Disk-backed, not memory-backed.** The handler spools the incoming parts past
1 MiB to a temp file, then re-encodes them into a streamed outbound body — small
envelope snapshots interleaved with the spooled files' own readers — sent with an
exact `Content-Length`. Peak heap is therefore independent of artifact size
(measured: a 48 MiB artifact relays with a ~5 MiB peak). The cost is ephemeral
storage: size `admin-service`'s disk for the concurrent-upload total, not its
RAM. Temp files are removed when the request ends.

**Size cap.** `CLIENT_UPDATE_MAX_UPLOAD_BYTES` (default `2147483648`, i.e. 2 GiB)
caps one request body, counted before anything is spooled, so an oversize upload
cannot fill the disk on its way to being rejected. Exceeding it ends the request
with a `400` naming the limit in bytes. It is a guard rail on this service's
ephemeral storage, not the artifact-size policy — that lives in
`client-update-service`.

The artifacts themselves are validated by `client-update-service`, not here: file
name and extension rules live there and are reported back verbatim on a `400`.

#### Request

`multipart/form-data`:

| Part | Type | Required | Notes |
|---|---|---|---|
| `configFile` | file (`.yaml`/`.yml`) | yes | Update descriptor. |
| `executeFile` | file (binary) | yes | The executable. |

#### Response

| Status | Condition |
|---|---|
| `200 OK` | Both artifacts published. |
| `400 Bad Request` | Body is not `multipart/form-data`, the multipart body was malformed or truncated, the body exceeded `CLIENT_UPDATE_MAX_UPLOAD_BYTES` (distinct message, names the limit), the browser did not finish sending within `CLIENT_UPDATE_UPLOAD_TIMEOUT` (distinct message), or `client-update-service` rejected the artifacts (its message is relayed). |
| `401 Unauthorized` | Missing or invalid admin session. |
| `403 Forbidden` | Valid session without the `admin` role, or issued for another site. |
| `500 Internal Server Error` | This service could not extend its own I/O deadlines for the upload (deployment fault). |
| `503 Service Unavailable` | `client-update-service` is unreachable, did not answer within `CLIENT_UPDATE_UPLOAD_TIMEOUT` (distinct message), or this service's upload credential is not configured or was rejected. |

##### Success response (`200`)

| Field | Type | Notes |
|---|---|---|
| `result` | string | Always `"success"`. |

```json
{ "result": "success" }
```

**Audit:** an upload that `client-update-service` accepted appends an
`AuditEntry` with action `client_update.upload` and **no `details`** — the
filenames are the upstream's record to keep, and duplicating them here made this
service responsible for matching how the upstream picks parts. A rejected or
failed upload is never audited. The append
is best-effort, as it is for every mutating admin endpoint: the artifacts are
already published by then, so a failed append is logged at `ERROR` and the
response still reports the publication. Treat the audit log as a record of
intent, not as a transaction log for the artifact store.

**Not atomic across the pair:** `client-update-service` writes the two objects
independently and without locking (§12). A `200` means both of this request's
writes landed; a failure after the first leaves the new descriptor beside the
previous executable, and two concurrent uploads can interleave into a mixed
pair with both callers seeing `200`. Re-uploading the pair repairs either case.
Publish one pair at a time. There is no versioning or rollback — a deliberate
non-goal of this endpoint.

**Redirects are refused.** A `3xx` from `client-update-service` is treated as an
error, not followed. `net/http` strips `Authorization` only when the redirect
changes host, so a same-host `https`→`http` hop would otherwise carry the
service-account token onward in the clear.

**Timeouts.** `CLIENT_UPDATE_UPLOAD_TIMEOUT` (default `10m`) is ONE budget for
the whole request — reading the browser's body and calling
`client-update-service` — pinned on the request context. The two phases are
sequential rather than overlapping: the handler spools the inbound parts in full
before it dials upstream, because the outbound body is sent with an exact
`Content-Length` and that length is only known once every part has arrived. Without
a single budget the upstream call would start a second full timeout on top of the
inbound one.

Which half the budget ran out in decides the answer, because the two have
different remedies: still receiving the browser's body is a `400` (the sender was
too slow), still waiting on `client-update-service` is a `503` (the upstream was).

The rest is ordered around it, so that whichever budget expires first the admin
still gets an envelope rather than a dropped connection: that value < that value
+ 30s (this request's write deadline) < the browser's own upload timeout.
`client-update-service`'s `HTTP_WRITE_TIMEOUT` should be at least
`CLIENT_UPDATE_UPLOAD_TIMEOUT` so it does not abandon a request this service is
still waiting on. Raising one without the others reintroduces a window where a
published upload is reported as a failure.

---

## 10. Botplatform Service

HTTP REST endpoints served by `botplatform-service` — the authoritative password-login and session-token store for bot/admin accounts. Any user may authenticate against any cluster (there is no home-site gate). Sessions are permanent (no TTL); the per-user cap is `SESSIONS_MAX_PER_ACCOUNT` (default 100), FIFO-evicted by `issuedAt` on overflow.

### 10.1 HTTP — POST /api/v1/login (bot SDK direct)

**Endpoint:** `POST /api/v1/login`
**Reply:** synchronous HTTP response

Direct bot-SDK login at botplatform-service. Returns the **legacy Rocket.Chat 3-field shape** `{userId, authToken, me}` so existing bot SDKs need no code changes. The portal response (§2.5) is for web/mobile/desktop/admin clients only.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `username` | string | yes | Account name (matches `users.account`). Must hold the `bot` or `admin` role. |
| `password` | string | yes | Plaintext password. |

```json
{
  "username": "name.shortcode.bot",
  "password": "<secret>"
}
```

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"success"` on the 200 path (legacy envelope shape). |
| `data.userId` | string | 17-char user identifier; mirrored in the `X-User-Id` header that the client sends on subsequent calls. |
| `data.authToken` | string | 43-char base64url opaque session token (wire-identical to legacy Rocket.Chat — no `bp_` prefix). Sent on subsequent calls as the `X-Auth-Token` header. |
| `data.me` | object | The legacy `me` block; see [Me](#me-botplatform) below. |

##### Me (botplatform)

| Field | Type | Notes |
|---|---|---|
| `_id` | string | Same as `data.userId`. |
| `username` | string | Same as the request's `username`. |
| `name` | string | Display name. |
| `active` | boolean | Always `true` on a successful login. |
| `roles` | string[] | Role tags (e.g. `["bot"]`, `["admin"]`). |
| `requirePasswordChange` | boolean | First-login flag, mirrored from the user doc. |

```json
{
  "status": "success",
  "data": {
    "userId": "abcdef1234567890x",
    "authToken": "<43-char base64url>",
    "me": {
      "_id": "abcdef1234567890x",
      "username": "name.shortcode.bot",
      "name": "FOD Bot",
      "active": true,
      "roles": ["bot"],
      "requirePasswordChange": false
    }
  }
}
```

#### Error response

See [Error envelope](#6-error-envelope-reference). HTTP statuses:

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `missing_fields` | `username` or `password` empty. |
| 401 | `unauthenticated` | `invalid_credentials` | Uniform: unknown account, wrong password, or account without `bot`/`admin` role. |
| 500 | `internal` | — | Mongo failure; cause logged server-side. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

### 10.2 HTTP — POST /api/v1/auth/validate

**Endpoint:** `POST /api/v1/auth/validate`
**Reply:** synchronous HTTP response

Validates a session `authToken` and returns the associated principal. Called by auth-service (§2.2) when minting a NATS JWT for a session-token holder, and by the gateway during request routing. Validation is local-DB only — cross-site routing is the caller's responsibility.

This endpoint is intended for **server-to-server use**; bot SDKs do not call it directly.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `authToken` | string | yes | The 43-char raw token returned by `/api/v1/login`. |

```json
{ "authToken": "<43-char base64url>" }
```

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `valid` | boolean | `true` on a session match. |
| `principal.userId` | string | 17-char user identifier. |
| `principal.account` | string | The `{account}` used in NATS subjects. |
| `principal.siteId` | string | The user's home site. |
| `principal.roles` | string[] | Roles at the time the session was issued (denormalized). |

```json
{
  "valid": true,
  "principal": {
    "userId": "abcdef1234567890x",
    "account": "name.shortcode.bot",
    "siteId": "site-a",
    "roles": ["bot"]
  }
}
```

#### Error response

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `missing_fields` | `authToken` empty. |
| 401 | `unauthenticated` | `invalid_token` | Token hash not found. Body carries `{"valid": false, ...}`. |
| 500 | `internal` | — | Mongo failure. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

### 10.3 Bot HTTP endpoints — routing model

The 5 bot endpoints below all resolve to a target site before forwarding to
`bot-message-handler` / `bot-room-service` over NATS. BP reads the bot's local
Mongo subscription for the operation's key (`roomID` for room-scoped ops,
`otherAccount` for DMs) and forwards to the site the subscription's
`siteId` names. **The message canonical stream, Cassandra write, and any
local `sysmsg` all happen at the room's home site** — the same rule the
user pipeline follows via `chat.user.{account}.request.room.{roomID}.{siteID}.…`.

| Endpoint | Sub lookup | Site the RPC runs at | Missing sub |
|---|---|---|---|
| `POST /rooms/:roomID/messages` | `(bot, roomID)` | `sub.siteId` | 403 `not_a_room_member` |
| `POST /dms/:userID/messages` | `(bot, otherAccount)` | `sub.siteId`; else BP's own site (first-DM auto-created there) | Triggers DM-ensure at BP's own site |
| `POST /rooms` | n/a — creating | BP's own site (new rooms live where the creator lives) | n/a |
| `POST /rooms/:roomID/members` | `(bot, roomID)` | `sub.siteId` | 403 `not_a_room_member` |
| `DELETE /rooms/:roomID/members` | `(bot, roomID)` | `sub.siteId` | 403 `not_a_room_member` |

All 5 endpoints require an authenticated bot session via `X-User-Id` +
`X-Auth-Token` (see §10.1). Per-caller and global token-bucket rate
limits apply (Valkey `IncrEx`); an idempotency sentinel keyed on
`(siteID, endpoint, opID)` (Valkey `SetNX`) rejects duplicates inside
its TTL window (30 s for message flow, 60 s for room management).
Request bodies are capped at 96 KiB; oversized payloads are rejected
during read via `http.MaxBytesReader`.

---

### 10.4 HTTP — POST /api/v1/rooms/:roomID/messages

**Endpoint:** `POST /api/v1/rooms/:roomID/messages`
**Reply:** synchronous HTTP response

Sends a message into an existing room the bot is a member of. Returns
the canonical `Message` document that landed on
`BOT-MESSAGES-CANONICAL-{sub.siteId}`.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `content` | string | yes | 1 ≤ length ≤ 20 KiB (bytes). Empty rejected as `content_invalid`. |
| `mentions` | `Participant[]` | no | Server re-canonicalises against the users collection; any mention not a room member rejects the whole request. |
| `card` | `Card` | no | Optional AdaptiveCard payload (same shape as user messages). |
| `threadParentMessageId` | string | no | If set, this is a thread reply. |
| `threadParentMessageCreatedAt` | timestamp | no | Optional; server re-resolves if absent. |
| `tshow` | boolean | no | Only meaningful when `threadParentMessageId` is set. |

Attachments are rejected as `unknown_field` — bots send text and cards only.

#### Success response

`HTTP 200` — the canonical `Message` document.

#### Error response

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `content_invalid` | Empty / oversized content, malformed JSON, empty body. |
| 400 | `bad_request` | `unknown_field` | Body contained a field not in the schema (e.g. `attachments`). |
| 400 | `bad_request` | `mention_invalid` | A mention userId is missing or not a room member. |
| 400 | `bad_request` | `invalid_header` | Missing / malformed `X-Bot-Identity` (BP wiring bug). |
| 401 | `unauthenticated` | `invalid_token` | Session token missing / hash unknown / lacks bot role. |
| 403 | `forbidden` | `not_a_room_member` | Bot has no subscription to `roomID`. |
| 404 | `not_found` | `room_not_found` | Room disappeared between subscription lookup and forward. |
| 409 | `conflict` | `in_flight` | Duplicate op detected inside the idempotency window. |
| 429 | `too_many_requests` | `rate_limited_caller` \| `rate_limited_global` | Per-caller or global RPM cap exceeded. |
| 503 | `unavailable` | `handler_timeout` | Downstream NATS request timed out (3 s budget). |

---

### 10.5 HTTP — POST /api/v1/dms/:userID/messages

**Endpoint:** `POST /api/v1/dms/:userID/messages`
**Reply:** synchronous HTTP response

Sends a DM to `userID` (the counterparty's user ID). First-time DM
auto-creates the room at BP's own site with a deterministic ID from
`idgen.BuildDMRoomID(botID, otherID)` — both parties converge on the
same `_id` if they race. Subsequent messages route to the room's home
site via subscription lookup.

Request body: identical to §10.4.
Success response: identical to §10.4.

Error response: identical to §10.4 with these additions:

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `cannot_dm_self` | Target user is the bot itself. |
| 404 | `not_found` | `dm_target_not_found` | Counterparty user does not exist at any site. |
| 503 | `unavailable` | `dm_ensure_unavailable` | DM-ensure client wiring gap. |

---

### 10.6 HTTP — POST /api/v1/rooms

**Endpoint:** `POST /api/v1/rooms`
**Reply:** synchronous HTTP response

Creates a new channel room at BP's own site with the bot as owner.
Optional seed members / orgs. For remote members, subscriptions federate
through OUTBOX → inbox-worker at their home site.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | yes | Room name; empty rejected as `content_invalid`. |
| `topic` | string | no | Optional room topic. |
| `members` | string[] | no | Seed user IDs. Max 100. |
| `orgs` | string[] | no | Seed org IDs. Max 5. Org expansion not yet supported (rejected as `unsupported`). |

#### Success response

`HTTP 200` — endpoint-specific envelope from `bot-room-service` passed
through verbatim: `{ id, name, owner, members[], createdAt }`.

#### Error response

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 400 | `bad_request` | `content_invalid` | Missing name, malformed body. |
| 400 | `bad_request` | `batch_too_large` | `members` > 100 or `orgs` > 5. |
| 400 | `bad_request` | `unsupported` | Non-empty `orgs` (Phase 3+). |
| 409 | `conflict` | `room_exists` | Deterministic room ID collision. |
| 429 / 503 | | | Same as §10.4. |

---

### 10.7 HTTP — POST /api/v1/rooms/:roomID/members and DELETE /api/v1/rooms/:roomID/members

**Endpoints:**
- `POST   /api/v1/rooms/:roomID/members` — add members
- `DELETE /api/v1/rooms/:roomID/members` — remove members

Both operate on the same body shape and follow the room-anchored
routing rule (§10.3): the RPC runs at the room's home site and OUTBOX
federates the subscription delta to each affected remote site.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `userIds` | string[] | at least one of `userIds` / `orgIds` non-empty | Max 100. |
| `orgIds` | string[] | | Max 5. Not yet supported (rejected as `unsupported`). |

#### Success response

`HTTP 200` — endpoint-specific envelope from `bot-room-service`:
- add: `{ "added": { "userIds": [...], "orgIds": [...] } }`
- remove: `{ "removed": { "userIds": [...], "orgIds": [...] } }`

#### Error response

Same catalogue as §10.4 plus:

| Status | `code` | `reason` | Notes |
|---|---|---|---|
| 403 | `forbidden` | `not_a_room_owner` | Caller is not the room's owning bot. |
| 403 | `forbidden` | `cannot_remove_self` | Bot cannot remove itself from its own room. |
| 404 | `not_found` | `member_not_found` | A user in `userIds` does not exist. |

---

## 11. tcard-service

Serves versioned **AdaptiveCard** template documents from an in-memory cache backed by the MongoDB `cards` collection. Clients read a template by path + version (§11.1); an admin review client validates candidate templates before publishing them (§11.2). The internal `POST /api/v1/cards/refresh` (service-to-service cache reload) is **not** a client API and is intentionally omitted here.

### 11.1 HTTP — GET /api/v1/cards/{path}@{cardVersion}.template.json

**Endpoint:** `GET /api/v1/cards/{path}@{cardVersion}.template.json`
**Reply:** synchronous HTTP response

Fetches one card template by `path` and `cardVersion`, served from cache with no per-request database read. The service strips the `.template.json` suffix, splits the remainder on the **last** `@` into `path` and `cardVersion`, and returns the stored document verbatim minus the routing `path` and the storage-only fields `_id` and `migratedAt`. Only top-level keys are removed — an `id` on an element inside `body` is preserved.

#### Request

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `path` | URL | string | yes | The card path — everything before the last `@`. Contains `/` separators (`a/b/c`), matched by the route's trailing wildcard, so it spans multiple URL segments. |
| `cardVersion` | URL | string | yes | Semantic version `a.b.c` — between the last `@` and the `.template.json` suffix. |

```http
GET /api/v1/cards/greetings/en/welcome@1.0.0.template.json
```

#### Success response

`HTTP 200`, `Content-Type: application/json`. The stored card document minus `path`, `_id` and `migratedAt`.

| Field | Type | Notes |
|---|---|---|
| `_tcardVersion` | string | The document's semantic version. |
| `cardUsage` | any | Optional usage metadata; absent when the card has none. |
| `type` | string | `"AdaptiveCard"`. |
| `schema` | string | `"http://adaptivecards.io/schemas/adaptive-card.json"`. |
| `version` | string | AdaptiveCard schema version, `"1.5"`. |
| `body` | array | AdaptiveCard body elements. |

```json
{
  "_tcardVersion": "1.0.0",
  "cardUsage": "greeting",
  "type": "AdaptiveCard",
  "schema": "http://adaptivecards.io/schemas/adaptive-card.json",
  "version": "1.5",
  "body": [{ "type": "TextBlock", "id": "greeting", "text": "Hi" }]
}
```

#### Error response

See [Error envelope](#6-error-envelope-reference). HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "card template request must include a version: {path}@{version}.template.json" }` — missing `@` version separator or empty path/version. A request without the `.template.json` suffix is a directory listing instead (see below), not an error. |
| 404 | `not_found` | — | `{ "code": "not_found", "error": "card template not found" }` — no cached card for that `(path, cardVersion)`. |

#### Directory listing

The same route without the `.template.json` suffix lists the **direct children** of the requested prefix. `GET /api/v1/cards/` lists the root; `GET /api/v1/cards/greetings/en` lists that folder.

| Field | Type | Notes |
|---|---|---|
| `statusCode` | integer | Always `200`. |
| `cards` | string[] | Direct child cards as `path@cardVersion`, sorted by path then descending semver. Empty array when none. |
| `folders` | string[] | Direct child folders as full paths from root, sorted. Empty array when none. |

```json
{ "statusCode": 200, "cards": ["greetings/en/welcome@1.0.0"], "folders": [] }
```

Listing errors: `400 bad_request` `no version specified for card "<prefix>"` when the prefix names a card exactly (add `@version.template.json`); `404 not_found` `given path "<prefix>" for card list not found` for an unknown prefix; `404 not_found` `no paths or cards exist` before the cache's first load.

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

### 11.2 HTTP — POST /api/v1/cards/validate (admin)

**Endpoint:** `POST /api/v1/cards/validate`
**Reply:** synchronous HTTP response

**Admin only.** Checks a card document and stores nothing: an admin review client calls this to confirm a submitted card is well-formed and correctly versioned before it is published. A validated card is **not** inserted into `cards` and **not** added to the cache — it becomes servable via §11.1 only once it is written to the collection and the cache reloads. Not for end-user browsers. The request body is the full card document.

#### Request

| Field | Type | Required | Notes |
|---|---|---|---|
| `path` | string | yes | Card path: exactly 3 non-empty `/`-separated segments (`a/b/c`). Must not contain `@`. |
| `_tcardVersion` | string | yes | Semantic version `a.b.c`, no leading zeros. Must be **strictly greater** than every cached `_tcardVersion` for this `path`. |
| `cardUsage` | any | no | Optional usage metadata. Not validated. |
| `type` | string | yes | Must equal `"AdaptiveCard"`. |
| `schema` | string | yes | Must equal `"http://adaptivecards.io/schemas/adaptive-card.json"`. |
| `version` | string | yes | Must equal `"1.5"`. |
| `body` | array | yes | Non-empty AdaptiveCard body array. |

```http
POST /api/v1/cards/validate
Content-Type: application/json
```

```json
{
  "path": "greetings/en/welcome",
  "_tcardVersion": "1.0.0",
  "cardUsage": "greeting",
  "type": "AdaptiveCard",
  "schema": "http://adaptivecards.io/schemas/adaptive-card.json",
  "version": "1.5",
  "body": [{ "type": "TextBlock", "text": "Hi" }]
}
```

#### Success response

`HTTP 200`.

| Field | Type | Notes |
|---|---|---|
| `success` | boolean | Always `true`. |

```json
{ "success": true }
```

#### Error response

See [Error envelope](#6-error-envelope-reference). HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "body must be a non-empty array" }` — malformed JSON, a missing/empty required field, a `path` that isn't 3 non-empty segments or that contains `@`, a non-semver or leading-zero `_tcardVersion`, a non-array `body`, or a `type`/`schema`/`version` that isn't the pinned value. |
| 409 | `conflict` | — | `{ "code": "conflict", "error": "_tcardVersion must be the highest for this path" }` — `_tcardVersion` isn't strictly higher than every cached version for this `path`. |
| 503 | `unavailable` | — | `{ "code": "unavailable", "error": "card cache not loaded" }` — the cache has not finished its first load, so version ordering can't be judged yet. |

Ordering is judged against the in-memory cache, which is as fresh as the last refresh (at startup, daily, or on demand). A card written to `cards` out-of-band since then is not yet counted.

**This endpoint is advisory — it does not reserve the version or gate publishing.** The publishing path writes to `cards` directly and is not required to call this first; the collection's only hard constraint is the unique `(path, _tcardVersion)` index, which rejects an exact duplicate but accepts a *lower* version. A `200` means the card was well-formed and correctly ordered at the time of the call, not that publishing it is guaranteed safe.

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---

## 12. Client Update Service

Public HTTP endpoints served by `client-update-service`. Distributes client
software-update artifacts (a `.yaml` descriptor + an executable) stored in MinIO.
Uploads and downloads stream end-to-end; downloads are fronted by a bounded
TTL+size in-memory cache.

> [!WARNING]
> **`GET /api/v1/version/:fileName` is UNAUTHENTICATED.** Anyone who can reach the
> service can download update artifacts. It **MUST be network-restricted**. Uploads
> are gated on a service account (below).

### POST /api/v1/version

**Auth:** `Authorization: Bearer <service-account token>`. The token must match an
entry in the service's `UPLOAD_TOKENS` table (`account:token`, comma-separated).
Only `admin-service` is provisioned; browsers and end-user clients never call this
endpoint directly — they go through
[`POST /v1/admin/client-updates`](#917-http--post-v1adminclient-updates).

`UPLOAD_TOKENS` is **optional**. Unset or empty authorizes nobody: the service
starts normally and answers **every** upload with `401`, so a site that does not
publish client updates can deploy without it. Downloads are unaffected.

Uploads an update-artifact pair as `multipart/form-data`. Both parts are required
and there is no size cap. An upload of an existing file name overwrites it and
evicts any cached copy.

**Disk-backed, not end-to-end streamed.** The handler reads its parts via
`c.FormFile`, which buffers up to 32 MiB in memory and spills the remainder to
a temporary file before the object is written to MinIO. Size the container's
ephemeral storage for the largest artifact you intend to publish. The MinIO
write itself streams from that temporary file.

The two objects are written **independently, not atomically**, with no locking
between requests. Two consequences:

- A failure after the first write returns an error with that object already
  replaced, so a downloader sees the new descriptor beside the previous
  executable until the pair is re-uploaded.
- Two uploads in flight at once can interleave their writes, leaving the
  descriptor from one submission beside the executable from the other — and
  **both callers receive `200`**. Publish one artifact pair at a time.

Versioning and rollback are out of scope for this service.

#### Request

| Part | Type | Required | Notes |
|---|---|---|---|
| `configFile` | file (`.yaml`/`.yml`) | yes | Update descriptor. Stored with the part's declared `Content-Type` (fallback `application/x-yaml` when the part sends none). Rejected if empty or not `.yaml`/`.yml`. |
| `executeFile` | file (binary) | yes | The executable. Stored with the part's declared `Content-Type` (fallback `application/octet-stream` when the part sends none). Rejected if empty. |

#### Response

| Status | Condition |
|---|---|
| `200 OK` | Both files stored. |
| `400 Bad Request` | Missing/empty `configFile` or `executeFile`; `configFile` not `.yaml`/`.yml`; malformed multipart body. |
| `401 Unauthorized` | Missing, malformed, or unrecognized service-account token. Identical response for all three. |
| `500 Internal Server Error` | MinIO write failure. |

##### Success response (`200`)

| Field | Type | Notes |
|---|---|---|
| `result` | string | Always `"success"`. |

```json
{ "result": "success" }
```

### GET /api/v1/version/:fileName

**Auth:** none (v1)

Downloads an artifact by file name. Served from an in-memory cache when present
(TTL `CACHE_TTL`, default 24h); on a miss the object is fetched from MinIO and
cached if it is within `CACHE_MAX_OBJECT_BYTES` (default 512 MiB), otherwise
streamed uncached. A re-upload of the same name busts the cache.

#### Response

| Status | Condition | Notes |
|---|---|---|
| `200 OK` | Artifact found | Streams the bytes. `Content-Type` as stored; `Content-Disposition: attachment; filename="<fileName>"`; `Content-Length` set. |
| `400 Bad Request` | Empty or path-unsafe `fileName` (contains `/`, `\`, or `..`) | |
| `404 Not Found` | No artifact with that name | |
| `500 Internal Server Error` | MinIO read failure | |

```
GET /api/v1/version/app.yaml   -> 200 (application/x-yaml)
GET /api/v1/version/app.exe    -> 200 (application/octet-stream)
```

### GET /healthz

**Auth:** none

Liveness probe.

#### Success response (`200`)

| Field | Type | Notes |
|---|---|---|
| `status` | string | Always `"ok"`. |

```json
{ "status": "ok" }
```

---

## 13. User Service HTTP API

HTTP endpoints served by `user-service`, alongside its NATS request/reply surface. They exist because a NATS reply is capped at **128 KB**, which a full sidebar exceeds: a user with 200–300 subscriptions must otherwise page at ~40 rows and issue 5–8 round trips at startup. Over HTTP the same data arrives in one request.

**Base URL:** the user-service HTTP listener (`HTTP_PORT`, default `8080`). Health probes are on a separate port (`HEALTH_ADDR`, default `:8081`) and are never rate-limited.

### 13.1 Authentication

Every `/api/v1` endpoint takes exactly one credential:

| Header(s) | Credential | Notes |
|---|---|---|
| `ssoToken` | OIDC access token | **Recommended.** Verified locally against cached JWKS — no network hop. |
| `x-user-id` + `x-auth-token` | Botplatform session | Costs a botplatform round trip, and `pkg/botauth` caps concurrent validations at 64. |

Supplying both is a `400`. The account is taken from the verified credential, never from the request, so a caller can only ever read its own data — the same guarantee NATS subject scoping provides.

> **Use `ssoToken` for sidebar initialization.** The session-token path depends on botplatform being reachable and shares a 64-validation ceiling, which binds well before the server's own concurrency cap during a mass reconnect.

### 13.2 HTTP — GET /api/v1/subscriptions

The HTTP form of [`subscription.list`](#subscriptionlist). Same enrichment, same response body, no payload ceiling.

#### Query parameters

| Parameter | Type | Required | Notes |
|---|---|---|---|
| `type` | string | yes | One of `current`, `rooms`, `apps`. Same meaning as the NATS field. |
| `favorite` | boolean | no | Filters to favorites and pins the caller's self-DM first. |
| `updatedWithinDays` | number | no | `rooms` only. Must be non-negative. |
| `includeLastMessage` | boolean | no | Omitted ⇒ include. Send `false` to skip the per-room `previewMessage` resolve. |
| `offset` | integer | no | Zero-based. Negative ⇒ `0`. Default `0`. |
| `limit` | integer | no | Omitted or ≤ 0 ⇒ `HTTP_SUBSCRIPTION_DEFAULT_LIMIT` (default `40`); values above `HTTP_SUBSCRIPTION_MAX_LIMIT` (default `400`) are capped to it. |

> **Send `limit=200`.** It covers a typical sidebar in one request. Larger pages cost the server nothing extra in database work — the room join and activity sort run over the account's full subscription set regardless of page size, so one 200-row page is roughly 5× cheaper in total than five 40-row pages.

```http
GET /api/v1/subscriptions?type=current&limit=200 HTTP/1.1
ssoToken: <oidc-access-token>
Accept-Encoding: gzip
```

#### Success response

Identical to the NATS reply — see [`subscription.list`](#subscriptionlist) for the full row schema.

| Field | Type | Notes |
|---|---|---|
| `subscriptions` | array<[Subscription](#subscription)> | One page of room-info-enriched records. |
| `hasMore` | boolean | `true` when at least one more record follows this page. |

```json
{
  "subscriptions": [
    {
      "id": "01970a4f8c2d7c9a01970a4f8c2d7c9b",
      "u": { "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a", "account": "alice", "isBot": false },
      "roomId": "01970a4f8c2d7c9aQ",
      "siteId": "siteA",
      "roomType": "channel",
      "roles": ["member"],
      "name": "engineering-general",
      "joinedAt": "2026-05-06T08:01:23Z",
      "hasMention": false,
      "hasUnread": true,
      "hasGroupMention": false,
      "alert": true,
      "muted": false,
      "favorite": true,
      "room": {
        "siteId": "siteA",
        "name": "engineering-general",
        "userCount": 42,
        "lastMsgAt": "2026-06-01T10:00:00Z",
        "previewMessage": {
          "messageId": "01970a4f8c2d7c9aBB",
          "sender": { "userId": "01970a4f8c2d7c9a", "account": "alice", "displayName": "Alice" },
          "content": "morning team",
          "createdAt": "2026-06-01T10:00:00Z"
        }
      }
    }
  ],
  "hasMore": true
}
```

**Compression.** Send `Accept-Encoding: gzip`. A 200-row page is roughly 150 KB of JSON and compresses to a small fraction of that. Responses under 1 KB are returned uncompressed.

**Request correlation.** Send `X-Request-ID` as a hyphenated UUID to correlate with server logs; the server mints one when absent and echoes it on every response.

#### Error responses

Same envelope as every other endpoint (see [§6](#6-error-envelope-reference)).

| Condition | Status | `code` | `reason` |
|---|---|---|---|
| Unparseable parameter (e.g. `limit=abc`) | `400` | `bad_request` | — |
| Unknown `type` | `400` | `bad_request` | — |
| Negative `updatedWithinDays` | `400` | `bad_request` | — |
| Both `ssoToken` and `x-auth-token` supplied | `400` | `bad_request` | `ambiguous_token` |
| No credential supplied | `401` | `unauthenticated` | `missing_fields` |
| Invalid credential | `401` | `unauthenticated` | `invalid_sso_token` |
| Expired SSO token | `401` | `unauthenticated` | `sso_token_expired` |
| Credential type not configured on this deployment | `503` | `unavailable` | `upstream_unavailable` |
| Pod at its in-flight capacity | `429` | `too_many_requests` | `overloaded` |
| Server exceeded its own handler budget | `503` | `unavailable` | — |
| Internal failure | `500` | `internal` | — |

##### 429 — server at capacity

The pod caps concurrent in-flight requests (`HTTP_MAX_CONCURRENCY`, default `256`) and sheds the overflow immediately rather than queueing work whose client has already timed out.

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 1
Content-Type: application/json
X-Request-ID: 01970a4f-8c2d-7c9a-abcd-e0123456789f

{
  "code": "too_many_requests",
  "error": "server is at capacity, retry shortly",
  "reason": "overloaded"
}
```

**Client contract:** honour `Retry-After` and retry with jitter. A 429 means *this pod* is momentarily full, not that the request was wrong — an immediate uniform retry from every client re-creates the burst that caused it. Do not treat it as a fatal error or force re-login.
