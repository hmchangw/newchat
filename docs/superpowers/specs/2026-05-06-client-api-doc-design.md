# Client API Documentation — Design

## Purpose

Produce a single integrator-facing reference, `docs/client-api.md`, that
documents the complete client-facing surface of the chat backend: every
NATS request/reply method a client can call, the message-send publish flow,
the HTTP authentication endpoint, and — for each of those — the
server-pushed events the client will receive as a result of the action.

The audience is a developer (web, mobile, or third-party) writing a client
that connects to the chat backend. They need to know what to publish, what
to subscribe to, what the payloads look like on both success and failure,
and which downstream events to expect after each action.

## Scope

In scope:

- HTTP `POST /auth` (auth-service) — full schema and examples.
- NATS request/reply methods exposed by:
  - `room-service` — rooms.create, rooms.list, rooms.get, member.add,
    member.remove, member.role-update, member.list, orgs.{orgID}.members.
  - `history-service` — msg.history, msg.next, msg.surrounding, msg.get,
    msg.edit, msg.delete, msg.thread, msg.thread.parent.
  - `search-service` — search.messages, search.rooms.
- The message-send publish flow handled by `message-gatekeeper`
  (`chat.user.{account}.room.{roomID}.{siteID}.msg.send`), which replies
  asynchronously on `chat.user.{account}.response.{requestID}`. This single
  method covers plain messages, thread replies, and quoted messages —
  variations are different optional fields in the same payload.
- For every method above: the success-path and error-path server-pushed
  events the client will receive, with payload schemas and examples.

Out of scope:

- Backend-only subjects (MESSAGES, MESSAGES_CANONICAL, FANOUT, OUTBOX,
  INBOX, ROOMS streams). These are documented elsewhere
  (`docs/nats-subject-naming.md`) and clients never interact with them.
- Server-pushed events not triggered by a client RPC (federation arrivals,
  presence, room-key rotation, cross-site member events). Removed from
  scope at user request — clients don't need to act on these to integrate.
- Internal handler implementation details, queue groups, and Go subject
  builders — irrelevant to a client integrator.
- Server-to-server subjects (`chat.server.request.…`).

## Document location and format

- Path: `docs/client-api.md` (single file, Markdown).
- Hand-maintained. No code generation in this iteration; revisit if drift
  becomes a real maintenance problem.
- Linked from `README.md` and from `docs/nats-subject-naming.md` (which
  remains the authoritative subject-naming reference for backend developers
  and complements but does not replace the client doc).

## Top-level structure

```
1. Overview
2. Connection & Auth
3. Request/Reply Methods
   3.1 room-service
   3.2 history-service
   3.3 search-service
4. Message Send (publish + async reply)
5. Error envelope reference
```

### Section 1 — Overview

A short page (under one screen) covering:

- What the doc covers and what it does not.
- The four conventions every method block relies on:
  - Subject placeholders: `{account}`, `{roomID}`, `{siteID}`,
    `{requestID}` — what each represents.
  - All NATS payloads are JSON.
  - ID formats clients see: room IDs (channel = 17-char base62; DM =
    sorted concat of two accounts), message IDs (17- or 20-char base62),
    request IDs (36-char hyphenated UUIDv7).
  - Request-ID propagation: clients may set the `X-Request-ID` NATS
    message header on outbound requests (must be a valid hyphenated UUID
    per `idgen.IsValidUUID`); if absent, the server generates one.
- The async-reply pattern used by `msg.send`:
  client publishes with a `requestID` field in the payload, then receives
  the reply on `chat.user.{account}.response.{requestID}`. Distinct from
  standard NATS request/reply (which uses `_INBOX.>` automatically).

### Section 2 — Connection & Auth

Two subsections:

#### 2.1 NATS connection

- The `chat.user.{account}.>` namespace rule: clients may publish only
  under their own namespace; they may subscribe to their own namespace
  plus `chat.room.>` and `_INBOX.>`. (Pulled from auth-service's
  `signNATSJWT` permission set.)
- Standard reply pattern: NATS request/reply uses an auto-generated
  `_INBOX.>` reply subject (the NATS client library handles this).
- Async reply pattern (msg.send only): documented here once and then
  referenced from §4 — client sets a `requestID` in the payload, then
  awaits `chat.user.{account}.response.{requestID}`.

#### 2.2 HTTP — POST /auth

Documents the auth-service endpoint that exchanges an SSO token for a
signed NATS user JWT. Uses the same per-method block format as §3 (see
"Per-method block" below), with HTTP-specific fields substituted:

```
### Authenticate

Endpoint:   POST /auth
Reply:      synchronous HTTP response

Request body
  Schema:
    | Field          | Type   | Required | Notes |
    | ssoToken       | string | yes      | OIDC-issued SSO token |
    | natsPublicKey  | string | yes      | NATS user public NKey |
  Example:
    {
      "ssoToken": "<sso-token>",
      "natsPublicKey": "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4"
    }

Success response  (HTTP 200)
  Schema:
    | Field         | Type   | Notes |
    | natsJwt       | string | Signed NATS user JWT, used to connect |
    | user.email    | string |       |
    | user.account  | string | Used as {account} in all NATS subjects |
    | user.employeeId, user.engName, user.chineseName, user.deptName, user.deptId
  Example:
    {
      "natsJwt": "eyJ0eXAiOiJKV1Q…",
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

Error response
  Schema: { "error": string }
  Statuses:
    400 — missing/invalid fields, invalid natsPublicKey
    401 — SSO token expired or invalid
    500 — server-side JWT signing failure
  Example:
    { "error": "SSO token has expired, please re-login" }

Triggered events — success path
  None — HTTP-only.

Triggered events — error path
  None.
```

Dev-mode variant (accepts `account` instead of `ssoToken`) is mentioned
briefly with a note that it is local-only and not part of the production
contract.

### Section 3 — Request/Reply Methods

One subsection per service, each containing one method block per
client-facing RPC. All blocks use the same format described under
"Per-method block" below.

For each method, the spec author writing `docs/client-api.md` must
inspect the actual handler and Go request/response types to fill in the
field tables. The list of methods to cover is fixed by what the services
register (see references below).

### Section 4 — Message Send

A single method block for `chat.user.{account}.room.{roomID}.{siteID}.msg.send`,
expanded with three example payloads side-by-side to cover plain message,
thread reply, and quoted message. Payload schema lists `parentMessageId`
and `quotedMessage` as optional fields and shows when each is set.

The "Triggered events — success path" subsection lists the events the
sending client and other room members will receive (room message stream
event, room metadata update, mention notification for mentioned users)
with payload examples for each. The "error path" subsection documents the
error reply on the response subject.

### Section 5 — Error envelope reference

Documents `model.ErrorResponse`:

```json
{ "error": "<human-readable reason>" }
```

Notes:

- All NATS reply errors use this envelope, sent via
  `natsutil.ReplyError`. The error string is sanitized at service
  boundaries — never expects raw internal error text.
- All HTTP errors from `auth-service` use the same shape with an HTTP
  status code (400, 401, 500).
- This envelope is referenced from every method's "Error response"
  subsection so we don't repeat the schema in every block.

## Per-method block format (used in §2.2, §3.x, §4)

Every method, regardless of service, uses this exact block layout:

```
### <Human-readable method name>

Subject:        <full subject, with {placeholders}>
Reply subject:  auto-generated _INBOX.> (NATS request/reply)
                — OR —
                chat.user.{account}.response.{requestID}   (msg.send only)

Request body
  Schema:   <Go struct name in pkg/model> rendered as a field table:
            | Field | Type | Required | Notes |
  Example:  ```json { ... } ```

Success response
  Schema:   <Go struct name> as a field table.
  Example:  ```json { ... } ```

Error response
  Schema:   model.ErrorResponse — see §5.
  Example:  ```json { "error": "<example reason>" } ```

Triggered events — success path
  For each event the client (or another client in the same room) will
  receive after this RPC succeeds:
    Subject:    <full subject>
    Recipients: <who receives it: requester / room members / invited users>
    Payload:    <Go struct> as a field table.
    Example:    ```json { ... } ```
  If none, write exactly: "None — reply only."

Triggered events — error path
  For each event the client may receive when this RPC fails (uncommon —
  most failures return only the reply error). If none, write exactly:
    "None — error returned only via the reply subject."
```

Header fields removed at user request: Builder (Go-only), Direction
(implied by section), Queue group (irrelevant to clients).

### Field-table conventions

- `Type` uses JSON-level types (`string`, `number`, `boolean`,
  `object`, `array<T>`, `string[]`) — not Go types like `int64`.
- `Required` is `yes` / `no`. For `no`, note default behavior or what
  omitting the field means.
- Timestamps are documented as "milliseconds since Unix epoch (UTC)".
- IDs reference the format conventions from §1.

### Example conventions

- Use realistic-looking IDs: a 36-char hyphenated UUIDv7 for
  `requestID`, 17-char base62 for channel-room IDs, 20-char base62 for
  message IDs, account names like `alice` / `bob`. This keeps copy-paste
  examples meaningful for integrators.
- Use stable example accounts (`alice`, `bob`, `carol`) and a stable
  example site ID (`siteA`) across the doc so cross-method examples
  read as a coherent story.
- Pretty-print JSON across multiple lines.

## Method coverage checklist

The client doc must cover every method below. The implementation plan
(produced after this design is approved) will translate this list into
per-method writing tasks.

### §2.2 HTTP

- POST /auth

### §3.1 room-service

Source of truth: `room-service/handler.go` (`registerNats`):

- Create Room — `chat.user.{account}.request.rooms.create`
- List Rooms — `chat.user.{account}.request.rooms.list`
- Get Room — `chat.user.{account}.request.rooms.get.{roomID}`
- Add Members — `chat.user.{account}.request.room.{roomID}.{siteID}.member.add`
- Remove Member — `chat.user.{account}.request.room.{roomID}.{siteID}.member.remove`
- Update Member Role —
  `chat.user.{account}.request.room.{roomID}.{siteID}.member.role-update`
- List Members — `chat.user.{account}.request.room.{roomID}.{siteID}.member.list`
- List Org Members — `chat.user.{account}.request.orgs.{orgID}.members`

(Excluded: `chat.server.request.room.{siteID}.info.batch` — server-to-server.)

### §3.2 history-service

Source of truth: `history-service/internal/service/service.go`:

- Load History — `chat.user.{account}.request.room.{roomID}.{siteID}.msg.history`
- Load Next Messages —
  `chat.user.{account}.request.room.{roomID}.{siteID}.msg.next`
- Load Surrounding Messages —
  `chat.user.{account}.request.room.{roomID}.{siteID}.msg.surrounding`
- Get Message By ID —
  `chat.user.{account}.request.room.{roomID}.{siteID}.msg.get`
- Edit Message — `chat.user.{account}.request.room.{roomID}.{siteID}.msg.edit`
- Delete Message —
  `chat.user.{account}.request.room.{roomID}.{siteID}.msg.delete`
- Get Thread Messages —
  `chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread`
- Get Thread Parent Messages —
  `chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread.parent`

### §3.3 search-service

Source of truth: `search-service/handler.go`:

- Search Messages — `chat.user.{account}.request.search.messages`
- Search Rooms — `chat.user.{account}.request.search.rooms`

### §4 message-gatekeeper

Source of truth: `message-gatekeeper/handler.go`:

- Send Message — `chat.user.{account}.room.{roomID}.{siteID}.msg.send`
  (covers plain, thread, and quoted; reply on
  `chat.user.{account}.response.{requestID}`).

## Maintenance

- The doc is hand-maintained. Each PR that adds or changes a
  client-facing handler must update `docs/client-api.md` in the same PR.
  This rule will be added to `CLAUDE.md` Section 5 ("Workflow Guardrails")
  in the implementation phase.
- Subject literals in the doc are the canonical client view. The Go
  builders in `pkg/subject` remain the source of truth for generating
  them in code; the doc is verified against the builders by reading the
  builder code, not by string comparison.
- If drift becomes a recurring problem, revisit auto-generation
  (AsyncAPI or a doc-from-tags Go tool) in a follow-up.

## Testing

Documentation-only deliverable. "Testing" means a manual review pass:

- Spot-check three random examples by sending the exact JSON via
  `nats req` (or the `tools/nats-debug` helper) against a local stack
  and confirming the response matches the documented schema.
- Confirm every subject in the doc is producible by a `pkg/subject`
  builder (no hand-typed subjects that drift from code).
- Confirm error envelope examples match what `natsutil.ReplyError`
  actually produces.
