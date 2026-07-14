# admin-service + admin-portal — design

**Date:** 2026-06-30
**Branch:** `claude/admin-service-portal-m99rqg` (stacked on `claude/chat-frontend-bot-login`, which is itself stacked on PR #428's `botplatform-login-endpoint`)
**Status:** implemented (backend). **Descoped 2026-07-03:** the §3 room admin-override
is **not shipped** — admins get the same NATS scope as a normal user
(`chat.user.{account}.>`, no `chat.>` god-mode) and no handler-level owner-check bypass;
"no extra permissions for admin now" (the `chat-frontend-bot-login` base already carries
its own platform-admin room logic). Admin power in v1 is the management console only.

## Context

Bot/admin **password login is already built** on this branch's base
(`claude/chat-frontend-bot-login`):

- `botplatform-service` — per-site password **login + validate + change-password**,
  serving **both `bot` and `admin`** roles.
- `portal-service` — `/v1/login` forwarder (username → home-site routing) + role-aware
  `/api/userInfo`.
- `auth-service` — `/auth` mints a NATS JWT from a session `authToken`, dispatching on
  which token field is present (no `kind`); **role-derived scope** comes from
  `pkg/principal.NATSSubjectScope` (admin → `chat.>` + `_INBOX.>` god-mode).
- `chat-frontend` — `/dev-login` → `BotLoginPage`, session connect + `sessionStorage`
  resume, session-token re-mint branch in `useJwtRefresh`, `ChangePasswordForm`.

The chat-frontend bot-login design
(`docs/superpowers/specs/2026-06-30-chat-frontend-bot-login-design.md`) explicitly
deferred two pieces:

> **Out of scope:** Admin-service / admin frontend (own design + PR per #428).

**This spec covers exactly those two pieces.** Admins do **not** get a new login flow —
they authenticate through the *existing* `/v1/login` → `/auth` path (an admin is just an
account with `roles:["admin"]`). What is new is (1) a backend for **admin account
management**, (2) **admin-override** in the room services so an admin can manage any
room, and (3) a **separate internal admin-frontend** app that hosts the management
console and a chat client.

## Goal

Give site operators an internal web console to:

- **Manage accounts** (the new `admin-service`): search users; create accounts with any
  role (`user`/`bot`/`admin`); set / force-reset passwords for any account (bot, admin,
  or manually-created user); change roles; activate / deactivate accounts; list and
  revoke a user's sessions.
- **Chat** as themselves over NATS (search users/bots, DM, post) — works today from the
  minted admin JWT, no backend change.
- **Administer any room** (add / remove members, change member roles) regardless of
  ownership — requires admin-awareness in `room-service` (+ `room-worker`).

All site-scoped: each admin-frontend + admin-service deployment serves exactly one site;
an admin of site A can only ever act on site A (enforced by `SITE_ID` + the existing
`wrong_site` login gate).

## Non-goals (v1)

- Any new **login / validate / mint** endpoint — reuse the existing bot/admin flow.
- Multi-site admin console / portal discovery — admin login is site-direct via its one
  portal; no `/api/userInfo` hop.
- Voluntary in-app password change for the admin's *own* account (the first-login
  `ChangePasswordForm` already exists and is reused).
- Immediate revocation of an already-minted NATS JWT beyond session-revoke (bounded by
  one JWT lifetime via the refresh gate, same property as bots).

---

## Component overview

```
┌──────────────────┐    REST (Bearer authToken)     ┌───────────────┐
│  admin-frontend  │ ─────────────────────────────▶ │ admin-service │ ──▶ MongoDB
│  (separate app,  │      account management         │  (REST only)  │   (users, sessions)
│   internal-only) │                                 └───────────────┘
│                  │
│  /admin-login    │ ── POST ${PORTAL_URL}/v1/login ─▶ portal → botplatform   (existing)
│                  │ ── POST ${authServiceUrl}/auth ─▶ auth-service           (existing)
│                  │ ── NATS WS (admin chat.> JWT) ──▶ room/user/msg services (existing
│                  │                                    + room admin-override) (new gate)
└──────────────────┘
```

Boundary principle (unchanged from earlier reasoning):
**account administration → admin-service REST → Mongo**;
**chat & room operations → NATS** (from the frontend, using the admin's JWT).
admin-service makes **no NATS calls**.

---

## 1. admin-service (new Go service)

Flat service directory at repo root, `package main`, mirroring the auth-domain siblings
(`auth-service`, `portal-service`, `botplatform-service`) — Gin HTTP, no JetStream, no
OTEL. Files per the standard layout: `main.go`, `config.go`, `handler.go`, `routes.go`,
`middleware.go`, `store.go`, `store_mongo.go`, `handler_test.go`, `integration_test.go`,
`mock_store_test.go` (generated), `deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}`.

### 1.1 Authentication & authorization

admin-service does **not** mint or issue tokens. Every management request carries the
caller's existing session `authToken` (obtained from `/v1/login` at admin-frontend login)
as `Authorization: Bearer <authToken>`.

A `requireAdmin` middleware:

1. Extracts the bearer token; absent or unknown → `401 invalid_token`.
2. Hashes it via `sessiontoken.Hash` and looks it up in the `sessions` collection
   (`store.FindSessionByHash`). Not found → `401 invalid_token`.
3. Requires the session's denormalized `roles` to contain `admin` → else `403 not_admin`.
4. Stamps the resolved principal (`userId`, `account`, `siteId`, `roles`) into the request
   context for handlers and access logging.

This validates **locally against the shared `sessions` collection** rather than calling
botplatform's `/v1/auth/validate` over HTTP — admin-service already needs `sessions`
access for the session-management endpoints (§1.3), so a local lookup avoids an HTTP hop
and a hard runtime dependency on botplatform. To guarantee the hash scheme can never
drift from botplatform's, the token primitives are promoted to a shared package (§4).

> Note (accepted v1 limitation): `session.roles` is a snapshot taken at login. If an
> admin is demoted after logging in, their existing session still authorizes admin calls
> until it expires/is revoked or they re-login. Acceptable for v1; the deactivate/revoke
> flow (§1.3) is the immediate lever.

### 1.2 User management endpoints

All under `requireAdmin`. All operations are **scoped to `SITE_ID`** — the service only
sees/manages users whose `siteId == SITE_ID` (the per-site `users` collection also
contains other sites' rows for cross-site rooms, so every query carries the `siteId`
predicate).

| Method | Path | Body / Query | Result |
|---|---|---|---|
| `GET` | `/v1/admin/users` | `?q=&page=&limit=` | paged list, projected (see below) |
| `POST` | `/v1/admin/users` | `{account, engName, chineseName?, roles[], password, requirePasswordChange?}` | `201` created user (projected) |
| `GET` | `/v1/admin/users/:id` | — | one user (projected) |
| `PATCH` | `/v1/admin/users/:id` | `{engName?, chineseName?, roles?, deactivated?}` | `200` updated user |
| `POST` | `/v1/admin/users/:id/password` | `{newPassword, requirePasswordChange?}` | `200 {status:"success"}` |

**Projection** (never includes the bcrypt hash — `Services` is `json:"-"`):
`id, account, siteId, engName, chineseName, roles, deactivated, requirePasswordChange`.

Behaviors:

- **Search** (`GET /v1/admin/users`): `q` matches `account`/`engName`/`chineseName`
  (case-insensitive prefix/contains), `siteId == SITE_ID`, paginated via
  `pkg/mongoutil/pagination`. Explicit projection (per CLAUDE.md "always project
  precisely"). Returns inactive and all-role users (admin needs to see everyone).
- **Create** (`POST /v1/admin/users`): `account` must be unique within the site →
  `409 account_exists` (relies on the unique `users(account)` index). `siteId` is forced
  to `SITE_ID` (admins cannot create cross-site users). Password is hashed via the shared
  recipe `bcrypt(sha256_hex(plaintext))` at `BCRYPT_COST`. `requirePasswordChange`
  defaults **`true`** (admin-created accounts must rotate on first login). `_id` generated
  via `idgen` per the existing user-id convention.
- **Update** (`PATCH`): change display names, **roles** (promote/demote user/bot/admin),
  and **deactivated** flag. Deactivating (`deactivated:true`) also **revokes all of that
  user's sessions** (§1.3) so the lockout is immediate (next NATS re-mint fails). Role/active
  changes do not touch the password.
- **Set password** (`POST …/password`): set/reset the password for *any* account
  (bot/admin/user). Hashes + stores at `services.password.bcrypt`, sets
  `requirePasswordChange` per the body (default `true`), and **revokes all of that user's
  sessions** (force re-login with the new credential). This is the admin's "reset/force
  change password" capability.

### 1.3 Session management endpoints

| Method | Path | Result |
|---|---|---|
| `GET` | `/v1/admin/users/:id/sessions` | list active sessions (`id`, `userId`, `siteId`, `issuedAt`) |
| `DELETE` | `/v1/admin/users/:id/sessions` | revoke **all** sessions for the user |
| `DELETE` | `/v1/admin/users/:id/sessions/:sessionId` | revoke one session (`sessionId` = the listed `id`) |

The returned `id` is the session `_id` = `base64(sha256(rawToken))` — a one-way hash, safe
to expose for targeting a revoke; the usable raw token is never stored or returned.

Revocation = deleting session rows; the next NATS-JWT re-mint for that user then fails and
the user is evicted within one JWT lifetime (same property bots have).

### 1.3a Audit log (`admin_audit`)

Every **mutating** admin action is recorded in an `admin_audit` collection owned by
admin-service, so privileged changes are accountable.

- **Recorded on:** `createUser`, `updateUser` (roles/names/deactivate), `setPassword`,
  `revokeAllSessions`, `revokeSession`. (Read-only `GET`s are not logged.)
- **Entry fields:** `id` (`idgen`), `actorUserId` + `actorAccount` (from the request
  principal), `action` (e.g. `user.create`, `user.update`, `user.password.set`,
  `user.deactivate`, `session.revoke_all`, `session.revoke`), `targetUserId` /
  `targetAccount`, `details map[string]string` (non-secret context only — e.g. changed
  role set; **never** passwords, hashes, or tokens), `siteId`, `timestamp` (unix ms).
- **Write semantics:** the audit entry is appended **after** the mutation succeeds. An
  audit-write failure is logged at `slog.Error` but does **not** roll back or fail the
  request (no multi-doc transaction in v1; the alternative — failing a completed mutation —
  is worse). This is an accepted v1 limitation.
- **Read endpoint** (under `requireAdmin`, site-scoped, newest-first, paginated):
  `GET /v1/admin/audit?targetUserId=&actor=&action=&page=&limit=`.
- **Indexes:** `admin_audit(siteId, timestamp)` and `admin_audit(targetUserId, timestamp)`.

### 1.4 Health

- `GET /healthz` — liveness only (`{"status":"ok"}`).
- `GET /readyz` — `503` until a Mongo `Ping` succeeds.

### 1.5 Store interface (`store.go`, consumer-defined, mockgen-generated mock)

```go
type AdminStore interface {
    SearchUsers(ctx context.Context, siteID, q string, page, limit int) ([]model.User, int64, error)
    GetUserByID(ctx context.Context, id string) (*model.User, error)          // ErrUserNotFound
    GetUserByAccount(ctx context.Context, siteID, account string) (*model.User, error)
    CreateUser(ctx context.Context, u *model.User) error                      // ErrAccountExists on dup
    UpdateUser(ctx context.Context, id string, fields UserUpdate) error
    UpdateUserPassword(ctx context.Context, id, bcryptHash string, requireChange bool) error

    FindSessionByHash(ctx context.Context, hash string) (*Session, error)     // for requireAdmin
    ListSessionsByUser(ctx context.Context, userID string) ([]Session, error)
    DeleteSessionsByUser(ctx context.Context, userID string) (int64, error)
    DeleteSession(ctx context.Context, sessionID string) (int64, error)

    AppendAudit(ctx context.Context, e *AuditEntry) error
    ListAudit(ctx context.Context, siteID string, f AuditFilter, page, limit int) ([]AuditEntry, int64, error)

    EnsureIndexes(ctx context.Context) error                                  // unique users(account); audit indexes
    Ping(ctx context.Context) error
}
```

`AuditEntry` (fields per §1.3a) and `AuditFilter` (`TargetUserID`, `Actor`, `Action` —
all optional) are defined locally in admin-service.

`Session` mirrors botplatform's session row shape (`_id`, `userId`, `account`, `siteId`,
`roles`, `issuedAt`) — defined locally in admin-service (a small read/write model), since
the type is not exported from botplatform.

### 1.6 Config (`caarlos0/env`)

| Env | Default | Notes |
|---|---|---|
| `PORT` | `8082` | 8080 auth/botplatform, 8081 portal |
| `SITE_ID` | — | **required**; scopes all user queries + the admin's site |
| `MONGO_URI` | — | **required** |
| `MONGO_DB` | `chat` | shared chat DB (same as botplatform) |
| `MONGO_USERNAME` / `MONGO_PASSWORD` | `""` | optional |
| `BCRYPT_COST` | `10` | must match the legacy/botplatform cost |
| `DEV_MODE` | `false` | dev affordances only (see §6) |

### 1.7 Errors (`pkg/errcode/codes_admin.go`, mirroring `codes_botplatform.go`)

```go
const (
    AdminNotAuthorized  Reason = "not_admin"        // 403 valid session, role != admin
    AdminInvalidToken   Reason = "invalid_token"    // 401 missing/unknown session token
    AdminUserNotFound   Reason = "user_not_found"   // 404
    AdminAccountExists  Reason = "account_exists"   // 409 duplicate account on create
)
```

Reuse existing shared reasons for form validation (`errcode.AuthMissingFields` →
`missing_fields`). Infra failures return raw wrapped errors → collapse to `internal` at
the boundary via `errhttp.Write`.

---

## 2. User model additions (`pkg/model/user.go`)

The current `User` struct has no active/inactive field, but botplatform's login response
emits `me.active`. To make **activate/deactivate** real (and to let login reject disabled
accounts):

- Add `Deactivated bool \`json:"deactivated,omitempty" bson:"deactivated,omitempty"\``.
  Absent/`false` = active (safe for existing docs, no migration needed).
- Audit botplatform's `me.active`: derive it as `!user.Deactivated` rather than a
  hardcoded `true`, and have botplatform `/v1/login` **reject a deactivated account**
  (uniform `401 invalid_credentials`, so deactivation locks login as well as the existing
  sessions). This keeps "deactivate a bot/admin" meaningful end-to-end.

(`HasLoginRole`, `IsPlatformAdmin`, role constants already exist and are reused.)

---

## 3. Room admin-override (`room-service`, `room-worker`)

Today member ops gate on room ownership in `room-service/handler.go`:

- `addMembers` (`errNotRoomMember`, and `errOnlyOwnersCanAddToRes` for restricted rooms)
- `removeMember` (`errOnlyOwnersCanRemove`)
- `updateRole` (`errOnlyOwners`)

Each handler already knows the actor (`c.Param("account")`) and already has a
`store.GetUser` method. Add a single helper and short-circuit the ownership checks when
the actor is a platform admin:

```go
// isPlatformAdmin reports whether the acting account holds the platform admin role.
func (h *Handler) isPlatformAdmin(ctx context.Context, account string) (bool, error) {
    u, err := h.store.GetUser(ctx, account)
    if err != nil { return false, err }
    return model.IsPlatformAdmin(u), nil
}
```

In each gate: `if !isOwner && !isAdmin { return errOnlyOwners… }`. The admin still must
target an existing room/member (the not-found paths are unchanged); admin only bypasses
the **ownership/membership** requirement. The actor account is trustworthy here because a
non-admin's NATS JWT cannot publish outside `chat.user.{ownAccount}.>`, while the admin's
`chat.>` lets them act under their own account namespace; the handler derives the actor
from the subject and confirms the admin role from Mongo (not from the JWT), so the gate
cannot be spoofed by a non-admin.

**room-worker:** audit the async member-mutation path (the `ROOMS` consumer that #358
optimized) for any duplicate ownership re-check; if present, apply the same admin bypass
so the sync RPC and the async apply agree. If room-worker performs no independent
permission check (room-service is the sole gate), no change is needed — confirm during
implementation and record the finding in the PR.

> `GetUser` per membership op is one extra point read on an already-DB-bound path; admin
> traffic is low-volume, so no caching is introduced.

---

## 4. Shared session-token package (`pkg/sessiontoken`)

botplatform currently generates and hashes session tokens with a local helper
(`raw = base64url(32 random bytes)`, `id = base64(sha256(raw))`). admin-service must hash
inbound tokens with the **identical** scheme to validate callers (§1.1). To prevent drift,
extract the primitives into a tiny shared package:

```go
package sessiontoken
func New() (raw string, err error)   // 32 random bytes → base64.RawURLEncoding (43 chars)
func Hash(raw string) string         // base64.StdEncoding(sha256(raw)) (44 chars)
```

Refactor botplatform to consume it (behavior-identical; covered by its existing tests).
admin-service uses `Hash` in `requireAdmin`. This is the only change to botplatform in
this PR beyond §2's `me.active` derivation.

---

## 5. admin-frontend (new internal app)

A **separate Vite/React app** at repo root (`admin-frontend/`), internal-only, never
shipped in the mobile/desktop product. It reuses chat-frontend's proven patterns and
copies the slim bot-login pieces it needs (it does not import across app boundaries).

### 5.1 Login surface

- Route **`/admin-login`** (the only unauthenticated screen; root `/` redirects to it
  when not connected). Deliberately **not** named `/dev-login` — that is a chat-frontend
  artifact meaning "bot password page"; this is a dedicated admin product.
- The page calls the **existing** backend contract — `POST ${PORTAL_URL}/v1/login`
  `{username, password}` → bundle `{userId, authToken, account, siteId, authServiceUrl,
  baseUrl, natsUrl, requirePasswordChange}` — reusing the `botAuth` logic
  (`botLogin`/`changePassword`) and the `ChangePasswordForm` first-login gate ported from
  chat-frontend.
- On success → connect to NATS in **session mode** (`/auth` with `{authToken,
  natsPublicKey}` → admin `chat.>` JWT → NATS WS), reusing the session-connect +
  `useJwtRefresh` session branch + `sessionStorage` resume already designed for bots.
- The login `bundle.authToken` is the bearer for all admin-service REST calls.

### 5.2 App shell (post-login)

Two areas behind admin login:

1. **Chat** — reuse chat-frontend's chat surface and `api/` NATS operations (search
   users/bots, open/DM rooms, post, manage room membership). Works against the admin's
   `chat.>` JWT; member add/remove now succeed on any room thanks to §3.
2. **Settings → Users** — the management console. A small REST client
   (`api/admin/`) hitting `${ADMIN_SERVICE_URL}/v1/admin/*` with
   `Authorization: Bearer ${authToken}`: a searchable user table; a create-user form
   (account, names, role multiselect, initial password, force-change toggle); per-user
   actions (edit roles, activate/deactivate, set/reset password, view & revoke sessions).
   Errors render via the existing `formatAsyncJobError` / errcode-envelope pattern.

### 5.3 Config (`runtimeConfig`)

| Var | Notes |
|---|---|
| `VITE_PORTAL_URL` | this site's portal (drives `/v1/login`) |
| `VITE_ADMIN_SERVICE_URL` | this site's admin-service (drives `/v1/admin/*`) |

`authServiceUrl` / `natsUrl` come **from the login bundle** (not static) — same as the
bot path. One portal + one admin-service per admin deployment (site-tied).

### 5.4 Deploy

Own `deploy/` (Dockerfile multi-stage Node build → static serve; docker-compose for local
dev joining `docker-local`; azure-pipelines). Internal-network/locked-down deployment is
an ops concern, noted but not configured here.

---

## 6. Local dev

- admin-service joins `docker-local` with `MONGO_DB=chat`, `SITE_ID=site-local`,
  `BCRYPT_COST=10`. In `DEV_MODE=true` it may relax to ease local use, **but never bypasses
  the admin-role check** — the management surface is always admin-gated. (Local admin
  accounts come from the existing seed; an admin can be promoted via a seeded `roles`
  entry.)
- admin-frontend dev: `VITE_PORTAL_URL=http://localhost:8081`,
  `VITE_ADMIN_SERVICE_URL=http://localhost:8082`; log in with a seeded admin account's
  username/password via `/admin-login`.

---

## 7. Documentation (`docs/client-api.md`)

admin-service's endpoints are **HTTP on a new service**, not `chat.user.*` NATS handlers,
but they are client-facing for the admin app. Add a new subsection documenting
`/v1/admin/users*` and `/v1/admin/users/:id/sessions*` (request/response field tables,
JSON examples, error tables) in the current doc style, plus the new `not_admin` /
`account_exists` reasons in §6's reason catalog. The room admin-override changes the
*behavior* of existing `addMembers`/`removeMember`/`updateRole` RPCs (admins bypass
owner checks) — note this in their existing entries. No change to the login/`/auth`
sections (owned by the bot-login PRs).

---

## 8. Testing (TDD, ≥80% coverage)

- **admin-service `handler_test.go`** — table-driven, mocked `AdminStore` + a fake
  session in the store for `requireAdmin`: each endpoint happy path; `missing_fields`;
  `not_admin` (valid session, non-admin role); `invalid_token` (absent/unknown);
  `account_exists` on duplicate create; `user_not_found`; deactivate/password-reset revoke
  sessions; site-scoping (a user with a different `siteId` is invisible / not found);
  password never serialized.
- **admin-service `integration_test.go`** — `testutil.MongoDB` + `TestMain(testutil.RunTests)`:
  search hit/miss + pagination, create + unique-index enforcement, update roles/deactivated,
  password update clears `requirePasswordChange` + revokes sessions, session list/revoke,
  audit append + list (filtered, paginated, newest-first; secrets never recorded),
  `EnsureIndexes` idempotent.
- **`pkg/sessiontoken`** — `New` length/charset, `Hash` determinism + matches botplatform's
  prior output (golden), round-trip used by both services.
- **`pkg/model`** — `Deactivated` marshal/round-trip via the existing `roundTrip` helper.
- **room-service `handler_test.go`** — admin actor bypasses owner/member gates on
  `addMembers`/`removeMember`/`updateRole`; non-admin still rejected; admin still blocked
  on genuine not-found. room-worker: add/adjust tests only if it carries a duplicate gate.
- **admin-frontend (vitest)** — `/admin-login` renders and logs in via `/v1/login`;
  `requirePasswordChange` routes to the change step; admin REST client sends the Bearer
  token and maps errcode envelopes; Users console actions call the right endpoints;
  session-mode connect + refresh reused from the bot path.

---

## 9. Rollout & follow-ups

- **Ordering:** ships after the bot-login PRs it is stacked on (#428 + chat-frontend
  bot-login) merge. Within this PR: `pkg/sessiontoken` + `pkg/model` first, then
  admin-service, then room admin-override, then admin-frontend.
- **`docs/reviews/`**: delete any session-scoped review reports before opening the PR
  (per CLAUDE.md).
- **Follow-ups (not in v1):** least-privilege admin NATS scope (today `chat.>` god-mode);
  voluntary self password change in the admin app; locking the admin-frontend behind
  network policy in deploy.

## Open question carried into the plan

- **Admin room power level** is specified here as **full override** (admin bypasses owner
  checks on any room), per the requested "admin can add/remove members from rooms." If the
  team prefers normal-user-level room power for v1, drop §3 (and its tests) — the rest of
  the design is unaffected.
