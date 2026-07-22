# Admin login via admin-service + bot-login feature flag

**Date:** 2026-07-20
**Branch:** `claude/admin-login-bot-feature-flag-3x87ic`

## Overview

Two related changes to the auth surface:

1. **admin-frontend logs in through admin-service directly.** Today the admin console POSTs credentials to `portal-service /api/v1/login`, which forwards to `botplatform-service`. Portal-service is out of the admin-frontend path after this change; admin-service owns admin login end-to-end and remains runtime-independent from botplatform-service (no HTTP hop between them).
2. **A feature flag on portal-service disables bot-account logins.** Bots log into `chat-frontend`'s `/dev-login` today via `portal → botplatform`. A future dedicated "bot-devs" client will replace that surface. A single `BOT_LOGIN_ENABLED` env var on portal-service lets us cut bots off from the chat clients when that new client ships, without touching botplatform (so the bot-devs client, which will call botplatform directly, keeps working).

## Goals

- admin-frontend talks to admin-service only for authentication — no portal-service, no botplatform HTTP hop from admin-service.
- Admin sessions share the same Mongo `sessions` collection as botplatform-issued sessions, so `auth-service`'s existing session-validate path works unchanged and password-change revocations kick both surfaces.
- One env var flips bot-accounts' ability to log into chat-frontend without redeploying any other service. UI hides the surface when the flag is off; backend enforces regardless.
- No new dependency on botplatform-service from admin-service.

## Non-goals

- Not touching the SSO path (chat-frontend humans).
- Not removing admin-role logins from portal-service — admins who happen to use chat-frontend continue to log in via portal → botplatform.
- Not building the future bot-devs client. Only ensuring its login path (direct to botplatform) will not be gated by the flag added here.
- Not distinguishing session origin (admin-service-issued vs botplatform-issued). A valid session with the `admin` role is admin-caliber regardless of who minted it; the role gate is the security boundary. Adding a `surface` field on session docs is out of scope.
- Not changing chat-frontend's SSO login page.

## Design

### 1. Admin-frontend logs in through admin-service

#### HTTP surface on admin-service (new)

| Method | Path | Middleware | Purpose |
|---|---|---|---|
| POST | `/v1/login` | none | Admin-only password login |
| POST | `/v1/password/change` | `requireAdmin` | Rotate password; revoke sibling sessions |

Existing `/v1/admin/*` routes and `/healthz`/`/readyz` unchanged.

#### `POST /v1/login {username, password}` sequence

1. `store.GetUserByAccount(ctx, cfg.SiteID, username)` — one Mongo read of `users`.
2. Not-found → `errcode.Unauthenticated("invalid credentials", reason=invalid_credentials)`.
3. Role gate: `!model.IsPlatformAdmin(u)` → **same** generic `invalid_credentials` (timing-safe — bots must not be able to distinguish "wrong password" from "not admin" via response shape or reason).
4. `pwhash.Verify(u.Services.Password.Bcrypt, password)` — false → same generic error.
5. Deactivated check *after* password verify (mirrors `botplatform-service/handler.go` timing pattern).
6. `raw := sessiontoken.New()`; insert `session.Session{ID: sessiontoken.Hash(raw), UserID, Account, SiteID, Roles, IssuedAt}` into the shared `sessions` collection via `pkg/session.Insert`.
7. FIFO evict: `pkg/session.DeleteBeyondCap(userID, cfg.SessionsMaxPerAccount)` — best-effort, log on failure but don't fail the login.
8. Set gin context tag `login_outcome = "ok"`, log `login ok`, return:

```json
{
  "authToken": "<raw>",
  "account": "<canonical account>",
  "siteId": "<site>",
  "requirePasswordChange": <bool>
}
```

No `userId`, `baseUrl`, or `natsUrl` — admin-frontend never opens NATS and never reads `userId`.

Outcome tags for observability: `ok`, `bad_request`, `invalid_credentials`. All login denials — unknown account, not-admin, wrong password, deactivated — collapse to `invalid_credentials` for timing safety; there is no separate `deactivated` tag, since exposing one would leak account state through the outcome label even if the response body stays uniform. Denied paths call a shared helper (mirrors botplatform's `handler.denied`) so wording and timing stay uniform.

#### `POST /v1/password/change {oldPassword, newPassword}` sequence

Middleware has already validated the Bearer token and stashed the session-id-hash and account in gin context.

1. `store.GetUserForAuth(ctx, siteID, account)` — dedicated auth-path read that includes `services.password.bcrypt` (the standard `GetUserByAccount` projection scrubs it).
2. `pwhash.Verify(u.Services.Password.Bcrypt, oldPassword)` — false → `errcode.Unauthenticated("old password does not match", reason=old_password_mismatch)`.
3. `newHash := pwhash.Hash(newPassword)`.
4. `store.UpdateUserPasswordAndRevoke(ctx, siteID, account, newHash, requireChange=false, exceptSessionID=callerSessionID)` — one Mongo transaction: sets the new hash, clears `requirePasswordChange`, and deletes every session for this account **except** the caller's (siteId-scoped filter). Kills sibling sessions on both admin-frontend and chat-frontend surfaces (they share the collection).
5. `store.AppendAudit(ctx, ...)` with `Action = "password_change"`, `TargetAccount = account`. Details map excludes any credential material.

Response: `204 No Content`.

#### `requireAdmin` middleware rewrite

Currently the middleware HTTP-calls `botplatform-service /api/v1/auth/validate` to check the session token. Rewrite to read the shared `sessions` collection directly:

1. Extract Bearer.
2. `s, err := pkg/session.FindByHash(ctx, sessiontoken.Hash(raw))`.
3. Not-found → `errcode.Unauthenticated(reason=invalid_token)`.
4. `!containsAdminRole(s.Roles)` → `errcode.Forbidden(reason=not_admin)`.
5. Stash `session_id`, `account`, `user_id`, `site_id` into gin context for handlers.

Removes an inter-service HTTP hop from every admin API call.

#### admin-frontend changes

- `src/api/auth/botAuth.ts`:
  - `botLogin` swaps `${PORTAL_URL}/api/v1/login` → `${ADMIN_SERVICE_URL}/v1/login`.
  - Response type trims to `{authToken, account, siteId, requirePasswordChange}`.
  - `changePassword` unchanged (already targets admin-service).
- `src/lib/runtimeConfig`: verify nothing else in admin-frontend reads `PORTAL_URL`; if nothing does, drop the field from the runtime config schema.
- `AuthContext.jsx`: `toExposedSession` already keeps `{authToken, account, siteId}` — no change.
- Tests: update `botAuth.test.ts` fixtures to the new URL and trimmed response shape; update `AdminLoginPage.test.jsx` if it asserts on the old bundle.

### 2. Bot-login feature flag on portal-service

#### Backend enforcement (portal-service)

New config field via `caarlos0/env`:

```go
type Config struct {
  // …existing fields
  BotLoginEnabled bool `env:"BOT_LOGIN_ENABLED" envDefault:"true"`
}
```

`portal-service/handler.go` `HandleLogin`, immediately after the existing role gate (`!model.HasLoginRole(e.Roles)`) and before the upstream forward to botplatform:

```go
if !h.cfg.BotLoginEnabled && model.ContainsBotRole(e.Roles) {
    errhttp.Write(ctx, c, errcode.Forbidden(
        "bot accounts cannot log in through this client",
        errcode.WithReason(errcode.PortalBotLoginDisabled)))
    return
}
```

`ContainsBotRole` lives in `pkg/model/user.go` alongside `HasLoginRole` and `IsPlatformAdmin` — one-line helper, one source of truth for the "is this account a bot" check.

Admin accounts continue to log in through portal → botplatform for chat-frontend use, unaffected by the flag.

#### UI surfacing (portal `/api/settings` + chat-frontend)

Extend `portal-service` `settingsResponse`:

```go
type settingsResponse struct {
  APIVersion      string `json:"apiVersion"`
  OTELBaseURL     string `json:"otelBaseUrl"`
  BotLoginEnabled bool   `json:"botLoginEnabled"`
}
```

`HandleSettings` already serves the response with `Cache-Control: no-cache`, so a flag flip is visible on the next chat-frontend load without a redeploy.

chat-frontend:

- `src/lib/runtimeConfig` exposes `BOT_LOGIN_ENABLED` (reads `botLoginEnabled` from `/api/settings`).
- `src/App.jsx` — the `/dev-login` branch (line 38 today):

```jsx
if (!connected && pathname === '/dev-login') {
  if (!BOT_LOGIN_ENABLED) {
    window.history.replaceState({}, '', '/')
    return <LoginPage />
  }
  return <BotLoginPage />
}
```

- No UI link to `/dev-login` exists today — it's an unlinked URL — so no button-hiding is required.
- When the flag flips off: direct nav to `/dev-login` falls through to the SSO login page; a determined POST to portal is server-side-rejected with `reason=bot_login_disabled`.

#### Future bot-devs client

Out of scope to build, in scope to ensure it is unaffected. The bot-devs client will hit `botplatform-service /api/v1/login` directly (its own `baseUrl` in runtime config, no portal involvement). Since portal is the flag site, that client is inherently unaffected by `BOT_LOGIN_ENABLED`.

## Shared package: `pkg/session`

Both `admin-service` and `botplatform-service` currently define their own `Session` struct in `main`, both serialize to the same Mongo collection, both duplicate `FindSessionByHash`-shape helpers. This design adds `Insert` and `DeleteForAccountExcept` on the admin-service side; that's the moment to consolidate.

New package `pkg/session`:

```go
package session

const Collection = "sessions"

type Session struct {
  ID       string   `bson:"_id"`        // sessiontoken.Hash(raw)
  UserID   string   `bson:"userId"`
  Account  string   `bson:"account"`
  SiteID   string   `bson:"siteId"`
  Roles    []string `bson:"roles"`
  IssuedAt int64    `bson:"issuedAt"`
}

type Store interface {
  Insert(ctx context.Context, s *Session) error
  FindByHash(ctx context.Context, hash string) (*Session, error)
  DeleteBeyondCap(ctx context.Context, account string, max int) (int64, error)
  DeleteForAccountExcept(ctx context.Context, siteID, account, exceptID string) (int64, error)
  DeleteForAccount(ctx context.Context, siteID, account string) (int64, error)
  ListForAccount(ctx context.Context, siteID, account string) ([]Session, error)
  DeleteByID(ctx context.Context, siteID, account, id string) (int64, error)
  EnsureIndexes(ctx context.Context) error
}

func NewMongoStore(db *mongo.Database) Store { ... }
```

- Both services import `pkg/session` and drop their local `Session`/`session` struct + method set. `admin-service`'s existing `AdminStore` interface loses its session methods; the login handler and admin session-management handlers use a `session.Store` field alongside `AdminStore`.
- `EnsureIndexes` centralizes the `_id` (auto) + `account` + `userId` + `issuedAt` index setup; both services call it at startup. Idempotent — safe under repeated calls.
- Preserves per-service ownership of *when* to call each helper (login inserts, FIFO cap, password-change revocation) without duplicating the *how*.

## New error reasons

`pkg/errcode/codes_admin.go` (new):

- `AdminInvalidCredentials = "invalid_credentials"` — reused constant name, admin-service scope.
- `AdminOldPasswordMismatch = "old_password_mismatch"` — password-change endpoint only (caller already authenticated, so this is safe to distinguish from invalid_credentials).
- `AdminNotAuthorized = "not_admin"` — `requireAdmin` middleware when a valid session lacks the admin role.

`pkg/errcode/codes_portal.go`:

- `PortalBotLoginDisabled = "bot_login_disabled"` — portal login gate.

`chat-frontend`'s `formatAsyncJobError` REASON_COPY map gains a line for `bot_login_disabled` so a stray direct POST surfaces readable copy.

## Configuration

New env vars:

| Service | Var | Default | Purpose |
|---|---|---|---|
| admin-service | `SESSIONS_MAX_PER_ACCOUNT` | `100` | FIFO cap on admin sessions (matches botplatform's existing var name/default) |
| portal-service | `BOT_LOGIN_ENABLED` | `true` | When `false`, portal `/api/v1/login` rejects bot-role accounts |

admin-service `deploy/docker-compose.yml`: add `SESSIONS_MAX_PER_ACCOUNT=100` for local parity. Portal's local compose keeps the default.

## Testing

Follow TDD per project CLAUDE.md — write tests first, red-green-refactor.

### admin-service unit tests

- `TestHandler_HandleLogin` table cases: `{ok}`, `{user_not_found → invalid_credentials}`, `{not_admin → invalid_credentials}` (assert same reason and same response shape as wrong-password — proves no info leak), `{wrong_password → invalid_credentials}`, `{deactivated → invalid_credentials}`, `{session_insert_error → 500}`.
- `TestHandler_HandleChangePassword` table cases: `{ok, requirePasswordChange cleared, sibling sessions revoked, caller session preserved}`, `{wrong_old_password → old_password_mismatch}`, `{user_not_found → 500}` (should never happen mid-session but fail loud), `{bcrypt_hash_error → 500}`.
- `TestRequireAdmin` table cases: `{valid_admin_session → passes with context tags set}`, `{missing_bearer → 401}`, `{invalid_token → 401 invalid_token}`, `{valid_session_no_admin_role → 403 not_admin}`.

### admin-service integration test

- `TestAdminLoginAndChangePasswordEndToEnd` in `integration_test.go` (build tag `integration`): real Mongo via `testutil.MongoDB`, seed an admin user with a bcrypt hash, POST `/v1/login`, assert bundle shape and that the session row exists in `sessions`, POST `/v1/password/change` with a second seeded session for the same account, assert the second session is gone and the caller's isn't. Uses `pkg/session.NewMongoStore` end-to-end.

### portal-service tests

- `TestPortalHandler_HandleLogin_BotLoginFlag` table: `{bot + flag_on → ok}`, `{bot + flag_off → 403 bot_login_disabled}`, `{admin + flag_off → ok}` (proves the flag doesn't over-block).
- `TestSettingsResponse_ExposesBotLoginFlag`.

### pkg/session tests

- Unit tests on a mock; integration tests on a real Mongo via `testutil.MongoDB`. Cover Insert on an existing session hash returns a Mongo duplicate-key error (not idempotent — callers see a distinct error), FindByHash (found / not-found), DeleteBeyondCap (evicts oldest N first — sort-by-issuedAt, `_id` as a deterministic tie-breaker), DeleteForAccountExcept (leaves the exception, kills the rest), EnsureIndexes (idempotent under repeated calls).

### botplatform-service tests

- No behavior change. Existing tests must still pass after the internal swap to `pkg/session`. If any test constructs the local `session` struct, migrate to `pkg/session.Session`.

### chat-frontend tests

- `App.test.jsx`: `{BOT_LOGIN_ENABLED=false + pathname=/dev-login} → renders LoginPage` (not BotLoginPage); `{BOT_LOGIN_ENABLED=true + pathname=/dev-login} → renders BotLoginPage` (regression guard).
- `runtimeConfig.test.js`: `botLoginEnabled` from `/api/settings` maps to `BOT_LOGIN_ENABLED`; default `true` when absent.

### admin-frontend tests

- `botAuth.test.ts`: `botLogin` POSTs to `${ADMIN_SERVICE_URL}/v1/login` and returns the trimmed bundle. Existing `changePassword` test unchanged.

### Coverage floor

Project floor is 80% (CLAUDE.md §4). New handler methods and `pkg/session` should hit 90%+.

## Rollout notes

Order matters, but the blast radius is small — each step is independently deployable.

1. **`pkg/session` extraction.** No behavior change. botplatform-service uses `pkg/session` in place of its local session type; existing tests must pass. Ship this first so admin-service can build against it.
2. **admin-service — add `POST /v1/login` and `POST /v1/password/change`; rewrite `requireAdmin`.** Existing admin-frontend still POSTs to portal, so this is a no-op from the frontend's perspective; the new routes just exist and are tested.
3. **admin-frontend — swap `botLogin` URL to admin-service, drop `PORTAL_URL`.** Deploy after step 2 is in production. From this point admin-frontend never hits portal.
4. **portal-service — add `BOT_LOGIN_ENABLED` (default `true`), expose `botLoginEnabled` in `/api/settings`.** Ship with default `true` — no behavior change yet. chat-frontend consumes `botLoginEnabled` in the same or a follow-up deploy.
5. **Flip `BOT_LOGIN_ENABLED=false` in portal production env when the bot-devs client ships.** No code change, no other service redeployed.

Rollback: `BOT_LOGIN_ENABLED=true` restores bot chat-frontend login. Admin-frontend routing is one-way — if admin-service `/v1/login` breaks in production, the working escape is to redeploy admin-frontend with the old portal URL, not to leave the new one broken.

## Documentation

- `docs/client-api.md` — CLAUDE.md §5 requires client-facing handler changes to update this doc. admin-service is not strictly listed there today, but the new `/v1/login` and `/v1/password/change` are client-facing HTTP surfaces used by admin-frontend; add them under a new admin-service section. The bot-login flag reason (`bot_login_disabled`) goes into §6 error reasons.
- No changes to `docs/client-api/request-reply.md` or `docs/client-api/events.md` (no NATS request/reply or event shape changes).
- Delete `docs/reviews/*` before opening the PR (CLAUDE.md §5).
