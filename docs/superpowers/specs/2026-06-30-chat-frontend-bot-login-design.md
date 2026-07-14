# chat-frontend bot/admin password login — design

**Date:** 2026-06-30
**Branch:** `claude/chat-frontend-bot-login` (stacked on PR #428's `botplatform-login-endpoint` branch)
**Status:** approved design, pre-implementation

## Context

PR #428 (`feat: bot-platform login + validate (botplatform-service) — v1`) shipped the
backend for bot/admin password login and explicitly deferred the frontend:

> chat-frontend bot login + change-password pages — frontend-only PR.

This spec covers that follow-up. It is **frontend-only** and consumes endpoints that
land with #428, so it should merge after #428.

### Backend contract consumed (from #428, `docs/client-api.md` §2.2/§2.5/§2.6b)

- `POST ${PORTAL_URL}/v1/login` — body `{username, password}` → 8-field bundle
  `{userId, authToken, account, siteId, authServiceUrl, baseUrl, natsUrl, requirePasswordChange}`.
  Errors: `400 missing_fields`, `401 invalid_credentials` (uniform), `403 wrong_site`,
  `500 site_unknown`, `503 upstream_unavailable`.
- `POST ${authServiceUrl}/auth` — body `{authToken, natsPublicKey}` → `{natsJwt, user}`.
  The session-token branch added in #428 (auto-routed by which token field is present).
- `POST ${baseUrl}/v1/password/change` — header `Authorization: Bearer <authToken>`,
  body `{oldPassword, newPassword}` → `{status:"success"}`. Errors: `400 missing_fields`,
  `401 invalid_token`, `401 invalid_credentials`, `500 internal`. The caller's session
  stays valid; all other sessions for the user are revoked server-side.

### Existing frontend auth (unchanged by this work)

- `LoginPage` at `/` branches on the `DEV_MODE` runtime flag: dev account-name form vs
  "Sign in with Keycloak" (auto-redirects to the IdP in prod).
- `OidcCallback` at `/oidc-callback`.
- `NatsContext.connectToNats({mode})` supports `dev` and `sso`. Both resolve the home
  site via `portal /api/userInfo`, mint a NATS JWT at `${authServiceUrl}/auth`, then dial
  NATS. `useJwtRefresh` owns the live credentials and a background re-mint loop (SSO
  silent-renew at ~80% of JWT life).
- The only path-based route today is `/oidc-callback`; all other routing is flag-driven.

## Decisions (locked with the user)

1. **Entry point:** a dedicated route **`/dev-login`** renders the password-login page.
   It is **independent of `DEV_MODE`** (bots never SSO) — the route check precedes the
   `DEV_MODE` branch, so it always shows the username/password form. The existing SSO
   `LoginPage` is untouched.
2. **Change-password scope:** forced first-login only (driven by `requirePasswordChange`).
   No voluntary in-app password change in this PR.
3. **Session refresh:** auto-refresh — the NATS JWT is re-minted before expiry using the
   stored `authToken` (parity with the SSO silent-renew loop). The bot/admin session
   token is permanent, so the session survives indefinitely.
4. **Reload persistence:** the login bundle (incl. `authToken`) is kept in `sessionStorage`
   so a tab reload auto-reconnects. It clears when the tab closes or on disconnect.

## Architecture

### 1. Routing (`App.jsx`)

New branch ordering:

1. `pathname === '/oidc-callback'` → `OidcCallback`
2. `pathname === '/dev-login'` *(new)* → `BotLoginPage`
3. `!connected` → `LoginPage` (existing; `DEV_MODE` decides dev-form vs Keycloak)
4. connected → `MainApp`

`DEV_MODE` only ever affects branch 3. The bot path (branch 2) never touches it.

### 2. API layer — `src/api/auth/botAuth.js`

Two thin `fetch` wrappers, following the existing envelope-error pattern (throw
`AsyncJobError` carrying `code`/`reason`/`metadata`, formatted for display via
`formatAsyncJobError`):

- `botLogin({ username, password }) → bundle` — `POST ${PORTAL_URL}/v1/login`.
- `changePassword({ baseUrl, authToken, oldPassword, newPassword }) → void` —
  `POST ${baseUrl}/v1/password/change` with `Authorization: Bearer <authToken>`.

A small local `throwHttpEnvelopeError(resp, fallback)` helper parses the errcode
envelope (mirrors the private helper in `NatsContext`; not refactored out, to keep this
PR focused).

**Assumption — change-password URL.** #428 adds no portal proxy for password change, and
the only site origin in the login bundle is `baseUrl`, so the client calls
`${baseUrl}/v1/password/change` (gateway-routed to botplatform). If the gateway exposes
botplatform at a different origin, only this one URL constant changes.

### 3. `connectToNats` — bot path reuses the user-account flow

The bot path is **not** a special-cased flow. `connectToNats` is parameterized so all
three callers share one body, differing on only two axes: whether discovery is needed, and
which token goes in the `/auth` body.

| caller | discovery | `/auth` body |
|---|---|---|
| sso | `/api/userInfo` | `{ssoToken, natsPublicKey}` |
| dev | `/api/userInfo` | `{account, natsPublicKey}` |
| session (bot) | pre-resolved `bundle` from `/v1/login` | `{authToken, natsPublicKey}` |

`connectToNats({mode, account, ssoToken, authToken, bundle})`:

- **Discovery:** when a pre-resolved `bundle` is supplied (bot), use it directly; otherwise
  call `portal /api/userInfo?account=` as today. `/v1/login` already did the
  `username → siteId` lookup, so the bot path never needs a second discovery call. (This is
  the merged-bundle path the contract is designed around — §2.5 returns the URL bundle "in
  one round trip"; `/api/userInfo` *can* serve non-dotted bot/admin accounts but would be a
  redundant call here, and it rejects dotted bot usernames.)
- **Mint:** `POST ${authServiceUrl}/auth` with the token field for this caller.
- **Shared tail (unchanged for all):** stage credentials → dial `natsUrl` → set
  `user`/`connected` → wire `closed()`.

`bundle = {account, siteId, authServiceUrl, baseUrl, natsUrl, authToken}`. The existing
`dev`/`sso` behavior is byte-for-byte unchanged; the bot path is the same code with the
discovery step short-circuited and `authToken` selected for the body.

### 4. Session persistence + auto-reconnect (`sessionStorage`)

- On a successful `session` connect, persist `bundle` under a single key
  (`chat.botSession`) in `sessionStorage`.
- On `NatsProvider` mount, if a stored session exists and we are not connected,
  auto-reconnect via `mode:'session'`. On failure, clear the key and fall through to the
  login form.
- `disconnect()` clears the key.

Security note: a session token in `sessionStorage` is exposed to same-origin script (XSS),
at parity with how `oidc-client-ts` stores SSO tokens. `sessionStorage` (not `localStorage`)
scopes it to the tab lifetime.

### 5. JWT refresh for session tokens (`useJwtRefresh.js`)

`setCredentials({jwt, seed, natsPublicKey, refreshable, mode, authToken})` — gains `mode`
(`'sso'` default | `'session'`) and `authToken`.

`refresh()` branches:

- **sso** (existing): `renewSsoToken()` → re-mint with `{ssoToken, natsPublicKey}`.
- **session** (new): skip SSO renewal; re-mint directly with `{authToken, natsPublicKey}`
  at `${getAuthUrl()}/auth`. Same 80%-of-life timer, same transient retry/backoff.

Terminal failure handling diverges by mode: SSO redirects to Keycloak re-login
(`redirectToReloginOnTokenInvalid`); **session** instead clears the stored bundle and drops
to the login form (no IdP redirect — bots have no IdP). A small injected `onSessionLost`
callback (provided by `NatsContext`) performs the clear + disconnect.

### 6. Change-password gate + components

`BotLoginPage` (`src/pages/BotLoginPage/`) owns the two-step flow with local state:

1. **Login step:** username + password → `botLogin`.
   - `requirePasswordChange === false` → `connect({mode:'session', bundle})` → `MainApp`.
   - `requirePasswordChange === true` → render the change-password step, holding `bundle`.
2. **Change-password step:** `ChangePasswordForm` (`src/pages/ChangePasswordPage/`) —
   `newPassword` + `confirmNewPassword` (+ `oldPassword`). On submit →
   `changePassword({baseUrl, authToken, ...})`; on success → `connect({mode:'session', bundle})`.
   The same `authToken` stays valid, so the user lands directly in chat — no re-login.

Client-side validation on the change step: `newPassword` non-empty, `newPassword === confirm`,
`newPassword !== oldPassword`. The backend does no strength checks; no strength policy is
imposed here beyond non-empty.

Error display reuses `formatAsyncJobError`. `invalid_credentials` shows a uniform
"invalid username or password"; `wrong_site` / `upstream_unavailable` show their messages.

## Files

**New**
- `src/api/auth/botAuth.js` + `botAuth.test.js`
- `src/pages/BotLoginPage/{BotLoginPage.jsx, index.jsx, style.css, BotLoginPage.test.jsx}`
- `src/pages/ChangePasswordPage/{ChangePasswordForm.jsx, index.jsx, style.css, ChangePasswordForm.test.jsx}`

**Modified**
- `src/App.jsx` — `/dev-login` route branch
- `src/context/NatsContext/NatsContext.jsx` — `session` mode, persistence, auto-reconnect, `onSessionLost`
- `src/context/NatsContext/useJwtRefresh.js` — `mode`/`authToken` in `setCredentials`, session refresh branch

No `docs/client-api.md` change — that doc tracks backend handlers (owned by #428); a
frontend consumer does not touch it.

## Testing (vitest + @testing-library/react, `fetch` mocked)

- `botAuth`: success bundle parse; each error status → `AsyncJobError` with right `reason`;
  change-password sends the `Authorization: Bearer` header and body.
- `BotLoginPage`: happy login → connect called with the bundle; `requirePasswordChange`
  routes to the change step; `invalid_credentials` renders the uniform error; change-step
  validation (mismatch / empty / unchanged) blocks submit; successful change → connect.
- `NatsContext`: `session` mode skips `/api/userInfo`, mints with `authToken`, persists to
  `sessionStorage`; mount auto-reconnect from a stored bundle; bad stored bundle clears +
  falls back; `disconnect()` clears storage.
- `useJwtRefresh`: session re-mint posts `{authToken, natsPublicKey}` (not SSO); terminal
  4xx in session mode calls `onSessionLost` instead of the IdP redirect.
- `App`: `/dev-login` renders `BotLoginPage` regardless of `DEV_MODE`.

## Out of scope

- Voluntary in-app password change.
- Admin-service / admin frontend (own design + PR per #428).
- Any backend change — endpoints are owned by #428.
