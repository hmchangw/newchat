# Bot-Platform Service — Login (v1) — Design

**Date:** 2026-06-29
**Status:** Approved — ready for plan
**Branch:** `claude/botplatform-login-endpoint-4dtcqa`
**Relationship:** Pares down `docs/specs/botplatform/auth.md` (full token+session+validate+cache+rate-limit spec) to a v1 slice that ships the login flow without HMAC token prefixing, Valkey cache, or login rate-limiting. Compatible with the full spec — those features can be layered on later without wire breaks.

## Problem

We are migrating bot authentication off legacy Rocket.Chat. Existing bots use `POST /api/v1/login` with a username/password body and receive `{userId, authToken, me}`. We need a nextgen Go service that owns this login flow, plus a portal entry path so web/mobile/desktop/admin clients (which do not use SSO for this case) can login with the same credentials and discover their home site's URLs in one call. The bot SDK must not need code changes during cutover; the wire token format must match legacy so the Istio VirtualService can switch origins 0→100 without per-client coordination.

## Goals

- New per-site `botplatform-service` that owns the bot login + session table; bot SDK hits it directly with the legacy wire contract.
- Portal entry path that authenticates the same credentials, forwards to the home-site botplatform, and returns site-discovery URLs to web/mobile/desktop/admin clients in one round trip.
- Auth-service grows a session-token branch (`kind=bot|admin`) that exchanges the session token for a NATS JWT with a role-derived scope.
- Validate endpoint on botplatform-service so the gateway/auth-service can verify a token without touching Mongo themselves later.
- Session cap with FIFO eviction so a misbehaving bot can't blow up the sessions collection.

### Audience of `/v1/login`

Password login (`/v1/login` on both portal and botplatform) is **only valid for accounts whose `users.roles` contains `bot` or `admin`**. Regular users keep their existing SSO flow — `GET /api/userInfo` for site discovery (returns the existing rich employee shape) and `POST /auth` on auth-service with `kind=user, ssoToken=<OIDC>`. A regular user reaching `/v1/login` (even if some bcrypt material happened to exist on their doc) gets `401 auth_invalid_credentials`, uniform with wrong-password, so the wire never reveals which accounts are password-eligible.

The role gate is **authoritative at botplatform** (because bot SDKs hit botplatform directly, bypassing portal) and **early at portal** (fail-fast to avoid the east-west hop on the common rejection).

## Non-Goals (v1)

- HMAC-keyed token storage / `bp_` prefix dispatch (auth.md §2). Single-scheme storage hash.
- Valkey session cache (auth.md §8). All reads hit Mongo directly.
- Login rate-limit / lockout (auth.md `LOGIN_MAX_ATTEMPTS` / `LOGIN_LOCKOUT`).
- Cross-site validate forwarding (validate is local-DB only; routing to the right site is a gateway concern).
- Migration job to import legacy `users.services.resume.loginTokens[]` (separate runbook).
- Admin-revoke / list-sessions / rotate-password endpoints.
- `principal.class` field on validate (auth.md). Callers derive their own from `roles`.

## Architecture

```
[bot SDK]              ──direct──> [botplatform /v1/login] ──> bcrypt, write session
                                          │
                                          └── legacy {userId, authToken, me}

[web/mobile/desktop]   ──short-url GDNS──> [portal /v1/login]
[admin UI]                                      ├── lookup users.{username} in chat DB → siteId
                                                ├── forward {username, password} to home-site botplatform
                                                └── transform response → {userId, authToken, account,
                                                       authServiceUrl, baseUrl, natsUrl, siteId}

[any client + token]   ──> [auth-service /auth]
                              ├── kind=user (existing OIDC) — UNCHANGED
                              └── kind=bot|admin (NEW)
                                     ├── POST botplatform /v1/auth/validate
                                     ├── derive scope from principal.roles
                                     │     admin → chat.>
                                     │     bot   → chat.bot.{stripped}.>     (drop trailing `.bot`)
                                     │     user  → chat.user.{account}.>
                                     └── mint NATS JWT
```

Three services touched. One new (`botplatform-service`). Two modified (`portal-service`, `auth-service`). Bot SDK has no portal dependency; the portal path is for web/mobile/desktop/admin clients only.

The `users` collection lives in the chat DB and is **globally replicated** to every site's chat DB, so portal at any site can find any user; botplatform/auth-service at any site can look up any user locally.

### Client flows

**Bot SDK** (legacy wire compatibility — no portal `/v1/login` hop):

1. `GET /api/userInfo?account=<botname>.bot` against **any** portal (closest via GDNS). Returns the 5-field minimal shape `{account, siteId, authServiceUrl, baseUrl, natsUrl}`.
2. `POST /api/v1/login` against the home site (`baseUrl/api/v1/login`, ingress-routed to `botplatform-service`). Username + password → legacy `{userId, authToken, me}`.
3. `POST /auth` against home `authServiceUrl` with `kind=bot, authToken=<session>` → NATS JWT scoped `chat.bot.{stripped}.>`.

**Web / mobile / desktop / admin** (one-call login; discovery folded into the response):

1. `POST /v1/login` against **any** portal (closest via GDNS). Portal looks up `users.{username}` for siteId, east-west-forwards `{username, password}` to home-site `botplatform /v1/login`, transforms the response → `{userId, authToken, account, siteId, authServiceUrl, baseUrl, natsUrl}`.
2. `POST /auth` against home `authServiceUrl` with `kind=user|admin, authToken=<session>` → NATS JWT scoped `chat.user.{account}.>` (or `chat.>` for admin).

## Section 1 — Data model

### `users` collection (extended)

Per-site chat DB, globally replicated. One new nested field added at the legacy Rocket.Chat path so a migration can rsync passwords without remapping:

```
{
  _id:                     "<17-char Meteor ID>",
  account:                 "<botname>.<shortcode>.bot",   // bot pattern; shortcode is opaque to login
  siteId:                  "<siteId>",                    // authoritative home site
  roles:                   ["bot"] | ["admin"] | ["user"] | ...,
  requirePasswordChange:   true | false,                  // existing legacy field; surfaced in login response
  services: {
    password: {
      bcrypt: "<bcrypt hash>"                             // NEW field path
    }
  },
  // existing employee/HR fields unchanged
}
```

**Account string is opaque to login.** The `shortcode` segment in a bot's account string is a human-friendly site shortcode and may differ from the bot's actual home `siteId`. The server NEVER parses the account to extract a siteId — `users.siteId` is the only authoritative source. No `siteId ↔ shortcode` map is needed for v1 (the map is potentially useful for admin/creation UIs, out of scope here).

- Hash recipe (exact legacy match): `bcrypt(sha256_hex(plaintext))`, cost 10 — so legacy passwords round-trip with no rehash.
- Go struct addition to `pkg/model/user.go`: `Services struct { Password struct { Bcrypt string `bson:"bcrypt" json:"-"` } `bson:"password,omitempty"` } `bson:"services,omitempty"`` and a masked `String()` override so logging the user never leaks the hash.
- No new index — `account` is already unique.

### `sessions` collection (NEW)

Per-site chat DB. Owned by `botplatform-service`.

```
{
  _id:        "<base64(sha256(rawToken))>",    // 44-char std-base64
  userId:     "<17-char Meteor ID>",
  account:    "<botname>.<shortcode>.bot",     // denormalized so validate avoids a users join
  siteId:     "<siteId>",                      // stamped at issue, never mutated
  roles:      ["bot"],                         // denormalized snapshot at login time
  issuedAt:   1735000000000                    // unix ms
}
```

Indexes:

- `{_id: 1}` — automatic; primary lookup for validate.
- `{userId: 1, issuedAt: 1}` — compound; used by FIFO eviction (oldest by `issuedAt` ASC) and by future list-sessions / revoke-all.

No TTL index (permanent sessions per v1 decision). No `scheme` field (single hash scheme — opaque sha256, no `bp_` prefix dispatch).

### Token format

- Raw token = 32 random bytes (`crypto/rand`) → `base64.RawURLEncoding` → 43-char string. No prefix.
- Wire-identical to legacy opaque Rocket.Chat token shape so the bot SDK doesn't notice the switch.
- Storage hash = `base64.StdEncoding(sha256(rawToken))` → 44-char `_id`.
- Raw token returned to the client exactly once at login; only the hash lives in Mongo.

## Section 2 — Endpoint contracts

### `botplatform-service` (NEW, per-site)

#### `POST /v1/login` (and `/api/v1/login` alias)

Both paths registered on the same handler so legacy bots sending the `/api/` path work without any ingress rewrite rule. Alias removed after migration sunset.

Request:

```json
{ "username": "<botname>.<shortcode>.bot", "password": "<plaintext>" }
```

Success — `200 OK`:

```json
{
  "status": "success",
  "data": {
    "userId":    "<17-char Meteor ID>",
    "authToken": "<43-char base64url>",
    "me": {
      "_id":                    "<17-char>",
      "username":               "<botname>.<shortcode>.bot",
      "name":                   "<display name>",
      "active":                 true,
      "roles":                  ["bot"],
      "requirePasswordChange":  false
    }
  }
}
```

The `requirePasswordChange` field passes through verbatim from `users.requirePasswordChange`. Login still succeeds and returns a session token even when `true`; the client (Desktop 2.0, admin portal) is responsible for routing the user to a password-update flow before normal app use.

Errors:

| Status | reason | When |
|---|---|---|
| 400 | `auth_missing_fields` | empty username or password |
| 401 | `auth_invalid_credentials` | unknown account OR wrong password OR account exists but `roles` lacks both `bot` AND `admin` (uniform — no enumeration) |
| 403 | `auth_wrong_site` | account's `siteId` ≠ this service's `SITE_ID` (provisioning gate) |
| 500 | (internal) | Mongo find/insert error; raw `fmt.Errorf("verb noun: %w", err)` |

The handler **always runs bcrypt** even when the account is unknown or the role gate fails (compare against a fixed dummy hash), so timing is indistinguishable between "wrong password", "unknown account", and "not password-eligible".

#### `POST /v1/auth/validate`

Request:

```json
{ "authToken": "<raw>" }
```

Success — `200 OK`:

```json
{
  "valid": true,
  "principal": {
    "userId":  "<17-char>",
    "account": "<botname>.<shortcode>.bot",
    "siteId":  "<siteId>",
    "roles":   ["bot"]
  }
}
```

Errors:

| Status | reason | When |
|---|---|---|
| 400 | `auth_missing_fields` | empty token |
| 401 | `auth_invalid_token` | hash not found in `sessions` |

#### `GET /healthz`

200 OK after a successful Mongo ping.

### `portal-service` (MODIFIED)

#### `POST /v1/login` (NEW)

Request:

```json
{ "username": "<botname>.<shortcode>.bot", "password": "<plaintext>" }
```

Success — `200 OK` (8 fields):

```json
{
  "userId":                 "<17-char>",
  "authToken":              "<43-char base64url>",
  "account":                "<botname>.<shortcode>.bot",
  "siteId":                 "<siteId>",
  "authServiceUrl":         "<auth-service URL for the home site>",
  "baseUrl":                "<chat REST base URL for the home site>",
  "natsUrl":                "<NATS WebSocket URL for the home site>",
  "requirePasswordChange":  false
}
```

`requirePasswordChange` is read by portal **directly from the user doc** that it already loaded to discover `siteId` — not from the upstream botplatform response. Portal therefore never introspects botplatform's legacy `me` block; the two response shapes are fully decoupled.

Handler logic:

1. Read `users.{username}` from the chat DB. If not found → `401 auth_invalid_credentials` (portal-local; no upstream call).
2. Pluck `siteId` from the doc.
3. Resolve `botplatformUrl`, `authServiceUrl`, `baseUrl`, `natsUrl` from `PORTAL_SITE_URLS[siteId]`. If missing → `500 site_unknown`.
4. Forward `{username, password}` to `{botplatformUrl}/v1/login` via Resty (5s timeout; propagate inbound `X-Request-ID` header).
5. On `200`: read upstream `{userId, authToken}`, drop `me`, emit the 7-field shape.
6. On non-200: propagate the upstream errcode envelope verbatim (401 stays 401, 403 stays 403).
7. On Resty timeout / network error: `502 auth_upstream_unavailable`.

Portal does **not** run bcrypt itself — botplatform is the single auth authority. The user-doc read is just discovery for `siteId`.

#### `GET /api/userInfo?account=…` (MODIFIED — role-aware response)

Existing endpoint, but the response shape now branches on `users.{account}.roles`:

- If `roles` contains `bot` OR `admin`:
  ```json
  {
    "account":        "<botname>.<shortcode>.bot",
    "siteId":         "<siteId>",
    "authServiceUrl": "...",
    "baseUrl":        "...",
    "natsUrl":        "..."
  }
  ```
- Otherwise: existing rich employee shape (unchanged for the human SSO flow).

### `auth-service` (MODIFIED — extended for session-token branch)

#### `POST /auth` — extended

Request (backward-compatible — exactly one of `ssoToken` / `authToken` present; the server routes on which field is set, not on any client-declared "kind"):

```json
{
  "ssoToken":      "<OIDC token>",         // existing SSO path
  "authToken":     "<session token>",      // NEW session-token path
  "natsPublicKey": "..."
}
```

Logic:

- **Auto-route by which token is present:**
  - `ssoToken` set, `authToken` absent → existing OIDC path. UNCHANGED.
  - `authToken` set, `ssoToken` absent → NEW session-validate path.
  - Both set OR neither set → `400 auth_ambiguous_token` / `auth_missing_token`.
- **Session-validate path:**
  1. POST `{authToken}` to local `botplatform /v1/auth/validate` (5s timeout).
  2. On `valid=true`, derive scope from `principal.roles` via `pkg/principal.NATSSubjectScope` (role priority **admin > bot > user**; multi-role users always get the highest-privilege scope they qualify for).
  3. Mint NATS JWT with that scope.
- Validate failure → `401 auth_invalid_token`.
- Botplatform unreachable → `502 auth_upstream_unavailable`.

**Why no `kind` field:** a user may legitimately hold multiple roles (e.g. `roles=[admin, bot]`). A client-declared `kind` would couple the wire shape to a single role and bug-out at role changes. Scope is server-authoritative, derived from `users.roles` at validate time.

Response envelope unchanged. For the session-validate path, the `user` block carries `{account, siteId, roles}` from the validate principal (no employee fields).

### Shared helper — `pkg/principal/scope.go`

The helper returns the **complete pub/sub allowlist** for a principal — not just the own-namespace pattern — so `auth-service` composes the NATS JWT by iterating the returned slices. This makes the role→permission mapping a single source of truth, isolated from the JWT-minting code, and unit-testable without I/O.

```go
type Scope struct {
    PubAllow []string
    SubAllow []string
}

// NATSSubjectScope returns the full pub/sub allowlist for a principal.
// Role priority: admin > bot > user (admin wins on multi-role users).
func NATSSubjectScope(account string, roles []string) Scope {
    if hasRole(roles, "admin") {
        return Scope{
            PubAllow: []string{"chat.>", "_INBOX.>"},
            SubAllow: []string{"chat.>", "_INBOX.>"},
        }
    }
    if hasRole(roles, "bot") && strings.HasSuffix(account, ".bot") {
        own := "chat.bot." + strings.TrimSuffix(account, ".bot") + ".>"
        return Scope{
            PubAllow: []string{own, "_INBOX.>", "chat.user.presence.*.query.batch"},
            SubAllow: []string{own, "chat.room.>", "_INBOX.>", "chat.user.presence.state.*"},
        }
    }
    own := "chat.user." + account + ".>"
    return Scope{
        PubAllow: []string{own, "_INBOX.>", "chat.user.presence.*.query.batch"},
        SubAllow: []string{own, "chat.room.>", "_INBOX.>", "chat.user.presence.state.*"},
    }
}
```

#### Concrete pub/sub allowlists by role

The bot and user shapes are deliberately parallel — same room visibility, same presence behavior, same inbox semantics — they differ ONLY in which subject tree their own namespace lives under. Admin is a single broad allow that subsumes everything except NATS system / JetStream API subjects.

| Permission line | User (today) | Bot (new) | Admin (new) |
|---|---|---|---|
| Own namespace Pub | `chat.user.{account}.>` | `chat.bot.{stripped}.>` | covered by `chat.>` |
| Own namespace Sub | `chat.user.{account}.>` | `chat.bot.{stripped}.>` | covered by `chat.>` |
| Room broadcasts Sub | `chat.room.>` | `chat.room.>` | covered by `chat.>` |
| Inbox Pub/Sub | `_INBOX.>` | `_INBOX.>` | `_INBOX.>` |
| Presence read (Sub) | `chat.user.presence.state.*` | `chat.user.presence.state.*` | covered by `chat.>` |
| Presence query (Pub) | `chat.user.presence.*.query.batch` | `chat.user.presence.*.query.batch` | covered by `chat.>` |
| Other principals' namespaces | denied | denied | allowed (via `chat.>`) |

Where `{stripped}` for a bot is `strings.TrimSuffix(account, ".bot")` — e.g. `<botname>.<shortcode>.bot` → `<botname>.<shortcode>`. Strip is suffix-only so `siteId`-shaped shortcodes naturally stay in the subject path (useful for the traffic-isolation spec's per-site routing).

#### Composition in auth-service

```go
scope := principal.NATSSubjectScope(account, roles)
for _, s := range scope.PubAllow { uc.Pub.Allow.Add(s) }
for _, s := range scope.SubAllow { uc.Sub.Allow.Add(s) }
```

That's the entire integration. Mechanical, no role-specific branching in `auth-service` itself — all the policy is in `pkg/principal`.

Sole caller: `auth-service`. Centralizes the role→permission mapping so it doesn't drift if a second consumer appears.

## Section 3 — Service skeleton (`botplatform-service`)

Flat per-service directory at repo root, matching the repo convention:

```
botplatform-service/
├── main.go                 # Config, Mongo client, Gin engine, graceful shutdown via pkg/shutdown.Wait
├── routes.go               # registerRoutes(r, h)
├── handler.go              # HandleLogin, HandleValidate, HandleHealth
├── handler_test.go         # Unit tests with mocked store
├── store.go                # store interface + //go:generate mockgen
├── store_mongo.go          # Mongo implementation
├── mock_store_test.go      # Generated; never edited
├── integration_test.go     # //go:build integration; uses testutil.MongoDB
├── helper.go               # errcode sentinels (errInvalidCreds, errWrongSite)
└── deploy/
    ├── Dockerfile
    ├── docker-compose.yml
    └── azure-pipelines.yml
```

`store.go` interface (kept narrow per CLAUDE.md):

```go
type store interface {
    FindUserByAccount(ctx context.Context, account string) (*model.User, error)
    InsertSession(ctx context.Context, s session) error
    FindSessionByHash(ctx context.Context, hash string) (*session, error)
    CountSessions(ctx context.Context, userID string) (int64, error)
    DeleteOldestSessions(ctx context.Context, userID string, drop int) error
    Ping(ctx context.Context) error
}
```

Config (env vars, parsed via `caarlos0/env`):

| Var | Type | Default | Required | Notes |
|---|---|---|---|---|
| `PORT` | string | `"8080"` | no | HTTP listen |
| `SITE_ID` | string | — | yes | Provisioning gate target |
| `MONGO_URI` | string | — | yes | Chat DB connection |
| `MONGO_DB` | string | — | yes | Database name |
| `SESSIONS_MAX_PER_ACCOUNT` | int | `100` | no | FIFO cap |
| `BCRYPT_COST` | int | `10` | no | Legacy match |
| `DEV_MODE` | bool | `false` | no | Local-dev affordances |

No NATS, no Valkey, no auth-service URL — botplatform-service is a self-contained HTTP service with one Mongo dependency.

## Section 4 — Login handler logic (botplatform)

```
1. Decode body. If username or password is empty → 400 auth_missing_fields.
2. user = store.FindUserByAccount(account)
   - On mongo.ErrNoDocuments → bcrypt-compare against a fixed dummy hash to equalize timing,
     then return 401 auth_invalid_credentials.
   - On other error → return wrapped error (collapses to 500 internal).
3. Role gate: if user.Roles does NOT contain "bot" and does NOT contain "admin"
   → bcrypt-compare against a fixed dummy hash to equalize timing,
     then return 401 auth_invalid_credentials.
4. If user.SiteID != cfg.SiteID → 403 auth_wrong_site.
5. ok = bcryptVerify(user.Services.Password.Bcrypt, plaintext)
   - bcryptVerify performs sha256_hex(plaintext) then bcrypt.CompareHashAndPassword.
   - If !ok → 401 auth_invalid_credentials.
6. raw = generateToken()                 // 32 bytes → 43-char base64url
   hash = base64(sha256(raw))             // 44-char std-base64
7. store.InsertSession(session{_id: hash, userId, account, siteId, roles, issuedAt: now})
8. Cap eviction:
   count = store.CountSessions(user.ID)
   over  = count - cfg.SessionsMaxPerAccount
   if over > 0:
       store.DeleteOldestSessions(user.ID, over)
       metric: botplatform_sessions_evicted_total += over
9. Emit {status:"success", data:{userId: user.ID, authToken: raw, me: {...}}}.
10. slog.Info("login ok", "account", account, "userId", user.ID)
```

Failure logs: `slog.Warn("login denied", "account", account, "reason", reason)`. Never log the plaintext password, the raw token, the storage hash, or any field of `user.Services.Password`.

## Section 5 — Validate handler logic (botplatform)

```
1. Decode body. If authToken is empty → 400 auth_missing_fields.
2. hash = base64(sha256(authToken))
3. s = store.FindSessionByHash(hash)
   - On mongo.ErrNoDocuments → 401 auth_invalid_token; body {valid: false}.
   - On other error → wrapped 500.
4. Emit {valid: true, principal: {userId, account, siteId, roles}}.
```

Validate does **not** check `siteId == SITE_ID` — a token in the local DB is by definition local. Cross-site routing is the gateway's job.

## Section 6 — Portal forwarder logic

```
1. Decode body. Empty username/password → 400 auth_missing_fields.
2. user = store.FindUserByAccount(username)
   - Not found → 401 auth_invalid_credentials (no upstream call).
3. Role gate: if user.Roles does NOT contain "bot" and does NOT contain "admin"
   → 401 auth_invalid_credentials (no upstream call; fail-fast for SSO-only users).
4. site = cfg.SiteURLs[user.SiteID]
   - Not present → 500 site_unknown.
5. Forward {username, password} to site.BotplatformURL + "/v1/login" via Resty.
   - Propagate inbound X-Request-ID on the outbound call.
   - 5s timeout.
6. On upstream 200: decode {userId, authToken} from the legacy 3-field
   response (the upstream `me` block is discarded — portal never reads it).
   Respond with the 8-field shape built from LOCAL data only:
   {userId, authToken, account: username, siteId: user.SiteID,
    authServiceUrl: site.AuthServiceURL, baseUrl: site.BaseURL,
    natsUrl: site.NatsURL,
    requirePasswordChange: user.RequirePasswordChange}.
7. On upstream 401/403/400: re-marshal the upstream errcode envelope verbatim.
8. On Resty timeout / network error: 502 auth_upstream_unavailable.
9. slog.Info("login ok", "account", username, "userId", userId) on success.
```

Portal's role gate is **early-rejection only** — botplatform re-runs the same check authoritatively, since bot SDKs bypass portal. The two checks are intentionally redundant.

Portal's `PORTAL_SITE_URLS` env var (already JSON-shaped per the existing `portal-service` config) gains a `botplatformUrl` per-site entry alongside `authServiceUrl`/`baseUrl`/`natsUrl`.

## Section 7 — Error handling, logging, observability

Standard project pattern (`docs/error-handling.md`, CLAUDE.md §6 "Error Handling at the NATS/HTTP Boundary"):

- Handlers return typed errors built from `errcode.BadRequest`/`Unauthenticated`/`Forbidden`/`Internal`/`BadGateway` constructors with `errcode.WithReason(...)`. `errhttp.Write(ctx, c, err)` does wire serialization.
- New reason codes added in `pkg/errcode/codes_botplatform.go`:
  - `auth_missing_fields`, `auth_invalid_credentials`, `auth_invalid_token`, `auth_wrong_site`, `auth_upstream_unavailable`, `site_unknown`.
- Sentinels (`errInvalidCreds`, `errWrongSite`) per-service in `helper.go` so `errors.Is` works.
- Infra failures → raw `fmt.Errorf("verb noun: %w", err)` (collapses to `internal` at the boundary).

Logging (`log/slog` JSON, never interpolated):

- Success: `slog.Info("login ok", "account", account, "userId", userId)` (botplatform + portal).
- Failure: `slog.Warn("login denied", "account", account, "reason", reason)`.
- **Never logged:** plaintext password, raw `authToken`, raw bcrypt hash, `services.password.bcrypt` value.

Request-ID propagation: existing `pkg/idgen.GenerateRequestID()` middleware; portal forwards inbound `X-Request-ID` on the Resty call so the same ID threads end-to-end.

Metrics (Prometheus, `/metrics` endpoint on botplatform):

- `botplatform_login_total{outcome="ok|invalid_credentials|wrong_site|error"}`
- `botplatform_login_duration_seconds` (histogram)
- `botplatform_validate_total{outcome="ok|invalid_token|error"}`
- `botplatform_validate_duration_seconds`
- `botplatform_sessions_evicted_total`

OpenTelemetry tracing inherited from existing `pkg/` middleware. No new spans defined.

## Section 8 — Test plan

TDD per CLAUDE.md §4. 80% coverage floor, 90% target for handlers.

### Unit tests — `botplatform-service`

Table-driven; mocked store via `mockgen`.

- **Login happy path** — valid creds → 200, response shape, fresh authToken each call, inserted session has correct `_id` (`base64(sha256(token))`), `userId`, `account`, `siteId`, `roles`, `issuedAt ≈ now`.
- **Login failure paths** — empty fields (400), unknown account (401, uniform), wrong password (401, uniform), account without bot/admin role (401, uniform — SSO-only user attempted password login), `siteId` mismatch (403), Mongo find/insert errors (500).
- **Username enumeration guard** — wrong-password vs unknown-account vs not-role-eligible responses byte-identical except request-id; average wall-clock within tolerance (bcrypt-compare runs on all three rejection paths).
- **bcryptVerify** — golden fixture: known bcrypt hash for known plaintext (under `testdata/`); positive and negative cases.
- **Cap eviction (FIFO)** — below cap: no eviction; at cap: one eviction by oldest `issuedAt` ASC, metric increment; over cap: trim to exact cap; cross-account safety (login as user A doesn't evict user B's sessions).
- **Validate happy / error** — known token returns principal; unknown returns 401 `{valid:false}`; empty body 400; trailing whitespace not trimmed.
- **Token format** — generated tokens are 43-char base64url, no padding; 32-byte entropy distribution check; never start with `bp_`.

### Unit tests — `portal-service`

Additions to existing `handler_test.go`:

- **`/v1/login` forwarder** — known user with bot/admin role → look up siteId → Resty POST to upstream (mocked via `httptest.Server`) → 7-field response. Unknown user → 401 portal-local (no upstream call). Known user without bot/admin role → 401 portal-local (no upstream call). siteId not in registry → 500 `site_unknown`. Upstream 401 → propagate. Upstream timeout → 502 `auth_upstream_unavailable`. Inbound `X-Request-ID` header propagated.
- **`/api/userInfo` role split** — `roles=[user]` → existing rich shape; `roles=[bot]` → minimal 5-field shape; `roles=[admin]` → minimal; `roles=[bot,admin]` → minimal; unknown account → 404.

### Unit tests — `auth-service`

Additions to existing `handler_test.go`:

- `ssoToken` set, `authToken` absent → existing OIDC path runs, no botplatform validate call.
- `authToken` set, `ssoToken` absent → botplatform validate mocked.
  - principal `roles=["bot"]` + account `<botname>.<shortcode>.bot` → NATS JWT scope `chat.bot.<botname>.<shortcode>.>`.
  - principal `roles=["admin"]` → scope `chat.>`.
  - principal `roles=["admin","bot"]` → admin scope wins (role priority admin > bot > user).
  - principal `roles=["bot","user"]` → bot scope wins.
- Both tokens set → 400 `auth_ambiguous_token`.
- Neither token set → 400 `auth_missing_token`.
- Invalid session token → upstream 401 → 401 `auth_invalid_token`.
- Botplatform unreachable → 502 `auth_upstream_unavailable`.

### Unit tests — `pkg/principal/scope_test.go`

Pure, no I/O. Each case asserts the FULL pub + sub allowlists (not just the own-namespace pattern):

- `roles=[admin]` → `Pub=[chat.>, _INBOX.>]`, `Sub=[chat.>, _INBOX.>]`.
- `roles=[bot]` + `<botname>.<shortcode>.bot` →
  - Pub: `[chat.bot.<botname>.<shortcode>.>, _INBOX.>, chat.user.presence.*.query.batch]`
  - Sub: `[chat.bot.<botname>.<shortcode>.>, chat.room.>, _INBOX.>, chat.user.presence.state.*]`
- `roles=[user]` + `<account>` →
  - Pub: `[chat.user.<account>.>, _INBOX.>, chat.user.presence.*.query.batch]`
  - Sub: `[chat.user.<account>.>, chat.room.>, _INBOX.>, chat.user.presence.state.*]`
- Empty roles → identical to `roles=[user]` (default).
- `roles=[bot]` + no `.bot` suffix → identical to `roles=[user]` (defensive fallback).
- `roles=[admin, bot]` → admin scope (priority admin > bot > user).
- `roles=[admin, user]` → admin scope.
- `roles=[bot, user]` → bot scope.
- **Subject-injection guard** — `account="alice.>"` or `account="alice.*"` must be rejected upstream by `subject.IsValidAccountToken` (existing helper used by auth-service today). The scope helper trusts the caller to validate; tests assert that on an invalid account the helper still returns a well-formed Scope (no panic) — auth-service is responsible for refusing the request before this point.

### Integration tests (`//go:build integration`)

`botplatform-service/integration_test.go`:

- `TestMain(m) { testutil.RunTestsWithPrewarm(m, testutil.EnsureMongo) }`.
- `testutil.MongoDB(t, "botplatform")` per-test isolation.
- End-to-end login → validate: seed user with real bcrypt of "secret"; POST `/v1/login`; capture authToken; POST `/v1/auth/validate` with it; assert principal.
- Index assertions: `sessions` has `{_id:1}` and `{userId:1, issuedAt:1}` after service start.
- Real FIFO eviction: `SESSIONS_MAX_PER_ACCOUNT=3`, issue 5 logins for the same user, assert exactly 3 sessions remain (oldest 2 gone), earlier tokens 401.
- Cross-account safety: two users at cap; login as A doesn't evict B's sessions.

Portal and auth-service integration tests follow the same shape using their existing testutil helpers.

### Smoke

- `make build SERVICE=botplatform-service` → binary.
- `docker compose up` in `botplatform-service/deploy/` → bot can login locally with a seeded user.
- `make lint`, `make test`, `make test-integration SERVICE=botplatform-service`, `make sast` all green before commit.

## Section 9 — `docs/client-api.md` updates (in scope this PR)

The repo rule (CLAUDE.md §5 "Before Committing") requires `docs/client-api.md` updates when a client-facing handler changes. This PR ships three client-facing endpoints, so the doc gets three new entries in the same PR:

- `POST /v1/login` (botplatform-service) — request body, response body (legacy shape), error reasons.
- `POST /v1/auth/validate` (botplatform-service) — request, response (principal), error reasons.
- `POST /v1/login` (portal-service) — request, response (7-field shape), error reasons.
- Updated `GET /api/userInfo` — note the role-aware response branching and document both shapes.
- Extended `POST /auth` (auth-service) — document the new `kind` field and `authToken` field on the request; document role-derived scope on the response side.

## Section 10 — Deployment & cutover

Per the user's directive: **no canary rollout**. Direct 0→100 switch on the Istio VirtualService at cutover. Header-based routing (e.g., `bp_` prefix sniffing) is explicitly not implemented for v1 because we kept the token format identical to legacy — there is no in-band way to tell new from old.

- Per-site rollout: stand up `botplatform-service` deployment at site, run migration (out-of-scope job) to backfill `users.services.password.bcrypt` for bot accounts, smoke-test, then flip the VirtualService origin for `/api/v1/login` from legacy → botplatform.
- Rollback: flip VirtualService back. Sessions issued during the new-service window will not validate against legacy and the affected bots will re-login on next 401 (bot SDKs treat 401 as "re-login").

## Files

New:

- `botplatform-service/main.go`
- `botplatform-service/routes.go`
- `botplatform-service/handler.go`
- `botplatform-service/handler_test.go`
- `botplatform-service/store.go`
- `botplatform-service/store_mongo.go`
- `botplatform-service/mock_store_test.go`
- `botplatform-service/integration_test.go`
- `botplatform-service/helper.go`
- `botplatform-service/testdata/bcrypt_fixture.json`
- `botplatform-service/deploy/Dockerfile`
- `botplatform-service/deploy/docker-compose.yml`
- `botplatform-service/deploy/azure-pipelines.yml`
- `pkg/principal/scope.go`
- `pkg/principal/scope_test.go`
- `pkg/errcode/codes_botplatform.go`

Changed:

- `pkg/model/user.go` — add `Services.Password.Bcrypt` field (`json:"-"`); update `String()` mask.
- `portal-service/handler.go` — add `HandleLogin`, role-aware `HandleUserInfo` branch.
- `portal-service/handler_test.go` — forwarder + role-split tests.
- `portal-service/main.go` — config additions: `botplatformUrl` per-site entry, Resty client.
- `portal-service/routes.go` — register `POST /v1/login`.
- `portal-service/store.go` / `store_mongo.go` — add `FindUserByAccount` (already partly present via `ListEmployees`).
- `auth-service/handler.go` — `kind=bot|admin` branch, botplatform validate call, scope derivation via `pkg/principal`.
- `auth-service/handler_test.go` — new kind branches.
- `auth-service/main.go` — `BOTPLATFORM_URL` env var, Resty client.
- `docs/client-api.md` — three new endpoints + two updated.

## Out of scope (deferred)

- Valkey session cache (auth.md §8).
- Login rate-limit / lockout.
- Cross-site validate forwarding.
- `principal.class` field on validate response.
- Migration job to import legacy `users.services.resume.loginTokens[]`.
- Admin self-service: list-sessions, revoke-session, rotate-password.
- HMAC-keyed token storage and `bp_` prefix dispatch (auth.md §2).
- Header-based canary routing.
