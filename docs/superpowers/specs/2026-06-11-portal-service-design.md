# Portal Service — Site Discovery for Login — Design

**Date:** 2026-06-11
**Status:** Approved (amended 2026-06-11: enforced provisioning at auth-service, shared `pkg/ginutil` middleware; amended 2026-06-12: in-memory directory cache — see below)
**Branch:** `claude/eager-einstein-7u2je6`

## Amendments (2026-06-12)

The portal's data model changed during implementation review. **These
amendments supersede the body below**, which is retained unchanged as the
original approved design. As shipped:

- **No portal `users`/`sites` collections.** The directory is the HR-owned
  `hr_employee` collection (`account`, `employeeId`, `siteId`, `natsUrl`),
  rewritten by the daily HR cron.
- **In-memory cache, zero per-request Mongo.** A startup goroutine loads
  `hr_employee` into a map; the whole map is swapped on a periodic refresh
  (`PORTAL_CACHE_REFRESH_INTERVAL`, default 24h, matching the cron; 30s retry
  after a failed load). Entries have no TTL. `/lookup` is a single cache hit.
- **`authServiceUrl` is derived, not stored**: `PORTAL_AUTH_URL_TEMPLATE`
  (required) substitutes `{siteId}` into a placeholder URL; a value without
  the placeholder is used verbatim (single-site local dev).
- **Liveness/readiness split**: `/healthz` is liveness-only; new `GET /readyz`
  returns 503 until the cache holds directory data.
- **Portal miss reason is `account_not_ready`** (403), reflecting the daily
  sync cadence; `account_not_provisioned` is emitted only by the auth-service
  minting gate. The `DirectoryStore` interface is `ListEmployees` only; the
  Mongo store additionally ensures a unique `hr_employee(account)` index at
  startup, so duplicate HR rows fail at write time (the cache also rejects a
  duplicate-bearing snapshot defensively, e.g. if the cron drops and recreates
  the collection, losing the index until the next portal restart).
- **Dev fallback** synthesizes an entry from `PORTAL_DEV_FALLBACK_SITE_ID` +
  `PORTAL_DEV_FALLBACK_NATS_URL` and the same auth-URL template — no seeded
  fallback site record required.
- **Endpoint is `GET /api/userInfo?account={account}`, discovery-only.** The
  portal validates no SSO token and has no OIDC dependency at all; the account
  is the query-param lookup key (the frontend derives it from the SSO token's
  `preferred_username` in prod, or the dev login form in dev). It returns
  non-secret directory data; the authoritative gate is auth-service, which
  validates the token and enforces provisioning before minting a JWT. `devMode`
  now governs only the dev-site fallback, not token handling. Body references to
  `POST /lookup` and portal-side OIDC validation are obsolete.

## Problem

The system is federated: each site runs its own NATS, MongoDB, and
auth-service. But the frontend's connection targets are **static, single-site
runtime config** (`AUTH_URL`, `NATS_URL` in
`chat-frontend/src/lib/runtimeConfig.js`), and the user must **manually type
their Site ID** on the LoginPage before signing in (stashed in
`sessionStorage('oidc.siteId')` across the Keycloak redirect). A user has no
way to discover which site they belong to — they must be told out-of-band, and
a multi-site deployment cannot serve all users from one frontend config.

## Goal

After a successful Keycloak login, the frontend resolves the user's home site
automatically: which auth-service to mint the NATS JWT from, which NATS server
to connect to, and which siteId scopes their data. No typed Site ID anywhere —
SSO **and** dev-mode login. One frontend deployment serves users of all sites.

Provisioning is also **enforced**, not advisory: an account absent from the
directory cannot mint NATS credentials at all. The gate lives in auth-service
(the minting authority), so it holds at login *and* at every background JWT
refresh — see "Enforced provisioning" below.

## Approach

A new **`portal-service`**: a small Gin HTTP service sitting *in front of*
auth-service in the login sequence. It is a **discovery directory, not an auth
proxy** — it validates the SSO token, looks the account up in a portal-owned
directory, and returns connection coordinates. The frontend then talks to the
resolved auth-service directly (including the background NATS-JWT refresh
loop) and opens the NATS WebSocket itself.

**Rejected alternative — auth facade (credential broker):** portal could
proxy the `/auth` call and return `natsJwt` + `natsUrl` in one round trip,
with auth-service locked to internal-only ingress (the approach on branch
`claude/dreamy-bell-cqdibt`). Rejected because every JWT re-mint for every
user of every site would then flow through the portal (a new choke point and
outage domain on the credential hot path), it duplicates auth-service's
contract, and it needs network-level lockdown plus reload choreography for an
in-memory directory. As a pure directory, a portal outage only blocks new
logins; connected users keep refreshing against their site's auth-service.
The broker's two genuinely architecture-independent ideas — an *enforced*
provisioning gate and shared Gin middleware — are adopted below, with the
gate relocated to the minting authority where it cannot be bypassed.

### Login flow (after)

```text
Browser                    portal-service              site auth-service       site NATS
   │ Keycloak login (PKCE)      │                            │                     │
   │──────────────────────────▶ │                            │                     │
   │ POST /lookup {ssoToken}    │                            │                     │
   │──────────────────────────▶ │ validate OIDC,             │                     │
   │                            │ users[account] → siteId    │                     │
   │ ◀──────────────────────────│ sites[siteId] → URLs       │                     │
   │ {account, employeeId,      │                            │                     │
   │  authServiceUrl, natsUrl,  │                            │                     │
   │  siteId}                   │                            │                     │
   │ POST {authServiceUrl}/auth {ssoToken, natsPublicKey}    │                     │
   │────────────────────────────────────────────────────────▶│ minting gate:       │
   │                            │                            │ users[account]      │
   │                            │                            │ + siteId == SITE_ID │
   │ ◀────────────────────────────────────────────{natsJwt, user}                  │
   │ WebSocket connect {natsUrl} + natsJwt                                         │
   │──────────────────────────────────────────────────────────────────────────────▶│
```

Dev mode is the same shape with `{account}` replacing `{ssoToken}` at both
HTTP hops (portal and auth-service both run `DEV_MODE=true` locally).

## portal-service (backend)

Flat service directory at repo root, `package main`, mirroring auth-service's
layout and middleware (request-ID, access log, CORS — portal is called
directly from the browser).

### Endpoints

- `POST /lookup`
  - Prod (`DEV_MODE=false`): body `{ "ssoToken": "..." }`. Validate via
    `pkg/oidc` (same `Validator` + `TokenValidator` interface shape as
    auth-service). Account via the shared `Claims.Account()` helper
    (`preferred_username` → `name`); blank → reject (same rule as
    auth-service).
  - Dev (`DEV_MODE=true`): body `{ "account": "alice" }`, no OIDC.
  - Lookup: `users` by account → user's `siteId`; `sites` by that siteId →
    URLs. Success response (200):

    ```json
    {
      "account": "alice",
      "employeeId": "E12345",
      "authServiceUrl": "https://auth.site-a.example.com",
      "natsUrl": "wss://nats.site-a.example.com",
      "siteId": "site-a"
    }
    ```
- `GET /healthz` → `{"status":"ok"}`.

### Errors (standard `pkg/errcode` envelope via `errhttp.Write`)

| HTTP | code | reason | when |
|------|------|--------|------|
| 400 | `bad_request` | `missing_fields` | body missing `ssoToken` (prod) / `account` (dev) |
| 401 | `unauthenticated` | `sso_token_expired` | OIDC says expired — reuses auth-service's reason so the frontend's existing redirect-to-relogin handles it unchanged |
| 401 | `unauthenticated` | `invalid_sso_token` | any other OIDC validation failure (cause attached server-side) / blank account claim |
| 403 | `forbidden` | `account_not_provisioned` | account authenticated but not in the portal directory (prod; see dev fallback). Same reason auth-service emits from its minting gate. |
| 500 | `internal` | — | store errors; **also** a `sites` record missing for a user's `siteId` — that is an ops data bug, not a client error (raw wrapped error collapses at the boundary) |

Token/validation reasons reuse the existing constants
(`errcode.AuthMissingFields`, `AuthTokenExpired`, `AuthInvalidToken`). The
provisioning case gets one new constant in `pkg/errcode/codes_portal.go`:
`PortalAccountNotProvisioned Reason = "account_not_provisioned"`, emitted by
both portal-service (lookup) and auth-service (minting gate).

### Dev fallback (account not in directory, DEV_MODE only)

Local devs log in as arbitrary accounts ("alice", "bob") without seeding each
one. In `DEV_MODE=true`, when the account is not in `users`, portal falls back
to `siteId = PORTAL_DEV_FALLBACK_SITE_ID` (envDefault `site-local`) with
`employeeId: ""` and the account echoed back. The fallback site's `sites`
record must still exist (seeded by docker-local); if it doesn't, the lookup
fails as internal. In prod the same miss is a 403 `account_not_provisioned`.

### Data model (portal-owned MongoDB, `MONGO_DB` default `portal`)

- `users` — the global account → site directory. Documents decode into
  `pkg/model.User` (projection: `_id`, `account`, `siteId`, `employeeId`).
  Unique index on `account` (created by `EnsureIndexes` at startup, per repo
  convention). Populated by ops/HR sync — out of scope here beyond local
  seeding.
- `sites` — one document per site:

  ```json
  { "_id": "site-a", "authServiceUrl": "https://auth.site-a.example.com", "natsUrl": "wss://nats.site-a.example.com" }
  ```

  Local `site` struct in portal-service (json+bson tags, `bson:"_id"` ↔ `ID`);
  not shared in `pkg/model` until a second consumer exists.

  **Ownership:** `sites` is infra configuration, owned by ops/IaC (same model
  as JetStream stream configs) — maintained by hand or pipeline, never
  written by services or the HR cron. The portal only reads it.

### Store interface (`store.go`, consumer-defined, mockgen-generated mock)

```go
type DirectoryStore interface {
    FindUserByAccount(ctx, account) (*model.User, error) // ErrUserNotFound sentinel
    FindSiteByID(ctx, siteID) (*site, error)             // ErrSiteNotFound sentinel
    EnsureIndexes(ctx) error
}
```

### Config (`caarlos0/env`)

| Env | Default | Notes |
|-----|---------|-------|
| `PORT` | `8081` | 8080 is auth-service locally |
| `DEV_MODE` | `false` | |
| `OIDC_ISSUER_URL`, `OIDC_AUDIENCES` | — | required when `DEV_MODE=false`, exactly like auth-service |
| `TLS_SKIP_VERIFY` | `false` | optional, dev/staging only — same as auth-service |
| `MONGO_URI` | required | |
| `MONGO_DB` | `portal` | |
| `MONGO_USERNAME`, `MONGO_PASSWORD` | `""` | |
| `PORTAL_DEV_FALLBACK_SITE_ID` | `site-local` | dev-mode only |

### Files

`main.go`, `handler.go`, `routes.go`, `store.go`, `store_mongo.go`,
`handler_test.go`, `integration_test.go`, `mock_store_test.go` (generated),
`deploy/Dockerfile`, `deploy/docker-compose.yml` (with one-shot mongosh seed
of the local site + demo users), `deploy/azure-pipelines.yml`. Gin middleware
comes from the shared `pkg/ginutil` (below) — no per-service copy. No
JetStream, no streams, no OTEL (parity with auth-service).

## Enforced provisioning — the auth-service minting gate

The portal's directory check alone would be advisory: auth-service is public
(the frontend calls it directly to mint and to refresh), so a valid-SSO but
unprovisioned user could skip the portal and still obtain a NATS JWT. The
broker design closes this by hiding auth-service behind the portal; we close
it at the minting authority instead. The portal check remains as fast,
friendly feedback at login; auth-service is the enforcement point.

- **Check:** after deriving the account from the validated token (and before
  signing), auth-service queries its site's `users` collection — the same
  per-site collection `pkg/userstore` consumers already read, maintained by
  the daily ops sync — for `{account: <account>, siteId: <SITE_ID>}`. Match →
  mint. No match → 403 `forbidden` with reason `account_not_provisioned`.
  The explicit `siteId` predicate matters: the per-site `users` collection
  contains users of **other** sites too (e.g. the sample seeder writes
  `site-remote` users into the local DB for cross-site rooms), so a bare
  existence check would not refuse minting on the wrong site's auth-service —
  the compound predicate refuses both unprovisioned and wrong-site accounts.
- **Refresh inherits the gate:** the background JWT re-mint calls the same
  `POST /auth`, so deprovisioning locks a user out within one JWT lifetime —
  no portal involvement and no network lockdown required.
- **Store:** auth-service gains `store.go` (`ProvisionStore` interface:
  `AccountProvisioned(ctx context.Context, account, siteID string) (bool,
  error)` + mockgen directive) and `store_mongo.go` (compound existence query
  against `users`), per the standard service layout. `NewAuthHandler` takes
  the store.
- **Config:** auth-service gains `SITE_ID` (required when the gate is active;
  same convention as room-service/room-worker) and `REQUIRE_PROVISIONED`
  (envDefault `true`) for staged rollout; `MONGO_URI` / `MONGO_DB` (default
  `chat`) / `MONGO_USERNAME` / `MONGO_PASSWORD` are validated and connected
  only when the gate is active. In `DEV_MODE=true` the gate is skipped
  entirely (no Mongo needed locally), preserving log-in-as-anyone.
- **Fail closed:** a store error returns 500 `internal` (raw wrapped error) —
  a Mongo outage must not mint credentials for unverifiable accounts.
- **Rollout note:** this is a breaking deployment change for auth-service in
  prod — upgrading requires either supplying `MONGO_URI` + `SITE_ID` (gate
  on) or setting `REQUIRE_PROVISIONED=false` (gate deferred until the cron
  data is verified). The local `deploy/docker-compose.yml` gains the Mongo +
  `SITE_ID` env lines so the documented "flip `DEV_MODE` to false to test
  OIDC" path keeps working with the gate on.

## Shared pieces adopted from the broker branch

- **`pkg/ginutil`** — auth-service's request-ID / access-log / CORS Gin
  middleware moves to a shared `pkg/ginutil` (exported `RequestID()`,
  `AccessLog()`, `CORS()`), consumed by both auth-service and portal-service
  instead of a copy-paste. The middleware tests move with it.
- **`pkg/oidc` `Claims.Account()`** — the account-derivation fallback chain
  (`preferred_username` → `name`, blank = reject) becomes a method on
  `pkg/oidc.Claims`, used by both services so they can never disagree about
  which account a token belongs to.

## chat-frontend changes

- **`runtimeConfig.js`**: add `PORTAL_URL` (`VITE_PORTAL_URL`, default
  `http://localhost:8081`). `AUTH_URL`, `NATS_URL`, `DEFAULT_SITE_ID` are no
  longer read by the connect path and are removed along with their config
  plumbing (`config.js.template`, `30-render-config.sh`, deploy compose,
  `setup.sh`'s `.env.local`) — portal is the single source of connection
  coordinates.
- **`NatsContext.connect(opts)`**: drops `siteId` from its signature. New
  sequence: portal lookup (`POST {PORTAL_URL}/lookup`, body `{ssoToken}` or
  `{account}` by mode) → `POST {resolved authServiceUrl}/auth` →
  `natsConnect({servers: resolved natsUrl})` → `setUser({...userInfo, siteId:
  resolved siteId})`. Portal errors throw the same `AsyncJobError` shape the
  auth fetch already throws (envelope-aware), so `sso_token_expired` /
  `invalid_sso_token` trigger the existing relogin redirect for free.
- **`useJwtRefresh`**: takes a `getAuthUrl()` getter (ref populated at
  connect time) instead of a static `authUrl`, so background re-mints hit the
  resolved site's auth-service. Refresh does **not** re-query portal — site
  assignment is stable within a session.
- **`LoginPage`**: Site ID input removed from both branches. Dev branch:
  account only. SSO branch: just the "Sign in with Keycloak" button. The
  `oidc.siteId` sessionStorage stash is deleted everywhere (LoginPage,
  OidcCallback, oidcClient cleanup).
- **`OidcCallback`**: `connect({ mode: 'sso', ssoToken })` — no siteId.
- **`REASON_COPY`** (`api/_transport/asyncJob.ts`) gains
  `account_not_provisioned: "Your account isn't set up for chat yet — contact
  your administrator."` so both the portal lookup refusal and an auth-service
  minting refusal render friendly copy via `formatAsyncJobError`.
- **`vite.config.js`**: drop the now-dead `/auth → localhost:8080` dev-server
  proxy — every credential call uses absolute URLs (portal, then the resolved
  `authServiceUrl`).

## docker-local / local dev

- Portal service joins `docker-local/compose.services.yaml` via its
  `deploy/docker-compose.yml`, using the shared `mongodb` container with DB
  `portal`.
- Seed (one-shot mongosh container in portal's deploy compose):
  `sites`: `{_id: "site-local", authServiceUrl: "http://localhost:8080",
  natsUrl: "ws://localhost:9222"}`; demo `users` reusing the
  `tools/seed-sample-data` fixture accounts (`alice`/E001, `bob`/E002 →
  `site-local`) so the portal directory agrees with the site data `make seed`
  creates. Unlisted accounts still work via the dev fallback.
- `setup.sh` writes `VITE_PORTAL_URL=http://localhost:8081` into
  `chat-frontend/.env.local` (and stops writing the retired vars for fresh
  setups).

## Documentation

`docs/client-api.md`: new §2 subsection "Site discovery — POST /lookup"
(request/response field tables, JSON example, error table per current doc
style); the §2 connection narrative gains the portal step; the §2.2
`POST /auth` error table gains the 403 `account_not_provisioned` row; the §6
reason catalog gains `account_not_provisioned` (emitted by portal-service and
auth-service). `chat-frontend/CLAUDE.md`'s reason catalog gains the same
entry.

## Testing (TDD, ≥80% coverage)

- **Backend unit (`handler_test.go`)** — table-driven, mocked store + fake
  `TokenValidator`: happy path (prod + dev); missing fields; invalid token;
  expired token; blank account claim; unprovisioned account (prod 403
  `account_not_provisioned`); dev fallback (unknown account → fallback site);
  dev fallback site missing → internal; site missing for known user →
  internal; store error → internal.
- **auth-service gate (`auth-service/handler_test.go`)** — provisioned
  account mints; unprovisioned → 403 `account_not_provisioned`; account
  provisioned on a **different** site → 403 (wrong-site refusal); store error
  → 500 (fail closed); gate skipped in dev mode and when
  `REQUIRE_PROVISIONED=false`. auth-service `integration_test.go` gains a
  Mongo-backed `ProvisionStore` test via `testutil.MongoDB` (including the
  compound account+siteId predicate).
- **`pkg/ginutil`** (moved middleware tests) and **`pkg/oidc`
  `Claims.Account()`** get their own unit tests.
- **Backend integration (`integration_test.go`)** — `testutil.MongoDB` +
  `TestMain(testutil.RunTests)`: find user by account (hit/miss), find site
  (hit/miss), `EnsureIndexes` idempotent + uniqueness enforced.
- **Frontend (vitest)** — NatsContext connect: portal lookup feeds auth fetch
  URL + NATS servers + user.siteId (both modes); portal error propagation
  (envelope → AsyncJobError, relogin reasons); LoginPage renders without Site
  ID field, dev submit passes account only; OidcCallback passes only the
  token; useJwtRefresh uses the resolved auth URL.

## Confirmed context

- Multiple NATS sites run in production; connection coordinates differ per
  site.
- A daily ops cron maintains directory data — the global account → site
  mapping and the per-site `users` collections. This spec relies on that sync
  but does not build it.

## Out of scope

- Populating the production portal directory and per-site `users` collections
  (the confirmed daily cron owns both; this PR only seeds docker-local).
- Multi-site docker-local topology (single `site-local` remains).
- Cross-site frontend redirects (`redirectTo`) — moot while one global
  frontend deployment serves all sites; revisit if per-site frontends arrive.
- Immediate revocation of an already-issued NATS JWT — bounded by JWT
  lifetime; deprovisioning locks out at the next refresh via the minting
  gate.
- Repointing the frontend smoke scripts (`smoke-test.mjs`,
  `scripts/*.smoke.mjs`) at the portal — they call auth-service directly with
  dev-mode bodies, and that contract is unchanged; a portal hop in
  `smoke:livestack` is a worthwhile follow-up.
