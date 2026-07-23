# user-service SSO Token Endpoints — Design

**Date:** 2026-07-20
**Status:** Approved (pending implementation plan) — revised after 3-lens review (repo conventions / user-service patterns / auth-service OIDC)
**Owner:** user-service
**Related:** `docs/superpowers/specs/2026-05-03-global-search-and-keycloak-oidc-design.md` (Keycloak/OIDC introduction), `docs/superpowers/specs/2026-07-01-oplog-direct-transfer-design.md` (migration of the legacy token collection)

## 0. Reference — original request (verbatim)

> No No No, the value of this endpoints ifs diffferent, the thing is we dont want to put the mongo database anywhere in the auth-service and also the auth service is tcp api not nats core right. ok so about the 2 endpoints:
>
> 1st one: POST (core nats req repo i mean you understand)
> sth like: chat.user.{acct}.request.user.{siteID}.sso.set
> Request {
> ssoToken: string,
> refreshToken: string,
> }
> response{
> success: true
> }
> What this endpoint will do is it will just receive this from user, will check if user is an admin, then will verify the token and save it to the db. TOken verification needs to be double checked  with auth service, we totally prefer to use the oicd package just like wuth service uses. Now about hte db schema. We need wo strictly follow the current schema ( for backward compatibility will liik like:
> sso_token {
> _is: string,
> username: string, (we do use account almost everywhere but we already have this collection as as a backward compatibility we will use this one)
> idToken: string ( the actual sso token itself)
> idTokenExp: (some nnumber idk what type it is)
> refreshToken: string ( the actual refresh token)
> _updatedAt: standard for the repo
> }
>
> ok now about the second endpoint
> 2nd get
> chat.user.{acct}.request.user.{siteID}sso.refresh
> no request just the acct from route is enough
> response: {
> ssoToken: string
> }
> This endpoint will be used for retreiving the token from the system and if the token will be expired we will refresh it using the saved refresh token.
> This endpoint will first retrieve user by username then (in the old system it was being checked by metheor framework if user is calling the endpoint on himself or is he has admin priviliges. But in the new system im not sure how to handle that becase in the system we dont use framework. in old system it was isAdmin || user._id === headers['x-user-id'}. After the checks we get by username from the sso_tokens coll then check the ifTokenExp from db and jwt decoded the idToken ( we probably should add idTokenExp as a param in the POST endpoint) if it is in the range of one hour to being expired (if 1hour to expiry it triggers the token refresh operation using the issuer from the decoded token and the refreshToken from the db, then it saves the idToken, idTokenExp and refreshToken to the db. Ath the end (in both cases) it returns the ssoToken (old one also returned the idTokenExp and issuer, I think we should still return both the idToken and idTokenExp but we dont need to return the issuerr why would someone need this). Now please save this exact prompt at the top of the spec as a reference. Now you will research the sso handling in the codebase auth-servce also check the user-service. then please use brainstorming. We will be creating the spec. You will ask questions about anything unclear from my prompt. Also check the config values needed check if we need anything extra compared to auth service sso values. put a lot of work into this thanks

## 0.1 Decisions log (clarifications on top of the reference prompt)

| Topic | Decision |
|---|---|
| Collection | `sso_tokens` — a dedicated collection in the new stack. Legacy field names are kept (`username`, `idToken`, `idTokenExp`, `refreshToken`, `_updatedAt`); the legacy token docs copied verbatim by `oplog-direct-transfer` must be loaded into `sso_tokens` (destination remap is an ops prerequisite). |
| `sso.set` authz | **Admin-only always**, even when storing the caller's own tokens. |
| On-behalf-of | `sso.set` request gets an **optional `account` field** (new-stack naming; maps to legacy `username` in the DB) so an admin can store tokens for another user. |
| Expiry source | **Derived server-side** from the verified token's `exp` claim (`pkg/oidc.Claims` gains `Expiry`). No expiry request param, no trust in client-supplied expiry. No change to issued tokens or to auth-service behavior. |
| `sso.refresh` request/response | Optional `account` param (self + admin-for-others, mirroring the old `isAdmin \|\| self` rule). Response is **`{ssoToken}` only** — no expiry, no issuer. |
| Refresh issuer | **Configured `OIDC_ISSUER_URL`** (discovered `token_endpoint`), not the `iss` claim of the stored token. |
| Refresh OIDC client | **Public client** (`nats-chat`-style): new `OIDC_CLIENT_ID` config, no client secret. |
| Refresh failure | **`unauthenticated` / `sso_token_expired`** for any refresh failure (rejected grant or Keycloak unreachable) — drives the frontend's existing re-login redirect. Never return a dead token. See §8 note on the recorded alternative. |
| Refresh window | **`SSO_REFRESH_WINDOW` env, default `1h`.** |
| Refresh-client location | **Option A** — extend the shared `pkg/oidc` (validator already owns issuer discovery); no service-local Keycloak client, no new package. |

### 0.2 Post-review decisions (from the 3-lens spec review)

| Topic | Decision |
|---|---|
| **Vault token type** | The vault stores the platform's **`ssoToken` = the Keycloak access token** — the same value the frontend sends to auth-service/upload-service today (`OidcCallback.jsx` sends `user.access_token`). The legacy DB field name `idToken` is kept for backward compatibility, but its value is the access token. On refresh, the **`access_token`** field of the token response is verified and stored (not `id_token`), so the vault's token type never drifts. Both existing validators already accept access tokens (aud allow-list + `SkipClientIDCheck`; the audience mapper stamps `nats-chat` into access tokens). |
| Empty `sso.refresh` payload | `natsrouter.Register` rejects zero-length payloads (unmarshal runs before the handler). Add a small shared variant **`natsrouter.RegisterOptionalBody[Req, Resp]`** that treats an empty payload as the zero-value request, used by `sso.refresh` — preserves the "no body needed" contract. |
| Deactivated users | Caller and target lookups go through the existing `activeUserFilter` idiom (`active: {$ne:false}`). A deactivated caller cannot pass the admin gate; a deactivated target ⇒ `not_found`. |
| Store row type | Lives in **`pkg/model/ssotoken.go`** (NOT exported from `mongorepo`) so the consumer-side interface exchanges only `pkg/model` types, per the existing boundary rule. Secrets carry `json:"-"` (precedent: `model.PasswordCredentials`). |
| Token endpoint retrieval | `NewValidator` retains the discovered `token_endpoint` via `provider.Claims(&struct{ TokenEndpoint string }{})` — avoids promoting `golang.org/x/oauth2` to a direct dependency. |
| Refresh HTTP client | `v.httpClient` when non-nil (TLS-skip path); otherwise a package-default `*http.Client` with an explicit timeout (`refreshTimeout = 10s`, matching `issuerDiscoveryTimeout`). Never the nil/`http.DefaultClient` (no-timeout) path. |
| OIDC config optionality | The OIDC block is **optional as a unit**: `OIDC_ISSUER_URL` unset ⇒ validator not constructed, both SSO handlers still registered but reply `unavailable`/`upstream_unavailable` (precedent: auth-service's unset `BOTPLATFORM_URL` branch). Set ⇒ fail-fast init like auth-service/upload-service, and `OIDC_AUDIENCES` + `OIDC_CLIENT_ID` become required (validated in `config.Load`). Keeps the other 17 handlers bootable without Keycloak (user-service has no `DEV_MODE`). |
| Forbidden reason | **Reason-less `errcode.Forbidden`** via package-level sentinels (precedent: room-service `helper.go` owner checks). `errcode.AdminNotAuthorized` stays admin-service-session-scoped. |
| Naming | All-caps initialism per repo style: `UserSSOSet`/`UserSSOSetPattern`/`UserSSORefresh`/`UserSSORefreshPattern`, `SSOTokenRepository`, `UserSSOTokenNotFound`. |
| Set response DTO | Reuse the existing **`models.OKResponse`** (`{"success": true}`) instead of a new `SsoSetResponse`. |

## 1. Purpose & placement

user-service becomes the SSO-token vault. It stores each user's Keycloak token pair (`ssoToken` = access token + refresh token) in Mongo (`sso_tokens`) and serves the `ssoToken` back on request, transparently refreshing near-expired tokens against Keycloak using the stored refresh token.

Placement rationale:
- **auth-service stays DB-free.** It is an HTTP (Gin) service that validates SSO tokens and mints NATS JWTs; it has no store and gains none. It is untouched by this feature (the `pkg/oidc` change is additive; auth-service never reads the new fields).
- user-service already owns the `users` collection, the NATS request/reply surface (`chat.user.{account}.request.user.{siteID}.…`), and the sanctioned sub-package layout — these endpoints are ordinary additions to it.
- All OIDC mechanics (verify + refresh) live in the shared `pkg/oidc`, the same package auth-service uses, so token handling never forks between services.

Scope: site-local only. No cross-site federation of tokens, no INBOX/OUTBOX/stream involvement.

## 2. API surface

Two new NATS request/reply endpoints, registered via `natsrouter` in `service.RegisterHandlers`, queue group `user-service`. New subject builder pairs in `pkg/subject` (new `sso` area — `ParseUserSubject`'s area whitelist is NOT extended: it has zero production callers, and the `thread`/`me` areas already ship without whitelist entries; the scoped-signing-key template's `--allow-pub "chat.user.{{tag(account)}}.>"` already covers the new subjects, so no permission-template change):

```go
// Concrete builders (client side) — panic on wildcard account via isValidAccountToken:
UserSSOSet(account, siteID)      // chat.user.<account>.request.user.<siteID>.sso.set
UserSSORefresh(account, siteID)  // chat.user.<account>.request.user.<siteID>.sso.refresh
// Pattern builders (server side) — {account} router placeholder, siteID baked as a literal:
UserSSOSetPattern(siteID)        // chat.user.{account}.request.user.<literal siteID>.sso.set
UserSSORefreshPattern(siteID)    // chat.user.{account}.request.user.<literal siteID>.sso.refresh
```

### 2.1 `sso.set`

```text
Request:  { "ssoToken": string, "refreshToken": string, "account": string (optional) }
Response: { "success": true }        // models.OKResponse (existing DTO)
```

- Registered with `natsrouter.Register`. Request DTO `SSOSetRequest` in `user-service/models/sso.go`.
- `ssoToken` and `refreshToken` are required → missing either ⇒ `bad_request` / `missing_fields`.
- `account` (optional) targets another user; absent ⇒ target is the caller. Must pass `subject.IsValidAccountToken` ⇒ else `bad_request`.
- Flow: admin check (§3) → target user exists & active (§3) → verify `ssoToken` via `pkg/oidc` (expired ⇒ `unauthenticated`/`sso_token_expired`; otherwise-invalid ⇒ `unauthenticated`/`invalid_sso_token`) → token-owner integrity check (§3) → upsert doc (§4) → `{success: true}`.

### 2.2 `sso.refresh`

```text
Request:  { "account": string (optional) }    — absent/empty body ⇒ self
Response: { "ssoToken": string }
```

- Registered with the **new `natsrouter.RegisterOptionalBody`** (§0.2): a zero-length payload is treated as the zero-value request (self-service), a non-empty payload is unmarshalled normally. Request DTO `SSORefreshRequest`, response `SSORefreshResponse` in `user-service/models/sso.go`.
- Flow: authz (§3) → target user exists & active → load doc by `username` (missing ⇒ `not_found`/`sso_token_not_found`) → freshness check (§5) → return stored or refreshed `ssoToken`.

## 3. Authorization & integrity

- **Caller identity** is the NATS-JWT-pinned `{account}` token of the subject (`c.Param("account")`). Nothing is read from headers — the old Meteor `isAdmin || user._id === headers['x-user-id']` check is replaced by subject pinning plus a role check.
- **Admin check**: fetch the CALLER's user record via a new `UserRepository` method (e.g. `GetUserRoles(ctx, account)`) using the existing `activeUserFilter` and an explicit projection `{"_id": 0, "account": 1, "roles": 1}` (sibling projections exclude `_id`); check `model.IsPlatformAdmin`. Applied:
  - `sso.set`: always. Non-admin (or caller not found/deactivated) ⇒ `forbidden`.
  - `sso.refresh`: only when `account` is present and differs from the caller. Non-admin ⇒ `forbidden`.
- **Target existence**: the target user (caller or `account`) must exist and be active (`activeUserFilter`) ⇒ else `not_found`.
- **Token-owner integrity (new behavior vs old system, deliberate)**: on `sso.set`, the verified token's `preferred_username` (`Claims.Account()`) must equal the target account, so an admin cannot file user X's token under user Y ⇒ mismatch ⇒ `bad_request`.

## 4. Data model — `sso_tokens`

Domain row type in **`pkg/model/ssotoken.go`** (shared-model boundary rule: consumer interfaces exchange `pkg/model`/`models` types only; mongorepo doc types stay unexported). Both `json` and `bson` tags per CLAUDE.md, with `json:"-"` on secret fields (precedent: `model.PasswordCredentials`) and a redacting `String()` (precedent: `model.User`) so token material can never reach a log line:

```go
type SSOToken struct {
    ID           string    `json:"id"          bson:"_id"`
    Username     string    `json:"username"    bson:"username"`
    IDToken      string    `json:"-"           bson:"idToken"`      // value = platform ssoToken (access token); legacy field name
    IDTokenExp   int64     `json:"idTokenExp"  bson:"idTokenExp"`   // epoch millis
    RefreshToken string    `json:"-"           bson:"refreshToken"`
    UpdatedAt    time.Time `json:"updatedAt"   bson:"_updatedAt"`   // legacy Meteor field name
}
```

| Field | Notes |
|---|---|
| `_id` | Existing docs keep their legacy ids (migrated verbatim). New docs: `idgen.GenerateID()` — same 17-char length as legacy ids (base62 superset of the legacy alphabet; harmless), set via `$setOnInsert`. The CLAUDE.md per-entity `_id` table gains a row for this entity (docs step, §9). |
| `username` | The target account (legacy field name kept). Unique index, created in `EnsureIndexes`. |
| `idToken` | The platform `ssoToken` (access token — §0.2). |
| `idTokenExp` | Stored as a decimal-millis **string** (legacy schema); repo converts to `int64` millis on read (non-numeric ⇒ 0 ⇒ refresh); new writes are `strconv.FormatInt(exp.UnixMilli())`. |
| `refreshToken` | The refresh token. Migrated rows may hold foreign-issuer refresh tokens; refreshing those yields `invalid_grant` ⇒ re-login (self-healing, accepted). |
| `_updatedAt` | Set on every upsert (legacy Meteor field name kept). |

Writes are **upserts keyed on `username`** — one document per user, last-write-wins.

New repo `user-service/mongorepo/ssotokens.go` (`SSOTokenRepo`, constructor `NewSSOTokenRepo(db)`, sibling idioms throughout):
- `GetByUsername(ctx, username) (*model.SSOToken, error)` — explicit projection (`username`, `idToken`, `idTokenExp`, `refreshToken`; `"_id": 0`); `mongo.ErrNoDocuments` ⇒ `(nil, nil)`.
- `Upsert(ctx, username, ssoToken string, ssoTokenExpMs int64, refreshToken string) error` — `$set` fields + `_updatedAt`, `$setOnInsert` `_id`.
- `EnsureIndexes(ctx)` — unique index on `username`; wired in `main.go` startup like the existing repos.
- Integration-test helper `newTestSSOTokenRepo` in `mongorepo/setup_test.go` per the `newTestUserRepo` idiom.

Service-side interface (consumer-defined, in `user-service/service/service.go`):

```go
type SSOTokenRepository interface {
    GetByUsername(ctx context.Context, username string) (*model.SSOToken, error)
    Upsert(ctx context.Context, username, ssoToken string, ssoTokenExpMs int64, refreshToken string) error
}
```

added to the `//go:generate mockgen` list and mocked in `service/mocks/`. Interface conformance is enforced by the two existing compile-time assertion blocks — `user-service/main.go:24-34` (`var ( _ service.UserRepository = (*mongorepo.UserRepo)(nil) … )`) and `user-service/mongorepo/setup_test.go:17-21` (the `go vet -tags integration` guard). `SSOTokenRepository` is added to **both** blocks. Note the `service.New(...)` signature change ripples through `service_test.go`'s `newSvc` helper and every test constructing the service.

## 5. Refresh flow (`sso.refresh`)

1. Authz + target resolution (§3); load doc (missing ⇒ `not_found`/`sso_token_not_found`).
2. If `idTokenExp` is more than `SSO_REFRESH_WINDOW` (default 1h) in the future ⇒ return the stored `ssoToken` unchanged.
3. Otherwise (inside the window **or already expired**) ⇒ refresh: `pkg/oidc` POSTs `grant_type=refresh_token` with `client_id` (public client, no secret) to the **configured issuer's** discovered `token_endpoint`.
4. On success: the response's **`access_token`** (§0.2 — a missing `access_token` in the response is a distinct error, never verified-as-empty-string) is verified through the same validator (integrity + authoritative new `Expiry`), then the doc is upserted (`idToken` ⇐ new access token, `idTokenExp`, `refreshToken` — Keycloak may rotate it — and `_updatedAt`) and the new `ssoToken` is returned.
5. On ANY refresh failure (rejected grant `invalid_grant`, HTTP 4xx/5xx, Keycloak unreachable) ⇒ `unauthenticated` / `sso_token_expired`. The frontend already redirects to re-login on that reason. A dead or stale token is never returned. (§8 records the reviewed alternative.)

Concurrency: two concurrent refreshes for the same user may race; with Keycloak refresh-token rotation one may lose. Accepted — last-write-wins upsert; the loser's client re-logins. Explicit non-goal to coordinate.

### 5.1 IdP prerequisites (ops note)

The vault is only as useful as the refresh token's lifetime. The **dev** realm export sets no session-lifespan overrides and no `offline_access` scope, so Keycloak defaults apply (≈5-min access tokens, 30-min SSO idle): stored refresh tokens die after ~30 idle minutes and the 1h window means step 2's short-circuit rarely fires in dev. That degrades safely (`sso_token_expired` ⇒ re-login), but for production usefulness the realm must provide longer SSO session idle/max (or `offline_access`) — an ops/realm-config prerequisite, out of scope for this feature.

## 6. `pkg/oidc` changes (additive only — auth-service untouched)

- `Claims` gains `Expiry time.Time`, copied from the already-parsed `idToken.Expiry` in `Validate`. No behavior change for existing callers.
- `Config` gains `ClientID string` — used only by refresh; validators constructed without it behave exactly as today.
- `NewValidator` additionally retains the issuer's `token_endpoint`, read via `provider.Claims` into a small struct with a `token_endpoint` JSON tag — deliberately NOT `provider.Endpoint()`, which would promote `golang.org/x/oauth2` to a direct dependency (CLAUDE.md: ask before adding deps).
- New method on `*Validator`:

```go
type TokenSet struct {
    SSOToken     string    // the token response's access_token (verified)
    RefreshToken string
    Expiry       time.Time // exp of the new access token (from verification)
}

func (v *Validator) Refresh(ctx context.Context, refreshToken string) (TokenSet, error)
```

  - POSTs `application/x-www-form-urlencoded` `grant_type=refresh_token&refresh_token=…&client_id=…` to the retained `token_endpoint`.
  - HTTP client: a `restyutil.New` Resty client built once in `NewValidator` — 10s timeout, otelhttp-instrumented, a 1 MiB response-body cap, and (only when `TLSSkipVerify`) an `InsecureSkipVerify` transport. This satisfies the repo's "Resty for outbound HTTP" rule. (go-oidc's own discovery/JWKS fetch stays on `net/http` — that's the library's transport, not ours.)
  - Verifies the returned `access_token` with the existing verifier before returning (fills `TokenSet.Expiry`); a response missing `access_token` is an error.
  - Sentinel `ErrRefreshRejected` for OAuth-level rejection (`invalid_grant` etc.) so callers can distinguish it from transport errors — user-service maps **both** to `sso_token_expired` per the decisions log, but the sentinel keeps the package honest for future callers.
  - Refresh calls or errors NEVER include token material in messages/causes (CLAUDE.md secret rule; the legacy token collection is already flagged "never log document contents" in the direct-transfer design).

## 7. Configuration (user-service) — delta vs auth-service

New fields in `user-service/config.Config`. **The OIDC block is optional as a unit** (§0.2): `OIDC_ISSUER_URL` unset ⇒ SSO endpoints reply `unavailable` (§8) and no validator is constructed; set ⇒ fail-fast startup init exactly like auth-service/upload-service, and `config.Load` cross-validates that `OIDC_AUDIENCES` and `OIDC_CLIENT_ID` are non-empty and `SSO_REFRESH_WINDOW > 0`.

| Env var | Default | Constraint | Purpose | In auth-service? |
|---|---|---|---|---|
| `OIDC_ISSUER_URL` | `""` (feature off) | — | Keycloak realm URL (discovery + JWKS + token endpoint) | ✅ same |
| `OIDC_AUDIENCES` | — | required when issuer set (comma-sep) | Audience allow-list for verification | ✅ same |
| `TLS_SKIP_VERIFY` | `false` | — | Dev-only TLS skip toward the issuer | ✅ same |
| `OIDC_CLIENT_ID` | — | required when issuer set | Public client id for the refresh grant | ❌ **new** (auth-service never calls the token endpoint) |
| `SSO_REFRESH_WINDOW` | `1h` | `> 0` | Refresh when the stored token is this close to (or past) expiry | ❌ **new** |

No client secret (public client — decisions log). `deploy/docker-compose.yml` gains the new envs pointing at the local Keycloak (`http://keycloak:8080/realms/chatapp`, client `nats-chat`).

Startup: when configured, `main.go` constructs one `pkg/oidc` validator (with `ClientID`) and injects it into the service behind consumer-defined interfaces:

```go
type TokenValidator interface { Validate(ctx context.Context, raw string) (oidc.Claims, error) }
type TokenRefresher interface { Refresh(ctx context.Context, refreshToken string) (oidc.TokenSet, error) }
```

both satisfied by `*oidc.Validator`, both in the mockgen list; nil injections (feature off) short-circuit the handlers to `unavailable`. This newly couples user-service *restarts* to Keycloak availability **only when the feature is enabled** — accepted: both existing `pkg/oidc` consumers fail fast, CLAUDE.md mandates fail-fast startup, and a Keycloak outage already blocks all logins platform-wide while connected clients keep working on their NATS JWTs.

## 8. Error handling

Tier-1 errcode usage; the router replies. Reasons (reused constants are auth-service's `Auth*` set in `codes_auth.go`):

| Case | Code | Reason |
|---|---|---|
| Missing `ssoToken`/`refreshToken` on set | `bad_request` | `missing_fields` (`AuthMissingFields`) |
| Invalid `account` value / token-owner mismatch | `bad_request` | — |
| Caller not admin (set always; refresh for others) | `forbidden` | — (reason-less package-level sentinel, room-service precedent; `AdminNotAuthorized` stays admin-service-scoped) |
| Target user missing or deactivated | `not_found` | — |
| No stored token doc on refresh | `not_found` | `sso_token_not_found` (**new** `UserSSOTokenNotFound` in `codes_user.go`) |
| Submitted token expired (set) | `unauthenticated` | `sso_token_expired` (`AuthTokenExpired`) |
| Submitted token invalid (set) | `unauthenticated` | `invalid_sso_token` (`AuthInvalidToken`) |
| Refresh grant fails / Keycloak unreachable (refresh) | `unauthenticated` | `sso_token_expired` — **deliberate deviation** from the auth-service precedent of `unavailable`/`upstream_unavailable` for upstream outages: the product decision (§0.1) is that any refresh failure sends the client to re-login. Recorded alternative: map transport errors to `unavailable` (retryable) via the `ErrRefreshRejected` sentinel — a one-line change if UX reconsiders. |
| SSO feature not configured (issuer unset) | `unavailable` | `upstream_unavailable` (`BotplatformUpstreamUnavailable`, auth-service `BOTPLATFORM_URL`-unset precedent) |
| Mongo/infra failure | `internal` (via raw wrapped error) | — |

New-reason checklist per `docs/error-handling.md` §5: constant in `codes_user.go`, added to `allReasons` in `pkg/errcode/codes_test.go`, documented in the client-api §6 reason catalog + endpoint error rows. The §6 catalog's "Emitted by" entries for `sso_token_expired`, `invalid_sso_token`, and `missing_fields` gain user-service as a new emitter.

Never log or wrap token material; log lines carry `account`, expiry timestamps, and request id only.

## 9. Testing

TDD (Red-Green-Refactor) per CLAUDE.md; 80% floor, **90%+ target** — handlers, store implementation, and `pkg/oidc` are all "core business logic" under the coverage rule.

- **`pkg/oidc` unit tests**: **build** a fake-issuer `httptest` harness (none exists today — current tests cover only `containsAudience`/`Account()`): discovery document, signed JWKS, token minting, and a `token_endpoint` — this is a materially larger test-infrastructure task than extending. Cases: `Validate` happy path + `Claims.Expiry` population, refresh success (new access token round-trip, rotated refresh token), refresh response missing `access_token`, `invalid_grant` ⇒ `ErrRefreshRejected`, 5xx ⇒ transport error, timeout behavior.
- **`pkg/natsrouter`**: tests for the new `RegisterOptionalBody` (empty payload ⇒ zero-value request; non-empty ⇒ unmarshal; malformed ⇒ `bad_request`).
- **`user-service/service/sso_test.go`**: table-driven with `newSvc(t)`-style mocks covering: set happy path (self + on-behalf-of), missing fields, non-admin caller, deactivated caller, invalid target account, target not found, expired token, invalid token, owner mismatch, store error, feature-off ⇒ `unavailable`; refresh happy path (fresh token returned unchanged), within-window ⇒ refresh + upsert + new token, already-expired ⇒ refresh, refresh rejected ⇒ `sso_token_expired`, no doc ⇒ `sso_token_not_found`, non-admin fetching other ⇒ forbidden, store errors ⇒ internal.
- **`user-service/mongorepo/ssotokens_test.go`** (`//go:build integration`, `testutil.MongoDB`): upsert-insert (generated 17-char `_id`, `_updatedAt` set), upsert-update (same `_id` kept, fields replaced), get-missing ⇒ `(nil, nil)`, unique-index behavior, projection completeness.
- **Docs (same PR)**: `docs/client-api.md` §3.4 user-service — both endpoints (subject, request/response field tables, JSON examples, error tables), routing table row, ToC, endpoint-count bump; §6 reason catalog updates (§8); mirrored into `docs/client-api/request-reply.md`; CLAUDE.md per-entity `_id` table gains the SSO-token row. No client events ⇒ `events.md` untouched.

## 10. Non-goals

- Cross-site federation of SSO tokens (site-local; no streams, no outbox).
- Token deletion / revocation endpoint.
- Encryption-at-rest of stored tokens — access AND refresh tokens are stored in plaintext, matching legacy behavior; Mongo access controls apply (explicitly accepted risk).
- Refresh-token rotation race coordination (last-write-wins accepted).
- Any auth-service behavior change (its `pkg/oidc` usage is unaffected by the additive fields).
- Returning `idTokenExp`/`issuer` from `sso.refresh` (old system did; deliberately dropped — response is `{ssoToken}` only).
- Keycloak realm/session-lifetime configuration (§5.1 prerequisite is an ops concern).
