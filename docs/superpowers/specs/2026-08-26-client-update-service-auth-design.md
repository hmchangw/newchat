# Service-account auth on client-update-service + admin-service upload proxy — Design

**Date:** 2026-08-26
**Status:** Draft, pending review
**Services:** `client-update-service`, `admin-service`, new `pkg/svcjwt`

---

## 1. Problem

`client-update-service` distributes client software-update artifacts: a `.yaml`
descriptor plus an executable, stored in MinIO. Both of its `/api/v1` routes are
unauthenticated, which `docs/client-api.md` §12 records in a warning block:

> **These endpoints are UNAUTHENTICATED in v1.** Anyone who can reach the service
> can upload or download update artifacts.

The write side is the dangerous half. Anyone who can reach the service can
overwrite the executable that every desktop client subsequently downloads and
runs — a software-supply-chain hole that network restriction alone is a thin
defence for.

Separately, there is no way for an administrator to publish an update at all
except by calling that open endpoint by hand. `admin-frontend` has no upload
path, and `admin-service` — the service that fronts every other administrative
action — has no route for it.

## 2. Goals / Non-goals

**Goals**

- `POST /api/v1/version` accepts only an authorized service account.
- Authorization is carried by an Ed25519-signed JWT minted by `admin-service`
  and verified by `client-update-service`; the allowlist of permitted service
  accounts lives on `client-update-service`.
- An admin can mint a service-account token through an admin-only endpoint.
- `admin-service` exposes an upload endpoint that forwards the artifact pair to
  `POST /api/v1/version`, authenticating itself, with peak memory independent of
  file size.
- Every mint and every upload lands in the existing admin audit ledger.

**Non-goals**

- **`GET /api/v1/version/:fileName` stays unauthenticated.** Desktop update
  clients hold no credential and cannot obtain one; gating reads would break
  every deployed client. The read hole is recorded as follow-up in §10.
- No change to how artifacts are stored, named, cached, or served.
- No new third-party dependency. Ed25519 is `crypto/ed25519` in the standard
  library.
- No revocation list. Tokens are short-lived and bounded by `exp`; the
  allowlist on `client-update-service` is the revocation mechanism.
- **No `admin-frontend` UI in this change.** The endpoints and their
  documentation are the deliverable; the React upload page is separate
  follow-up work (§10). *This is an assumption — see §10 if it is wrong.*

## 3. Decisions already settled

| Decision | Choice | Rationale |
|---|---|---|
| Credential type | Ed25519 (EdDSA) JWT | Asymmetric: `admin-service` holds the private key, `client-update-service` only the public key, so a compromised `client-update-service` cannot forge tokens for itself. `crypto/ed25519` is stdlib, so no new dependency. |
| Who is authorized | Allowlisted service accounts | The JWT `sub` must appear in `client-update-service`'s `ALLOWED_SERVICE_ACCOUNTS`. |
| Auth scope | Upload only | Download stays open; see §2 non-goals. |
| Token source for the proxy | Minted in-process per request | `admin-service` already holds the private key. The browser never handles a JWT. |
| Allowlist ownership | `client-update-service` only | `admin-service` will mint a token for any requested subject; only `client-update-service` decides what a subject may do. One source of truth, no drift between two services. |

## 4. Architecture — `pkg/svcjwt`

A new shared package owning service-to-service token minting and verification.
It does not go in `pkg/sessiontoken` (opaque session tokens, a different
credential with a different lifecycle) nor `pkg/ginutil` (generic transport
middleware with no domain or crypto dependencies).

```go
package svcjwt

// Claims is the service-account token payload. Registered JWT names only.
type Claims struct {
    Issuer    string `json:"iss"`
    Subject   string `json:"sub"` // service account name
    Audience  string `json:"aud"` // target service, e.g. "client-update-service"
    IssuedAt  int64  `json:"iat"`
    ExpiresAt int64  `json:"exp"`
    JTI       string `json:"jti"`
}

type Signer struct{ /* ed25519.PrivateKey, issuer */ }

// NewSigner decodes a base64 raw 32-byte Ed25519 seed.
func NewSigner(seedB64, issuer string) (*Signer, error)

// Sign mints a token and returns it with its exp (unix seconds).
func (s *Signer) Sign(subject, audience string, ttl time.Duration) (string, int64, error)

type Verifier struct{ /* ed25519.PublicKey, issuer, audience, leeway */ }

// NewVerifier decodes a base64 raw 32-byte Ed25519 public key.
func NewVerifier(pubKeyB64, issuer, audience string) (*Verifier, error)

// Verify returns the claims, or a typed *errcode.Error (Unauthenticated) for
// any invalid token, and a raw wrapped error only for internal failures.
func (v *Verifier) Verify(token string) (*Claims, error)
```

### 4.1 Token shape

Header is a fixed constant, never negotiated:

```json
{"alg":"EdDSA","typ":"JWT"}
```

### 4.2 Verification rules

These are the security core of the change; §8 lists a test for each.

1. The token must be exactly three `.`-separated base64url (unpadded) segments.
2. The header is decoded and `alg` must equal `EdDSA`. **There is no algorithm
   lookup table.** Any other value — including `none` and `HS256` — is
   rejected outright, so algorithm-confusion and the `alg: none` attack are
   structurally impossible rather than defended against.
3. The signature is verified over `header.payload` **before** the payload is
   unmarshalled, so no attacker-controlled JSON is parsed into claims until the
   bytes are proven authentic.
4. `iss` must equal the configured issuer; `aud` must equal the configured
   audience. Both are exact string comparisons.
5. `exp` must be in the future, allowing a 30s `leeway` for clock skew.
6. `iat` must not be further in the future than `leeway`.
7. Every failure returns the same `errcode.Unauthenticated("invalid token")`.
   The reason a token failed is never disclosed on the wire; `Classify` logs it
   once server-side.

### 4.3 Key handling

Keys travel as base64 of the raw 32-byte Ed25519 seed (private) and 32-byte
public key — no PEM or PKCS#8 parsing to get wrong. `NewSigner`/`NewVerifier`
reject a key of the wrong decoded length at startup, so a misconfigured key
fails fast rather than at the first request.

`tools/svcjwtkey` is a small `package main` that prints a fresh
`SVCJWT_PRIVATE_KEY` / `SVCJWT_PUBLIC_KEY` pair, so provisioning does not
depend on an ad-hoc script.

**The token string is never logged**, never placed in an `errcode` message, and
never wrapped into an `errcode` cause — per CLAUDE.md, a cause reaches the
server log.

## 5. client-update-service

### 5.1 New `auth.go`

```go
func requireServiceAccount(v *svcjwt.Verifier, allowed []string) gin.HandlerFunc
```

Reads `Authorization: Bearer <jwt>`, verifies it, then checks `sub` against the
allowlist. On success it sets the resolved account on the Gin context for the
access log.

| Failure | Status | Reason |
|---|---|---|
| Header missing, wrong scheme, malformed, bad signature, wrong `iss`/`aud`, expired | `401` | `invalid_token` |
| Valid token whose `sub` is not in `ALLOWED_SERVICE_ACCOUNTS` | `403` | `not_authorized` |

The two cases are deliberately distinguished. Unlike a password or an opaque
session token, a JWT cannot be guessed — forging one requires the private key —
so separating "your token is bad" from "your account is not permitted" leaks
nothing exploitable and turns a misconfigured allowlist into an immediately
diagnosable `403` instead of a mystery `401`.

New `pkg/errcode/codes_clientupdate.go`:

```go
const (
    ClientUpdateInvalidToken  Reason = "invalid_token"   // 401
    ClientUpdateNotAuthorized Reason = "not_authorized"  // 403: valid token, sub not allowlisted
)
```

### 5.2 `routes.go`

```go
api := r.Group("/api/v1")
api.POST("/version", requireServiceAccount(verifier, cfg.AllowedServiceAccounts), h.HandleUpload)
api.GET("/version/:fileName", h.HandleDownload)   // unchanged, still open
```

`/healthz` is untouched.

### 5.3 Config additions

| Env var | Default | Notes |
|---|---|---|
| `SVCJWT_PUBLIC_KEY` | *required* | base64 raw 32-byte Ed25519 public key |
| `SVCJWT_ISSUER` | `admin-service` | must match admin-service's `SVCJWT_ISSUER` |
| `SVCJWT_AUDIENCE` | `client-update-service` | must match admin-service's `CLIENT_UPDATE_AUDIENCE` |
| `ALLOWED_SERVICE_ACCOUNTS` | *required* | comma-separated; empty is rejected at startup |

`ALLOWED_SERVICE_ACCOUNTS` is required rather than defaulted: an empty
allowlist would silently refuse every upload, and a permissive default would
silently reopen the hole this change closes.

### 5.4 Access log

`accessLogMiddleware` gains a `service_account` field, populated from the Gin
context on the upload route and empty elsewhere. The token itself is never
logged.

### 5.5 Pre-existing bug fixed in the same change

`main.go` sets `WriteTimeout: cfg.HTTPWriteTimeout` (configurable, default
`10m`) but hardcodes `ReadTimeout: 30 * time.Second`. In `net/http`,
`ReadTimeout` covers reading the **request body**, so any upload whose body
takes more than 30 seconds to arrive is already killed today — the 10-minute
write timeout never gets the chance to matter. The service's own design intent
("No size cap — streaming keeps memory bounded regardless of size") is
therefore not achievable as configured.

This is squarely in the code the change touches, so it is fixed here rather
than left as a trap the new upload path walks into:

| Env var | Default | Notes |
|---|---|---|
| `HTTP_READ_TIMEOUT` | `10m` | matches `HTTP_WRITE_TIMEOUT`'s default; covers the full upload body read |

## 6. admin-service

Both new routes join the existing `/v1/admin` group, so `requireAdmin` already
gates them — no middleware changes.

```go
cu := admin.Group("/client-update")
cu.POST("/token", h.issueServiceToken)
cu.POST("/version", extendDeadlines(cfg.ClientUpdateUploadTimeout), h.uploadClientVersion)
```

### 6.1 `POST /v1/admin/client-update/token`

The admin-only token generator.

**Request**

| Field | Type | Required | Notes |
|---|---|---|---|
| `serviceAccount` | string | yes | becomes the JWT `sub` |
| `ttlSeconds` | int | no | default `CLIENT_UPDATE_TOKEN_TTL` (1h); capped at `CLIENT_UPDATE_TOKEN_MAX_TTL` (24h) |

**Response** `200`

| Field | Type | Notes |
|---|---|---|
| `token` | string | the signed JWT |
| `serviceAccount` | string | echoes `sub` |
| `expiresAt` | int64 | unix seconds |

`400` for an empty `serviceAccount` or a non-positive/over-cap `ttlSeconds`.

`admin-service` mints a token for **any** requested subject. Authorization
lives entirely in `client-update-service`'s allowlist (§3), so a token for an
unlisted subject is signed but useless — and there is exactly one place to
change when the set of permitted accounts changes.

### 6.2 `POST /v1/admin/client-update/version`

Accepts the same `multipart/form-data` pair the downstream expects
(`configFile`, `executeFile`), mints a short-lived JWT in-process for
`CLIENT_UPDATE_SERVICE_ACCOUNT` with `SVCJWT_TTL` (5m), and forwards to
`POST {CLIENT_UPDATE_BASE_URL}/api/v1/version`. The browser never sees a JWT.

Downstream status mapping:

| Downstream | admin-service returns | Rationale |
|---|---|---|
| `200` | `200 {"result":"success"}` | |
| `400` | `400`, message relayed | The admin's file really was invalid. |
| `401` / `403` | `503`, reason `upstream_unauthorized` | This is a *configuration* fault — a key mismatch or a missing allowlist entry — not the admin's credential failing. Relaying `401` would tell an authenticated admin their own session was rejected, sending them to debug the wrong thing entirely. |
| transport error, `5xx` | `503`, reason `upstream_unavailable` | |

### 6.3 Streaming the forward (`forwarder.go`)

Gin's `c.FormFile` buffers to memory up to `MaxMultipartMemory` and spills the
rest to local disk — unacceptable for a large `.exe` in a pod with a fixed disk
allowance.

Instead the handler reads the inbound body with `r.MultipartReader()` and
re-encodes each part directly into the outbound request through an `io.Pipe`.
Nothing is buffered whole and nothing touches disk; peak memory is one copy
buffer regardless of artifact size. Parts are streamed in the order they
arrive, so the field names are known for the audit entry (§6.5) without
materialising the files.

The outbound call uses a raw `*http.Client` obtained from
`restyutil.New(...).GetClient()`. This is the exception `pkg/drive/uploader.go`
already documents and justifies: Resty v2 materialises any `io.Reader` body it
cannot natively replay (`createHTTPRequest` → `getBodyCopy` → `io.ReadAll`),
which is the precise OOM this path exists to avoid. Going through `restyutil`
anyway preserves the shared transport, OTel instrumentation, and timeout.

Because the body is piped, it carries no `Content-Length` and is sent with
chunked encoding. `client-update-service` reads it with `c.FormFile`, which
handles chunked bodies normally, so this changes nothing downstream.

**Rejected alternative.** Forwarding `c.Request.Body` verbatim with the
original `Content-Type` header is ~20 lines and equally memory-safe, but
`admin-service` then cannot name the uploaded files in the audit ledger or
reject a malformed submission before spending the upload. The audit trail is
the reason this endpoint exists inside `admin-service` at all, so the extra
~80 lines are bought deliberately.

### 6.4 Timeouts

`admin-service` runs `ReadTimeout: 15s` and `WriteTimeout: 40s`
(`httpWriteTimeout`), and `checkHandlerTimeout` enforces that every handler
budget stays below the latter. A large `.exe` will not upload in 15 seconds.

The server-wide values are **not** raised. `config.go` and
`applyBaseMiddleware` both carry comments explaining that the cross-site
permission fanout is sized against them, and that a shorter router timeout
would silently truncate multi-site permission changes. Widening them to suit an
upload would put that reasoning at risk for every other route.

Instead, `extendDeadlines(d)` is applied to the upload route alone. It uses
`http.NewResponseController` (Go 1.20+; the repo is on Go 1.25) to push that
one request's read and write deadlines out to
`CLIENT_UPDATE_UPLOAD_TIMEOUT`. Every other route keeps today's behaviour
exactly.

### 6.5 Audit

Both endpoints append through the existing `h.audit` helper.

| Action | Details |
|---|---|
| `client_update.token_issued` | `serviceAccount`, `expiresAt` |
| `client_update.upload` | `configFile`, `executeFile` (names only) |

`AuditEntry.Details` holds non-secret context only, per its own doc comment: the
token is never recorded.

### 6.6 Config additions

| Env var | Default | Notes |
|---|---|---|
| `SVCJWT_PRIVATE_KEY` | *required* | base64 raw 32-byte Ed25519 seed |
| `SVCJWT_ISSUER` | `admin-service` | |
| `SVCJWT_TTL` | `5m` | TTL of the internally minted forwarding token |
| `CLIENT_UPDATE_BASE_URL` | *required* | e.g. `http://client-update-service:8080` |
| `CLIENT_UPDATE_AUDIENCE` | `client-update-service` | JWT `aud` |
| `CLIENT_UPDATE_SERVICE_ACCOUNT` | *required* | `sub` admin-service mints for itself |
| `CLIENT_UPDATE_TOKEN_TTL` | `1h` | default TTL for admin-issued tokens |
| `CLIENT_UPDATE_TOKEN_MAX_TTL` | `24h` | hard cap on `ttlSeconds` |
| `CLIENT_UPDATE_UPLOAD_TIMEOUT` | `10m` | per-request deadline on the upload route |

Secrets are marked `required` and never defaulted, per CLAUDE.md. Note that
`CLIENT_UPDATE_UPLOAD_TIMEOUT` is deliberately **not** passed through
`checkHandlerTimeout`: that guard exists to keep handler budgets under the
40s server write timeout, and this route escapes that timeout by design (§6.4).

### 6.7 New error reasons

`pkg/errcode/codes_admin.go` gains the two reasons §6.2 maps upstream failures
onto, alongside the existing admin reasons:

```go
AdminUpstreamUnauthorized Reason = "upstream_unauthorized" // 503: client-update-service rejected our JWT
AdminUpstreamUnavailable  Reason = "upstream_unavailable"  // 503: client-update-service unreachable or 5xx
```

Both are carried on an `errcode.Unavailable`, which `Code.HTTPStatus` maps to
`503`. They are distinct reasons because they demand different remedies: the
first is a key, issuer, audience, or allowlist mismatch that an operator must
correct, the second is a transient outage worth retrying.

## 7. Data flow

```
admin-frontend
   │  POST /v1/admin/client-update/version   (Authorization: Bearer <admin session>)
   ▼
admin-service ── requireAdmin ── extendDeadlines(10m)
   │  svcjwt.Sign(sub=CLIENT_UPDATE_SERVICE_ACCOUNT, aud=client-update-service, ttl=5m)
   │  MultipartReader ──► io.Pipe ──► streamed re-encode
   │  POST /api/v1/version  (Authorization: Bearer <jwt>)
   ▼
client-update-service ── requireServiceAccount ── svcjwt.Verify + allowlist
   │
   ▼
MinIO
```

The out-of-band path is the same minus the proxy: an admin calls
`POST /v1/admin/client-update/token`, receives a JWT, and presents it directly
to `client-update-service`.

## 8. Testing

TDD throughout — Red, Green, Refactor, per CLAUDE.md §4. Minimum 80% coverage;
90%+ on `pkg/svcjwt`, which is security-critical shared code.

**`pkg/svcjwt/svcjwt_test.go`** — table-driven, one case per rule in §4.2:

- round trip: sign then verify yields the original claims
- tampered payload; tampered signature; signature from a different key
- `alg` rewritten to `none`; `alg` rewritten to `HS256`; `alg` absent
- wrong segment count (2 and 4); non-base64 segments; payload that is not JSON
- wrong `iss`; wrong `aud`
- expired; expired but inside leeway; `iat` in the future beyond leeway
- `NewSigner`/`NewVerifier` reject a wrong-length or non-base64 key
- every failure returns an `errcode` of category `Unauthenticated`, and no
  failure message contains the token

**`client-update-service/auth_test.go`** — table-driven middleware tests:
missing header, `Basic` scheme, garbage token, expired token, valid token with
an unlisted `sub` (403), valid allowlisted token (passes through). Plus an
explicit test that `GET /api/v1/version/:fileName` still succeeds with **no**
`Authorization` header — the non-goal in §2 is a guarantee, so it gets a
regression test.

**`admin-service/clientupdate_test.go`** — against an `httptest` upstream:
token endpoint happy path, empty `serviceAccount`, over-cap `ttlSeconds`;
upload happy path with the audit entry asserted, missing part, upstream `400`
relayed, upstream `401` mapped to `503`, upstream transport failure mapped to
`503`. `requireAdmin` rejection is already covered by `middleware_test.go`.

**Integration** — `client-update-service/integration_test.go` gains an
authenticated upload against the real MinIO container, plus a rejected
unauthenticated upload.

`make generate` is not required: no store interface changes.

## 9. Documentation

- `docs/client-api.md` §12: the upload gains an **Auth** row and its `401`/`403`
  cases. The blanket "UNAUTHENTICATED in v1" warning is narrowed to the
  download only, since the write hole is now closed.
- `docs/client-api.md` §9 (Admin Service): both new endpoints, with request and
  response field tables and JSON examples, per the house style.
- Neither `docs/client-api/request-reply.md` nor `docs/client-api/events.md`
  changes — both are derived views of the NATS `chat.user.` surface, and this
  change adds no NATS subject and touches no `pkg/model` wire struct.
- `client-update-service/deploy/docker-compose.yml` and
  `admin-service/deploy/docker-compose.yml` gain the new env vars with a
  committed **dev-only** keypair, clearly labelled as such.

## 10. Follow-up work (explicitly out of scope)

1. **`admin-frontend` upload page.** This design ships the endpoints and their
   documentation only. If the React page is wanted in the same change, this
   assumption is the thing to correct before implementation starts.
2. **The download hole.** `GET /api/v1/version/:fileName` stays open and keeps
   its warning block. Gating it needs a credential a deployed desktop client
   can actually present — a shipped download token or signed URLs — and cannot
   be done without updating those clients in lockstep.
3. **Key rotation.** There is one active keypair. Supporting overlap would mean
   a `kid` header and a set of verifier keys; not needed for a single
   first-party caller, and adding it later is backward-compatible because
   unknown header fields are already tolerated.
