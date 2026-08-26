# Authenticating client-update-service — Design

**Date:** 2026-08-26 · **Status:** Draft, pending review

---

## 1. Problem

`client-update-service` distributes desktop client software updates: a `.yaml`
descriptor plus an executable, stored in MinIO. Both of its endpoints are
completely unauthenticated. `docs/client-api.md` §12 says so in a warning block:

> **These endpoints are UNAUTHENTICATED in v1.** Anyone who can reach the service
> can upload or download update artifacts. **They MUST be network-restricted
> before any production exposure.**

The two halves of that hole are not equally severe:

- `GET /api/v1/version/:fileName` leaks the client binary to anyone who can reach
  the service. Bad, but bounded — the artifact is shipped to every employee anyway.
- `POST /api/v1/version` **overwrites the executable that every desktop client
  auto-downloads and runs.** An unauthenticated write here is remote code
  execution on every workstation in the org, one request deep. It is the highest-
  value write surface in the system.

That asymmetry drives the whole design: the two routes get different credentials,
sized to what they protect.

## 2. Goals / Non-goals

**Goals**

- `GET` requires a valid end-user SSO session, matching how `upload-service`
  authenticates human callers.
- `POST` requires an authorized **service account** credential — the uploader is a
  release pipeline, not a person at a keyboard.
- Replace the §12 warning block with a documented auth contract.
- No new dependency the service does not strictly need.

**Non-goals**

- No authorization beyond "is this caller allowed to upload at all". There are no
  per-artifact permissions and no notion of artifact ownership.
- No rate limiting, no idempotency keys. The upload path is one caller at release
  cadence; the download path is cached and read-only.
- No change to upload/download behaviour, streaming, caching, or the MinIO layout.
- No cross-site concerns. Artifacts are per-site; nothing federates.

## 3. Decisions settled

| Decision | Choice | Rationale |
|---|---|---|
| GET credential | `ssoToken` header or cookie, validated by `pkg/oidc` | Matches `upload-service`'s SSO branch, which is how every human-facing HTTP endpoint here authenticates. |
| GET does **not** accept session tokens | SSO only | `pkg/botauth` would add a hard dependency on botplatform for a download path that only humans use. Explicitly scoped out. |
| POST credential | Static service token in `X-Service-Token` | Chosen for a CI/release pipeline that cannot perform a login round-trip. See §3.1 — this was a deliberate trade against the recommended alternative. |
| Token comparison | SHA-256 both sides, then `subtle.ConstantTimeCompare` | See §5.2. |
| Multiple valid tokens | Yes, comma-separated | Rotation without a downtime window. See §5.3. |
| Mongo | Not used | No credential on either route reads the `sessions` collection, so the service keeps zero database dependencies beyond MinIO. |

### 3.1 The POST credential trade, recorded honestly

Three options were weighed for the service-account credential.

**Rejected — gate on the `admin` role.** The repo's existing pattern
(`admin-service`, `media-service`) checks `UserRoleAdmin`. Wrong tool here: `admin`
is held by every administrator in the org, so it would authorize every one of them
to push executables to every workstation. Far too broad for this blast radius.

**Rejected — bot account with a named allowlist.** The recommended option. The repo
already has a service-account primitive: `model.HasLoginRole` lets exactly `admin`
and `bot` accounts password-login through botplatform, yielding a session token that
`pkg/botauth` validates. Gating on an `UPLOADER_ACCOUNTS` allowlist would have given
revocation (deactivate the account), per-caller attribution in the access log, and
zero new auth conventions. It was not chosen because it requires the pipeline to
obtain and store a session token rather than read one env var.

**Chosen — static service token.** Lowest friction for a pipeline: one secret in
config, no login call, no botplatform dependency on the write path. The costs are
real and are not hidden by this design:

| Cost | Mitigation in this design |
|---|---|
| No revocation short of a redeploy | Multi-token config (§5.3) makes rotation a config change with no downtime, not a code change. |
| No per-caller attribution — every upload looks identical in logs | None available. If two pipelines ever share the endpoint, the logs cannot tell them apart. Accepted. |
| Replayable by anyone who observes one request | TLS-only deployment, documented in §7 and in the code. |
| No precedent in this repo — first bespoke auth scheme | Contained to one middleware in one service; the reason code is namespaced to it. |

This is recorded so a future reader knows the alternative was considered and what
was traded away, not discovered later as an oversight.

## 4. GET — SSO session

`requireSSO` is `upload-service`'s `authMiddleware` with the session-token branch
and the Drive-specific parts removed.

```go
type authDeps struct {
    sso     TokenValidator // satisfied by *pkg/oidc.Validator
    devMode bool
}

func requireSSO(d authDeps) gin.HandlerFunc
```

Resolution order:

1. Token from the `ssoToken` **header**, falling back to the `ssoToken` **cookie**.
   The cookie fallback is carried over from `upload-service`, where `<img src>`
   downloads cannot set headers. A desktop updater can set a header, but keeping
   both means this endpoint behaves identically to every other download in the
   platform.
2. Empty → `401` `missing_fields`.
3. `DEV_MODE=true` → synthesize the account from the token, no OIDC call. Same
   escape hatch `upload-service` uses for local development.
4. Otherwise `pkg/oidc.Validator.Validate`:
   - `ErrTokenExpired` → `401` `sso_token_expired`
   - any other error → `401` `invalid_sso_token`
5. `claims.Account()` empty → `401` `invalid_sso_token`.

**One deliberate deviation from upload-service.** It falls back to `claims.Name`
when `preferred_username` is empty. `pkg/oidc`'s own `Claims.Account()` documents
the opposite rule — *"preferred_username — the only claim trusted as a principal;
name is user-editable display data. Empty means callers must reject the token"* —
so this service rejects instead. Following the package's stated contract beats
copying a call site that contradicts it.

**Identity stored.** Only the account, under a gin context key, for the access log.
There is no `Email`, `EngName`, or `ChineseName`: nothing downstream of a file
download reads directory metadata, so `parseDescription` is not ported.

**No site check.** The SSO path yields no `siteID` to check against `SITE_ID`;
requiring one would be impossible rather than merely strict.

## 5. POST — static service token

### 5.1 Header

`X-Service-Token`. A dedicated header rather than `Authorization: Bearer` so a
static shared secret can never be confused with the session and SSO conventions
used elsewhere in the platform — including on this service's own GET route.

### 5.2 Comparison

```go
func requireServiceToken(digests [][sha256.Size]byte) gin.HandlerFunc
```

1. Header missing or empty → `401`.
2. SHA-256 the presented token.
3. `subtle.ConstantTimeCompare` against every configured digest, OR-accumulating
   the results, **with no early return** on a match.
4. Accumulator zero → `401` `invalid_service_token`.

Two details that are the whole point of doing it this way:

- **Hash before comparing.** `subtle.ConstantTimeCompare` returns immediately when
  the two slices differ in length, which leaks the configured token's length to an
  attacker timing responses. Comparing fixed-width digests removes that channel.
- **No early return.** Breaking out of the loop on the first match would make a
  token matching the first configured entry measurably faster than one matching the
  last, leaking which slot a token occupies.

This is the repo's first use of `crypto/subtle`; there is no existing helper to
reuse.

### 5.3 Rotation

`CLIENT_UPDATE_UPLOAD_TOKENS` is a comma-separated **list**, and that plural is the
design's answer to "a static token cannot be revoked":

1. Append the new token to the list, deploy. Both old and new are accepted.
2. Roll the pipeline over to the new token.
3. Remove the old token from the list, deploy.

Two config changes, no window in which uploads fail. Without the list, rotation
means a redeploy that breaks every in-flight caller.

### 5.4 Fail-closed startup validation

`env:"CLIENT_UPDATE_UPLOAD_TOKENS,required"` alone is not sufficient — `caarlos0/env`
treats a set-but-empty variable as present, and an empty separator-split can yield a
single empty element. Config validation is therefore explicit and runs before the
server binds:

- at least one token after trimming surrounding whitespace and dropping empties;
- every token at least 32 characters — `pkg/sessiontoken.New()` (43-char base64url
  from 32 random bytes) is the recommended generator;
- duplicates rejected, since a duplicate silently halves an operator's intended
  rotation window.

Any failure exits non-zero with a message that names the variable and **never** its
value. Startup logs the token *count*, never the tokens.

## 6. Error contract

One new reason. Everything else reuses the existing catalog.

| Route | Case | Status | `code` | `reason` |
|---|---|---|---|---|
| POST | Missing, empty, or unrecognized `X-Service-Token` | 401 | `unauthenticated` | `invalid_service_token` |
| GET | No `ssoToken` header or cookie | 401 | `unauthenticated` | `missing_fields` |
| GET | Expired SSO token | 401 | `unauthenticated` | `sso_token_expired` |
| GET | Invalid SSO token, or empty `preferred_username` | 401 | `unauthenticated` | `invalid_sso_token` |

`pkg/errcode/codes_clientupdate.go` is new and holds exactly one constant:

```go
// ClientUpdateInvalidServiceToken: 401 on POST /api/v1/version when the
// X-Service-Token header is missing or does not match a configured token.
ClientUpdateInvalidServiceToken Reason = "invalid_service_token"
```

A distinct reason is justified rather than reusing `AdminInvalidToken`: a release
pipeline must be able to tell "my service token is wrong" from a human session
failure, and the two are issued and rotated by completely different mechanisms.

The three failure modes on POST — absent header, empty header, wrong value —
deliberately collapse to one identical response, so the wire cannot be used to
probe which part of the credential was wrong.

## 7. Configuration

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `CLIENT_UPDATE_UPLOAD_TOKENS` | yes | — | Comma-separated service tokens accepted on POST. ≥1 entry, each ≥32 chars. |
| `OIDC_ISSUER_URL` | when `DEV_MODE=false` | — | TSSO issuer for GET. |
| `OIDC_AUDIENCES` | when `DEV_MODE=false` | — | Comma-separated allowed audiences. |
| `TLS_SKIP_VERIFY` | no | `false` | Passed to the OIDC validator. |
| `DEV_MODE` | no | `false` | Bypasses OIDC on GET. Never true in a deployed environment. |

Resulting dependency set: **MinIO + TSSO**. No MongoDB, no botplatform, no NATS.

**TLS is load-bearing, not optional.** A static header secret is replayable by
anyone who observes a single request. The service must sit behind TLS termination,
and the existing network restriction from §12 stays in force — the token is a second
layer, not a replacement for it. This is stated in the middleware's doc comment as
well as here, because a code reader is the one most likely to relax it.

`deploy/docker-compose.yml` gains all five variables, with a clearly fake local
token and `DEV_MODE=true` so local dev needs no TSSO.

## 8. Rollout

`CLIENT_UPDATE_UPLOAD_TOKENS` is required, so **it must exist in every environment's
config before this version deploys**, or the service will not start. That is
deliberate: the alternative — an optional variable that silently leaves POST
anonymous — is exactly the hole this change closes.

**Mixed-version window.** Both endpoints are unauthenticated before this change, so
during a rolling deploy behind one address a request refused by a new replica can be
retried against an old one and succeed. The policy is not in force until the last
old replica is gone. Deploy blue/green or drain-and-cut-over; a rolling restart
leaves the hole open for the length of the rollout.

**Client coordination.** Every existing caller breaks the moment this ships — the
desktop updater must send `ssoToken`, and the release pipeline must send
`X-Service-Token`. Both live outside this repo. They need updating in lockstep with
the deploy, or downloads and uploads both start returning 401.

## 9. Testing

Per CLAUDE.md: TDD throughout, `-race`, ≥80% coverage, table-driven, no real network.

**`requireServiceToken`** — no header; empty header; whitespace-only header; wrong
token; correct token; correct *second* token (rotation); a token that is a strict
prefix of a valid one; a token that extends a valid one; differing case; a token of
the same length as a valid one but different content. The prefix/extension cases are
the ones that catch a comparison accidentally rewritten as `strings.HasPrefix` or a
truncated compare.

**`requireSSO`** — no header and no cookie; header present; cookie fallback when the
header is absent; header wins when both are present; valid claims; `ErrTokenExpired`;
generic validator error; empty `preferred_username`; `DEV_MODE=true` bypass. The SSO
validator is a stub implementing `TokenValidator` — no OIDC network calls.

**Config** — empty variable rejected; whitespace-only rejected; a sub-32-char token
rejected; duplicates rejected; surrounding whitespace trimmed; a valid multi-token
list parsed in order.

**Routes** — `/healthz` reachable with no credential; POST without a token 401s
before reaching the handler; GET without a token 401s before reaching the handler.
"Before reaching the handler" is asserted with a handler that fails the test if
called, so a middleware ordering mistake cannot pass silently.

**Secret hygiene** — a test asserts the configured token never appears in the access
log output or in any error body.

`integration_test.go` needs its router construction updated for the new
`registerRoutes` signature; its MinIO container usage is unchanged.

## 10. Documentation

Same-PR updates, per CLAUDE.md's client-facing-handler rule:

- `docs/client-api.md` §12 — delete the UNAUTHENTICATED warning block, and replace
  each endpoint's `**Auth:** none (v1)` line with the real credential, following the
  per-endpoint `**Auth:**` style the rest of the document already uses (e.g. §7's
  emoji PUT: *"`x-user-id` + `x-auth-token` …, **admin role required**"*). Add the
  401 rows to both response tables.
- `docs/client-api/request-reply.md` — the "HTTP — Client Update Service" section
  opens *"(no `ssoToken`/auth in v1 — must be network-restricted)"*; that parenthetical
  is rewritten to name both credentials. The derived view must not drift from the
  canonical doc.

There is no global per-service credential table in `client-api.md` — auth is stated
per endpoint — so §12 and the derived view are the complete documentation surface.
`events.md` is unaffected: no events change.

## 11. Risks

| Risk | Mitigation |
|---|---|
| Service token leaks via pipeline logs, a shared config store, or an observed request | Multi-token rotation (§5.3) makes replacing it a config change. The service never logs it. TLS-only deployment. This risk is inherent to the chosen credential and cannot be designed away — see §3.1. |
| Compromised token = RCE on every workstation | Network restriction stays in force alongside the token. Not reduced by this change beyond adding the token itself. |
| `CLIENT_UPDATE_UPLOAD_TOKENS` missing → service won't boot | Intentional, and called out in §8 so it lands in deploy config first. |
| Desktop updater not updated in lockstep → all downloads 401 | §8. Needs the client release coordinated with this deploy. |
| TSSO outage blocks all update downloads | Correct behaviour — the credential genuinely cannot be verified. Uploads are unaffected, since POST does not touch TSSO. |
| `DEV_MODE=true` reaching a deployed environment | It bypasses OIDC entirely. Guarded only by deploy config; worth a deployment-time assertion outside this repo. |
