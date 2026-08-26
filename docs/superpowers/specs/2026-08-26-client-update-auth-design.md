# Authenticating client-update-service uploads — Design

**Date:** 2026-08-26 · **Status:** Draft, pending review

---

## 1. Problem

`client-update-service` distributes desktop client software updates: a `.yaml`
descriptor plus an executable, stored in MinIO. Both of its endpoints are
completely unauthenticated. `docs/client-api.md` §12 says so in a warning block:

> **These endpoints are UNAUTHENTICATED in v1.** Anyone who can reach the service
> can upload or download update artifacts. **They MUST be network-restricted
> before any production exposure.**

The two halves of that hole are not equally severe, and this design closes only
one of them:

- `GET /api/v1/version/:fileName` serves the client binary to anyone who can reach
  the service. Bounded: the artifact is shipped to every employee anyway, and it
  is a read.
- `POST /api/v1/version` **overwrites the executable that every desktop client
  auto-downloads and runs.** An unauthenticated write here is remote code
  execution on every workstation in the org, one request deep. It is the highest-
  value write surface in the system.

**Scope: the write only.** Downloads stay public. That asymmetry is the design.

## 2. Goals / Non-goals

**Goals**

- `POST /api/v1/version` requires an authorized **service account** credential —
  the uploader is a release pipeline, not a person at a keyboard.
- Rewrite the §12 warning so it describes what is actually true afterwards.
- No new dependency the service does not strictly need.

**Non-goals**

- **No authentication on `GET /api/v1/version/:fileName`.** Explicitly considered
  and dropped. An earlier revision of this design gated downloads on an SSO
  session via `pkg/oidc`; that is removed. Downloads remain open to anyone who can
  reach the service, and the network restriction is what bounds them — see §4 and
  the risk in §9.
- No authorization beyond "is this caller allowed to upload at all". There are no
  per-artifact permissions and no notion of artifact ownership.
- No rate limiting, no idempotency keys. The upload path is one caller at release
  cadence.
- No change to upload/download behaviour, streaming, caching, or the MinIO layout.
- No cross-site concerns. Artifacts are per-site; nothing federates.

## 3. Decisions settled

| Decision | Choice | Rationale |
|---|---|---|
| GET credential | None — stays public | §4. The repo already has this shape: media-service's three GETs are public by design while its two PUTs require a session. |
| POST credential | Static service token in `X-Service-Token` | Chosen for a CI/release pipeline that cannot perform a login round-trip. See §3.1 — a deliberate trade against the recommended alternative. |
| Token comparison | SHA-256 both sides, then `subtle.ConstantTimeCompare` | §5.2. |
| Multiple valid tokens | Yes, comma-separated | Rotation without a downtime window. §5.3. |
| Mongo, OIDC, botplatform | None used | With GET public and POST on a static secret, no credential on either route needs a lookup. The service keeps MinIO as its only dependency. |

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
config, no login call, no upstream dependency on the write path. The costs are real
and are not hidden by this design:

| Cost | Mitigation in this design |
|---|---|
| No revocation short of a redeploy | Multi-token config (§5.3) makes rotation a config change with no downtime, not a code change. |
| No per-caller attribution — every upload looks identical in logs | None available. If two pipelines ever share the endpoint, the logs cannot tell them apart. Accepted. |
| Replayable by anyone who observes one request | TLS-only deployment, documented in §6 and in the code. |
| No precedent in this repo — first bespoke auth scheme | Contained to one middleware in one service; the reason code is namespaced to it. |

This is recorded so a future reader knows the alternative was considered and what
was traded away, not discovered later as an oversight.

## 4. GET stays public

`GET /api/v1/version/:fileName` gets no middleware. It keeps its current behaviour
exactly: name validation, cache lookup, MinIO fallback, streamed response.

This is a deliberate accepted risk, not an oversight. What it means concretely:
anyone who can reach the service can enumerate-by-guess and download the client
binary and its descriptor. The bound on that is the **network restriction**, which
therefore stays in force after this change — the §12 warning is rewritten, not
deleted, because half of what it warns about remains true.

The shape has precedent: `media-service` keeps `GET /avatar/*` and
`GET /emoji/:shortcode` public while requiring a session on its two PUTs, for the
same reason — reads are low-value and have callers that cannot easily hold a
credential.

An earlier revision of this design gated downloads on an SSO session through
`pkg/oidc`, ported from `upload-service`. It was dropped: it would have forced every
desktop updater in the field to start sending an `ssoToken` in lockstep with the
deploy, and added TSSO as a hard runtime dependency of software distribution — so a
TSSO outage would block client updates. Neither cost buys much when the protected
asset is a binary every employee already has.

## 5. POST — static service token

### 5.1 Header

`X-Service-Token`. A dedicated header rather than `Authorization: Bearer` so a
static shared secret can never be confused with the session and SSO conventions
used elsewhere in the platform.

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

One new reason. Nothing else changes — GET's existing 400/404/500 responses are
untouched.

| Route | Case | Status | `code` | `reason` |
|---|---|---|---|---|
| POST | Missing, empty, or unrecognized `X-Service-Token` | 401 | `unauthenticated` | `invalid_service_token` |

`pkg/errcode/codes_clientupdate.go` is new and holds exactly one constant:

```go
// ClientUpdateInvalidServiceToken: 401 on POST /api/v1/version when the
// X-Service-Token header is missing or does not match a configured token.
ClientUpdateInvalidServiceToken Reason = "invalid_service_token"
```

A distinct reason is justified rather than reusing `AdminInvalidToken`: a release
pipeline must be able to tell "my service token is wrong" from a human session
failure, and the two are issued and rotated by completely different mechanisms.

The three failure modes — absent header, empty header, wrong value — deliberately
collapse to one identical response, so the wire cannot be used to probe which part
of the credential was wrong.

**TLS is load-bearing, not optional.** A static header secret is replayable by
anyone who observes a single request. The service must sit behind TLS termination,
and the network restriction stays in force — the token is a second layer, not a
replacement for it. This is stated in the middleware's doc comment as well as here,
because a code reader is the one most likely to relax it.

## 7. Configuration

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `CLIENT_UPDATE_UPLOAD_TOKENS` | yes | — | Comma-separated service tokens accepted on POST. ≥1 entry, each ≥32 chars. |

That is the entire configuration change. Resulting dependency set: **MinIO**, exactly
as today. No MongoDB, no TSSO, no botplatform, no NATS.

`deploy/docker-compose.yml` gains the one variable with a clearly fake local token.

## 8. Rollout

`CLIENT_UPDATE_UPLOAD_TOKENS` is required, so **it must exist in every environment's
config before this version deploys**, or the service will not start. That is
deliberate: the alternative — an optional variable that silently leaves POST
anonymous — is exactly the hole this change closes.

**Mixed-version window.** POST is unauthenticated before this change, so during a
rolling deploy behind one address a request refused by a new replica can be retried
against an old one and succeed. The policy is not in force until the last old
replica is gone. Deploy blue/green or drain-and-cut-over; a rolling restart leaves
the hole open for the length of the rollout.

**Client coordination is limited to the uploader.** Because GET stays public, the
desktop updater in the field needs **no change** and cannot break — a significant
simplification over gating downloads. Only the release pipeline must start sending
`X-Service-Token`, and it must do so in lockstep with the deploy or uploads start
returning 401.

## 9. Testing

Per CLAUDE.md: TDD throughout, `-race`, ≥80% coverage, table-driven, no real network.

**`requireServiceToken`** — no header; empty header; whitespace-only header; wrong
token; correct token; correct *second* token (rotation); a token that is a strict
prefix of a valid one; a token that extends a valid one; differing case; a token of
the same length as a valid one but different content. The prefix/extension cases are
the ones that catch a comparison accidentally rewritten as `strings.HasPrefix` or a
truncated compare.

**Config** — empty variable rejected; whitespace-only rejected; a sub-32-char token
rejected; duplicates rejected; surrounding whitespace trimmed; a valid multi-token
list parsed in order.

**Routes** — `/healthz` reachable with no credential; **`GET /api/v1/version/:fileName`
reachable with no credential**, asserted explicitly so a future change cannot quietly
gate it; POST without a token 401s *before* reaching the handler. "Before reaching the
handler" is asserted with a store mock that has no expectations, so a middleware
ordering mistake cannot pass silently.

**Secret hygiene** — a test asserts the configured token never appears in the access
log output or in any error body.

`integration_test.go` needs only its `registerRoutes` call updated for the new
signature; its download assertions are unchanged, because downloads are unchanged.

## 10. Documentation

Same-PR updates, per CLAUDE.md's client-facing-handler rule:

- `docs/client-api.md` §12 — **rewrite** the UNAUTHENTICATED warning rather than
  deleting it: uploads are now gated, downloads are still open, so the
  network-restriction warning must survive in narrowed form. Replace `POST`'s
  `**Auth:** none (v1)` with the service-token contract, following the per-endpoint
  `**Auth:**` style the rest of the document uses. `GET`'s `**Auth:** none (v1)`
  stays, reworded to say public **by design** rather than "in v1" — the current
  wording implies a fix is pending, and after this change it is not. Add the 401 row
  to the POST response table only.
- `docs/client-api/request-reply.md` — the "HTTP — Client Update Service" section
  opens *"(no `ssoToken`/auth in v1 — must be network-restricted)"*; rewrite to say
  POST takes a service token and GET is public. The derived view must not drift from
  the canonical doc.

There is no global per-service credential table in `client-api.md` — auth is stated
per endpoint — so §12 and the derived view are the complete documentation surface.
`events.md` is unaffected: no events change.

## 11. Risks

| Risk | Mitigation |
|---|---|
| **Client binary downloadable by anyone who can reach the service** | Unchanged from today, and now permanent rather than pending. Bounded only by the network restriction, which §10 keeps in the docs. Accepted per §2. |
| Service token leaks via pipeline logs, a shared config store, or an observed request | Multi-token rotation (§5.3) makes replacing it a config change. The service never logs it. TLS-only deployment. Inherent to the chosen credential — see §3.1. |
| Compromised token = RCE on every workstation | Network restriction stays in force alongside the token. Not reduced by this change beyond adding the token itself. |
| `CLIENT_UPDATE_UPLOAD_TOKENS` missing → service won't boot | Intentional, and called out in §8 so it lands in deploy config first. |
| Release pipeline not updated in lockstep → uploads 401 | §8. The blast radius is one pipeline, not the whole fleet, because downloads are untouched. |
