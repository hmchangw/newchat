# Bot-Account Auth Tokens on the HTTP REST APIs — Design

**Date:** 2026-08-13 · **Status:** Draft, pending review

---

## 1. Problem

Bot and admin accounts log in with username + password against botplatform-service
(`POST /api/v1/login`, client-api.md §10.1) and receive an opaque 43-char session
token, which they present as `x-user-id` + `x-auth-token`. That credential works on
botplatform's own five bot endpoints and on admin-service — and nowhere else.

Every file endpoint on `upload-service` accepts **only** an OIDC `ssoToken`, which a
bot can never obtain: bots are not in the SSO directory. A bot today cannot upload an
image, upload a file, or download a protected attachment, even for a room it is a
member of.

Separately, media-service's two `PUT` endpoints accept **no credential at all**.
`docs/client-api.md` flags this in a warning block: *"This endpoint is UNAUTHENTICATED
in v1. Anyone who can reach the service can upload or overwrite any bot's avatar …
It MUST be network-restricted or gated before any production exposure."*

## 2. Audit — which services actually need work

Every HTTP endpoint documented in `client-api.md`, by credential:

| Service | Endpoints | Credential today | Needs work |
|---|---|---|---|
| upload-service | 6 (setCookie, upload images/file, 3 downloads) | `ssoToken` only | **Yes** |
| media-service | `PUT /avatar/bot/:botName`, `PUT /emoji/:shortcode` | none | **Yes** |
| media-service | 3 GETs (avatar ×2, emoji) | public by design | No |
| media-service | `GET /drive.members` | none | No — see below |
| auth-service | `POST /api/v1/auth` | `ssoToken` **or** `authToken` | No — already dual |
| admin-service | 12 `/v1/admin/*` | `Authorization: Bearer <authToken>` | No |
| botplatform-service | login, validate, 5 bot endpoints | `x-user-id` + `x-auth-token` | No |
| portal-service | userInfo, login, settings | none (discovery tier) | No |
| tcard-service | cards get/validate/refresh | none | No |
| client-update-service | version upload/download | none (v1) | No |

upload-service and media-service are the entire surface.

`GET /drive.members` is out of scope but not out of mind: it is unauthenticated
today and discloses room and member data — more than the two PUTs ever wrote. It
stays public here because gating it needs its Drive-side caller updated in
lockstep, which is a separate change. Until then it relies on the same network
restriction as the avatar PUT, and is recorded as follow-up work.

## 3. Goals / Non-goals

**Goals**

- A bot or admin session token authenticates five of the six upload-service
  endpoints. `setCookie` stays SSO-only — it mirrors a token into a cookie for
  `<img>` downloads, which a session caller has no use for.
- The two media-service PUTs require a session token; anonymous callers are refused.
- SSO callers keep their exact current behavior — no regression on the human path.
- One implementation of session-token validation, shared, not copy-pasted per service.

**Non-goals**

- No change to the three public media-service GETs. The frontend loads them from
  `<img src>`, which cannot send headers.
- No change to `GET /drive.members`, which also stays public (§2).
- No new credential type, no NATS-JWT changes, no session-storage changes.
- No rate limiting or idempotency on these endpoints (botplatform's bot endpoints have
  their own; file endpoints are not in that traffic tier).
- No cross-site token routing. Validation is local-site, as it is for auth-service.

## 4. Decisions already settled

| Decision | Choice | Rationale |
|---|---|---|
| Where validation happens | HTTP `POST {botplatform}/api/v1/auth/validate` | botplatform solely owns the `sessions` collection; asking over HTTP keeps that contract single-owner. Follows auth-service's existing `httpBotplatformValidator`. |
| media-service PUT auth | Mandatory | Closes the documented hole outright; no env var can silently reopen it. |
| Bot email to Drive | Empty by default | See §7.3 — the value is outbound attribution only. |

## 5. Architecture — `pkg/botauth`

A new shared package owns session-token validation. It does **not** go in
`pkg/ginutil`, which is generic transport middleware (request-ID, CORS, access log)
with no domain dependencies; adding resty + principal + errcode there would pollute it.

```go
package botauth

const (
    HeaderUserID    = "x-user-id"
    HeaderAuthToken = "x-auth-token"
)

// TokenValidator resolves a raw session token to its principal.
type TokenValidator interface {
    Validate(ctx context.Context, authToken string) (principal.Principal, error)
}

type Validator struct{ /* resty client + baseURL */ }
func NewValidator(client *resty.Client, baseURL string) *Validator
func (v *Validator) Validate(ctx context.Context, authToken string) (principal.Principal, error)

func Credentials(h http.Header) (userID, token string)
func Authenticate(ctx context.Context, v TokenValidator, userID, token string) (principal.Principal, error)
func HasRole(p principal.Principal, role model.UserRole) bool
```

`Validator.Validate` returns a typed `errcode.Unauthenticated` on an upstream 401 or a
`200 {"valid":false}` body, and a raw wrapped error on transport failures or any other
status — so callers can tell "your credential is bad" from "we could not check".

`Authenticate` layers the credential rules on top: it rejects empty credentials before
making any upstream call, and confirms `principal.UserID` matches the supplied
`x-user-id`. Missing credentials, an unknown token, and a userID/session mismatch all
return the **same** 401 `invalid_token`, so the wire never reveals which of the three
failed — the rule botplatform's own `requireBot` already follows. A raw error from the
validator becomes 503 `upstream_unavailable`.

**Duplicate removal.** `auth-service/bpvalidator.go` is the same logic and is deleted;
auth-service wires `botauth.NewValidator` instead. Its consumer-side
`BotplatformValidator` interface in `handler.go` is unchanged and `*botauth.Validator`
satisfies it, so `handler_test.go` and its fake are untouched.

## 6. upload-service

### 6.1 Credential selection

`authMiddleware` grows a second branch. Selection rules, in order:

1. Both an `ssoToken` **header** and an `x-auth-token` header → `400 ambiguous_token`.
   Mirrors auth-service §2.2, which already rejects `ssoToken` + `authToken` together.
2. `x-auth-token` header present → session path.
3. Otherwise → existing SSO path (`ssoToken` header, then `ssoToken` cookie).

An `x-auth-token` header beats an `ssoToken` **cookie**: the cookie is ambient state a
browser attaches automatically, the header is an explicit act. Only two explicit
headers are ambiguous.

The middleware signature moves to a small `authDeps` struct (SSO validator, bot
validator, bot email domain, dev mode) rather than growing to four positional
parameters.

### 6.2 Who is accepted

Any principal botplatform validates — upload-service performs **no role check at all**.
Botplatform issues sessions exclusively to password-eligible accounts (bot or admin),
so a validated session is already proof of eligibility; re-checking the role here would
only add a way to get it wrong. Concretely, restricting to the `bot` role would break
file upload for every admin using chat-frontend, which per the bot/admin auth design doc
already offers them username/password login. Room-scoped endpoints remain gated on
membership regardless of role.

This is the one place the three services differ: media-service **does** check roles
(§7.2), because its PUTs write shared, non-room-scoped assets that membership cannot
gate.

### 6.3 Identity mapping

`AuthenticatedUser` gains a `Session *principal.Principal` field — nil for SSO callers,
populated for session callers, letting handlers read roles/siteID and distinguish the
credential type.

| Field | SSO caller | Session caller |
|---|---|---|
| `Account` | `claims.PreferredUsername` | `principal.Account` |
| `EngName` / `ChineseName` | parsed from `claims.Description` | empty — botplatform's principal carries identity, not directory metadata |
| `DisplayName()` | `"Eng Chinese"` | falls back to `Account` (existing behavior when either name is empty) |
| `Email` | `claims.Email` | see §7.3 |

Membership (`store.IsMember(roomID, account)`) is unchanged: bots hold real
subscription rows, which is how botplatform already resolves a bot's room site.

### 6.4 `POST /file/setCookie` rejects session callers

This endpoint exists solely to convert an `ssoToken` header into an `ssoToken` cookie
so browser `<img>` downloads can authenticate. It is meaningless for a bot, and left
unguarded it would write a cookie with an empty value and fail confusingly later.
Session callers get `400` with a message saying the endpoint is SSO-only. The other
five endpoints accept both credentials.

## 7. media-service

### 7.1 Enforcement

`BOTPLATFORM_URL` becomes **required**. The service fails to start without it, so no
deployment can accidentally serve the PUTs unauthenticated. The three GETs are
untouched and stay public.

### 7.2 Authorization

| Endpoint | Rule | Failure |
|---|---|---|
| `PUT /api/v1/avatar/bot/:botName` | valid session **and** (`principal.Account == :botName` **or** admin role) | `403 not_admin` |
| `PUT /api/v1/emoji/:shortcode` | valid session **and** admin role | `403 not_admin` |

Rationale: a bot avatar is that bot's own property, so self-service plus an admin
override for provisioning. A **shortcode is a site-wide shared namespace** — one bot
silently overwriting `:party:` for every user on the site is exactly the privilege
escalation worth preventing, so emoji upload is an admin operation. This is safe to
tighten: no frontend and no tooling in this repo calls either PUT, so there are no
existing callers to break.

`?uploader=` becomes **ignored**. The `CreatedBy`/`UpdatedBy` audit fields are taken
from the authenticated session instead, which is strictly more trustworthy than a
client-supplied query parameter. The parameter is documented as accepted-and-ignored
rather than rejected, so an old caller does not start getting 400s.

### 7.3 Bot email to Drive (upload-service)

`user.Email` is passed to `drive.UploadGroupImages` and sent to the external Drive
backend as an `email` form field alongside `userId`/`userName` (`pkg/drive/uploader.go`).
Nothing in this codebase reads it back — it is outbound attribution only. The existing
`Email == ""` → 500 guard is a fail-fast so Drive never receives blank attribution.

Whether Drive rejects a blank `email` **cannot be determined from this repo**: Drive is
an external service, the only implementation here is a test fake, and the `drive-mock`
hostname in `upload-service/deploy/docker-compose.yml` has no service definition behind
it. Verifying it requires a real or staging Drive.

Therefore this is a config decision, not a code decision:

- `BOT_EMAIL_DOMAIN` **unset (default)** → session callers send an empty email; the
  500 guard is skipped for them and still applies to SSO callers, where a blank email
  means a broken token.
- `BOT_EMAIL_DOMAIN=bots.example.com` → session callers send `{account}@{domain}`.

If Drive turns out to reject blanks, ops sets one variable. No code change.

## 8. Error contract

No new reason codes. The existing catalog covers every case:

| Case | Status | `code` | `reason` |
|---|---|---|---|
| Missing / unknown token, or `x-user-id` disagrees with the session | 401 | `unauthenticated` | `invalid_token` |
| Both `ssoToken` and `x-auth-token` headers | 400 | `bad_request` | `ambiguous_token` |
| botplatform unreachable or erroring | 503 | `unavailable` | `upstream_unavailable` |
| Avatar PUT: session is neither the named bot nor an admin | 403 | `forbidden` | `not_admin` |
| Emoji PUT: session lacks the admin role | 403 | `forbidden` | `not_admin` |
| `setCookie` called with a session token | 400 | `bad_request` | — |

`AdminNotAuthorized` already serializes as `not_admin` and already means "valid
session, insufficient role", which is exactly both 403 cases.

## 9. Configuration

| Service | Variable | Required | Default | Purpose |
|---|---|---|---|---|
| upload-service | `BOTPLATFORM_URL` | yes | — | Local-site botplatform base URL |
| upload-service | `BOT_EMAIL_DOMAIN` | no | `""` | Empty → send empty email; set → `{account}@{domain}` |
| media-service | `BOTPLATFORM_URL` | yes | — | Local-site botplatform base URL |

Both services build the client with `restyutil.New("", restyutil.WithTimeout(5*time.Second))`,
matching auth-service.

**CORS.** upload-service adds `x-user-id, x-auth-token` to its
`Access-Control-Allow-Headers`; media-service does the same (its CORS is
wildcard-origin with no credentials, so custom headers are unproblematic there).

## 10. Rollout

`BOTPLATFORM_URL` is required in both services, so **it must be present in every
environment's config before this version is deployed**, or the services will not start.
This is deliberate: the alternative — an optional variable that silently leaves
media-service's PUTs anonymous — is the failure mode this change exists to remove.

**Mixed-version window.** media-service's PUTs are unauthenticated before this
change, so during a rolling deploy behind one address a request refused by a new
replica can be retried against an old one and succeed. The policy is not in force
until the last old replica is gone. Deploy media-service blue/green or
drain-and-cut-over, or enforce the credential at the gateway first; a rolling
restart leaves the hole open for the length of the rollout.

Local dev: both services' `deploy/docker-compose.yml` gain
`BOTPLATFORM_URL=http://botplatform-service:8080`.
`docker-local/compose.services.yaml` needs no change — it aggregates those files
via `include:` and only layers o11y settings on top, so the variable is inherited
(verified with `docker compose config`).
The Traefik gateway (`docker-local/traefik/dynamic.yml`) needs no change — it routes on
path prefix and forwards headers unmodified.

## 11. Testing

Per CLAUDE.md: TDD throughout, `-race`, ≥80% coverage (90%+ for the new `pkg/` package),
table-driven, no real network.

**`pkg/botauth`** — `httptest` server for the validator: valid session, `200
{valid:false}`, upstream 401, upstream 500, upstream 400, transport failure, request-ID
propagation. Stub validator for `Authenticate`: happy path, missing userID, missing
token, userID mismatch, upstream-invalid, upstream-unreachable, and a case asserting no
upstream call happens when credentials are absent. Table tests for `Credentials`
(including canonical header casing) and `HasRole`.

**upload-service** — extends `middleware_test.go`: session token populates the user,
invalid/mismatched token → 401, upstream down → 503, both headers → 400, `x-auth-token`
beats an `ssoToken` cookie, email synthesis on/off. Existing SSO tests must pass
unchanged — that is the regression guard. `handler_test.go` gains the setCookie
rejection and a bot-upload case asserting what reaches the Drive fake.

**media-service** — new `middleware_test.go` (the service has none today): anonymous →
401, bot uploading own avatar → allowed, bot uploading another bot's → 403, admin →
allowed for both, bot uploading emoji → 403, uploader recorded from session not
`?uploader=`. GET endpoints stay reachable without credentials.

**Integration** — both services have `integration_test.go` with testcontainers; the
botplatform dependency is an `httptest` server, not a container.

## 12. Documentation

Same-PR updates, per CLAUDE.md's client-facing-handler rule:

- `docs/client-api.md` §2.4 — upload-service now accepts either credential; auth table,
  header list, and error rows per endpoint.
- `docs/client-api.md` §7 — replace the 🔴 UNAUTHENTICATED warning on
  `PUT /avatar/bot/:botName`; document auth on both PUTs and the `?uploader=` change.
- `docs/client-api/request-reply.md` — the derived view carries all of these endpoints
  (§§ "HTTP — Connection & Auth", "Media Service — avatar/emoji endpoints") and must not
  drift. `events.md` is unaffected — no new events.
- `docs/specs/media-service.md` — the auth column currently reads **🔴 none (v1)**.

## 13. Risks

| Risk | Mitigation |
|---|---|
| Drive rejects a blank `email` for bot uploads | `BOT_EMAIL_DOMAIN` flips to synthesized addresses with no code change (§7.3). Needs one check against a real Drive. |
| `BOTPLATFORM_URL` missing → services won't boot | Intentional. Called out in §10 so it lands in deploy config first. |
| botplatform outage now blocks bot file uploads | Correct behavior — the credential genuinely cannot be verified. Surfaces as 503, distinct from 401, so callers can retry. SSO uploads are unaffected. |
| Per-request validation adds a hop to hot downloads | Only on the session path; SSO downloads are unchanged. Revisit with a short-TTL principal cache if measurements justify it — deliberately not in v1. |
| Tightening emoji upload to admin-only breaks a caller | No caller exists in this repo (frontends, tools, and migrations all checked). |
