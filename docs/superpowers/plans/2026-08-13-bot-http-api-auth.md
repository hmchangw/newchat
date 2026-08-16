# Bot Auth Tokens on HTTP REST APIs — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let bot/admin accounts authenticate every HTTP REST endpoint with their botplatform session token, and require that token on media-service's two currently-anonymous PUTs.

**Architecture:** A new shared package `pkg/botauth` validates a session token by POSTing it to botplatform-service `/api/v1/auth/validate` (botplatform solely owns the `sessions` collection). upload-service gains a second credential branch alongside its existing OIDC path; media-service gains mandatory session auth on its two write endpoints. `auth-service/bpvalidator.go` is deleted in favour of the shared package.

**Tech Stack:** Go 1.25, Gin, Resty (`pkg/restyutil`), `pkg/errcode` + `errhttp`, `pkg/principal`, testify, `go.uber.org/mock`.

**Spec:** `docs/superpowers/specs/2026-08-13-bot-http-api-auth-design.md`

## Global Constraints

- **TDD is mandatory** (CLAUDE.md §4): write the failing test, run it, confirm it fails, then implement. Never write implementation before its test.
- **Always use `make` targets**, never raw `go` commands. `make test SERVICE=<name>`, `make lint`, `make fmt`, `make sast`.
- **`-race` on every test run** — the Makefile handles this.
- **Coverage floor 80%**, target 90%+ for `pkg/` packages.
- **Error handling:** return typed `errcode.*` from handlers; `errhttp.Write(ctx, c, err)` at the Gin boundary. Never log AND return — `Write` classifies and logs once.
- **Never log tokens, passwords, or full message bodies** (CLAUDE.md §3).
- **Config via `caarlos0/env`** into a typed struct; never `os.Getenv` in service code. `SCREAMING_SNAKE_CASE` names.
- **Every commit** runs the pre-commit hook (lint + tests). Fix failures before retrying.
- **Branch:** `claude/bot-account-http-api-auth-y5ig48`. Never push elsewhere.
- **Reason codes:** reuse the existing catalog. Do NOT add new `Reason` constants — `BotplatformInvalidToken`, `BotplatformAmbiguousToken`, `BotplatformUpstreamUnavailable`, and `AdminNotAuthorized` cover every case.

---

## File Structure

| File | Responsibility |
|---|---|
| `pkg/botauth/botauth.go` (new) | Session-token validation + credential extraction + role check. Sole owner of the botplatform-validate wire call. |
| `pkg/botauth/botauth_test.go` (new) | Unit tests for the above. |
| `auth-service/bpvalidator.go` | **Deleted** — superseded by `pkg/botauth`. |
| `auth-service/main.go:77-81` | Wire `botauth.NewValidator` instead of the deleted local type. |
| `upload-service/middleware.go` | Credential selection: session branch vs existing SSO branch. |
| `upload-service/routes.go` | Pass the new `authDeps` through. |
| `upload-service/main.go` | `BOTPLATFORM_URL` + `BOT_EMAIL_DOMAIN` config, Resty client, wiring. |
| `upload-service/handler.go` | setCookie rejects session callers; email guard skipped for them. |
| `media-service/middleware_auth.go` (new) | `requireSession`, `requireBotSelfOrAdmin`, `requireAdmin`. Kept out of `middleware.go`, which is generic transport. |
| `media-service/middleware_auth_test.go` (new) | Unit tests for the above. |
| `media-service/routes.go` | Attach auth to the two PUTs; GETs untouched. |
| `media-service/main.go`, `config.go` | `BOTPLATFORM_URL` (required), Resty client, wiring. |
| `media-service/emoji_upload.go:138-140` | Uploader from the session, not `?uploader=`. |
| `media-service/middleware.go:28-40` | CORS: allow the two credential headers. |

---

### Task 1: `pkg/botauth` — shared session-token validator

**Files:**
- Create: `pkg/botauth/botauth.go`
- Test: `pkg/botauth/botauth_test.go`

**Interfaces:**
- Consumes: `principal.Principal` (`pkg/principal`), `errcode` constructors, `restyutil.New`, `natsutil.RequestIDFromContext`.
- Produces:
  - `const HeaderUserID = "x-user-id"`, `const HeaderAuthToken = "x-auth-token"`
  - `type TokenValidator interface { Validate(ctx context.Context, authToken string) (principal.Principal, error) }`
  - `func NewValidator(client *resty.Client, baseURL string) *Validator`
  - `func (v *Validator) Validate(ctx context.Context, authToken string) (principal.Principal, error)`
  - `func Credentials(h http.Header) (userID, token string)`
  - `func Authenticate(ctx context.Context, v TokenValidator, userID, token string) (principal.Principal, error)`
  - `func HasRole(p principal.Principal, role model.UserRole) bool`

- [ ] **Step 1: Write the failing test**

Create `pkg/botauth/botauth_test.go`:

```go
package botauth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/principal"
	"github.com/hmchangw/chat/pkg/restyutil"
)

// decodeJSON reads a request body into out.
func decodeJSON(r *http.Request, out any) error {
	return json.NewDecoder(r.Body).Decode(out)
}

// stubValidator is the Authenticate seam: it records the token it saw and
// returns the canned principal/error.
type stubValidator struct {
	principal principal.Principal
	err       error
	gotToken  string
}

func (s *stubValidator) Validate(_ context.Context, authToken string) (principal.Principal, error) {
	s.gotToken = authToken
	return s.principal, s.err
}

func TestCredentials(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		wantUserID string
		wantToken  string
	}{
		{
			name:       "both headers present",
			headers:    map[string]string{HeaderUserID: "u1", HeaderAuthToken: "tok"},
			wantUserID: "u1",
			wantToken:  "tok",
		},
		{
			name:       "canonical casing is accepted",
			headers:    map[string]string{"X-User-Id": "u1", "X-Auth-Token": "tok"},
			wantUserID: "u1",
			wantToken:  "tok",
		},
		{name: "missing token", headers: map[string]string{HeaderUserID: "u1"}, wantUserID: "u1"},
		{name: "missing user id", headers: map[string]string{HeaderAuthToken: "tok"}, wantToken: "tok"},
		{name: "no headers"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tt.headers {
				h.Set(k, v)
			}
			userID, token := Credentials(h)
			assert.Equal(t, tt.wantUserID, userID)
			assert.Equal(t, tt.wantToken, token)
		})
	}
}

func TestHasRole(t *testing.T) {
	tests := []struct {
		name  string
		roles []string
		role  model.UserRole
		want  bool
	}{
		{name: "bot role present", roles: []string{"bot"}, role: model.UserRoleBot, want: true},
		{name: "admin among many", roles: []string{"user", "admin"}, role: model.UserRoleAdmin, want: true},
		{name: "role absent", roles: []string{"user"}, role: model.UserRoleBot},
		{name: "no roles", role: model.UserRoleBot},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HasRole(principal.Principal{Roles: tt.roles}, tt.role))
		})
	}
}

func TestValidator_Validate(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantPrincipal principal.Principal
		wantCode      errcode.Code
		wantReason    errcode.Reason
		wantRawErr    bool
	}{
		{
			name:   "valid session",
			status: http.StatusOK,
			body: `{"valid":true,"principal":{"userId":"u1","account":"alerts.sa.bot",` +
				`"siteId":"site-a","roles":["bot"]}}`,
			wantPrincipal: principal.Principal{
				UserID: "u1", Account: "alerts.sa.bot", SiteID: "site-a", Roles: []string{"bot"},
			},
		},
		{
			name:       "200 with valid false",
			status:     http.StatusOK,
			body:       `{"valid":false}`,
			wantCode:   errcode.CodeUnauthenticated,
			wantReason: errcode.BotplatformInvalidToken,
		},
		{
			name:       "401 unknown session",
			status:     http.StatusUnauthorized,
			body:       `{"code":"unauthenticated","reason":"invalid_token"}`,
			wantCode:   errcode.CodeUnauthenticated,
			wantReason: errcode.BotplatformInvalidToken,
		},
		{name: "500 upstream fault is a raw error", status: http.StatusInternalServerError, body: `{"code":"internal"}`, wantRawErr: true},
		{name: "400 upstream rejection is a raw error", status: http.StatusBadRequest, body: `{"code":"bad_request"}`, wantRawErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath, gotRequestID string
			var gotBody map[string]string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotRequestID = r.Header.Get(natsutil.RequestIDHeader)
				_ = decodeJSON(r, &gotBody)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			v := NewValidator(restyutil.New(""), srv.URL)
			ctx := natsutil.WithRequestID(context.Background(), "01970a4f-8c2d-7c9a-abcd-e0123456789f")
			got, err := v.Validate(ctx, "raw-token")

			assert.Equal(t, "/api/v1/auth/validate", gotPath)
			assert.Equal(t, "raw-token", gotBody["authToken"])
			assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f", gotRequestID)

			var ec *errcode.Error
			switch {
			case tt.wantRawErr:
				require.Error(t, err)
				assert.False(t, errors.As(err, &ec), "upstream faults must stay raw so the caller maps them to 503")
			case tt.wantCode != "":
				require.Error(t, err)
				require.ErrorAs(t, err, &ec)
				assert.Equal(t, tt.wantCode, ec.Code)
				assert.Equal(t, tt.wantReason, ec.Reason)
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.wantPrincipal, got)
			}
		})
	}
}

func TestValidator_Validate_TransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close() // closed: the POST cannot connect

	v := NewValidator(restyutil.New(""), srv.URL)
	_, err := v.Validate(context.Background(), "raw-token")

	require.Error(t, err)
	var ec *errcode.Error
	assert.False(t, errors.As(err, &ec), "transport failures must stay raw")
}

func TestAuthenticate(t *testing.T) {
	validPrincipal := principal.Principal{
		UserID: "u1", Account: "alerts.sa.bot", SiteID: "site-a", Roles: []string{"bot"},
	}

	tests := []struct {
		name       string
		userID     string
		token      string
		stub       *stubValidator
		wantCode   errcode.Code
		wantReason errcode.Reason
	}{
		{name: "valid bot session", userID: "u1", token: "tok", stub: &stubValidator{principal: validPrincipal}},
		{
			name: "missing token", userID: "u1",
			stub:     &stubValidator{principal: validPrincipal},
			wantCode: errcode.CodeUnauthenticated, wantReason: errcode.BotplatformInvalidToken,
		},
		{
			name: "missing user id", token: "tok",
			stub:     &stubValidator{principal: validPrincipal},
			wantCode: errcode.CodeUnauthenticated, wantReason: errcode.BotplatformInvalidToken,
		},
		{
			// Same envelope as an unknown token: the wire must not reveal that the
			// token was real but belonged to a different user.
			name: "user id disagrees with session", userID: "someone-else", token: "tok",
			stub:     &stubValidator{principal: validPrincipal},
			wantCode: errcode.CodeUnauthenticated, wantReason: errcode.BotplatformInvalidToken,
		},
		{
			name: "upstream says invalid", userID: "u1", token: "tok",
			stub: &stubValidator{err: errcode.Unauthenticated("session token invalid",
				errcode.WithReason(errcode.BotplatformInvalidToken))},
			wantCode: errcode.CodeUnauthenticated, wantReason: errcode.BotplatformInvalidToken,
		},
		{
			name: "upstream unreachable", userID: "u1", token: "tok",
			stub:     &stubValidator{err: errors.New("dial tcp: connection refused")},
			wantCode: errcode.CodeUnavailable, wantReason: errcode.BotplatformUpstreamUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Authenticate(context.Background(), tt.stub, tt.userID, tt.token)

			if tt.wantCode == "" {
				require.NoError(t, err)
				assert.Equal(t, validPrincipal, got)
				assert.Equal(t, "tok", tt.stub.gotToken)
				return
			}

			require.Error(t, err)
			var ec *errcode.Error
			require.ErrorAs(t, err, &ec)
			assert.Equal(t, tt.wantCode, ec.Code)
			assert.Equal(t, tt.wantReason, ec.Reason)
		})
	}
}

func TestAuthenticate_NoUpstreamCallWhenCredentialsMissing(t *testing.T) {
	stub := &stubValidator{principal: principal.Principal{UserID: "u1"}}

	_, err := Authenticate(context.Background(), stub, "", "")

	require.Error(t, err)
	assert.Empty(t, stub.gotToken, "an empty credential must be rejected before the upstream call")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/botauth`
Expected: FAIL — `undefined: HeaderUserID`, `undefined: Credentials`, `undefined: NewValidator`, `undefined: Authenticate`, `undefined: HasRole`.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/botauth/botauth.go`:

```go
// Package botauth validates botplatform-service session tokens on behalf of
// the HTTP services that gate endpoints on a bot/admin credential. Sessions are
// owned solely by botplatform-service, so every other service asks it over HTTP
// (POST /api/v1/auth/validate) rather than reading the sessions collection —
// keeping the storage contract single-owner. The error envelopes here mirror
// botplatform's own requireBot middleware so a bot sees identical failures on
// every service it calls.
package botauth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/principal"
)

// Headers carrying the bot credential, matching the casing bot SDKs already
// send to botplatform-service. Header lookup is case-insensitive.
const (
	HeaderUserID    = "x-user-id"
	HeaderAuthToken = "x-auth-token"
)

// validatePath is botplatform's session-validation endpoint (client-api.md §10.2).
const validatePath = "/api/v1/auth/validate"

// TokenValidator resolves a raw session token to its principal. Authenticate
// consumes it so services can substitute a fake in tests; *Validator is the
// production implementation.
type TokenValidator interface {
	Validate(ctx context.Context, authToken string) (principal.Principal, error)
}

// Validator resolves session tokens against a botplatform-service instance.
type Validator struct {
	client  *resty.Client
	baseURL string
}

// NewValidator returns a Validator talking to the botplatform-service at
// baseURL. baseURL must be the LOCAL site's botplatform: validation is a
// local-DB lookup there, and cross-site routing is the gateway's concern.
func NewValidator(client *resty.Client, baseURL string) *Validator {
	return &Validator{client: client, baseURL: baseURL}
}

// Validate exchanges a raw session token for its principal. An unknown token
// (upstream 401, or a 200 carrying valid:false) returns a typed
// errcode.Unauthenticated; transport failures and every other status return a
// raw wrapped error so the caller surfaces them as an upstream fault rather
// than as a rejected credential.
func (v *Validator) Validate(ctx context.Context, authToken string) (principal.Principal, error) {
	var body struct {
		Valid     bool                `json:"valid"`
		Principal principal.Principal `json:"principal"`
	}
	req := v.client.R().
		SetContext(ctx).
		SetBody(map[string]string{"authToken": authToken}).
		SetResult(&body)
	if id := natsutil.RequestIDFromContext(ctx); id != "" {
		req = req.SetHeader(natsutil.RequestIDHeader, id)
	}

	resp, err := req.Post(v.baseURL + validatePath)
	if err != nil {
		return principal.Principal{}, fmt.Errorf("validate session token: %w", err)
	}
	switch resp.StatusCode() {
	case http.StatusOK:
		if !body.Valid {
			return principal.Principal{}, errInvalidToken()
		}
		return body.Principal, nil
	case http.StatusUnauthorized:
		return principal.Principal{}, errInvalidToken()
	default:
		// Body length only — a validate response can echo request material.
		return principal.Principal{}, fmt.Errorf("botplatform validate: HTTP %d (body %d bytes)",
			resp.StatusCode(), len(resp.Body()))
	}
}

// Credentials returns the bot user ID and raw session token from a request's
// headers. Either may be empty; Authenticate rejects that.
func Credentials(h http.Header) (userID, token string) {
	return h.Get(HeaderUserID), h.Get(HeaderAuthToken)
}

// Authenticate validates token and confirms it belongs to userID, returning the
// session principal. Missing credentials, an unknown token, and a userID that
// disagrees with the session all collapse to the same 401 invalid_token so the
// wire never reveals which of the three failed — the rule botplatform's own
// requireBot follows. An unreachable botplatform becomes 503
// upstream_unavailable, distinguishing "we could not check" from "you are not
// who you claim".
func Authenticate(ctx context.Context, v TokenValidator, userID, token string) (principal.Principal, error) {
	if userID == "" || token == "" {
		return principal.Principal{}, errInvalidToken()
	}

	p, err := v.Validate(ctx, token)
	if err != nil {
		// A typed error is already a client-facing verdict (invalid token);
		// anything raw means the check itself could not be completed.
		var ec *errcode.Error
		if errors.As(err, &ec) {
			return principal.Principal{}, ec
		}
		return principal.Principal{}, errcode.Unavailable("botplatform unavailable",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable),
			errcode.WithCause(err))
	}

	if p.UserID != userID {
		return principal.Principal{}, errInvalidToken()
	}
	return p, nil
}

// HasRole reports whether the principal carries role. Roles are denormalized
// onto the session at login, so this reflects the roles as of token issue.
func HasRole(p principal.Principal, role model.UserRole) bool {
	return slices.Contains(p.Roles, string(role))
}

// errInvalidToken builds the shared 401. Constructed per call rather than
// shared as a package sentinel so a caller cannot mutate it.
func errInvalidToken() error {
	return errcode.Unauthenticated("invalid session token",
		errcode.WithReason(errcode.BotplatformInvalidToken))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/botauth`
Expected: PASS.

- [ ] **Step 5: Check coverage meets the 90% pkg/ target**

Run: `go test -coverprofile=/tmp/botauth.out ./pkg/botauth/... && go tool cover -func=/tmp/botauth.out | tail -1`
Expected: total ≥ 90%. If below, add cases for the uncovered branch rather than lowering the bar.

- [ ] **Step 6: Lint and commit**

```bash
make fmt && make lint
git add pkg/botauth/
git commit -m "feat(botauth): shared botplatform session-token validator"
```

---

### Task 2: Delete auth-service's duplicate validator

**Files:**
- Delete: `auth-service/bpvalidator.go`
- Modify: `auth-service/main.go:77-81`

**Interfaces:**
- Consumes: `botauth.NewValidator` (Task 1).
- Produces: nothing new. `auth-service/handler.go`'s `BotplatformValidator` interface and `handler_test.go`'s fake are deliberately untouched — `*botauth.Validator` already satisfies that interface.

- [ ] **Step 1: Confirm the existing auth-service tests pass before touching anything**

Run: `make test SERVICE=auth-service`
Expected: PASS. This is the baseline the refactor must preserve — there is no new behavior here, so the existing suite IS the test.

- [ ] **Step 2: Delete the duplicate and rewire**

```bash
git rm auth-service/bpvalidator.go
```

In `auth-service/main.go`, add `"github.com/hmchangw/chat/pkg/botauth"` to the imports and replace the body of the `if cfg.BotplatformURL != ""` block (lines 77-81):

```go
	if cfg.BotplatformURL != "" {
		rc := restyutil.New("", restyutil.WithTimeout(5*time.Second))
		opts = append(opts, WithBotplatformValidator(
			botauth.NewValidator(rc, cfg.BotplatformURL)))
		slog.Info("session-token branch enabled", "botplatform_url", cfg.BotplatformURL)
	}
```

- [ ] **Step 3: Run the tests to verify nothing regressed**

Run: `make test SERVICE=auth-service`
Expected: PASS, same set of tests as Step 1.

- [ ] **Step 4: Lint and commit**

```bash
make fmt && make lint
git add auth-service/
git commit -m "refactor(auth-service): use shared pkg/botauth validator"
```

---

### Task 3: upload-service — dual-credential auth middleware

**Files:**
- Modify: `upload-service/middleware.go` (add session branch; `AuthenticatedUser` gains a field)
- Modify: `upload-service/routes.go`
- Modify: `upload-service/main.go` (config + wiring)
- Modify: `upload-service/deploy/docker-compose.yml`
- Test: `upload-service/middleware_test.go`

**Interfaces:**
- Consumes: `botauth.Credentials`, `botauth.Authenticate`, `botauth.TokenValidator`, `botauth.NewValidator` (Task 1).
- Produces:
  - `type authDeps struct { sso TokenValidator; bot botauth.TokenValidator; botEmailDomain string; devMode bool }`
  - `func authMiddleware(d authDeps) gin.HandlerFunc` (replaces `authMiddleware(v TokenValidator, devMode bool)`)
  - `AuthenticatedUser` gains `Session *principal.Principal` — nil for SSO callers. Task 4 branches on this.
  - `func registerRoutes(r *gin.Engine, h *Handler, d authDeps)` (replaces the `(r, h, v, devMode)` signature)

- [ ] **Step 1: Write the failing tests**

In `upload-service/middleware_test.go`, add these imports to the existing block: `"github.com/hmchangw/chat/pkg/botauth"`, `"github.com/hmchangw/chat/pkg/errcode"`, `"github.com/hmchangw/chat/pkg/principal"`.

Replace the existing `runAuth` helper (lines 26-45) with the version below — the existing SSO tests call it and must keep passing unchanged, which is the regression guard for this task:

```go
// fakeBotValidator implements botauth.TokenValidator for middleware tests.
type fakeBotValidator struct {
	principal principal.Principal
	err       error
}

func (f *fakeBotValidator) Validate(_ context.Context, _ string) (principal.Principal, error) {
	return f.principal, f.err
}

// runAuthReq drives authMiddleware over a caller-built request, returning the
// recorder and whatever AuthenticatedUser reached the handler.
func runAuthReq(t *testing.T, d authDeps, req *http.Request) (*httptest.ResponseRecorder, *AuthenticatedUser) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(authMiddleware(d))
	var captured *AuthenticatedUser
	r.GET("/x", func(c *gin.Context) {
		if u, ok := userFromContext(c); ok {
			captured = u
		}
		c.Status(http.StatusOK)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w, captured
}

// runAuth preserves the original SSO-only call shape used by the existing tests.
func runAuth(t *testing.T, v TokenValidator, devMode bool, token string) (*httptest.ResponseRecorder, *AuthenticatedUser) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	if token != "" {
		req.Header.Set("ssoToken", token)
	}
	return runAuthReq(t, authDeps{sso: v, devMode: devMode}, req)
}
```

Then update the two tests that build their own router — `TestAuthMiddleware_CookieFallback_PopulatesUser` (line 125) and any other direct `authMiddleware(v, false)` call — to use `authMiddleware(authDeps{sso: v})`.

> **Signature propagation — do this or the package will not compile.**
> `registerRoutes` is called from **six** places in `upload-service/handler_test.go`
> (lines 506, 524, 543, 932, 1097, 1110), each as `registerRoutes(r, h, nil, true)`.
> Rewrite every one to the new signature, preserving the same semantics
> (dev mode on, no SSO validator, no bot validator):
>
> ```go
> registerRoutes(r, h, authDeps{devMode: true})
> ```
>
> Line 1097 and 1110 use `&Handler{}` rather than `h` — keep whichever handler
> value each call site already passes and change only the trailing two arguments.

Now append the new tests:

```go
// botReq builds a request carrying the bot credential headers.
func botReq(userID, token string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	if userID != "" {
		req.Header.Set(botauth.HeaderUserID, userID)
	}
	if token != "" {
		req.Header.Set(botauth.HeaderAuthToken, token)
	}
	return req
}

func okPrincipal() principal.Principal {
	return principal.Principal{
		UserID: "u1", Account: "alerts.sa.bot", SiteID: "site-a", Roles: []string{"bot"},
	}
}

func TestAuthMiddleware_SessionToken_PopulatesUser(t *testing.T) {
	d := authDeps{bot: &fakeBotValidator{principal: okPrincipal()}}

	w, u := runAuthReq(t, d, botReq("u1", "tok"))

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, u)
	assert.Equal(t, "alerts.sa.bot", u.Account)
	assert.Equal(t, "site-a", u.SiteID)
	require.NotNil(t, u.Session, "session callers must carry their principal")
	assert.Equal(t, []string{"bot"}, u.Session.Roles)
	// No directory metadata on a session principal, so DisplayName falls back.
	assert.Equal(t, "alerts.sa.bot", u.DisplayName())
}

func TestAuthMiddleware_SessionToken_Failures(t *testing.T) {
	tests := []struct {
		name       string
		userID     string
		deps       authDeps
		wantStatus int
		wantReason string
	}{
		{
			name: "unknown token", userID: "u1",
			deps: authDeps{bot: &fakeBotValidator{err: errcode.Unauthenticated("nope",
				errcode.WithReason(errcode.BotplatformInvalidToken))}},
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name: "user id disagrees with session", userID: "someone-else",
			deps:       authDeps{bot: &fakeBotValidator{principal: okPrincipal()}},
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name: "missing user id header", userID: "",
			deps:       authDeps{bot: &fakeBotValidator{principal: okPrincipal()}},
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name: "botplatform unreachable", userID: "u1",
			deps:       authDeps{bot: &fakeBotValidator{err: errors.New("connection refused")}},
			wantStatus: http.StatusServiceUnavailable, wantReason: "upstream_unavailable",
		},
		{
			name: "session auth not configured", userID: "u1",
			deps:       authDeps{},
			wantStatus: http.StatusServiceUnavailable, wantReason: "upstream_unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, u := runAuthReq(t, tt.deps, botReq(tt.userID, "tok"))

			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Nil(t, u, "a rejected request must not reach the handler")
			var body struct {
				Reason string `json:"reason"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			assert.Equal(t, tt.wantReason, body.Reason)
		})
	}
}

func TestAuthMiddleware_BothHeaders_400Ambiguous(t *testing.T) {
	d := authDeps{sso: &fakeValidator{}, bot: &fakeBotValidator{principal: okPrincipal()}}
	req := botReq("u1", "tok")
	req.Header.Set("ssoToken", "sso-tok")

	w, u := runAuthReq(t, d, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Nil(t, u)
	var body struct {
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "ambiguous_token", body.Reason)
}

func TestAuthMiddleware_SessionTokenBeatsSSOCookie(t *testing.T) {
	// The cookie is ambient state a browser attaches automatically; the header is
	// an explicit act, so it wins rather than being ambiguous.
	d := authDeps{sso: &fakeValidator{}, bot: &fakeBotValidator{principal: okPrincipal()}}
	req := botReq("u1", "tok")
	req.Header.Set("Cookie", "ssoToken=c-tok")

	w, u := runAuthReq(t, d, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, u)
	assert.Equal(t, "alerts.sa.bot", u.Account)
	assert.NotNil(t, u.Session)
}

func TestAuthMiddleware_SessionToken_Email(t *testing.T) {
	tests := []struct {
		name   string
		domain string
		want   string
	}{
		{name: "unset domain sends empty email", domain: "", want: ""},
		{name: "configured domain synthesizes an address", domain: "bots.example.com", want: "alerts.sa.bot@bots.example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := authDeps{bot: &fakeBotValidator{principal: okPrincipal()}, botEmailDomain: tt.domain}

			w, u := runAuthReq(t, d, botReq("u1", "tok"))

			require.Equal(t, http.StatusOK, w.Code)
			require.NotNil(t, u)
			assert.Equal(t, tt.want, u.Email)
		})
	}
}
```

Add `"encoding/json"` to the test file's imports.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `undefined: authDeps`, and `authMiddleware` called with 1 arg instead of 2.

- [ ] **Step 3: Implement the middleware**

In `upload-service/middleware.go`, add imports `"github.com/hmchangw/chat/pkg/botauth"` and `"github.com/hmchangw/chat/pkg/principal"`.

Extend `AuthenticatedUser` (line 35):

```go
// AuthenticatedUser is the identity resolved from a validated credential.
type AuthenticatedUser struct {
	model.User
	Email string
	// Session is the botplatform principal when the caller authenticated with a
	// session token (bot/admin); nil for SSO callers. Handlers branch on it for
	// the two places the credential type matters: the SSO-only setCookie
	// endpoint and the Drive email guard.
	Session *principal.Principal
}
```

Add the deps struct and replace `authMiddleware` (lines 118-170):

```go
// authDeps is what authMiddleware needs to resolve either credential. A struct
// rather than positional parameters: the list grew past the point where call
// sites stayed readable.
type authDeps struct {
	sso            TokenValidator
	bot            botauth.TokenValidator
	botEmailDomain string
	devMode        bool
}

// authMiddleware resolves the caller from either credential and stores an
// AuthenticatedUser in the Gin context. Selection order: two explicit headers
// are ambiguous and rejected; an x-auth-token header takes the session path;
// everything else takes the SSO path (header, then cookie). The session header
// deliberately beats an ssoToken *cookie* — a browser attaches the cookie
// automatically, so only two explicit headers signal a genuine conflict.
func authMiddleware(d authDeps) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

		botUserID, botToken := botauth.Credentials(c.Request.Header)
		if botToken != "" && c.GetHeader(ssoTokenName) != "" {
			errhttp.Write(ctx, c, errcode.BadRequest("set exactly one of ssoToken / x-auth-token",
				errcode.WithReason(errcode.BotplatformAmbiguousToken)))
			c.Abort()
			return
		}

		if botToken != "" {
			user, err := d.sessionUser(ctx, botUserID, botToken)
			if err != nil {
				errhttp.Write(ctx, c, err)
				c.Abort()
				return
			}
			c.Set(ctxUserKey, user)
			c.Set("bot_account", user.Account)
			c.Next()
			return
		}

		token := tokenFromRequest(c)
		if token == "" {
			errhttp.Write(ctx, c, errcode.Unauthenticated("missing ssoToken",
				errcode.WithReason(errcode.AuthMissingFields)))
			c.Abort()
			return
		}

		var user AuthenticatedUser
		if d.devMode {
			user = AuthenticatedUser{
				User:  model.User{Account: token, EngName: token},
				Email: token + "@dev.local",
			}
		} else {
			claims, err := d.sso.Validate(ctx, token)
			if err != nil {
				if errors.Is(err, pkgoidc.ErrTokenExpired) {
					errhttp.Write(ctx, c, errcode.Unauthenticated("sso token has expired, please re-login",
						errcode.WithReason(errcode.AuthTokenExpired)))
					c.Abort()
					return
				}
				errhttp.Write(ctx, c, errcode.Unauthenticated("invalid sso token",
					errcode.WithReason(errcode.AuthInvalidToken)))
				c.Abort()
				return
			}
			account := claims.PreferredUsername
			if account == "" {
				account = claims.Name
			}
			engName, chineseName := parseDescription(claims.Description)
			user = AuthenticatedUser{
				User: model.User{
					Account:     account,
					EngName:     engName,
					ChineseName: chineseName,
				},
				Email: claims.Email,
			}
		}

		c.Set(ctxUserKey, &user)
		c.Next()
	}
}

// sessionUser resolves a botplatform session token into an AuthenticatedUser.
// The nil-validator branch is unreachable in production (BOTPLATFORM_URL is
// required) but keeps the failure explicit rather than a nil dereference.
func (d authDeps) sessionUser(ctx context.Context, userID, token string) (*AuthenticatedUser, error) {
	if d.bot == nil {
		return nil, errcode.Unavailable("session-token auth not configured",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable))
	}
	p, err := botauth.Authenticate(ctx, d.bot, userID, token)
	if err != nil {
		return nil, err
	}
	return &AuthenticatedUser{
		User:    model.User{Account: p.Account, SiteID: p.SiteID},
		Email:   botEmail(p.Account, d.botEmailDomain),
		Session: &p,
	}, nil
}

// botEmail returns the Drive attribution address for a session caller. Drive
// stores this field but nothing here reads it back, so an empty value is the
// default; BOT_EMAIL_DOMAIN switches to a synthesized address if Drive turns
// out to require one.
func botEmail(account, domain string) string {
	if domain == "" {
		return ""
	}
	return account + "@" + domain
}
```

Update the CORS `Access-Control-Allow-Headers` (line 96) so browser callers may send the credential:

```go
			c.Header("Access-Control-Allow-Headers", "Content-Type, "+ssoTokenName+", X-Request-ID, "+
				botauth.HeaderUserID+", "+botauth.HeaderAuthToken)
```

- [ ] **Step 4: Update routes.go and main.go so the service compiles**

`upload-service/routes.go`:

```go
package main

import "github.com/gin-gonic/gin"

// registerRoutes wires health plus the authenticated /api/v1 group.
func registerRoutes(r *gin.Engine, h *Handler, d authDeps) {
	r.GET("/healthz", h.HandleHealth)

	api := r.Group("/api/v1")
	api.Use(authMiddleware(d))
	api.POST("/file/setCookie", h.HandleSetCookie)
	api.POST("/file/rooms/:roomId/upload/images", h.HandleUploadImages)
	api.POST("/file/rooms/:roomId/upload/file", h.HandleUploadFile)
	api.GET("/file/rooms/:roomId/file/:fileId", h.HandleDownloadFile)
	api.GET("/file-upload/:fileId/:fileName", h.HandleDownloadMinioS3File)

	// v3 serves the backward-compatible protected-image download for legacy message data from a separate (legacy) Drive backend.
	apiV3 := r.Group("/api/v3")
	apiV3.Use(authMiddleware(d))
	apiV3.GET("/rooms/:roomId/protected-image/:fileId", h.HandleDownloadProtectedImageV3)
}
```

In `upload-service/main.go`, add to the `config` struct after the `TLSSkipVerify` line:

```go
	// BotplatformURL is the LOCAL site's botplatform-service, used to validate
	// bot/admin session tokens. Required: without it, session-token callers
	// could not be authenticated at all.
	BotplatformURL string `env:"BOTPLATFORM_URL,required"`
	// BotEmailDomain, when set, gives session callers a synthesized
	// {account}@{domain} address for Drive's attribution field. Empty (default)
	// sends no email — nothing in this codebase reads the value back.
	BotEmailDomain string `env:"BOT_EMAIL_DOMAIN" envDefault:""`
```

Add imports `"github.com/hmchangw/chat/pkg/botauth"` and `"github.com/hmchangw/chat/pkg/restyutil"`, then replace the `registerRoutes` call (line 146) and add the validator construction just above it:

```go
	botValidator := botauth.NewValidator(
		restyutil.New("", restyutil.WithTimeout(5*time.Second)), cfg.BotplatformURL)
	registerRoutes(r, handler, authDeps{
		sso:            validator,
		bot:            botValidator,
		botEmailDomain: cfg.BotEmailDomain,
		devMode:        cfg.DevMode,
	})
```

Add to `upload-service/deploy/docker-compose.yml` after the `TLS_SKIP_VERIFY` line:

```yaml
      - BOTPLATFORM_URL=${BOTPLATFORM_URL:-http://botplatform-service:8080}
      # Empty: bot uploads send no email to Drive. Set to a domain only if Drive
      # rejects blank attribution.
      - BOT_EMAIL_DOMAIN=${BOT_EMAIL_DOMAIN:-}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: PASS — the new session tests plus every pre-existing SSO test unchanged.

- [ ] **Step 6: Verify it builds and commit**

```bash
make build SERVICE=upload-service
make fmt && make lint
git add upload-service/
git commit -m "feat(upload-service): accept bot session tokens alongside ssoToken"
```

---

### Task 4: upload-service — setCookie guard and Drive email policy

**Files:**
- Modify: `upload-service/handler.go:111-124` (setCookie), `:142-145` and `:221-224` (email guards)
- Test: `upload-service/handler_test.go`, `upload-service/handler_setcookie_test.go`

**Interfaces:**
- Consumes: `AuthenticatedUser.Session` (Task 3).
- Produces: no new exported symbols.

- [ ] **Step 1: Write the failing tests**

Add to `upload-service/handler_test.go` (add `"github.com/hmchangw/chat/pkg/principal"` to imports):

```go
// botUser is a session-authenticated caller: no directory metadata, and by
// default no email — the state a bot upload actually arrives in.
func botUser() *AuthenticatedUser {
	p := principal.Principal{UserID: "u1", Account: "alerts.sa.bot", SiteID: "site-a", Roles: []string{"bot"}}
	return &AuthenticatedUser{User: model.User{Account: p.Account, SiteID: p.SiteID}, Session: &p}
}

func TestUpload_SessionCaller_NoEmail_Succeeds(t *testing.T) {
	// An SSO caller with no email is a broken token (500); a session caller with
	// no email is the normal case, because bots have no directory record.
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alerts.sa.bot").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-a", nil)
	fd := &fakeDrive{uploadResp: []drive.UploadGroupImageResponse{{
		Status: driveStatusSuccess,
		File:   drive.GroupImageObject{GroupID: "r1", FileID: "f1", Filename: "a.png"},
	}}}
	h := newHandler(store, fd)

	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	c, w := newUploadCtx(t, "r1", body, ct, botUser())
	h.HandleUploadImages(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "alerts.sa.bot", fd.uploadGot.userID)
	assert.Empty(t, fd.uploadGot.email, "an unset BOT_EMAIL_DOMAIN sends no email")
	assert.Equal(t, "alerts.sa.bot", fd.uploadGot.username, "DisplayName falls back to the account")
}

func TestUpload_SessionCaller_SynthesizedEmail_ReachesDrive(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alerts.sa.bot").Return(true, nil)
	store.EXPECT().GetRoomSiteID(gomock.Any(), "r1").Return("site-a", nil)
	fd := &fakeDrive{uploadResp: []drive.UploadGroupImageResponse{{
		Status: driveStatusSuccess,
		File:   drive.GroupImageObject{GroupID: "r1", FileID: "f1", Filename: "a.png"},
	}}}
	h := newHandler(store, fd)

	u := botUser()
	u.Email = "alerts.sa.bot@bots.example.com"
	body, ct := multipartBody(t, "images", map[string][]byte{"a.png": []byte("x")})
	c, w := newUploadCtx(t, "r1", body, ct, u)
	h.HandleUploadImages(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "alerts.sa.bot@bots.example.com", fd.uploadGot.email)
}
```

In `upload-service/handler_setcookie_test.go`, the existing
`TestHandleSetCookie_Partitioned` (line 15) drives the handler with **no user in the
gin context** and asserts `200`. The guard added in Step 3 returns `500` for that
state, so both subtests would fail. In production this endpoint only ever runs behind
`authMiddleware`, so a userless context is unreachable — the test was exercising an
impossible state. Add the user to the existing test, right after `c.Request = req`
(line 32):

```go
			c.Set(ctxUserKey, okUser())
```

`okUser()` lives in `handler_test.go` (same package) and returns an SSO caller with
`Session == nil`, so the cookie path still runs and every existing assertion holds.

Then append the new test to the same file:

```go
func TestHandleSetCookie_SessionCaller_400(t *testing.T) {
	// setCookie exists only to let browser <img> downloads authenticate. A bot
	// sends headers, and issuing it a cookie would write an empty value.
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/file/setCookie", nil)
	p := principal.Principal{UserID: "u1", Account: "alerts.sa.bot", Roles: []string{"bot"}}
	c.Set(ctxUserKey, &AuthenticatedUser{User: model.User{Account: p.Account}, Session: &p})

	NewHandler(nil, nil, nil, 0, 0, 0, 0, nil, nil, 0, false, nil).HandleSetCookie(c)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Empty(t, w.Header().Get("Set-Cookie"), "no cookie may be issued to a session caller")
}
```

Check the existing imports in `handler_setcookie_test.go` and add whatever of `gin`, `httptest`, `model`, `principal` is missing.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `TestUpload_SessionCaller_NoEmail_Succeeds` gets 500 ("the user has no email provided"); `TestHandleSetCookie_SessionCaller_400` gets 200 with a `Set-Cookie`.

- [ ] **Step 3: Implement**

In `upload-service/handler.go`, replace `HandleSetCookie` (lines 108-124):

```go
// HandleSetCookie issues the (already auth-validated) ssoToken as a cross-site session
// cookie so the browser can authenticate <img>-driven downloads that cannot send headers.
// SameSite=None + Partitioned require the hand-built http.Cookie; c.SetCookie cannot set them.
func (h *Handler) HandleSetCookie(c *gin.Context) {
	ctx := logCtx(c)

	user, ok := userFromContext(c)
	if !ok {
		errhttp.Write(ctx, c, errcode.Internal("user not authenticated"))
		return
	}
	// Session callers send headers on every request; there is no ssoToken to
	// mirror into a cookie, so issuing one would store an empty value that
	// fails confusingly on the next download.
	if user.Session != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("setCookie requires an ssoToken; session-token callers send credentials as headers"))
		return
	}

	token := tokenFromRequest(c)
	// #nosec G124 -- SameSite=None is required for the cross-site <img> download flow; mitigated by Secure + HttpOnly (and Partitioned when SETCOOKIE_PARTITIONED is enabled).
	http.SetCookie(c.Writer, &http.Cookie{
		Name:        ssoTokenName,
		Value:       token,
		Path:        "/",
		HttpOnly:    true,
		Secure:      true,
		SameSite:    http.SameSiteNoneMode,
		Partitioned: h.setCookiePartitioned,
	})
	c.JSON(http.StatusOK, gin.H{"success": true})
}
```

Then relax the email guard in **both** `HandleUploadImages` (line 142) and `HandleUploadFile` (line 221) — identical replacement in each:

```go
	// A blank email on an SSO caller means a broken token, so it stays a
	// fail-fast. Session callers have no directory record and legitimately have
	// none unless BOT_EMAIL_DOMAIN is configured.
	if user.Email == "" && user.Session == nil {
		errhttp.Write(ctx, c, errcode.Internal("the user has no email provided"))
		return
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: PASS, including the pre-existing `TestUpload_NoEmail_500` (which uses `okUser()`, an SSO caller with `Session == nil`, so it still 500s).

- [ ] **Step 5: Commit**

```bash
make fmt && make lint
git add upload-service/
git commit -m "feat(upload-service): session-caller email policy and setCookie guard"
```

---

### Task 5: media-service — session auth middleware

**Files:**
- Create: `media-service/middleware_auth.go`
- Test: `media-service/middleware_auth_test.go`

**Interfaces:**
- Consumes: `botauth.Credentials`, `botauth.Authenticate`, `botauth.HasRole`, `botauth.TokenValidator` (Task 1).
- Produces:
  - `func requireSession(v botauth.TokenValidator) gin.HandlerFunc`
  - `func requireBotSelfOrAdmin() gin.HandlerFunc`
  - `func requireAdmin() gin.HandlerFunc`
  - `func sessionFromContext(c *gin.Context) *principal.Principal` — Task 6 uses this for the emoji audit field.
  - `const ctxSessionKey = "session_principal"`

- [ ] **Step 1: Write the failing tests**

Create `media-service/middleware_auth_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/principal"
)

// fakeSessionValidator implements botauth.TokenValidator for middleware tests.
type fakeSessionValidator struct {
	principal principal.Principal
	err       error
}

func (f *fakeSessionValidator) Validate(_ context.Context, _ string) (principal.Principal, error) {
	return f.principal, f.err
}

func botPrincipal(account string) principal.Principal {
	return principal.Principal{UserID: "u1", Account: account, SiteID: "site-a", Roles: []string{"bot"}}
}

func adminPrincipal() principal.Principal {
	return principal.Principal{UserID: "u1", Account: "p_admin", SiteID: "site-a", Roles: []string{"admin"}}
}

// runAvatarPUT drives the avatar-upload chain against /api/v1/avatar/bot/:botName.
func runAvatarPUT(t *testing.T, v botauth.TokenValidator, botName, userID, token string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.PUT("/api/v1/avatar/bot/:botName", requireSession(v), requireBotSelfOrAdmin(),
		func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodPut, "/api/v1/avatar/bot/"+botName, nil)
	if userID != "" {
		req.Header.Set(botauth.HeaderUserID, userID)
	}
	if token != "" {
		req.Header.Set(botauth.HeaderAuthToken, token)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// runEmojiPUT drives the emoji-upload chain, capturing the principal that reaches the handler.
func runEmojiPUT(t *testing.T, v botauth.TokenValidator, userID, token string) (*httptest.ResponseRecorder, *principal.Principal) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	var captured *principal.Principal
	r.PUT("/api/v1/emoji/:shortcode", requireSession(v), requireAdmin(), func(c *gin.Context) {
		captured = sessionFromContext(c)
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPut, "/api/v1/emoji/party", nil)
	if userID != "" {
		req.Header.Set(botauth.HeaderUserID, userID)
	}
	if token != "" {
		req.Header.Set(botauth.HeaderAuthToken, token)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w, captured
}

func reasonOf(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	return body.Reason
}

func TestRequireSession_Rejections(t *testing.T) {
	tests := []struct {
		name       string
		validator  botauth.TokenValidator
		userID     string
		token      string
		wantStatus int
		wantReason string
	}{
		{
			name:      "anonymous caller",
			validator: &fakeSessionValidator{principal: botPrincipal("a.bot")},
			// No headers at all — the case that was allowed through before this change.
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name:      "token but no user id",
			validator: &fakeSessionValidator{principal: botPrincipal("a.bot")},
			token:     "tok",
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name: "unknown token",
			validator: &fakeSessionValidator{err: errcode.Unauthenticated("nope",
				errcode.WithReason(errcode.BotplatformInvalidToken))},
			userID: "u1", token: "tok",
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name:      "user id disagrees with session",
			validator: &fakeSessionValidator{principal: botPrincipal("a.bot")},
			userID:    "someone-else", token: "tok",
			wantStatus: http.StatusUnauthorized, wantReason: "invalid_token",
		},
		{
			name:      "botplatform unreachable",
			validator: &fakeSessionValidator{err: errors.New("connection refused")},
			userID:    "u1", token: "tok",
			wantStatus: http.StatusServiceUnavailable, wantReason: "upstream_unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := runAvatarPUT(t, tt.validator, "a.bot", tt.userID, tt.token)
			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Equal(t, tt.wantReason, reasonOf(t, w))
		})
	}
}

func TestRequireBotSelfOrAdmin(t *testing.T) {
	tests := []struct {
		name       string
		principal  principal.Principal
		botName    string
		wantStatus int
	}{
		{name: "bot uploads its own avatar", principal: botPrincipal("a.bot"), botName: "a.bot", wantStatus: http.StatusOK},
		{name: "admin uploads any bot avatar", principal: adminPrincipal(), botName: "a.bot", wantStatus: http.StatusOK},
		{name: "bot cannot upload another bot avatar", principal: botPrincipal("a.bot"), botName: "b.bot", wantStatus: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := runAvatarPUT(t, &fakeSessionValidator{principal: tt.principal}, tt.botName, "u1", "tok")
			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantStatus == http.StatusForbidden {
				assert.Equal(t, "not_admin", reasonOf(t, w))
			}
		})
	}
}

func TestRequireAdmin_EmojiUpload(t *testing.T) {
	t.Run("admin allowed and principal reaches the handler", func(t *testing.T) {
		w, p := runEmojiPUT(t, &fakeSessionValidator{principal: adminPrincipal()}, "u1", "tok")
		require.Equal(t, http.StatusOK, w.Code)
		require.NotNil(t, p)
		assert.Equal(t, "p_admin", p.Account)
	})

	t.Run("bot refused: a shortcode is a site-wide shared name", func(t *testing.T) {
		w, p := runEmojiPUT(t, &fakeSessionValidator{principal: botPrincipal("a.bot")}, "u1", "tok")
		assert.Equal(t, http.StatusForbidden, w.Code)
		assert.Equal(t, "not_admin", reasonOf(t, w))
		assert.Nil(t, p)
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=media-service`
Expected: FAIL — `undefined: requireSession`, `undefined: requireBotSelfOrAdmin`, `undefined: requireAdmin`, `undefined: sessionFromContext`.

- [ ] **Step 3: Write the implementation**

Create `media-service/middleware_auth.go`:

```go
package main

import (
	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/principal"
)

// ctxSessionKey is the gin context key requireSession stores the validated
// principal under.
const ctxSessionKey = "session_principal"

// sessionFromContext returns the principal stored by requireSession, or nil.
func sessionFromContext(c *gin.Context) *principal.Principal {
	v, ok := c.Get(ctxSessionKey)
	if !ok {
		return nil
	}
	p, _ := v.(*principal.Principal)
	return p
}

// requireSession validates the x-user-id / x-auth-token pair against
// botplatform and stores the principal. Only the write endpoints sit behind it;
// the avatar and emoji GETs stay public, because the frontend loads them from
// <img src>, which cannot send headers.
func requireSession(v botauth.TokenValidator) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		userID, token := botauth.Credentials(c.Request.Header)
		p, err := botauth.Authenticate(ctx, v, userID, token)
		if err != nil {
			errhttp.Write(ctx, c, err)
			c.Abort()
			return
		}

		c.Set(ctxSessionKey, &p)
		c.Set("bot_account", p.Account)
		c.Next()
	}
}

// requireBotSelfOrAdmin authorizes a bot-avatar upload: the session must belong
// to the bot named in the path, or hold the admin role for provisioning. Runs
// after requireSession.
func requireBotSelfOrAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		p := sessionFromContext(c)
		if p == nil {
			errhttp.Write(ctx, c, errcode.Internal("avatar upload: missing principal"))
			c.Abort()
			return
		}
		if p.Account == c.Param("botName") || botauth.HasRole(*p, model.UserRoleAdmin) {
			c.Next()
			return
		}

		errhttp.Write(ctx, c, errcode.Forbidden("a bot may only upload its own avatar",
			errcode.WithReason(errcode.AdminNotAuthorized)))
		c.Abort()
	}
}

// requireAdmin authorizes an emoji upload. A shortcode is a site-wide shared
// name every user renders, so uploads are an admin operation — one bot must not
// be able to overwrite an emoji for the whole site. Runs after requireSession.
func requireAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		p := sessionFromContext(c)
		if p == nil {
			errhttp.Write(ctx, c, errcode.Internal("emoji upload: missing principal"))
			c.Abort()
			return
		}
		if !botauth.HasRole(*p, model.UserRoleAdmin) {
			errhttp.Write(ctx, c, errcode.Forbidden("admin role required",
				errcode.WithReason(errcode.AdminNotAuthorized)))
			c.Abort()
			return
		}

		c.Next()
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=media-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
make fmt && make lint
git add media-service/middleware_auth.go media-service/middleware_auth_test.go
git commit -m "feat(media-service): session auth middleware for write endpoints"
```

---

### Task 6: media-service — wire auth onto the PUTs

**Files:**
- Modify: `media-service/routes.go`, `media-service/main.go`, `media-service/config.go`
- Modify: `media-service/emoji_upload.go:138-140` (uploader from session)
- Modify: `media-service/middleware.go:28-40` (CORS headers)
- Modify: `media-service/deploy/docker-compose.yml`
- Test: `media-service/emoji_upload_test.go`

**Interfaces:**
- Consumes: `requireSession`, `requireBotSelfOrAdmin`, `requireAdmin`, `sessionFromContext` (Task 5); `botauth.NewValidator` (Task 1).
- Produces: `func registerRoutes(r *gin.Engine, h *handler, sessions botauth.TokenValidator)` (replaces the `(r, h)` signature).

> **Blast radius — read before starting.** `media-service/handler_test.go:46`'s
> `newEmojiTestRouter` calls `registerRoutes(r, h)`, and `newTestRouter` (line 21)
> delegates to it. Both the signature change AND the new auth middleware hit every
> existing router-driven test: **20 PUT call sites** across `upload_test.go` and
> `emoji_upload_test.go` would start returning `401`. All 20 funnel through one
> router helper and one request builder, so Step 1 fixes them in two edits rather
> than twenty. Do Step 1 first or the suite is unreadably red.

- [ ] **Step 1: Adapt the shared test helpers**

In `media-service/handler_test.go`, change `newEmojiTestRouter` (line 46) to wire a
permissive validator. An **admin** principal is the right default: it satisfies both
`requireBotSelfOrAdmin` and `requireAdmin`, so every pre-existing upload test keeps
exercising the handler rather than the middleware.

```go
	r := gin.New()
	// Existing tests exercise handlers, not auth. An admin session satisfies both
	// authorization rules, so the middleware stays transparent to them; the
	// middleware itself is covered by middleware_auth_test.go.
	registerRoutes(r, h, &fakeSessionValidator{principal: adminPrincipal()})
	return r, store, emojis, blobs
```

`fakeSessionValidator` and `adminPrincipal` come from `middleware_auth_test.go`
(Task 5) — same package, so no import is needed.

In `media-service/upload_test.go`, change `putReq` (line 26) to carry the credential:

```go
// putReq builds an authenticated PUT. The credential headers are always present
// because every PUT route now sits behind requireSession; tests that need an
// unauthenticated request build it inline instead.
func putReq(path string, body []byte, ct string) *http.Request {
	req := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", ct)
	req.Header.Set(botauth.HeaderUserID, "u1")
	req.Header.Set(botauth.HeaderAuthToken, "tok")
	return req
}
```

Add `"github.com/hmchangw/chat/pkg/botauth"` to that file's imports.

- [ ] **Step 2: Write the failing test for the uploader field**

In `media-service/emoji_upload_test.go`, update the existing success test's audit
assertions (lines 47-48) — the session, not the query param, is now the source:

```go
		assert.Equal(t, "p_admin", e.CreatedBy)
		assert.Equal(t, "p_admin", e.UpdatedBy)
```

Then append a test proving the query param can no longer spoof the audit trail:

```go
func TestEmojiUpload_UploaderComesFromSessionNotQueryParam(t *testing.T) {
	// ?uploader= is client-controlled; the authenticated session is not. The
	// audit fields must record the session and ignore the param entirely.
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().UpsertEmoji(gomock.Any(), gomock.Any()).DoAndReturn(func(_ any, e *model.CustomEmoji) error {
		assert.Equal(t, "p_admin", e.CreatedBy)
		assert.Equal(t, "p_admin", e.UpdatedBy)
		return nil
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/api/v1/emoji/party?uploader=spoofed", pngSized(t, 2, 2), "image/png"))

	assert.Equal(t, http.StatusOK, w.Code)
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `make test SERVICE=media-service`
Expected: FAIL — `CreatedBy` is `"spoofed"`, read from the query param, not `"p_admin"`.
(Step 1's helper edits will not compile until Step 5 changes `registerRoutes`' signature;
that is expected — this task's Red phase spans both files.)

- [ ] **Step 4: Take the uploader from the session**

In `media-service/emoji_upload.go`, replace lines 138-140:

```go
	// Audit fields come from the authenticated session, never from a
	// client-supplied ?uploader= (which is now accepted and ignored).
	uploader := ""
	if p := sessionFromContext(c); p != nil {
		uploader = p.Account
	}
	if len(uploader) > 64 {
		uploader = uploader[:64]
	}
```

- [ ] **Step 5: Wire the routes, config, CORS, and compose**

`media-service/routes.go`:

```go
package main

import (
	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/botauth"
)

// registerRoutes wires the public read endpoints plus the two session-gated
// writes. GETs stay public: the frontend loads them from <img src>, which
// cannot send credential headers.
func registerRoutes(r *gin.Engine, h *handler, sessions botauth.TokenValidator) {
	r.GET("/healthz", h.HandleHealth)
	r.GET("/api/v1/avatar/room/:roomID", h.HandleRoomAvatar)
	r.GET("/api/v1/avatar/:accountName", h.HandleAccountAvatar)
	r.GET("/api/v1/drive.members", h.HandleDriveMembers)
	r.GET("/api/v1/emoji/:shortcode", h.HandleEmojiGet)

	auth := requireSession(sessions)
	r.PUT("/api/v1/avatar/bot/:botName", auth, requireBotSelfOrAdmin(), h.HandleBotUpload)
	r.PUT("/api/v1/emoji/:shortcode", auth, requireAdmin(), h.HandleEmojiUpload)
}
```

In `media-service/config.go`, add to the `config` struct after `AdminAcctPrefix`:

```go
	// BotplatformURL is the LOCAL site's botplatform-service, used to validate
	// session tokens on the write endpoints. Required: the service must not be
	// able to start in a configuration that serves those endpoints anonymously.
	BotplatformURL string `env:"BOTPLATFORM_URL,required"`
```

In `media-service/main.go`, add imports `"github.com/hmchangw/chat/pkg/botauth"` and `"github.com/hmchangw/chat/pkg/restyutil"`, then replace the `registerRoutes(r, h)` call (line 96):

```go
	sessions := botauth.NewValidator(
		restyutil.New("", restyutil.WithTimeout(5*time.Second)), cfg.BotplatformURL)
	registerRoutes(r, h, sessions)
```

Confirm `"time"` is already imported in `main.go`; add it if not.

In `media-service/middleware.go`, update the CORS header line (line 32):

```go
		c.Header("Access-Control-Allow-Headers", "Content-Type, X-Request-ID, "+
			botauth.HeaderUserID+", "+botauth.HeaderAuthToken)
```

Add the `botauth` import to that file.

Add to `media-service/deploy/docker-compose.yml` after `EID_CACHE_CAPACITY`:

```yaml
      - BOTPLATFORM_URL=${BOTPLATFORM_URL:-http://botplatform-service:8080}
```

- [ ] **Step 6: Verify the whole service builds and tests pass**

Run: `make build SERVICE=media-service && make test SERVICE=media-service`
Expected: PASS. Any `registerRoutes(r, h)` call left in a test file must be updated to pass a `&fakeSessionValidator{}`.

- [ ] **Step 7: Commit**

```bash
make fmt && make lint
git add media-service/
git commit -m "feat(media-service): require session auth on avatar and emoji uploads"
```

---

### Task 7: Documentation

**Files:**
- Modify: `docs/client-api.md` (§2.4 at line 423, §7 at line 6744)
- Modify: `docs/client-api/request-reply.md` (HTTP sections at lines 51, 92-213)
- Modify: `docs/specs/media-service.md:31` (the auth table)

**Interfaces:**
- Consumes: the finished behavior from Tasks 3-6.
- Produces: nothing code-facing.

CLAUDE.md requires this in the same PR as the handler changes, and requires the derived view to stay in sync with the canonical doc.

- [ ] **Step 1: Update `docs/client-api.md` §2.4**

Rewrite the intro paragraph (line 425-432) so it describes both credentials: an OIDC `ssoToken` (header or cookie, header wins) **or** a botplatform session token as `x-user-id` + `x-auth-token`. State that sending both an `ssoToken` header and an `x-auth-token` header is `400 ambiguous_token`, and that an `x-auth-token` header takes precedence over an `ssoToken` cookie. Note that `POST /api/v1/file/setCookie` is SSO-only and returns `400` for a session caller.

For each of the six endpoints, add an `x-user-id` / `x-auth-token` row to the request field table alongside the existing `ssoToken` row, and add these rows to each error table:

| 400 | `bad_request` | `ambiguous_token` | `{ "code": "bad_request", "reason": "ambiguous_token", "error": "set exactly one of ssoToken / x-auth-token" }` |
| 401 | `unauthenticated` | `invalid_token` | `{ "code": "unauthenticated", "reason": "invalid_token", "error": "invalid session token" }` |
| 503 | `unavailable` | `upstream_unavailable` | `{ "code": "unavailable", "reason": "upstream_unavailable", "error": "botplatform unavailable" }` |

- [ ] **Step 2: Update `docs/client-api.md` §7**

Delete the `> [!WARNING]` block above `PUT /api/v1/avatar/bot/:botName` (line 6828-6830) — it is no longer true — and change `**Auth:** none (v1)` to:

```
**Auth:** `x-user-id` + `x-auth-token` (botplatform session, §10.1). The session must be the bot named in the path, or hold the `admin` role; otherwise `403 not_admin`.
```

For `PUT /api/v1/emoji/:shortcode`, replace "v1: no auth; the optional `?uploader={account}` query parameter is recorded for audit only" with: admin-only session auth, and `?uploader=` accepted but ignored — the audit fields come from the session. Add a `403 not_admin` row to both PUT response tables. Leave all three GET sections untouched.

- [ ] **Step 3: Mirror both changes into `docs/client-api/request-reply.md`**

The derived view carries all these endpoints under "HTTP — Connection & Auth" (line 51) and "Media Service — avatar/emoji endpoints" (lines 184, 200). Apply the same auth and error changes. Update the `<!-- last synced: client-api.md @ <sha> -->` marker at line 3 to the commit from Step 1. `events.md` needs no change — this adds no events.

- [ ] **Step 4: Update `docs/specs/media-service.md`**

Line 31's table cell reads **🔴 none (v1)**. Replace with `session token; bot-self or admin`. Update the prose at line 379 to match, and scan the file for any other claim that the endpoint is unauthenticated.

- [ ] **Step 5: Verify no stale claims remain**

Run: `grep -rn "no auth\|none (v1)\|UNAUTHENTICATED" docs/client-api.md docs/client-api/request-reply.md docs/specs/media-service.md`
Expected: no hits describing the two PUT endpoints. Hits for genuinely public endpoints (avatar/emoji GETs, client-update-service) are correct and must stay.

- [ ] **Step 6: Commit**

```bash
git add docs/
git commit -m "docs: bot session-token auth on upload-service and media-service"
```

---

### Task 8: Full verification

**Files:** none — this task only runs checks.

- [ ] **Step 1: Full test suite**

Run: `make test`
Expected: PASS across every service.

- [ ] **Step 2: Lint**

Run: `make lint`
Expected: 0 issues.

- [ ] **Step 3: SAST — a blocking CI gate**

Run: `make sast`
Expected: no medium-or-higher findings. A new outbound HTTP call and new auth branches are exactly what `gosec`/`semgrep` inspect. Suppress only genuine false positives, with `// #nosec <RULE> -- reason` directly above the statement.

- [ ] **Step 4: Confirm the required-config failure mode is real**

Run: `env -i PATH=$PATH go run ./media-service 2>&1 | head -5`
Expected: a config parse error naming `BOTPLATFORM_URL` (among other required vars) and a non-zero exit — proving no deployment can start media-service with the PUTs anonymous.

- [ ] **Step 5: Push**

```bash
git push -u origin claude/bot-account-http-api-auth-y5ig48
```

Retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s) on network errors only.

---

## Verification Checklist

- [ ] A bot session token authenticates all six upload-service endpoints (except setCookie, which rejects it by design).
- [ ] Every pre-existing SSO test in upload-service passes unmodified.
- [ ] Both media-service PUTs reject an anonymous caller with `401 invalid_token`.
- [ ] A bot cannot upload another bot's avatar (`403 not_admin`); an admin can.
- [ ] A bot cannot upload emoji (`403 not_admin`); an admin can.
- [ ] All three media-service GETs still work with no credentials.
- [ ] media-service refuses to start without `BOTPLATFORM_URL`.
- [ ] Emoji audit fields come from the session, not `?uploader=`.
- [ ] `auth-service/bpvalidator.go` is gone and auth-service's tests still pass.
- [ ] No new `errcode.Reason` constants were added.
- [ ] `make test`, `make lint`, and `make sast` are all clean.
