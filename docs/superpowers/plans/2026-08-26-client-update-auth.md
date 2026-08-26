# client-update-service Authentication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gate `client-update-service`'s two endpoints — an SSO session on `GET /api/v1/version/:fileName`, a static service token on `POST /api/v1/version` — replacing the documented "UNAUTHENTICATED in v1" hole.

**Architecture:** Two independent Gin middlewares in the service's existing `middleware.go`. `requireSSO` validates an `ssoToken` (header, then cookie) through `pkg/oidc`, ported from `upload-service`'s SSO branch with the session-token and Drive parts dropped. `requireServiceToken` compares an `X-Service-Token` header against a configured list of SHA-256 digests in constant time. Neither touches MongoDB, so the service's dependency set stays MinIO + TSSO.

**Tech Stack:** Go 1.25, Gin, `caarlos0/env` v11, `pkg/oidc`, `pkg/errcode`/`errhttp`, `crypto/subtle`, testify, `go.uber.org/mock`.

**Spec:** `docs/superpowers/specs/2026-08-26-client-update-auth-design.md`

## Global Constraints

- **TDD is mandatory** (CLAUDE.md §4): write the failing test, run it, confirm it fails, then implement. Never write implementation before its test.
- **Never run raw `go` commands** (CLAUDE.md §2). Use `make test SERVICE=client-update-service`, `make lint`, `make fmt`, `make sast`.
- **`-race` always** — the Makefile handles it.
- **Minimum 80% coverage**, target 90%+ for middleware.
- Test files live in `package main`, same package as the code under test.
- **Never log tokens, passwords, or full message bodies** (CLAUDE.md §3). The service token and the SSO token must never reach a log line or an error body.
- Errors wrap with context: `fmt.Errorf("short description: %w", err)`. Never bare `err`, never `fmt.Errorf("error: %w", err)`.
- Client-facing errors go through `pkg/errcode` + `errhttp.Write`. **Never log AND return** — `errhttp.Write` runs `Classify`, which logs once.
- Config comes from env via `caarlos0/env` into the typed `config` struct. Never `os.Getenv` in service code.
- Every task ends with a commit.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `pkg/errcode/codes_clientupdate.go` | **Create** | The single new reason constant, `ClientUpdateInvalidServiceToken`. |
| `client-update-service/config.go` | Modify | Add SSO + upload-token env vars and `normalizeUploadTokens`. |
| `client-update-service/config_test.go` | Modify | Cover the new vars and every token-validation rule. |
| `client-update-service/middleware.go` | Modify | Add `requireServiceToken`, `requireSSO`, `authDeps`, `tokenFromRequest`, `uploadTokenDigests`, `accountFromContext`; extend the access log with `account`. |
| `client-update-service/middleware_test.go` | Modify | Table-driven tests for both guards. The two existing request-ID/access-log tests stay untouched — they are the regression guard. |
| `client-update-service/routes.go` | Modify | Attach each guard to its route. |
| `client-update-service/main.go` | Modify | Build the OIDC validator and token digests, pass them to `registerRoutes`. |
| `client-update-service/handler_test.go` | Modify | `registerRoutes` signature change (line 95). |
| `client-update-service/integration_test.go` | Modify | `registerRoutes` signature change (line 85). |
| `client-update-service/deploy/docker-compose.yml` | Modify | The five new env vars for local dev. |
| `docs/client-api.md` | Modify | §12 — delete the warning block, document both credentials. |
| `docs/client-api/request-reply.md` | Modify | Derived view must not drift. |

Both middlewares live in the existing `middleware.go` rather than a new file: it is currently 48 lines and holds exactly this kind of cross-cutting request-scoped concern, matching `upload-service/middleware.go` and `admin-service/middleware.go`.

---

### Task 1: New error reason

**Files:**
- Create: `pkg/errcode/codes_clientupdate.go`
- Test: `client-update-service/middleware_test.go` (wire-value assertion lands here in Task 3; this task's verification is compilation + lint)

**Interfaces:**
- Consumes: nothing.
- Produces: `errcode.ClientUpdateInvalidServiceToken Reason` — wire value `"invalid_service_token"`. Task 3 uses it.

**Why no entry in `codes_test.go`'s `allReasons`:** that list is curated and already omits every `Admin*` and `Botplatform*` reason. Adding one service-specific reason while its two closest neighbours are absent would be inconsistent. The wire value gets a direct assertion in Task 3 instead.

- [ ] **Step 1: Create the reason file**

```go
package errcode

// Reason emitted by client-update-service's upload guard.
const (
	// ClientUpdateInvalidServiceToken: 401 on POST /api/v1/version when the
	// X-Service-Token header is missing, empty, or does not match a configured
	// token. All three collapse to this one reason so the wire cannot be used
	// to probe which part of the credential was wrong.
	ClientUpdateInvalidServiceToken Reason = "invalid_service_token"
)
```

Write it to `pkg/errcode/codes_clientupdate.go`.

- [ ] **Step 2: Verify it compiles and lints**

Run: `make lint`
Expected: PASS, no new findings.

- [ ] **Step 3: Commit**

```bash
git add pkg/errcode/codes_clientupdate.go
git commit -m "feat(errcode): add invalid_service_token reason for client-update uploads"
```

---

### Task 2: Configuration and token validation

**Files:**
- Modify: `client-update-service/config.go`
- Test: `client-update-service/config_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `config.DevMode bool`, `config.OIDCIssuerURL string`, `config.OIDCAudiences []string`, `config.TLSSkipVerify bool`, `config.UploadTokens []string`
  - `normalizeUploadTokens(raw []string) ([]string, error)` — trims, drops empties, enforces the length floor, rejects duplicates. Task 5 calls it from `main.go`.
  - `const minUploadTokenLen = 32`

`env.ParseAs` cannot express these rules: `caarlos0/env` treats a set-but-empty variable as present, and splitting `""` on a comma can yield a single empty element. Validation is therefore an explicit function, which also makes it directly testable.

- [ ] **Step 1: Write the failing tests**

Append to `client-update-service/config_test.go`:

```go
func TestNormalizeUploadTokens(t *testing.T) {
	const valid = "0123456789abcdef0123456789abcdef"  // exactly 32
	const valid2 = "fedcba9876543210fedcba9876543210" // exactly 32

	tests := []struct {
		name    string
		in      []string
		want    []string
		wantErr string
	}{
		{
			name: "single valid token",
			in:   []string{valid},
			want: []string{valid},
		},
		{
			name: "two valid tokens preserve order",
			in:   []string{valid, valid2},
			want: []string{valid, valid2},
		},
		{
			name: "surrounding whitespace trimmed",
			in:   []string{"  " + valid + "  "},
			want: []string{valid},
		},
		{
			name: "empty elements dropped",
			in:   []string{valid, "", "   "},
			want: []string{valid},
		},
		{
			name:    "nil input rejected",
			in:      nil,
			wantErr: "at least one",
		},
		{
			name:    "all-empty input rejected",
			in:      []string{"", "  "},
			wantErr: "at least one",
		},
		{
			name:    "short token rejected",
			in:      []string{"tooshort"},
			wantErr: "at least 32 characters",
		},
		{
			name:    "token one char under the floor rejected",
			in:      []string{valid[:31]},
			wantErr: "at least 32 characters",
		},
		{
			name:    "duplicate tokens rejected",
			in:      []string{valid, valid},
			wantErr: "duplicate",
		},
		{
			name:    "duplicate after trimming rejected",
			in:      []string{valid, " " + valid},
			wantErr: "duplicate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeUploadTokens(tt.in)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// The error must name the variable so an operator can act on it, and must never
// echo the token value — it reaches stderr on a failed boot.
func TestNormalizeUploadTokens_ErrorNeverLeaksTokenValue(t *testing.T) {
	const secret = "sup3rsecret-but-way-too-short"
	_, err := normalizeUploadTokens([]string{secret})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CLIENT_UPDATE_UPLOAD_TOKENS")
	assert.NotContains(t, err.Error(), secret)
}

func TestConfig_ParsesAuthVars(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")
	t.Setenv("MINIO_BUCKET", "chat-updates")
	t.Setenv("CLIENT_UPDATE_UPLOAD_TOKENS", "tok-a,tok-b")
	t.Setenv("OIDC_ISSUER_URL", "https://sso.example.com")
	t.Setenv("OIDC_AUDIENCES", "chat,desktop")
	t.Setenv("DEV_MODE", "true")
	t.Setenv("TLS_SKIP_VERIFY", "true")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, []string{"tok-a", "tok-b"}, cfg.UploadTokens)
	assert.Equal(t, "https://sso.example.com", cfg.OIDCIssuerURL)
	assert.Equal(t, []string{"chat", "desktop"}, cfg.OIDCAudiences)
	assert.True(t, cfg.DevMode)
	assert.True(t, cfg.TLSSkipVerify)
}

func TestConfig_AuthDefaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")
	t.Setenv("MINIO_BUCKET", "chat-updates")
	t.Setenv("CLIENT_UPDATE_UPLOAD_TOKENS", "tok-a")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.False(t, cfg.DevMode)
	assert.False(t, cfg.TLSSkipVerify)
	assert.Empty(t, cfg.OIDCIssuerURL)
	assert.Empty(t, cfg.OIDCAudiences)
}
```

Also add `"CLIENT_UPDATE_UPLOAD_TOKENS"` to the `required` slice in the existing `TestConfig_RequiresEachRequiredVar`, and add `t.Setenv("CLIENT_UPDATE_UPLOAD_TOKENS", "tok")` to `TestConfig_Defaults` so it still parses.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=client-update-service`
Expected: FAIL — `undefined: normalizeUploadTokens`, and `cfg.UploadTokens` undefined.

- [ ] **Step 3: Implement**

Replace `client-update-service/config.go` with:

```go
package main

import (
	"fmt"
	"strings"
	"time"
)

// minUploadTokenLen is the floor for a service token. pkg/sessiontoken.New()
// produces a 43-char base64url token, comfortably above it.
const minUploadTokenLen = 32

type config struct {
	Port   string `env:"PORT" envDefault:"8080"`
	SiteID string `env:"SITE_ID,required"`

	// DevMode bypasses OIDC validation on the download route. Never true in a
	// deployed environment — it accepts any non-empty ssoToken.
	DevMode bool `env:"DEV_MODE" envDefault:"false"`

	OIDCIssuerURL string   `env:"OIDC_ISSUER_URL"`
	OIDCAudiences []string `env:"OIDC_AUDIENCES" envSeparator:","`
	TLSSkipVerify bool     `env:"TLS_SKIP_VERIFY" envDefault:"false"`

	// UploadTokens are the service tokens accepted on POST /api/v1/version.
	// A list so a token can be rotated without a window where uploads fail:
	// add the new one, roll the caller, drop the old one. Validated by
	// normalizeUploadTokens — the env tag alone cannot express the rules.
	UploadTokens []string `env:"CLIENT_UPDATE_UPLOAD_TOKENS,required" envSeparator:","`

	MinioEndpoint        string        `env:"MINIO_ENDPOINT,required"`
	MinioAccessKey       string        `env:"MINIO_ACCESS_KEY,required"`
	MinioSecretKey       string        `env:"MINIO_SECRET_KEY,required"`
	MinioUseSSL          bool          `env:"MINIO_USE_SSL" envDefault:"false"`
	MinioBucket          string        `env:"MINIO_BUCKET,required"`
	MinioDownloadTimeout time.Duration `env:"MINIO_DOWNLOAD_TIMEOUT" envDefault:"5m"`

	HTTPWriteTimeout time.Duration `env:"HTTP_WRITE_TIMEOUT" envDefault:"10m"`

	CacheMaxEntries     int           `env:"CACHE_MAX_ENTRIES" envDefault:"4"`
	CacheTTL            time.Duration `env:"CACHE_TTL" envDefault:"24h"`
	CacheMaxObjectBytes int64         `env:"CACHE_MAX_OBJECT_BYTES" envDefault:"536870912"`
}

// normalizeUploadTokens trims each token, drops empty entries, and enforces the
// length floor and uniqueness. Errors name the variable but never the value —
// they reach stderr on a failed boot.
func normalizeUploadTokens(raw []string) ([]string, error) {
	out := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))

	for _, t := range raw {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if len(t) < minUploadTokenLen {
			return nil, fmt.Errorf("CLIENT_UPDATE_UPLOAD_TOKENS: every token must be at least %d characters", minUploadTokenLen)
		}
		if _, dup := seen[t]; dup {
			return nil, fmt.Errorf("CLIENT_UPDATE_UPLOAD_TOKENS: duplicate token entry")
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("CLIENT_UPDATE_UPLOAD_TOKENS: at least one token is required")
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client-update-service/config.go client-update-service/config_test.go
git commit -m "feat(client-update-service): add SSO and upload-token configuration"
```

---

### Task 3: `requireServiceToken` — the upload guard

**Files:**
- Modify: `client-update-service/middleware.go`
- Test: `client-update-service/middleware_test.go`

**Interfaces:**
- Consumes: `errcode.ClientUpdateInvalidServiceToken` (Task 1), `minUploadTokenLen` (Task 2).
- Produces:
  - `const serviceTokenHeader = "X-Service-Token"`
  - `uploadTokenDigests(tokens []string) [][sha256.Size]byte`
  - `requireServiceToken(digests [][sha256.Size]byte) gin.HandlerFunc`

  Task 5 calls both from `main.go` and `routes.go`.

**The two comparison rules that are the point of this task.** `subtle.ConstantTimeCompare` returns immediately when the two slices differ in length, leaking the configured token's length to an attacker timing responses — so both sides are hashed to fixed-width digests first. And the loop must not `break` on a match, or a token matching the first configured entry becomes measurably faster than one matching the last, leaking which slot it occupies.

- [ ] **Step 1: Write the failing tests**

Append to `client-update-service/middleware_test.go`:

```go
// The wire value is a contract a release pipeline branches on.
func TestClientUpdateInvalidServiceToken_WireValue(t *testing.T) {
	assert.Equal(t, "invalid_service_token", string(errcode.ClientUpdateInvalidServiceToken))
}

func TestRequireServiceToken(t *testing.T) {
	const tokA = "0123456789abcdef0123456789abcdef"
	const tokB = "fedcba9876543210fedcba9876543210"
	digests := uploadTokenDigests([]string{tokA, tokB})

	tests := []struct {
		name       string
		header     string
		setHeader  bool
		wantStatus int
	}{
		{name: "no header", setHeader: false, wantStatus: http.StatusUnauthorized},
		{name: "empty header", header: "", setHeader: true, wantStatus: http.StatusUnauthorized},
		{name: "whitespace header", header: "   ", setHeader: true, wantStatus: http.StatusUnauthorized},
		{name: "wrong token", header: "not-the-token-not-the-token-nope", setHeader: true, wantStatus: http.StatusUnauthorized},
		{name: "first configured token", header: tokA, setHeader: true, wantStatus: http.StatusOK},
		{name: "second configured token (rotation)", header: tokB, setHeader: true, wantStatus: http.StatusOK},
		{name: "strict prefix of a valid token", header: tokA[:20], setHeader: true, wantStatus: http.StatusUnauthorized},
		{name: "valid token with extra suffix", header: tokA + "x", setHeader: true, wantStatus: http.StatusUnauthorized},
		{name: "case-flipped token", header: strings.ToUpper(tokA), setHeader: true, wantStatus: http.StatusUnauthorized},
		{name: "same length, different content", header: strings.Repeat("z", len(tokA)), setHeader: true, wantStatus: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerCalled := false
			r := gin.New()
			r.POST("/api/v1/version", requireServiceToken(digests), func(c *gin.Context) {
				handlerCalled = true
				c.Status(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
			if tt.setHeader {
				req.Header.Set(serviceTokenHeader, tt.header)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Equal(t, tt.wantStatus == http.StatusOK, handlerCalled,
				"handler must run only when the token is accepted")
		})
	}
}

func TestRequireServiceToken_RejectionCarriesReason(t *testing.T) {
	digests := uploadTokenDigests([]string{"0123456789abcdef0123456789abcdef"})

	r := gin.New()
	r.POST("/api/v1/version", requireServiceToken(digests), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "invalid_service_token")
}

// A rejection body must not echo the presented credential, nor reveal anything
// about the configured one.
func TestRequireServiceToken_ResponseNeverLeaksTokens(t *testing.T) {
	const configured = "0123456789abcdef0123456789abcdef"
	const presented = "attacker-supplied-value-here-1234"
	digests := uploadTokenDigests([]string{configured})

	r := gin.New()
	r.POST("/api/v1/version", requireServiceToken(digests), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	req.Header.Set(serviceTokenHeader, presented)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.NotContains(t, w.Body.String(), configured)
	assert.NotContains(t, w.Body.String(), presented)
}

func TestUploadTokenDigests_OneDigestPerToken(t *testing.T) {
	const tokA = "0123456789abcdef0123456789abcdef"
	const tokB = "fedcba9876543210fedcba9876543210"

	got := uploadTokenDigests([]string{tokA, tokB})
	require.Len(t, got, 2)
	assert.Equal(t, sha256.Sum256([]byte(tokA)), got[0])
	assert.Equal(t, sha256.Sum256([]byte(tokB)), got[1])
	assert.NotEqual(t, got[0], got[1])
}
```

Add these imports to `middleware_test.go`: `crypto/sha256`, `strings`, `github.com/stretchr/testify/require`, `github.com/hmchangw/chat/pkg/errcode`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=client-update-service`
Expected: FAIL — `undefined: uploadTokenDigests`, `undefined: requireServiceToken`, `undefined: serviceTokenHeader`.

- [ ] **Step 3: Implement**

Add to `client-update-service/middleware.go` (and add imports `crypto/sha256`, `crypto/subtle`, `strings`, `github.com/hmchangw/chat/pkg/errcode`, `github.com/hmchangw/chat/pkg/errcode/errhttp`):

```go
// serviceTokenHeader carries the upload credential. A dedicated header rather
// than Authorization: Bearer so a static shared secret can never be confused
// with the SSO session convention used on the download route.
const serviceTokenHeader = "X-Service-Token"

// uploadTokenDigests hashes each configured token once at startup, so the raw
// secrets are not walked on every request.
func uploadTokenDigests(tokens []string) [][sha256.Size]byte {
	out := make([][sha256.Size]byte, 0, len(tokens))
	for _, t := range tokens {
		out = append(out, sha256.Sum256([]byte(t)))
	}
	return out
}

// requireServiceToken gates artifact upload on a static shared secret.
//
// The secret is replayable by anyone who observes a single request, so this
// endpoint MUST be served over TLS and MUST stay network-restricted — the token
// is a second layer, not a replacement for either.
//
// Both sides are hashed before comparing because subtle.ConstantTimeCompare
// returns early on a length mismatch, which would leak the configured token's
// length. The loop deliberately does not break on a match: an early return would
// make a token matching the first entry faster than one matching the last,
// leaking which slot it occupies.
func requireServiceToken(digests [][sha256.Size]byte) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Classify reads its logger from the ctx; without this the auth-failure
		// lines would be the only ones here carrying no request_id.
		ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

		presented := strings.TrimSpace(c.GetHeader(serviceTokenHeader))
		if presented == "" {
			rejectServiceToken(ctx, c)
			return
		}

		sum := sha256.Sum256([]byte(presented))
		match := 0
		for i := range digests {
			match |= subtle.ConstantTimeCompare(sum[:], digests[i][:])
		}
		if match != 1 {
			rejectServiceToken(ctx, c)
			return
		}

		c.Next()
	}
}

// rejectServiceToken writes the one envelope every upload rejection returns.
// Missing, empty and wrong all collapse to it so the wire cannot be used to
// probe which part of the credential was wrong.
func rejectServiceToken(ctx context.Context, c *gin.Context) {
	errhttp.Write(ctx, c, errcode.Unauthenticated("invalid service token",
		errcode.WithReason(errcode.ClientUpdateInvalidServiceToken)))
	c.Abort()
}
```

Add `"context"` to the imports.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS, all subtests green.

- [ ] **Step 5: Run the security scanners**

Run: `make sast`
Expected: PASS. `gosec` inspects `crypto/subtle` usage; if it flags the digest slicing, the fix is code, not a suppression — do not add `#nosec` here without confirming with the reviewer.

- [ ] **Step 6: Commit**

```bash
git add client-update-service/middleware.go client-update-service/middleware_test.go
git commit -m "feat(client-update-service): gate artifact upload on a service token"
```

---

### Task 4: `requireSSO` — the download guard

**Files:**
- Modify: `client-update-service/middleware.go`
- Test: `client-update-service/middleware_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `const ssoTokenName = "ssoToken"`, `const ctxAccountKey = "auth_account"`
  - `type TokenValidator interface { Validate(ctx context.Context, rawToken string) (pkgoidc.Claims, error) }`
  - `type authDeps struct { sso TokenValidator; devMode bool }`
  - `tokenFromRequest(c *gin.Context) string`
  - `requireSSO(d authDeps) gin.HandlerFunc`
  - `accountFromContext(c *gin.Context) string`

  Task 5 constructs `authDeps` in `main.go` and passes it to `registerRoutes`.

**Deviation from `upload-service` to preserve.** `upload-service` falls back to `claims.Name` when `preferred_username` is empty. `pkg/oidc`'s `Claims.Account()` documents the opposite — *"the only claim trusted as a principal; name is user-editable display data. Empty means callers must reject the token"* — so this service rejects. Follow the package's contract, not the call site that contradicts it.

- [ ] **Step 1: Write the failing tests**

Append to `client-update-service/middleware_test.go`:

```go
// stubValidator is a TokenValidator that returns canned results. No network.
type stubValidator struct {
	claims pkgoidc.Claims
	err    error
	calls  int
}

func (s *stubValidator) Validate(_ context.Context, _ string) (pkgoidc.Claims, error) {
	s.calls++
	return s.claims, s.err
}

func TestRequireSSO(t *testing.T) {
	okClaims := pkgoidc.Claims{PreferredUsername: "alice"}

	tests := []struct {
		name        string
		header      string
		cookie      string
		devMode     bool
		claims      pkgoidc.Claims
		validateErr error
		wantStatus  int
		wantAccount string
		wantCalls   int
	}{
		{
			name: "no header and no cookie", wantStatus: http.StatusUnauthorized, wantCalls: 0,
		},
		{
			name: "valid header token", header: "tok", claims: okClaims,
			wantStatus: http.StatusOK, wantAccount: "alice", wantCalls: 1,
		},
		{
			name: "cookie fallback when header absent", cookie: "tok", claims: okClaims,
			wantStatus: http.StatusOK, wantAccount: "alice", wantCalls: 1,
		},
		{
			name: "header wins over cookie", header: "tok", cookie: "other", claims: okClaims,
			wantStatus: http.StatusOK, wantAccount: "alice", wantCalls: 1,
		},
		{
			name: "expired token", header: "tok", validateErr: pkgoidc.ErrTokenExpired,
			wantStatus: http.StatusUnauthorized, wantCalls: 1,
		},
		{
			name: "invalid token", header: "tok", validateErr: errors.New("bad signature"),
			wantStatus: http.StatusUnauthorized, wantCalls: 1,
		},
		{
			name: "empty preferred_username rejected", header: "tok", claims: pkgoidc.Claims{Name: "Alice A"},
			wantStatus: http.StatusUnauthorized, wantCalls: 1,
		},
		{
			name: "dev mode bypasses the validator", header: "alice", devMode: true,
			wantStatus: http.StatusOK, wantAccount: "alice", wantCalls: 0,
		},
		{
			name: "dev mode still requires a token", devMode: true,
			wantStatus: http.StatusUnauthorized, wantCalls: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sv := &stubValidator{claims: tt.claims, err: tt.validateErr}

			var gotAccount string
			handlerCalled := false
			r := gin.New()
			r.GET("/api/v1/version/:fileName",
				requireSSO(authDeps{sso: sv, devMode: tt.devMode}),
				func(c *gin.Context) {
					handlerCalled = true
					gotAccount = accountFromContext(c)
					c.Status(http.StatusOK)
				})

			req := httptest.NewRequest(http.MethodGet, "/api/v1/version/app.exe", nil)
			if tt.header != "" {
				req.Header.Set(ssoTokenName, tt.header)
			}
			if tt.cookie != "" {
				req.AddCookie(&http.Cookie{Name: ssoTokenName, Value: tt.cookie})
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Equal(t, tt.wantStatus == http.StatusOK, handlerCalled)
			assert.Equal(t, tt.wantAccount, gotAccount)
			assert.Equal(t, tt.wantCalls, sv.calls, "validator call count")
		})
	}
}

func TestRequireSSO_ReasonsDistinguishExpiredFromInvalid(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason string
	}{
		{name: "expired", err: pkgoidc.ErrTokenExpired, wantReason: "sso_token_expired"},
		{name: "invalid", err: errors.New("bad signature"), wantReason: "invalid_sso_token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := gin.New()
			r.GET("/f", requireSSO(authDeps{sso: &stubValidator{err: tt.err}}), func(c *gin.Context) {
				c.Status(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/f", nil)
			req.Header.Set(ssoTokenName, "tok")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			assert.Equal(t, http.StatusUnauthorized, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantReason)
		})
	}
}

func TestRequireSSO_MissingCredentialReason(t *testing.T) {
	r := gin.New()
	r.GET("/f", requireSSO(authDeps{sso: &stubValidator{}}), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/f", nil))

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "missing_fields")
}

// The presented SSO token must never be echoed back to the caller.
func TestRequireSSO_ResponseNeverLeaksToken(t *testing.T) {
	const presented = "sso-token-value-should-not-appear"

	r := gin.New()
	r.GET("/f", requireSSO(authDeps{sso: &stubValidator{err: errors.New("bad signature")}}), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/f", nil)
	req.Header.Set(ssoTokenName, presented)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.NotContains(t, w.Body.String(), presented)
}
```

Add imports to `middleware_test.go`: `context`, `errors`, and `pkgoidc "github.com/hmchangw/chat/pkg/oidc"`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=client-update-service`
Expected: FAIL — `undefined: requireSSO`, `undefined: authDeps`, `undefined: accountFromContext`, `undefined: ssoTokenName`.

- [ ] **Step 3: Implement**

Add to `client-update-service/middleware.go`:

```go
// ssoTokenName is the shared header and cookie key for the SSO token.
const ssoTokenName = "ssoToken"

// ctxAccountKey is the gin context key requireSSO stores the resolved account under.
const ctxAccountKey = "auth_account"

// TokenValidator validates an SSO token and returns OIDC claims.
// Satisfied by *pkg/oidc.Validator.
type TokenValidator interface {
	Validate(ctx context.Context, rawToken string) (pkgoidc.Claims, error)
}

// authDeps is what requireSSO needs, as a struct rather than positional parameters.
type authDeps struct {
	sso     TokenValidator
	devMode bool
}

// accountFromContext returns the account stored by requireSSO, or "".
func accountFromContext(c *gin.Context) string { return c.GetString(ctxAccountKey) }

// tokenFromRequest returns the ssoToken from the header, falling back to the
// cookie. Header-first keeps an explicit credential ahead of ambient browser state.
func tokenFromRequest(c *gin.Context) string {
	if t := c.GetHeader(ssoTokenName); t != "" {
		return t
	}
	t, _ := c.Cookie(ssoTokenName)
	return t
}

// requireSSO admits a caller holding a valid SSO session. Downloads are
// user-facing, so this is the human credential — the upload route uses a
// service token instead.
func requireSSO(d authDeps) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

		token := tokenFromRequest(c)
		if token == "" {
			errhttp.Write(ctx, c, errcode.Unauthenticated("missing ssoToken",
				errcode.WithReason(errcode.AuthMissingFields)))
			c.Abort()
			return
		}

		account := token
		if !d.devMode {
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
			// pkg/oidc: preferred_username is the only claim trusted as a
			// principal, and empty means the token must be rejected. Unlike
			// upload-service, no fallback to the user-editable name claim.
			account = claims.Account()
			if account == "" {
				errhttp.Write(ctx, c, errcode.Unauthenticated("invalid sso token",
					errcode.WithReason(errcode.AuthInvalidToken)))
				c.Abort()
				return
			}
		}

		c.Request = c.Request.WithContext(ctx)
		c.Set(ctxAccountKey, account)
		c.Next()
	}
}
```

Add imports `errors` and `pkgoidc "github.com/hmchangw/chat/pkg/oidc"`.

Then extend `accessLogMiddleware` — add one field after `"client_ip"`:

```go
			"account", c.GetString(ctxAccountKey),
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS. The two pre-existing request-ID/access-log tests must still pass untouched — they are the regression guard.

- [ ] **Step 5: Commit**

```bash
git add client-update-service/middleware.go client-update-service/middleware_test.go
git commit -m "feat(client-update-service): require an SSO session for artifact download"
```

---

### Task 5: Wire the guards onto the routes

**Files:**
- Modify: `client-update-service/routes.go`
- Modify: `client-update-service/main.go`
- Modify: `client-update-service/handler_test.go:95`
- Modify: `client-update-service/integration_test.go:85`
- Test: `client-update-service/handler_test.go`

**Interfaces:**
- Consumes: `requireServiceToken`, `uploadTokenDigests` (Task 3); `requireSSO`, `authDeps` (Task 4); `normalizeUploadTokens` (Task 2).
- Produces: `registerRoutes(r *gin.Engine, h *Handler, d authDeps, uploadDigests [][sha256.Size]byte)`.

- [ ] **Step 1: Write the failing tests**

Add to `client-update-service/handler_test.go`:

```go
// testAuth is a permissive authDeps for route tests that are not about auth.
func testAuth() authDeps { return authDeps{devMode: true} }

// testDigests is the digest list route tests authenticate against.
func testDigests() [][sha256.Size]byte {
	return uploadTokenDigests([]string{"0123456789abcdef0123456789abcdef"})
}

// Both guards must run before the handler, so a middleware-ordering mistake
// cannot pass silently: the store mock has no expectations, so any call fails.
func TestRoutes_GuardsRunBeforeHandlers(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "upload without service token", method: http.MethodPost, path: "/api/v1/version"},
		{name: "download without sso token", method: http.MethodGet, path: "/api/v1/version/app.exe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
			r := gin.New()
			registerRoutes(r, h, testAuth(), testDigests())

			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(tt.method, tt.path, nil))

			assert.Equal(t, http.StatusUnauthorized, w.Code)
		})
	}
}

// The liveness probe must stay reachable with no credential — Kubernetes sends none.
func TestRoutes_HealthzNeedsNoCredential(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	r := gin.New()
	registerRoutes(r, h, testAuth(), testDigests())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	assert.Equal(t, http.StatusOK, w.Code)
}
```

Add `crypto/sha256` to `handler_test.go`'s imports.

Update the existing `registerRoutes(r, h)` call at `handler_test.go:95` to `registerRoutes(r, h, testAuth(), testDigests())`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=client-update-service`
Expected: FAIL — too many arguments to `registerRoutes`.

- [ ] **Step 3: Implement `routes.go`**

```go
package main

import (
	"crypto/sha256"

	"github.com/gin-gonic/gin"
)

// registerRoutes wires the health probe plus the two authenticated /api/v1
// version endpoints. Upload takes a service token (release pipeline), download
// takes an SSO session (a person) — the credentials are sized to what each
// route protects.
func registerRoutes(r *gin.Engine, h *Handler, d authDeps, uploadDigests [][sha256.Size]byte) {
	r.GET("/healthz", h.HandleHealth)

	api := r.Group("/api/v1")
	api.POST("/version", requireServiceToken(uploadDigests), h.HandleUpload)
	api.GET("/version/:fileName", requireSSO(d), h.HandleDownload)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS.

- [ ] **Step 5: Fix the integration test's call site**

Exactly one integration test builds the router: `TestIntegration_DownloadServesFromCacheOnSecondHit` (line 77). The other three exercise the store directly and need no change.

Replace its body's router setup and request loop (lines 84–92) with:

```go
	r := gin.New()
	registerRoutes(r, h, authDeps{devMode: true},
		uploadTokenDigests([]string{"0123456789abcdef0123456789abcdef"}))

	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		// devMode accepts any non-empty ssoToken, so this needs no TSSO container.
		req := httptest.NewRequest(http.MethodGet, "/api/v1/version/app.exe", nil)
		req.Header.Set(ssoTokenName, "integration-user")
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "BINARY", w.Body.String())
	}
```

The request had to be hoisted to a variable because it was previously constructed inline inside the `ServeHTTP` call, leaving nowhere to set the header.

- [ ] **Step 6: Implement `main.go` wiring**

In `run()`, after `handler := NewHandler(store, cache)` and before `gin.SetMode`:

```go
	uploadTokens, err := normalizeUploadTokens(cfg.UploadTokens)
	if err != nil {
		return fmt.Errorf("validate upload tokens: %w", err)
	}
	uploadDigests := uploadTokenDigests(uploadTokens)

	var ssoValidator TokenValidator
	if !cfg.DevMode {
		if cfg.OIDCIssuerURL == "" || len(cfg.OIDCAudiences) == 0 {
			return fmt.Errorf("OIDC_ISSUER_URL and OIDC_AUDIENCES are required when DEV_MODE is false")
		}
		v, err := pkgoidc.NewValidator(ctx, pkgoidc.Config{
			IssuerURL:     cfg.OIDCIssuerURL,
			Audiences:     cfg.OIDCAudiences,
			TLSSkipVerify: cfg.TLSSkipVerify,
		})
		if err != nil {
			return fmt.Errorf("create oidc validator: %w", err)
		}
		ssoValidator = v
	}
```

Change the `registerRoutes` call to:

```go
	registerRoutes(r, handler, authDeps{sso: ssoValidator, devMode: cfg.DevMode}, uploadDigests)
```

And extend the startup log line to record the token **count**, never the values:

```go
		slog.Info("client-update-service starting", "addr", addr, "site", cfg.SiteID,
			"upload_tokens", len(uploadDigests), "dev_mode", cfg.DevMode)
```

Add `pkgoidc "github.com/hmchangw/chat/pkg/oidc"` to `main.go`'s imports.

- [ ] **Step 7: Run the full unit suite and lint**

Run: `make test SERVICE=client-update-service && make lint`
Expected: PASS.

- [ ] **Step 8: Run the integration suite**

Run: `make test-integration SERVICE=client-update-service`
Expected: PASS (requires Docker).

- [ ] **Step 9: Commit**

```bash
git add client-update-service/routes.go client-update-service/main.go \
        client-update-service/handler_test.go client-update-service/integration_test.go
git commit -m "feat(client-update-service): wire auth guards onto the version routes"
```

---

### Task 6: Local dev config and documentation

**Files:**
- Modify: `client-update-service/deploy/docker-compose.yml`
- Modify: `docs/client-api.md` (§12, from line 8555)
- Modify: `docs/client-api/request-reply.md` ("HTTP — Client Update Service", from line 229)

**Interfaces:**
- Consumes: the env var names from Task 2 and the header names from Tasks 3–4.
- Produces: nothing consumed by code.

- [ ] **Step 1: Add the env vars to docker-compose**

In the `environment:` list, after `MINIO_BUCKET`:

```yaml
      # Local dev only. DEV_MODE bypasses OIDC so no TSSO is needed; the upload
      # token is a throwaway that satisfies the 32-char floor.
      - DEV_MODE=${DEV_MODE:-true}
      - OIDC_ISSUER_URL=${OIDC_ISSUER_URL:-}
      - OIDC_AUDIENCES=${OIDC_AUDIENCES:-}
      - TLS_SKIP_VERIFY=${TLS_SKIP_VERIFY:-false}
      - CLIENT_UPDATE_UPLOAD_TOKENS=${CLIENT_UPDATE_UPLOAD_TOKENS:-local-dev-upload-token-not-a-secret}
```

- [ ] **Step 2: Verify compose still parses**

Run: `docker compose -f client-update-service/deploy/docker-compose.yml config`
Expected: renders with the five variables present and no error.

- [ ] **Step 3: Rewrite `docs/client-api.md` §12**

Delete the whole `> [!WARNING]` block (the "UNAUTHENTICATED in v1" paragraph).

Replace `**Auth:** none (v1)` under `### POST /api/v1/version` with:

```markdown
**Auth:** `X-Service-Token: <token>` — a static service token held by the release
pipeline. The service accepts any token in its configured list, so a token can be
rotated without downtime. Missing, empty, and unrecognized tokens all return the
same `401 invalid_service_token`. This endpoint must be served over TLS and stay
network-restricted: a static token is replayable by anyone who observes a request.
```

Replace `**Auth:** none (v1)` under `### GET /api/v1/version/:fileName` with:

```markdown
**Auth:** `ssoToken` header, falling back to an `ssoToken` cookie. Any valid SSO
session may download.
```

Add to the POST response table:

| `401 Unauthorized` | Missing, empty, or unrecognized `X-Service-Token` (`invalid_service_token`). |

Add to the GET response table:

| `401 Unauthorized` | No `ssoToken` header or cookie (`missing_fields`); expired token (`sso_token_expired`); invalid token or empty `preferred_username` (`invalid_sso_token`). | |

- [ ] **Step 4: Update the derived view**

In `docs/client-api/request-reply.md`, replace the parenthetical in the section intro:

```markdown
Public HTTP endpoints served by `client-update-service` (`POST` takes a static
`X-Service-Token`; `GET` takes an `ssoToken` header or cookie). Full request/response
schemas and the download cache behavior are in
[../client-api.md §12](../client-api.md#12-client-update-service).
```

`docs/client-api/events.md` needs no change — no events are affected.

- [ ] **Step 5: Verify no stale claims remain**

Run: `grep -rn "UNAUTHENTICATED\|no .ssoToken./auth in v1\|none (v1)" docs/client-api.md docs/client-api/request-reply.md`
Expected: no hits referring to client-update-service. Hits for other services are fine.

- [ ] **Step 6: Commit**

```bash
git add client-update-service/deploy/docker-compose.yml docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs(client-update-service): document upload and download credentials"
```

---

### Task 7: Full verification

**Files:** none modified — this task only runs checks and fixes what they surface.

- [ ] **Step 1: Format**

Run: `make fmt`
Expected: clean; commit any reformatting.

- [ ] **Step 2: Lint**

Run: `make lint`
Expected: PASS, zero findings.

- [ ] **Step 3: Full unit suite, all services**

Run: `make test`
Expected: PASS. This catches any other caller of `registerRoutes` or of the errcode catalog.

- [ ] **Step 4: Coverage floor**

The Makefile has no per-service coverage target (`coverage-loadgen-*` are loadgen-only), and CLAUDE.md §4 names these two commands directly for verifying coverage, so they are the sanctioned exception to the make-only rule:

```bash
go test -race -coverprofile=coverage.out ./client-update-service/...
go tool cover -func=coverage.out | grep -E "middleware.go|config.go|total:"
```

Expected: `requireServiceToken`, `requireSSO`, `normalizeUploadTokens`, `uploadTokenDigests`, and `tokenFromRequest` at 90%+; `total:` ≥80%. If any line is uncovered, add the missing case rather than lowering the bar. Delete `coverage.out` before committing — it is a build artifact.

- [ ] **Step 5: Integration suite**

Run: `make test-integration SERVICE=client-update-service`
Expected: PASS.

- [ ] **Step 6: SAST**

Run: `make sast`
Expected: PASS, no medium+ findings. This is a blocking CI gate. If `gosec` flags the constant-time comparison, fix the code — do not suppress without reviewer agreement.

- [ ] **Step 7: Secret-hygiene grep**

Run: `grep -rn "UploadTokens\|uploadTokens\|serviceTokenHeader\|ssoTokenName" client-update-service/*.go | grep -i "slog\|Printf\|Println"`
Expected: no hits. No token value or header value may reach a log line.

- [ ] **Step 8: Commit any fixes**

```bash
git add -A
git commit -m "chore(client-update-service): address lint, coverage and SAST findings"
```

---

## Verification Checklist

- [ ] `POST /api/v1/version` returns 401 with `invalid_service_token` for a missing, empty, or wrong `X-Service-Token`.
- [ ] `POST /api/v1/version` succeeds with **any** token in the configured list, proving rotation works.
- [ ] Token comparison hashes both sides and does not return early on a match.
- [ ] `GET /api/v1/version/:fileName` returns 401 without an `ssoToken`, and distinguishes `sso_token_expired` from `invalid_sso_token`.
- [ ] The `ssoToken` cookie is accepted when the header is absent; the header wins when both are present.
- [ ] An SSO token with an empty `preferred_username` is rejected — no fallback to `claims.Name`.
- [ ] `GET /healthz` is reachable with no credential.
- [ ] The service refuses to start when `CLIENT_UPDATE_UPLOAD_TOKENS` is unset, empty, under 32 chars, or contains duplicates.
- [ ] The service refuses to start when `DEV_MODE=false` and OIDC config is absent.
- [ ] No token value appears in any log line, error body, or startup message.
- [ ] `make lint`, `make test`, `make test-integration SERVICE=client-update-service`, and `make sast` all pass.
- [ ] `docs/client-api.md` §12 and `docs/client-api/request-reply.md` carry the real auth contract, with no "UNAUTHENTICATED in v1" text remaining.
