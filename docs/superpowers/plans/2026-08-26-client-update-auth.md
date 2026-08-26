# client-update-service Upload Authentication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gate `POST /api/v1/version` on a static service token held by the release pipeline, closing the write half of the documented "UNAUTHENTICATED in v1" hole. `GET /api/v1/version/:fileName` stays public by design.

**Architecture:** One Gin middleware added to the service's existing `middleware.go`. `requireServiceToken` compares an `X-Service-Token` header against a configured list of SHA-256 digests in constant time. The download route and every handler are untouched, so the service's dependency set stays exactly what it is today: MinIO.

**Tech Stack:** Go 1.25, Gin, `caarlos0/env` v11, `pkg/errcode`/`errhttp`, `crypto/subtle`, testify, `go.uber.org/mock`.

**Spec:** `docs/superpowers/specs/2026-08-26-client-update-auth-design.md`

## Global Constraints

- **TDD is mandatory** (CLAUDE.md §4): write the failing test, run it, confirm it fails, then implement. Never write implementation before its test.
- **Never run raw `go` commands** (CLAUDE.md §2). Use `make test SERVICE=client-update-service`, `make lint`, `make fmt`, `make sast`. The one sanctioned exception is the coverage pair in Task 6, which CLAUDE.md §4 names directly.
- **`-race` always** — the Makefile handles it.
- **Minimum 80% coverage**, target 90%+ for the middleware.
- Test files live in `package main`, same package as the code under test.
- **Never log tokens, passwords, or full message bodies** (CLAUDE.md §3). The service token must never reach a log line or an error body.
- Errors wrap with context: `fmt.Errorf("short description: %w", err)`. Never bare `err`, never `fmt.Errorf("error: %w", err)`.
- Client-facing errors go through `pkg/errcode` + `errhttp.Write`. **Never log AND return** — `errhttp.Write` runs `Classify`, which logs once.
- Config comes from env via `caarlos0/env` into the typed `config` struct. Never `os.Getenv` in service code.
- **`GET /api/v1/version/:fileName` must remain reachable with no credential.** This is a design requirement, not an omission — Task 4 asserts it explicitly.
- Every task ends with a commit.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `pkg/errcode/codes_clientupdate.go` | **Create** | The single new reason constant, `ClientUpdateInvalidServiceToken`. |
| `client-update-service/config.go` | Modify | Add `UploadTokens` and `normalizeUploadTokens`. |
| `client-update-service/config_test.go` | Modify | Cover the new var and every token-validation rule. |
| `client-update-service/middleware.go` | Modify | Add `serviceTokenHeader`, `uploadTokenDigests`, `requireServiceToken`, `rejectServiceToken`. |
| `client-update-service/middleware_test.go` | Modify | Table-driven tests for the guard. The two existing request-ID/access-log tests stay untouched — they are the regression guard. |
| `client-update-service/routes.go` | Modify | Attach the guard to POST only. |
| `client-update-service/main.go` | Modify | Validate tokens, build digests, pass them to `registerRoutes`. |
| `client-update-service/handler_test.go` | Modify | `registerRoutes` signature change (line 95) plus two new route tests. |
| `client-update-service/integration_test.go` | Modify | `registerRoutes` signature change (line 85). Nothing else — downloads are unchanged. |
| `client-update-service/deploy/docker-compose.yml` | Modify | The one new env var. |
| `docs/client-api.md` | Modify | §12 — narrow the warning, document the upload credential. |
| `docs/client-api/request-reply.md` | Modify | Derived view must not drift. |

`accessLogMiddleware` is **not** modified: with GET public there is no authenticated identity to log.

The middleware lives in the existing `middleware.go` rather than a new file — it is 48 lines and holds exactly this kind of cross-cutting request-scoped concern, matching `upload-service/middleware.go` and `admin-service/middleware.go`.

---

### Task 1: New error reason

**Files:**
- Create: `pkg/errcode/codes_clientupdate.go`
- Test: `client-update-service/middleware_test.go` (wire-value assertion lands in Task 3; this task's verification is compilation + lint)

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
  - `config.UploadTokens []string`
  - `normalizeUploadTokens(raw []string) ([]string, error)` — trims, drops empties, enforces the length floor, rejects duplicates. Task 4 calls it from `main.go`.
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

func TestConfig_ParsesUploadTokens(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")
	t.Setenv("MINIO_BUCKET", "chat-updates")
	t.Setenv("CLIENT_UPDATE_UPLOAD_TOKENS", "tok-a,tok-b")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, []string{"tok-a", "tok-b"}, cfg.UploadTokens)
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
git commit -m "feat(client-update-service): add upload-token configuration"
```

---

### Task 3: `requireServiceToken` — the upload guard

**Files:**
- Modify: `client-update-service/middleware.go`
- Test: `client-update-service/middleware_test.go`

**Interfaces:**
- Consumes: `errcode.ClientUpdateInvalidServiceToken` (Task 1).
- Produces:
  - `const serviceTokenHeader = "X-Service-Token"`
  - `uploadTokenDigests(tokens []string) [][sha256.Size]byte`
  - `requireServiceToken(digests [][sha256.Size]byte) gin.HandlerFunc`

  Task 4 calls both from `main.go` and `routes.go`.

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

Add to `client-update-service/middleware.go` (and add imports `context`, `crypto/sha256`, `crypto/subtle`, `strings`, `github.com/hmchangw/chat/pkg/errcode`, `github.com/hmchangw/chat/pkg/errcode/errhttp`):

```go
// serviceTokenHeader carries the upload credential. A dedicated header rather
// than Authorization: Bearer so a static shared secret can never be confused
// with the session conventions used elsewhere in the platform.
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

// requireServiceToken gates artifact upload on a static shared secret. Download
// is deliberately ungated — see the design doc; this guard is the write half only.
//
// The secret is replayable by anyone who observes a single request, so this
// endpoint MUST be served over TLS and the service MUST stay network-restricted —
// the token is a second layer, not a replacement for either.
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

### Task 4: Wire the guard onto the upload route

**Files:**
- Modify: `client-update-service/routes.go`
- Modify: `client-update-service/main.go`
- Modify: `client-update-service/handler_test.go:95`
- Modify: `client-update-service/integration_test.go:85`
- Test: `client-update-service/handler_test.go`

**Interfaces:**
- Consumes: `requireServiceToken`, `uploadTokenDigests` (Task 3); `normalizeUploadTokens` (Task 2).
- Produces: `registerRoutes(r *gin.Engine, h *Handler, uploadDigests [][sha256.Size]byte)`.

- [ ] **Step 1: Write the failing tests**

Add to `client-update-service/handler_test.go`:

```go
// testDigests is the digest list route tests authenticate against.
func testDigests() [][sha256.Size]byte {
	return uploadTokenDigests([]string{"0123456789abcdef0123456789abcdef"})
}

// The guard must run before the handler, so a middleware-ordering mistake cannot
// pass silently: the store mock has no expectations, so any store call fails.
func TestRoutes_UploadGuardRunsBeforeHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	r := gin.New()
	registerRoutes(r, h, testDigests())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/version", nil))

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// Downloads are public BY DESIGN. This asserts it so a future change cannot
// quietly gate them: reaching the handler (404 from the empty store, not 401)
// proves no credential was demanded.
func TestRoutes_DownloadNeedsNoCredential(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Open(gomock.Any(), objectKey("app.exe")).
		Return(nil, blobInfo{}, fmt.Errorf("stat object: %w", ErrObjectNotFound))
	h := NewHandler(store, testCache(1024))
	r := gin.New()
	registerRoutes(r, h, testDigests())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/version/app.exe", nil))

	assert.NotEqual(t, http.StatusUnauthorized, w.Code,
		"download must not require a credential")
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// The liveness probe must stay reachable with no credential — Kubernetes sends none.
func TestRoutes_HealthzNeedsNoCredential(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	r := gin.New()
	registerRoutes(r, h, testDigests())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	assert.Equal(t, http.StatusOK, w.Code)
}
```

Add `crypto/sha256` to `handler_test.go`'s imports (`fmt` is already imported).

The `Open` stub above matches `versionStore` in `store.go` — it returns `(io.ReadCloser, blobInfo, error)`, and the handler detects absence with `errors.Is(err, ErrObjectNotFound)`, so the sentinel must be wrapped rather than returned bare. This is the same form `TestHandleDownload_NotFound_404` uses in `version_test.go:255`.

Update the existing `registerRoutes(r, h)` call at `handler_test.go:95` to `registerRoutes(r, h, testDigests())`.

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

// registerRoutes wires the health probe plus the /api/v1 version endpoints.
// Upload takes a service token (the release pipeline); download is public by
// design — the artifact ships to every employee anyway, and the write is the
// surface worth protecting.
func registerRoutes(r *gin.Engine, h *Handler, uploadDigests [][sha256.Size]byte) {
	r.GET("/healthz", h.HandleHealth)

	api := r.Group("/api/v1")
	api.POST("/version", requireServiceToken(uploadDigests), h.HandleUpload)
	api.GET("/version/:fileName", h.HandleDownload)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS.

- [ ] **Step 5: Fix the integration test's call site**

Change `integration_test.go:85` from `registerRoutes(r, h)` to:

```go
	registerRoutes(r, h, uploadTokenDigests([]string{"0123456789abcdef0123456789abcdef"}))
```

Nothing else in that file changes: the only test that builds a router
(`TestIntegration_DownloadServesFromCacheOnSecondHit`, line 77) exercises download,
which needs no credential.

- [ ] **Step 6: Implement `main.go` wiring**

In `run()`, after `handler := NewHandler(store, cache)` and before `gin.SetMode`:

```go
	uploadTokens, err := normalizeUploadTokens(cfg.UploadTokens)
	if err != nil {
		return fmt.Errorf("validate upload tokens: %w", err)
	}
	uploadDigests := uploadTokenDigests(uploadTokens)
```

Change the `registerRoutes` call to:

```go
	registerRoutes(r, handler, uploadDigests)
```

And extend the startup log line to record the token **count**, never the values:

```go
		slog.Info("client-update-service starting", "addr", addr, "site", cfg.SiteID,
			"upload_tokens", len(uploadDigests))
```

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
git commit -m "feat(client-update-service): require a service token to upload artifacts"
```

---

### Task 5: Local dev config and documentation

**Files:**
- Modify: `client-update-service/deploy/docker-compose.yml`
- Modify: `docs/client-api.md` (§12, from line 8555)
- Modify: `docs/client-api/request-reply.md` ("HTTP — Client Update Service", from line 229)

**Interfaces:**
- Consumes: the env var name from Task 2 and the header name from Task 3.
- Produces: nothing consumed by code.

- [ ] **Step 1: Add the env var to docker-compose**

In the `environment:` list, after `MINIO_BUCKET`:

```yaml
      # Local dev only — a throwaway that satisfies the 32-char floor.
      - CLIENT_UPDATE_UPLOAD_TOKENS=${CLIENT_UPDATE_UPLOAD_TOKENS:-local-dev-upload-token-not-a-secret}
```

- [ ] **Step 2: Verify compose still parses**

Run: `docker compose -f client-update-service/deploy/docker-compose.yml config`
Expected: renders with the variable present and no error.

- [ ] **Step 3: Rewrite `docs/client-api.md` §12**

Narrow the `> [!WARNING]` block rather than deleting it — downloads are still open:

```markdown
> [!WARNING]
> **Downloads are UNAUTHENTICATED by design.** Anyone who can reach the service can
> download update artifacts. The service **MUST** remain network-restricted.
> Uploads require a service token (below), but that token is replayable if observed,
> so it is a second layer rather than a replacement for the network restriction.
```

Replace `**Auth:** none (v1)` under `### POST /api/v1/version` with:

```markdown
**Auth:** `X-Service-Token: <token>` — a static service token held by the release
pipeline. The service accepts any token in its configured list, so a token can be
rotated without downtime. Missing, empty, and unrecognized tokens all return the
same `401 invalid_service_token`.
```

Replace `**Auth:** none (v1)` under `### GET /api/v1/version/:fileName` with:

```markdown
**Auth:** none — public by design. Any caller that can reach the service may download.
```

The reword from "none (v1)" matters: "(v1)" implies a fix is pending, and after this
change it is not.

Add one row to the POST response table (2 columns):

| `401 Unauthorized` | Missing, empty, or unrecognized `X-Service-Token` (`invalid_service_token`). |

The GET response table is unchanged — downloads gain no new status codes.

- [ ] **Step 4: Update the derived view**

In `docs/client-api/request-reply.md`, replace the parenthetical in the section intro:

```markdown
Public HTTP endpoints served by `client-update-service` (`POST` requires a static
`X-Service-Token`; `GET` is public by design — the service must stay
network-restricted). Full request/response schemas and the download cache behavior
are in [../client-api.md §12](../client-api.md#12-client-update-service).
```

`docs/client-api/events.md` needs no change — no events are affected.

- [ ] **Step 5: Verify no stale claims remain**

Run: `grep -n "none (v1)\|no .ssoToken./auth in v1\|upload or download" docs/client-api.md docs/client-api/request-reply.md`
Expected: no hits referring to client-update-service. Hits for other services are fine.

- [ ] **Step 6: Commit**

```bash
git add client-update-service/deploy/docker-compose.yml docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs(client-update-service): document the upload service-token credential"
```

---

### Task 6: Full verification

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

Expected: `requireServiceToken`, `normalizeUploadTokens`, and `uploadTokenDigests` at 90%+; `total:` ≥80%. If any line is uncovered, add the missing case rather than lowering the bar. Delete `coverage.out` before committing — it is a build artifact.

- [ ] **Step 5: Integration suite**

Run: `make test-integration SERVICE=client-update-service`
Expected: PASS.

- [ ] **Step 6: SAST**

Run: `make sast`
Expected: PASS, no medium+ findings. This is a blocking CI gate. If `gosec` flags the constant-time comparison, fix the code — do not suppress without reviewer agreement.

- [ ] **Step 7: Secret-hygiene grep**

Run: `grep -rn "UploadTokens\|uploadTokens\|serviceTokenHeader" client-update-service/*.go | grep -i "slog\|Printf\|Println"`
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
- [ ] **`GET /api/v1/version/:fileName` is reachable with no credential**, asserted by a test.
- [ ] `GET /healthz` is reachable with no credential.
- [ ] The service refuses to start when `CLIENT_UPDATE_UPLOAD_TOKENS` is unset, empty, under 32 chars, or contains duplicates.
- [ ] No token value appears in any log line, error body, or startup message.
- [ ] `make lint`, `make test`, `make test-integration SERVICE=client-update-service`, and `make sast` all pass.
- [ ] `docs/client-api.md` §12 warns that downloads are public and network restriction is required, and documents the upload credential; the derived view matches.
