# Admin-Driven Client Update Uploads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gate `client-update-service`'s upload endpoint behind a service-account token, and give admins a supported way to publish client update artifacts through an authenticated, audited streaming relay in `admin-service` driven by a new console in `admin-frontend`.

**Architecture:** Two independent credentials in one chain — the browser holds an admin session, `admin-service` holds a service-account bearer token, and only `admin-service` may call `POST /api/v1/version`. `admin-service` re-encodes the multipart stream through an `io.Pipe` into a resty request, and extends only its own request deadlines rather than raising the server-wide timeouts that double as a config validation ceiling. (This line originally claimed `admin-service` never buffers the artifact. It does: resty v2.17.2 reads an `io.Reader` body into memory before dialling — see the design record §2.3 correction.)

**Tech Stack:** Go 1.25, Gin, resty v2, `caarlos0/env` v11, `go.uber.org/mock`, testify, React 19 + Vite + vitest.

**Spec:** `docs/superpowers/specs/2026-08-26-client-update-admin-upload-design.md`

## Global Constraints

- Never run raw `go` commands — always `make` targets (CLAUDE.md §2).
- TDD is mandatory: write the failing test, **run it and confirm it fails**, then implement (CLAUDE.md §4).
- All tests run with `-race` (the Makefile handles it).
- Minimum 80% coverage; 90%+ on handlers and middleware.
- Client-facing errors use `pkg/errcode` + `errhttp.Write`; infra failures return raw `fmt.Errorf("…: %w", err)` and collapse to `internal` (CLAUDE.md §3).
- Never log tokens. `AuditEntry.Details` carries non-secret context only.
- Never default a secret in `envDefault`; mark it `required` and fail fast.
- Structured `log/slog` only, never `fmt.Println`.
- Docs: any change to a documented HTTP surface updates `docs/client-api.md` **and** its derived view `docs/client-api/request-reply.md` in the same PR.
- Pre-commit hook runs lint + tests. `make sast` must pass before pushing.

## Verified Facts (do not re-litigate)

These were checked against source while writing the spec. They are load-bearing:

| Fact | Evidence |
|---|---|
| `http.NewResponseController(c.Writer)` reaches the real conn | gin v1.12.0 `response_writer.go:57` defines `Unwrap()`; neither `o11ygin` v0.11.0 nor `otelgin` v0.68.0 replaces `c.Writer` |
| resty streams an `io.Reader` body | resty v2.17.2 `middleware.go:226` |
| resty **buffers the whole body** if `SetContentLength` is on | resty v2.17.2 `middleware.go:519-527` — never enable it here |
| `errcode.Unauthenticated` → 401, `errcode.Unavailable` → 503 | `pkg/errcode/category.go:34,44` |
| `caarlos0/env` v11 parses `map[string]string` | `pkg/obs/obs.go:86` uses `envKeyValSeparator` |

---

## File Structure

**`client-update-service/`** (auth on the upload endpoint)
- `config.go` — add `UploadTokens` + `validateUploadTokens`
- `middleware.go` — add `bearer`, `lookupAccount`, `requireServiceAccount`
- `routes.go` — gate POST only
- `main.go` — validate config, pass tokens to `registerRoutes`
- `config_test.go`, `middleware_test.go`, `handler_test.go`, `integration_test.go`
- `deploy/docker-compose.yml`

**`admin-service/`** (the relay)
- `client_update.go` — **new**: config plumbing, `versionUploader`, the resty impl, error mapping, the handler, the relay goroutine
- `client_update_test.go` — **new**
- `config.go` — add three env vars + validation
- `handler.go` — add the `uploader` field + a variadic option
- `routes.go` — one route
- `main.go` — build the resty client, pass the option
- `deploy/docker-compose.yml`

**`admin-frontend/`**
- `src/api/admin/index.ts` — add `uploadClientVersion`
- `src/components/UpdatesConsole/{UpdatesPage.jsx,index.jsx,style.css,UpdatesPage.test.jsx}` — **new**
- `src/components/AppShell/AppShell.jsx` — one nav entry

**`docs/`** — `client-api.md` §9.16 + §12, `client-api/request-reply.md`

---

## Task 1: `client-update-service` — token config

**Files:**
- Modify: `client-update-service/config.go`
- Test: `client-update-service/config_test.go`

**Interfaces:**
- Consumes: nothing
- Produces: `config.UploadTokens map[string]string`; `validateUploadTokens(tokens map[string]string) error`

- [ ] **Step 1: Write the failing tests**

Append to `client-update-service/config_test.go`:

```go
func TestConfig_ParsesUploadTokens(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")
	t.Setenv("MINIO_BUCKET", "chat-updates")
	t.Setenv("UPLOAD_TOKENS", "admin-service:0123456789abcdef,ops-cli:fedcba9876543210")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"admin-service": "0123456789abcdef",
		"ops-cli":       "fedcba9876543210",
	}, cfg.UploadTokens)
}

func TestValidateUploadTokens(t *testing.T) {
	tests := []struct {
		name    string
		tokens  map[string]string
		wantErr bool
	}{
		{"one valid entry", map[string]string{"admin-service": "0123456789abcdef"}, false},
		{"two valid entries", map[string]string{"a": "0123456789abcdef", "b": "fedcba9876543210"}, false},
		{"empty map", map[string]string{}, true},
		{"empty account name", map[string]string{"": "0123456789abcdef"}, true},
		{"empty token", map[string]string{"admin-service": ""}, true},
		{"token under 16 chars", map[string]string{"admin-service": "short"}, true},
		{"token exactly 16 chars", map[string]string{"admin-service": "0123456789abcdef"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUploadTokens(tt.tokens)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestValidateUploadTokens_ErrorNeverLeaksTheToken(t *testing.T) {
	const secret = "supersecrettoken0123"
	err := validateUploadTokens(map[string]string{"": secret})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret,
		"a config error must never carry the token value — it reaches the logs")
}
```

Also extend the existing `TestConfig_RequiresEachRequiredVar` `required` slice to include `"UPLOAD_TOKENS"`, and add `t.Setenv("UPLOAD_TOKENS", "admin-service:0123456789abcdef")` to `TestConfig_Defaults` so it keeps passing.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=client-update-service`
Expected: FAIL — `cfg.UploadTokens` undefined, `validateUploadTokens` undefined.

- [ ] **Step 3: Implement**

In `client-update-service/config.go`, add the import `"fmt"` and this field to `config`:

```go
	// UploadTokens authorizes POST /api/v1/version, as account->token. Required:
	// an unset value would leave the upload endpoint open. Neither "," nor ":"
	// may appear in a token — both are separators, and a value containing one
	// splits into an entry that validateUploadTokens rejects.
	UploadTokens map[string]string `env:"UPLOAD_TOKENS,required" envSeparator:"," envKeyValSeparator:":"`
```

Append to the same file:

```go
// minUploadTokenLen rejects a token short enough to be brute-forced or to be a
// placeholder left in a deploy manifest.
const minUploadTokenLen = 16

// validateUploadTokens fails fast on a token table that would authorize nothing
// or, worse, authorize the empty string. Error text names the account only —
// never the token, which would reach the logs.
func validateUploadTokens(tokens map[string]string) error {
	if len(tokens) == 0 {
		return fmt.Errorf("UPLOAD_TOKENS must define at least one account:token pair")
	}
	for account, token := range tokens {
		if account == "" {
			return fmt.Errorf("UPLOAD_TOKENS has an entry with an empty account name")
		}
		if len(token) < minUploadTokenLen {
			return fmt.Errorf("UPLOAD_TOKENS entry %q: token must be at least %d characters", account, minUploadTokenLen)
		}
	}
	return nil
}
```

The empty-token case is covered by the length check, so it needs no separate branch.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client-update-service/config.go client-update-service/config_test.go
git commit -m "feat(client-update-service): required service-account token table"
```

---

## Task 2: `client-update-service` — the auth middleware

**Files:**
- Modify: `client-update-service/middleware.go`
- Test: `client-update-service/middleware_test.go`

**Interfaces:**
- Consumes: `validateUploadTokens` (Task 1) — not called here, but the same map shape
- Produces: `const ctxServiceAccount = "service_account"`; `bearer(c *gin.Context) string`; `lookupAccount(tokens map[string]string, tok string) (string, bool)`; `requireServiceAccount(tokens map[string]string) gin.HandlerFunc`

- [ ] **Step 1: Write the failing tests**

Append to `client-update-service/middleware_test.go`:

```go
func TestLookupAccount(t *testing.T) {
	tokens := map[string]string{
		"admin-service": "0123456789abcdef",
		"ops-cli":       "fedcba9876543210",
	}
	tests := []struct {
		name        string
		token       string
		wantAccount string
		wantOK      bool
	}{
		{"first account", "0123456789abcdef", "admin-service", true},
		{"second account", "fedcba9876543210", "ops-cli", true},
		{"unknown token", "not-a-real-token", "", false},
		{"empty token", "", "", false},
		{"proper prefix of a valid token", "0123456789abcde", "", false},
		{"valid token plus a suffix", "0123456789abcdefX", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, ok := lookupAccount(tokens, tt.token)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantAccount, account)
		})
	}
}

func TestRequireServiceAccount(t *testing.T) {
	tokens := map[string]string{"admin-service": "0123456789abcdef"}
	tests := []struct {
		name       string
		authHeader string
		wantStatus int
		wantAcct   string
	}{
		{"valid bearer", "Bearer 0123456789abcdef", http.StatusOK, "admin-service"},
		{"valid bearer with padding", "Bearer   0123456789abcdef  ", http.StatusOK, "admin-service"},
		{"no header", "", http.StatusUnauthorized, ""},
		{"unknown token", "Bearer nope-nope-nope-nope", http.StatusUnauthorized, ""},
		{"empty token after prefix", "Bearer ", http.StatusUnauthorized, ""},
		{"basic scheme", "Basic 0123456789abcdef", http.StatusUnauthorized, ""},
		{"lowercase bearer scheme", "bearer 0123456789abcdef", http.StatusUnauthorized, ""},
		{"bare token, no scheme", "0123456789abcdef", http.StatusUnauthorized, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seenAccount string
			var handlerRan bool
			r := gin.New()
			r.Use(requireServiceAccount(tokens))
			r.POST("/api/v1/version", func(c *gin.Context) {
				handlerRan = true
				seenAccount = c.GetString(ctxServiceAccount)
				c.Status(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Equal(t, tt.wantAcct, seenAccount)
			assert.Equal(t, tt.wantStatus == http.StatusOK, handlerRan,
				"the handler must run only when the credential is accepted")
		})
	}
}

// Every rejection must be byte-identical, so a caller cannot tell an unknown
// token from a malformed header and probe the token table.
func TestRequireServiceAccount_RejectionsAreIndistinguishable(t *testing.T) {
	tokens := map[string]string{"admin-service": "0123456789abcdef"}
	bodies := make([]string, 0, 3)
	for _, hdr := range []string{"", "Basic x", "Bearer wrong-token-value-here"} {
		r := gin.New()
		r.Use(requireServiceAccount(tokens))
		r.POST("/api/v1/version", func(c *gin.Context) { c.Status(http.StatusOK) })

		req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
		if hdr != "" {
			req.Header.Set("Authorization", hdr)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		bodies = append(bodies, w.Body.String())
	}
	assert.Equal(t, bodies[0], bodies[1])
	assert.Equal(t, bodies[1], bodies[2])
}

func TestRequireServiceAccount_NeverEchoesTheToken(t *testing.T) {
	const secret = "0123456789abcdef"
	r := gin.New()
	r.Use(requireServiceAccount(map[string]string{"admin-service": secret}))
	r.POST("/api/v1/version", func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.NotContains(t, w.Body.String(), secret)
	assert.NotContains(t, w.Body.String(), "wrong", "the rejection must not echo the presented token either")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=client-update-service`
Expected: FAIL — `lookupAccount`, `requireServiceAccount`, `ctxServiceAccount` undefined.

- [ ] **Step 3: Implement**

Add to `client-update-service/middleware.go`'s imports: `"crypto/subtle"`, `"net/http"` is not needed, `"strings"`, plus `"github.com/hmchangw/chat/pkg/errcode"` and `"github.com/hmchangw/chat/pkg/errcode/errhttp"`.

```go
// ctxServiceAccount holds the authenticated caller's account name, set by
// requireServiceAccount and read by accessLogMiddleware.
const ctxServiceAccount = "service_account"

// bearer extracts the token from "Authorization: Bearer <token>", mirroring
// admin-service/middleware.go so the two services agree on the header shape.
// Returns "" when absent or when the scheme differs.
func bearer(c *gin.Context) string {
	h := c.GetHeader("Authorization")
	if after, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(after)
	}
	return ""
}

// lookupAccount resolves a presented token to its account. It compares against
// every entry with no early break, so response timing cannot reveal which
// account a guessed token is closest to. (ConstantTimeCompare short-circuits on
// a length mismatch; that length leak is inherent and accepted.)
func lookupAccount(tokens map[string]string, tok string) (string, bool) {
	if tok == "" {
		return "", false
	}
	var found string
	for account, want := range tokens {
		if subtle.ConstantTimeCompare([]byte(tok), []byte(want)) == 1 {
			found = account
		}
	}
	return found, found != ""
}

// requireServiceAccount gates a route on a configured service-account token.
// Missing, malformed and unknown credentials all produce one identical 401, so
// the endpoint cannot be used to probe the token table.
func requireServiceAccount(tokens map[string]string) gin.HandlerFunc {
	return func(c *gin.Context) {
		account, ok := lookupAccount(tokens, bearer(c))
		if !ok {
			errhttp.Write(c.Request.Context(), c,
				errcode.Unauthenticated("invalid or missing service account token"))
			c.Abort()
			return
		}
		c.Set(ctxServiceAccount, account)
		c.Next()
	}
}
```

Add the account to the access log — in `accessLogMiddleware`'s `slog.Info` call, after `"client_ip"`:

```go
			"service_account", c.GetString(ctxServiceAccount),
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client-update-service/middleware.go client-update-service/middleware_test.go
git commit -m "feat(client-update-service): constant-time service-account bearer auth"
```

---

## Task 3: `client-update-service` — wire the route

**Files:**
- Modify: `client-update-service/routes.go`, `client-update-service/main.go`, `client-update-service/deploy/docker-compose.yml`
- Test: `client-update-service/handler_test.go`

**Interfaces:**
- Consumes: `requireServiceAccount` (Task 2), `validateUploadTokens` (Task 1)
- Produces: `registerRoutes(r *gin.Engine, h *Handler, uploadTokens map[string]string)` — note the new third parameter

- [ ] **Step 1: Write the failing tests**

Append to `client-update-service/handler_test.go`:

```go
// testTokens is the token table every route-level test authenticates against.
func testTokens() map[string]string {
	return map[string]string{"admin-service": "0123456789abcdef"}
}

func TestRoutes_UploadRequiresServiceAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	// No EXPECT() at all: gomock fails the test if Put is called, which is
	// exactly the assertion — an unauthenticated upload must never reach MinIO.
	h := NewHandler(store, testCache(1024))

	r := gin.New()
	registerRoutes(r, h, testTokens())

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "config"},
		"executeFile": {name: "app.exe", content: "bin!"},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	req.Header.Set("Content-Type", ct)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestRoutes_UploadSucceedsWithServiceAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Put(gomock.Any(), objectKey("app.yaml"), gomock.Any(), int64(6), "application/x-yaml").Return(nil)
	store.EXPECT().Put(gomock.Any(), objectKey("app.exe"), gomock.Any(), int64(4), "application/octet-stream").Return(nil)
	h := NewHandler(store, testCache(1024))

	r := gin.New()
	registerRoutes(r, h, testTokens())

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "config"},
		"executeFile": {name: "app.exe", content: "bin!"},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Authorization", "Bearer 0123456789abcdef")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// The client fleet pulling updates holds no credential, so downloads stay open.
// This pins that asymmetry: gating GET would break every deployed client.
func TestRoutes_DownloadStaysUnauthenticated(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Open(gomock.Any(), objectKey("app.yaml")).
		Return(rc("config"), blobInfo{Size: 6, ContentType: "application/x-yaml"}, nil)
	h := NewHandler(store, testCache(1024))

	r := gin.New()
	registerRoutes(r, h, testTokens())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/version/app.yaml", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}
```

The existing `TestRoutesRegistered` calls `registerRoutes(r, h)` — update its call to `registerRoutes(r, h, testTokens())`, and if it asserts the POST reaches the handler, give that request an `Authorization: Bearer 0123456789abcdef` header.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=client-update-service`
Expected: FAIL — `registerRoutes` takes 2 arguments, not 3.

- [ ] **Step 3: Implement**

`client-update-service/routes.go` in full:

```go
package main

import "github.com/gin-gonic/gin"

// registerRoutes wires the health probe plus the /api/v1 version endpoints.
// Upload is gated on a service-account token; download deliberately is not —
// the client fleet pulling updates holds no credential.
func registerRoutes(r *gin.Engine, h *Handler, uploadTokens map[string]string) {
	r.GET("/healthz", h.HandleHealth)

	api := r.Group("/api/v1")
	api.POST("/version", requireServiceAccount(uploadTokens), h.HandleUpload)
	api.GET("/version/:fileName", h.HandleDownload)
}
```

In `client-update-service/main.go`, immediately after the `env.ParseAs` block:

```go
	if err := validateUploadTokens(cfg.UploadTokens); err != nil {
		return fmt.Errorf("validate upload tokens: %w", err)
	}
```

and change the registration call to:

```go
	registerRoutes(r, handler, cfg.UploadTokens)
```

In `client-update-service/deploy/docker-compose.yml`, add to `environment:`:

```yaml
      # Dev-only credential. Must match admin-service's CLIENT_UPDATE_TOKEN.
      - UPLOAD_TOKENS=${CLIENT_UPDATE_TOKENS:-admin-service:local-dev-token-0123456789}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=client-update-service && make build SERVICE=client-update-service`
Expected: PASS, and the binary builds.

- [ ] **Step 5: Commit**

```bash
git add client-update-service/routes.go client-update-service/main.go \
        client-update-service/handler_test.go client-update-service/deploy/docker-compose.yml
git commit -m "feat(client-update-service): gate POST /api/v1/version on a service account"
```

---

## Task 4: `client-update-service` — integration coverage

**Files:**
- Modify: `client-update-service/integration_test.go`

**Interfaces:**
- Consumes: `registerRoutes` (Task 3)
- Produces: nothing

- [ ] **Step 1: Write the failing test**

Append to `client-update-service/integration_test.go` (it already carries `//go:build integration` and a `TestMain`):

```go
// A full round-trip through the real router and a real MinIO: the credential
// gates the write, and the artifact that lands is byte-identical.
func TestIntegration_UploadRequiresServiceAccountThenRoundTrips(t *testing.T) {
	client, bucket := testutil.MinIO(t, "clientupdate")
	store := newMinioVersionStore(client, bucket, 30*time.Second)
	h := NewHandler(store, newBlobCache(4, time.Hour, 1<<20))

	tokens := map[string]string{"admin-service": "0123456789abcdef"}
	r := gin.New()
	registerRoutes(r, h, tokens)

	newUpload := func() (*http.Request, error) {
		body, ct := multipartBody(t, map[string]fileSpec{
			"configFile":  {name: "itest.yaml", content: "version: 1"},
			"executeFile": {name: "itest.bin", content: "MZbinarypayload"},
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
		req.Header.Set("Content-Type", ct)
		return req, nil
	}

	// Unauthenticated: rejected, and nothing is stored.
	req, err := newUpload()
	require.NoError(t, err)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)

	getW := httptest.NewRecorder()
	r.ServeHTTP(getW, httptest.NewRequest(http.MethodGet, "/api/v1/version/itest.yaml", nil))
	require.Equal(t, http.StatusNotFound, getW.Code,
		"a rejected upload must not have written anything to MinIO")

	// Authenticated: stored, and downloadable without a credential.
	req, err = newUpload()
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer 0123456789abcdef")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	getW = httptest.NewRecorder()
	r.ServeHTTP(getW, httptest.NewRequest(http.MethodGet, "/api/v1/version/itest.bin", nil))
	require.Equal(t, http.StatusOK, getW.Code)
	assert.Equal(t, "MZbinarypayload", getW.Body.String())
}
```

Check the file's existing imports and add any of `net/http`, `net/http/httptest`, `time`, `github.com/gin-gonic/gin` that are missing.

- [ ] **Step 2: Run the test to verify it fails**

First confirm it fails for the right reason by temporarily reverting the middleware — or simply observe it fail before Task 3 lands. If Tasks 1-3 are already committed, run it and confirm PASS, then verify the assertion has teeth by commenting out the `requireServiceAccount` argument in `routes.go`, re-running (expect FAIL on the 401 assertion), and restoring it.

Run: `make test-integration SERVICE=client-update-service`

- [ ] **Step 3: No implementation needed**

This task adds coverage over Tasks 1-3. If the test fails, the bug is in those tasks.

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test-integration SERVICE=client-update-service`
Expected: PASS (requires Docker).

- [ ] **Step 5: Commit**

```bash
git add client-update-service/integration_test.go
git commit -m "test(client-update-service): integration coverage for upload auth"
```

---

## Task 5: `admin-service` — relay config

**Files:**
- Modify: `admin-service/config.go`
- Test: `admin-service/config_test.go`

**Interfaces:**
- Consumes: nothing
- Produces: `Config.ClientUpdateURL string`, `Config.ClientUpdateToken string`, `Config.ClientUpdateTimeout time.Duration`; `validateClientUpdate(c Config) error`

- [ ] **Step 1: Write the failing tests**

Append to `admin-service/config_test.go`:

```go
func TestValidateClientUpdate(t *testing.T) {
	base := func() Config {
		return Config{
			ClientUpdateURL:     "http://client-update-service:8080",
			ClientUpdateToken:   "0123456789abcdef",
			ClientUpdateTimeout: 10 * time.Minute,
		}
	}
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(*Config) {}, false},
		{"unparseable url", func(c *Config) { c.ClientUpdateURL = "://nope" }, true},
		{"url without scheme", func(c *Config) { c.ClientUpdateURL = "client-update-service:8080" }, true},
		{"empty token", func(c *Config) { c.ClientUpdateToken = "" }, true},
		{"zero timeout", func(c *Config) { c.ClientUpdateTimeout = 0 }, true},
		{"negative timeout", func(c *Config) { c.ClientUpdateTimeout = -time.Second }, true},
		// Deliberately ABOVE httpWriteTimeout — that is the whole point of the
		// per-route deadline extension. checkHandlerTimeout must not be applied.
		{"timeout far above httpWriteTimeout", func(c *Config) { c.ClientUpdateTimeout = 30 * time.Minute }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base()
			tt.mutate(&cfg)
			err := validateClientUpdate(cfg)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestValidateClientUpdate_ErrorNeverLeaksTheToken(t *testing.T) {
	cfg := Config{
		ClientUpdateURL:     "://nope",
		ClientUpdateToken:   "supersecrettoken0123",
		ClientUpdateTimeout: time.Minute,
	}
	err := validateClientUpdate(cfg)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "supersecrettoken0123")
}
```

No import changes needed — `admin-service/config_test.go` already imports `time`,
`testify/assert` and `testify/require`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=admin-service`
Expected: FAIL — `validateClientUpdate` and the three fields undefined.

- [ ] **Step 3: Implement**

Add `"net/url"` to `admin-service/config.go`'s imports, and these fields to `Config`:

```go
	// ClientUpdateURL is the base URL of the LOCAL site's client-update-service,
	// whose upload endpoint only this service's account may call.
	ClientUpdateURL string `env:"CLIENT_UPDATE_URL,required"`
	// ClientUpdateToken is admin-service's entry in that service's UPLOAD_TOKENS.
	// Never logged, never returned to a caller.
	ClientUpdateToken string `env:"CLIENT_UPDATE_TOKEN,required"`
	// ClientUpdateTimeout bounds one artifact upload end to end. It is
	// deliberately far ABOVE httpWriteTimeout: the upload handler extends its own
	// read/write deadlines (client_update.go) rather than raising the server's,
	// so this value must NOT be passed through checkHandlerTimeout.
	ClientUpdateTimeout time.Duration `env:"CLIENT_UPDATE_UPLOAD_TIMEOUT" envDefault:"10m"`
```

Append to the same file:

```go
// validateClientUpdate checks the relay's configuration at startup. Error text
// names the field only — never the token, which would reach the logs.
func validateClientUpdate(c Config) error { //nolint:gocritic // hugeParam: startup value, called once
	u, err := url.Parse(c.ClientUpdateURL)
	if err != nil {
		return fmt.Errorf("invalid CLIENT_UPDATE_URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("invalid CLIENT_UPDATE_URL %q: need an absolute URL with scheme and host", c.ClientUpdateURL)
	}
	if c.ClientUpdateToken == "" {
		return fmt.Errorf("CLIENT_UPDATE_TOKEN must not be empty")
	}
	if c.ClientUpdateTimeout <= 0 {
		return fmt.Errorf("invalid CLIENT_UPDATE_UPLOAD_TIMEOUT %s: must be > 0", c.ClientUpdateTimeout)
	}
	return nil
}
```

Wire it into `loadConfig`, after the `c.Pool.Validate()` check:

```go
	if err := validateClientUpdate(c); err != nil {
		return Config{}, err
	}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=admin-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add admin-service/config.go admin-service/config_test.go
git commit -m "feat(admin-service): client-update relay configuration"
```

---

## Task 6: `admin-service` — the uploader and upstream error mapping

**Files:**
- Create: `admin-service/client_update.go`
- Test: `admin-service/client_update_test.go`

**Interfaces:**
- Consumes: `Config` (Task 5)
- Produces: `versionUploader` interface with `Upload(ctx context.Context, contentType string, body io.Reader) error`; `newRestyVersionUploader(client *resty.Client) *restyVersionUploader`; `mapUpstreamStatus(status int, body string) error`; `const clientUpdateVersionPath = "/api/v1/version"`

- [ ] **Step 1: Write the failing tests**

Create `admin-service/client_update_test.go`:

```go
package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/restyutil"
)

func TestMapUpstreamStatus(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     string
		wantNil  bool
		wantCode errcode.Code
		wantMsg  string
	}{
		{
			name: "200 is success", status: 200, body: `{"result":"success"}`, wantNil: true,
		},
		{
			name: "400 relays the upstream message", status: 400,
			body:     `{"code":"bad_request","error":"configFile must be a .yaml or .yml file"}`,
			wantCode: errcode.CodeBadRequest,
			wantMsg:  "configFile must be a .yaml or .yml file",
		},
		{
			name: "400 with an unparseable body falls back", status: 400, body: "not json",
			wantCode: errcode.CodeBadRequest,
			wantMsg:  "client update service rejected the upload",
		},
		{
			// Our own credential is bad. Relaying 401 would read to the admin as
			// an expired session and send them to a pointless re-login.
			name: "401 becomes unavailable, not unauthenticated", status: 401,
			body: `{"code":"unauthenticated","error":"invalid or missing service account token"}`,
			wantCode: errcode.CodeUnavailable,
		},
		{
			name: "403 becomes unavailable", status: 403, body: "{}",
			wantCode: errcode.CodeUnavailable,
		},
		{
			name: "500 becomes unavailable", status: 500, body: "{}",
			wantCode: errcode.CodeUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mapUpstreamStatus(tt.status, tt.body)
			if tt.wantNil {
				assert.NoError(t, err)
				return
			}
			var ec *errcode.Error
			require.ErrorAs(t, err, &ec)
			assert.Equal(t, tt.wantCode, ec.Code)
			if tt.wantMsg != "" {
				assert.Equal(t, tt.wantMsg, ec.Message)
			}
		})
	}
}

// The upstream's reason is a contract between client-update-service and its own
// clients. Re-emitting it would put codes into admin-service's surface that
// docs/client-api.md §9 does not document.
func TestMapUpstreamStatus_DoesNotCopyUpstreamReason(t *testing.T) {
	err := mapUpstreamStatus(400, `{"code":"bad_request","reason":"some_upstream_reason","error":"nope"}`)
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Empty(t, ec.Reason)
}

func TestRestyVersionUploader_PostsBodyAndCredential(t *testing.T) {
	var gotAuth, gotContentType, gotBody, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer srv.Close()

	u := newRestyVersionUploader(restyutil.New(srv.URL,
		restyutil.WithBearerToken("0123456789abcdef"),
		restyutil.WithTimeout(30*time.Second)))

	err := u.Upload(context.Background(), "multipart/form-data; boundary=xyz", strings.NewReader("PAYLOAD"))

	require.NoError(t, err)
	assert.Equal(t, "Bearer 0123456789abcdef", gotAuth)
	assert.Equal(t, "multipart/form-data; boundary=xyz", gotContentType)
	assert.Equal(t, "PAYLOAD", gotBody)
	assert.Equal(t, clientUpdateVersionPath, gotPath)
}

// resty buffers an entire io.Reader body into memory when SetContentLength is
// on (resty v2.17.2 middleware.go:519-527), which would defeat streaming for a
// multi-hundred-MB artifact. This pins that it stays off.
func TestRestyVersionUploader_StreamsWithoutContentLength(t *testing.T) {
	const size = 4 << 20 // 4 MiB — larger than any internal buffer
	var gotLen int
	var gotTransferEncoding []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTransferEncoding = r.TransferEncoding
		b, _ := io.ReadAll(r.Body)
		gotLen = len(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u := newRestyVersionUploader(restyutil.New(srv.URL,
		restyutil.WithBearerToken("0123456789abcdef"),
		restyutil.WithTimeout(30*time.Second)))

	err := u.Upload(context.Background(), "application/octet-stream",
		io.LimitReader(zeroReader{}, size))

	require.NoError(t, err)
	assert.Equal(t, size, gotLen, "the whole body must arrive intact")
	assert.Contains(t, gotTransferEncoding, "chunked",
		"a streamed body of unknown length must be chunked, not buffered and measured")
}

// zeroReader is an endless source of zero bytes; pair it with io.LimitReader.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func TestRestyVersionUploader_TransportFailureIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close() // closed: every request fails at the transport

	u := newRestyVersionUploader(restyutil.New(srv.URL,
		restyutil.WithBearerToken("0123456789abcdef"),
		restyutil.WithTimeout(2*time.Second)))

	err := u.Upload(context.Background(), "application/octet-stream", strings.NewReader("x"))

	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeUnavailable, ec.Code)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=admin-service`
Expected: FAIL — `mapUpstreamStatus`, `newRestyVersionUploader`, `clientUpdateVersionPath` undefined.

- [ ] **Step 3: Implement**

Create `admin-service/client_update.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/errcode"
)

// clientUpdateVersionPath is client-update-service's upload endpoint
// (docs/client-api.md §12).
const clientUpdateVersionPath = "/api/v1/version"

// versionUploader ships one artifact pair to client-update-service. Defined here,
// in the consumer, so tests can substitute a fake without an HTTP server.
type versionUploader interface {
	// Upload streams body — an already-encoded multipart payload whose boundary
	// contentType declares — to the upload endpoint.
	Upload(ctx context.Context, contentType string, body io.Reader) error
}

// restyVersionUploader is the production versionUploader over resty.
type restyVersionUploader struct {
	client *resty.Client
}

// newRestyVersionUploader wraps a client built by restyutil.New with the
// service-account bearer token and the upload timeout already applied.
//
// Two properties of that client are load-bearing and must not change:
//   - SetContentLength stays OFF. resty buffers an entire io.Reader body into
//     memory when it is on (v2.17.2 middleware.go:519-527), defeating streaming.
//   - No retries. The body is a pipe; once drained, a retry would send nothing.
func newRestyVersionUploader(client *resty.Client) *restyVersionUploader {
	return &restyVersionUploader{client: client}
}

func (u *restyVersionUploader) Upload(ctx context.Context, contentType string, body io.Reader) error {
	resp, err := u.client.R().
		SetContext(ctx).
		SetHeader("Content-Type", contentType).
		SetBody(body).
		Post(clientUpdateVersionPath)
	if err != nil {
		return errcode.Unavailable("client update service is unavailable", errcode.WithCause(err))
	}
	return mapUpstreamStatus(resp.StatusCode(), resp.String())
}

// mapUpstreamStatus turns client-update-service's verdict into this service's.
// A 401/403 means OUR credential is wrong — a deployment fault, not the admin's:
// relaying it would read as an expired admin session and prompt a pointless
// re-login, so it becomes a 503 with the real reason logged server-side.
func mapUpstreamStatus(status int, body string) error {
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == http.StatusBadRequest:
		return errcode.BadRequest(upstreamMessage(body, "client update service rejected the upload"))
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return errcode.Unavailable("client update upload is misconfigured",
			errcode.WithCause(fmt.Errorf("client-update-service rejected this service's credential with status %d", status)))
	default:
		return errcode.Unavailable("client update service is unavailable",
			errcode.WithCause(fmt.Errorf("client-update-service returned status %d", status)))
	}
}

// upstreamMessage lifts the human-readable text out of an errcode envelope. The
// upstream `reason` is deliberately NOT copied: reasons are a contract between a
// service and its own clients, and re-emitting another service's would put
// undocumented codes into admin-service's surface.
func upstreamMessage(body, fallback string) string {
	var env struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &env); err == nil && env.Error != "" {
		return env.Error
	}
	return fallback
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=admin-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add admin-service/client_update.go admin-service/client_update_test.go
git commit -m "feat(admin-service): streaming client-update uploader with upstream error mapping"
```

---

## Task 7: `admin-service` — the relay handler

**Files:**
- Modify: `admin-service/client_update.go`, `admin-service/handler.go`
- Test: `admin-service/client_update_test.go`

**Interfaces:**
- Consumes: `versionUploader` (Task 6), `Config.ClientUpdateTimeout` (Task 5), the existing `h.audit(ctx, c, action, targetUserID, targetAccount, details)`
- Produces: `(h *Handler) uploadClientVersion(c *gin.Context)`; `Handler.uploader versionUploader`; `withVersionUploader(u versionUploader) handlerOption`; `const clientUpdateAuditAction = "client_update.upload"`

**Why a variadic option, not a parameter:** `newHandler` has 56 call sites. A sixth positional parameter would churn ~55 test lines for no behavioral gain. A variadic option is backward-compatible — every existing call compiles untouched.

**Why these tests use `httptest.NewServer`, not `httptest.NewRecorder`:** the handler extends its read/write deadlines via `http.NewResponseController`, and a recorder cannot support deadlines — it returns `http.ErrNotSupported`, which the handler treats as fatal. Only a real server exercises the real path, and it round-trips real multipart bytes, which is what the streaming assertions need anyway.

- [ ] **Step 1: Write the failing tests**

Append to `admin-service/client_update_test.go` (add imports `"bytes"`, `"mime/multipart"`, `"net/textproto"`, `"fmt"`, `"sync"`, `"github.com/gin-gonic/gin"`):

```go
// fakeUploader records what the handler streamed and returns a scripted verdict.
type fakeUploader struct {
	mu          sync.Mutex
	called      bool
	contentType string
	body        []byte
	err         error
}

func (f *fakeUploader) Upload(_ context.Context, contentType string, body io.Reader) error {
	b, readErr := io.ReadAll(body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called = true
	f.contentType = contentType
	f.body = b
	if f.err != nil {
		return f.err
	}
	if readErr != nil {
		return readErr
	}
	return nil
}

func (f *fakeUploader) wasCalled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.called
}

// recordingAuditStore captures audit writes; every other AdminStore method is
// left nil so an unexpected call panics loudly.
type recordingAuditStore struct {
	AdminStore
	mu      sync.Mutex
	entries []AuditEntry
}

func (r *recordingAuditStore) AppendAudit(_ context.Context, e *AuditEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, *e)
	return nil
}

func (r *recordingAuditStore) audited() []AuditEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]AuditEntry(nil), r.entries...)
}

// uploadTestServer builds the real router (base middleware + a stubbed admin
// principal) around a live server, so deadlines and streaming behave as in prod.
func uploadTestServer(t *testing.T, h *Handler) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	applyBaseMiddleware(r, nil)
	r.POST("/v1/admin/client-updates", func(c *gin.Context) {
		c.Set(ctxPrincipal, sessionForTest())
		c.Next()
	}, h.uploadClientVersion)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

// uploadBody builds a multipart payload; a part with an empty contentType is
// written with no Content-Type header at all.
func uploadBody(t *testing.T, parts map[string]struct{ filename, content, contentType string }) (*bytes.Buffer, string) {
	t.Helper()
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	for field, p := range parts {
		hdr := textproto.MIMEHeader{}
		hdr.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, field, p.filename))
		if p.contentType != "" {
			hdr.Set("Content-Type", p.contentType)
		}
		fw, err := w.CreatePart(hdr)
		require.NoError(t, err)
		_, err = io.WriteString(fw, p.content)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return body, w.FormDataContentType()
}

func postUpload(t *testing.T, srv *httptest.Server, body *bytes.Buffer, contentType string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/client-updates", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestUploadClientVersion_Success(t *testing.T) {
	up := &fakeUploader{}
	store := &recordingAuditStore{}
	h := newHandler(store, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", "text/yaml"},
		"executeFile": {"app.exe", "MZbinary", ""},
	})
	resp := postUpload(t, srv, body, ct)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, up.wasCalled())
	assert.Contains(t, up.contentType, "multipart/form-data; boundary=")

	entries := store.audited()
	require.Len(t, entries, 1)
	assert.Equal(t, clientUpdateAuditAction, entries[0].Action)
	assert.Equal(t, "app.yaml", entries[0].Details["configFile"])
	assert.Equal(t, "app.exe", entries[0].Details["executeFile"])
}

// client-update-service picks a stored object's content type from the part's own
// header, falling back only when there is none. Re-encoding with CreateFormFile
// would stamp application/octet-stream on every part and silently change what
// the .yaml is stored as, so the relay must copy the header through verbatim.
func TestUploadClientVersion_PreservesPartContentTypes(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", "text/yaml"},
		"executeFile": {"app.exe", "MZbinary", ""},
	})
	resp := postUpload(t, srv, body, ct)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	relayed := parseRelayed(t, up.body, up.contentType)
	assert.Equal(t, "text/yaml", relayed["configFile"].contentType,
		"a declared part Content-Type must survive the relay")
	assert.Empty(t, relayed["executeFile"].contentType,
		"a part with no Content-Type must stay bare so the upstream fallback applies")
	assert.Equal(t, "app.yaml", relayed["configFile"].filename)
	assert.Equal(t, "version: 1", relayed["configFile"].content)
	assert.Equal(t, "MZbinary", relayed["executeFile"].content)
}

// relayedPart is one part as it arrived at the uploader.
type relayedPart struct{ filename, content, contentType string }

func parseRelayed(t *testing.T, body []byte, contentType string) map[string]relayedPart {
	t.Helper()
	_, params, err := mime.ParseMediaType(contentType)
	require.NoError(t, err)
	mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	out := map[string]relayedPart{}
	for {
		p, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		content, err := io.ReadAll(p)
		require.NoError(t, err)
		out[p.FormName()] = relayedPart{
			filename:    p.FileName(),
			content:     string(content),
			contentType: p.Header.Get("Content-Type"),
		}
		require.NoError(t, p.Close())
	}
	return out
}

// A body far larger than any internal buffer must arrive intact — proof the
// relay streams rather than truncating at a buffer boundary.
func TestUploadClientVersion_StreamsLargeBodyIntact(t *testing.T) {
	const size = 4 << 20 // 4 MiB
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	big := strings.Repeat("A", size)
	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", big, ""},
	})
	resp := postUpload(t, srv, body, ct)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	relayed := parseRelayed(t, up.body, up.contentType)
	assert.Len(t, relayed["executeFile"].content, size)
	assert.Equal(t, big, relayed["executeFile"].content)
}

func TestUploadClientVersion_UpstreamErrorsAreMapped(t *testing.T) {
	tests := []struct {
		name       string
		uploadErr  error
		wantStatus int
	}{
		{"upstream rejects the files", errcode.BadRequest("configFile must be a .yaml or .yml file"), http.StatusBadRequest},
		{"our credential is wrong", errcode.Unavailable("client update upload is misconfigured"), http.StatusServiceUnavailable},
		{"upstream is down", errcode.Unavailable("client update service is unavailable"), http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := &fakeUploader{err: tt.uploadErr}
			store := &recordingAuditStore{}
			h := newHandler(store, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
			srv := uploadTestServer(t, h)

			body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
				"configFile":  {"app.yaml", "version: 1", ""},
				"executeFile": {"app.exe", "MZ", ""},
			})
			resp := postUpload(t, srv, body, ct)

			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Empty(t, store.audited(),
				"a failed upload must not be recorded as a published artifact")
		})
	}
}

func TestUploadClientVersion_NonMultipartIsBadRequest(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/client-updates", strings.NewReader(`{"not":"multipart"}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.False(t, up.wasCalled(), "nothing may be sent upstream before the body is known to be multipart")
}

func TestUploadClientVersion_UnconfiguredUploaderIsUnavailable(t *testing.T) {
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil)
	srv := uploadTestServer(t, h)

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile": {"app.yaml", "version: 1", ""},
	})
	resp := postUpload(t, srv, body, ct)

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// The handler pushes its own read/write deadlines past the server's 15s/40s.
// If a middleware ever wraps c.Writer in a type without Unwrap,
// http.NewResponseController stops reaching the connection and this fails —
// which is the point: a silent no-op would kill real uploads at 15s.
func TestUploadClientVersion_ExtendsItsOwnDeadlines(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))
	srv := uploadTestServer(t, h)

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", "MZ", ""},
	})
	resp := postUpload(t, srv, body, ct)

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"a 500 here means SetReadDeadline/SetWriteDeadline returned ErrNotSupported")
}
```

Add these helpers to the same file:

```go
// uploadTestCfg is a Config with only what the upload handler reads.
func uploadTestCfg() Config {
	return Config{
		SiteID:              "site-A",
		ClientUpdateURL:     "http://client-update-service:8080",
		ClientUpdateToken:   "0123456789abcdef",
		ClientUpdateTimeout: 10 * time.Minute,
	}
}

// sessionForTest is the admin principal the stub middleware injects.
func sessionForTest() session.Session {
	return session.Session{
		ID:      "sess-1",
		UserID:  "admin-user-id",
		Account: "p_admin",
		SiteID:  "site-A",
		Roles:   []string{"admin"},
	}
}
```

Additional imports for the test file: `"errors"`, `"mime"`, `"strings"`, `"github.com/hmchangw/chat/pkg/session"`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=admin-service`
Expected: FAIL — `uploadClientVersion`, `withVersionUploader`, `clientUpdateAuditAction` undefined; `newHandler` takes 5 arguments.

- [ ] **Step 3: Implement**

In `admin-service/handler.go`, add the field to `Handler`:

```go
	// uploader relays client update artifacts to client-update-service. Nil when
	// unconfigured — the handler answers 503 rather than dereferencing it, the
	// same tolerance roomRPC and publishInbox already have.
	uploader versionUploader
```

Add below `newHandler`:

```go
// handlerOption configures optional Handler dependencies. Variadic, so adding
// one does not churn newHandler's 50-plus existing call sites.
type handlerOption func(*Handler)

// withVersionUploader injects the client-update relay.
func withVersionUploader(u versionUploader) handlerOption {
	return func(h *Handler) { h.uploader = u }
}
```

Change `newHandler`'s signature to accept `opts ...handlerOption` and apply them before returning:

```go
func newHandler(store AdminStore, sessions session.Store, cfg Config, rpc roomRequester, publishInbox func(ctx context.Context, subj string, data []byte) error, opts ...handlerOption) *Handler { //nolint:gocritic // hugeParam: Config is a startup value copied once at construction
	h := &Handler{
		store: store, sessions: sessions, cfg: cfg, roomRPC: rpc, publishInbox: publishInbox,
		remoteDests: remoteSites(cfg.AllSiteIDs, cfg.SiteID),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}
```

Append to `admin-service/client_update.go` (add imports `"errors"`, `"mime/multipart"`, `"net/textproto"`, `"strings"`, `"time"`, `"github.com/gin-gonic/gin"`, `"github.com/hmchangw/chat/pkg/errcode/errhttp"`):

```go
// clientUpdateAuditAction is the audit action for a published artifact pair.
const clientUpdateAuditAction = "client_update.upload"

// quoteEscaper mirrors mime/multipart's own escaping for Content-Disposition
// values, so a filename containing a quote or backslash cannot break out of the
// header it is written into.
var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

// relayResult carries the relay goroutine's outcome back to the handler. The
// filenames travel on the channel rather than a shared map so the handler can
// read them without racing the goroutine that filled them.
type relayResult struct {
	names map[string]string
	err   error
}

// uploadClientVersion relays an artifact pair to client-update-service under this
// service's own credential, then records the publication in the audit log.
//
// It validates nothing about the artifacts: client-update-service owns the
// extension and content rules, and duplicating them here would let the two
// services disagree about what a valid upload is.
func (h *Handler) uploadClientVersion(c *gin.Context) {
	ctx := c.Request.Context()

	if h.uploader == nil {
		errhttp.Write(ctx, c, errcode.Unavailable("client update upload is not configured"))
		return
	}

	// A large artifact outlives the server's 15s read / 40s write timeouts. Those
	// stay put — httpWriteTimeout doubles as the ceiling checkHandlerTimeout
	// validates ROOM_RPC_TIMEOUT and FANOUT_TIMEOUT against — so only this
	// request's deadlines move.
	if err := extendUploadDeadlines(c, h.cfg.ClientUpdateTimeout); err != nil {
		errhttp.Write(ctx, c, err)
		return
	}

	mr, err := c.Request.MultipartReader()
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("request body must be multipart/form-data"))
		return
	}

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	done := make(chan relayResult, 1)
	go func() {
		names, relayErr := relayParts(mr, mw)
		// Closing the pipe is what unblocks the reader, so it must happen on every
		// path out of this goroutine.
		_ = pw.CloseWithError(relayErr)
		done <- relayResult{names: names, err: relayErr}
	}()

	uploadErr := h.uploader.Upload(ctx, mw.FormDataContentType(), pr)
	// Unblocks the goroutine if the upload gave up mid-body; a no-op otherwise.
	_ = pr.CloseWithError(uploadErr)
	res := <-done

	if uploadErr != nil {
		errhttp.Write(ctx, c, uploadErr)
		return
	}
	if res.err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("could not read the uploaded files"))
		return
	}

	h.audit(ctx, c, clientUpdateAuditAction, "", "", res.names)
	c.JSON(http.StatusOK, gin.H{"result": "success"})
}

// extendUploadDeadlines pushes this request's read and write deadlines out to d.
// Verified reachable: gin's responseWriter implements Unwrap, and neither
// o11ygin nor otelgin replaces c.Writer. A failure here is fatal rather than
// ignored — a silent no-op would kill every large upload at the server's 15s
// read timeout, with nothing in the logs to explain it.
func extendUploadDeadlines(c *gin.Context, d time.Duration) error {
	rc := http.NewResponseController(c.Writer)
	deadline := time.Now().Add(d)
	if err := rc.SetReadDeadline(deadline); err != nil {
		return fmt.Errorf("extend upload read deadline: %w", err)
	}
	if err := rc.SetWriteDeadline(deadline); err != nil {
		return fmt.Errorf("extend upload write deadline: %w", err)
	}
	return nil
}

// relayParts copies every file part through to mw, preserving each part's field
// name, filename and declared Content-Type, and returns field->filename for the
// audit entry. Non-file fields are skipped: client-update-service reads only
// file parts.
//
// CreatePart, not CreateFormFile: the latter stamps application/octet-stream on
// every part, which would change what client-update-service stores the .yaml as.
func relayParts(mr *multipart.Reader, mw *multipart.Writer) (map[string]string, error) {
	names := make(map[string]string)
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return names, fmt.Errorf("read upload part: %w", err)
		}
		if err := relayOnePart(part, mw, names); err != nil {
			_ = part.Close()
			return names, err
		}
		if err := part.Close(); err != nil {
			return names, fmt.Errorf("close upload part: %w", err)
		}
	}
	if err := mw.Close(); err != nil {
		return names, fmt.Errorf("finish relay body: %w", err)
	}
	return names, nil
}

// relayOnePart copies a single part and records its filename.
func relayOnePart(part *multipart.Part, mw *multipart.Writer, names map[string]string) error {
	if part.FileName() == "" {
		return nil // not a file part
	}
	names[part.FormName()] = part.FileName()

	hdr := textproto.MIMEHeader{}
	hdr.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`,
		quoteEscaper.Replace(part.FormName()), quoteEscaper.Replace(part.FileName())))
	// Copied only when present, so a bare part stays bare and the upstream's own
	// content-type fallback still applies.
	if ct := part.Header.Get("Content-Type"); ct != "" {
		hdr.Set("Content-Type", ct)
	}
	fw, err := mw.CreatePart(hdr)
	if err != nil {
		return fmt.Errorf("create relay part %q: %w", part.FormName(), err)
	}
	if _, err := io.Copy(fw, part); err != nil {
		return fmt.Errorf("relay part %q: %w", part.FormName(), err)
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=admin-service`
Expected: PASS. The `-race` detector must stay quiet — if it reports a race on the filenames map, the channel handoff was skipped.

- [ ] **Step 5: Commit**

```bash
git add admin-service/client_update.go admin-service/client_update_test.go admin-service/handler.go
git commit -m "feat(admin-service): streaming, audited client-update relay handler"
```

---

## Task 8: `admin-service` — wire the route and the client

**Files:**
- Modify: `admin-service/routes.go`, `admin-service/main.go`, `admin-service/deploy/docker-compose.yml`
- Test: `admin-service/client_update_test.go`

**Interfaces:**
- Consumes: `uploadClientVersion` (Task 7), `newRestyVersionUploader` (Task 6), `Config` (Task 5)
- Produces: the route `POST /v1/admin/client-updates`

- [ ] **Step 1: Write the failing test**

Append to `admin-service/client_update_test.go`:

```go
// The route must sit inside the /v1/admin group so requireAdmin gates it. A
// non-admin caller must be turned away before any byte reaches the upstream.
func TestRoutes_ClientUpdatesRequiresAdmin(t *testing.T) {
	up := &fakeUploader{}
	h := newHandler(&recordingAuditStore{}, emptySessionStore(), uploadTestCfg(), nil, nil, withVersionUploader(up))

	sessions := &fakeSessionStore{
		FindByHashFn: func(context.Context, string) (*session.Session, error) {
			return &session.Session{
				ID: "s1", UserID: "u1", Account: "someone", SiteID: "site-A",
				Roles: []string{"user"}, // not an admin
			}, nil
		},
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	applyBaseMiddleware(r, nil)
	registerRoutes(r, h, sessions, "site-A")
	srv := httptest.NewServer(r)
	defer srv.Close()

	body, ct := uploadBody(t, map[string]struct{ filename, content, contentType string }{
		"configFile":  {"app.yaml", "version: 1", ""},
		"executeFile": {"app.exe", "MZ", ""},
	})
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/client-updates", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Authorization", "Bearer some-session-token")
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.False(t, up.wasCalled(), "a non-admin request must never reach the upstream")
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=admin-service`
Expected: FAIL — 404, because the route is not registered.

- [ ] **Step 3: Implement**

In `admin-service/routes.go`, add to the `admin` group (after the permissions routes):

```go
	admin.POST("/client-updates", h.uploadClientVersion)
```

In `admin-service/main.go`, before `h := newHandler(...)`:

```go
	// No retries and no SetContentLength on this client — see newRestyVersionUploader.
	uploader := newRestyVersionUploader(restyutil.New(cfg.ClientUpdateURL,
		restyutil.WithBearerToken(cfg.ClientUpdateToken),
		restyutil.WithTimeout(cfg.ClientUpdateTimeout)))
```

and change the construction to:

```go
	h := newHandler(st, sessStore, cfg, nc, publishInbox, withVersionUploader(uploader))
```

Add `"github.com/hmchangw/chat/pkg/restyutil"` to `main.go`'s imports.

In `admin-service/deploy/docker-compose.yml`, add to `environment:`:

```yaml
      - CLIENT_UPDATE_URL=${CLIENT_UPDATE_URL:-http://client-update-service:8080}
      # Dev-only credential. Must match client-update-service's UPLOAD_TOKENS entry.
      - CLIENT_UPDATE_TOKEN=${CLIENT_UPDATE_TOKEN:-local-dev-token-0123456789}
      - CLIENT_UPDATE_UPLOAD_TIMEOUT=10m
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=admin-service && make build SERVICE=admin-service`
Expected: PASS, and the binary builds.

- [ ] **Step 5: Commit**

```bash
git add admin-service/routes.go admin-service/main.go \
        admin-service/client_update_test.go admin-service/deploy/docker-compose.yml
git commit -m "feat(admin-service): POST /v1/admin/client-updates"
```

---

## Task 9: `admin-frontend` — the API client method

**Files:**
- Modify: `admin-frontend/src/api/admin/index.ts`, `admin-frontend/src/api/index.ts`
- Test: `admin-frontend/src/api/admin/admin.test.ts`

**Interfaces:**
- Consumes: `POST /v1/admin/client-updates` (Task 8), `AsyncJobError` from `@/api`
- Produces: `uploadClientVersion(authToken: string, configFile: File, executeFile: File, onProgress?: (pct: number) => void): Promise<void>`, **re-exported from the `@/api` barrel** — `src/api/index.ts` documents that components must import from the barrel and never reach into `@/api/admin` directly, and Task 10's component and tests depend on that

**Why `XMLHttpRequest` here and `fetch` everywhere else:** `fetch` cannot report upload progress. A multi-hundred-MB artifact would leave the console frozen with no feedback for minutes. The deviation is confined to this one method and still throws the same `AsyncJobError`.

- [ ] **Step 1: Write the failing tests**

Append to `admin-frontend/src/api/admin/admin.test.ts`:

```ts
describe('uploadClientVersion', () => {
  class MockXHR {
    static instances: MockXHR[] = []
    upload = { onprogress: null as ((e: ProgressEvent) => void) | null }
    onload: (() => void) | null = null
    onerror: (() => void) | null = null
    status = 0
    responseText = ''
    method = ''
    url = ''
    headers: Record<string, string> = {}
    body: FormData | null = null

    constructor() {
      MockXHR.instances.push(this)
    }
    open(method: string, url: string) {
      this.method = method
      this.url = url
    }
    setRequestHeader(k: string, v: string) {
      this.headers[k] = v
    }
    send(body: FormData) {
      this.body = body
    }
    // Test helpers
    succeed(status = 200, text = '{"result":"success"}') {
      this.status = status
      this.responseText = text
      this.onload?.()
    }
    fail(status: number, text: string) {
      this.status = status
      this.responseText = text
      this.onload?.()
    }
  }

  beforeEach(() => {
    MockXHR.instances = []
    vi.stubGlobal('XMLHttpRequest', MockXHR)
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  const cfgFile = () => new File(['version: 1'], 'app.yaml', { type: 'text/yaml' })
  const exeFile = () => new File(['MZ'], 'app.exe', { type: 'application/octet-stream' })

  it('posts both files as multipart with the bearer token', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    const xhr = MockXHR.instances[0]
    xhr.succeed()
    await promise

    expect(xhr.method).toBe('POST')
    expect(xhr.url).toContain('/v1/admin/client-updates')
    expect(xhr.headers.Authorization).toBe('Bearer tok-123')
    expect(xhr.body?.get('configFile')).toBeInstanceOf(File)
    expect(xhr.body?.get('executeFile')).toBeInstanceOf(File)
  })

  // The browser must write its own multipart boundary; setting Content-Type by
  // hand produces a body the server cannot parse.
  it('never sets Content-Type by hand', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    const xhr = MockXHR.instances[0]
    xhr.succeed()
    await promise

    const keys = Object.keys(xhr.headers).map((k) => k.toLowerCase())
    expect(keys).not.toContain('content-type')
  })

  it('reports upload progress as a percentage', async () => {
    const seen: number[] = []
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile(), (pct) => seen.push(pct))
    const xhr = MockXHR.instances[0]
    xhr.upload.onprogress?.({ lengthComputable: true, loaded: 50, total: 200 } as ProgressEvent)
    xhr.upload.onprogress?.({ lengthComputable: true, loaded: 200, total: 200 } as ProgressEvent)
    xhr.succeed()
    await promise

    expect(seen).toEqual([25, 100])
  })

  it('ignores progress events whose total is unknown', async () => {
    const seen: number[] = []
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile(), (pct) => seen.push(pct))
    const xhr = MockXHR.instances[0]
    xhr.upload.onprogress?.({ lengthComputable: false, loaded: 50, total: 0 } as ProgressEvent)
    xhr.succeed()
    await promise

    expect(seen).toEqual([])
  })

  it('throws AsyncJobError carrying the envelope on a non-2xx response', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    const xhr = MockXHR.instances[0]
    xhr.fail(400, '{"code":"bad_request","error":"configFile must be a .yaml or .yml file"}')

    await expect(promise).rejects.toMatchObject({
      name: 'AsyncJobError',
      code: 'bad_request',
      message: 'configFile must be a .yaml or .yml file',
    })
  })

  it('throws AsyncJobError when the response body is not JSON', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    MockXHR.instances[0].fail(502, '<html>gateway</html>')
    await expect(promise).rejects.toBeInstanceOf(Error)
  })

  it('rejects on a transport error', async () => {
    const promise = uploadClientVersion('tok-123', cfgFile(), exeFile())
    MockXHR.instances[0].onerror?.()
    await expect(promise).rejects.toBeInstanceOf(Error)
  })
})
```

Ensure the file imports `uploadClientVersion` alongside its existing imports, and `beforeEach`, `afterEach`, `vi` from `vitest`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd admin-frontend && npm test`
Expected: FAIL — `uploadClientVersion` is not exported.

- [ ] **Step 3: Implement**

Append to `admin-frontend/src/api/admin/index.ts`:

```ts
/**
 * Uploads a client update artifact pair. Unlike every other call here this uses
 * `XMLHttpRequest`, because `fetch` cannot report upload progress and these
 * artifacts are large enough that a silent UI would look hung.
 *
 * @throws {AsyncJobError} on a non-2xx response or a transport failure.
 */
export function uploadClientVersion(
  authToken: string,
  configFile: File,
  executeFile: File,
  onProgress?: (percent: number) => void,
): Promise<void> {
  const form = new FormData()
  form.append('configFile', configFile)
  form.append('executeFile', executeFile)

  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    xhr.open('POST', `${ADMIN_SERVICE_URL}/v1/admin/client-updates`)
    xhr.setRequestHeader('Authorization', `Bearer ${authToken}`)
    // Content-Type is deliberately unset: the browser writes the multipart
    // boundary, and overriding it produces a body the server cannot parse.

    if (onProgress) {
      xhr.upload.onprogress = (e: ProgressEvent) => {
        if (!e.lengthComputable || !e.total) return
        onProgress(Math.round((e.loaded / e.total) * 100))
      }
    }

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve()
        return
      }
      reject(uploadEnvelopeError(xhr.status, xhr.responseText))
    }
    xhr.onerror = () => reject(new AsyncJobError('upload failed: could not reach the server'))

    xhr.send(form)
  })
}

/** Builds an `AsyncJobError` from an XHR error body, mirroring `parseHttpEnvelopeError`. */
function uploadEnvelopeError(status: number, responseText: string): AsyncJobError {
  const fallback = `upload failed with status ${status}`
  try {
    const body = JSON.parse(responseText) as {
      error?: string
      code?: string
      reason?: string
      metadata?: Record<string, string>
    }
    return new AsyncJobError(body.error || fallback, {
      code: body.code,
      reason: body.reason,
      metadata: body.metadata,
    })
  } catch {
    return new AsyncJobError(fallback)
  }
}
```

Update the import at the top of the file so `AsyncJobError` is available:

```ts
import { AsyncJobError, parseHttpEnvelopeError } from '@/api'
```

- [ ] **Step 4: Export it from the barrel**

In `admin-frontend/src/api/index.ts`, add `uploadClientVersion` to the alphabetical
`export { … } from './admin'` list — between `setPassword` and `updateUser`:

```ts
  setPassword,
  updateUser,
  uploadClientVersion,
```

Components import from `@/api`, never `@/api/admin` — the barrel's own header
states this, and Task 10 relies on it for both the import and the `vi.mock('@/api', …)`
test idiom every existing console test uses.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd admin-frontend && npm test && npm run typecheck`
Expected: PASS, and typecheck is clean.

- [ ] **Step 6: Commit**

```bash
git add admin-frontend/src/api/admin/index.ts admin-frontend/src/api/index.ts \
        admin-frontend/src/api/admin/admin.test.ts
git commit -m "feat(admin-frontend): uploadClientVersion API client with progress"
```

---

## Task 10: `admin-frontend` — the Updates console

**Files:**
- Create: `admin-frontend/src/components/UpdatesConsole/UpdatesPage.jsx`, `index.jsx`, `style.css`, `UpdatesPage.test.jsx`

**Interfaces:**
- Consumes: `uploadClientVersion` and `formatAsyncJobError` from the `@/api` barrel (Task 9), `useAuth` from `@/context/AuthContext`
- Produces: default-exported `UpdatesPage` component

**Codebase idioms this task must follow** (verified against `UsersConsole/UsersPage`
and `AppShell`, not assumed):
- `@testing-library/user-event` is **not** a dependency. Use `fireEvent` from
  `@testing-library/react`, as every existing test does. Do not add the package
  (CLAUDE.md §5: ask before adding a dependency).
- The auth session field is `session.authToken`, **not** `session.token`.
- Mock the barrel — `vi.mock('@/api', async (importOriginal) => …)` — and
  `vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))` with a
  `useAuth.mockReturnValue(...)` in `beforeEach`.
- Selecting a file with `fireEvent.change` requires defining `target.files` as a
  real `FileList`-like array, shown below.

- [ ] **Step 1: Write the failing tests**

Create `admin-frontend/src/components/UpdatesConsole/UpdatesPage.test.jsx`:

```jsx
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, uploadClientVersion: vi.fn() }
})

import UpdatesPage from './UpdatesPage'
import { useAuth } from '@/context/AuthContext'
import { uploadClientVersion, AsyncJobError } from '@/api'

const yaml = () => new File(['version: 1'], 'app.yaml', { type: 'text/yaml' })
const exe = () => new File(['MZ'], 'app.exe', { type: 'application/octet-stream' })

// fireEvent.change cannot set an <input type="file"> value directly; assigning a
// FileList-like array to target.files is the standard workaround.
function selectFile(input, file) {
  fireEvent.change(input, { target: { files: [file] } })
}

beforeEach(() => {
  vi.clearAllMocks()
  useAuth.mockReturnValue({
    session: { authToken: 'tok', account: 'root', siteId: 'site-1' },
    logout: vi.fn(),
  })
})

describe('UpdatesPage', () => {
  it('disables upload until both files are chosen', () => {
    render(<UpdatesPage />)

    const button = screen.getByRole('button', { name: /upload/i })
    expect(button).toBeDisabled()

    selectFile(screen.getByLabelText(/config file/i), yaml())
    expect(button).toBeDisabled()

    selectFile(screen.getByLabelText(/executable/i), exe())
    expect(button).toBeEnabled()
  })

  it('uploads both files and shows a success message', async () => {
    uploadClientVersion.mockResolvedValue(undefined)
    render(<UpdatesPage />)

    selectFile(screen.getByLabelText(/config file/i), yaml())
    selectFile(screen.getByLabelText(/executable/i), exe())
    fireEvent.click(screen.getByRole('button', { name: /upload/i }))

    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent(/uploaded/i))
    expect(uploadClientVersion).toHaveBeenCalledTimes(1)
    const [token, cfg, bin] = uploadClientVersion.mock.calls[0]
    expect(token).toBe('tok')
    expect(cfg.name).toBe('app.yaml')
    expect(bin.name).toBe('app.exe')
  })

  it('shows the server error message when the upload is rejected', async () => {
    uploadClientVersion.mockRejectedValue(
      new AsyncJobError('configFile must be a .yaml or .yml file', { code: 'bad_request' }),
    )
    render(<UpdatesPage />)

    selectFile(screen.getByLabelText(/config file/i), yaml())
    selectFile(screen.getByLabelText(/executable/i), exe())
    fireEvent.click(screen.getByRole('button', { name: /upload/i }))

    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent(/\.yaml or \.yml/i))
  })

  it('disables the button while an upload is in flight', async () => {
    let release
    uploadClientVersion.mockImplementation(() => new Promise((r) => { release = r }))
    render(<UpdatesPage />)

    selectFile(screen.getByLabelText(/config file/i), yaml())
    selectFile(screen.getByLabelText(/executable/i), exe())
    fireEvent.click(screen.getByRole('button', { name: /upload/i }))

    await waitFor(() =>
      expect(screen.getByRole('button', { name: /upload/i })).toBeDisabled(),
    )
    release()
    await waitFor(() =>
      expect(screen.getByRole('button', { name: /upload/i })).toBeEnabled(),
    )
  })

  it('passes a progress callback through to the client', async () => {
    uploadClientVersion.mockImplementation((_t, _c, _e, onProgress) => {
      onProgress(42)
      return Promise.resolve()
    })
    render(<UpdatesPage />)

    selectFile(screen.getByLabelText(/config file/i), yaml())
    selectFile(screen.getByLabelText(/executable/i), exe())
    fireEvent.click(screen.getByRole('button', { name: /upload/i }))

    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent(/uploaded/i))
    expect(uploadClientVersion).toHaveBeenCalledWith(
      'tok', expect.any(File), expect.any(File), expect.any(Function),
    )
  })
})
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd admin-frontend && npm test`
Expected: FAIL — cannot resolve `./UpdatesPage`.

- [ ] **Step 3: Implement**

Create `admin-frontend/src/components/UpdatesConsole/UpdatesPage.jsx`:

```jsx
import { useState } from 'react'
import { uploadClientVersion, formatAsyncJobError } from '@/api'
import { useAuth } from '@/context/AuthContext'
import './style.css'

// Publishes a client update artifact pair. Validation of the files themselves
// lives in client-update-service, so this form deliberately does not second-guess
// extensions — the server's message is what the admin sees.
export default function UpdatesPage() {
  const { session } = useAuth()
  const [configFile, setConfigFile] = useState(null)
  const [executeFile, setExecuteFile] = useState(null)
  const [busy, setBusy] = useState(false)
  const [percent, setPercent] = useState(0)
  const [error, setError] = useState('')
  const [done, setDone] = useState('')

  const ready = Boolean(configFile && executeFile) && !busy

  const handleUpload = async () => {
    setBusy(true)
    setError('')
    setDone('')
    setPercent(0)
    try {
      await uploadClientVersion(session.authToken, configFile, executeFile, setPercent)
      setDone(`Uploaded ${configFile.name} and ${executeFile.name}.`)
    } catch (err) {
      setError(formatAsyncJobError(err))
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="updates-page">
      <h2>Client updates</h2>
      <p className="updates-page-hint">
        Upload the update descriptor and its executable. An upload replaces any
        artifact already stored under the same file name.
      </p>

      <div className="updates-page-field">
        <label htmlFor="configFile">Config file (.yaml)</label>
        <input
          id="configFile"
          type="file"
          disabled={busy}
          onChange={(e) => setConfigFile(e.target.files?.[0] ?? null)}
        />
      </div>

      <div className="updates-page-field">
        <label htmlFor="executeFile">Executable</label>
        <input
          id="executeFile"
          type="file"
          disabled={busy}
          onChange={(e) => setExecuteFile(e.target.files?.[0] ?? null)}
        />
      </div>

      <button type="button" className="updates-page-submit" disabled={!ready} onClick={handleUpload}>
        {busy ? `Uploading… ${percent}%` : 'Upload'}
      </button>

      {done && (
        <p className="updates-page-ok" role="status">
          {done}
        </p>
      )}
      {error && (
        <p className="updates-page-error" role="alert">
          {error}
        </p>
      )}
    </section>
  )
}
```

Create `admin-frontend/src/components/UpdatesConsole/index.jsx`:

```jsx
export { default } from './UpdatesPage'
```

Create `admin-frontend/src/components/UpdatesConsole/style.css`, matching the visual language of `UsersConsole/UsersPage/style.css` — read that file first and reuse its spacing, colors and control styling rather than inventing new ones:

```css
.updates-page {
  padding: 1.5rem;
  max-width: 40rem;
}

.updates-page-hint {
  color: #555;
  margin-bottom: 1.5rem;
}

.updates-page-field {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
  margin-bottom: 1rem;
}

.updates-page-submit {
  margin-top: 0.5rem;
}

.updates-page-ok {
  margin-top: 1rem;
  color: #1a7f37;
}

.updates-page-error {
  margin-top: 1rem;
  color: #b3261e;
}
```

The session field is `session.authToken` — verified in
`src/context/AuthContext/AuthContext.jsx`, whose header documents the exposed
shape as `{authToken, account, siteId}`. `session.token` does not exist.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd admin-frontend && npm test && npm run typecheck`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add admin-frontend/src/components/UpdatesConsole
git commit -m "feat(admin-frontend): Updates console for publishing client artifacts"
```

---

## Task 11: `admin-frontend` — nav entry

**Files:**
- Modify: `admin-frontend/src/components/AppShell/AppShell.jsx`
- Test: `admin-frontend/src/components/AppShell/AppShell.test.jsx`

**Interfaces:**
- Consumes: `UpdatesConsole` (Task 10)
- Produces: an `updates` section in `SECTIONS`

- [ ] **Step 1: Write the failing test**

Append to `admin-frontend/src/components/AppShell/AppShell.test.jsx`. The file
already mocks `@/context/AuthContext` and the `@/api` barrel and uses `fireEvent`
(**not** `userEvent`, which is not a dependency) — this test reuses that existing
setup and adds nothing to it:

```jsx
it('switches from Users to Updates via nav and mounts UpdatesConsole', async () => {
  render(<AppShell />)
  await waitFor(() => expect(listUsers).toHaveBeenCalled())

  fireEvent.click(screen.getByRole('button', { name: 'Updates' }))

  await waitFor(() =>
    expect(screen.getByRole('heading', { name: /client updates/i })).toBeInTheDocument(),
  )
})
```

The console is lazy-loaded, so the `waitFor` is doing real work — it waits for the
dynamic import to resolve. `UpdatesPage` calls only `useAuth` on mount and fetches
nothing, so the existing `beforeEach` mocks cover it with no additions to the
`vi.mock('@/api', …)` list.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd admin-frontend && npm test`
Expected: FAIL — no button named "Updates".

- [ ] **Step 3: Implement**

In `admin-frontend/src/components/AppShell/AppShell.jsx`, add to `SECTIONS` after the permissions entry:

```jsx
  {
    key: 'updates',
    label: 'Updates',
    Component: lazy(() => import('@/components/UpdatesConsole')),
  },
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd admin-frontend && npm test && npm run typecheck && npm run build`
Expected: PASS, and the production bundle builds.

- [ ] **Step 5: Commit**

```bash
git add admin-frontend/src/components/AppShell/AppShell.jsx admin-frontend/src/components/AppShell/AppShell.test.jsx
git commit -m "feat(admin-frontend): Updates nav section"
```

---

## Task 12: Documentation

**Files:**
- Modify: `docs/client-api.md`, `docs/client-api/request-reply.md`

**Interfaces:**
- Consumes: the finished endpoints from Tasks 3 and 8
- Produces: nothing code-facing

- [ ] **Step 1: Update `docs/client-api.md` §12**

Replace the warning block under `## 12. Client Update Service` with:

```markdown
> [!WARNING]
> **`GET /api/v1/version/:fileName` is UNAUTHENTICATED.** Anyone who can reach the
> service can download update artifacts. It **MUST be network-restricted**. Uploads
> are gated on a service account (below).
```

Change the `### POST /api/v1/version` auth line from `**Auth:** none (v1)` to:

```markdown
**Auth:** `Authorization: Bearer <service-account token>`. The token must match an
entry in the service's `UPLOAD_TOKENS` table (`account:token`, comma-separated).
Only `admin-service` is provisioned; browsers and end-user clients never call this
endpoint directly — they go through
[`POST /v1/admin/client-updates`](#917-http--post-v1adminclient-updates).
```

Add to that endpoint's response table, above the `500` row:

```markdown
| `401 Unauthorized` | Missing, malformed, or unrecognized service-account token. Identical response for all three. |
```

Leave `### GET /api/v1/version/:fileName`'s `**Auth:** none (v1)` as-is — it is accurate and deliberate.

- [ ] **Step 2: Add `docs/client-api.md` §9.16**

After §9.15, following the field-table style §9 already uses:

```markdown
### 9.16 HTTP — POST /v1/admin/client-updates

**Auth:** admin session (`Authorization: Bearer <session token>`), same as every `/v1/admin/…` route.

Publishes a client update artifact pair. `admin-service` streams both parts
straight through to `client-update-service` under its own service-account
credential. (The "nothing is buffered" claim this plan originally carried is
wrong — see the design record §2.3 correction; the browser still never holds
that credential.)

The artifacts themselves are validated by `client-update-service`, not here: file
name and extension rules live there and are reported back verbatim on a `400`.

#### Request

`multipart/form-data`:

| Part | Type | Required | Notes |
|---|---|---|---|
| `configFile` | file (`.yaml`/`.yml`) | yes | Update descriptor. |
| `executeFile` | file (binary) | yes | The executable. |

#### Response

| Status | Condition |
|---|---|
| `200 OK` | Both artifacts published. |
| `400 Bad Request` | Body is not `multipart/form-data`, or `client-update-service` rejected the artifacts (its message is relayed). |
| `401 Unauthorized` | Missing or invalid admin session. |
| `403 Forbidden` | Valid session without the `admin` role, or issued for another site. |
| `503 Service Unavailable` | `client-update-service` is unreachable, or this service's upload credential is not configured or was rejected. |

##### Success response (`200`)

| Field | Type | Notes |
|---|---|---|
| `result` | string | Always `"success"`. |

```json
{ "result": "success" }
```

**Audit:** a successful upload appends an `AuditEntry` with action
`client_update.upload` and `details` naming both uploaded file names.
```

Add `- [9.17 POST /v1/admin/client-updates](#917-http--post-v1adminclient-updates)` to the §9 line in the table of contents, matching how §9.13-§9.15 are listed.

- [ ] **Step 3: Update `docs/client-api/request-reply.md`**

Under `### HTTP — Client Update Service`, change the preamble to:

```markdown
HTTP endpoints served by `client-update-service`. Uploads require a service-account
bearer token (only `admin-service` holds one); downloads are unauthenticated and
must be network-restricted. Full request/response schemas and the download cache
behavior are in
[../client-api.md §12](../client-api.md#12-client-update-service).
```

and the upload row's Purpose cell to:

```markdown
| `POST /api/v1/version` | synchronous HTTP | Upload a `configFile` (.yaml/.yml) + `executeFile` pair (multipart, no size cap; disk-backed — parts over 32 MiB spill to a temp file before the MinIO write). Service-account bearer token required. |
```

Under `## HTTP — Admin Service`, add a row to its endpoint table:

```markdown
| `POST /v1/admin/client-updates` | synchronous HTTP | Publish a client update artifact pair; streamed on to client-update-service under this service's own credential. Audited as `client_update.upload`. |
```

- [ ] **Step 4: Verify the docs are consistent**

Run:

```bash
grep -n "client-updates" docs/client-api.md docs/client-api/request-reply.md
grep -n "UNAUTHENTICATED" docs/client-api.md
```

Expected: the new endpoint appears in both files and in the §9 TOC; the only remaining `UNAUTHENTICATED` mention scopes itself to `GET`.

- [ ] **Step 5: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs(client-api): service-account upload auth and the admin client-update endpoint"
```

---

## Final Verification

- [ ] **Full suite, lint and SAST**

```bash
make lint
make test
make sast
make test-integration SERVICE=client-update-service
cd admin-frontend && npm test && npm run typecheck && npm run build
```

All must pass. `make sast` fails on medium+ findings; if `gosec` flags the
`fmt.Sprintf` building the Content-Disposition header, the `quoteEscaper` in
Task 7 is the answer — confirm it is applied to **both** the field name and the
filename rather than suppressing the finding.

- [ ] **Coverage floor**

```bash
make test SERVICE=client-update-service
make test SERVICE=admin-service
```

Confirm the new files clear 80%, and that `client_update.go` and
`middleware.go` clear 90% per CLAUDE.md §4.

- [ ] **Push**

```bash
git push -u origin claude/client-update-service-auth-i3hvpf
```

Retry on network failure with backoff: 2s, 4s, 8s, 16s.
