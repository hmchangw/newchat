# upload-service `/setCookie` + cookie auth — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /api/v1/setCookie` to `upload-service` that hands the browser an `ssoToken` cookie, make `authMiddleware` accept that cookie, and allow credentialed cross-origin requests via a configurable CORS allowlist — so `<img>`-driven file downloads authenticate without a header.

**Architecture:** All changes live in the `upload-service` flat `package main` directory. A `tokenFromRequest` helper unifies header-then-cookie token acquisition for both `authMiddleware` and the new handler. A new engine-level `corsMiddleware` echoes an allowlisted `Origin` with `Access-Control-Allow-Credentials: true` (never wildcard, which is illegal with credentials) and answers preflight `OPTIONS` with 204. The new handler sets a `SameSite=None; Secure; Partitioned; HttpOnly` session cookie via a hand-built `http.Cookie` (Gin's `c.SetCookie` cannot express those attributes).

**Tech Stack:** Go 1.25, Gin, `stretchr/testify`, `caarlos0/env`. Standard-library `net/http` for the cookie.

## Global Constraints

- Language: Go 1.25. Framework: Gin. HTTP client: Resty (not needed here).
- Config: environment variables via `caarlos0/env` only — never `os.Getenv`. `SCREAMING_SNAKE_CASE` names; provide `envDefault` for non-critical vars.
- Errors: client-facing failures use `pkg/errcode` + `errhttp.Write`; infra failures return raw `fmt.Errorf("…: %w", err)`. Never log-and-return the same error.
- Logging: `log/slog` JSON only. Never log tokens or full bodies.
- Testing: TDD Red-Green-Refactor. Tests in `package main`. `stretchr/testify` assertions. Run via `make test SERVICE=upload-service` (adds `-race`). Minimum 80% coverage; target 90% for new handler/middleware.
- Never run raw `go` commands — use `make` targets. Lint via `make lint`, format via `make fmt`.
- Client-facing HTTP change ⇒ update `docs/client-api.md` in the same change set.
- Commit trailer: end each commit message body with
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` on its own line.
- Git identity must be `user.email=noreply@anthropic.com`, `user.name=Claude` (already configured this session).

---

### Task 1: `tokenFromRequest` helper + cookie fallback in `authMiddleware`

Extract token acquisition into a shared helper and make `authMiddleware` fall back to the `ssoToken` cookie when the header is absent. Header-first preserves existing header callers exactly.

**Files:**
- Modify: `upload-service/middleware.go` (add `tokenFromRequest`; change the token line in `authMiddleware`)
- Test: `upload-service/middleware_test.go` (add helper + cases)

**Interfaces:**
- Produces: `func tokenFromRequest(c *gin.Context) string` — returns the `ssoToken` header value, else the `ssoToken` cookie value, else `""`. Consumed by `authMiddleware` (this task) and `HandleSetCookie` (Task 3).

- [ ] **Step 1: Write the failing tests**

Add to `upload-service/middleware_test.go`:

```go
func TestTokenFromRequest(t *testing.T) {
	tests := []struct {
		name, header, cookie, want string
	}{
		{"header only", "h-tok", "", "h-tok"},
		{"cookie only", "", "c-tok", "c-tok"},
		{"header wins over cookie", "h-tok", "c-tok", "h-tok"},
		{"neither", "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodGet, "/x", nil)
			if tc.header != "" {
				c.Request.Header.Set("ssoToken", tc.header)
			}
			if tc.cookie != "" {
				c.Request.AddCookie(&http.Cookie{Name: "ssoToken", Value: tc.cookie})
			}
			assert.Equal(t, tc.want, tokenFromRequest(c))
		})
	}
}

func TestAuthMiddleware_CookieFallback_PopulatesUser(t *testing.T) {
	v := &fakeValidator{claims: pkgoidc.Claims{PreferredUsername: "bob", Email: "bob@x.com"}}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(authMiddleware(v, false))
	var captured *AuthenticatedUser
	r.GET("/x", func(c *gin.Context) {
		if u, ok := userFromContext(c); ok {
			captured = u
		}
		c.Status(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.AddCookie(&http.Cookie{Name: "ssoToken", Value: "tok"})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, captured)
	assert.Equal(t, "bob", captured.Account)
	assert.Equal(t, "bob@x.com", captured.Email)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `undefined: tokenFromRequest`, and `TestAuthMiddleware_CookieFallback_PopulatesUser` returns 401 (cookie not read yet).

- [ ] **Step 3: Add the helper and use it in `authMiddleware`**

In `upload-service/middleware.go`, add the helper (place it just above `authMiddleware`):

```go
// tokenFromRequest returns the ssoToken from the request header, falling back to
// the ssoToken cookie. <img>-driven download requests cannot set headers, so they
// carry the token in the cookie instead; header-first keeps existing callers exact.
func tokenFromRequest(c *gin.Context) string {
	if t := c.GetHeader("ssoToken"); t != "" {
		return t
	}
	t, _ := c.Cookie("ssoToken")
	return t
}
```

Then, inside `authMiddleware`'s returned handler, replace:

```go
		token := c.GetHeader("ssoToken")
```

with:

```go
		token := tokenFromRequest(c)
```

(Leave the `if token == "" { … Unauthenticated("missing ssoToken" …) }` block and everything below it unchanged.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: PASS (including the pre-existing `TestAuthMiddleware_*` cases, which still exercise the header path).

- [ ] **Step 5: Commit**

```bash
git add upload-service/middleware.go upload-service/middleware_test.go
git commit -m "feat(upload-service): accept ssoToken from cookie as header fallback

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `corsMiddleware` + `CORS_ALLOWED_ORIGINS` config + engine wiring

Add a credentialed-CORS middleware driven by an ops-configured origin allowlist, wire it at the engine level in `main.go`, and add the config field. Engine-level registration is required so preflight `OPTIONS` (routed to `NoRoute`) reaches it.

**Files:**
- Modify: `upload-service/middleware.go` (add `corsMiddleware`; add `net/http` import)
- Modify: `upload-service/main.go` (add `CORSAllowedOrigins` config field; `r.Use(corsMiddleware(cfg.CORSAllowedOrigins))`)
- Test: `upload-service/middleware_test.go` (add CORS cases)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `func corsMiddleware(allowed []string) gin.HandlerFunc`. `config.CORSAllowedOrigins []string`.

- [ ] **Step 1: Write the failing tests**

Add to `upload-service/middleware_test.go`:

```go
func runCORS(t *testing.T, allowed []string, method, origin string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(corsMiddleware(allowed))
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.POST("/x", func(c *gin.Context) { c.Status(http.StatusOK) })
	req := httptest.NewRequest(method, "/x", nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestCORSMiddleware_AllowedOrigin_EmitsCredentialedHeaders(t *testing.T) {
	w := runCORS(t, []string{"https://app.example.com"}, http.MethodPost, "https://app.example.com")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "https://app.example.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "true", w.Header().Get("Access-Control-Allow-Credentials"))
	assert.Equal(t, "Origin", w.Header().Get("Vary"))
	assert.Contains(t, w.Header().Get("Access-Control-Allow-Headers"), "ssoToken")
}

func TestCORSMiddleware_DisallowedOrigin_NoHeaders(t *testing.T) {
	w := runCORS(t, []string{"https://app.example.com"}, http.MethodGet, "https://evil.example.com")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"))
}

func TestCORSMiddleware_NoOrigin_NoHeaders(t *testing.T) {
	w := runCORS(t, []string{"https://app.example.com"}, http.MethodGet, "")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORSMiddleware_Preflight_AllowedOrigin_204(t *testing.T) {
	w := runCORS(t, []string{"https://app.example.com"}, http.MethodOptions, "https://app.example.com")
	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "https://app.example.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "true", w.Header().Get("Access-Control-Allow-Credentials"))
}

func TestCORSMiddleware_Preflight_NoOrigin_204_NoHeaders(t *testing.T) {
	w := runCORS(t, []string{"https://app.example.com"}, http.MethodOptions, "")
	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"))
}

func TestCORSMiddleware_Preflight_DisallowedOrigin_204_NoHeaders(t *testing.T) {
	w := runCORS(t, []string{"https://app.example.com"}, http.MethodOptions, "https://evil.example.com")
	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"))
}
```

These two cases lock in that the `OPTIONS` → 204 abort fires for *any* origin, while the CORS headers are still gated by the allowlist — so a preflight from an unknown/empty origin gets a bare 204 and the browser blocks the actual request.

Note: the preflight test relies on engine-level `r.Use` — an unmatched `OPTIONS /x` routes to `NoRoute`, which runs engine middleware, so `corsMiddleware` fires and aborts 204. This mirrors the production wiring in Step 4.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `undefined: corsMiddleware`.

- [ ] **Step 3: Implement `corsMiddleware`**

In `upload-service/middleware.go`, add `"net/http"` to the import block, then add:

```go
// corsMiddleware emits credentialed CORS headers when the request Origin is in the
// ops-configured allowlist, and answers preflight OPTIONS from any origin with 204.
// Wildcard origin is intentionally NOT used: Access-Control-Allow-Origin: * is invalid
// together with Access-Control-Allow-Credentials: true. A disallowed or absent Origin
// yields no CORS headers, so the browser blocks the cross-origin read. Register this at
// the engine level (r.Use) so preflight OPTIONS — which matches no route and falls to
// NoRoute — still reaches it; group middleware would not fire for NoRoute.
func corsMiddleware(allowed []string) gin.HandlerFunc {
	allowSet := make(map[string]struct{}, len(allowed))
	for _, o := range allowed {
		if o != "" {
			allowSet[o] = struct{}{}
		}
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if _, ok := allowSet[origin]; ok {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Content-Type, ssoToken, X-Request-ID")
			c.Header("Access-Control-Max-Age", "300")
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
```

(The empty-string guard when building `allowSet` means an empty `Origin` header can never match, even if the allowlist somehow contains `""`.)

- [ ] **Step 4: Add the config field and wire it in `main.go`**

In `upload-service/main.go`, add to the `config` struct (near the other HTTP-related fields, e.g. right after the `Port`/`DevMode`/`SiteID` block):

```go
	// CORSAllowedOrigins is the credentialed-CORS allowlist. Empty (default) emits no
	// CORS headers. Comma-separated exact origins, e.g. "https://app.example.com".
	CORSAllowedOrigins []string `env:"CORS_ALLOWED_ORIGINS" envSeparator:"," envDefault:""`
```

Then, in `run()`, add the engine-level registration between `accessLogMiddleware` and `registerRoutes`:

```go
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	r.Use(corsMiddleware(cfg.CORSAllowedOrigins))
	registerRoutes(r, handler, validator, cfg.DevMode)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 6: Lint**

Run: `make lint`
Expected: no new findings.

- [ ] **Step 7: Commit**

```bash
git add upload-service/middleware.go upload-service/main.go upload-service/middleware_test.go
git commit -m "feat(upload-service): credentialed CORS with an origin allowlist

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: `HandleSetCookie` handler + route registration

Add the handler that issues the `ssoToken` cookie and register it under the authenticated group. The auth gate validates the token before the handler runs.

**Files:**
- Modify: `upload-service/handler.go` (add `HandleSetCookie`)
- Modify: `upload-service/routes.go` (register `POST /setCookie`)
- Test: `upload-service/handler_test.go` (add handler test)

**Interfaces:**
- Consumes: `tokenFromRequest(c)` from Task 1.
- Produces: `func (h *Handler) HandleSetCookie(c *gin.Context)`.

- [ ] **Step 1: Write the failing test**

Add to `upload-service/handler_test.go`:

```go
func TestHandler_HandleSetCookie_SetsCookieAttributes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &Handler{}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/setCookie", nil)
	c.Request.Header.Set("ssoToken", "jwt-abc")

	h.HandleSetCookie(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"success":true}`, w.Body.String())

	setCookie := w.Header().Get("Set-Cookie")
	require.NotEmpty(t, setCookie)
	assert.Contains(t, setCookie, "ssoToken=jwt-abc")
	assert.Contains(t, setCookie, "Path=/")
	assert.Contains(t, setCookie, "HttpOnly")
	assert.Contains(t, setCookie, "Secure")
	assert.Contains(t, setCookie, "SameSite=None")
	assert.Contains(t, setCookie, "Partitioned")
}

func TestHandler_HandleSetCookie_FallsBackToCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &Handler{}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/setCookie", nil)
	c.Request.AddCookie(&http.Cookie{Name: "ssoToken", Value: "cookie-jwt"})

	h.HandleSetCookie(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Set-Cookie"), "ssoToken=cookie-jwt")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `h.HandleSetCookie undefined (type *Handler has no field or method HandleSetCookie)`.

- [ ] **Step 3: Implement the handler**

In `upload-service/handler.go`, add (near the other `Handle*` methods, e.g. after `HandleHealth`):

```go
// HandleSetCookie issues the ssoToken as a cross-site cookie so the browser can
// authenticate <img>-driven downloads, which cannot send the ssoToken header. It runs
// under authMiddleware, so the token is already validated by the time this executes.
// No Max-Age/Expires is set — a session cookie mirrors the token's transient nature, and
// re-issue is a single re-call of this endpoint. SameSite=None + Partitioned require the
// hand-built http.Cookie: Gin's c.SetCookie signature cannot express either attribute.
func (h *Handler) HandleSetCookie(c *gin.Context) {
	token := tokenFromRequest(c)
	http.SetCookie(c.Writer, &http.Cookie{
		Name:        "ssoToken",
		Value:       token,
		Path:        "/",
		HttpOnly:    true,
		Secure:      true,
		SameSite:    http.SameSiteNoneMode,
		Partitioned: true,
	})
	c.JSON(http.StatusOK, gin.H{"success": true})
}
```

`handler.go` already imports `net/http`, so no import change is needed.

- [ ] **Step 4: Register the route**

In `upload-service/routes.go`, add inside the `api := r.Group("/api/v1")` block, after the existing `api.*` lines:

```go
	api.POST("/setCookie", h.HandleSetCookie)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add upload-service/handler.go upload-service/routes.go upload-service/handler_test.go
git commit -m "feat(upload-service): add POST /api/v1/setCookie endpoint

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Docs (`client-api.md` + derived `request-reply.md`) + dev config (`docker-compose.yml`)

Document the new endpoint and the cookie-auth fallback in the canonical client API reference **and** its derived request/reply view, and expose the new env var in the local compose file.

Per `CLAUDE.md` (Documenting the Client API): a change to `docs/client-api.md` that touches the HTTP endpoints must update the derived view `docs/client-api/request-reply.md` in the same change set — the views must never drift. The events view `docs/client-api/events.md` needs **no** change here: `setCookie` emits no server→client events, and it documents only events.

**Files:**
- Modify: `docs/client-api.md` (canonical — §2.4 preamble + new `POST /api/v1/setCookie` subsection + cookie note on the two download endpoints)
- Modify: `docs/client-api/request-reply.md` (derived view — TOC line + condensed `POST /api/v1/setCookie` entry + cookie note on the two download entries)
- Modify: `upload-service/deploy/docker-compose.yml` (add `CORS_ALLOWED_ORIGINS=`)
- Unchanged (verify only): `docs/client-api/events.md` — no `setCookie` events.

**Interfaces:** none (docs/config only).

- [ ] **Step 1: Update the §2.4 preamble**

In `docs/client-api.md`, replace the §2.4 intro paragraph (the one beginning "HTTP endpoints on `upload-service` …") so it notes the cookie fallback and CORS. Change:

```
proxied to/from an internal Drive. All require the `ssoToken` header (validated
via OIDC) and that the caller is a member (has a subscription) of `:roomId`. Errors
use the standard [§6](#6-error-envelope-reference) envelope `{ code, reason?, error }`.
```

to:

```
proxied to/from an internal Drive. All require an OIDC-validated `ssoToken` — sent
as the `ssoToken` header, or (for browser `<img>` downloads that cannot set headers)
as an `ssoToken` cookie obtained from `POST /api/v1/setCookie` below; the header takes
precedence. Room-scoped endpoints also require that the caller is a member (has a
subscription) of `:roomId`. Cross-origin browsers are served credentialed CORS headers
only when their `Origin` is in the server's `CORS_ALLOWED_ORIGINS` allowlist. Errors use
the standard [§6](#6-error-envelope-reference) envelope `{ code, reason?, error }`.
```

- [ ] **Step 2: Add the `setCookie` subsection**

In `docs/client-api.md`, insert this block at the **start** of §2.4's endpoint list — immediately after the §2.4 preamble and before the `#### POST /api/v1/rooms/:roomId/upload/images` heading:

````markdown
#### POST /api/v1/setCookie

**Endpoint:** `POST /api/v1/setCookie`
**Reply:** synchronous HTTP response

Exchanges the `ssoToken` header for an `ssoToken` cookie so the browser can then load
protected files via `<img src>` (which cannot send headers). The token is validated
before the cookie is issued. Call this once after login, then again whenever a download
starts returning `401` (the cookie is a session cookie and the token can expire).

#### Request

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `ssoToken` | header | string | yes | OIDC-issued SSO token; validated before the cookie is set. |

Cross-origin callers must send the request with credentials (e.g. `fetch(..., { credentials: "include" })`) and be served from an origin in `CORS_ALLOWED_ORIGINS`.

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `success` | boolean | Always `true` on a 200. |

Response header:

```
Set-Cookie: ssoToken=<token>; Path=/; HttpOnly; Secure; SameSite=None; Partitioned
```

```json
{ "success": true }
```

#### Error response

Uses the [§6](#6-error-envelope-reference) envelope. HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 401 | `unauthenticated` | `invalid_sso_token` / `sso_token_expired` / `missing_fields` | `{ "code": "unauthenticated", "reason": "invalid_sso_token", "error": "invalid sso token" }` |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---
````

- [ ] **Step 3: Note the cookie fallback on the two download endpoints**

In `docs/client-api.md`, in the Request field tables of **`GET /api/v1/rooms/:roomId/file/:fileId`** and **`GET /api/v1/file-upload/:fileId/:fileName`**, change the `ssoToken` header row's Notes cell. Change:

```
| `ssoToken` | header | string | yes | OIDC-issued SSO token. |
```

to (in both tables):

```
| `ssoToken` | header/cookie | string | yes | OIDC-issued SSO token. Sent as the `ssoToken` header, or as the `ssoToken` cookie from `POST /api/v1/setCookie` (browser `<img>` downloads); header wins. |
```

- [ ] **Step 4: Add the TOC line in the derived view**

In `docs/client-api/request-reply.md`, in the "HTTP — Connection & Auth" table of contents block, add the `setCookie` link as the first HTTP endpoint entry — immediately after the `POST /auth` / `GET /api/userInfo` lines and before the `upload/images` line. Change:

```
   - [POST /api/v1/rooms/:roomId/upload/images](#post-apiv1roomsroomiduploadimages)
```

to:

```
   - [POST /api/v1/setCookie](#post-apiv1setcookie)
   - [POST /api/v1/rooms/:roomId/upload/images](#post-apiv1roomsroomiduploadimages)
```

(Leave the existing `POST /auth` and `GET /api/userInfo` TOC lines above unchanged.)

- [ ] **Step 5: Add the condensed `setCookie` entry in the derived view**

In `docs/client-api/request-reply.md`, insert this block in the "HTTP — Connection & Auth" section, immediately before the `### POST /api/v1/rooms/:roomId/upload/images` heading (so it sits right after the `GET /api/userInfo` entry's trailing `---`):

````markdown
### POST /api/v1/setCookie

**Endpoint:** `POST /api/v1/setCookie`
**Reply:** synchronous HTTP response

Exchanges the `ssoToken` header for an `ssoToken` cookie so the browser can load
protected files via `<img src>` (which cannot send headers). Token is validated before
the cookie is issued. Credentialed request; caller's `Origin` must be in the server's
`CORS_ALLOWED_ORIGINS` allowlist. See
[../client-api.md §2.4](../client-api.md#post-apiv1setcookie).

**Emits:** `None — HTTP-only.`

---
````

- [ ] **Step 6: Note the cookie fallback on the two download entries in the derived view**

In `docs/client-api/request-reply.md`, in the two download entries, append the cookie note to their `ssoToken` sentence. For `### GET /api/v1/rooms/:roomId/file/:fileId`, change:

```
Downloads a protected file (image/audio/video/document). `ssoToken` header
required; caller must be a room member. `drive_host` query param required.
```

to:

```
Downloads a protected file (image/audio/video/document). `ssoToken` required (header,
or the `ssoToken` cookie from `POST /api/v1/setCookie` for browser `<img>` downloads;
header wins); caller must be a room member. `drive_host` query param required.
```

For `### GET /api/v1/file-upload/:fileId/:fileName`, change:

```
collection, streamed from MinIO/S3); `fileName` is cosmetic. `ssoToken` header
required; caller must be a member of the file's room. See
```

to:

```
collection, streamed from MinIO/S3); `fileName` is cosmetic. `ssoToken` required
(header, or the `ssoToken` cookie from `POST /api/v1/setCookie` for browser `<img>`
downloads; header wins); caller must be a member of the file's room. See
```

- [ ] **Step 7: Add the env var to docker-compose**

In `upload-service/deploy/docker-compose.yml`, add to the `environment:` list (e.g. right after the `TLS_SKIP_VERIFY=false` line):

```yaml
      - CORS_ALLOWED_ORIGINS=${CORS_ALLOWED_ORIGINS:-}
```

- [ ] **Step 8: Verify the service still builds**

Run: `make build SERVICE=upload-service`
Expected: builds successfully (docs/compose changes don't affect the binary, but this confirms Tasks 1–3 compile together).

- [ ] **Step 9: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md upload-service/deploy/docker-compose.yml
git commit -m "docs(upload-service): document /setCookie and cookie auth; add CORS env

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Final verification

Run the full service test suite with coverage and the linters/SAST before hand-off.

**Files:** none (verification only).

- [ ] **Step 1: Run the service tests with race**

Run: `make test SERVICE=upload-service`
Expected: PASS.

- [ ] **Step 2: Check coverage of the new code**

Run: `go test -coverprofile=coverage.out ./upload-service/... && go tool cover -func=coverage.out | grep -E "corsMiddleware|tokenFromRequest|HandleSetCookie"`
Expected: each new function ≥ 90%. (This is a read-only inspection; it does not replace `make test`.)

- [ ] **Step 3: Lint and SAST**

Run: `make lint && make sast-gosec`
Expected: no new findings. (The `http.Cookie{Secure:true}` cookie is intentional; no suppression should be needed.)

- [ ] **Step 4: Push the branch**

```bash
git push -u origin claude/upload-service-setcookie-e1o6nk
```
Expected: branch updated on origin. Do NOT open a PR unless explicitly asked.

---

## Self-Review

- **Spec coverage:** config field → T2; `corsMiddleware` → T2; cookie fallback → T1; `HandleSetCookie` → T3; route + engine wiring → T2/T3; canonical `client-api.md` + derived `request-reply.md` (events.md unaffected — no events) → T4; docker-compose → T4; tests → T1/T2/T3 + T5. All spec sections mapped.
- **Placeholder scan:** every code/step is concrete; no TBD/TODO/"handle edge cases".
- **Type consistency:** `tokenFromRequest(c *gin.Context) string` defined in T1, consumed identically in T1 (`authMiddleware`) and T3 (`HandleSetCookie`). `corsMiddleware(allowed []string) gin.HandlerFunc` defined and called with `cfg.CORSAllowedOrigins` ([]string) in T2. `HandleSetCookie` is a `*Handler` method, matching the `h.HandleSetCookie` route registration. Cookie name `ssoToken` consistent across helper, handler, and tests.
