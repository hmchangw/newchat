# upload-service `/file/setCookie` + cookie-based auth — design

**Date:** 2026-07-03
**Service:** `upload-service`
**Scope:** Backend only. The frontend flow below is context for *why*; no frontend work is in scope.

## Problem

The frontend loads protected files/images with `<img src="…/api/v1/rooms/:roomId/file/:fileId">`.
A browser cannot attach custom headers to an `<img>` request, so it cannot send the
`ssoToken` header that `upload-service` currently requires. It *can* send a cookie.

We need a way to hand the browser an `ssoToken` cookie, and we need the download
endpoints to accept identity from that cookie. Because the frontend is served from a
different origin than `upload-service`, the exchange is cross-origin and credentialed,
which requires explicit CORS handling.

### End-to-end flow (frontend is context only)

```
1. fetch POST https://upload/api/v1/file/setCookie
     headers: { ssoToken: <jwt> }, credentials: "include"
   → authMiddleware validates the JWT
   → handler emits: Set-Cookie: ssoToken=<jwt>; SameSite=None; Secure; Partitioned; HttpOnly; Path=/
   → 200 {"success": true}

2. <img src="https://upload/api/v1/rooms/:roomId/file/:fileId?drive_host=…" crossorigin>
   → browser auto-sends the ssoToken cookie (SameSite=None ⇒ sent cross-site)
   → authMiddleware reads the cookie (header absent), validates, checks membership
   → streams the bytes
```

## Design

All changes are in the `upload-service` directory.

### 1. Config (`main.go`)

Add one field to `config`:

```go
CORSAllowedOrigins []string `env:"CORS_ALLOWED_ORIGINS" envSeparator:"," envDefault:""`
```

Empty (the default) ⇒ no CORS headers are ever emitted, so the feature is opt-in and
existing deployments are unaffected. Ops populates it with the frontend origin(s).

### 2. CORS middleware (`middleware.go`)

New `corsMiddleware(allowed []string) gin.HandlerFunc`. For each request:

- Read the `Origin` request header.
- If `Origin` is in `allowed`, set:
  - `Access-Control-Allow-Origin: <that origin>` (echoed, never `*`)
  - `Access-Control-Allow-Credentials: true`
  - `Vary: Origin` (cache correctness — response varies by origin)
  - `Access-Control-Allow-Methods: GET, POST, OPTIONS`
  - `Access-Control-Allow-Headers: Content-Type, ssoToken, X-Request-ID`
  - `Access-Control-Max-Age: 300`
- If `Origin` is absent or not allowed: emit **no** CORS headers (browser blocks the read).
- If method is `OPTIONS`: `c.AbortWithStatus(http.StatusNoContent)` (204) — preflight ends here.

Wildcard origin from the existing `pkg/ginutil.CORS` / `media-service` middleware is
deliberately **not** reused: `Access-Control-Allow-Origin: *` is illegal together with
`Access-Control-Allow-Credentials: true`. The allowlist is pre-hashed into a set at
construction; lookup is exact string match, no normalization.

**Wiring — engine level, not the group.** `corsMiddleware` is registered with
`r.Use(...)` in `main.go` (mirroring `media-service`), *not* on the `/api/v1` group. This
is required for preflight to work: a preflight `OPTIONS` matches no registered route, so
Gin routes it to `NoRoute`, which runs only **engine-level** middleware — group
middleware never fires. Registering at the engine level guarantees the preflight is
answered (204) before, and independently of, the group's `authMiddleware`, so preflight
carries no token.

### 3. Cookie fallback in `authMiddleware` (`middleware.go`)

Extract token acquisition into a small helper shared by the middleware and the
`setCookie` handler (DRY — both need identical header-then-cookie semantics):

```go
// tokenFromRequest returns the ssoToken from the header, falling back to the
// ssoToken cookie (<img>-driven download requests cannot set headers).
func tokenFromRequest(c *gin.Context) string {
    if t := c.GetHeader("ssoToken"); t != "" {
        return t
    }
    t, _ := c.Cookie("ssoToken")
    return t
}
```

`authMiddleware` uses `token := tokenFromRequest(c)` in place of the bare
`c.GetHeader("ssoToken")`. Header-first keeps every existing header-based caller
byte-for-byte identical; the cookie is a pure fallback. The rest of the middleware
(empty-token 401, dev-mode passthrough, OIDC validate, claims → `AuthenticatedUser`)
is unchanged.

### 4. `HandleSetCookie` handler (`handler.go`)

Registered under `authMiddleware`, so by the time it runs the token is already
validated (invalid/expired/missing never reach it — the middleware has already written
the 401). The handler re-reads the validated token via the same `tokenFromRequest`
helper and writes the cookie:

```go
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

`http.SetCookie` with a hand-built `http.Cookie` is used because Gin's `c.SetCookie`
signature cannot express `SameSite=None` or `Partitioned`. No `Max-Age`/`Expires`
⇒ a session cookie, matching the target `Set-Cookie` string exactly. Token lifetime is
still enforced on every subsequent request by the OIDC validator, so a session cookie
outliving the token only means the next request 401s and the frontend re-runs step 1.

### 5. Wiring (`main.go` + `routes.go`)

`main.go` registers CORS at the engine level (see §2), leaving the group untouched:

```go
r.Use(requestIDMiddleware())
r.Use(accessLogMiddleware())
r.Use(corsMiddleware(cfg.CORSAllowedOrigins)) // engine-level so preflight OPTIONS is answered
registerRoutes(r, handler, validator, cfg.DevMode)
```

`routes.go` only adds the new route to the existing group (its signature is unchanged).
The route lives under the `/file` prefix, alongside the sibling upload/download endpoints:

```go
api.POST("/file/setCookie", h.HandleSetCookie)
// … existing routes unchanged …
```

## Error handling

| Case | Result |
|------|--------|
| `/file/setCookie` with missing `ssoToken` header | 401 `AuthMissingFields` (authMiddleware, handler never runs) |
| `/file/setCookie` with invalid/expired token | 401 `AuthInvalidToken` / `AuthTokenExpired` (authMiddleware) |
| Download with expired/invalid cookie token | 401 via the same validator path, token sourced from cookie |
| Request from a disallowed origin | No CORS headers; browser blocks the cross-origin read |
| Preflight `OPTIONS` | 204, no body, no auth |

No new `errcode` reasons are introduced; all failures reuse the existing auth codes.

## Testing (TDD, table-driven)

`middleware_test.go`:
- `corsMiddleware`: allowed origin → all headers present incl. `Allow-Credentials: true`
  and echoed origin; disallowed origin → no CORS headers; absent origin → none;
  `OPTIONS` from allowed origin → 204.
- `authMiddleware`: header-only (unchanged), cookie-only (new fallback), header-wins-when-
  both-present, neither → 401. Covers dev-mode and validated-mode via the existing fake
  validator.

`handler_test.go`:
- `HandleSetCookie`: valid token → 200 `{"success":true}` and the response `Set-Cookie`
  asserts every attribute — `HttpOnly`, `Secure`, `SameSite=None`, `Partitioned`,
  `Path=/`, and `Value` == token. (401 paths are owned by the middleware tests since the
  handler is gated.)

Coverage stays ≥80% (target 90% for the new handler/middleware). Run with `-race` via
`make test SERVICE=upload-service`.

## Documentation & config deliverables

- **`docs/client-api.md` §2.4** (canonical) — add a `POST /api/v1/file/setCookie` subsection
  (Endpoint, Request table, Success response with the exact `Set-Cookie` string, Error
  table, and the `Triggered events` = None footers) matching the style of the sibling
  upload/download endpoints. Also amend the §2.4 preamble and the two download endpoints'
  `ssoToken` header rows to note the token may arrive **via the `ssoToken` cookie** as a
  fallback to the header (that's what makes `<img>` downloads work). No CORS
  request/response field belongs in the field tables — CORS is a transport concern,
  mentioned in prose only.
- **`docs/client-api/request-reply.md`** (derived view) — the canonical doc's HTTP
  endpoints are mirrored here in condensed form. Per `CLAUDE.md`, the derived view must
  not drift, so add a TOC line + a condensed `setCookie` entry (linking back to §2.4) and
  the same cookie-fallback note on the two download entries.
- **`docs/client-api/events.md`** — **no change**: `setCookie` emits no server→client
  events, and this view documents only events.
- **`upload-service/deploy/docker-compose.yml`** — add `CORS_ALLOWED_ORIGINS=` (empty by
  default in dev) so the service can be configured locally.

## Out of scope / notes

- `Secure` cookies are not transmitted over plain HTTP, so the flow only works end-to-end
  over HTTPS. This matches the required `Set-Cookie` string; no dev-mode special-casing.
