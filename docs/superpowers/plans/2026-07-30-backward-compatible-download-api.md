# Backward-compatible `/api/v3` protected-image download — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an authenticated `GET /api/v3/rooms/:roomId/protected-image/:fileId?drive_host=<host>` endpoint to `upload-service` that streams inline images for legacy message data from a separate (legacy) Drive backend.

**Architecture:** The new endpoint reuses the existing protected-file download logic verbatim; the only difference is which Drive client (and therefore which api-token/config) serves the request. A second `drive.Config` (env prefix `LEGACY_DRIVE_`) builds a second `*drive.Client`, injected into the handler as `legacyDrive`. The download body of `HandleDownloadFile` is extracted into a shared `downloadFrom(c, dc)` so v1 and v3 share one implementation.

**Tech Stack:** Go 1.25, Gin, Resty, `caarlos0/env`, `stretchr/testify`, `go.uber.org/mock`.

## Global Constraints

- Go 1.25; use `make` targets, never raw `go` (`make test SERVICE=upload-service`, `make lint`, `make sast`).
- All config via `caarlos0/env` typed struct — no `os.Getenv`. `SCREAMING_SNAKE_CASE` env names.
- Client-facing errors via `pkg/errcode` + `errhttp.Write`; infra failures via `fmt.Errorf("...: %w", err)`.
- TDD: no production code without a failing test first. `-race` always (Makefile handles it).
- Minimum 80% coverage; do not add new third-party dependencies.
- No new error codes — reuse existing `bad_request` / `forbidden` / `internal` / `unavailable`.
- Client-facing HTTP endpoint → update `docs/client-api.md` and `docs/client-api/request-reply.md` in the same PR.

## File Structure

- `upload-service/handler.go` — add `legacyDrive` field, append `NewHandler` param, extract `downloadFrom`, add `HandleDownloadProtectedImageV3`.
- `upload-service/routes.go` — register the `/api/v3` group + route.
- `upload-service/main.go` — add `LegacyDrive drive.Config`, load its base URLs, build the legacy client, pass to `NewHandler`.
- `upload-service/deploy/docker-compose.yml` — add `LEGACY_DRIVE_*` env vars.
- `upload-service/handler_test.go` — new v3 tests + update `NewHandler` call sites; `handler_setcookie_test.go` — update its `NewHandler` call site.
- `docs/client-api.md` — new §2.4 download subsection.
- `docs/client-api/request-reply.md` — matching derived entry + TOC.

`pkg/drive/*` is unchanged — `drive.Config`, `LoadBaseURLs`, and `NewClient` already support a second independent backend via env prefix.

---

### Task 1: v3 protected-image download endpoint (legacy Drive backend)

**Files:**
- Modify: `upload-service/handler.go`
- Modify: `upload-service/routes.go`
- Modify: `upload-service/main.go`
- Modify: `upload-service/deploy/docker-compose.yml`
- Test: `upload-service/handler_test.go`, `upload-service/handler_setcookie_test.go`

**Interfaces:**
- Consumes: existing `driveClient` interface (`GetGroupImage(host, groupID, fileID string) (*drive.GetGroupImageResponse, error)`), `drive.Config`, `drive.NewClient(*drive.Config) *drive.Client`, `(*drive.Config).LoadBaseURLs()`, `requireMembership`, `userFromContext`, `logCtx`, `contentDisposition`.
- Produces:
  - `NewHandler(store Store, dc driveClient, s3 objectStore, maxImages, maxAttachments int, maxImageSize, maxFileSize int64, mimeFilter *mediaTypeFilter, preview previewFunc, cacheMaxAge int, setCookiePartitioned bool, legacyDrive driveClient) *Handler` — note the appended trailing `legacyDrive` param.
  - `(*Handler).HandleDownloadProtectedImageV3(c *gin.Context)` — Gin handler.
  - `(*Handler).downloadFrom(c *gin.Context, dc driveClient)` — shared unexported download body.
  - Route: `GET /api/v3/rooms/:roomId/protected-image/:fileId`.
  - Config field `LegacyDrive drive.Config` (env prefix `LEGACY_DRIVE_`).

- [ ] **Step 1: Write the failing tests**

Add to `upload-service/handler_test.go` (place after the existing `TestDownload_Success_NoFilename` test, before `multipartTyped`):

```go
// newHandlerWithLegacy wires a handler with a distinct primary and legacy Drive
// client so v3 download tests can prove which backend served the request.
func newHandlerWithLegacy(store Store, dc, legacy driveClient) *Handler {
	return NewHandler(store, dc, &fakeS3{}, testMaxImages, testMaxAttachments, testMaxImageSize, 0, nil, nil, testCacheMaxAge, true, legacy)
}

func TestDownloadV3_MissingRoomID_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newDownloadCtx(t, "", "f1", "https://legacy.example.com", okUser())
	h.HandleDownloadProtectedImageV3(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestDownloadV3_MissingFileID_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "", "https://legacy.example.com", okUser())
	h.HandleDownloadProtectedImageV3(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestDownloadV3_MissingDriveHost_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "f1", "", okUser())
	h.HandleDownloadProtectedImageV3(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "bad_request", decodeErr(t, w).Code)
}

func TestDownloadV3_NoUser_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "f1", "https://legacy.example.com", nil)
	h.HandleDownloadProtectedImageV3(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Equal(t, "internal", decodeErr(t, w).Code)
}

func TestDownloadV3_NotMember_403(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(false, nil)
	h := newHandler(store, &fakeDrive{})
	c, w := newDownloadCtx(t, "r1", "f1", "https://legacy.example.com", okUser())
	h.HandleDownloadProtectedImageV3(c)
	assert.Equal(t, http.StatusForbidden, w.Code)
	env := decodeErr(t, w)
	assert.Equal(t, "forbidden", env.Code)
	assert.Equal(t, "not_room_member", env.Reason)
}

func TestDownloadV3_DriveError_503(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	legacy := &fakeDrive{getErr: errors.New("image not found")}
	h := newHandlerWithLegacy(store, &fakeDrive{}, legacy)
	c, w := newDownloadCtx(t, "r1", "f1", "https://legacy.example.com", okUser())
	h.HandleDownloadProtectedImageV3(c)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "unavailable", decodeErr(t, w).Code)
}

// TestDownloadV3_UsesLegacyDrive proves the v3 endpoint serves bytes from the
// legacy Drive client (its own api-token/config), leaving the primary untouched.
func TestDownloadV3_UsesLegacyDrive(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().IsMember(gomock.Any(), "r1", "alice").Return(true, nil)
	primary := &fakeDrive{getResp: &drive.GetGroupImageResponse{
		Reader: readCloser{strings.NewReader("PRIMARY")}, ContentType: "image/png", ContentLength: 7,
	}}
	legacy := &fakeDrive{getResp: &drive.GetGroupImageResponse{
		Reader: readCloser{strings.NewReader("LEGACY")}, ContentType: "image/png", ContentLength: 6, Filename: "old.png",
	}}
	h := newHandlerWithLegacy(store, primary, legacy)
	c, w := newDownloadCtx(t, "r1", "f1", "https://legacy.example.com", okUser())
	h.HandleDownloadProtectedImageV3(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "LEGACY", w.Body.String())
	assert.Equal(t, "image/png", w.Header().Get("Content-Type"))
	// Routed to the legacy client with the request's drive_host/room/file...
	assert.Equal(t, "https://legacy.example.com", legacy.getGot.host)
	assert.Equal(t, "r1", legacy.getGot.groupID)
	assert.Equal(t, "f1", legacy.getGot.fileID)
	// ...and the primary client was never called.
	assert.Equal(t, "", primary.getGot.fileID)
	assert.Equal(t, "default-src 'none'", w.Header().Get("Content-Security-Policy"))
	assert.Equal(t, "private, max-age=604800", w.Header().Get("Cache-Control"))
	assert.Equal(t, "attachment; filename*=UTF-8''old.png", w.Header().Get("Content-Disposition"))
}

// TestDownloadV3_RouteRegistered proves the /api/v3 route is wired and auth-gated
// (401 without a token, not a 404 for an unregistered path).
func TestDownloadV3_RouteRegistered(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newHandler(NewMockStore(ctrl), &fakeDrive{})
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, h, nil, true)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet,
		"/api/v3/rooms/r1/protected-image/f1?drive_host=https://legacy.example.com", nil))
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}
```

- [ ] **Step 2: Run tests to verify they fail (compile failure — feature missing)**

Run: `make test SERVICE=upload-service`
Expected: FAIL — `too many arguments in call to NewHandler` and `h.HandleDownloadProtectedImageV3 undefined`.

- [ ] **Step 3: Add the `legacyDrive` field to the `Handler` struct**

In `upload-service/handler.go`, add the field right after `drive`:

```go
// Handler holds the upload-service dependencies.
type Handler struct {
	store          Store
	drive          driveClient
	legacyDrive    driveClient
	s3             objectStore
```

- [ ] **Step 4: Append `legacyDrive` to `NewHandler` and set it**

Replace the `NewHandler` doc comment, signature, and struct literal in `upload-service/handler.go`:

```go
// NewHandler wires the handler dependencies. maxImages/maxImageSize gate the image
// endpoint; maxAttachments/maxFileSize/mimeFilter/preview gate the file endpoint; s3
// backs the MinIO/S3 download endpoint; cacheMaxAge is its Cache-Control max-age in
// seconds; setCookiePartitioned gates the Partitioned attribute on HandleSetCookie;
// legacyDrive serves the backward-compatible /api/v3 protected-image download from a
// separate (legacy) Drive backend with its own URL/api-token.
func NewHandler(store Store, dc driveClient, s3 objectStore, maxImages, maxAttachments int, maxImageSize, maxFileSize int64,
	mimeFilter *mediaTypeFilter, preview previewFunc, cacheMaxAge int, setCookiePartitioned bool, legacyDrive driveClient) *Handler {
	return &Handler{
		store: store, drive: dc, legacyDrive: legacyDrive, s3: s3, maxImages: maxImages, maxAttachments: maxAttachments,
		maxImageSize: maxImageSize, maxFileSize: maxFileSize, mimeFilter: mimeFilter,
		preview: preview, cacheMaxAge: cacheMaxAge, setCookiePartitioned: setCookiePartitioned,
		nowMilli: func() int64 { return time.Now().UTC().UnixMilli() },
	}
}
```

- [ ] **Step 5: Extract `downloadFrom` and add both handlers**

In `upload-service/handler.go`, replace the entire `HandleDownloadFile` function with:

```go
// HandleDownloadFile proxies a protected file: it resolves a signed URL from
// Drive, fetches the bytes, and streams them straight to the client.
func (h *Handler) HandleDownloadFile(c *gin.Context) {
	h.downloadFrom(c, h.drive)
}

// HandleDownloadProtectedImageV3 is the backward-compatible /api/v3 download used
// by the frontend to fetch inline images referenced by legacy message data (from a
// prior system version). It is identical to HandleDownloadFile except it proxies
// from the legacy Drive backend, which has its own URL and api-token.
func (h *Handler) HandleDownloadProtectedImageV3(c *gin.Context) {
	h.downloadFrom(c, h.legacyDrive)
}

// downloadFrom resolves a signed URL from the given Drive client, fetches the
// bytes, and streams them straight to the client. Both the v1 and v3 download
// endpoints share this logic, differing only in which Drive backend serves them.
func (h *Handler) downloadFrom(c *gin.Context, dc driveClient) {
	ctx := logCtx(c)

	roomID := c.Param("roomId")
	if roomID == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("roomId is required"))
		return
	}
	fileID := c.Param("fileId")
	if fileID == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("fileId is required"))
		return
	}
	driveHost := c.Query("drive_host")
	if driveHost == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("drive_host is required"))
		return
	}

	user, ok := userFromContext(c)
	if !ok {
		errhttp.Write(ctx, c, errcode.Internal("user not authenticated"))
		return
	}

	if !h.requireMembership(ctx, c, roomID, user.Account) {
		return
	}

	img, err := dc.GetGroupImage(driveHost, roomID, fileID)
	if err != nil {
		errhttp.Write(ctx, c, errcode.Unavailable("failed to retrieve file", errcode.WithCause(err)))
		return
	}
	defer img.Reader.Close()

	// Download headers mirror the MinIO/S3 path: force-download, lock down
	// execution, and allow private (per-user) caching only — auth + membership gated.
	extraHeaders := map[string]string{
		"Content-Disposition":     contentDisposition(img.Filename),
		"Content-Security-Policy": "default-src 'none'",
		"Cache-Control":           fmt.Sprintf("private, max-age=%d", h.cacheMaxAge),
	}
	c.DataFromReader(http.StatusOK, img.ContentLength, img.ContentType, img.Reader, extraHeaders)
}
```

- [ ] **Step 6: Register the `/api/v3` route**

In `upload-service/routes.go`, replace the tail of `registerRoutes` (from the `file-upload` line to the closing brace):

```go
	api.GET("/file/rooms/:roomId/file/:fileId", h.HandleDownloadFile)
	api.GET("/file-upload/:fileId/:fileName", h.HandleDownloadMinioS3File)

	// v3 serves the backward-compatible protected-image download for legacy
	// message data, proxied from a separate (legacy) Drive backend.
	apiV3 := r.Group("/api/v3")
	apiV3.Use(authMiddleware(v, devMode))
	apiV3.GET("/rooms/:roomId/protected-image/:fileId", h.HandleDownloadProtectedImageV3)
}
```

- [ ] **Step 7: Update the existing `NewHandler` call sites in tests**

In `upload-service/handler_test.go`, every existing `NewHandler(...)` call that ends with `, testCacheMaxAge, true)` must gain a trailing default legacy fake: change the suffix `, testCacheMaxAge, true)` to `, testCacheMaxAge, true, &fakeDrive{})`. This affects the `newHandler` helper and the direct-construction call sites (image limit, per-image ceiling, file-size, S3 download tests, etc.). Do NOT touch the `newHandlerWithLegacy` line added in Step 1 (it already passes `legacy`).

In `upload-service/handler_setcookie_test.go`, change the single `NewHandler(...)` call's suffix `testCacheMaxAge, tt.partitioned)` to `testCacheMaxAge, tt.partitioned, &fakeDrive{})`.

- [ ] **Step 8: Wire the legacy Drive config and client in `main.go`**

In `upload-service/main.go`, add the config field after the existing `Drive` field:

```go
	Drive drive.Config `envPrefix:"DRIVE_"`
	// LegacyDrive backs the backward-compatible /api/v3 protected-image download
	// used for legacy message data. It is a separate Drive backend with its own
	// URL/API_TOKEN/baseurls.json — point LEGACY_DRIVE_BASE_URL_CONFIG_PATH at a
	// distinct file (its envDefault matches Drive's).
	LegacyDrive drive.Config `envPrefix:"LEGACY_DRIVE_"`
}
```

Load its base URLs next to the primary:

```go
	ctx := context.Background()
	cfg.Drive.LoadBaseURLs()
	cfg.LegacyDrive.LoadBaseURLs()
```

Build the legacy client next to the primary:

```go
	store := NewMongoStore(mongoClient.Database(cfg.MongoDB))
	driveClient := drive.NewClient(&cfg.Drive)
	legacyDriveClient := drive.NewClient(&cfg.LegacyDrive)
```

Pass it as the final `NewHandler` argument:

```go
	handler := NewHandler(store, driveClient, s3Store, cfg.MaxImages, cfg.MaxAttachments, cfg.MaxImageSizeBytes,
		cfg.FileUploadMaxFileSize, mimeFilter, imagePreview, cfg.FileDownloadCacheMaxAgeSeconds, cfg.SetCookiePartitioned,
		legacyDriveClient)
```

- [ ] **Step 9: Add the `LEGACY_DRIVE_*` env vars to the deploy compose file**

In `upload-service/deploy/docker-compose.yml`, after the three `DRIVE_*` lines:

```yaml
      - DRIVE_URL=${DRIVE_URL:-http://drive-mock:9000}
      - DRIVE_API_TOKEN=${DRIVE_API_TOKEN:-dev-token}
      - DRIVE_BASE_URL_CONFIG_PATH=/etc/config/baseurls.json
      # Legacy Drive backend for the backward-compatible /api/v3 protected-image
      # download. Distinct baseurls.json from the primary Drive above.
      - LEGACY_DRIVE_URL=${LEGACY_DRIVE_URL:-http://legacy-drive-mock:9000}
      - LEGACY_DRIVE_API_TOKEN=${LEGACY_DRIVE_API_TOKEN:-dev-legacy-token}
      - LEGACY_DRIVE_BASE_URL_CONFIG_PATH=/etc/config/legacy-baseurls.json
```

- [ ] **Step 10: Run tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: PASS — `ok  github.com/hmchangw/chat/upload-service`.

- [ ] **Step 11: Lint and SAST**

Run: `make lint` then `make sast`
Expected: both pass (no new medium+ findings; the `InsecureSkipVerify` in `pkg/drive` is pre-existing and unchanged).

- [ ] **Step 12: Commit**

```bash
git add upload-service/handler.go upload-service/routes.go upload-service/main.go \
  upload-service/deploy/docker-compose.yml upload-service/handler_test.go \
  upload-service/handler_setcookie_test.go
git commit -m "feat(upload-service): backward-compatible /api/v3 protected-image download"
```

---

### Task 2: Document the v3 endpoint in the client API docs

**Files:**
- Modify: `docs/client-api.md` (§2.4, after the `GET /api/v1/file/rooms/:roomId/file/:fileId` subsection)
- Modify: `docs/client-api/request-reply.md` (TOC + new entry after the v1 download entry)

**Interfaces:**
- Consumes: the wire behavior implemented in Task 1 (route, params, status codes).
- Produces: documentation only — no code.

- [ ] **Step 1: Add the v3 subsection to `docs/client-api.md`**

In `docs/client-api.md`, immediately after the `---` that closes the `GET /api/v1/file/rooms/:roomId/file/:fileId` subsection (right before `#### GET /api/v1/file-upload/:fileId/:fileName`), insert:

```markdown
#### GET /api/v3/rooms/:roomId/protected-image/:fileId

**Endpoint:** `GET /api/v3/rooms/:roomId/protected-image/:fileId`
**Reply:** synchronous HTTP response (raw file bytes, not JSON)

Backward-compatible download for inline images referenced by **legacy message
data** (produced by a prior system version). Behaves exactly like
`GET /api/v1/file/rooms/:roomId/file/:fileId` above, except the bytes are proxied
from a **separate (legacy) Drive backend** with its own credentials. The client
calls this with the old-style path preserved in legacy messages.

#### Request

| Field | Source | Type | Required | Notes |
|---|---|---|---|---|
| `ssoToken` | header/cookie | string | yes | OIDC-issued SSO token. Sent as the `ssoToken` header, or as the `ssoToken` cookie from `POST /api/v1/file/setCookie` (browser `<img>` downloads); header wins. |
| `roomId` | path | string | yes | Room the image belongs to; the caller must be a member. |
| `fileId` | path | string | yes | Legacy Drive file ID (from the original message data). |
| `drive_host` | query | string | yes | Legacy Drive base URL carried in the legacy message data. |

#### Success response

`HTTP 200` — raw image binary streamed directly (not JSON), with the upstream
`Content-Type` (defaulting to `application/octet-stream`).

#### Error response

See [Error envelope](#6-error-envelope-reference). HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | — | `{ "code": "bad_request", "error": "drive_host is required" }` — also `roomId is required`, `fileId is required`. |
| 401 | `unauthenticated` | `invalid_sso_token` / `sso_token_expired` / `missing_fields` | `{ "code": "unauthenticated", "reason": "invalid_sso_token", "error": "invalid sso token" }` |
| 403 | `forbidden` | `not_room_member` | `{ "code": "forbidden", "reason": "not_room_member", "error": "user alice is not in room abc123" }` |
| 500 | `internal` | — | `{ "code": "internal", "error": "internal error" }` — user missing in context. |
| 503 | `unavailable` | — | `{ "code": "unavailable", "error": "failed to retrieve file" }` — Drive signer/download failure. |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`

---
```

- [ ] **Step 2: Add the v3 entry + TOC line to `docs/client-api/request-reply.md`**

In the TOC list (the block containing `- [GET /api/v1/file/rooms/:roomId/file/:fileId](#get-apiv1fileroomsroomidfilefileid)`), add a line directly after it:

```markdown
   - [GET /api/v3/rooms/:roomId/protected-image/:fileId](#get-apiv3roomsroomidprotected-imagefileid)
```

Then, after the `---` that closes the `### GET /api/v1/file/rooms/:roomId/file/:fileId` entry (before `### GET /api/v1/file-upload/:fileId/:fileName`), insert:

```markdown
### GET /api/v3/rooms/:roomId/protected-image/:fileId

**Endpoint:** `GET /api/v3/rooms/:roomId/protected-image/:fileId`
**Reply:** synchronous HTTP response (raw file bytes, any type)

Backward-compatible download for inline images in **legacy message data** (prior
system version). Identical to `GET /api/v1/file/rooms/:roomId/file/:fileId` but
proxied from a separate (legacy) Drive backend with its own credentials.
`ssoToken` required (header, or the `ssoToken` cookie from `POST /api/v1/file/setCookie`
for browser `<img>` downloads; header wins); caller must be a room member.
`drive_host` query param required. See
[../client-api.md §2.4](../client-api.md#get-apiv3roomsroomidprotected-imagefileid).

**Emits:** `None — HTTP-only.`

---
```

- [ ] **Step 3: Verify anchor consistency**

Confirm the cross-link anchor `#get-apiv3roomsroomidprotected-imagefileid` matches the generated slug of the new `docs/client-api.md` heading (GitHub lowercases, drops non-alphanumerics except hyphens, and turns spaces into hyphens). The heading `GET /api/v3/rooms/:roomId/protected-image/:fileId` → slug `get-apiv3roomsroomidprotected-imagefileid`.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs(client-api): document GET /api/v3 protected-image download"
```

---

## Self-Review

**Spec coverage:**
- New route `GET /api/v3/rooms/:roomId/protected-image/:fileId?drive_host` → Task 1 Steps 5–6. ✓
- Second `drive.Config` with `LEGACY_DRIVE_` prefix + separate client → Task 1 Steps 4, 8. ✓
- Shared `downloadFrom` extraction; `legacyDrive` as last `NewHandler` param → Task 1 Steps 3–5. ✓
- Deploy env vars with distinct baseurls path → Task 1 Step 9. ✓
- TDD test coverage (missing params, no user, not member, drive error, legacy-routing, route wiring) → Task 1 Step 1. ✓
- Docs (client-api.md + request-reply.md) → Task 2. ✓

**Placeholder scan:** No TBD/TODO; every code and doc step contains the full literal content. ✓

**Type consistency:** `NewHandler` trailing `legacyDrive driveClient` param, `HandleDownloadProtectedImageV3`, and `downloadFrom(c, dc)` are used identically across Steps 4, 5, 6, 7, 8, and the tests. ✓
