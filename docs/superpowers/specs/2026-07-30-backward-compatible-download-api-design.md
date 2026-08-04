# Backward-compatible `/api/v3` protected-image download

**Date:** 2026-07-30
**Service:** `upload-service`
**Branch:** `claude/backward-compatible-download-api-kctu16`

## Goal

Add an authenticated route that lets the frontend download inline images
referenced by **legacy message data** (produced by a prior system version, not
stored in this repo) using their old path shape. The proxy logic is identical to
the existing `HandleDownloadFile`, but it must talk to a **different (legacy)
Drive backend** with its own URL / API_TOKEN / baseurls.json.

**New route:** `GET /api/v3/rooms/:roomId/protected-image/:fileId?drive_host=<host>`

## Why a second Drive client is required

`HandleDownloadFile` calls `h.drive.GetGroupImage(driveHost, roomID, fileID)`.
Although `driveHost` comes from the query param, `GetGroupImage` signs the
request with the **client's own `api-token`** (from its `drive.Config`). The
legacy Drive uses a different token, so the endpoint needs a second
`*drive.Client` built from a second `drive.Config`. The second config carries the
full set (URL, API_TOKEN, baseurls.json) exactly like the existing one.

## Changes

All in `upload-service`; `pkg/drive` is untouched.

### 1. Config (`main.go`)

Add a second `drive.Config` with env prefix `LEGACY_DRIVE_`:

```go
LegacyDrive drive.Config `envPrefix:"LEGACY_DRIVE_"`
```

Yields `LEGACY_DRIVE_URL`, `LEGACY_DRIVE_API_TOKEN`,
`LEGACY_DRIVE_BASE_URL_CONFIG_PATH`. In `run()`: call
`cfg.LegacyDrive.LoadBaseURLs()` and build
`legacyDriveClient := drive.NewClient(&cfg.LegacyDrive)`, then pass it to
`NewHandler`.

> Operators **must** point `LEGACY_DRIVE_BASE_URL_CONFIG_PATH` at a *distinct*
> baseurls.json — its `envDefault` matches the primary
> (`etc/config/baseurls.json`), so leaving it unset loads the same file. The
> deploy compose file sets a distinct path.

### 2. Handler (`handler.go`)

- Add field `legacyDrive driveClient`, appended as the last `NewHandler` param.
- Extract the shared body of `HandleDownloadFile` into an unexported
  `downloadFrom(c *gin.Context, dc driveClient)`.
- `HandleDownloadFile` calls `h.downloadFrom(c, h.drive)`.
- New `HandleDownloadProtectedImageV3` calls `h.downloadFrom(c, h.legacyDrive)`.

Zero logic duplication; the only difference between the two endpoints is which
Drive client (and therefore which api-token) serves the request.

### 3. Routes (`routes.go`)

```go
apiV3 := r.Group("/api/v3")
apiV3.Use(authMiddleware(v, devMode))
apiV3.GET("/rooms/:roomId/protected-image/:fileId", h.HandleDownloadProtectedImageV3)
```

### 4. Deploy (`deploy/docker-compose.yml`)

Add `LEGACY_DRIVE_URL`, `LEGACY_DRIVE_API_TOKEN`, and a distinct
`LEGACY_DRIVE_BASE_URL_CONFIG_PATH`.

## Behavior / error handling

Identical to `HandleDownloadFile`: 400 on missing `roomId`/`fileId`/`drive_host`,
401 unauthenticated, 403 not member, 500 no user, 503 on Drive failure; otherwise
stream the bytes with the same download headers. No new error codes.

## Testing (TDD)

- Update the `newHandler` test helper and the direct `NewHandler(...)` call sites
  to pass a legacy `fakeDrive` (mechanical).
- New tests for `HandleDownloadProtectedImageV3` mirroring the existing
  `TestDownload_*` cases (missing params, no user, not member, drive error,
  happy-path streaming).
- One test asserting the v3 handler routes to the **legacy** Drive client, not
  the primary (distinct `fakeDrive` instances; assert which received the call).

## Docs

Client-facing HTTP endpoint, so update in the same PR:

- `docs/client-api.md` §2.4 — new `GET /api/v3/rooms/:roomId/protected-image/:fileId`
  subsection (request/response/errors) mirroring the v1 download entry, plus TOC.
- `docs/client-api/request-reply.md` — matching derived entry, plus TOC.
