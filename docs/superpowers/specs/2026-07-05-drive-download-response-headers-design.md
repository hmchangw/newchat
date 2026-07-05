# Drive Download Response Headers — Design

**Date:** 2026-07-05
**Service:** `upload-service` (+ `pkg/drive`)

## Problem

`upload-service` exposes two authenticated file-download endpoints:

| Route | Handler | Backing store |
|-------|---------|---------------|
| `GET /api/v1/file/rooms/:roomId/file/:fileId` | `HandleDownloadFile` | Drive API (presigned storage) |
| `GET /api/v1/file-upload/:fileId/:fileName` | `HandleDownloadMinioS3File` | MinIO/S3 (legacy) |

The legacy MinIO/S3 path sets three response headers when streaming the object:

- `Content-Disposition: attachment; filename*=UTF-8''<encoded name>`
- `Content-Security-Policy: default-src 'none'`
- `Cache-Control: private, max-age=<cacheMaxAge>`

The Drive path streams its body with an **empty** extra-headers map
(`handler.go:345`), so downloads served from Drive are missing all three.
The goal is parity: the Drive path should set the same three headers.

Two of the three are trivial — CSP is a constant and `h.cacheMaxAge` is
already on the handler. The one open question is the **filename** for
`Content-Disposition`: the MinIO/S3 path reads it from Mongo (`up.Name`),
but the Drive path has no filename available — the route carries no name
segment and `drive.GetGroupImageResponse` exposes only
`{Reader, ContentType, ContentLength}`.

**Decision (approved):** forward the filename from the Drive storage
response's own `Content-Disposition` header, falling back to a bare
`attachment` disposition when it is absent or unparseable. This mirrors how
`ContentType` is already lifted off that same response and requires no
frontend change.

## Design

### 1. `pkg/drive` — expose the filename from storage

- Add `Filename string` to `GetGroupImageResponse`.
- In `GetGroupImage`, after the successful storage fetch, read
  `resp.Header().Get("Content-Disposition")` and parse the filename via
  `mime.ParseMediaType` (handles both `filename=` and RFC 5987
  `filename*=`). Set `resp.Filename` to the parsed value, or `""` when the
  header is absent or unparseable.
- Rationale: the drive client returns clean metadata (like `ContentType`);
  the handler owns the wire format of the header it emits.

### 2. `upload-service/handler.go` — set the three headers

- Extract a shared helper `contentDisposition(name string) string`:
  - `name != ""` → `attachment; filename*=UTF-8''<rfc5987-encoded>`
    (encoding matches the current MinIO/S3 code: `url.QueryEscape` with
    `+` rewritten to `%20`).
  - `name == ""` → `attachment`.
- Refactor `HandleDownloadMinioS3File` to use the helper (it currently
  inlines the encoding) so both paths format identically.
- In `HandleDownloadFile`, replace the empty `map[string]string{}` with:
  - `Content-Disposition`: `contentDisposition(img.Filename)`
  - `Content-Security-Policy`: `default-src 'none'`
  - `Cache-Control`: `fmt.Sprintf("private, max-age=%d", h.cacheMaxAge)`

### 3. Safety: inline `<img>` display is unaffected

These image URLs are also served inline via `<img>` (`att.ImageURL`).
`Content-Disposition: attachment` does not break embedded subresources —
browsers only honour it for top-level navigation, so inline rendering is
unchanged. The security value (forcing download rather than inline HTML/SVG
execution, plus `default-src 'none'`) applies on direct navigation.

## Testing (TDD: Red → Green → Refactor)

**`pkg/drive` (`uploader_test.go`, extending the existing
`httptest.NewServer`-based `TestClient_GetGroupImage_*` cases):**
- `GetGroupImage` sets `Filename` from the storage `Content-Disposition`
  when present (parsed value) — the download stub sets the header.
- `GetGroupImage` sets `Filename == ""` when the header is absent.

**`upload-service` (`handler_test.go`):**
- Extend `fakeDrive.getResp` usage with a `Filename` field.
- `HandleDownloadFile` success asserts `Content-Security-Policy`,
  `Cache-Control`, and `Content-Disposition` with a forwarded filename.
- `HandleDownloadFile` with empty `Filename` asserts the bare `attachment`
  fallback.
- `contentDisposition` helper: encoding of a UTF-8/space name, and the
  empty-name case.

## Scope / non-goals

- `upload-service` HTTP routes are not `chat.user.` NATS subjects, so
  `docs/client-api.md` needs no update.
- No new dependencies — `mime` is standard library.
- No change to the MinIO/S3 path's observable behaviour (only the shared
  helper refactor, which must keep byte-identical output).
