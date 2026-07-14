# upload-service: `MAX_ATTACHMENTS` cap on the single-file endpoint

**Date:** 2026-07-07
**Status:** Approved

## Problem

`HandleUploadFile` retrieves the upload with `c.FormFile("file")`, which silently
returns only the **first** part when a client sends multiple `file` parts. There
is no explicit, configurable limit on how many files the single-file endpoint
accepts, and an over-count upload is accepted (first file only) rather than
rejected. We want a configurable ceiling that rejects over-count uploads with a
`too many files` error, mirroring the images endpoint.

## Decisions

1. **Over-count is rejected**, not truncated: `len(files) > maxAttachments` →
   `errcode.BadRequest("too many files")` (400). Matches the existing
   `HandleUploadImages` behavior.
2. **Rename `maxFiles` → `maxImages`** throughout the images path — config field
   `MaxFiles` → `MaxImages`, env var `MAX_FILES` → `MAX_IMAGES`, handler field
   `maxFiles` → `maxImages`, test constant `testMaxFiles` → `testMaxImages`. The
   old name conflated "images" with the new single-file "attachments" concept;
   the rename disambiguates them.
3. The new `maxAttachments` parameter sits **immediately after** `maxImages` in
   the `NewHandler` signature, grouping the two count-limits together.

## Changes

### `upload-service/main.go`
- Rename config field `MaxFiles` (`MAX_FILES`) → `MaxImages` (`MAX_IMAGES`);
  update the doc comment.
- Add `MaxAttachments int \`env:"MAX_ATTACHMENTS" envDefault:"1"\`` — caps the
  number of files the single-file endpoint accepts.
- Pass `cfg.MaxAttachments` into `NewHandler` (after `cfg.MaxImages`).

### `upload-service/handler.go`
- Rename struct field `maxFiles` → `maxImages`; add `maxAttachments int`.
- `NewHandler` gains a `maxAttachments int` parameter after `maxImages`.
- Add constant `fileFormField = "file"`.
- In `HandleUploadFile`, replace the `c.FormFile("file")` block with a
  `c.MultipartForm()` read of the `file` field:
  - not multipart → `BadRequest("request must be multipart/form-data")`
  - `len == 0` → `BadRequest("file is required")` (preserves current behavior)
  - `len > maxAttachments` → `BadRequest("too many files")`
  - else use `files[0]` exactly as today.
- `HandleUploadImages` reference `h.maxFiles` → `h.maxImages` (unchanged behavior).

### `upload-service/handler_test.go`
- Rename `testMaxFiles` → `testMaxImages`; update all `NewHandler(...)` call
  sites for the new parameter.
- Add `TestHandleUploadFile_TooManyFiles` (2 `file` parts, limit 1 → 400
  `bad_request`) and a single-file happy-path assertion that the cap allows one.

### `upload-service/deploy/docker-compose.yml`
- Rename `MAX_FILES=10` → `MAX_IMAGES=10`; add `MAX_ATTACHMENTS=1`.

### `docs/client-api.md`
- Update the images-endpoint field note that references `MAX_FILES` →
  `MAX_IMAGES` (env-var rename, keeps the doc accurate).

## Out of scope

- No new NATS/`chat.user.` handler, so no request/reply or events view changes.
- The single-file endpoint remains a single-attachment API by default
  (`MAX_ATTACHMENTS=1`); it does not begin uploading multiple attachments.

## Testing

TDD: add the failing `too many files` test first, then implement. Existing
handler tests must stay green after the rename and signature change.
