# Drive Upload Content-Type — Design

**Date:** 2026-07-29
**Service:** `upload-service` (+ `pkg/drive`)

## Problem

`GET /api/v1/file/rooms/:roomId/file/:fileId` always responds with
`Content-Type: application/octet-stream`, even for PNGs and PDFs.

The download path is not at fault. `GetGroupImage` lifts the storage response's
`Content-Type` (`pkg/drive/uploader.go`) and `HandleDownloadFile` hands it to
`c.DataFromReader`; a repro driving the real `drive.Client` against a storage
stub that returns `image/png` yields `ContentType="image/png"`, and no
middleware in `upload-service` writes a `Content-Type` header.

The type was lost at **upload** time. `UploadGroupImages` passed the
package-level `defaultContentType` (`application/octet-stream`) as the multipart
part's `Content-Type` for every file:

```go
req.SetMultipartField(fmt.Sprintf("files[%d].file", i), f.Filename, defaultContentType, f.File)
```

Downloads are served by a **presigned URL straight to object storage**, so the
response header is the stored object's metadata, which Drive set when it PUT the
object. S3/MinIO defaults that metadata to `application/octet-stream` when the
PUT does not specify a type — nothing sniffs bytes or reads the extension. The
bulk-upload form carries no other per-file type field (`files[i].fileName` and
`files[i].mode` only), so the part header is the sole type signal available to
the caller.

Drive's own API reports `image/png` / `application/pdf` for the same file
because it infers the type from the extension rather than reading stored
metadata — hence the discrepancy.

## Design

Use Resty's `SetFileReader` instead of `SetMultipartField`. It takes no content
type argument: it routes through `writeMultipartFormFile`, which reads the
leading bytes of each part and calls `http.DetectContentType`, so every part
declares its true media type without any caller having to supply one.

```go
req.SetFileReader(fmt.Sprintf("files[%d].file", i), f.Filename, f.File)
```

Consequences:

- `drive.MultipartFile` stays `{File, Filename}` — no content-type field, and no
  caller plumbing in `upload-service`.
- Sniffing reads bytes, so it cannot be fooled by a wrong client-declared MIME
  or a misleading extension.
- Part ordering is unchanged: Resty writes `FormData` fields first, then
  `multipartFiles` (where `SetFileReader` lands), then `multipartFields`. Only
  one file bucket is in use, so the wire order matches the previous behavior.

### Known gap: HEIC

`http.DetectContentType` has no HEIC signature — HEIC is an ISO-BMFF `ftyp` box
and Go's sniffer only recognizes the `mp4` brand. HEIC files therefore still
store as `application/octet-stream`, despite `.heic` being in
`drive.AllowedImageFileTypes`. This is not a regression (every format was
octet-stream before), but it is the one accepted image format sniffing cannot
name. Closing it would need an extension-based fallback when the sniffer returns
octet-stream.

## Scope / non-goals

- **Existing Drive objects are not repaired.** Files uploaded before this change
  remain stored as `application/octet-stream` and will keep downloading as such;
  correcting them needs a re-upload or a Drive-side metadata migration. A
  download-time fallback deriving the type from the filename was considered and
  explicitly declined.
- No download-path change; `HandleDownloadFile` continues to forward whatever
  storage reports.
- `upload-service` HTTP routes are not `chat.user.` NATS subjects, so
  `docs/client-api.md` needs no update.
- No new dependencies.

## Open verification

Drive performs the S3 PUT and is not in this repo, so **whether Drive forwards
the part's `Content-Type` into that PUT is unverified.** The part header is the
only type signal the bulk-upload API accepts, so it is the only lever available
to a caller, but confirming the fix is load-bearing requires one manual test:
upload a PNG through this code against dev/staging Drive, then check the
`Content-Type` on the file endpoint (or the stored metadata behind the presigned
URL). Unit tests and CI both stub Drive out and cannot answer this.

If Drive turns out to ignore the part header, the fix has to move — Drive-side
(infer at store time, or set `response-content-type` on the URL it signs) or
download-side in `upload-service` (derive from `img.Filename` when storage
reports octet-stream, which also repairs already-stored files).

## Testing (TDD: Red → Green → Refactor)

**`pkg/drive` (`uploader_test.go`):**
- Each part's `Content-Type` is sniffed from its bytes: PNG, JPEG, PDF, text,
  HEIC (→ octet-stream, documenting the gap), unrecognized binary
  (→ octet-stream), empty body (→ `text/plain`, which is what
  `DetectContentType` returns for no bytes).
- Sniffing does not consume the leading bytes — the full body still reaches
  Drive.
- Multiple files are sniffed independently and keep their `files[i]` index
  mapping.
