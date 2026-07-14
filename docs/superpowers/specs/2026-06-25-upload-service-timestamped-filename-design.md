# Upload-service: Timestamped Drive filenames

**Date:** 2026-06-25
**Service:** `upload-service`
**Status:** Approved

## Problem

Drive rejects an upload when an object with the same `fileName` already exists
in a group. Re-uploading the same file (across requests) therefore fails. We
need each upload to land in Drive under a unique name, while the client still
sees the original filename in the response.

## Solution

Send a unique filename to Drive — original base + millisecond timestamp +
per-batch index — and return the original (suffix-free) name to the client.
Applies to both handlers that call `drive.UploadGroupImages`:
`HandleUploadImages` (bulk) and `HandleUploadFile` (single file).

### Filename transform

New unexported helper in `upload-service/handler.go`:

```go
// uniqueName inserts a millisecond timestamp and a per-batch index before the
// file extension: "photo.png" -> "photo_1719312000000_0.png". The timestamp
// separates re-uploads across requests; the index separates duplicate filenames
// within a single batch (processed in the same millisecond). The extension is
// preserved so Drive's content-type sniffing is unaffected; a name with no
// extension just gets the suffix appended ("README" -> "README_1719312000000_0").
func uniqueName(name string, milli int64, i int) string
```

- Split on `filepath.Ext`, insert `_<milli>_<i>` between base and extension.
- Multi-dot names (`a.tar.gz`) keep only the final extension as the ext, which
  matches `filepath.Ext` semantics — acceptable.

### Why timestamp + index (not timestamp alone)

A bulk request's files are processed in a tight loop that completes within a
single millisecond, so a timestamp alone cannot distinguish two files with the
same name in one batch — both would get the same `_<milli>` suffix and collide
in Drive. The per-file index guarantees within-batch uniqueness regardless of
clock resolution. The timestamp still does the cross-request work.

### Clock injection (testability)

Add a `nowMilli func() int64` field to `Handler`, defaulted in `NewHandler` to
`func() int64 { return time.Now().UTC().UnixMilli() }`. `NewHandler`'s signature
is unchanged. Tests live in `package main` and override `h.nowMilli` directly for
deterministic assertions, so no existing call site changes.

### HandleUploadFile (single file)

The response `name` already uses the original `fh.Filename`, so only the upload
path changes:

- Send `uniqueName(fh.Filename, h.nowMilli(), 0)` as the `MultipartFile.Filename`
  to `UploadGroupImages` (single file → index `0`).
- `meta.name` stays `fh.Filename`. No strip-back required.

### HandleUploadImages (bulk)

The response `Name` currently echoes Drive's `resp.File.Filename`, which would be
the unique name. To return originals, track them in send order:

- `preprocessFiles` builds `MultipartFile`s via `uniqueName(fh.Filename, milli, i)`
  — where `i` is the accepted-file index — and returns an `origNames []string`
  slice of the originals in send order. It takes the current `milli` (computed
  once per request by the caller) so all files in a batch share a consistent
  timestamp source while the index keeps them distinct.
- In the response loop (`for i, resp := range responses`), set the response name
  from `origNames[i]` when `i < len(origNames)`, ignoring Drive's echo. This
  correlates each response to the file we sent by position.

### Empty filename on Drive failure

When Drive reports a per-file failure, `resp.File.Filename` is an empty string.
Because the response name comes from `origNames[i]` rather than the echo, the
response `Name` is never empty — the client still sees which original file
failed, alongside the `Error` text. (This concern is bulk-only: `HandleUploadFile`
returns a 500 on Drive failure and builds no per-file item.)

This position-based correlation assumes Drive returns response items in the same
order as the files were sent — the same assumption the empty-failure case forces
anyway. (The pre-existing code treated Drive's echo as authoritative; switching
to send-order is a deliberate, documented change so the timestamp can be stripped
and empty echoes handled uniformly.)

## Edge cases

- **No extension:** suffix appended to the whole name.
- **Identical filenames in one bulk request:** handled — each accepted file gets a
  distinct index, so `a.png` twice becomes `a_<milli>_0.png` and `a_<milli>_1.png`.
- **Two separate requests in the same millisecond, same filename:** still
  theoretically collides (same `milli`, both index `0`). Negligible for human
  re-uploads (seconds apart); if true timing-independence is ever required, swap
  the timestamp for an `idgen` unique token.
- **Rejected files** (size/type/open failures in `preprocessFiles`) never reach
  Drive and already report the original `fh.Filename` — unchanged. The index is
  the accepted-file index, so rejections don't create gaps that misalign
  `origNames[i]` with `responses[i]`.

## Testing (TDD: Red → Green → Refactor)

- `uniqueName`: table-driven — extension, no extension, dotfile (`.gitignore`),
  multi-dot (`a.tar.gz`), with a fixed `milli` and index.
- `HandleUploadFile`: fake drive captures the uploaded `Filename`; assert it is
  the unique form (`..._0.png`) while the response attachment `name` is the original.
- `HandleUploadImages`: fake drive captures uploaded names; assert Drive receives
  unique names and each response item `Name` is the original. Cover mixed
  success/failure, the existing per-file-rejection paths, and **two identical
  filenames in one batch get distinct indexed names**.

## Out of scope / no change

- `docs/client-api.md`: response schema is unchanged (still the original name),
  and upload-service is neither a `chat.user.` NATS handler nor an auth-service
  HTTP route, so no update is required.
- `pkg/drive`: no change — the timestamp is applied by the handler before calling
  `UploadGroupImages`, keeping the Drive client a thin transport.
