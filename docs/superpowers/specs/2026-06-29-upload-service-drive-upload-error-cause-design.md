# Upload-Service: Diagnostic Cause for Drive Upload Failure

**Date:** 2026-06-29
**Service:** `upload-service`
**Scope:** One handler branch + one test. No client-facing contract change.

## Problem

In `HandleUploadFile` (`upload-service/handler.go`), when Drive returns no
upload responses or a non-success status, the handler replies with a bare
`errcode.Unavailable("drive upload failed")`. Drive's per-file response
(`drive.UploadGroupImageResponse`) carries both a `Status` and an `Error`
string describing why the upload failed, but none of that detail reaches the
server logs — leaving an operator with no diagnostic signal beyond "drive
upload failed".

The sibling branch immediately above (`uploadErr != nil`) already gets context
via `fmt.Errorf("upload file to drive: %w", err)`. This status-failure branch
is the gap.

## Change

Attach a synthetic cause via `errcode.WithCause` that captures the actual
failure shape. The cause is logged once server-side by `Classify` and is never
serialized to the client, so the wire response is unchanged (still
`503 / "drive upload failed"`).

```go
if len(responses) == 0 || responses[0].Status != driveStatusSuccess {
    var cause error
    if len(responses) == 0 {
        cause = errors.New("drive returned no upload response")
    } else {
        cause = fmt.Errorf("drive upload status %q: %s", responses[0].Status, responses[0].Error)
    }
    errhttp.Write(ctx, c, errcode.Unavailable("drive upload failed", errcode.WithCause(cause)))
    return
}
```

### Rationale

- **Distinct messages** for the empty-response vs non-success-status cases so
  the log is unambiguous about which condition tripped.
- `Status` and `Error` are operational diagnostics from Drive — not message
  bodies or tokens — so wrapping them in a cause complies with the CLAUDE.md
  rule "never wrap raw message bodies/tokens into a cause".
- The cause is a raw error (not an `*errcode.Error`), so it does not trip the
  one-errcode-per-chain panic guard in `WithCause`.

## Testing (TDD)

The non-success-status branch is currently **untested** —
`TestHandleUploadFile_DriveError` only exercises the `uploadErr != nil` path.

Add `TestHandleUploadFile_DriveStatusFailure`: drive a `fakeDrive` returning
`{Status: "failure", Error: "..."}` and assert `503 / "unavailable"`.

**Caveat:** the cause is not wire-observable by design (server-log only), so
the test asserts the status-failure branch + response code, not the cause
string itself. Branch coverage, per the agreed test depth.

## Out of Scope

- No change to the client API contract (`docs/client-api.md` unaffected — the
  response code and body are identical).
- No change to the multi-file `HandleUploadImages` handler, which already
  surfaces per-file `Status`/`Error` in its result items.
