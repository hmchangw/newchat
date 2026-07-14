# MinIO Download Request Timeout — upload-service

**Date:** 2026-07-02
**Service:** `upload-service`
**Status:** Approved

## Problem

`HandleDownloadMinioS3File` streams a legacy-uploaded file out of MinIO/S3. The
call chain is:

```
HandleDownloadMinioS3File
  → h.s3.Open(ctx, path)        // minioObjectStore.Open: GetObject (lazy) + Stat probe
  → c.DataFromReader(...)       // streams the object body to the client
```

Neither the `Stat` probe nor the subsequent streaming Reads have an explicit
deadline. If the MinIO/S3 backend hangs, the request hangs — the only backstop
is the server-level `WriteTimeout` (5m), which is coarse and unrelated to the
download itself.

The `pkg/drive` client already solves the equivalent problem for its own
downloads: `NewClient` gives its `downloadClient` a hardcoded `5 * time.Minute`
resty timeout that bounds the whole streamed download. We want the MinIO
download path to have the same guarantee.

## Goal

Bound the entire MinIO download (Stat probe **and** body streaming) at a
configurable timeout defaulting to 5 minutes, so a hung backend cannot hang the
request indefinitely.

## Design

### Where the timeout lives — the store

The timeout becomes a property of `minioObjectStore`, exactly paralleling
`pkg/drive.Client.downloadClient`. The store is the "download client" for the
MinIO path, so the bound belongs there.

### Lifecycle: the timeout must outlive `Open`

minio-go's `GetObject` is lazy and ties every subsequent `Read` on the returned
object to the context passed into `GetObject`. The timeout context must
therefore cover **both** the `Stat` probe inside `Open` **and** the streaming
that `c.DataFromReader` performs *after* `Open` returns. A plain
`defer cancel()` inside `Open` would cancel the context before the caller
streams, breaking the download.

Solution:

- `Open` derives `tctx, cancel := context.WithTimeout(ctx, s.downloadTimeout)`
  from the **request** context — so a client disconnect still cancels early.
- On any error path in `Open` (GetObject error, Stat error) → call `cancel()`
  before returning, so the context is never leaked.
- On success → return a small wrapper `ReadCloser` (`cancelReadCloser`) whose
  `Close()` calls the underlying object's `Close()` **and** `cancel()`. The
  handler's existing `defer reader.Close()` releases the timeout exactly when
  streaming ends.

Net effect: probe + stream are bounded at the configured timeout. A hung
backend surfaces as a context-deadline error — mapped by `Open`'s existing
`fmt.Errorf` wrap to the handler's existing `errcode.Unavailable("failed to
retrieve file")` (503) when it happens during the probe, or aborts the stream
mid-flight if it happens during streaming.

### Configuration

Add to `config` in `main.go`:

```go
MinioDownloadTimeout time.Duration `env:"MINIO_DOWNLOAD_TIMEOUT" envDefault:"5m"`
```

`caarlos0/env` parses Go `time.Duration` strings natively. Threaded through:

```go
s3Store := newMinioObjectStore(minioClient, bucket, cfg.MinioDownloadTimeout)
```

## Components Changed

| File | Change |
|------|--------|
| `store_minio.go` | `minioObjectStore` gains `downloadTimeout time.Duration`; `newMinioObjectStore` gains the param; `Open` applies the timeout context and returns a `cancelReadCloser`; new unexported `cancelReadCloser` type. |
| `main.go` | New `MinioDownloadTimeout` config field; pass to `newMinioObjectStore`. |
| `store_minio_test.go` (new) | Unit tests for `cancelReadCloser` (Close closes inner + cancels ctx). |
| `integration_test.go` | Update the existing `newMinioObjectStore(client, bucket)` call site to pass a timeout. |

## Error Handling

Unchanged at the boundary. `Open` still returns a wrapped `fmt.Errorf(...)`; the
handler still maps it to `errcode.Unavailable`. A context-deadline is an infra
failure that correctly collapses to a 503 with the cause logged once at the
boundary. No new `errcode` reason is needed — the frontend does not branch on
"backend was slow" vs "backend was down".

## Testing (TDD, Red → Green)

The cheaply unit-testable piece is `cancelReadCloser`. Write these first
(they fail — the type does not exist yet), then implement:

1. `Close()` closes the inner `io.ReadCloser` (assert a spy's Close was called).
2. `Close()` invokes the cancel func (assert the derived context is cancelled
   afterward).
3. `Close()` returns the inner Close's error (propagation).

The `Open` timeout wiring is exercised by the existing integration test
(`TestMinioObjectStore_Open`, backed by `testutil.MinIO`) for the happy path —
the call site is updated to pass a timeout, confirming probe + stream still
succeed within the bound.

## Non-Goals

- No change to the `pkg/drive` download path (already has its 5-minute bound).
- No change to the upload paths.
- No new client-facing error reason (this is not a client-facing RPC — it is an
  HTTP route whose behavior is unchanged except for the added deadline).
