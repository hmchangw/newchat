# MinIO Download Request Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound the entire MinIO/S3 download (Stat probe + body streaming) in `upload-service` at a configurable timeout defaulting to 5 minutes, so a hung backend cannot hang the request indefinitely.

**Architecture:** The timeout becomes a property of `minioObjectStore` (the "download client", mirroring `pkg/drive.Client.downloadClient`). Because minio-go ties every `Read` on the returned object to the context passed into `GetObject`, `Open` derives a `context.WithTimeout` from the request context that must span both the `Stat` probe and the post-`Open` streaming. A `cancelReadCloser` wrapper ties the context's `cancel` to the reader's `Close`, so the bound covers the whole download and is released exactly when the handler finishes streaming.

**Tech Stack:** Go 1.25, `github.com/minio/minio-go/v7`, `github.com/caarlos0/env/v11`, `stretchr/testify`.

## Global Constraints

- Language: Go 1.25 — use `make` targets, never raw `go` commands.
- Config: all env vars parsed via `caarlos0/env` into the typed `config` struct; `SCREAMING_SNAKE_CASE` names; always provide `envDefault` for non-critical config.
- Error handling: wrap infra errors with `fmt.Errorf("desc: %w", err)`; the handler boundary maps them to `errcode.Unavailable`. No new `errcode` reason.
- TDD: Red → Green → Refactor → Commit. Write failing tests first.
- Testing: unit tests in `package main`, same-package; integration tests tagged `//go:build integration`; use `-race` (Makefile handles it).
- Minimum 80% coverage; do not edit generated mocks manually.
- This is NOT a client-facing NATS RPC (`chat.user.…`) — no `docs/client-api.md` change required. Behavior of the HTTP route is unchanged except for the added deadline.

---

### Task 1: Add `cancelReadCloser` and thread the timeout through `minioObjectStore`

**Files:**
- Modify: `upload-service/store_minio.go` (whole file)
- Create: `upload-service/store_minio_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `func newMinioObjectStore(client *minio.Client, bucket string, downloadTimeout time.Duration) *minioObjectStore` — new third parameter.
  - `minioObjectStore.Open(ctx context.Context, key string) (io.ReadCloser, error)` — signature unchanged; now applies the timeout.
  - `cancelReadCloser` — unexported `io.ReadCloser` wrapping an inner `io.ReadCloser` plus a `context.CancelFunc`; its `Close()` calls the inner `Close()` then `cancel()`, returning the inner Close's error.

- [ ] **Step 1: Write the failing unit tests for `cancelReadCloser`**

Create `upload-service/store_minio_test.go`:

```go
package main

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// spyReadCloser records whether Close was called and returns a preset error.
type spyReadCloser struct {
	closed   bool
	closeErr error
}

func (s *spyReadCloser) Read(p []byte) (int, error) { return 0, io.EOF }
func (s *spyReadCloser) Close() error {
	s.closed = true
	return s.closeErr
}

func TestCancelReadCloser_CloseClosesInnerAndCancels(t *testing.T) {
	inner := &spyReadCloser{}
	ctx, cancel := context.WithCancel(context.Background())

	rc := &cancelReadCloser{ReadCloser: inner, cancel: cancel}
	require.NoError(t, rc.Close())

	require.True(t, inner.closed, "inner reader should be closed")
	require.ErrorIs(t, ctx.Err(), context.Canceled, "context should be cancelled after Close")
}

func TestCancelReadCloser_ClosePropagatesInnerError(t *testing.T) {
	wantErr := errors.New("boom")
	inner := &spyReadCloser{closeErr: wantErr}
	_, cancel := context.WithCancel(context.Background())

	rc := &cancelReadCloser{ReadCloser: inner, cancel: cancel}
	require.ErrorIs(t, rc.Close(), wantErr, "Close should propagate the inner Close error")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=upload-service`
Expected: FAIL to compile — `undefined: cancelReadCloser`.

- [ ] **Step 3: Rewrite `store_minio.go` to add the field, param, timeout, and wrapper**

Replace the full contents of `upload-service/store_minio.go` with:

```go
package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
)

// minioObjectStore streams objects out of a single MinIO/S3 bucket.
type minioObjectStore struct {
	client          *minio.Client
	bucket          string
	downloadTimeout time.Duration
}

// newMinioObjectStore binds a minio client to a bucket. downloadTimeout bounds a
// single download (the Stat probe plus the streamed body) so a hung backend
// cannot hang the request indefinitely.
func newMinioObjectStore(client *minio.Client, bucket string, downloadTimeout time.Duration) *minioObjectStore {
	return &minioObjectStore{client: client, bucket: bucket, downloadTimeout: downloadTimeout}
}

// Open returns a streaming reader for the object at key. It Stats the object so
// a missing object or unreachable backend surfaces here — before any response
// body is written — letting the handler map it to 503.
//
// minio-go ties every Read on the returned object to the context passed into
// GetObject, so the timeout context must outlive Open and span the streaming
// the caller does afterward. The returned reader carries the cancel func and
// releases it on Close; every early-return path cancels before returning.
func (s *minioObjectStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	tctx, cancel := context.WithTimeout(ctx, s.downloadTimeout)
	obj, err := s.client.GetObject(tctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("get object %s/%s: %w", s.bucket, key, err)
	}
	// minio-go's GetObject is lazy; the request only fires on Stat/Read, so probe now.
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		cancel()
		return nil, fmt.Errorf("stat object %s/%s: %w", s.bucket, key, err)
	}
	return &cancelReadCloser{ReadCloser: obj, cancel: cancel}, nil
}

// cancelReadCloser wraps a download reader so closing it also cancels the
// timeout context that bounds the download.
type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

// Close closes the underlying reader and then releases the timeout context.
func (c *cancelReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=upload-service`
Expected: PASS (both `cancelReadCloser` tests). The non-integration build compiles.

- [ ] **Step 5: Commit**

```bash
git add upload-service/store_minio.go upload-service/store_minio_test.go
git commit -m "feat(upload-service): bound MinIO downloads with a timeout context"
```

---

### Task 2: Wire `MINIO_DOWNLOAD_TIMEOUT` config and update the integration call site

**Files:**
- Modify: `upload-service/main.go:50-54` (Minio config block) and `upload-service/main.go:96` (constructor call)
- Modify: `upload-service/integration_test.go:90` (constructor call site)

**Interfaces:**
- Consumes: `newMinioObjectStore(client, bucket, downloadTimeout)` from Task 1.
- Produces: `config.MinioDownloadTimeout time.Duration` (env `MINIO_DOWNLOAD_TIMEOUT`, default `5m`).

- [ ] **Step 1: Update the integration test call site (the failing check for this task)**

In `upload-service/integration_test.go`, change line 90 from:

```go
	s := newMinioObjectStore(client, bucket)
```

to:

```go
	s := newMinioObjectStore(client, bucket, 5*time.Minute)
```

(The `time` package is already imported in this file.)

- [ ] **Step 2: Run the integration build to verify it fails**

Run: `go build -tags integration ./upload-service/`
Expected: FAIL — `not enough arguments in call to newMinioObjectStore` at `main.go:96` (the production call site still passes two args).

> Note: `go build` here is a compile-only check of the integration-tagged tree; the actual test run uses `make test-integration SERVICE=upload-service`.

- [ ] **Step 3: Add the config field and pass it through in `main.go`**

In `upload-service/main.go`, add the config field inside the Minio block (after `MinioBucket`, currently line 54):

```go
	MinioBucket    string `env:"MINIO_BUCKET"`
	// MinioDownloadTimeout bounds a single MinIO/S3 download (Stat probe + streamed
	// body) so a hung backend cannot hang the request.
	MinioDownloadTimeout time.Duration `env:"MINIO_DOWNLOAD_TIMEOUT" envDefault:"5m"`
```

Then change the constructor call (currently line 96) from:

```go
	s3Store := newMinioObjectStore(minioClient, bucket)
```

to:

```go
	s3Store := newMinioObjectStore(minioClient, bucket, cfg.MinioDownloadTimeout)
```

- [ ] **Step 4: Verify the whole service compiles (both build tags) and unit tests pass**

Run: `make build SERVICE=upload-service && go build -tags integration ./upload-service/ && make test SERVICE=upload-service`
Expected: build succeeds for both trees; unit tests PASS.

- [ ] **Step 5: Run the MinIO integration test (requires Docker)**

Run: `make test-integration SERVICE=upload-service`
Expected: PASS — `TestMinioObjectStore_Open` still reads the payload and still errors on the missing key, now through the timeout-bounded path.

> If Docker is unavailable in this environment, skip this step and note it — the unit tests plus the two compile checks in Step 4 cover the wiring; CI runs the integration suite.

- [ ] **Step 6: Commit**

```bash
git add upload-service/main.go upload-service/integration_test.go
git commit -m "feat(upload-service): add MINIO_DOWNLOAD_TIMEOUT config (default 5m)"
```

---

### Task 3: Lint and final verification

**Files:** none (verification only).

- [ ] **Step 1: Run the linter**

Run: `make lint`
Expected: PASS. In particular, `go vet`'s `lostcancel` check is satisfied — every path in `Open` either calls `cancel()` or hands it to `cancelReadCloser`.

- [ ] **Step 2: Run SAST**

Run: `make sast`
Expected: PASS (no new medium+ findings).

- [ ] **Step 3: Confirm coverage did not regress below the floor**

Run: `make test SERVICE=upload-service` and confirm the run is green with `-race`.
Expected: PASS. `cancelReadCloser` is fully covered by Task 1's tests; `Open`'s timeout branch is covered by the integration test.

- [ ] **Step 4: Commit any formatting fixups (only if `make fmt` changed files)**

```bash
make fmt
git add -A
git commit -m "style(upload-service): gofmt/goimports fixups" || echo "nothing to commit"
```

---

## Self-Review

**Spec coverage:**
- "Timeout lives in the store" → Task 1 (`downloadTimeout` field). ✓
- "Timeout spans Stat probe + streaming; cancel tied to Close" → Task 1 (`Open` + `cancelReadCloser`). ✓
- "Every error path cancels" → Task 1 Step 3 (both early returns call `cancel()`). ✓
- "`MINIO_DOWNLOAD_TIMEOUT` env, 5m default, threaded through constructor" → Task 2. ✓
- "Parent is the request context" → Task 1 (`context.WithTimeout(ctx, …)` where `ctx` is the handler's request context). ✓
- "Unit tests for `cancelReadCloser` first; integration call site updated" → Task 1 + Task 2. ✓
- "Error handling unchanged at boundary; no new reason" → no handler change; noted in Global Constraints. ✓
- "No client-api.md change" → Global Constraints. ✓

**Placeholder scan:** No TBD/TODO/"handle edge cases"/vague steps — every code step shows full code. ✓

**Type consistency:** `newMinioObjectStore(client, bucket, downloadTimeout)`, `cancelReadCloser{ReadCloser, cancel}`, and `config.MinioDownloadTimeout` are named identically across Tasks 1–2 and the tests. ✓
