# Client Update Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new flat Gin service `client-update-service` that streams a client update-artifact pair (`configFile` + `executeFile`) into MinIO and serves artifacts back by file name, fronted by a bounded TTL+size download cache.

**Architecture:** Streaming upload (`multipart.FileHeader.Open()` → `PutObject(reader, size)`) and streaming download (`GetObject` → `c.DataFromReader`), with a size+TTL-bounded `expirable.LRU` + `singleflight` cache (the `media-service/cache.go` idiom). Cacheable objects (≤ `CACHE_MAX_OBJECT_BYTES`) are buffered and served from RAM; larger ones stream uncached. The store interface is consumer-defined (`Put` + `Open`); the MinIO impl reuses the `cancelReadCloser` pattern from `upload-service/store_minio.go`. The bucket is created at startup if absent.

**Tech Stack:** Go 1.25, Gin, `minio-go/v7`, `caarlos0/env/v11`, `hashicorp/golang-lru/v2/expirable`, `golang.org/x/sync/singleflight`, `stretchr/testify`, `go.uber.org/mock`, `pkg/{minioutil,obs,shutdown,errcode,idgen,natsutil,testutil}`.

**Spec:** `docs/superpowers/specs/2026-08-03-client-update-service-design.md` (authoritative).

## Global Constraints

- Go 1.25; use `make` targets, never raw `go`: `make test SERVICE=client-update-service`, `make generate SERVICE=client-update-service`, `make lint`, `make sast`, `make test-integration SERVICE=client-update-service`, `make build SERVICE=client-update-service`.
- All config via `caarlos0/env` typed struct — no `os.Getenv`. `SCREAMING_SNAKE_CASE` names. `envDefault` for non-secrets; `SITE_ID`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET` are `required` (never defaulted).
- Client-facing errors via `pkg/errcode` Tier-1 constructors + `errhttp.Write`; infra failures via `fmt.Errorf("...: %w", err)` (collapse to `internal`). No bespoke error/response string maps.
- Logging: `log/slog` JSON only; never log file bytes. Request-ID + access-log middleware.
- TDD: no production code before a failing test (Red → Green → Refactor → Commit). `-race` always (the Makefile adds it).
- Minimum 80% coverage; target ≥90% on `version.go`, `cache.go`, `store_minio.go`. **No new third-party dependencies.**
- Never `io.ReadAll` a request or object body; the only buffer is the bounded cache read (exactly `info.Size` bytes, gated by `≤ CACHE_MAX_OBJECT_BYTES`).
- Generated mocks live in `mock_store_test.go` (never hand-edit); run `make generate SERVICE=client-update-service` after any `store.go` change.
- Object key = `chat.go/chat-versions/<fileName>`. Content types: `configFile` → `application/x-yaml`, `executeFile` → `application/octet-stream`.
- Cache defaults: `CACHE_MAX_ENTRIES=4`, `CACHE_TTL=24h`, `CACHE_MAX_OBJECT_BYTES=536870912` (512 MiB). No upload size cap.
- Endpoints: `POST /api/v1/version`, `GET /api/v1/version/:fileName`, `GET /healthz`. Unauthenticated (matches legacy) — documented as network-restricted.
- Client-facing HTTP endpoints → update `docs/client-api.md` + `docs/client-api/request-reply.md` in the same PR.

## File Structure

```
client-update-service/
  cache.go             # blobCache: expirable.LRU + singleflight  (Task 1)
  store.go             # versionStore interface, blobInfo, ErrObjectNotFound, //go:generate  (Task 2)
  store_minio.go       # minioVersionStore (Put/Open), ensureBucket, bucketClient, cancelReadCloser, isNotFound  (Task 2)
  handler.go           # Handler struct, NewHandler, HandleHealth  (Task 3)
  version.go           # HandleUpload, HandleDownload + helpers  (Task 3)
  routes.go            # registerRoutes  (Task 3)
  middleware.go        # requestIDMiddleware, accessLogMiddleware  (Task 3)
  config.go            # config struct  (Task 4)
  main.go              # main, run()  (Task 4)
  cache_test.go        # Task 1
  store_minio_test.go  # Task 2 (ensureBucket + isNotFound units)
  handler_test.go      # Task 3 (health + shared test helpers)
  version_test.go      # Task 3 (upload/download)
  config_test.go       # Task 4
  integration_test.go  # //go:build integration  (Task 5)
  mock_store_test.go   # generated  (Task 2)
  deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}  (Task 4)
docs/client-api.md                 # new §12  (Task 6)
docs/client-api/request-reply.md   # matching HTTP block  (Task 6)
```

Reference files to read before writing: `media-service/cache.go`, `upload-service/{store_minio.go,middleware.go,main.go,routes.go,deploy/*}`, `pkg/minioutil/minio.go`, `pkg/testutil/minio.go`.

---

### Task 1: `blobCache` — bounded TTL+size download cache with singleflight

**Files:**
- Create: `client-update-service/cache.go`
- Test: `client-update-service/cache_test.go`

**Interfaces:**
- Consumes: `lru "github.com/hashicorp/golang-lru/v2/expirable"`, `golang.org/x/sync/singleflight`.
- Produces:
  - `type cachedBlob struct { body []byte; contentType string }`
  - `type blobCache struct { lru *lru.LRU[string, cachedBlob]; sf singleflight.Group; maxObjectBytes int64 }`
  - `func newBlobCache(maxEntries int, ttl time.Duration, maxObjectBytes int64) *blobCache`
  - `func (c *blobCache) get(key string) (cachedBlob, bool)`
  - `func (c *blobCache) add(key string, b cachedBlob)`
  - `func (c *blobCache) remove(key string)`
  - `func (c *blobCache) loadCacheable(key string, loader func() (cachedBlob, bool, error)) (cachedBlob, bool, error)`

- [ ] **Step 1: Write the failing tests**

Create `client-update-service/cache_test.go`:

```go
package main

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobCache_AddGetRemove(t *testing.T) {
	c := newBlobCache(4, time.Hour, 1024)
	_, ok := c.get("k")
	assert.False(t, ok)

	c.add("k", cachedBlob{body: []byte("v"), contentType: "text/plain"})
	got, ok := c.get("k")
	require.True(t, ok)
	assert.Equal(t, []byte("v"), got.body)
	assert.Equal(t, "text/plain", got.contentType)

	c.remove("k")
	_, ok = c.get("k")
	assert.False(t, ok)
}

func TestBlobCache_AddRefusesOversized(t *testing.T) {
	c := newBlobCache(4, time.Hour, 3)
	c.add("k", cachedBlob{body: []byte("toolong")})
	_, ok := c.get("k")
	assert.False(t, ok, "body over maxObjectBytes must not be cached")
}

func TestBlobCache_TTLExpiry(t *testing.T) {
	c := newBlobCache(4, 50*time.Millisecond, 1024)
	c.add("k", cachedBlob{body: []byte("v")})
	_, ok := c.get("k")
	require.True(t, ok)
	assert.Eventually(t, func() bool {
		_, ok := c.get("k")
		return !ok
	}, time.Second, 10*time.Millisecond, "entry must expire after ttl")
}

func TestBlobCache_LRUEviction(t *testing.T) {
	c := newBlobCache(2, time.Hour, 1024)
	c.add("a", cachedBlob{body: []byte("a")})
	c.add("b", cachedBlob{body: []byte("b")})
	c.add("d", cachedBlob{body: []byte("d")}) // evicts "a"
	_, ok := c.get("a")
	assert.False(t, ok)
	_, ok = c.get("b")
	assert.True(t, ok)
	_, ok = c.get("d")
	assert.True(t, ok)
}

func TestBlobCache_Disabled(t *testing.T) {
	c := newBlobCache(0, time.Hour, 1024)
	c.add("k", cachedBlob{body: []byte("v")})
	_, ok := c.get("k")
	assert.False(t, ok)
	c.remove("k") // must not panic

	blob, cacheable, err := c.loadCacheable("k", func() (cachedBlob, bool, error) {
		return cachedBlob{body: []byte("loaded")}, true, nil
	})
	require.NoError(t, err)
	assert.True(t, cacheable)
	assert.Equal(t, []byte("loaded"), blob.body)
	_, ok = c.get("k") // disabled cache stored nothing
	assert.False(t, ok)
}

func TestBlobCache_LoadCacheable_CachesAndReuses(t *testing.T) {
	c := newBlobCache(4, time.Hour, 1024)
	blob, cacheable, err := c.loadCacheable("k", func() (cachedBlob, bool, error) {
		return cachedBlob{body: []byte("v"), contentType: "x"}, true, nil
	})
	require.NoError(t, err)
	assert.True(t, cacheable)
	assert.Equal(t, []byte("v"), blob.body)
	got, ok := c.get("k")
	require.True(t, ok)
	assert.Equal(t, []byte("v"), got.body)
}

func TestBlobCache_LoadCacheable_NonCacheableNotStored(t *testing.T) {
	c := newBlobCache(4, time.Hour, 1024)
	_, cacheable, err := c.loadCacheable("k", func() (cachedBlob, bool, error) {
		return cachedBlob{contentType: "x"}, false, nil
	})
	require.NoError(t, err)
	assert.False(t, cacheable)
	_, ok := c.get("k")
	assert.False(t, ok)
}

func TestBlobCache_LoadCacheable_PropagatesError(t *testing.T) {
	c := newBlobCache(4, time.Hour, 1024)
	sentinel := errors.New("boom")
	_, _, err := c.loadCacheable("k", func() (cachedBlob, bool, error) {
		return cachedBlob{}, false, sentinel
	})
	assert.ErrorIs(t, err, sentinel)
}

func TestBlobCache_LoadCacheable_SingleflightCollapses(t *testing.T) {
	c := newBlobCache(4, time.Hour, 1024)
	var calls int32
	entered := make(chan struct{}) // closed once, when the first loader is in-flight
	release := make(chan struct{})
	var once sync.Once

	loader := func() (cachedBlob, bool, error) {
		atomic.AddInt32(&calls, 1)
		once.Do(func() { close(entered) })
		<-release
		return cachedBlob{body: []byte("v")}, true, nil
	}

	const n = 8
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _, _, err := c.loadCacheable("k", loader); assert.NoError(t, err) }()
	<-entered // deterministic: first loader is inside singleflight, no time.Sleep needed
	for i := 0; i < n-1; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _, err := c.loadCacheable("k", loader); assert.NoError(t, err) }()
	}
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "singleflight must collapse concurrent misses into one loader call")
}
```

> Note: `time.Sleep` here is a test-only convergence nudge for singleflight, not production goroutine synchronization — the CLAUDE.md ban is on sync in production code.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=client-update-service`
Expected: FAIL — `undefined: newBlobCache` / `cachedBlob` / `blobCache`.

- [ ] **Step 3: Implement `cache.go`**

Create `client-update-service/cache.go`:

```go
package main

import (
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"
)

// cachedBlob is a fully-read artifact held in memory for fast re-serving.
type cachedBlob struct {
	body        []byte
	contentType string
}

// blobCache fronts version downloads with a size+TTL-bounded LRU and singleflight.
// A nil lru means the cache is disabled (get always misses, add/remove no-op).
type blobCache struct {
	lru            *lru.LRU[string, cachedBlob]
	sf             singleflight.Group
	maxObjectBytes int64
}

// newBlobCache builds a cache bounded to maxEntries LRU slots and a per-entry ttl.
// Objects larger than maxObjectBytes are never stored. maxEntries<=0 or ttl<=0
// disables caching entirely.
func newBlobCache(maxEntries int, ttl time.Duration, maxObjectBytes int64) *blobCache {
	c := &blobCache{maxObjectBytes: maxObjectBytes}
	if maxEntries > 0 && ttl > 0 {
		c.lru = lru.NewLRU[string, cachedBlob](maxEntries, nil, ttl)
	}
	return c
}

func (c *blobCache) get(key string) (cachedBlob, bool) {
	if c.lru == nil {
		return cachedBlob{}, false
	}
	return c.lru.Get(key)
}

func (c *blobCache) add(key string, b cachedBlob) {
	if c.lru == nil || int64(len(b.body)) > c.maxObjectBytes {
		return
	}
	c.lru.Add(key, b)
}

func (c *blobCache) remove(key string) {
	if c.lru == nil {
		return
	}
	c.lru.Remove(key)
}

// loadCacheable collapses concurrent misses for key via singleflight. loader opens
// the object and returns (blob, cacheable): a cacheable blob is stored and shared
// with all waiters; a non-cacheable result (object over maxObjectBytes) is returned
// but never stored, so its callers fall back to direct streaming.
func (c *blobCache) loadCacheable(key string, loader func() (cachedBlob, bool, error)) (cachedBlob, bool, error) {
	type result struct {
		blob      cachedBlob
		cacheable bool
	}
	v, err, _ := c.sf.Do(key, func() (any, error) {
		if b, ok := c.get(key); ok {
			return result{blob: b, cacheable: true}, nil
		}
		blob, cacheable, err := loader()
		if err != nil {
			return nil, err
		}
		if cacheable {
			c.add(key, blob)
		}
		return result{blob: blob, cacheable: cacheable}, nil
	})
	if err != nil {
		return cachedBlob{}, false, err
	}
	r := v.(result)
	return r.blob, r.cacheable, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

```bash
make lint
git add client-update-service/cache.go client-update-service/cache_test.go
git commit -m "feat(client-update-service): bounded TTL+size download cache"
```

---

### Task 2: Store interface + MinIO streaming impl + ensureBucket

**Files:**
- Create: `client-update-service/store.go`
- Create: `client-update-service/store_minio.go`
- Create: `client-update-service/store_minio_test.go`
- Generate: `client-update-service/mock_store_test.go`

**Interfaces:**
- Consumes: `minioutil.ObjectStore` (has `BucketExists/PutObject/GetObject/ListObjects/RemoveObject`), `minio.{PutObjectOptions,GetObjectOptions,MakeBucketOptions,ToErrorResponse}`, `*minio.Object` (`Stat()`, `Read`, `Close`).
- Produces:
  - `type blobInfo struct { Size int64; ContentType string }`
  - `var ErrObjectNotFound = errors.New("object not found")`
  - `type versionStore interface { Put(ctx, key, r, size, contentType) error; Open(ctx, key) (io.ReadCloser, blobInfo, error) }`
  - `func newMinioVersionStore(client minioutil.ObjectStore, bucket string, downloadTimeout time.Duration) *minioVersionStore`
  - `type bucketClient interface { BucketExists(ctx, name) (bool, error); MakeBucket(ctx, name, opts minio.MakeBucketOptions) error }`
  - `func ensureBucket(ctx context.Context, client bucketClient, name string) error`
  - `func isNotFound(err error) bool`
  - `cancelReadCloser`
  - Generated `MockversionStore` (used by Task 3).

- [ ] **Step 1: Write `store.go` (interface + generate directive)**

Create `client-update-service/store.go`:

```go
package main

import (
	"context"
	"errors"
	"io"
)

// ErrObjectNotFound is returned (wrapped) by Open when no object matches the key.
var ErrObjectNotFound = errors.New("object not found")

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// blobInfo is the object metadata the download path needs for response headers
// and the cache size decision.
type blobInfo struct {
	Size        int64
	ContentType string
}

// versionStore is the subset of object storage the update handlers need.
type versionStore interface {
	// Put streams r (of known size) to the object at key with the given content type.
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	// Open returns a streaming reader plus metadata (from the object's own Stat),
	// or ErrObjectNotFound (wrapped) when the object is absent.
	Open(ctx context.Context, key string) (io.ReadCloser, blobInfo, error)
}
```

- [ ] **Step 2: Generate the mock**

Run: `make generate SERVICE=client-update-service`
Expected: creates `client-update-service/mock_store_test.go` with `MockversionStore`.

- [ ] **Step 3: Write the failing unit tests**

Create `client-update-service/store_minio_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBucketClient stands in for a MinIO client's bucket-management surface.
type fakeBucketClient struct {
	exists     bool
	existsErr  error
	makeErr    error
	madeBucket string
}

func (f *fakeBucketClient) BucketExists(_ context.Context, _ string) (bool, error) {
	return f.exists, f.existsErr
}

func (f *fakeBucketClient) MakeBucket(_ context.Context, name string, _ minio.MakeBucketOptions) error {
	f.madeBucket = name
	return f.makeErr
}

func TestEnsureBucket_ExistsNoCreate(t *testing.T) {
	f := &fakeBucketClient{exists: true}
	require.NoError(t, ensureBucket(context.Background(), f, "b"))
	assert.Equal(t, "", f.madeBucket, "must not create an existing bucket")
}

func TestEnsureBucket_AbsentCreates(t *testing.T) {
	f := &fakeBucketClient{exists: false}
	require.NoError(t, ensureBucket(context.Background(), f, "b"))
	assert.Equal(t, "b", f.madeBucket)
}

func TestEnsureBucket_RacyCreateTreatedSuccess(t *testing.T) {
	f := &fakeBucketClient{exists: false, makeErr: minio.ErrorResponse{Code: "BucketAlreadyOwnedByYou"}}
	require.NoError(t, ensureBucket(context.Background(), f, "b"))
}

func TestEnsureBucket_ExistsCheckError(t *testing.T) {
	f := &fakeBucketClient{existsErr: errors.New("net")}
	assert.Error(t, ensureBucket(context.Background(), f, "b"))
}

func TestEnsureBucket_MakeError(t *testing.T) {
	f := &fakeBucketClient{exists: false, makeErr: minio.ErrorResponse{Code: "AccessDenied"}}
	assert.Error(t, ensureBucket(context.Background(), f, "b"))
}

func TestIsNotFound(t *testing.T) {
	assert.True(t, isNotFound(minio.ErrorResponse{Code: "NoSuchKey"}))
	assert.False(t, isNotFound(minio.ErrorResponse{Code: "AccessDenied"}))
	assert.False(t, isNotFound(errors.New("plain")))
}
```

Run: `make test SERVICE=client-update-service`
Expected: FAIL — `undefined: ensureBucket` / `isNotFound`.

- [ ] **Step 4: Implement `store_minio.go`**

Create `client-update-service/store_minio.go`:

```go
package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/hmchangw/chat/pkg/minioutil"
)

// minioVersionStore streams version artifacts in and out of a single MinIO bucket.
type minioVersionStore struct {
	client          minioutil.ObjectStore
	bucket          string
	downloadTimeout time.Duration
}

// newMinioVersionStore binds a minio client to a bucket. downloadTimeout bounds a
// single Open (Stat probe + streamed body) so a hung backend cannot hang forever.
func newMinioVersionStore(client minioutil.ObjectStore, bucket string, downloadTimeout time.Duration) *minioVersionStore {
	return &minioVersionStore{client: client, bucket: bucket, downloadTimeout: downloadTimeout}
}

func (s *minioVersionStore) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	if _, err := s.client.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{ContentType: contentType}); err != nil {
		return fmt.Errorf("put object %s/%s: %w", s.bucket, key, err)
	}
	return nil
}

// Open returns a streaming reader for key. It Stat-probes first so a missing object
// or dead backend surfaces before any body is written; the returned blobInfo carries
// the object's Size and ContentType. minio-go ties Reads to the GetObject context, so
// the cancel must outlive Open — cancelReadCloser releases it on Close.
func (s *minioVersionStore) Open(ctx context.Context, key string) (io.ReadCloser, blobInfo, error) {
	tctx, cancel := context.WithTimeout(ctx, s.downloadTimeout)
	obj, err := s.client.GetObject(tctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		cancel()
		return nil, blobInfo{}, fmt.Errorf("get object %s/%s: %w", s.bucket, key, err)
	}
	info, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		cancel()
		if isNotFound(err) {
			return nil, blobInfo{}, fmt.Errorf("stat object %s/%s: %w", s.bucket, key, ErrObjectNotFound)
		}
		return nil, blobInfo{}, fmt.Errorf("stat object %s/%s: %w", s.bucket, key, err)
	}
	return &cancelReadCloser{ReadCloser: obj, cancel: cancel}, blobInfo{Size: info.Size, ContentType: info.ContentType}, nil
}

// isNotFound reports whether err is MinIO's NoSuchKey (missing object).
func isNotFound(err error) bool {
	return minio.ToErrorResponse(err).Code == "NoSuchKey"
}

// bucketClient is the bucket-management surface not exposed by minioutil.ObjectStore.
// Both *minio.Client and *o11yminio.Client satisfy it.
type bucketClient interface {
	BucketExists(ctx context.Context, name string) (bool, error)
	MakeBucket(ctx context.Context, name string, opts minio.MakeBucketOptions) error
}

// ensureBucket creates the bucket when absent. It is idempotent and race-safe: a
// concurrent create surfacing as BucketAlreadyOwnedByYou/BucketAlreadyExists is
// treated as success.
func ensureBucket(ctx context.Context, client bucketClient, name string) error {
	exists, err := client.BucketExists(ctx, name)
	if err != nil {
		return fmt.Errorf("bucket exists check %q: %w", name, err)
	}
	if exists {
		return nil
	}
	if err := client.MakeBucket(ctx, name, minio.MakeBucketOptions{}); err != nil {
		switch minio.ToErrorResponse(err).Code {
		case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			return nil
		}
		return fmt.Errorf("make bucket %q: %w", name, err)
	}
	return nil
}

// cancelReadCloser cancels the download's timeout context when the reader is closed.
type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS (cache + store units). MinIO `Put`/`Open` are exercised end-to-end in Task 5.

- [ ] **Step 6: Lint + commit**

```bash
make lint
git add client-update-service/store.go client-update-service/store_minio.go \
  client-update-service/store_minio_test.go client-update-service/mock_store_test.go
git commit -m "feat(client-update-service): streaming MinIO store + ensure-bucket"
```

---

### Task 3: Handlers — upload, download, health, routes, middleware

**Files:**
- Create: `client-update-service/handler.go`
- Create: `client-update-service/version.go`
- Create: `client-update-service/routes.go`
- Create: `client-update-service/middleware.go`
- Create: `client-update-service/handler_test.go`
- Create: `client-update-service/version_test.go`

**Interfaces:**
- Consumes: `versionStore` + generated `MockversionStore` (Task 2), `blobCache` + `newBlobCache` (Task 1), `objectKey`, `errcode`, `errhttp`, `gin`, `pkg/idgen`, `pkg/natsutil`.
- Produces:
  - `type Handler struct { store versionStore; cache *blobCache }`
  - `func NewHandler(store versionStore, cache *blobCache) *Handler`
  - `func (h *Handler) HandleHealth(c *gin.Context)`
  - `func (h *Handler) HandleUpload(c *gin.Context)`
  - `func (h *Handler) HandleDownload(c *gin.Context)`
  - `func registerRoutes(r *gin.Engine, h *Handler)`
  - `func objectKey(fileName string) string`, `func validFileName(name string) bool`
  - `func requestIDMiddleware() gin.HandlerFunc`, `func accessLogMiddleware() gin.HandlerFunc`

- [ ] **Step 1: Write the failing tests**

Create `client-update-service/handler_test.go` (health + shared helpers):

```go
package main

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func init() { gin.SetMode(gin.TestMode) }

// testCache returns an enabled cache with a small object cap so "too large" is easy to hit.
func testCache(maxObjectBytes int64) *blobCache {
	return newBlobCache(4, time.Hour, maxObjectBytes)
}

func rc(s string) io.ReadCloser { return io.NopCloser(strings.NewReader(s)) }

type fileSpec struct{ name, content string }

// multipartBody builds a multipart form; omit a field by leaving it out of files.
func multipartBody(t *testing.T, files map[string]fileSpec) (*bytes.Buffer, string) {
	t.Helper()
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	for field, fs := range files {
		fw, err := w.CreateFormFile(field, fs.name)
		require.NoError(t, err)
		_, err = io.WriteString(fw, fs.content)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return body, w.FormDataContentType()
}

func uploadCtx(t *testing.T, body *bytes.Buffer, contentType string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	c.Request.Header.Set("Content-Type", contentType)
	return c, w
}

func downloadCtx(t *testing.T, fileName string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/version/"+fileName, nil)
	c.Params = gin.Params{{Key: "fileName", Value: fileName}}
	return c, w
}

func TestHandleHealth(t *testing.T) {
	h := NewHandler(nil, testCache(1024))
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	h.HandleHealth(c)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"status":"ok"}`, w.Body.String())
}

// TestRoutesRegistered proves the three routes are wired.
func TestRoutesRegistered(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	r := gin.New()
	registerRoutes(r, h)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, w.Code)
}
```

Create `client-update-service/version_test.go`:

```go
package main

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestHandleUpload_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Put(gomock.Any(), objectKey("app.yaml"), gomock.Any(), int64(6), "application/x-yaml").Return(nil)
	store.EXPECT().Put(gomock.Any(), objectKey("app.exe"), gomock.Any(), int64(4), "application/octet-stream").Return(nil)
	h := NewHandler(store, testCache(1024))

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "config"},
		"executeFile": {name: "app.exe", content: "bin!"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"result":"success"}`, w.Body.String())
}

func TestHandleUpload_MissingConfigFile_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	body, ct := multipartBody(t, map[string]fileSpec{"executeFile": {name: "app.exe", content: "bin"}})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_MissingExecuteFile_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	body, ct := multipartBody(t, map[string]fileSpec{"configFile": {name: "app.yaml", content: "c"}})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_EmptyFile_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: ""},
		"executeFile": {name: "app.exe", content: "bin"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_WrongConfigExt_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.txt", content: "c"},
		"executeFile": {name: "app.exe", content: "bin"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_MalformedMultipart_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	c, w := uploadCtx(t, bytesBufferString("not multipart"), "multipart/form-data; boundary=nope")
	h.HandleUpload(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpload_StoreError_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Put(gomock.Any(), objectKey("app.yaml"), gomock.Any(), int64(1), "application/x-yaml").
		Return(errors.New("minio down"))
	h := NewHandler(store, testCache(1024))
	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "c"},
		"executeFile": {name: "app.exe", content: "bin"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleUpload_OverwriteEvictsCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Put(gomock.Any(), objectKey("app.yaml"), gomock.Any(), gomock.Any(), "application/x-yaml").Return(nil)
	store.EXPECT().Put(gomock.Any(), objectKey("app.exe"), gomock.Any(), gomock.Any(), "application/octet-stream").Return(nil)
	cache := testCache(1024)
	cache.add(objectKey("app.yaml"), cachedBlob{body: []byte("stale"), contentType: "application/x-yaml"})
	h := NewHandler(store, cache)

	body, ct := multipartBody(t, map[string]fileSpec{
		"configFile":  {name: "app.yaml", content: "new"},
		"executeFile": {name: "app.exe", content: "bin"},
	})
	c, w := uploadCtx(t, body, ct)
	h.HandleUpload(c)
	require.Equal(t, http.StatusOK, w.Code)
	_, ok := cache.get(objectKey("app.yaml"))
	assert.False(t, ok, "overwrite must evict the stale cached copy")
}

func TestHandleDownload_CacheHit_NoStoreCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl) // no EXPECT: any store call fails the test
	cache := testCache(1024)
	cache.add(objectKey("app.exe"), cachedBlob{body: []byte("BIN"), contentType: "application/octet-stream"})
	h := NewHandler(store, cache)

	c, w := downloadCtx(t, "app.exe")
	h.HandleDownload(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "BIN", w.Body.String())
	assert.Equal(t, "application/octet-stream", w.Header().Get("Content-Type"))
	assert.Contains(t, w.Header().Get("Content-Disposition"), `filename="app.exe"`)
}

func TestHandleDownload_MissCacheable_CachesAndServes(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Open(gomock.Any(), objectKey("app.yaml")).
		Return(rc("hello"), blobInfo{Size: 5, ContentType: "application/x-yaml"}, nil).Times(1)
	cache := testCache(1024)
	h := NewHandler(store, cache)

	c, w := downloadCtx(t, "app.yaml")
	h.HandleDownload(c)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "hello", w.Body.String())
	assert.Equal(t, "application/x-yaml", w.Header().Get("Content-Type"))

	// Second request must be served from cache — no further Open (Times(1) above enforces it).
	c2, w2 := downloadCtx(t, "app.yaml")
	h.HandleDownload(c2)
	require.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, "hello", w2.Body.String())
}

func TestHandleDownload_MissTooLarge_StreamsUncached(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	// cap = 3; object is 7 bytes → non-cacheable → loader Open + stream Open = 2 opens.
	store.EXPECT().Open(gomock.Any(), objectKey("big.exe")).
		DoAndReturn(func(_ any, _ string) (any, blobInfo, error) {
			return rc("BIGDATA"), blobInfo{Size: 7, ContentType: "application/octet-stream"}, nil
		}).Times(2)
	cache := testCache(3)
	h := NewHandler(store, cache)

	c, w := downloadCtx(t, "big.exe")
	h.HandleDownload(c)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "BIGDATA", w.Body.String())
	_, ok := cache.get(objectKey("big.exe"))
	assert.False(t, ok, "oversized object must not be cached")
}

func TestHandleDownload_NotFound_404(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Open(gomock.Any(), objectKey("missing.exe")).
		Return(nil, blobInfo{}, errors.New("stat: "+ErrObjectNotFound.Error()))
	h := NewHandler(store, testCache(1024))
	c, w := downloadCtx(t, "missing.exe")
	h.HandleDownload(c)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleDownload_StoreError_500(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockversionStore(ctrl)
	store.EXPECT().Open(gomock.Any(), objectKey("app.exe")).Return(nil, blobInfo{}, errors.New("minio down"))
	h := NewHandler(store, testCache(1024))
	c, w := downloadCtx(t, "app.exe")
	h.HandleDownload(c)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleDownload_InvalidName_400(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewHandler(NewMockversionStore(ctrl), testCache(1024))
	for _, name := range []string{"", "..", "a/b", "a..b"} {
		c, w := downloadCtx(t, name)
		h.HandleDownload(c)
		assert.Equal(t, http.StatusBadRequest, w.Code, "name %q must be rejected", name)
	}
}
```

Add the tiny helper to `handler_test.go`:

```go
func bytesBufferString(s string) *bytes.Buffer { return bytes.NewBufferString(s) }
```

> The `TestHandleDownload_NotFound_404` mock returns an error whose chain includes `ErrObjectNotFound` — build it with `fmt.Errorf("stat: %w", ErrObjectNotFound)` in the real impl; the handler uses `errors.Is`. In the test above the store is a mock, so return `fmt.Errorf("stat object: %w", ErrObjectNotFound)` — replace the placeholder string with that exact wrap. (Import `fmt`.)

Fix that one mock return to use the real sentinel so `errors.Is` matches:

```go
store.EXPECT().Open(gomock.Any(), objectKey("missing.exe")).
	Return(nil, blobInfo{}, fmt.Errorf("stat object: %w", ErrObjectNotFound))
```

Run: `make test SERVICE=client-update-service`
Expected: FAIL — handlers/routes undefined.

- [ ] **Step 2: Implement `handler.go`**

Create `client-update-service/handler.go`:

```go
package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Handler holds the client-update-service dependencies.
type Handler struct {
	store versionStore
	cache *blobCache
}

// NewHandler wires the handler. store backs artifact persistence; cache fronts
// downloads with a bounded TTL+size in-memory cache.
func NewHandler(store versionStore, cache *blobCache) *Handler {
	return &Handler{store: store, cache: cache}
}

// HandleHealth is the liveness probe.
func (h *Handler) HandleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
```

- [ ] **Step 3: Implement `version.go`**

Create `client-update-service/version.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

const (
	objectPrefix     = "chat.go/chat-versions/"
	configFileField  = "configFile"
	executeFileField = "executeFile"
)

// objectKey namespaces a version file within the shared bucket.
func objectKey(fileName string) string { return objectPrefix + fileName }

// validFileName rejects empty or path-unsafe names (traversal / separators).
func validFileName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
		return false
	}
	return true
}

// HandleUpload stores a configFile (.yaml/.yml) + executeFile pair, streaming each
// straight to MinIO. No size cap — streaming keeps memory bounded regardless of size.
func (h *Handler) HandleUpload(c *gin.Context) {
	ctx := c.Request.Context()

	cfgFile, err := c.FormFile(configFileField)
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("configFile is required"))
		return
	}
	exeFile, err := c.FormFile(executeFileField)
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("executeFile is required"))
		return
	}
	if cfgFile.Size == 0 || exeFile.Size == 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("uploaded files must not be empty"))
		return
	}
	if !hasYAMLExt(cfgFile.Filename) {
		errhttp.Write(ctx, c, errcode.BadRequest("configFile must be a .yaml or .yml file"))
		return
	}

	if err := h.storeFormFile(ctx, cfgFile, "application/x-yaml"); err != nil {
		errhttp.Write(ctx, c, err)
		return
	}
	if err := h.storeFormFile(ctx, exeFile, "application/octet-stream"); err != nil {
		errhttp.Write(ctx, c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": "success"})
}

// storeFormFile streams one multipart part to MinIO and drops any stale cached copy.
func (h *Handler) storeFormFile(ctx context.Context, fh *multipart.FileHeader, contentType string) error {
	f, err := fh.Open()
	if err != nil {
		return fmt.Errorf("open upload %q: %w", fh.Filename, err)
	}
	defer f.Close()
	key := objectKey(fh.Filename)
	if err := h.store.Put(ctx, key, f, fh.Size, contentType); err != nil {
		return fmt.Errorf("store upload %q: %w", fh.Filename, err)
	}
	h.cache.remove(key)
	return nil
}

func hasYAMLExt(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return ext == ".yaml" || ext == ".yml"
}

// HandleDownload serves an artifact by name from the cache, or MinIO on a miss.
func (h *Handler) HandleDownload(c *gin.Context) {
	ctx := c.Request.Context()
	fileName := c.Param("fileName")
	if !validFileName(fileName) {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid fileName"))
		return
	}
	key := objectKey(fileName)

	if blob, ok := h.cache.get(key); ok {
		serveBytes(c, fileName, blob)
		return
	}

	blob, cacheable, err := h.cache.loadCacheable(key, func() (cachedBlob, bool, error) {
		return h.loadObject(ctx, key)
	})
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			errhttp.Write(ctx, c, errcode.NotFound("version not found"))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("load version %q: %w", fileName, err))
		return
	}
	if cacheable {
		serveBytes(c, fileName, blob)
		return
	}
	h.streamObject(ctx, c, fileName, key)
}

// loadObject opens key and, when it fits the cache cap, reads its whole body into a
// cachedBlob (cacheable=true). Oversized objects return cacheable=false with only the
// content-type, so the caller streams them instead.
func (h *Handler) loadObject(ctx context.Context, key string) (cachedBlob, bool, error) {
	rc, info, err := h.store.Open(ctx, key)
	if err != nil {
		return cachedBlob{}, false, err
	}
	defer rc.Close()
	if info.Size > h.cache.maxObjectBytes {
		return cachedBlob{contentType: info.ContentType}, false, nil
	}
	body := make([]byte, info.Size)
	if _, err := io.ReadFull(rc, body); err != nil {
		return cachedBlob{}, false, fmt.Errorf("read object body: %w", err)
	}
	return cachedBlob{body: body, contentType: info.ContentType}, true, nil
}

// streamObject re-opens key and streams it straight to the client, uncached.
func (h *Handler) streamObject(ctx context.Context, c *gin.Context, fileName, key string) {
	rc, info, err := h.store.Open(ctx, key)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			errhttp.Write(ctx, c, errcode.NotFound("version not found"))
			return
		}
		errhttp.Write(ctx, c, fmt.Errorf("open version %q: %w", fileName, err))
		return
	}
	defer rc.Close()
	c.DataFromReader(http.StatusOK, info.Size, info.ContentType, rc, map[string]string{
		"Content-Disposition": contentDisposition(fileName),
	})
}

func serveBytes(c *gin.Context, fileName string, blob cachedBlob) {
	c.Header("Content-Disposition", contentDisposition(fileName))
	c.Data(http.StatusOK, blob.contentType, blob.body)
}

func contentDisposition(fileName string) string {
	return fmt.Sprintf("attachment; filename=%q", fileName)
}
```

- [ ] **Step 4: Implement `routes.go`**

Create `client-update-service/routes.go`:

```go
package main

import "github.com/gin-gonic/gin"

// registerRoutes wires the health probe plus the /api/v1 version endpoints.
func registerRoutes(r *gin.Engine, h *Handler) {
	r.GET("/healthz", h.HandleHealth)

	api := r.Group("/api/v1")
	api.POST("/version", h.HandleUpload)
	api.GET("/version/:fileName", h.HandleDownload)
}
```

- [ ] **Step 5: Implement `middleware.go`**

Create `client-update-service/middleware.go`:

```go
package main

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// requestIDMiddleware extracts or mints the request correlation ID.
func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(natsutil.RequestIDHeader)
		if !idgen.IsValidUUID(id) {
			id = idgen.GenerateRequestID()
		}
		c.Set("request_id", id)
		c.Request = c.Request.WithContext(natsutil.WithRequestID(c.Request.Context(), id))
		c.Header(natsutil.RequestIDHeader, id)
		c.Next()
	}
}

// accessLogMiddleware logs one structured line per request.
func accessLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		slog.Info("request",
			"request_id", c.GetString("request_id"),
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", time.Since(start).Milliseconds(),
			"client_ip", c.ClientIP(),
		)
	}
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS.

- [ ] **Step 7: Lint + commit**

```bash
make lint
git add client-update-service/handler.go client-update-service/version.go \
  client-update-service/routes.go client-update-service/middleware.go \
  client-update-service/handler_test.go client-update-service/version_test.go
git commit -m "feat(client-update-service): upload/download/health handlers + routes"
```

---

### Task 4: Wiring (`config.go`, `main.go`) + deploy

**Files:**
- Create: `client-update-service/config.go`
- Create: `client-update-service/main.go`
- Create: `client-update-service/config_test.go`
- Create: `client-update-service/deploy/Dockerfile`
- Create: `client-update-service/deploy/docker-compose.yml`
- Create: `client-update-service/deploy/azure-pipelines.yml`

**Interfaces:**
- Consumes: `env.ParseAs`, `obs.Init`, `minioutil.Connect`/`WithObservability`, `bucketClient`, `ensureBucket`, `newMinioVersionStore`, `newBlobCache`, `NewHandler`, `registerRoutes`, `requestIDMiddleware`, `accessLogMiddleware`, `shutdown.Wait`, `o11ygin.Middleware`.
- Produces: `type config struct{...}`, `func run() error`, `func main()`.

- [ ] **Step 1: Write the failing config test**

Create `client-update-service/config_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Defaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")
	t.Setenv("MINIO_BUCKET", "chat-updates")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, "8080", cfg.Port)
	assert.Equal(t, 4, cfg.CacheMaxEntries)
	assert.Equal(t, 24*time.Hour, cfg.CacheTTL)
	assert.Equal(t, int64(536870912), cfg.CacheMaxObjectBytes)
	assert.Equal(t, 5*time.Minute, cfg.MinioDownloadTimeout)
	assert.Equal(t, 10*time.Minute, cfg.HTTPWriteTimeout)
}

func TestConfig_RequiresSecrets(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	// MINIO_* and MINIO_BUCKET unset → parse must fail.
	_, err := env.ParseAs[config]()
	assert.Error(t, err)
}
```

Run: `make test SERVICE=client-update-service`
Expected: FAIL — `undefined: config`.

- [ ] **Step 2: Implement `config.go`**

Create `client-update-service/config.go`:

```go
package main

import "time"

type config struct {
	Port   string `env:"PORT" envDefault:"8080"`
	SiteID string `env:"SITE_ID,required"`

	MinioEndpoint        string        `env:"MINIO_ENDPOINT,required"`
	MinioAccessKey       string        `env:"MINIO_ACCESS_KEY,required"`
	MinioSecretKey       string        `env:"MINIO_SECRET_KEY,required"`
	MinioUseSSL          bool          `env:"MINIO_USE_SSL" envDefault:"false"`
	MinioBucket          string        `env:"MINIO_BUCKET,required"`
	MinioDownloadTimeout time.Duration `env:"MINIO_DOWNLOAD_TIMEOUT" envDefault:"5m"`

	HTTPWriteTimeout time.Duration `env:"HTTP_WRITE_TIMEOUT" envDefault:"10m"`

	CacheMaxEntries     int           `env:"CACHE_MAX_ENTRIES" envDefault:"4"`
	CacheTTL            time.Duration `env:"CACHE_TTL" envDefault:"24h"`
	CacheMaxObjectBytes int64         `env:"CACHE_MAX_OBJECT_BYTES" envDefault:"536870912"`
}
```

- [ ] **Step 3: Implement `main.go`**

Create `client-update-service/main.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/gin-gonic/gin"

	o11ygin "github.com/flywindy/o11y/gin"

	"github.com/hmchangw/chat/pkg/minioutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	minioClient, err := minioutil.Connect(ctx, cfg.MinioEndpoint, cfg.MinioUseSSL, cfg.MinioAccessKey, cfg.MinioSecretKey, minioutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("minio connect: %w", err)
	}
	bc, ok := minioClient.(bucketClient)
	if !ok {
		return fmt.Errorf("minio client %T does not support bucket creation", minioClient)
	}
	if err := ensureBucket(ctx, bc, cfg.MinioBucket); err != nil {
		return fmt.Errorf("ensure bucket: %w", err)
	}

	store := newMinioVersionStore(minioClient, cfg.MinioBucket, cfg.MinioDownloadTimeout)
	cache := newBlobCache(cfg.CacheMaxEntries, cfg.CacheTTL, cfg.CacheMaxObjectBytes)
	handler := NewHandler(store, cache)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(o11ygin.Middleware("client-update-service", sdk.TracerProvider(), sdk.MeterProvider(), sdk.Propagator, o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	registerRoutes(r, handler)

	addr := fmt.Sprintf(":%s", cfg.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: cfg.HTTPWriteTimeout,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("client-update-service starting", "addr", addr, "site", cfg.SiteID)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second,
			func(ctx context.Context) error { return srv.Shutdown(ctx) },
			// obsShutdown LAST so all prior teardown telemetry is exported.
			func(ctx context.Context) error { return obsShutdown(ctx) },
		)
	}()

	err = <-srvErr
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen client-update server: %w", err)
	}
	<-shutdownDone
	return nil
}
```

- [ ] **Step 4: Run the config test + build**

Run: `make test SERVICE=client-update-service` then `make build SERVICE=client-update-service`
Expected: tests PASS, binary builds.

- [ ] **Step 5: Create the deploy files**

Create `client-update-service/deploy/Dockerfile`:

```dockerfile
FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY client-update-service/ client-update-service/
RUN CGO_ENABLED=0 go build -o /client-update-service ./client-update-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /client-update-service /client-update-service
USER app
ENTRYPOINT ["/client-update-service"]
```

Create `client-update-service/deploy/docker-compose.yml`:

```yaml
name: client-update-service

services:
  client-update-service:
    build:
      context: ../..
      dockerfile: client-update-service/deploy/Dockerfile
    ports:
      - "8087:8080"
    env_file:
      - path: ../../docker-local/.env
        required: false
    environment:
      - OTEL_SERVICE_NAME=client-update-service
      - OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://otel-collector:4318}
      - PORT=8080
      - SITE_ID=${SITE_ID:-site-local}
      - MINIO_ENDPOINT=${MINIO_ENDPOINT:-minio:9000}
      - MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
      - MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}
      - MINIO_USE_SSL=false
      - MINIO_BUCKET=${MINIO_BUCKET:-chat-updates}
      - CACHE_MAX_ENTRIES=4
      - CACHE_TTL=24h
      - CACHE_MAX_OBJECT_BYTES=536870912
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

> Port 8087 chosen to avoid the 8086 (upload-service) / 8085 (portal-service) host clashes noted in `upload-service/deploy/docker-compose.yml`. Verify no other compose file binds 8087 before committing; bump if taken.

Create `client-update-service/deploy/azure-pipelines.yml`:

```yaml
trigger:
  branches:
    include:
      - main
      - develop
  paths:
    include:
      - client-update-service/
      - pkg/

pr:
  branches:
    include:
      - main
  paths:
    include:
      - client-update-service/
      - pkg/

variables:
  GO_VERSION: '1.25.10'
  SERVICE_NAME: client-update-service
  REGISTRY: '$(containerRegistry)'

stages:
  - stage: Validate
    displayName: 'Lint & Test'
    jobs:
      - job: LintAndTest
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: GoTool@0
            inputs:
              version: '$(GO_VERSION)'
            displayName: 'Install Go $(GO_VERSION)'

          - script: go vet ./$(SERVICE_NAME)/... ./pkg/...
            displayName: 'Go Vet'

          - script: go test ./pkg/... -v -race -coverprofile=coverage-pkg.out
            displayName: 'Test shared packages'

          - script: go test ./$(SERVICE_NAME)/... -v -race -coverprofile=coverage-$(SERVICE_NAME).out
            displayName: 'Test $(SERVICE_NAME)'

          - script: go build -o /dev/null ./$(SERVICE_NAME)/
            displayName: 'Build $(SERVICE_NAME)'

  - stage: Build
    displayName: 'Build & Push Image'
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - job: BuildImage
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: Docker@2
            inputs:
              containerRegistry: '$(containerRegistry)'
              repository: 'chat/$(SERVICE_NAME)'
              command: 'buildAndPush'
              Dockerfile: '$(SERVICE_NAME)/deploy/Dockerfile'
              buildContext: '.'
              tags: |
                $(Build.BuildId)
                latest
            displayName: 'Build & push $(SERVICE_NAME)'
```

- [ ] **Step 6: Lint + commit**

```bash
make lint
git add client-update-service/config.go client-update-service/main.go \
  client-update-service/config_test.go client-update-service/deploy/
git commit -m "feat(client-update-service): main wiring + deploy files"
```

---

### Task 5: Integration test (real MinIO)

**Files:**
- Create: `client-update-service/integration_test.go`

**Interfaces:**
- Consumes: `testutil.RunTests`, `testutil.MinIO`, `newMinioVersionStore`, `ensureBucket`, `NewHandler`, `newBlobCache`, `registerRoutes`.

- [ ] **Step 1: Write the integration test**

Create `client-update-service/integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// countingStore wraps a versionStore to count Open calls (proves cache reuse).
type countingStore struct {
	versionStore
	opens int
}

func (c *countingStore) Open(ctx context.Context, key string) (io.ReadCloser, blobInfo, error) {
	c.opens++
	return c.versionStore.Open(ctx, key)
}

func TestIntegration_StoreRoundTrip(t *testing.T) {
	client, bucket := testutil.MinIO(t, "clientupdate")
	store := newMinioVersionStore(client, bucket, 30*time.Second)
	ctx := context.Background()

	require.NoError(t, store.Put(ctx, objectKey("app.yaml"), bytesReader("version: 1"), 10, "application/x-yaml"))

	rc, info, err := store.Open(ctx, objectKey("app.yaml"))
	require.NoError(t, err)
	defer rc.Close()
	body, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "version: 1", string(body))
	assert.Equal(t, int64(10), info.Size)
	assert.Equal(t, "application/x-yaml", info.ContentType)
}

func TestIntegration_OpenMissing_NotFound(t *testing.T) {
	client, bucket := testutil.MinIO(t, "clientupdate")
	store := newMinioVersionStore(client, bucket, 30*time.Second)
	_, _, err := store.Open(context.Background(), objectKey("nope.exe"))
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

func TestIntegration_EnsureBucketCreatesAbsent(t *testing.T) {
	client, _ := testutil.MinIO(t, "clientupdate")
	name := "cus-fresh-bucket-" + testutil.RandSuffix() // unique per run
	ctx := context.Background()

	require.NoError(t, ensureBucket(ctx, client, name))
	exists, err := client.BucketExists(ctx, name)
	require.NoError(t, err)
	assert.True(t, exists)
	// Idempotent second call.
	require.NoError(t, ensureBucket(ctx, client, name))
	_ = client.RemoveBucket(ctx, name)
}

func TestIntegration_DownloadServesFromCacheOnSecondHit(t *testing.T) {
	client, bucket := testutil.MinIO(t, "clientupdate")
	base := newMinioVersionStore(client, bucket, 30*time.Second)
	require.NoError(t, base.Put(context.Background(), objectKey("app.exe"), bytesReader("BINARY"), 6, "application/octet-stream"))

	cs := &countingStore{versionStore: base}
	h := NewHandler(cs, newBlobCache(4, time.Hour, 1024))
	r := gin.New()
	registerRoutes(r, h)

	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/version/app.exe", nil))
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "BINARY", w.Body.String())
	}
	assert.Equal(t, 1, cs.opens, "second download must be served from cache, not re-opened")
}
```

Add to `integration_test.go` a small reader helper (or reuse one; keep it local):

```go
func bytesReader(s string) io.Reader { return io.NopCloser(stringsNewReader(s)) }
```

> Implementation note: replace `stringsNewReader` with `strings.NewReader` and import `strings`; and if `testutil.RandSuffix` does not exist, generate a unique name with `idgen.GenerateID()` from `pkg/idgen` instead (verify which helper the repo exposes when writing the file). `minio.MakeBucketOptions` import stays via the `minio` package already imported.

- [ ] **Step 2: Run the integration test**

Run: `make test-integration SERVICE=client-update-service`
Expected: PASS (Docker required; MinIO container via `testutil.MinIO`).

- [ ] **Step 3: Commit**

```bash
git add client-update-service/integration_test.go
git commit -m "test(client-update-service): MinIO integration round-trip + cache reuse"
```

---

### Task 6: Client API docs

**Files:**
- Modify: `docs/client-api.md` (add `## 12. Client Update Service` after §11 tcard-service; add TOC entry)
- Modify: `docs/client-api/request-reply.md` (add an `HTTP — Client Update Service` block near the other `HTTP —` entries; add TOC entry)

**Interfaces:**
- Consumes: the wire behavior from Tasks 3–4 (routes, fields, status codes).
- Produces: documentation only.

- [ ] **Step 1: Add §12 to `docs/client-api.md`**

At the end of the file (after the `### PUT /api/v1/emoji/:shortcode` block that closes §11's neighbourhood — i.e. after the last section, which is §11 tcard-service), append:

```markdown
## 12. Client Update Service

Public HTTP endpoints served by `client-update-service`. Distributes client
software-update artifacts (a `.yaml` descriptor + an executable) stored in MinIO.
Uploads and downloads stream end-to-end; downloads are fronted by a bounded
TTL+size in-memory cache.

> [!WARNING]
> **These endpoints are UNAUTHENTICATED in v1.** Anyone who can reach the service
> can upload or download update artifacts. **They MUST be network-restricted
> before any production exposure.**

### POST /api/v1/version

**Auth:** none (v1)

Uploads an update-artifact pair as `multipart/form-data`. Both parts are required
and streamed straight to MinIO (no size cap). An upload of an existing file name
overwrites it and evicts any cached copy.

#### Request

| Part | Type | Required | Notes |
|---|---|---|---|
| `configFile` | file (`.yaml`/`.yml`) | yes | Update descriptor. Stored as `application/x-yaml`. Rejected if empty or not `.yaml`/`.yml`. |
| `executeFile` | file (binary) | yes | The executable. Stored as `application/octet-stream`. Rejected if empty. |

#### Response

| Status | Condition |
|---|---|
| `200 OK` | Both files stored. |
| `400 Bad Request` | Missing/empty `configFile` or `executeFile`; `configFile` not `.yaml`/`.yml`; malformed multipart body. |
| `500 Internal Server Error` | MinIO write failure. |

##### Success response (`200`)

| Field | Type | Notes |
|---|---|---|
| `result` | string | Always `"success"`. |

```json
{ "result": "success" }
```

### GET /api/v1/version/:fileName

**Auth:** none (v1)

Downloads an artifact by file name. Served from an in-memory cache when present
(TTL `CACHE_TTL`, default 24h); on a miss the object is fetched from MinIO and
cached if it is within `CACHE_MAX_OBJECT_BYTES` (default 512 MiB), otherwise
streamed uncached. A re-upload of the same name busts the cache.

#### Response

| Status | Condition | Notes |
|---|---|---|
| `200 OK` | Artifact found | Streams the bytes. `Content-Type` as stored; `Content-Disposition: attachment; filename="<fileName>"`; `Content-Length` set. |
| `400 Bad Request` | Empty or path-unsafe `fileName` (contains `/`, `\`, or `..`) | |
| `404 Not Found` | No artifact with that name | |
| `500 Internal Server Error` | MinIO read failure | |

```
GET /api/v1/version/app.yaml   → 200 (application/x-yaml)
GET /api/v1/version/app.exe    → 200 (application/octet-stream)
```

### GET /healthz

**Auth:** none

Liveness probe. Always `200 {"status":"ok"}`.
```

Then add a Table-of-contents line for §12 in the `## Table of contents` block (mirroring the `- [11. tcard-service](#11-tcard-service)` style):

```markdown
- [12. Client Update Service](#12-client-update-service)
```

- [ ] **Step 2: Add the request-reply.md block**

In `docs/client-api/request-reply.md`, after the `### Media Service — emoji endpoints` block (the last `HTTP —` family entry before the `## room-service` section), insert:

```markdown
### HTTP — Client Update Service

Public HTTP endpoints served by `client-update-service` (no `ssoToken`/auth in v1
— must be network-restricted). Full request/response schemas and the download
cache behavior are in
[../client-api.md §12](../client-api.md#12-client-update-service).

| Endpoint | Reply | Purpose |
|---|---|---|
| `POST /api/v1/version` | synchronous HTTP | Upload a `configFile` (.yaml/.yml) + `executeFile` pair (multipart, streamed to MinIO, no size cap). |
| `GET /api/v1/version/:fileName` | synchronous HTTP (raw bytes) | Download an artifact by name; served from a bounded TTL+size cache, else streamed from MinIO. |
| `GET /healthz` | synchronous HTTP | Liveness (`{"status":"ok"}`). |

**Emits:** `None — HTTP-only.`

---
```

Add a matching TOC entry in `request-reply.md`'s table of contents alongside the other `HTTP —` entries:

```markdown
   - [HTTP — Client Update Service](#http--client-update-service)
```

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs(client-api): client-update-service endpoints (§12 + request-reply)"
```

---

### Task 7: Final verification & push

**Files:** none (verification only).

- [ ] **Step 1: Full local gate**

```bash
make generate SERVICE=client-update-service   # mocks current
make lint
make test SERVICE=client-update-service       # unit, -race
make test-integration SERVICE=client-update-service   # Docker
make sast
```

- [ ] **Step 2: Confirm coverage ≥ 80%**

Run: `go test ./client-update-service/ -coverprofile=/tmp/cov.out && go tool cover -func=/tmp/cov.out | tail -1`
Expected: total ≥ 80% (target ≥90% on `cache.go`/`version.go`/`store_minio.go`). If short, add cases for the uncovered branch and re-run.

- [ ] **Step 3: Push the branch**

```bash
git push -u origin claude/client-update-service-a1orrp
```
Retry with exponential backoff (2s/4s/8s/16s) on network error only. Do **not** open a PR unless the user asks.

---

## Self-Review

**Spec coverage:**
- Streaming upload (multipart → PutObject) → Task 3 (`HandleUpload`/`storeFormFile`). ✓
- Streaming download + open-once decision → Task 3 (`HandleDownload`/`loadObject`/`streamObject`). ✓
- Bounded TTL+size cache + singleflight → Task 1. ✓
- Cache eviction on overwrite → Task 3 (`storeFormFile` → `cache.remove`) + test. ✓
- Store interface `Put`+`Open` (no `Stat`) → Task 2. ✓
- MinIO impl + `cancelReadCloser` + `NoSuchKey`→`ErrObjectNotFound` → Task 2. ✓
- `ensureBucket` create-if-absent (service-local, race-safe) → Task 2. ✓
- Object key `chat.go/chat-versions/<fileName>` → Task 3 (`objectKey`). ✓
- Content types (`application/x-yaml` / `application/octet-stream`) → Task 3. ✓
- `fileName` traversal validation → Task 3 (`validFileName`) + test. ✓
- Config (all env, defaults incl. `CACHE_TTL=24h`, no upload cap) → Task 4 + `config_test`. ✓
- `/healthz` only → Task 3 (`registerRoutes`). ✓
- errcode + errhttp boundary; slog JSON; request-ID/access-log → Tasks 3–4. ✓
- Graceful shutdown (`shutdown.Wait`, server→obs) → Task 4. ✓
- Deploy (Dockerfile/compose/azure) → Task 4. ✓
- Integration (round-trip, missing→404, ensureBucket, cache reuse) → Task 5. ✓
- Docs (`client-api.md` §12 + `request-reply.md`) → Task 6. ✓
- No new deps → uses only go.mod-present libs. ✓

**Placeholder scan:** Two intentional "verify when writing the file" notes exist in Task 5 (`testutil.RandSuffix` vs `idgen.GenerateID`, and the `bytesReader` helper) — these are flagged because the exact `testutil` helper name must be confirmed against the repo at implementation time; both include the concrete fallback (`idgen.GenerateID()`, `strings.NewReader`). No other TBD/TODO.

**Type consistency:**
- `versionStore` = `Put(ctx,key,io.Reader,int64,string) error` + `Open(ctx,key) (io.ReadCloser, blobInfo, error)` — identical in Task 2 (def), Task 3 (mock use), Task 5 (wrap). ✓
- `blobCache`: `get/add/remove/loadCacheable` + field `maxObjectBytes` — defined Task 1, used Task 3 (`h.cache.maxObjectBytes`, `loadCacheable`). ✓
- `cachedBlob{body,contentType}` — consistent Tasks 1/3. ✓
- `NewHandler(versionStore, *blobCache)` — Task 3 def, used Tasks 3/5 tests + Task 4 main. ✓
- `ensureBucket(ctx, bucketClient, string)` / `bucketClient{BucketExists,MakeBucket}` — Task 2 def, Task 4 use (`minioClient.(bucketClient)`). ✓
- `objectKey`, `validFileName`, `isNotFound`, `newMinioVersionStore`, `newBlobCache` signatures match across tasks. ✓
```
