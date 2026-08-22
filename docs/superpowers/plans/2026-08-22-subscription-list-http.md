# subscription.list over HTTP — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose `subscription.list` as `GET /api/v1/subscriptions` on user-service so the desktop client fetches its whole sidebar in one request instead of paging around the 128 KB NATS payload ceiling.

**Architecture:** user-service gains a Gin listener beside its existing NATS router in the same process. Both transports call one transport-neutral service core, so there is no duplicated business logic. The room-enrichment fan-out is chunked so large pages do not silently lose data, and the HTTP path gets its own Mongo client, concurrency limiter, and gzip so it cannot degrade the NATS path.

**Tech Stack:** Go 1.25, Gin, `pkg/natsrouter`, `pkg/errcode`, `pkg/ginutil`, `pkg/mongoutil`, `pkg/oidc`, `pkg/botauth`, `pkg/health`, `klauspost/compress/gzip`, `go.uber.org/mock`, `testify`, testcontainers via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-08-22-subscription-list-http-design.md`

## Global Constraints

- Go 1.25. Single `go.mod` at repo root. Services are flat `package main` dirs.
- **No new third-party dependencies.** `klauspost/compress` is already a direct dependency; use it. Do not add `gin-contrib/gzip`.
- All commands via `make` targets — never raw `go` commands.
- TDD is mandatory: write the failing test, watch it fail, then implement.
- Errors: wrap with context (`fmt.Errorf("doing x: %w", err)`), never bare. Client-facing errors via `pkg/errcode` + `errhttp.Write`. Never log **and** return an error.
- Logging: `log/slog` only, structured key-value pairs, never interpolated. Never log tokens or message bodies.
- Comments: short and neat. Explain WHY, not WHAT. Max 2 lines.
- Coverage floor 80%; target 90% on new middleware and chunking logic.
- `make lint`, `make test`, and `make sast` must pass before every commit.
- Never edit `mock_store_test.go` by hand — regenerate with `make generate`.

### Verified facts this plan depends on

| Fact | Source |
|---|---|
| `history-service` rejects `rooms.get` above 100 room IDs **and** above 100 hints | `history-service/internal/service/rooms.go:18,41,46` |
| `room-service` bounds its reply via `marshalBounded` | `room-service/helper.go:209` |
| `errcode.TooManyRequests` → 429 | `pkg/errcode/category.go:42` |
| `errhttp.Write` writes status + body only, **no headers** | `pkg/errcode/errhttp/write.go:13` |
| `net/http` has no concurrency cap; one goroutine per connection | `net/http/server.go:3491` |
| `json.Encoder.Encode` buffers the whole value, it does not stream | `encoding/json/stream.go:209-233` |
| `mongoutil.ConnectRead` applies `secondaryPreferred` | `pkg/mongoutil/mongo.go:30` |
| Reasons: `missing_fields`, `invalid_sso_token`, `sso_token_expired`, `ambiguous_token`, `upstream_unavailable` | `pkg/errcode/codes_auth.go`, `codes_botplatform.go` |
| Probes belong on `HEALTH_ADDR`, readiness checks NATS only | `docs/health-probes.md` |

---

## File Structure

**Create**
| File | Responsibility |
|---|---|
| `pkg/ginutil/limit.go` | `MaxConcurrency` middleware: non-blocking semaphore, 429 + `Retry-After` |
| `pkg/ginutil/limit_test.go` | Its tests |
| `pkg/ginutil/gzip.go` | `Gzip` middleware: threshold-sniffing, pooled writers |
| `pkg/ginutil/gzip_test.go` | Its tests |
| `pkg/memlimit/memlimit.go` | cgroup-derived `GOMEMLIMIT` |
| `pkg/memlimit/memlimit_test.go` | Its tests |
| `user-service/middleware.go` | Dual-credential auth for the HTTP surface |
| `user-service/middleware_test.go` | Its tests |
| `user-service/handler.go` | Gin handler: bind query → service → JSON |
| `user-service/handler_test.go` | Its tests |
| `user-service/routes.go` | Route registration |
| `user-service/store.go` | `subscriptionLister` consumer interface + mockgen directive |
| `user-service/mock_store_test.go` | Generated (never hand-edited) |
| `user-service/httpserver.go` | `http.Server` construction and tuning |
| `user-service/integration_test.go` | Full HTTP → service → Mongo at `limit=200` |
| `user-service/bench_test.go` | Marshal + gzip benchmarks |

**Modify**
| File | Change |
|---|---|
| `pkg/errcode/codes_user.go` | Add `UserOverloaded` |
| `user-service/service/subscriptions.go` | Transport-neutral core; chunked fan-out |
| `user-service/service/service.go` | `maxFanout` field |
| `user-service/config/config.go` | `HTTPConfig` block + validation |
| `user-service/main.go` | memlimit, second Mongo client, second service, Gin + health servers, shutdown order |
| `user-service/deploy/docker-compose.yml` | Expose ports, new env |
| `docs/client-api.md` | Document the endpoint |
| `docs/health-probes.md` | user-service gets its own row |

---

## Task 1: `ginutil.MaxConcurrency`

**Files:**
- Create: `pkg/ginutil/limit.go`, `pkg/ginutil/limit_test.go`
- Modify: `pkg/errcode/codes_user.go`

**Interfaces:**
- Consumes: `errcode.TooManyRequests`, `errhttp.Write`
- Produces: `ginutil.MaxConcurrency(n int, opts ...LimiterOption) gin.HandlerFunc`, `ginutil.WithRetryAfter(time.Duration) LimiterOption`, `ginutil.WithOnShed(func()) LimiterOption`, `errcode.UserOverloaded`

- [ ] **Step 1: Add the reason constant**

In `pkg/errcode/codes_user.go`, inside the existing `const` block:

```go
	// UserOverloaded: 429 when the HTTP listener is at its in-flight cap.
	UserOverloaded Reason = "overloaded"
```

- [ ] **Step 2: Write the failing tests**

`pkg/ginutil/limit_test.go`:

```go
package ginutil

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLimited builds an engine whose handler blocks until release is closed.
func newLimited(t *testing.T, n int, opts ...LimiterOption) (*gin.Engine, chan struct{}) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	release := make(chan struct{})
	r := gin.New()
	r.Use(MaxConcurrency(n, opts...))
	r.GET("/x", func(c *gin.Context) {
		<-release
		c.Status(http.StatusOK)
	})
	return r, release
}

func TestMaxConcurrency_ShedsBeyondCap(t *testing.T) {
	r, release := newLimited(t, 1, WithRetryAfter(2*time.Second))
	defer close(release)

	started := make(chan struct{})
	go func() {
		w := httptest.NewRecorder()
		close(started)
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	}()
	<-started
	// The in-flight request must actually hold the slot before we probe.
	require.Eventually(t, func() bool {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
		return w.Code == http.StatusTooManyRequests
	}, time.Second, 5*time.Millisecond)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "2", w.Header().Get("Retry-After"))
	assert.JSONEq(t,
		`{"code":"too_many_requests","reason":"overloaded","error":"server is at capacity, retry shortly"}`,
		w.Body.String())
}

func TestMaxConcurrency_ReleasesSlotAfterCompletion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(MaxConcurrency(1))
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
		assert.Equal(t, http.StatusOK, w.Code, "sequential requests must each reclaim the slot")
	}
}

func TestMaxConcurrency_ReleasesSlotOnPanic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery(), MaxConcurrency(1))
	r.GET("/boom", func(c *gin.Context) { panic("boom") })
	r.GET("/ok", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/boom", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ok", nil))
	assert.Equal(t, http.StatusOK, w.Code, "a panicking handler must not leak its slot")
}

func TestMaxConcurrency_ZeroDisablesLimiting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(MaxConcurrency(0))
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
			assert.Equal(t, http.StatusOK, w.Code)
		}()
	}
	wg.Wait()
}

func TestMaxConcurrency_OnShedObserverFires(t *testing.T) {
	var shed atomic.Int64
	r, release := newLimited(t, 1, WithOnShed(func() { shed.Add(1) }))
	defer close(release)

	go func() {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	}()
	require.Eventually(t, func() bool {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
		return w.Code == http.StatusTooManyRequests
	}, time.Second, 5*time.Millisecond)
	assert.Positive(t, shed.Load())
}
```

- [ ] **Step 3: Run to verify failure**

Run: `make test SERVICE=../pkg/ginutil` (or `go test ./pkg/ginutil/` if the target does not accept a pkg path — check the Makefile first).
Expected: FAIL, `undefined: MaxConcurrency`.

- [ ] **Step 4: Implement**

`pkg/ginutil/limit.go`:

```go
package ginutil

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// defaultRetryAfter is the Retry-After advertised on a shed request. One second
// is long enough to clear a burst, short enough that a client init still feels live.
const defaultRetryAfter = time.Second

type limiterConfig struct {
	retryAfter time.Duration
	onShed     func()
}

type LimiterOption func(*limiterConfig)

// WithRetryAfter overrides the Retry-After advertised on a 429.
func WithRetryAfter(d time.Duration) LimiterOption {
	return func(c *limiterConfig) { c.retryAfter = d }
}

// WithOnShed registers a callback invoked once per shed request, for metrics.
// It runs on the request goroutine, so it must not block.
func WithOnShed(f func()) LimiterOption {
	return func(c *limiterConfig) { c.onShed = f }
}

// MaxConcurrency caps in-flight requests at n and sheds the overflow with 429 +
// Retry-After. n <= 0 disables the cap.
//
// Acquire is non-blocking on purpose: a queued request is one whose client has
// usually already given up, so queueing converts a burst into wasted work and
// memory. 429 rather than 503 because service-mesh outlier detection ejects
// hosts on consecutive 5xx, which would shrink the fleet mid-burst.
func MaxConcurrency(n int, opts ...LimiterOption) gin.HandlerFunc {
	if n <= 0 {
		return func(c *gin.Context) { c.Next() }
	}
	cfg := limiterConfig{retryAfter: defaultRetryAfter}
	for _, opt := range opts {
		opt(&cfg)
	}
	sem := make(chan struct{}, n)

	return func(c *gin.Context) {
		select {
		case sem <- struct{}{}:
			// defer, not a trailing release: a panicking handler must not leak the slot.
			defer func() { <-sem }()
			c.Next()
		default:
			if cfg.onShed != nil {
				cfg.onShed()
			}
			// errhttp.Write sets no headers, so Retry-After has to be set here.
			c.Header("Retry-After", strconv.Itoa(int(cfg.retryAfter.Seconds())))
			errhttp.Write(c.Request.Context(), c, errcode.TooManyRequests(
				"server is at capacity, retry shortly",
				errcode.WithReason(errcode.UserOverloaded)))
			c.Abort()
		}
	}
}
```

- [ ] **Step 5: Run to verify pass**

Run: `go test ./pkg/ginutil/ -race -run TestMaxConcurrency -v`
Expected: all PASS.

- [ ] **Step 6: Lint and commit**

```bash
make fmt && make lint
git add pkg/ginutil/limit.go pkg/ginutil/limit_test.go pkg/errcode/codes_user.go
git commit -m "feat(ginutil): concurrency limiter shedding 429 + Retry-After"
```

---

## Task 2: `ginutil.Gzip`

**Files:**
- Create: `pkg/ginutil/gzip.go`, `pkg/ginutil/gzip_test.go`

**Interfaces:**
- Produces: `ginutil.Gzip(minSize int) gin.HandlerFunc`

**Design note for the implementer.** The body size is unknown when headers are
written, so the writer *sniffs*: it buffers until `minSize` bytes have accumulated,
then commits to gzip; if the handler finishes below the threshold the buffer is
flushed uncompressed. Getting this wrong truncates responses, so the tests below
round-trip every case.

- [ ] **Step 1: Write the failing tests**

`pkg/ginutil/gzip_test.go`:

```go
package ginutil

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gzipEngine(t *testing.T, minSize int, body string) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip(minSize))
	r.GET("/x", func(c *gin.Context) {
		c.Header("Content-Type", "application/json")
		_, _ = c.Writer.WriteString(body)
	})
	return r
}

func doGet(r *gin.Engine, acceptGzip bool) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	if acceptGzip {
		req.Header.Set("Accept-Encoding", "gzip")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestGzip_CompressesAboveThreshold(t *testing.T) {
	body := strings.Repeat("a", 4096)
	w := doGet(gzipEngine(t, 1024, body), true)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	assert.Equal(t, "Accept-Encoding", w.Header().Get("Vary"))
	assert.Empty(t, w.Header().Get("Content-Length"), "stale Content-Length would truncate the body")
	assert.Less(t, w.Body.Len(), len(body), "compressed body must be smaller")

	zr, err := gzip.NewReader(bytes.NewReader(w.Body.Bytes()))
	require.NoError(t, err)
	got, err := io.ReadAll(zr)
	require.NoError(t, err)
	assert.Equal(t, body, string(got), "round-trip must be lossless")
}

func TestGzip_PassesThroughBelowThreshold(t *testing.T) {
	body := strings.Repeat("a", 100)
	w := doGet(gzipEngine(t, 1024, body), true)

	assert.Empty(t, w.Header().Get("Content-Encoding"))
	assert.Equal(t, body, w.Body.String())
}

func TestGzip_PassesThroughWithoutAcceptEncoding(t *testing.T) {
	body := strings.Repeat("a", 4096)
	w := doGet(gzipEngine(t, 1024, body), false)

	assert.Empty(t, w.Header().Get("Content-Encoding"))
	assert.Equal(t, body, w.Body.String())
}

func TestGzip_ExactThresholdBoundary(t *testing.T) {
	// Exactly minSize bytes must compress: the check is >=, not >.
	body := strings.Repeat("a", 1024)
	w := doGet(gzipEngine(t, 1024, body), true)
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
}

func TestGzip_ManySmallWritesAccumulateToThreshold(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip(1024))
	r.GET("/x", func(c *gin.Context) {
		for i := 0; i < 200; i++ {
			_, _ = c.Writer.WriteString(strings.Repeat("b", 16))
		}
	})
	w := doGet(r, true)

	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	zr, err := gzip.NewReader(bytes.NewReader(w.Body.Bytes()))
	require.NoError(t, err)
	got, err := io.ReadAll(zr)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("b", 16*200), string(got))
}

func TestGzip_PreservesStatusCode(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip(1024))
	r.GET("/x", func(c *gin.Context) {
		c.String(http.StatusTeapot, strings.Repeat("c", 4096))
	})
	w := doGet(r, true)

	assert.Equal(t, http.StatusTeapot, w.Code)
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
}

func TestGzip_EmptyBodyIsSafe(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Gzip(1024))
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	w := doGet(r, true)

	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Empty(t, w.Body.String())
	assert.Empty(t, w.Header().Get("Content-Encoding"))
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./pkg/ginutil/ -run TestGzip -v`
Expected: FAIL, `undefined: Gzip`.

- [ ] **Step 3: Implement**

`pkg/ginutil/gzip.go`:

```go
// Gzip response compression for the JSON read endpoints. Uses klauspost/compress
// (already a direct dependency) rather than adding gin-contrib/gzip.
package ginutil

import (
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/gzip"
)

var gzipPool = sync.Pool{
	New: func() any {
		w, _ := gzip.NewWriterLevel(nil, gzip.DefaultCompression)
		return w
	},
}

// Gzip compresses responses of at least minSize bytes for clients that accept it.
// Smaller bodies pass through untouched — compressing them costs CPU and usually
// grows the payload.
//
// Body length is unknown when headers are written, so the writer buffers until
// the threshold is crossed and only then commits to gzip.
func Gzip(minSize int) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !acceptsGzip(c.Request.Header.Get("Accept-Encoding")) {
			c.Next()
			return
		}
		// Vary is set even when this response ends up uncompressed: a shared cache
		// must key on Accept-Encoding either way.
		c.Header("Vary", "Accept-Encoding")

		gw := &gzipResponseWriter{ResponseWriter: c.Writer, minSize: minSize}
		c.Writer = gw
		defer func() {
			gw.close()
			c.Writer = gw.ResponseWriter
		}()
		c.Next()
	}
}

func acceptsGzip(header string) bool {
	for _, enc := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(strings.SplitN(enc, ";", 2)[0]), "gzip") {
			return true
		}
	}
	return false
}

// gzipResponseWriter buffers the first minSize bytes, then either switches to
// gzip or, at close, flushes the buffer uncompressed.
type gzipResponseWriter struct {
	gin.ResponseWriter
	minSize int
	buf     []byte
	gz      *gzip.Writer
	status  int
	closed  bool
}

func (w *gzipResponseWriter) WriteHeader(status int) {
	// Held back: the encoding headers are not decided until the threshold check.
	w.status = status
}

func (w *gzipResponseWriter) WriteHeaderNow() {}

func (w *gzipResponseWriter) Write(p []byte) (int, error) {
	if w.gz != nil {
		return w.gz.Write(p)
	}
	w.buf = append(w.buf, p...)
	if len(w.buf) < w.minSize {
		return len(p), nil
	}
	if err := w.startGzip(); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *gzipResponseWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

// startGzip commits to compression and drains the sniff buffer into the encoder.
func (w *gzipResponseWriter) startGzip() error {
	h := w.ResponseWriter.Header()
	h.Set("Content-Encoding", "gzip")
	// A Content-Length measured on the uncompressed body would truncate the response.
	h.Del("Content-Length")
	w.flushStatus()

	w.gz = gzipPool.Get().(*gzip.Writer)
	w.gz.Reset(w.ResponseWriter)
	buf := w.buf
	w.buf = nil
	_, err := w.gz.Write(buf)
	return err
}

func (w *gzipResponseWriter) flushStatus() {
	if w.status == 0 {
		w.status = 200
	}
	w.ResponseWriter.WriteHeader(w.status)
}

// close finalizes the response: flush the encoder, or emit the buffered plain body.
func (w *gzipResponseWriter) close() {
	if w.closed {
		return
	}
	w.closed = true

	if w.gz != nil {
		//nolint:errcheck // the connection is already going away; nothing to recover.
		_ = w.gz.Close()
		w.gz.Reset(nil)
		gzipPool.Put(w.gz)
		w.gz = nil
		return
	}
	if w.status != 0 {
		w.ResponseWriter.WriteHeader(w.status)
	}
	if len(w.buf) > 0 {
		//nolint:errcheck // same: a failed write here has no recovery path.
		_, _ = w.ResponseWriter.Write(w.buf)
		w.buf = nil
	}
}

func (w *gzipResponseWriter) Flush() {
	if w.gz != nil {
		//nolint:errcheck // best-effort flush.
		_ = w.gz.Flush()
	}
	w.ResponseWriter.Flush()
}

func (w *gzipResponseWriter) Size() int { return w.ResponseWriter.Size() }
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./pkg/ginutil/ -race -run TestGzip -v`
Expected: all PASS. If `Size()`/`Written()` assertions in gin trip, satisfy the rest of the `gin.ResponseWriter` interface by delegation — do not stub methods with wrong semantics.

- [ ] **Step 5: Lint and commit**

```bash
make fmt && make lint
git add pkg/ginutil/gzip.go pkg/ginutil/gzip_test.go
git commit -m "feat(ginutil): threshold-sniffing gzip middleware"
```

---

## Task 3: `pkg/memlimit`

**Files:**
- Create: `pkg/memlimit/memlimit.go`, `pkg/memlimit/memlimit_test.go`

**Interfaces:**
- Produces: `memlimit.SetFromCgroup(fraction float64) (int64, bool, error)`

- [ ] **Step 1: Write the failing tests**

`pkg/memlimit/memlimit_test.go`:

```go
package memlimit

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func TestSetFromFiles_CgroupV2(t *testing.T) {
	dir := t.TempDir()
	v2 := writeFile(t, dir, "memory.max", "1073741824\n") // 1 GiB

	var got int64
	limit, applied, err := setFromFiles(v2, filepath.Join(dir, "absent"), 0.8, "",
		func(n int64) int64 { got = n; return math.MaxInt64 })

	require.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, int64(858993459), limit)
	assert.Equal(t, limit, got)
}

func TestSetFromFiles_CgroupV2Unlimited(t *testing.T) {
	dir := t.TempDir()
	v2 := writeFile(t, dir, "memory.max", "max\n")

	_, applied, err := setFromFiles(v2, filepath.Join(dir, "absent"), 0.8, "",
		func(int64) int64 { t.Fatal("must not set a limit when the cgroup is unlimited"); return 0 })

	require.NoError(t, err)
	assert.False(t, applied)
}

func TestSetFromFiles_FallsBackToCgroupV1(t *testing.T) {
	dir := t.TempDir()
	v1 := writeFile(t, dir, "memory.limit_in_bytes", "2147483648\n") // 2 GiB

	limit, applied, err := setFromFiles(filepath.Join(dir, "absent"), v1, 0.5, "",
		func(int64) int64 { return math.MaxInt64 })

	require.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, int64(1073741824), limit)
}

func TestSetFromFiles_V1SentinelIsUnlimited(t *testing.T) {
	dir := t.TempDir()
	v1 := writeFile(t, dir, "memory.limit_in_bytes", "9223372036854771712\n")

	_, applied, err := setFromFiles(filepath.Join(dir, "absent"), v1, 0.8, "",
		func(int64) int64 { t.Fatal("sentinel means unlimited"); return 0 })

	require.NoError(t, err)
	assert.False(t, applied)
}

func TestSetFromFiles_EnvOverrideWins(t *testing.T) {
	dir := t.TempDir()
	v2 := writeFile(t, dir, "memory.max", "1073741824\n")

	_, applied, err := setFromFiles(v2, "", 0.8, "500MiB",
		func(int64) int64 { t.Fatal("an explicit GOMEMLIMIT must not be overridden"); return 0 })

	require.NoError(t, err)
	assert.False(t, applied)
}

func TestSetFromFiles_NoCgroupFilesIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	_, applied, err := setFromFiles(filepath.Join(dir, "a"), filepath.Join(dir, "b"), 0.8, "",
		func(int64) int64 { return math.MaxInt64 })

	require.NoError(t, err, "a non-containerized host must start normally")
	assert.False(t, applied)
}

func TestSetFromFiles_RejectsBadFraction(t *testing.T) {
	for _, f := range []float64{0, -1, 1.5} {
		_, applied, err := setFromFiles("", "", f, "", func(int64) int64 { return 0 })
		require.Error(t, err, "fraction %v must be rejected", f)
		assert.False(t, applied)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./pkg/memlimit/ -v`
Expected: FAIL, `undefined: setFromFiles`.

- [ ] **Step 3: Implement**

`pkg/memlimit/memlimit.go`:

```go
// Package memlimit derives Go's soft memory limit from the container's cgroup
// quota, so a burst degrades into GC pressure instead of an OOMKill.
package memlimit

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	cgroupV2Path = "/sys/fs/cgroup/memory.max"
	cgroupV1Path = "/sys/fs/cgroup/memory/memory.limit_in_bytes"

	// v1 reports "no limit" as a near-maxint sentinel rather than a keyword.
	v1UnlimitedFloor = int64(1) << 62
)

// SetFromCgroup sets GOMEMLIMIT to fraction of the container's memory limit.
// Returns the limit applied and whether one was applied at all: an explicit
// GOMEMLIMIT, an unlimited cgroup, or a non-containerized host are all no-ops,
// not errors.
func SetFromCgroup(fraction float64) (int64, bool, error) {
	return setFromFiles(cgroupV2Path, cgroupV1Path, fraction, os.Getenv("GOMEMLIMIT"), debug.SetMemoryLimit)
}

func setFromFiles(v2Path, v1Path string, fraction float64, envLimit string, set func(int64) int64) (int64, bool, error) {
	if fraction <= 0 || fraction > 1 {
		return 0, false, fmt.Errorf("memory limit fraction must be in (0,1], got %v", fraction)
	}
	// An operator-set GOMEMLIMIT is deliberate; never second-guess it.
	if envLimit != "" {
		return 0, false, nil
	}

	quota, err := readQuota(v2Path, v1Path)
	if err != nil {
		return 0, false, err
	}
	if quota <= 0 {
		return 0, false, nil
	}

	limit := int64(float64(quota) * fraction)
	if limit <= 0 {
		return 0, false, nil
	}
	set(limit)
	return limit, true, nil
}

// readQuota returns the cgroup memory quota in bytes, or 0 when unlimited or absent.
func readQuota(v2Path, v1Path string) (int64, error) {
	v, err := readLimitFile(v2Path)
	if err != nil {
		return 0, err
	}
	if v != 0 {
		return v, nil
	}
	return readLimitFile(v1Path)
}

func readLimitFile(path string) (int64, error) {
	if path == "" {
		return 0, nil
	}
	b, err := os.ReadFile(path) // #nosec G304 -- paths are package constants, not caller input
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("read cgroup memory limit %s: %w", path, err)
	}
	s := strings.TrimSpace(string(b))
	if s == "max" || s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse cgroup memory limit %s: %w", path, err)
	}
	if n <= 0 || n >= v1UnlimitedFloor || n == math.MaxInt64 {
		return 0, nil
	}
	return n, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./pkg/memlimit/ -race -v`
Expected: all PASS.

- [ ] **Step 5: Lint and commit**

```bash
make fmt && make lint
git add pkg/memlimit/
git commit -m "feat(memlimit): derive GOMEMLIMIT from the cgroup quota"
```

---

## Task 4: Transport-neutral service core

**Files:**
- Modify: `user-service/service/subscriptions.go`, `user-service/service/service.go`
- Test: `user-service/service/subscriptions_test.go` (existing, adjust call sites)

**Interfaces:**
- Produces: `(*UserService).ListSubscriptionsFor(ctx context.Context, account string, req models.SubscriptionListRequest, defaultLimit, maxLimit int) (*models.PagedSubscriptionListResponse, error)`

**This task must not change behavior.** Existing tests are the safety net: they may change only where a signature forces it.

- [ ] **Step 1: Write the failing test**

Append to `user-service/service/subscriptions_test.go`:

```go
func TestUserService_ListSubscriptionsFor_UsesSuppliedPageBounds(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)

	var gotPage mongoutil.OffsetPageRequest
	subs.EXPECT().
		AggregateSubscriptions(gomock.Any(), "alice", "current", false, nil, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ bool, _ *int, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error) {
			gotPage = page
			return mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{}, nil
		})

	svc := newTestService(t, subs) // existing helper in this package
	// limit omitted ⇒ the caller's default, not the NATS default.
	_, err := svc.ListSubscriptionsFor(context.Background(), "alice",
		models.SubscriptionListRequest{Type: "current"}, 40, 400)

	require.NoError(t, err)
	assert.Equal(t, int64(40), gotPage.Limit)
}

func TestUserService_ListSubscriptionsFor_CapsAtSuppliedMax(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)

	var gotPage mongoutil.OffsetPageRequest
	subs.EXPECT().
		AggregateSubscriptions(gomock.Any(), "alice", "current", false, nil, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ bool, _ *int, page mongoutil.OffsetPageRequest) (mongoutil.OffsetPageHasMore[model.EnrichedSubscription], error) {
			gotPage = page
			return mongoutil.OffsetPageHasMore[model.EnrichedSubscription]{}, nil
		})

	svc := newTestService(t, subs)
	_, err := svc.ListSubscriptionsFor(context.Background(), "alice",
		models.SubscriptionListRequest{Type: "current", Limit: 5000}, 40, 400)

	require.NoError(t, err)
	assert.Equal(t, int64(400), gotPage.Limit, "limit must clamp to the supplied max")
}
```

If `newTestService` does not exist under that name, use whatever constructor the
neighbouring tests in this file already use — do not invent a second one.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=user-service`
Expected: FAIL, `svc.ListSubscriptionsFor undefined`.

- [ ] **Step 3: Implement the split**

In `user-service/service/subscriptions.go`, replace the body of `ListSubscriptions`:

```go
// ListSubscriptions serves the NATS transport; the account comes from the subject.
func (s *UserService) ListSubscriptions(c *natsrouter.Context, req models.SubscriptionListRequest) (*models.PagedSubscriptionListResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	return s.ListSubscriptionsFor(c, account, req, s.defaultLimit, s.maxSubs)
}

// ListSubscriptionsFor is the transport-neutral core. Page bounds are parameters
// because HTTP and NATS have different ceilings.
func (s *UserService) ListSubscriptionsFor(ctx context.Context, account string, req models.SubscriptionListRequest, defaultLimit, maxLimit int) (*models.PagedSubscriptionListResponse, error) {
	if !validListTypes[req.Type] {
		return nil, errcode.BadRequest("unknown subscription type")
	}
	if req.UpdatedWithinDays != nil && *req.UpdatedWithinDays < 0 {
		// A negative window computes a FUTURE cutoff and silently returns empty.
		return nil, errcode.BadRequest("updatedWithinDays must be non-negative")
	}
	page := normalizePage(req.Offset, req.Limit, defaultLimit, maxLimit)
	favorite := req.Favorite != nil && *req.Favorite
	// Favorite filtering and the self-DM pin are applied in the query so the page
	// slice and hasMore stay consistent.
	res, err := s.subs.AggregateSubscriptions(ctx, account, req.Type, favorite, req.UpdatedWithinDays, page)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	withLastMsg := req.IncludeLastMessage == nil || *req.IncludeLastMessage
	res.Data = s.enrichWithRoomInfoAndLastMsg(ctx, account, res.Data, true, withLastMsg)
	return &models.PagedSubscriptionListResponse{
		Subscriptions: s.buildListItems(ctx, account, res.Data),
		HasMore:       res.HasMore,
	}, nil
}
```

- [ ] **Step 4: Rewrite the helper signatures**

Change these from `(c *natsrouter.Context, …)` to `(ctx context.Context, account string, …)`,
replacing every internal `c.Param("account")` with the new `account` parameter and
every `c` used as a context with `ctx`:

`enrichWithRoomInfoAndLastMsg`, `enrichCrossSite`, `enrichLastMessage`,
`buildListItems`, `lookupApps`, `lookupHRInfo`.

`enrichLocal` takes no context today — leave it alone.

Update the three other call sites in the same file (`getChannels` at ~line 490,
`getDM` at ~line 519, `getByRoomID` at ~line 542) to
`s.enrichWithRoomInfoAndLastMsg(c, c.Param("account"), …)`. `*natsrouter.Context`
satisfies `context.Context`, so these compile unchanged otherwise.

Add `"context"` to the import block.

- [ ] **Step 5: Run the full service suite**

Run: `make test SERVICE=user-service`
Expected: PASS. Existing tests must pass with only mechanical signature edits — if
an assertion changes meaning, you have changed behavior; revert and re-do.

- [ ] **Step 6: Commit**

```bash
make fmt && make lint
git add user-service/service/
git commit -m "refactor(user-service): transport-neutral ListSubscriptionsFor core"
```

---

## Task 5: Chunked enrichment fan-out

**Files:**
- Modify: `user-service/service/subscriptions.go`, `user-service/service/service.go`, `user-service/config/config.go`
- Test: `user-service/service/enrich_test.go`

**Interfaces:**
- Produces: `chunkRoomIDs(ids []string, size int) [][]string`; `UserService.roomBatchChunk` and `UserService.maxFanout` fields

**This is a correctness fix.** Above 100 room IDs, `history-service` rejects the
batch outright (`rooms.go:41`) and *also* rejects more than 100 hints (`rooms.go:46`),
so an unchunked 200-row page returns every row with no `previewMessage`.

- [ ] **Step 1: Write the failing tests**

Add to `user-service/service/enrich_test.go`:

```go
func TestChunkRoomIDs(t *testing.T) {
	ids := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("r%d", i)
		}
		return out
	}
	tests := []struct {
		name      string
		n, size   int
		wantSizes []int
	}{
		{"empty", 0, 100, nil},
		{"single", 1, 100, []int{1}},
		{"just under", 99, 100, []int{99}},
		{"exact", 100, 100, []int{100}},
		{"one over", 101, 100, []int{100, 1}},
		{"two and a half", 250, 100, []int{100, 100, 50}},
		{"non-positive size is one chunk", 250, 0, []int{250}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := chunkRoomIDs(ids(tc.n), tc.size)
			require.Len(t, got, len(tc.wantSizes))
			var total int
			for i, c := range got {
				assert.Len(t, c, tc.wantSizes[i])
				total += len(c)
			}
			assert.Equal(t, tc.n, total, "chunking must not drop or duplicate ids")
		})
	}
}

func TestEnrichLastMessage_ChunksBeyondBatchCap(t *testing.T) {
	const rooms = 250
	ctrl := gomock.NewController(t)
	history := mocks.NewMockHistoryClient(ctrl)

	var mu sync.Mutex
	var batchSizes []int
	history.EXPECT().
		RoomsGet(gomock.Any(), "site-a", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, ids []string, hints map[string]model.RoomTimeHint) (map[string]model.PreviewMessage, error) {
			mu.Lock()
			batchSizes = append(batchSizes, len(ids))
			mu.Unlock()
			assert.LessOrEqual(t, len(ids), 100, "history-service rejects batches over 100")
			assert.LessOrEqual(t, len(hints), 100, "history-service rejects more than 100 hints too")
			out := make(map[string]model.PreviewMessage, len(ids))
			for _, id := range ids {
				out[id] = model.PreviewMessage{MessageID: id + "-msg"}
			}
			return out, nil
		}).Times(3)

	svc, subs := newEnrichFixture(t, history, "site-a", rooms)
	svc.enrichLastMessage(context.Background(), "alice", subs, indexBySite(subs), roomIDsBySite(subs))

	sort.Ints(batchSizes)
	assert.Equal(t, []int{50, 100, 100}, batchSizes)
	for i := range subs {
		require.NotNil(t, subs[i].Room, "row %d", i)
		assert.NotNil(t, subs[i].Room.PreviewMessage, "every row must get its preview, row %d", i)
	}
}

func TestEnrichLastMessage_OneFailedChunkDegradesOnlyItsRooms(t *testing.T) {
	const rooms = 250
	ctrl := gomock.NewController(t)
	history := mocks.NewMockHistoryClient(ctrl)

	history.EXPECT().
		RoomsGet(gomock.Any(), "site-a", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, ids []string, _ map[string]model.RoomTimeHint) (map[string]model.PreviewMessage, error) {
			if len(ids) == 50 {
				return nil, errors.New("boom")
			}
			out := make(map[string]model.PreviewMessage, len(ids))
			for _, id := range ids {
				out[id] = model.PreviewMessage{MessageID: id + "-msg"}
			}
			return out, nil
		}).Times(3)

	svc, subs := newEnrichFixture(t, history, "site-a", rooms)
	svc.enrichLastMessage(context.Background(), "alice", subs, indexBySite(subs), roomIDsBySite(subs))

	var withPreview int
	for i := range subs {
		if subs[i].Room != nil && subs[i].Room.PreviewMessage != nil {
			withPreview++
		}
	}
	assert.Equal(t, 200, withPreview, "a failed chunk must cost only its own 50 rooms, not the whole site")
}
```

Write `newEnrichFixture`, `indexBySite`, and `roomIDsBySite` as local `_test.go`
helpers building `[]model.EnrichedSubscription` with `SiteID: site`, sequential
`RoomID`s, and a non-nil `Room` carrying a `LastMsgAt`, mirroring the fixtures the
existing tests in this file already build.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=user-service`
Expected: FAIL — `undefined: chunkRoomIDs`, and the fan-out test fails with one
250-id call instead of three.

- [ ] **Step 3: Implement `chunkRoomIDs`**

```go
// chunkRoomIDs splits ids into batches of at most size. size <= 0 yields a single
// chunk — a misconfiguration should not silently disable batching.
func chunkRoomIDs(ids []string, size int) [][]string {
	if len(ids) == 0 {
		return nil
	}
	if size <= 0 || len(ids) <= size {
		return [][]string{ids}
	}
	chunks := make([][]string, 0, (len(ids)+size-1)/size)
	for start := 0; start < len(ids); start += size {
		end := min(start+size, len(ids))
		chunks = append(chunks, ids[start:end])
	}
	return chunks
}
```

- [ ] **Step 4: Chunk `enrichLastMessage`**

Replace the per-site goroutine body so each site fans out over its chunks, each
chunk carrying only its own hints. Merge results under a mutex; leave the site's
map nil only if every chunk failed, so the existing apply loop still means
"whole site degraded".

```go
	lastMsgBySite := make([]map[string]model.PreviewMessage, len(sites))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, s.maxFanout)

	for i, site := range sites {
		for _, chunk := range chunkRoomIDs(roomIDsBySite[site], s.roomBatchChunk) {
			if ctx.Err() != nil {
				break
			}
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if ctx.Err() != nil {
					return
				}
				m, err := s.history.RoomsGet(ctx, site, chunk, chunkHints(subs, idxBySite[site], chunk))
				if err != nil {
					slog.WarnContext(ctx, "last-message enrichment degraded",
						"account", account, "site", site, "chunk_size", len(chunk),
						"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
					return
				}
				mu.Lock()
				if lastMsgBySite[i] == nil {
					lastMsgBySite[i] = make(map[string]model.PreviewMessage, len(m))
				}
				for k, v := range m {
					lastMsgBySite[i][k] = v
				}
				mu.Unlock()
			}()
		}
	}
	wg.Wait()
```

And the hint builder, scoped to the chunk so the 100-hint cap is respected:

```go
// chunkHints returns the caller-known walk bounds for just this chunk's rooms;
// history-service caps hints at the same 100 as room ids.
func chunkHints(subs []model.EnrichedSubscription, siteIdx []int, chunk []string) map[string]model.RoomTimeHint {
	want := make(map[string]struct{}, len(chunk))
	for _, id := range chunk {
		want[id] = struct{}{}
	}
	hints := make(map[string]model.RoomTimeHint, len(chunk))
	for _, j := range siteIdx {
		if _, ok := want[subs[j].RoomID]; !ok {
			continue
		}
		if subs[j].Room == nil || subs[j].Room.LastMsgAt == nil {
			continue
		}
		hints[subs[j].RoomID] = model.RoomTimeHint{LastMsgAt: timeutil.TimeToMillis(subs[j].Room.LastMsgAt)}
	}
	return hints
}
```

- [ ] **Step 5: Chunk `enrichCrossSite` the same way**

Same structure: iterate `chunkRoomIDs(roomIDsBySite[site], s.roomBatchChunk)`,
call `s.rooms.GetRoomsInfo(ctx, site, chunk)`, merge into `infoBySite[i]` under the
mutex keyed by `info.RoomID`. Keep the existing "collect `Del-` rows to drop" logic
in the post-`wg.Wait()` apply loop, unchanged.

- [ ] **Step 6: Add the config fields**

`user-service/config/config.go`:

```go
	// RoomBatchChunk caps room ids per enrichment RPC. 100 is history-service's
	// hard batch cap and keeps each reply well under the 128 KB NATS payload.
	RoomBatchChunk int `env:"ROOM_BATCH_CHUNK" envDefault:"100"`
	// MaxSiteFanout bounds concurrent enrichment RPCs across all sites and chunks.
	MaxSiteFanout int `env:"MAX_SITE_FANOUT" envDefault:"8"`
```

Validate in `Load()`:

```go
	if cfg.RoomBatchChunk < 1 || cfg.RoomBatchChunk > 100 {
		return Config{}, fmt.Errorf("ROOM_BATCH_CHUNK must be in [1,100], got %d", cfg.RoomBatchChunk)
	}
	if cfg.MaxSiteFanout < 1 {
		return Config{}, fmt.Errorf("MAX_SITE_FANOUT must be >= 1, got %d", cfg.MaxSiteFanout)
	}
```

In `service.New`, set `roomBatchChunk: cfg.RoomBatchChunk` and `maxFanout: cfg.MaxSiteFanout`,
adding both to the `UserService` struct. Delete the now-unused `maxSiteFanout` constant.

- [ ] **Step 7: Run to verify pass**

Run: `make test SERVICE=user-service`
Expected: all PASS, including the pre-existing enrichment tests.

- [ ] **Step 8: Commit**

```bash
make fmt && make lint
git add user-service/service/ user-service/config/
git commit -m "fix(user-service): chunk room enrichment so large pages keep previews"
```

---

## Task 6: HTTP configuration block

**Files:**
- Modify: `user-service/config/config.go`
- Test: `user-service/config/config_test.go`

**Interfaces:**
- Produces: `config.HTTPConfig`; `Config.HTTP`; `Config.HealthAddr`; `Config.BotplatformURL`; `Config.GoMemLimitFraction`

- [ ] **Step 1: Write the failing tests**

Add to `user-service/config/config_test.go`, following the table style already there:

```go
func TestLoad_HTTPDefaults(t *testing.T) {
	setRequiredEnv(t) // existing helper
	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "8080", cfg.HTTP.Port)
	assert.Equal(t, 256, cfg.HTTP.MaxConcurrency)
	assert.Equal(t, 30*time.Second, cfg.HTTP.HandlerTimeout)
	assert.Equal(t, 35*time.Second, cfg.HTTP.WriteTimeout)
	assert.Equal(t, 1024, cfg.HTTP.GzipMinBytes)
	assert.Equal(t, uint64(128), cfg.HTTP.MongoMaxPoolSize)
	assert.Equal(t, uint64(16), cfg.HTTP.MongoMinPoolSize)
	assert.Equal(t, 40, cfg.HTTP.DefaultLimit)
	assert.Equal(t, 400, cfg.HTTP.MaxLimit)
	assert.Equal(t, ":8081", cfg.HealthAddr)
	assert.InDelta(t, 0.8, cfg.GoMemLimitFraction, 1e-9)
}

func TestLoad_RejectsInvalidHTTPConfig(t *testing.T) {
	tests := []struct {
		name, env, val, wantMsg string
	}{
		{"default above max", "SUBSCRIPTION_HTTP_DEFAULT_LIMIT", "500", "SUBSCRIPTION_HTTP_DEFAULT_LIMIT"},
		{"write timeout not above handler", "HTTP_WRITE_TIMEOUT", "10s", "HTTP_WRITE_TIMEOUT"},
		{"negative concurrency", "HTTP_MAX_CONCURRENCY", "-1", "HTTP_MAX_CONCURRENCY"},
		{"zero max limit", "SUBSCRIPTION_HTTP_MAX_LIMIT", "0", "SUBSCRIPTION_HTTP_MAX_LIMIT"},
		{"fraction above one", "GOMEMLIMIT_FRACTION", "1.5", "GOMEMLIMIT_FRACTION"},
		{"zero mongo pool", "HTTP_MONGO_MAX_POOL_SIZE", "0", "HTTP_MONGO_MAX_POOL_SIZE"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(tc.env, tc.val)
			_, err := Load()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=user-service`
Expected: FAIL, `cfg.HTTP undefined`.

- [ ] **Step 3: Implement**

```go
// HTTPConfig configures the client-facing HTTP listener (env prefix: HTTP_).
type HTTPConfig struct {
	Port string `env:"PORT" envDefault:"8080"`
	// MaxConcurrency caps in-flight HTTP handlers; overflow is shed with 429.
	// Separate from the NATS router's budget so a client burst cannot starve RPCs.
	MaxConcurrency int           `env:"MAX_CONCURRENCY" envDefault:"256"`
	HandlerTimeout time.Duration `env:"HANDLER_TIMEOUT" envDefault:"30s"`
	// WriteTimeout must exceed HandlerTimeout: net/http starts its clock at header
	// read, so an equal value kills the connection mid-write.
	WriteTimeout     time.Duration `env:"WRITE_TIMEOUT" envDefault:"35s"`
	GzipMinBytes     int           `env:"GZIP_MIN_BYTES" envDefault:"1024"`
	MongoMaxPoolSize uint64        `env:"MONGO_MAX_POOL_SIZE" envDefault:"128"`
	MongoMinPoolSize uint64        `env:"MONGO_MIN_POOL_SIZE" envDefault:"16"`
	DefaultLimit     int           `env:"-"`
	MaxLimit         int           `env:"-"`
}
```

`DefaultLimit`/`MaxLimit` carry the `SUBSCRIPTION_HTTP_*` names, which do not fit
the `HTTP_` prefix, so parse them as top-level fields and copy them in `Load()`:

```go
	HTTPDefaultLimit int     `env:"SUBSCRIPTION_HTTP_DEFAULT_LIMIT" envDefault:"40"`
	HTTPMaxLimit     int     `env:"SUBSCRIPTION_HTTP_MAX_LIMIT"     envDefault:"400"`
	HealthAddr       string  `env:"HEALTH_ADDR" envDefault:":8081"`
	BotplatformURL   string  `env:"BOTPLATFORM_URL" envDefault:""`
	GoMemLimitFraction float64 `env:"GOMEMLIMIT_FRACTION" envDefault:"0.8"`
	HTTP             HTTPConfig `envPrefix:"HTTP_"`
```

At the end of `Load()`, before returning:

```go
	cfg.HTTP.DefaultLimit = cfg.HTTPDefaultLimit
	cfg.HTTP.MaxLimit = cfg.HTTPMaxLimit
```

Validation, in the existing style:

```go
	if cfg.HTTPMaxLimit < 1 {
		return Config{}, fmt.Errorf("SUBSCRIPTION_HTTP_MAX_LIMIT must be >= 1, got %d", cfg.HTTPMaxLimit)
	}
	if cfg.HTTPDefaultLimit < 1 {
		return Config{}, fmt.Errorf("SUBSCRIPTION_HTTP_DEFAULT_LIMIT must be >= 1, got %d", cfg.HTTPDefaultLimit)
	}
	if cfg.HTTPDefaultLimit > cfg.HTTPMaxLimit {
		return Config{}, fmt.Errorf("SUBSCRIPTION_HTTP_DEFAULT_LIMIT (%d) must be <= SUBSCRIPTION_HTTP_MAX_LIMIT (%d)", cfg.HTTPDefaultLimit, cfg.HTTPMaxLimit)
	}
	if cfg.HTTP.MaxConcurrency < 0 {
		return Config{}, fmt.Errorf("HTTP_MAX_CONCURRENCY must be >= 0, got %d", cfg.HTTP.MaxConcurrency)
	}
	if cfg.HTTP.WriteTimeout <= cfg.HTTP.HandlerTimeout {
		return Config{}, fmt.Errorf("HTTP_WRITE_TIMEOUT (%s) must exceed HTTP_HANDLER_TIMEOUT (%s)", cfg.HTTP.WriteTimeout, cfg.HTTP.HandlerTimeout)
	}
	if cfg.HTTP.MongoMaxPoolSize < 1 {
		return Config{}, fmt.Errorf("HTTP_MONGO_MAX_POOL_SIZE must be >= 1, got %d", cfg.HTTP.MongoMaxPoolSize)
	}
	if cfg.HTTP.MongoMinPoolSize > cfg.HTTP.MongoMaxPoolSize {
		return Config{}, fmt.Errorf("HTTP_MONGO_MIN_POOL_SIZE (%d) must be <= HTTP_MONGO_MAX_POOL_SIZE (%d)", cfg.HTTP.MongoMinPoolSize, cfg.HTTP.MongoMaxPoolSize)
	}
	if cfg.GoMemLimitFraction <= 0 || cfg.GoMemLimitFraction > 1 {
		return Config{}, fmt.Errorf("GOMEMLIMIT_FRACTION must be in (0,1], got %v", cfg.GoMemLimitFraction)
	}
```

- [ ] **Step 4: Run to verify pass**

Run: `make test SERVICE=user-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
make fmt && make lint
git add user-service/config/
git commit -m "feat(user-service): HTTP listener configuration"
```

---

## Task 7: Dual-credential auth middleware

**Files:**
- Create: `user-service/middleware.go`, `user-service/middleware_test.go`

**Interfaces:**
- Consumes: `pkg/botauth`, `pkg/oidc`, `pkg/errcode`
- Produces: `ssoValidator` interface, `authDeps` struct, `authMiddleware(authDeps) gin.HandlerFunc`, `accountFromContext(*gin.Context) string`, const `ctxAccountKey`

- [ ] **Step 1: Write the failing tests**

`user-service/middleware_test.go` — table-driven, one subtest per row:

| Case | Headers | Want status | Want reason |
|---|---|---|---|
| valid sso | `ssoToken: good` | 200, account `alice` | — |
| expired sso | `ssoToken: expired` | 401 | `sso_token_expired` |
| invalid sso | `ssoToken: bad` | 401 | `invalid_sso_token` |
| valid session | `x-user-id`, `x-auth-token` | 200, account `bob` | — |
| both credentials | `ssoToken` + `x-auth-token` | 400 | `ambiguous_token` |
| no credential | none | 401 | `missing_fields` |
| session token, botplatform unset | `x-user-id`, `x-auth-token` | 503 | `upstream_unavailable` |

Use hand-written fakes for `ssoValidator` and `botauth.TokenValidator` — both are
one-method interfaces, so mockgen is not worth it here. Assert the account reaches
the handler via `accountFromContext`, and assert that **no test asserts on a token
value appearing in a log or response** (tokens must never be echoed).

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=user-service`
Expected: FAIL, `undefined: authMiddleware`.

- [ ] **Step 3: Implement**

`user-service/middleware.go`, modelled on `upload-service/middleware.go`:

```go
package main

import (
	"context"
	"errors"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/botauth"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
)

// ssoTokenHeader is the SSO credential header, matching upload-service and the
// Drive endpoints the client already calls.
const ssoTokenHeader = "ssoToken"

// ctxAccountKey holds the authenticated account resolved from the credential.
const ctxAccountKey = "auth_account"

// ssoValidator validates an SSO token and returns its OIDC claims.
// Satisfied by *pkg/oidc.Validator.
type ssoValidator interface {
	Validate(ctx context.Context, rawToken string) (pkgoidc.Claims, error)
}

// authDeps are the credential validators; either may be nil when unconfigured.
type authDeps struct {
	sso ssoValidator
	bot botauth.TokenValidator
}

// accountFromContext returns the account authMiddleware resolved, or "".
func accountFromContext(c *gin.Context) string { return c.GetString(ctxAccountKey) }

// authMiddleware resolves an ssoToken or an x-user-id/x-auth-token pair into the
// caller's account. The account never comes from the request path, so a caller
// can only ever read its own data.
func authMiddleware(d authDeps) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

		ssoToken := c.GetHeader(ssoTokenHeader)
		botUserID, botToken := botauth.Credentials(c.Request.Header)

		account, err := d.resolve(ctx, ssoToken, botUserID, botToken)
		if err != nil {
			errhttp.Write(ctx, c, err)
			c.Abort()
			return
		}
		// Carry the enriched ctx forward so the handler logs the same request_id.
		c.Request = c.Request.WithContext(errcode.WithLogValues(ctx, "account", account))
		c.Set(ctxAccountKey, account)
		c.Next()
	}
}

func (d authDeps) resolve(ctx context.Context, ssoToken, botUserID, botToken string) (string, error) {
	if ssoToken != "" && botToken != "" {
		return "", errcode.BadRequest("set exactly one of ssoToken / x-auth-token",
			errcode.WithReason(errcode.BotplatformAmbiguousToken))
	}
	// Half a session credential is still a session attempt: route it here for the
	// uniform 401 rather than the SSO branch's "missing ssoToken".
	if botToken != "" || (botUserID != "" && ssoToken == "") {
		return d.sessionAccount(ctx, botUserID, botToken)
	}
	if ssoToken == "" {
		return "", errcode.Unauthenticated("missing ssoToken",
			errcode.WithReason(errcode.AuthMissingFields))
	}
	return d.ssoAccount(ctx, ssoToken)
}

func (d authDeps) ssoAccount(ctx context.Context, token string) (string, error) {
	if d.sso == nil {
		return "", errcode.Unavailable("sso auth not configured",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable))
	}
	claims, err := d.sso.Validate(ctx, token)
	if err != nil {
		if errors.Is(err, pkgoidc.ErrTokenExpired) {
			return "", errcode.Unauthenticated("sso token has expired, please re-login",
				errcode.WithReason(errcode.AuthTokenExpired))
		}
		// Cause carries the verification error, never the token, to the server log.
		return "", errcode.Unauthenticated("invalid sso token",
			errcode.WithReason(errcode.AuthInvalidToken), errcode.WithCause(err))
	}
	account := claims.PreferredUsername
	if account == "" {
		account = claims.Name
	}
	if account == "" {
		return "", errcode.Unauthenticated("sso token carries no account",
			errcode.WithReason(errcode.AuthInvalidToken))
	}
	return account, nil
}

func (d authDeps) sessionAccount(ctx context.Context, userID, token string) (string, error) {
	if d.bot == nil {
		return "", errcode.Unavailable("session-token auth not configured",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable))
	}
	p, err := botauth.Authenticate(ctx, d.bot, userID, token)
	if err != nil {
		return "", err
	}
	return p.Account, nil
}
```

Check `pkgoidc.Claims` field names against `pkg/oidc` before implementing; use
whatever `upload-service/middleware.go` uses, since it reads the same claims.

- [ ] **Step 4: Run to verify pass**

Run: `make test SERVICE=user-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
make fmt && make lint
git add user-service/middleware.go user-service/middleware_test.go
git commit -m "feat(user-service): dual-credential auth for the HTTP surface"
```

---

## Task 8: HTTP handler and routes

**Files:**
- Create: `user-service/store.go`, `user-service/handler.go`, `user-service/routes.go`, `user-service/handler_test.go`
- Generated: `user-service/mock_store_test.go`

**Interfaces:**
- Consumes: `service.ListSubscriptionsFor` (Task 4), `accountFromContext` (Task 7), `ginutil.MaxConcurrency`/`Gzip` (Tasks 1-2)
- Produces: `subscriptionLister` interface, `handler` struct, `newHandler(subscriptionLister, int, int) *handler`, `(*handler).ListSubscriptions(*gin.Context)`, `registerRoutes(*gin.Engine, *handler, authDeps, httpDeps)`

- [ ] **Step 1: Define the consumer interface**

`user-service/store.go`:

```go
package main

import (
	"context"

	"github.com/hmchangw/chat/user-service/models"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// subscriptionLister is the slice of user-service's service layer the HTTP
// handler needs. Defined here, in the consumer, per the repo convention.
type subscriptionLister interface {
	ListSubscriptionsFor(ctx context.Context, account string, req models.SubscriptionListRequest, defaultLimit, maxLimit int) (*models.PagedSubscriptionListResponse, error)
}
```

Run `make generate SERVICE=user-service`.

- [ ] **Step 2: Write the failing tests**

`user-service/handler_test.go` — table-driven over query strings:

| Query | Expect |
|---|---|
| `?type=current` | 200; `Limit` reaching the service is `0` (service applies the default) |
| `?type=current&limit=200` | 200; `Limit == 200` |
| `?type=current&limit=99999` | 200; service receives `99999`, clamping happens in the service against `maxLimit` |
| `?type=current&favorite=true` | `*Favorite == true` |
| `?type=current&includeLastMessage=false` | `*IncludeLastMessage == false` |
| `?type=current` | `IncludeLastMessage == nil` (omitted ≠ false) |
| `?type=current&offset=-5` | service receives `-5`; normalization is the service's job |
| `?type=bogus` | 400 `bad_request` (service returns it) |
| `?limit=abc` | 400 `bad_request`, binding error |
| `?type=current&updatedWithinDays=-1` | 400 `bad_request` |
| no `type` | 400 `bad_request` |
| service returns `fmt.Errorf` | 500 `internal`, and the raw error text must **not** appear in the body |

Also assert the success body is exactly `{"subscriptions":[...],"hasMore":false}`
and that the handler passes the authenticated account, not any query parameter.

- [ ] **Step 3: Run to verify failure**

Run: `make test SERVICE=user-service`
Expected: FAIL, `undefined: newHandler`.

- [ ] **Step 4: Implement the handler**

`user-service/handler.go`:

```go
package main

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/user-service/models"
)

type handler struct {
	subs         subscriptionLister
	defaultLimit int
	maxLimit     int
}

func newHandler(subs subscriptionLister, defaultLimit, maxLimit int) *handler {
	return &handler{subs: subs, defaultLimit: defaultLimit, maxLimit: maxLimit}
}

// listQuery mirrors models.SubscriptionListRequest. Pointer fields keep "omitted"
// distinct from the zero value — includeLastMessage omitted means include.
type listQuery struct {
	Type               string `form:"type"`
	Favorite           *bool  `form:"favorite"`
	UpdatedWithinDays  *int   `form:"updatedWithinDays"`
	IncludeLastMessage *bool  `form:"includeLastMessage"`
	Offset             int    `form:"offset"`
	Limit              int    `form:"limit"`
}

// ListSubscriptions serves GET /api/v1/subscriptions for the authenticated caller.
func (h *handler) ListSubscriptions(c *gin.Context) {
	ctx := c.Request.Context()

	var q listQuery
	if err := c.ShouldBindQuery(&q); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid query parameters", errcode.WithCause(err)))
		return
	}

	resp, err := h.subs.ListSubscriptionsFor(ctx, accountFromContext(c), models.SubscriptionListRequest{
		Type:               q.Type,
		Favorite:           q.Favorite,
		UpdatedWithinDays:  q.UpdatedWithinDays,
		IncludeLastMessage: q.IncludeLastMessage,
		Offset:             q.Offset,
		Limit:              q.Limit,
	}, h.defaultLimit, h.maxLimit)
	if err != nil {
		errhttp.Write(ctx, c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}
```

- [ ] **Step 5: Implement routes**

`user-service/routes.go`:

```go
package main

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/ginutil"
)

// httpDeps are the tuning knobs registerRoutes needs from config.
type httpDeps struct {
	maxConcurrency int
	gzipMinBytes   int
	handlerTimeout time.Duration
	onShed         func()
}

// registerRoutes wires the authenticated read API. Health probes deliberately
// live on the separate HEALTH_ADDR listener, so a shed request can never fail a
// kubelet probe and restart the pod mid-burst.
func registerRoutes(r *gin.Engine, h *handler, auth authDeps, d httpDeps) {
	api := r.Group("/api/v1")
	// Limiter first: a shed request must not pay for token validation.
	api.Use(ginutil.MaxConcurrency(d.maxConcurrency, ginutil.WithOnShed(d.onShed)))
	api.Use(ginutil.Gzip(d.gzipMinBytes))
	api.Use(handlerTimeout(d.handlerTimeout))
	api.Use(authMiddleware(auth))
	api.GET("/subscriptions", h.ListSubscriptions)
}

// handlerTimeout bounds a request's work; the enrichment fan-out checks ctx
// between RPCs, so an abandoned request stops doing work.
func handlerTimeout(d time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), d)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
```

Add the `context` import.

- [ ] **Step 6: Run to verify pass**

Run: `make test SERVICE=user-service`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
make fmt && make lint && make generate SERVICE=user-service
git add user-service/store.go user-service/handler.go user-service/routes.go user-service/handler_test.go user-service/mock_store_test.go
git commit -m "feat(user-service): GET /api/v1/subscriptions handler and routes"
```

---

## Task 9: Wire the servers in `main.go`

**Files:**
- Create: `user-service/httpserver.go`
- Modify: `user-service/main.go`

**Interfaces:**
- Consumes: everything above
- Produces: `newHTTPServer(addr string, h http.Handler, cfg config.HTTPConfig) *http.Server`

- [ ] **Step 1: Write the failing test**

`user-service/httpserver_test.go`:

```go
func TestNewHTTPServer_Timeouts(t *testing.T) {
	cfg := config.HTTPConfig{Port: "8080", WriteTimeout: 35 * time.Second}
	srv := newHTTPServer(":8080", http.NotFoundHandler(), cfg)

	assert.Equal(t, ":8080", srv.Addr)
	assert.Equal(t, 5*time.Second, srv.ReadHeaderTimeout)
	assert.Equal(t, 10*time.Second, srv.ReadTimeout)
	assert.Equal(t, 35*time.Second, srv.WriteTimeout, "must exceed the handler budget")
	assert.Equal(t, 120*time.Second, srv.IdleTimeout)
	assert.Equal(t, 16<<10, srv.MaxHeaderBytes)
}
```

- [ ] **Step 2: Run to verify failure**

Expected: FAIL, `undefined: newHTTPServer`.

- [ ] **Step 3: Implement `httpserver.go`**

```go
package main

import (
	"net/http"
	"time"

	"github.com/hmchangw/chat/user-service/config"
)

// newHTTPServer applies the listener tuning: bounded header reads, an idle window
// long enough for a desktop client to reuse connections, and a write deadline that
// outlives the handler budget (net/http starts it at header read, so an equal
// value would cut the response mid-write).
func newHTTPServer(addr string, h http.Handler, cfg config.HTTPConfig) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}
```

- [ ] **Step 4: Wire `main.go`**

In order:

1. **First statement in `main`**, before config load:

```go
	if limit, applied, err := memlimit.SetFromCgroup(0.8); err != nil {
		slog.Warn("could not derive GOMEMLIMIT from cgroup; continuing with the runtime default", "error", err)
	} else if applied {
		slog.Info("GOMEMLIMIT derived from cgroup", "bytes", limit)
	}
```

Re-apply with `cfg.GoMemLimitFraction` after config load only if it differs from 0.8 —
simpler: move the whole call after `config.Load()` and use `cfg.GoMemLimitFraction`.
Do that; the few allocations before it are negligible.

2. **Second Mongo client** after the existing one:

```go
	// A dedicated pool for HTTP: a large page must not exhaust the connections the
	// NATS handlers share. ConnectRead bakes in secondaryPreferred.
	httpMongo, err := mongoutil.ConnectRead(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password,
		mongoutil.WithMaxPoolSize(cfg.HTTP.MongoMaxPoolSize),
		mongoutil.WithMinPoolSize(cfg.HTTP.MongoMinPoolSize),
		mongoutil.WithObservability(sdk),
	)
	if err != nil {
		slog.Error("http mongo connect failed", "error", err)
		os.Exit(1)
	}
```

Add `mongoutil.WithMaxPoolSize(cfg.Mongo.MaxPoolSize)` to the existing NATS-path
`mongoutil.Connect` call, with `MaxPoolSize uint64 \`env:"MAX_POOL_SIZE" envDefault:"100"\``
added to `MongoConfig`. Making the existing implicit default explicit is the point.

3. **Second service instance** over HTTP repos. Reuse the same clients:

```go
	httpDB := httpMongo.Database(cfg.Mongo.DB)
	httpSvc := service.New(
		mongorepo.NewSubscriptionRepo(httpDB, cfg.SiteID, mongorepo.WithShowTeamsRoom(cfg.ShowTeamsRoom), mongorepo.WithShowTeamsAccounts(cfg.ShowTeamsAccounts)),
		mongorepo.NewUserRepo(httpDB), mongorepo.NewAppRepo(httpDB),
		threadSubRepo, roomclient.New(nc, cfg.SiteID), historyclient.New(nc), presenceclient.New(nc),
		publisher.New(js), publisher.NewCore(nc), badge, ssoTokenRepo, tokenValidator, tokenRefresher, &cfg)
```

`EnsureIndexes` runs on the NATS repos only — indexes are shared, and running them
twice is pointless work at startup. Do not pass `WithReadPreference` here: the
client is already `secondaryPreferred`.

4. **Gin engine** (`gin.SetMode(gin.ReleaseMode)`, `gin.New()`) with, in order:
`ginutil.CORS()`, `o11ygin.Middleware("user-service", sdk.TracerProvider(), sdk.MeterProvider(), obs.PublicIngressPropagator(), o11ygin.WithSkipPaths())...`,
`gin.Recovery()`, `ginutil.RequestID()`, `ginutil.AccessLog()`, then `registerRoutes`.

5. **Shed counter** from `sdk.MeterProvider()`, passed as `httpDeps.onShed`.

6. **Health listener**: `health.Serve(cfg.HealthAddr, 5*time.Second, natsutil.HealthCheck(nc))`.

7. **Start the API server** in a goroutine, logging and exiting on any error other
than `http.ErrServerClosed`.

8. **Shutdown order** in `shutdown.Wait`: `httpSrv.Shutdown` → `healthStop` →
`router.Shutdown` → `natsutil.Drain` → `mongoutil.Disconnect(mongoClient)` →
`mongoutil.Disconnect(httpMongo)` → Valkey close → `obsShutdown`.

- [ ] **Step 5: Verify the build and full suite**

Run: `make build SERVICE=user-service && make test SERVICE=user-service`
Expected: builds clean, all tests PASS.

- [ ] **Step 6: Commit**

```bash
make fmt && make lint
git add user-service/main.go user-service/httpserver.go user-service/httpserver_test.go user-service/config/
git commit -m "feat(user-service): serve the HTTP API and health probes"
```

---

## Task 10: Integration test and benchmark

**Files:**
- Create: `user-service/integration_test.go`, `user-service/bench_test.go`

- [ ] **Step 1: Write the integration test**

`//go:build integration`, `package main`, with the mandatory
`func TestMain(m *testing.M) { testutil.RunTests(m) }`.

Seed `testutil.MongoDB(t, "usersvc_http")` with 250 subscriptions and their rooms,
build the real `service.UserService` over `mongorepo` repos plus stub
`RoomClient`/`HistoryClient` that assert every batch is ≤ 100 and return a preview
per requested room. Serve through the real Gin engine with a stub auth validator,
then `GET /api/v1/subscriptions?type=current&limit=200` and assert:

- 200, exactly 200 rows, `hasMore == true`
- **every** row has a non-nil `room.previewMessage` — the regression guard for Task 5
- with `Accept-Encoding: gzip`, `Content-Encoding: gzip` and the decompressed body
  is byte-identical to the uncompressed response

- [ ] **Step 2: Write the benchmark**

`user-service/bench_test.go`: `BenchmarkListResponseMarshalGzip` over 40 / 200 / 400
rows, reporting `b.ReportAllocs()` and the compressed size, so the spec's payload and
memory figures become measurements.

- [ ] **Step 3: Run**

```bash
make test-integration SERVICE=user-service
go test ./user-service/ -bench=. -benchmem -run='^$'
```

Expected: integration PASS; record the benchmark numbers for the PR description.

- [ ] **Step 4: Commit**

```bash
git add user-service/integration_test.go user-service/bench_test.go
git commit -m "test(user-service): integration and benchmark coverage for the HTTP list"
```

---

## Task 11: Documentation and deployment

**Files:**
- Modify: `docs/client-api.md`, `docs/health-probes.md`, `user-service/deploy/docker-compose.yml`

- [ ] **Step 1: Document the endpoint**

Add an HTTP section to `docs/client-api.md` in the existing field-table style:
endpoint, auth headers (noting `ssoToken` is preferred — `botauth` caps concurrent
session validations at 64), the parameter table from spec §5, a success example,
the full error table, and the 429 example verbatim:

```
HTTP/1.1 429 Too Many Requests
Retry-After: 1
Content-Type: application/json

{
  "code": "too_many_requests",
  "error": "server is at capacity, retry shortly",
  "reason": "overloaded"
}
```

State that clients should send `limit=200` and honour `Retry-After` on 429.

- [ ] **Step 2: Update the probe table**

In `docs/health-probes.md`, give user-service its own row: probes on `HEALTH_ADDR`
(default `:8081`), separate from the API listener on `HTTP_PORT`.

- [ ] **Step 3: Update compose**

Expose `8080` and `8081` in `user-service/deploy/docker-compose.yml` and add the
new env vars with their defaults.

- [ ] **Step 4: Verify and commit**

```bash
make lint && make test && make sast
git add docs/ user-service/deploy/
git commit -m "docs(client-api): document GET /api/v1/subscriptions"
```

---

## Self-Review

**Spec coverage.** §5 contract → Tasks 6, 8. §5 429 → Tasks 1, 11. §6 auth → Task 7.
§7 core → Task 4. §8 chunking → Task 5. §9 limiter/GOMEMLIMIT → Tasks 1, 3, 9.
§10 Gin/http.Server/gzip → Tasks 2, 8, 9. §11 health → Task 9. §12 config → Tasks 5, 6.
§13 Mongo isolation → Task 9. §14 deployment → Tasks 9, 11. §15 observability → Tasks 1, 9.
§16 testing → every task, plus Task 10. §17 docs → Task 11. No gaps.

**Deliberately deferred.** Spec §10's optional per-row JSON streaming is not a task:
it saves ~150 KB per in-flight request but risks the response contract, and the
benchmark in Task 10 should justify it before it is written. Recorded as follow-up,
not silently dropped.

**Type consistency.** `ListSubscriptionsFor(ctx, account, req, defaultLimit, maxLimit)`
is identical in Tasks 4, 8, and the Task 8 interface. `chunkRoomIDs(ids, size)` matches
between Task 5's tests and implementation. `authDeps`/`accountFromContext`/`ctxAccountKey`
match between Tasks 7 and 8. `httpDeps` fields match between Tasks 8 and 9.
`newHTTPServer(addr, handler, cfg)` matches between Task 9's test and implementation.

---

## Divergences from this plan

Recorded so the plan stays a faithful record of what was intended, while pointing
at what actually shipped. The design doc and `docs/client-api.md` are the live
documents; where they disagree with the code blocks above, they win.

| Planned | Shipped | Why |
|---|---|---|
| `SUBSCRIPTION_HTTP_DEFAULT_LIMIT` / `SUBSCRIPTION_HTTP_MAX_LIMIT`, mirrored into `HTTPConfig` with `env:"-"` and a hand-written copy step | `HTTP_SUBSCRIPTION_DEFAULT_LIMIT` / `HTTP_SUBSCRIPTION_MAX_LIMIT`, inside the prefixed block | The mirror stored every value twice and needed a manual sync a future field would forget |
| `chunkRoomIDs` helper | stdlib `slices.Chunk` | The package already had `chunkStrings`; a third chunker was not worth it |
| Two hand-written chunked fan-out loops | one generic `fanOutChunks`, per-chunk result slots | The loops were the same skeleton twice, and per-chunk slots removed the merge mutex |
| Chunk the room ids, rebuild hints per chunk | chunk the row indices | ids and hints then fall out of one pass, deleting a lookup set and a per-chunk rescan |
| `MaxConcurrency(n, opts...)` with `WithRetryAfter` | `MaxConcurrency(n, onShed)` | The option had no production caller |
| `memlimit.SetFromCgroup` returning `(int64, bool, error)` | `(int64, error)` | The bool was derivable from the limit |
| `handlerTimeout` in `user-service/routes.go` | `ginutil.Timeout` | Generic middleware belongs with its two siblings in `pkg/` |
| gzip at `DefaultCompression` | `BestSpeed` | Measured 39% faster for 3 KB more on a 200-row page |
| No connection ceiling | `HTTP_MAX_CONNS` via `netutil.LimitListener` | Review: the handler cap does not bound accepted connections |
| `MAX_SITE_FANOUT` dropped as redundant | restored, per this plan | Review: chunking multiplies downstream RPCs, so the bound has to be tunable |
