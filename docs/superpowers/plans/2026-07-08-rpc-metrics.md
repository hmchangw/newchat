# RPC latency & error-rate metrics — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit uniform `rpc_server_requests_total` and `rpc_server_request_duration_seconds` metrics for every synchronous RPC handler (6 NATS request/reply services + 4 Gin HTTP services), queryable by a single `service` label.

**Architecture:** One shared `pkg/rpcmetrics` package owns the Prometheus collectors and status taxonomy. Two thin middlewares — `natsrouter.Metrics` and `ginutil.Metrics` — feed it, so both transports emit identical series. `route` is always the low-cardinality pattern/template; `status` is the errcode `Code`.

**Tech Stack:** Go 1.25, `prometheus/client_golang` (`promauto`, default registry), `pkg/natsrouter`, `pkg/ginutil`, `pkg/errcode`, `stretchr/testify`, `prometheus/testutil`.

## Global Constraints

- Go 1.25; single `go.mod` at repo root; services are flat `package main` at repo root; shared code in `pkg/`.
- Use `make` targets, never raw `go`: `make test SERVICE=<name>`, `make lint`, `make sast`, `make build SERVICE=<name>`. Package tests: `make test` runs all with `-race`.
- No new third-party dependencies — `prometheus/client_golang` (incl. `prometheus/testutil`) is already in `go.mod`.
- Metric collectors register with the **default** Prometheus registry via `promauto`; `promhttp.Handler()` / `otelutil.MetricsServer()` exposes them on `/metrics`. `InitMeter` is NOT required for these metrics.
- Package names must be descriptive — never `utils`/`helpers`/`common`/`base`. New package is `rpcmetrics`.
- TDD: Red → Green → Refactor → Commit. Tests in `package <pkg>` / `package main` (same package). Minimum 80% coverage for new `pkg/` code; target 90%+.
- `status` allowlist is exactly: `ok` + the 8 canonical errcode Codes (`bad_request`, `unauthenticated`, `forbidden`, `not_found`, `conflict`, `too_many_requests`, `unavailable`, `internal`). Anything else collapses to `internal`.
- `route` label MUST be a pattern/template, never a live subject or URL (cardinality guard).
- Commit identity: `git config user.email noreply@anthropic.com && git config user.name Claude`. Commit trailer lines:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` and
  `Claude-Session: https://claude.ai/code/session_01Re53EQYKpXS3m9tBkRhbVb`.
- No `docs/client-api.md` change (server-side observability only).

---

## File Structure

- **Create** `pkg/rpcmetrics/metrics.go` — collectors + `Observe` + `StatusLabel`.
- **Create** `pkg/rpcmetrics/doc.go` — package doc: metric names, labels, cardinality rule.
- **Create** `pkg/rpcmetrics/metrics_test.go` — unit tests.
- **Modify** `pkg/natsrouter/params.go` — add `pattern` field to `route`.
- **Modify** `pkg/natsrouter/router.go` — thread `rt.pattern` into `acquireContext`.
- **Modify** `pkg/natsrouter/context.go` — add `route`/`status` fields + accessors.
- **Modify** `pkg/natsrouter/register.go` — stamp terminal status in the 3 `Register*` wrappers.
- **Create** `pkg/natsrouter/metrics.go` — `Metrics(service) HandlerFunc`.
- **Create** `pkg/natsrouter/metrics_test.go` — middleware test.
- **Modify** `pkg/errcode/errhttp/write.go` — stamp classified Code onto gin ctx.
- **Create** `pkg/ginutil/metrics.go` — `Metrics(service) gin.HandlerFunc`.
- **Create** `pkg/ginutil/metrics_test.go` — middleware test.
- **Modify** `search-service/metrics.go` + `search-service/handler.go` + `search-service/main.go` — migrate off bespoke request metrics.
- **Modify** service mains: `room-service`, `room-worker`, `user-service`, `user-presence-service`, `history-service` (router side); `auth-service`, `portal-service`, `media-service`, `upload-service` (HTTP side).

---

## Task 1: `pkg/rpcmetrics` core

**Files:**
- Create: `pkg/rpcmetrics/metrics.go`
- Create: `pkg/rpcmetrics/doc.go`
- Test: `pkg/rpcmetrics/metrics_test.go`

**Interfaces:**
- Consumes: `github.com/hmchangw/chat/pkg/errcode` (`*errcode.Error`, `CodeBadRequest`…`CodeInternal`).
- Produces:
  - `func Observe(service, route, status string, d time.Duration)`
  - `func StatusLabel(err error) string`
  - `func CounterValue(service, route, status string) float64` — test seam consumed by the natsrouter and ginutil tests (Tasks 2, 3).
  - metric names `rpc_server_requests_total{service,route,status}` (counter), `rpc_server_request_duration_seconds{service,route}` (histogram).

- [ ] **Step 1: Write the failing tests**

Create `pkg/rpcmetrics/metrics_test.go`:

```go
package rpcmetrics

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/errcode"
)

func TestStatusLabel(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil is ok", nil, "ok"},
		{"bad request", errcode.BadRequest("bad"), "bad_request"},
		{"not found", errcode.NotFound("nope"), "not_found"},
		{"forbidden", errcode.Forbidden("no"), "forbidden"},
		{"conflict", errcode.Conflict("dup"), "conflict"},
		{"unauthenticated", errcode.Unauthenticated("who"), "unauthenticated"},
		{"too many requests", errcode.TooManyRequests("slow"), "too_many_requests"},
		{"unavailable", errcode.Unavailable("down"), "unavailable"},
		{"internal", errcode.Internal("boom"), "internal"},
		{"wrapped errcode resolves via errors.As", fmt.Errorf("ctx: %w", errcode.NotFound("nope")), "not_found"},
		{"plain error collapses to internal", errors.New("raw"), "internal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, StatusLabel(tt.err))
		})
	}
}

func TestObserve(t *testing.T) {
	Observe("svc-test", "chat.route.{id}.get", "ok", 12*time.Millisecond)

	// Counter incremented for the exact label tuple (via the exported seam).
	assert.Equal(t, float64(1), CounterValue("svc-test", "chat.route.{id}.get", "ok"))

	// The duration histogram registered a series for this service/route pair.
	count := testutil.CollectAndCount(requestDuration, "rpc_server_request_duration_seconds")
	assert.GreaterOrEqual(t, count, 1)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=rpcmetrics` (or `go test ./pkg/rpcmetrics/`)
Expected: FAIL — `undefined: StatusLabel`, `undefined: Observe`, `undefined: requestsTotal`.

> Note: `make test SERVICE=<name>` targets a service dir; for a `pkg/` package the runner may not resolve `SERVICE=rpcmetrics`. If so, run `go test -race ./pkg/rpcmetrics/...` directly — this is the one sanctioned raw-`go` use, for a package with no `make` target.

- [ ] **Step 3: Write `pkg/rpcmetrics/metrics.go`**

```go
// Package rpcmetrics holds the shared Prometheus collectors and status
// taxonomy for synchronous RPC handler metrics, emitted identically by the
// NATS (pkg/natsrouter) and HTTP (pkg/ginutil) middlewares.
package rpcmetrics

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/hmchangw/chat/pkg/errcode"
)

// Collectors register with the default Prometheus registry via promauto, so a
// plain promhttp.Handler() (or otelutil.MetricsServer) exposes them on /metrics.
var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rpc_server_requests_total",
		Help: "Total RPC request/reply invocations handled, partitioned by service, route pattern, and terminal status.",
	}, []string{"service", "route", "status"})

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rpc_server_request_duration_seconds",
		Help:    "End-to-end RPC handler latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"service", "route"})
)

// Observe records one completed RPC: its latency and terminal status.
// route MUST be a pattern/template (e.g. "chat.user.{account}.request.room.get"
// or a Gin FullPath), never a live subject/URL, to keep cardinality bounded.
func Observe(service, route, status string, d time.Duration) {
	requestDuration.WithLabelValues(service, route).Observe(d.Seconds())
	requestsTotal.WithLabelValues(service, route, status).Inc()
}

// StatusLabel maps a handler's returned error onto the `status` label:
// nil -> "ok"; a non-empty *errcode.Error Code in the chain that is in the
// pinned allowlist -> that Code; everything else -> "internal". It is a pure,
// non-logging Code extractor (errors.As) — it never double-logs against
// errcode.Classify, so it is safe to call on the reply path.
func StatusLabel(err error) string {
	if err == nil {
		return "ok"
	}
	var ee *errcode.Error
	if errors.As(err, &ee) && ee.Code != "" {
		if _, ok := allowedStatusLabels[string(ee.Code)]; ok {
			return string(ee.Code)
		}
	}
	return string(errcode.CodeInternal)
}

// allowedStatusLabels pins the cardinality of the status label to the 8
// canonical errcode Codes + "ok". Any label outside this set collapses to
// "internal" via StatusLabel, so a future Code added without updating this
// allowlist cannot mint a fresh time series.
var allowedStatusLabels = map[string]struct{}{
	"ok":                                {},
	string(errcode.CodeBadRequest):      {},
	string(errcode.CodeUnauthenticated): {},
	string(errcode.CodeForbidden):       {},
	string(errcode.CodeNotFound):        {},
	string(errcode.CodeConflict):        {},
	string(errcode.CodeTooManyRequests): {},
	string(errcode.CodeUnavailable):     {},
	string(errcode.CodeInternal):        {},
}

// CounterValue returns the current rpc_server_requests_total value for the
// given label tuple. It is a test seam for consumer packages (natsrouter,
// ginutil) that cannot reach the unexported collector; side-effect-free.
func CounterValue(service, route, status string) float64 {
	return testutil.ToFloat64(requestsTotal.WithLabelValues(service, route, status))
}
```

- [ ] **Step 4: Write `pkg/rpcmetrics/doc.go`**

```go
// Package rpcmetrics — RPC observability vocabulary.
//
// Metrics (default Prometheus registry, exposed on /metrics):
//
//	rpc_server_requests_total{service, route, status}        counter
//	rpc_server_request_duration_seconds{service, route}      histogram (DefBuckets)
//
// Labels:
//
//   - service: the process's service name (same string passed to
//     natsrouter.New / InitTracer), one value per process.
//   - route:   the ROUTE PATTERN, never a live subject/URL. NATS uses the
//     natsrouter pattern with {name} placeholders; HTTP uses Gin's FullPath
//     (registered template). This is the cardinality guard.
//   - status:  the errcode Code (ok + 8 canonical codes). Anything outside the
//     pinned allowlist collapses to "internal".
//
// Emitted by pkg/natsrouter.Metrics (NATS request/reply) and
// pkg/ginutil.Metrics (Gin HTTP). Do not add per-service copies of these
// series — query by the `service` label instead.
package rpcmetrics
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test -race ./pkg/rpcmetrics/...`
Expected: PASS (both tests).

- [ ] **Step 6: Lint**

Run: `make lint`
Expected: no findings in `pkg/rpcmetrics`.

- [ ] **Step 7: Commit**

```bash
git add pkg/rpcmetrics/
git commit -m "feat(rpcmetrics): shared RPC metrics collectors and status taxonomy"
```

---

## Task 2: `natsrouter.Metrics` middleware + Context plumbing

**Files:**
- Modify: `pkg/natsrouter/params.go` (add `pattern` to `route`, set in `parsePattern`)
- Modify: `pkg/natsrouter/router.go` (pass `rt.pattern` into `acquireContext`)
- Modify: `pkg/natsrouter/context.go` (add `route`/`status` fields + accessors)
- Modify: `pkg/natsrouter/register.go` (stamp status in `Register`/`RegisterNoBody`/`RegisterVoid`)
- Create: `pkg/natsrouter/metrics.go` (`Metrics(service) HandlerFunc`)
- Test: `pkg/natsrouter/metrics_test.go`

**Interfaces:**
- Consumes: `rpcmetrics.Observe`, `rpcmetrics.StatusLabel`; existing `Context`, `HandlerFunc`, `acquireContext`, `route`.
- Produces:
  - `func (c *Context) Route() string`
  - `func (c *Context) SetStatus(s string)` and `func (c *Context) Status() string` (defaults `"ok"` when unset)
  - `func Metrics(service string) HandlerFunc`

- [ ] **Step 1: Write the failing test**

Create `pkg/natsrouter/metrics_test.go`:

```go
package natsrouter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/rpcmetrics"
)

// runChain drives a middleware+handler chain against a bare Context, the same
// way acquireContext wires `all` in addRoute.
func runChain(route string, handlers ...HandlerFunc) {
	c := NewContext(nil)
	c.route = route
	c.chain.handlers = handlers
	c.chain.index = -1
	c.Next()
}

func TestMetrics_RecordsOKStatus(t *testing.T) {
	before := rpcmetrics.CounterValue("user-service", "chat.route.ok", "ok")

	runChain("chat.route.ok",
		Metrics("user-service"),
		func(c *Context) { c.SetStatus("ok") },
	)

	after := rpcmetrics.CounterValue("user-service", "chat.route.ok", "ok")
	assert.Equal(t, before+1, after)
}

func TestMetrics_RecordsErrorStatus(t *testing.T) {
	before := rpcmetrics.CounterValue("user-service", "chat.route.err", "not_found")

	runChain("chat.route.err",
		Metrics("user-service"),
		func(c *Context) { c.SetStatus(rpcmetrics.StatusLabel(errcode.NotFound("nope"))) },
	)

	after := rpcmetrics.CounterValue("user-service", "chat.route.err", "not_found")
	assert.Equal(t, before+1, after)
}

func TestMetrics_UnsetStatusDefaultsOK(t *testing.T) {
	before := rpcmetrics.CounterValue("user-service", "chat.route.default", "ok")

	runChain("chat.route.default",
		Metrics("user-service"),
		func(c *Context) { /* never sets status */ },
	)

	after := rpcmetrics.CounterValue("user-service", "chat.route.default", "ok")
	assert.Equal(t, before+1, after)
}
```

> `rpcmetrics.CounterValue` is defined in Task 1, so it is already available to
> this test — no per-package accessor is needed.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -race ./pkg/natsrouter/ -run TestMetrics`
Expected: FAIL — `undefined: Metrics`, `c.route` unexported field not set, `undefined: SetStatus`.

- [ ] **Step 3: Add `pattern` to the `route` struct**

In `pkg/natsrouter/params.go`, extend the struct and `parsePattern` return:

```go
type route struct {
	pattern     string         // original pattern with {name} placeholders — the metrics `route` label
	natsSubject string         // "chat.user.*.request.room.*.*.msg.history"
	params      map[int]string // {2: "account", 5: "roomID", 6: "siteID"}
}
```

```go
	return route{
		pattern:     pattern,
		natsSubject: strings.Join(nats, "."),
		params:      params,
	}
```

- [ ] **Step 4: Thread `pattern` into the Context**

In `pkg/natsrouter/context.go`, add fields to `Context` (near `keys`):

```go
	// route is the matched route pattern (with {name} placeholders); set once
	// at acquire, read by the Metrics middleware. Stable for the request's life.
	route string
	// status is the terminal RPC status label. Set by the Register* wrappers
	// synchronously within the chain, before the reply; read by the Metrics
	// middleware after c.Next(). Single-goroutine, sequential — no lock needed.
	status string
```

Add accessors (below the existing `context.Context` methods):

```go
// Route returns the matched route pattern (with {name} placeholders), the
// low-cardinality label the Metrics middleware uses. Empty for test contexts.
func (c *Context) Route() string { return c.route }

// SetStatus records the terminal RPC status label for the Metrics middleware.
func (c *Context) SetStatus(s string) { c.status = s }

// Status returns the terminal status label, defaulting to "ok" when a handler
// completed without setting one (e.g. a fire-and-forget void success).
func (c *Context) Status() string {
	if c.status == "" {
		return "ok"
	}
	return c.status
}
```

Update `acquireContext` signature to accept and set the route:

```go
func acquireContext(ctx context.Context, msg *nats.Msg, params Params, handlers []HandlerFunc, route string) *Context {
	cs := chainPool.Get().(*chainState)
	cs.handlers = handlers
	cs.index = -1
	return &Context{
		ctx:    ctx,
		Msg:    msg,
		Params: params,
		route:  route,
		chain:  cs,
	}
}
```

In `releaseContext`, no change needed for `route`/`status` (the `*Context` is left to GC; only `chainState` is pooled).

- [ ] **Step 5: Update the `acquireContext` call site**

In `pkg/natsrouter/router.go` `addRoute`, change the call inside the spawned goroutine:

```go
			c := acquireContext(m.Context(), m.Msg, rt.extractParams(m.Msg.Subject), all, rt.pattern)
```

- [ ] **Step 6: Stamp terminal status in the Register wrappers**

In `pkg/natsrouter/register.go`, import `rpcmetrics`, and set the status in each wrapper before replying.

`Register`:

```go
		resp, err := fn(c, req)
		if err != nil {
			c.SetStatus(rpcmetrics.StatusLabel(err))
			replyErr(c, err)
			return
		}
		c.SetStatus("ok")
		c.ReplyJSON(resp)
```

Also set the status on the unmarshal-failure branch:

```go
		if err := json.Unmarshal(c.Msg.Data, &req); err != nil {
			c.SetStatus(string(errcode.CodeBadRequest))
			replyErr(c, errcode.BadRequest("invalid request payload", errcode.WithCause(err)))
			return
		}
```

`RegisterNoBody`:

```go
		resp, err := fn(c)
		if err != nil {
			c.SetStatus(rpcmetrics.StatusLabel(err))
			replyErr(c, err)
			return
		}
		c.SetStatus("ok")
		c.ReplyJSON(resp)
```

`RegisterVoid` (no reply, but still record a status for metrics):

```go
		var req Req
		if err := json.Unmarshal(c.Msg.Data, &req); err != nil {
			c.SetStatus(string(errcode.CodeBadRequest))
			slog.Error("invalid payload in void handler", "error", err, "subject", c.Msg.Subject)
			return
		}
		if err := fn(c, req); err != nil {
			c.SetStatus(rpcmetrics.StatusLabel(err))
			slog.Error("void handler error", "error", err, "subject", c.Msg.Subject)
			return
		}
		c.SetStatus("ok")
```

Add the import `"github.com/hmchangw/chat/pkg/rpcmetrics"` to `register.go`.

- [ ] **Step 7: Write the `Metrics` middleware**

Create `pkg/natsrouter/metrics.go`:

```go
package natsrouter

import (
	"time"

	"github.com/hmchangw/chat/pkg/rpcmetrics"
)

// Metrics returns middleware that records rpc_server_requests_total and
// rpc_server_request_duration_seconds for every request handled by this
// router. The `route` label is the matched pattern (c.Route()); the `status`
// label is the terminal status stamped by the Register* wrappers (c.Status(),
// defaulting to "ok"). Install once via r.Use — placement relative to other
// middleware only shifts what latency window is measured; place it outermost
// (first) to capture the full chain.
func Metrics(service string) HandlerFunc {
	return func(c *Context) {
		start := time.Now()
		c.Next()
		rpcmetrics.Observe(service, c.Route(), c.Status(), time.Since(start))
	}
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `go test -race ./pkg/natsrouter/ -run TestMetrics`
Expected: PASS (three cases). Then full package: `go test -race ./pkg/natsrouter/...` → PASS (no regressions from the `acquireContext` signature change; the only caller is `addRoute`).

- [ ] **Step 9: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add pkg/natsrouter/ pkg/rpcmetrics/metrics.go
git commit -m "feat(natsrouter): Metrics middleware with route/status plumbing"
```

---

## Task 3: `ginutil.Metrics` middleware + errhttp Code stamping

**Files:**
- Modify: `pkg/errcode/errhttp/write.go`
- Create: `pkg/ginutil/metrics.go`
- Test: `pkg/ginutil/metrics_test.go`

**Interfaces:**
- Consumes: `rpcmetrics.Observe`, `rpcmetrics.CounterValue`; `gin`, `errcode`.
- Produces:
  - `func Metrics(service string) gin.HandlerFunc`
  - errhttp writes the classified Code under gin key `ginutil.ErrCodeKey`.
  - `const ErrCodeKey = "errcode"` exported from `pkg/ginutil`.

- [ ] **Step 1: Write the failing test**

Create `pkg/ginutil/metrics_test.go`:

```go
package ginutil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/rpcmetrics"
)

func newEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Metrics("auth-service"))
	return r
}

func do(r *gin.Engine, method, target string) {
	req := httptest.NewRequest(method, target, nil)
	r.ServeHTTP(httptest.NewRecorder(), req)
}

func TestGinMetrics_ErrcodeStatus(t *testing.T) {
	r := newEngine()
	r.GET("/api/rooms/:id", func(c *gin.Context) {
		errhttp.Write(context.Background(), c, errcode.NotFound("nope"))
	})
	before := rpcmetrics.CounterValue("auth-service", "/api/rooms/:id", "not_found")

	do(r, http.MethodGet, "/api/rooms/42")

	after := rpcmetrics.CounterValue("auth-service", "/api/rooms/:id", "not_found")
	assert.Equal(t, before+1, after)
}

func TestGinMetrics_HTTPClassFallback(t *testing.T) {
	r := newEngine()
	r.GET("/api/ping", func(c *gin.Context) { c.Status(http.StatusOK) })
	before := rpcmetrics.CounterValue("auth-service", "/api/ping", "ok")

	do(r, http.MethodGet, "/api/ping")

	after := rpcmetrics.CounterValue("auth-service", "/api/ping", "ok")
	assert.Equal(t, before+1, after)
}

func TestGinMetrics_SkipsHealthzAndUnmatched(t *testing.T) {
	r := newEngine()
	r.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })

	// /healthz is skipped: empty-route counter must not move.
	beforeHealth := rpcmetrics.CounterValue("auth-service", "/healthz", "ok")
	do(r, http.MethodGet, "/healthz")
	assert.Equal(t, beforeHealth, rpcmetrics.CounterValue("auth-service", "/healthz", "ok"))

	// Unmatched route (empty FullPath) is skipped: assert the "" route stays absent.
	beforeEmpty := rpcmetrics.CounterValue("auth-service", "", "not_found")
	do(r, http.MethodGet, "/does-not-exist")
	assert.Equal(t, beforeEmpty, rpcmetrics.CounterValue("auth-service", "", "not_found"))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -race ./pkg/ginutil/ -run TestGinMetrics`
Expected: FAIL — `undefined: Metrics`.

- [ ] **Step 3: Stamp the classified Code in errhttp.Write**

Modify `pkg/errcode/errhttp/write.go`:

```go
// Package errhttp adapts errcode.Error to Gin HTTP responses.
package errhttp

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
)

// ErrCodeKey is the gin.Context key under which Write records the classified
// errcode Code, so metrics middleware can label the response without
// re-classifying (which would double-log).
const ErrCodeKey = "errcode"

// Write classifies err (logging once) and writes the envelope with its HTTP
// status. It also records the classified Code on the gin context under
// ErrCodeKey for downstream metrics middleware.
func Write(ctx context.Context, c *gin.Context, err error) {
	e := errcode.Classify(ctx, err)
	c.Set(ErrCodeKey, string(e.Code))
	c.JSON(e.HTTPStatus(), e)
}
```

> `ginutil` reads this key by its literal string (`"errcode"`) to avoid a
> `ginutil → errhttp` import edge; the constant lives in `errhttp` where
> `Write` sets it.

- [ ] **Step 4: Write the ginutil Metrics middleware**

Create `pkg/ginutil/metrics.go`:

```go
package ginutil

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/rpcmetrics"
)

// errCodeKey mirrors errhttp.ErrCodeKey by value to avoid an import edge from
// ginutil to errhttp. errhttp.Write stores the classified Code here.
const errCodeKey = "errcode"

// Metrics returns middleware that records rpc_server_requests_total and
// rpc_server_request_duration_seconds for each matched HTTP route. The `route`
// label is the registered template (c.FullPath()), never the live URL. The
// `status` label is the errcode Code stamped by errhttp.Write when present,
// else an HTTP-status class (2xx->ok, 4xx->bad_request, 5xx->internal).
// Unmatched routes (empty FullPath) and /healthz are skipped.
func Metrics(service string) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" || route == "/healthz" {
			return
		}
		rpcmetrics.Observe(service, route, statusForHTTP(c), time.Since(start))
	}
}

// statusForHTTP prefers the errcode Code stamped by errhttp.Write; otherwise it
// derives a coarse class from the HTTP status code so plain responses still map
// onto the shared status taxonomy.
func statusForHTTP(c *gin.Context) string {
	if v, ok := c.Get(errCodeKey); ok {
		if code, ok := v.(string); ok && code != "" {
			return code
		}
	}
	switch code := c.Writer.Status(); {
	case code >= 500:
		return "internal"
	case code >= 400:
		return "bad_request"
	default:
		return "ok"
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test -race ./pkg/ginutil/ -run TestGinMetrics`
Expected: PASS (three tests). Then `go test -race ./pkg/errcode/errhttp/...` → PASS (existing `write_test.go` unaffected; the new `c.Set` is inert for its assertions).

- [ ] **Step 6: Lint**

Run: `make lint`
Expected: clean (`pkg/ginutil/metrics.go` imports only `time`, `gin`, `rpcmetrics`).

- [ ] **Step 7: Commit**

```bash
git add pkg/ginutil/metrics.go pkg/ginutil/metrics_test.go pkg/errcode/errhttp/write.go
git commit -m "feat(ginutil): HTTP RPC Metrics middleware; errhttp stamps Code"
```

---

## Task 4: Migrate `search-service` to the shared middleware

**Files:**
- Modify: `search-service/metrics.go` (delete request-path machinery; keep ES metric)
- Modify: `search-service/handler.go` (remove `defer observeRequest(...)()` calls)
- Modify: `search-service/main.go` (add `router.Use(natsrouter.Metrics("search-service"))`)
- Existing tests: `search-service/handler_test.go` (adjust if they assert on removed symbols)

**Interfaces:**
- Consumes: `natsrouter.Metrics`.
- Produces: no exported change; `search_service_es_duration_seconds` retained.

- [ ] **Step 1: Remove request-path metrics from `search-service/metrics.go`**

Delete: `metricRequestsTotal`, `metricRequestDuration`, the `metricKind*`
consts, the `durMessages/durRooms/durApps/durUsers` vars, `observeRequest`,
`durFor`, `statusLabel`, `allowedStatusLabels`. **Keep**: `metricESDuration`,
`observeES`, and `metricsHandler()`. The file should reduce to the ES histogram
+ `observeES` + `metricsHandler`. Resulting file:

```go
package main

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metricESDuration is search-service-specific (Elasticsearch _search latency)
// and stays here; the generic request-path metrics now come from
// natsrouter.Metrics (pkg/rpcmetrics).
var metricESDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "search_service_es_duration_seconds",
	Help:    "Elasticsearch _search call latency in seconds.",
	Buckets: prometheus.DefBuckets,
})

func observeES() func() {
	start := time.Now()
	return func() { metricESDuration.Observe(time.Since(start).Seconds()) }
}

func metricsHandler() http.Handler { return promhttp.Handler() }
```

- [ ] **Step 2: Remove `observeRequest` call sites in `search-service/handler.go`**

Delete the four `defer observeRequest(metricKind..., &err)()` lines
(handler.go:74, 129, 218, 254). Leave the `observeES()` usages (lines 109-111,
161-163) untouched. If a handler's only use of its named `err` return was
`observeRequest`, keep the named return if still referenced elsewhere;
otherwise simplify the signature to a plain `error` return only if the linter
flags an unused named result — do not otherwise change handler logic.

- [ ] **Step 3: Add the middleware in `search-service/main.go`**

Change the router middleware block (main.go:179-183):

```go
	router := natsrouter.New(nc, "search-service")
	router.Use(natsrouter.RequestID())
	router.Use(natsrouter.Recovery())
	router.Use(natsrouter.Metrics("search-service"))
	router.Use(natsrouter.Logging())
	handler.Register(router)
```

- [ ] **Step 4: Update tests that referenced removed symbols**

Run: `go test -race ./search-service/... 2>&1 | head -40`
Expected initially: compile errors only if `handler_test.go` referenced
`observeRequest`/`statusLabel`/`metricKind*`. If so, delete those specific
assertions/tests (the behavior is now covered by `pkg/natsrouter/metrics_test.go`
and `pkg/rpcmetrics/metrics_test.go`). Do not weaken unrelated tests.

- [ ] **Step 5: Build + test search-service**

Run: `make build SERVICE=search-service && make test SERVICE=search-service`
Expected: build OK; tests PASS.

- [ ] **Step 6: Lint**

Run: `make lint`
Expected: clean (no unused imports/vars left in `search-service`).

- [ ] **Step 7: Commit**

```bash
git add search-service/
git commit -m "refactor(search-service): use shared natsrouter.Metrics; drop bespoke request metrics"
```

---

## Task 5: Wire the remaining NATS request/reply services

**Files (each service's main; add the `Metrics` Use line; add a `/metrics` listener + `MetricsAddr` config to the three that lack one):**
- Modify: `room-worker/main.go` — has `/metrics` already; add Use line only.
- Modify: `history-service/cmd/main.go` — has `/metrics` already; add Use line only.
- Modify: `room-service/main.go` — add Use line + `/metrics` listener + config field.
- Modify: `user-service/main.go` — add Use line + `/metrics` listener + config field.
- Modify: `user-presence-service/main.go` — add Use line + `/metrics` listener + config field.

**Interfaces:**
- Consumes: `natsrouter.Metrics`, `otelutil.MetricsServer`.
- Produces: each service exposes `rpc_server_*` on `/metrics`.

Two edit patterns follow. Apply Pattern A to all five services; additionally apply Pattern B to the three without a listener.

- [ ] **Step 1 (Pattern A — add the middleware, all 5 services):**

Insert `natsrouter.Metrics("<service>")` into each router's middleware chain, immediately before `Logging` (outermost after Recovery/RequestID). Exact per-service edits:

- `room-service/main.go:208`:
  ```go
  router.Use(natsrouter.Recovery(), natsrouter.RequestID(), natsrouter.Metrics("room-service"), natsrouter.Logging())
  ```
- `room-worker/main.go` (find its `router.Use(...)` line): add `natsrouter.Metrics("room-worker")` before `natsrouter.Logging()` in the same call.
- `history-service/cmd/main.go` (find its `router.Use(...)`): add `natsrouter.Metrics("history-service")` before `Logging`.
- `user-service/main.go` (find its `router.Use(...)`): add `natsrouter.Metrics("user-service")` before `Logging`.
- `user-presence-service/main.go:133`: it uses `natsrouter.Default(...)` (which already installed Recovery/RequestID/Logging). Add a `router.Use(natsrouter.Metrics("user-presence-service"))` line immediately after the `natsrouter.Default(...)` construction (Metrics still wraps because `Use` appends and all are outer to the terminal handler; the duration window is the handler + any middleware registered after it — acceptable, since Default's Logging is already installed and Metrics measures from its own `c.Next()`).

> If a service constructs middleware across multiple `router.Use(...)` calls (like search-service did), insert a dedicated `router.Use(natsrouter.Metrics("<service>"))` line before the `Logging` line instead of editing an existing multi-arg call.

- [ ] **Step 2 (Pattern B — add `/metrics` listener; only room-service, user-service, user-presence-service):**

For each of these three, (a) add a config field, (b) add the listener, (c) add shutdown.

(a) In the service's `Config`/`config` struct (in `main.go`), add:

```go
	MetricsAddr string `env:"METRICS_ADDR" envDefault:":9090"`
```

(b) After the router is constructed and handlers registered, before `shutdown.Wait`, add (mirrors `history-service/cmd/main.go:212-223`):

```go
	// Bind synchronously so a port conflict fails startup loudly rather than
	// running blind — /metrics exposes rpc_server_* RPC metrics.
	metricsServer := otelutil.MetricsServer()
	metricsLn, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		slog.Error("metrics listen failed", "addr", cfg.MetricsAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("metrics server listening", "addr", cfg.MetricsAddr)
		if err := metricsServer.Serve(metricsLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", err)
		}
	}()
```

(c) Add a shutdown step inside the existing `shutdown.Wait(ctx, 25*time.Second, ...)` list, placed AFTER `router.Shutdown` and BEFORE `nc.Drain` (so final drain-window samples are still scrapeable, matching history-service):

```go
		func(ctx context.Context) error { return metricsServer.Shutdown(ctx) },
```

(d) Ensure imports include `"net"`, `"net/http"`, `"errors"`, and `"github.com/hmchangw/chat/pkg/otelutil"`. Add any that are missing (goimports via `make fmt` will order them).

- [ ] **Step 3: Format**

Run: `make fmt`
Expected: imports ordered; no diff noise.

- [ ] **Step 4: Build each modified service**

Run:
```bash
make build SERVICE=room-service && \
make build SERVICE=room-worker && \
make build SERVICE=history-service && \
make build SERVICE=user-service && \
make build SERVICE=user-presence-service
```
Expected: all build OK.

- [ ] **Step 5: Unit tests for modified services**

Run:
```bash
for s in room-service room-worker history-service user-service user-presence-service; do make test SERVICE=$s || break; done
```
Expected: PASS for each (main-wiring changes shouldn't touch handler tests).

- [ ] **Step 6: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add room-service/ room-worker/ history-service/ user-service/ user-presence-service/
git commit -m "feat(rpc-metrics): wire natsrouter.Metrics into all request/reply services"
```

---

## Task 6: Wire the HTTP (Gin) services

**Files:**
- Modify: `auth-service/main.go`
- Modify: `portal-service/main.go`
- Modify: `media-service/main.go`
- Modify: `upload-service/main.go`

**Interfaces:**
- Consumes: `ginutil.Metrics`, `otelutil.MetricsServer`.
- Produces: each HTTP service exposes `rpc_server_*` on `/metrics`.

- [ ] **Step 1: Add the Gin middleware (all 4 services)**

In each service's gin setup, add `ginutil.Metrics("<service>")` to the middleware chain. For `auth-service/main.go` (lines 88-92):

```go
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(ginutil.Metrics("auth-service"))
	r.Use(ginutil.AccessLog())
	r.Use(ginutil.CORS())
```

For `portal-service`, `media-service`, `upload-service`: locate the equivalent `r.Use(ginutil.RequestID())` / `AccessLog()` block and insert `r.Use(ginutil.Metrics("<service-name>"))` (with the matching literal: `"portal-service"`, `"media-service"`, `"upload-service"`) right after `RequestID`.

- [ ] **Step 2: Add a `/metrics` listener (all 4 services)**

None of the four currently expose `/metrics`. Add the same config field + listener + shutdown as Task 5 Pattern B. Since these are HTTP services (no `natsrouter`), the shutdown ordering per CLAUDE.md is `nc.Drain()` → disconnect DBs; add the metrics-server shutdown as a distinct step in whatever graceful-shutdown mechanism the service uses.

(a) Config struct — add:
```go
	MetricsAddr string `env:"METRICS_ADDR" envDefault:":9090"`
```

(b) Listener — after building the gin engine and before/alongside the main HTTP server start:
```go
	// /metrics on a separate port so scrapes don't hit the public API listener.
	metricsServer := otelutil.MetricsServer()
	metricsLn, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		slog.Error("metrics listen failed", "addr", cfg.MetricsAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("metrics server listening", "addr", cfg.MetricsAddr)
		if err := metricsServer.Serve(metricsLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", err)
		}
	}()
```

(c) Shutdown — where the service tears down (e.g. after the main `srv.Shutdown(...)` / on the shutdown path), add:
```go
	if err := metricsServer.Shutdown(context.Background()); err != nil {
		slog.Error("metrics server shutdown failed", "error", err)
	}
```
Match the existing shutdown style of each service (some use `shutdown.Wait`, `auth-service` uses a `srvErr` channel + `srv.Shutdown`); place the metrics shutdown alongside the existing server shutdown.

(d) Imports — ensure `"net"`, `"net/http"`, `"errors"`, `"context"` (if used), and `"github.com/hmchangw/chat/pkg/otelutil"` are present.

- [ ] **Step 3: Format**

Run: `make fmt`
Expected: imports ordered.

- [ ] **Step 4: Build each HTTP service**

Run:
```bash
make build SERVICE=auth-service && \
make build SERVICE=portal-service && \
make build SERVICE=media-service && \
make build SERVICE=upload-service
```
Expected: all build OK.

- [ ] **Step 5: Unit tests**

Run:
```bash
for s in auth-service portal-service media-service upload-service; do make test SERVICE=$s || break; done
```
Expected: PASS (or "no test files" for services without unit tests — acceptable; wiring is exercised by the `pkg/ginutil` test).

- [ ] **Step 6: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add auth-service/ portal-service/ media-service/ upload-service/
git commit -m "feat(rpc-metrics): wire ginutil.Metrics into all HTTP services"
```

---

## Task 7: Full verification & docs finalize

**Files:**
- Verify only; `pkg/rpcmetrics/doc.go` already written in Task 1.

- [ ] **Step 1: Full unit test suite with race detector**

Run: `make test`
Expected: all packages PASS, no data races.

- [ ] **Step 2: Lint the whole repo**

Run: `make lint`
Expected: no findings.

- [ ] **Step 3: SAST gate**

Run: `make sast`
Expected: no medium+ findings introduced (new code adds no `InsecureSkipVerify`, unchecked conversions, or command exec).

- [ ] **Step 4: Manual /metrics smoke (one router + one HTTP service)**

Verify the series names appear. Using the search-service or room-service docker-compose (which sets `BOOTSTRAP_STREAMS=true` and `METRICS_ADDR=:9090`), start the service and:

```bash
curl -s localhost:9090/metrics | grep -E '^rpc_server_(requests_total|request_duration_seconds)'
```
Expected: both `rpc_server_requests_total` and `rpc_server_request_duration_seconds` families are listed (they appear after the first request; the `# HELP`/`# TYPE` lines appear immediately once a handler has run). If no traffic yet, drive one RPC/HTTP call first.

- [ ] **Step 5: Confirm no client-api drift**

Run: `git diff --name-only origin/main...HEAD | grep -E 'docs/client-api'`
Expected: empty (server-side change only — no client-api edit required).

- [ ] **Step 6: Final commit (if verification produced any fmt/doc touch-ups)**

```bash
git add -A
git commit -m "chore(rpc-metrics): finalize wiring and verification" || echo "nothing to commit"
```

---

## Self-review notes (coverage map)

- Spec §1 `pkg/rpcmetrics` → Task 1. §2 natsrouter middleware → Task 2. §3 ginutil + errhttp → Task 3. §4 per-service wiring → Tasks 5–6. §5 search-service migration → Task 4. Testing → per-task TDD steps + Task 7. Docs/`doc.go` → Task 1 Step 4. Non-goals (JetStream/inbox-worker) → untouched by any task.
- `route`/`status`/`Metrics` names are used consistently across Tasks 2, 3, 5, 6. `rpcmetrics.CounterValue` (Task 2 Step 3) is the single test seam reused by Tasks 2 and 3.
- Every code step shows real code; no TBD/placeholder logic (the two "reminder" lines in Tasks 2/3 are explicitly instructed to be removed).
