# natsrouter

Gin-style pattern-based routing for NATS request/reply services.

Handles subject pattern matching, parameter extraction, JSON marshal/unmarshal, middleware, admission control, panic safety, and graceful shutdown — so your handlers focus on business logic.

## Quick Start

```go
nc, err := natsutil.Connect(natsURL, credsFile)
if err != nil {
    log.Fatal(err)
}
// Default pre-installs Recovery, RequestID, and Logging.
router := natsrouter.Default(nc, "my-service")
// Add HandlerTimeout explicitly — duration varies per service.
router.Use(natsrouter.HandlerTimeout(5 * time.Second))

natsrouter.Register(router, "chat.user.{account}.msg.send", svc.SendMessage)

// On shutdown:
router.Shutdown(ctx)
nc.Drain()
```

## Table of Contents

- [Installation](#installation)
- [Core Concepts](#core-concepts)
- [Concurrency Model](#concurrency-model)
- [API Reference](#api-reference)
- [Registration Functions](#registration-functions)
- [Context](#context)
- [Middleware](#middleware)
- [Error Handling](#error-handling)
- [Pattern Routing](#pattern-routing)
- [Panic Safety](#panic-safety)
- [Shutdown](#shutdown)
- [Testing](#testing)
- [Scope and Limitations](#scope-and-limitations)
- [Examples](#examples)

## Installation

```bash
go get github.com/hmchangw/chat/pkg/natsrouter
```

## Core Concepts

natsrouter is designed for **NATS request/reply** endpoints — a client sends a request to a subject and waits for a response. The router:

1. Subscribes to NATS subjects using **queue groups** (load balancing across instances)
2. Converts `{param}` patterns to NATS wildcards at registration time
3. Extracts params from incoming subjects at request time
4. Spawns each message into its own goroutine for parallel handler execution (unbounded by default; opt into a cap with `WithMaxConcurrency`)
5. Runs the **middleware chain** (Gin-style `c.Next()` / `c.Abort()`)
6. Unmarshals JSON request bodies into typed Go structs
7. Calls your handler with a `*Context` (implements `context.Context`)
8. Marshals the response back as JSON
9. Tracks every spawned handler in a `WaitGroup` so `Shutdown` can wait for in-flight work

## Concurrency Model

The router spawns one goroutine per incoming message. By default there is **no admission control** — the model is analogous to HTTP/2's per-stream goroutine model (one goroutine per request, not per connection). Backpressure flows from downstream timeouts (`HandlerTimeout` middleware, ctx-aware database drivers).

Under the unbounded default, callers that hit a timeout receive a generic `{"error":"internal error"}` reply unless the handler maps `context.DeadlineExceeded` to `ErrUnavailable` explicitly:

```go
if errors.Is(err, context.DeadlineExceeded) {
    return nil, errcode.Unavailable("request timed out")
}
```

Without that mapping there is no structured retry signal in the default path; opt into `WithMaxConcurrency(N)` if you want the router itself to emit `ErrUnavailable` on saturation.

For services that need a hard cap on in-flight handlers (memory-constrained pods, downstream pools that don't queue cleanly, etc.), opt into admission control with `WithMaxConcurrency(N)` at construction:

```go
// Opt into a concurrency cap when your service needs one.
// Tune per environment via env var:
//   ROUTER_MAX_CONCURRENCY=200
router := natsrouter.New(nc, "my-service",
    natsrouter.WithMaxConcurrency(cfg.Router.MaxConcurrency),
)
```

When admission control is enabled, a non-blocking acquire on a semaphore inside the per-subscription dispatcher gates each spawn:

- **Acquire success** → spawn a goroutine that runs the middleware chain + handler. The goroutine releases the semaphore on return.
- **Acquire failure (cap reached)** → publish an `ErrUnavailable` reply (`{"error":"service busy","code":"unavailable"}`) immediately and return. Callers should retry with exponential backoff and jitter (50–200 ms typically); queue-group routing will redistribute the retry across pods, often landing on a less-loaded one. NATS itself has no built-in retry semantics — the burden is entirely on the caller.

### Important properties

- **Per-route overrides are not supported today.** A single router-wide semaphore (when admission control is enabled) covers every route. The `Registrar` interface is intentionally minimal so a future wrapper (e.g. a route group with its own admission semaphore) can be added without breaking the existing API. Route-level isolation should wait until real evidence of noisy-neighbor contention surfaces in production.

- **Per-subject FIFO ordering is NOT preserved.** Two messages that arrive on the same subscription are spawned into independent goroutines and race; whichever wins the goroutine schedule runs first. Handlers must be idempotent or use external coordination (Cassandra LWTs, Mongo conditional updates) to ensure correctness under concurrent invocation.

- **When admission control is enabled, `HandlerTimeout` does not free a slot early.** When a handler runs past its deadline, the goroutine is NOT interrupted — it continues holding its semaphore slot until it actually returns. CPU-bound code or non-context-aware libraries will run past the deadline and starve other requests of admission. Either propagate `ctx` into every blocking call so the timeout takes effect, or size `MaxConcurrency` accordingly.

- **When admission control is enabled, fire-and-forget routes drop silently under saturation.** `RegisterVoid` handlers are designed for `nc.Publish` callers (no Reply subject). When a fire-and-forget message arrives while the semaphore is saturated, the router has no reply channel on which to publish `ErrUnavailable`, so the message is dropped with a `slog.Warn` log line keyed by subject. Size `MaxConcurrency` conservatively for services that expose `RegisterVoid` endpoints, or front them with JetStream so dropped messages can be redelivered. *Note:* if a caller publishes to a `RegisterVoid` route via `nc.Request` or `nc.PublishRequest` (supplying a reply subject), the saturated router DOES reply with `ErrUnavailable` to that subject — though under normal load the void handler still never replies, so the caller times out.

- **When admission control is enabled, NATS queue-group fairness shifts under saturation.** NATS queue-group routing distributes messages among subscribers without knowing whether any individual subscriber's process-level admission control is full. A saturated pod will continue to receive (and busy-reply) its share of messages even while other pods sit idle. Monitor the per-pod busy-reply rate as a real SLI rather than assume queue-group routing alone provides load balancing.

## API Reference

### Router

```go
// Create a router with a NATS connection and queue group name.
// Queue group ensures only one instance handles each request (load balancing).
//
// Variadic options preserve backward compatibility for existing callers.
func New(nc *otelnats.Conn, queue string, opts ...Option) *Router

// Append middleware to the router's chain. Runs for ALL routes.
func (r *Router) Use(mw ...HandlerFunc)

// Drain every registered route, wait for dispatcher goroutines to exit
// (SubscriptionClosed), then wait for spawned handler goroutines to
// finish. Returns when all in-flight handlers have completed or ctx
// expires (whichever comes first). Idempotent. Call before nc.Drain()
// if you need to stop routing while keeping the NATS connection open.
func (r *Router) Shutdown(ctx context.Context) error
```

### Default Constructor

```go
// Default returns a Router pre-configured with the recommended middleware
// stack — Recovery, RequestID, Logging — mirroring gin.Default(). Forwards
// opts to New(). HandlerTimeout is intentionally omitted; add it
// explicitly with r.Use(HandlerTimeout(d)) if your service needs one.
func Default(nc *otelnats.Conn, queue string, opts ...Option) *Router
```

### Options

```go
// Option configures a Router on construction.
type Option func(*Router)

// WithMaxConcurrency sets the maximum number of in-flight handler
// invocations across all routes registered on this router. By default,
// no admission control is applied (unbounded spawn). Calling
// WithMaxConcurrency installs a semaphore of capacity n.
//
// Non-positive values are ignored AND emit a slog.Warn — a misconfigured
// deployment (e.g. ROUTER_MAX_CONCURRENCY=0 from an unset env var) won't
// silently disable admission control without a trace. Once a semaphore
// is installed, a subsequent WithMaxConcurrency(0) does NOT remove it.
//
// If multiple WithMaxConcurrency options are supplied, the last call
// with a positive n takes effect. Saturation triggers a 503-style
// ErrUnavailable reply.
func WithMaxConcurrency(n int) Option
```

### Registration Functions

All accept a `Registrar` (currently `*Router`). They are free functions, not `*Router` methods, because Go's type-parameter rules (as of Go 1.22) do not permit type parameters on methods. `Register[Req, Resp]`'s typed handlers can only live on a free function.

```go
// Request body + JSON response. The standard request/reply handler.
func Register[Req, Resp any](
    r Registrar,
    pattern string,
    fn func(c *Context, req Req) (*Resp, error),
)

// No request body, JSON response. For GET-style lookups where all data is in the subject.
func RegisterNoBody[Resp any](
    r Registrar,
    pattern string,
    fn func(c *Context) (*Resp, error),
)

// Request body, no response. For fire-and-forget events.
// CAUTION: messages on saturated routers are silently dropped (see Concurrency Model).
func RegisterVoid[Req any](
    r Registrar,
    pattern string,
    fn func(c *Context, req Req) error,
)
```

All three **panic** if the NATS subscription fails. This is intentional — registration happens at startup, and a failed subscription means the service cannot function (same pattern as `http.HandleFunc`).

### Context

`*Context` implements `context.Context` and can be passed directly to database calls.

```go
// Named parameter from the subject. Shortcut for c.Params.Get(key).
func (c *Context) Param(key string) string

// Inbound NATS message header value. Shortcut for c.Msg.Header.Get(key);
// name and shape match gin.Context.GetHeader. Returns "" for missing keys
// or for test-constructed contexts (NewContext) where Msg is nil.
//
// IMPORTANT: NATS headers are CASE-SENSITIVE (unlike net/http). Pick a
// canonical case and use it consistently on both ends.
//
// IMPORTANT: GetHeader("X-Request-ID") returns the inbound header, not
// the ID assigned by the RequestID middleware. Use c.Get("requestID")
// or natsutil.RequestIDFrom(c.Context()) for the router-assigned ID.
func (c *Context) GetHeader(key string) string

// Key-value store for middleware-to-handler data passing.
func (c *Context) Set(key string, value any)
func (c *Context) Get(key string) (any, bool)
func (c *Context) MustGet(key string) any

// Replace the underlying context.Context. Used by HandlerTimeout to
// install a deadline. Single-writer contract: call only from middleware
// before c.Next(); never concurrently with handler-spawned goroutines.
func (c *Context) SetContext(ctx context.Context)

// Continue the middleware chain. Must be called from middleware to proceed.
func (c *Context) Next()

// Stop the middleware chain. Subsequent handlers will not run.
func (c *Context) Abort()

// Check if the chain was aborted.
func (c *Context) IsAborted() bool

// Reply helpers. For errors prefer returning a typed *errcode.Error from
// the handler; the router calls ReplyError automatically and replies through
// the traced o11y/nats responder when available.
func (c *Context) ReplyJSON(v any)
func (c *Context) ReplyError(err error)
func (c *Context) ReplyErrorQuiet(err error)

// WithLogValues enriches the ctx logger so the centralized errcode.Classify
// log line carries the given attrs. Cycle-safe (derives from the inner ctx).
func (c *Context) WithLogValues(args ...any)

// The raw NATS message (for advanced use cases).
c.Msg *nats.Msg

// The full Params struct (for iteration or Require).
c.Params Params
```

### Params

```go
// Get a param value by name. Returns "" if not found.
func (p Params) Get(key string) string

// Get a param value. Panics if not found (developer error — pattern mismatch).
func (p Params) MustGet(key string) string

// Get a param value. Returns a user-facing error if missing or empty.
func (p Params) Require(key string) (string, error)

// Create params from a map (for testing).
func NewParams(values map[string]string) Params
```

### HandlerFunc

```go
// The universal function type for handlers and middleware.
type HandlerFunc func(c *Context)

// Middleware is a type alias for HandlerFunc (for documentation clarity).
type Middleware = HandlerFunc
```

### Error replies — owned by `pkg/errcode`

Client-facing errors live in `pkg/errcode` (not in this package). natsrouter is the transport: when a handler returns any error, the router invokes `Context.ReplyError(err)`, which calls `errcode.Classify` and writes the JSON envelope through the traced responder. The full developer guide is `docs/error-handling.md`; the wire-side reference is `docs/client-api.md` §6.

Quick reference for handler authors:

```go
// Typed client-facing errors — named constructor per category.
return nil, errcode.BadRequest("name is required")
return nil, errcode.NotFound("room not found")
return nil, errcode.Forbidden("only owners can update roles")
return nil, errcode.Conflict("room is at maximum capacity",
    errcode.WithReason(errcode.RoomMaxSizeReached))

// Dynamic message — format at the call site (no *f variants on purpose).
return nil, errcode.BadRequest(fmt.Sprintf("batch size %d exceeds limit %d", n, max))

// Infra / DB / third-party — DON'T classify manually; bubble up and let
// Classify collapse to internal at the boundary (real cause logged once,
// never sent to the client).
if err := h.store.Find(ctx, id); err != nil {
    return nil, fmt.Errorf("loading room: %w", err) // → client sees "internal error"
}
```

`errcode.Unavailable("service busy")` is also what the router emits automatically when the admission semaphore is saturated. Application code can emit it explicitly to signal a recoverable condition (e.g. mapping `context.DeadlineExceeded` from a downstream call — see `HandlerTimeout`).

### Built-in Middleware

```go
// Generates or extracts a request ID (from X-Request-ID header or new UUID).
// Stores it via c.Set("requestID", id). Recovery and Logging include it automatically.
func RequestID() HandlerFunc

// Catches panics, logs them at slog.Error with request ID + subject, replies
// with `{"error":"internal error"}`. Recommended even though the router has
// its own spawn-site panic backstop — Recovery's value over the backstop is
// richer logging (request ID, subject) and louder severity (Error vs Warn);
// the reply payload itself is identical.
func Recovery() HandlerFunc

// Logs subject, duration, and request ID for each request.
func Logging() HandlerFunc

// Wraps the handler context with a deadline of d. Downstream calls that
// respect context (Cassandra/Mongo drivers, otelnats.Conn.Publish, etc.)
// will abort if the chain runs longer than d. The deadline is released
// when the chain returns. Place AFTER RequestID so the request ID is
// set before any timeout-related downstream work runs. Position
// relative to Logging does not affect what Logging records: Logging
// measures via time.Since(start) in its post-c.Next() phase, which
// captures the full chain duration regardless of where HandlerTimeout
// sits.
//
// Caveat — does NOT actively interrupt non-context-aware handlers; CPU-
// bound code will run past the deadline. Recommended pattern when a
// downstream call returns context.DeadlineExceeded:
//   if errors.Is(err, context.DeadlineExceeded) {
//       return nil, errcode.Unavailable("request timed out")
//   }
func HandlerTimeout(d time.Duration) HandlerFunc
```

### NewContext (Testing)

```go
// Create a Context for testing handlers without a live NATS connection.
func NewContext(params map[string]string) *Context
```

## Registration Functions

Three handler shapes for three use cases:

| Function | Request Body | Response | Use Case |
|----------|-------------|----------|----------|
| `Register[Req, Resp]` | Yes | Yes | Standard request/reply (most endpoints) |
| `RegisterNoBody[Resp]` | No | Yes | GET-style lookups where subject has all info |
| `RegisterVoid[Req]` | Yes | No | Fire-and-forget events (under `WithMaxConcurrency` saturation: dropped with a Warn log; under unbounded default: always spawns) |

```go
// Request/reply — the most common pattern.
natsrouter.Register(router, "chat.user.{account}.msg.send",
    func(c *natsrouter.Context, req SendRequest) (*SendResponse, error) {
        account := c.Param("account")
        // ... business logic ...
        return &SendResponse{ID: msg.ID}, nil
    })

// GET-style — no request body needed.
natsrouter.RegisterNoBody(router, "chat.user.{account}.rooms.get.{roomID}",
    func(c *natsrouter.Context) (*Room, error) {
        return store.FindRoom(c, c.Param("roomID"))
    })

// Fire-and-forget — no response sent. Dropped with a Warn log on saturation.
natsrouter.RegisterVoid(router, "chat.user.{account}.event.typing",
    func(c *natsrouter.Context, req TypingEvent) error {
        return broadcast(c, c.Param("account"), req)
    })
```

## Context

Every handler receives `*Context`, which implements `context.Context`. You can pass it directly to database calls:

```go
func (s *Service) GetRoom(c *natsrouter.Context, req GetRoomReq) (*Room, error) {
    // c is a context.Context — pass it to DB calls for deadline/cancellation.
    room, err := s.store.FindByID(c, req.ID)
    if err != nil {
        return nil, fmt.Errorf("finding room: %w", err)
    }
    if room == nil {
        return nil, errcode.NotFound("room not found")
    }
    return room, nil
}
```

If `HandlerTimeout` middleware is installed, `c` carries the timeout deadline and downstream context-aware calls abort cleanly when it expires.

### Using the Context in Goroutines

Pass `c` (or any descendant `context.Context` derived via `context.WithTimeout` etc.) freely to anything that expects a `context.Context` — `http.NewRequestWithContext`, database drivers, async goroutines. The pool only recycles the middleware chain-state, and `*Context` itself is allocated fresh per request (never pooled), so its `Msg`, `Params`, and `keys` fields are stable for the lifetime of `*Context` regardless of whether the handler has returned.

`c.ctx` is mutable but only inside the chain: middleware that calls `SetContext` does so before `c.Next()`, never concurrently with handler-spawned goroutines. Once the handler body executes, `c.ctx` is stable from that goroutine's perspective. The one exception is `HandlerTimeout`'s `defer cancel()` — it fires after the chain returns and cancels the timeout-derived ctx that was installed via `SetContext`. A goroutine retaining `*Context` past handler return that reads `c.Done()` or `c.Err()` will see a cancelled ctx (`DeadlineExceeded` or `Canceled`). This matches `gin.Context.Request.Context()` semantics. `c.Param`, `c.Get`, `c.Set` go through the `Params` / `keys` map — not `c.ctx` — and remain valid indefinitely.

```go
func MetricsMiddleware() natsrouter.HandlerFunc {
    return func(c *natsrouter.Context) {
        start := time.Now()
        c.Next()
        go func() {
            emitMetric(c.Msg.Subject, time.Since(start), c.Param("account"))
        }()
    }
}
```

Rule of thumb: treat `*Context` exactly like a `*http.Request` — safe to pass to downstream ctx consumers, safe to capture in goroutines. Do not call `Respond` on `c.Msg` from a background goroutine once the handler has already replied; publish to a fresh subject instead.

### Storing data in middleware

Prefer `c.Set(key, value)` / `c.Get(key)` over wrapping the underlying context. The keys map is what natsrouter is built around and is safe to use from any middleware:

```go
func AuthMiddleware() natsrouter.HandlerFunc {
    return func(c *natsrouter.Context) {
        user := authenticate(c.Msg.Header.Get("Authorization"))
        c.Set("user", user)
        c.Next()
    }
}
```

If you specifically need to enrich the underlying `context.Context` (for downstream context-aware libraries that don't know about `c.Set`), call `SetContext` BEFORE `c.Next()` — but **never pass `c` itself** as the parent of the new context:

```go
// WRONG — c.Value(k) delegates to c.ctx.Value(k); passing c as the parent
// creates a cycle in which any later c.Value(other) lookup loops forever.
ctx := context.WithValue(c, myKey{}, value) // ⚠ broken
c.SetContext(ctx)
```

The package's underlying `context.Context` field is unexported, so external middleware cannot derive from it directly. For external middleware that genuinely needs `context.WithValue` (e.g. propagating a span to a third-party library), derive from `context.Background()` and accept the trade-off (you lose any upstream-context values not migrated explicitly), or contribute a `Context.UnwrapContext()` accessor upstream.

For most use cases, `c.Set` / `c.Get` is the intended mechanism and avoids the cycle entirely.

### Middleware Data Passing

Middleware can store values that handlers read:

```go
// Auth middleware stores the user.
func AuthMiddleware() natsrouter.HandlerFunc {
    return func(c *natsrouter.Context) {
        token := c.Msg.Header.Get("Authorization")
        user, err := validateToken(token)
        if err != nil {
            c.ReplyError(errcode.Forbidden("unauthorized"))
            c.Abort()
            return
        }
        c.Set("user", user)
        c.Next()
    }
}

// Handler reads it.
func (s *Service) CreateRoom(c *natsrouter.Context, req CreateReq) (*Room, error) {
    user := c.MustGet("user").(User)
    // ...
}
```

`c.MustGet` panics if the key is absent and the bare `.(User)` assertion panics if the value has the wrong type. Both panics will be caught by `Recovery` middleware (or the spawn-site backstop), but the request still fails with `internal error`. This pattern is fine when you fully control middleware ordering for a route — every handler that calls `MustGet("user")` must be registered behind `AuthMiddleware`. If your registration is split across files or handlers may be registered without the middleware, prefer the explicit form:

```go
v, ok := c.Get("user")
if !ok {
    return nil, errcode.Forbidden("authentication required")
}
user, ok := v.(User)
if !ok {
    return nil, errcode.Internal("user value has unexpected type")
}
```

## Middleware

Middleware is a `HandlerFunc` that calls `c.Next()` to continue the chain:

```go
func RequestIDMiddleware() natsrouter.HandlerFunc {
    return func(c *natsrouter.Context) {
        reqID := c.Msg.Header.Get("X-Request-ID")
        if reqID == "" {
            reqID = uuid.New().String()
        }
        c.Set("requestID", reqID)
        c.Next()
    }
}
```

### Recommended Ordering

Two equivalent setups, depending on whether you start from `Default()` or `New()`:

```go
// (a) Using Default — recommended. Simplest one-liner; HandlerTimeout
// goes after Logging in the chain, but Logging still records the full
// duration (Logging measures via time.Since(start) in its post-c.Next()
// phase, regardless of position).
router := natsrouter.Default(nc, "my-service")
router.Use(natsrouter.HandlerTimeout(5 * time.Second))
// Resulting chain: Recovery → RequestID → Logging → HandlerTimeout

// (b) Using New + explicit Use — useful if you want HandlerTimeout
// strictly before Logging (e.g., a custom Logging middleware that does
// NOT measure in its post-c.Next() phase).
router := natsrouter.New(nc, "my-service")
router.Use(natsrouter.Recovery())
router.Use(natsrouter.RequestID())
router.Use(natsrouter.HandlerTimeout(5 * time.Second))
router.Use(natsrouter.Logging())
// custom middleware (auth, rate-limit, …) goes between RequestID and Logging
```

Both produce the same observable behavior with the built-in `Logging` middleware. Pick (a) for brevity; pick (b) if you've replaced `Logging` with a variant that records timing in its pre-`c.Next()` phase.

### Execution Order

```text
router.Use(A, B, C) + handler

A:before → B:before → C:before → handler → C:after → B:after → A:after
```

### Short-Circuiting

Don't call `c.Next()` (and optionally call `c.Abort()`) to stop the chain:

```go
func RateLimiter() natsrouter.HandlerFunc {
    return func(c *natsrouter.Context) {
        if isRateLimited(c.Param("account")) {
            c.ReplyError(errcode.Unavailable("rate limited"))
            c.Abort()
            return
        }
        c.Next()
    }
}
```

## Error Handling

Handlers return Go errors. The router distinguishes user-facing from internal:

```go
func (s *Service) GetRoom(c *natsrouter.Context, req GetReq) (*Room, error) {
    room, err := s.store.Find(c, req.ID)
    if err != nil {
        // Internal error — logged, client sees: {"error":"internal error"}
        return nil, fmt.Errorf("finding room: %w", err)
    }
    if room == nil {
        // User-facing error — client sees: {"error":"room not found","code":"not_found"}
        return nil, errcode.NotFound("room not found")
    }
    return room, nil
}
```

RouteErrors can be wrapped and still detected:

```go
return nil, fmt.Errorf("access check: %w", errcode.Forbidden("denied"))
// Client still receives: {"error":"denied","code":"forbidden"}
```

### Mapping Timeouts and Cancellation

When `HandlerTimeout` (or any other context source) cancels a request mid-flight, downstream calls bubble up `context.DeadlineExceeded`. The router's default error path returns a generic `internal error`; for a structured signal callers can act on, map explicitly:

```go
if errors.Is(err, context.DeadlineExceeded) {
    return nil, errcode.Unavailable("request timed out")
}
```

## Pattern Routing

Patterns use `{name}` placeholders that convert to NATS single-token wildcards (`*`):

```text
Pattern:  "chat.user.{account}.request.room.{roomID}.{siteID}.msg.history"
NATS:     "chat.user.*.request.room.*.*.msg.history"
Params:   {account: pos 2, roomID: pos 5, siteID: pos 6}
```

At request time, the incoming subject is split by `.` and param values are extracted by position.

### Limitations

- The NATS multi-token wildcard `>` is **not** supported. Patterns must consist entirely of literal tokens and `{name}` single-token parameters. A pattern containing `>` will be subscribed to NATS as the literal string `>` token, and no real subject will match.
- Each `{name}` placeholder consumes exactly one subject token. A multi-token tail must be modelled as multiple named parameters (`{a}.{b}.{c}`) or split into separate routes.

## Panic Safety

The router installs a process-safety backstop in every spawned handler goroutine: an unrecovered panic is caught at the spawn site, logged with stack trace, and (if the message has a Reply subject) replied to with `"internal error"`. This guarantees the process cannot be crashed by a single bad handler regardless of middleware configuration.

`Recovery()` middleware is still the recommended path because it produces richer logs — keyed by request ID and subject — and uses `slog.Error` severity so panics page operators in the same way as other errors. The reply payload from `Recovery()` and from the spawn-site backstop is identical (`{"error":"internal error"}`); only the log line differs. The backstop fires only when `Recovery` is absent or somehow bypassed, and logs at `Warn` (not `Error`) since the visible symptom is "operator should fix the middleware setup," not a production incident.

**Edge case to be aware of:** if `Recovery` is registered AFTER another middleware (e.g. `RequestID`) and that earlier middleware panics, the panic skips Recovery and falls through to the spawn-site backstop. The reply still says `"internal error"` but the log line carries no request ID (since RequestID never completed). Register Recovery FIRST in the middleware chain to avoid this gap.

## Shutdown

`Router.Shutdown(ctx)` orchestrates a graceful stop:

1. **Initiate drain on each subscription.** `Subscription.Drain()` is non-blocking — it signals the NATS server to stop routing new messages to this subscription and starts a background drain goroutine inside nats.go. New messages stop arriving immediately; pending messages in the subscription's internal queue continue to be delivered to the dispatcher.
2. **Wait for `SubscriptionClosed`.** This is the actual blocking step — it fires after the per-subscription dispatcher loop has fully exited, meaning every callback that was ever going to run has already returned and no more handler goroutines will be spawned. (Implementation note: nats.go uses a non-blocking send on the status channel, then closes the channel unconditionally; the router synchronizes on channel close, not on guaranteed value delivery.)
3. **Wait on the WaitGroup.** Block until every spawned handler goroutine returns — bounded by `ctx`. If `ctx` expires mid-drain, the function still falls through to step 3 (a labeled break, not an early return) so in-flight handlers are not abandoned.

The function returns `nil` on full graceful shutdown. If `ctx` expired, the returned error is an `errors.Join` of at most two entries: one for the first subscription whose close-wait timed out (the loop breaks at that point and does not annotate the remaining subscriptions individually) and optionally one for the WaitGroup wait. `errors.Is(err, context.DeadlineExceeded)` works on the joined error.

```go
ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
defer cancel()
if err := router.Shutdown(ctx); err != nil {
    slog.Error("router shutdown", "error", err)
}
nc.Drain() // close the NATS connection after the router has stopped routing
```

Place `Shutdown` before `nc.Drain()` in the shutdown chain — the router needs the connection to publish in-flight replies. (`router.Shutdown` drains only this router's subscriptions while leaving the connection open for other work; `nc.Drain` drains all remaining subscriptions and pending publishes and then closes the connection.)

## Testing

Use `NewContext` to create a `*Context` for unit testing handlers without a NATS connection:

```go
func TestLoadHistory(t *testing.T) {
    svc := service.New(mockMsgs, mockSubs)
    c := natsrouter.NewContext(map[string]string{
        "account": "alice",
        "roomID":  "room-1",
    })

    resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{RoomID: "room-1"})
    require.NoError(t, err)
    assert.Len(t, resp.Messages, 5)
}
```

Integration tests against a real NATS server live in `integration_test.go` (build tag `integration`) and use `testcontainers-go` to spin up a NATS container. The repo's Makefile is the canonical entry point:

```bash
make test-integration SERVICE=pkg/natsrouter
```

(or directly: `go test -tags=integration -race ./pkg/natsrouter/...` — same effect, but the Makefile is the supported path.)

## Scope and Limitations

natsrouter is designed for **NATS request/reply** (core NATS `QueueSubscribe`). It is **not** for:

| Pattern | What to Use Instead |
|---------|-------------------|
| JetStream stream consumers (subscribe, process, ack) | Manual consumer pattern or future `pkg/natsworker` |
| Subscribe-and-publish (consume from one subject, publish to another) | JetStream consumer with explicit publish |
| Streaming / long-lived subscriptions | Direct NATS subscription |
| WebSocket / SSE push to clients | HTTP framework |

### Why not JetStream consumers?

JetStream consumers have fundamentally different semantics:
- **Acknowledgment** — messages must be acked/nacked (natsrouter has no concept of this; replies are the acknowledgment)
- **Redelivery** — failed messages are redelivered automatically (natsrouter relies on caller retry on `ErrUnavailable`)
- **Durability** — consumer state persists across restarts (natsrouter is stateless)
- **Ordering** — stream ordering is guaranteed (natsrouter explicitly does not preserve per-subject ordering — see Concurrency Model)

These concerns don't belong in a request/reply router. If you need a typed handler framework for JetStream consumers, consider a separate `pkg/natsworker` package that shares concepts (Context, middleware) but has JetStream-specific semantics.

## Examples

See `example_test.go` for runnable examples:
- `Example_basicUsage` — register a handler with params
- `Example_withMiddleware` — canonical `Default()` + `HandlerTimeout` setup
- `Example_noBodyHandler` — GET-style endpoint
- `Example_errorHandling` — user-facing vs internal errors
- `Example_fireAndForget` — RegisterVoid for events
- `Example_customMiddleware` — write your own middleware

See `history-service/internal/service/` for a production usage example.
