package natsrouter

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// HandlerFunc is the function type for handlers and middleware.
// Middleware calls c.Next() to continue the chain.
type HandlerFunc func(c *Context)

// Context carries request state through the middleware chain. It implements
// context.Context and is safe to pass anywhere a context.Context is expected,
// including consumers that retain it past the handler's return (net/http
// keep-alive cancel watchers, background goroutines, deferred async work).
//
// Most fields — Msg, Params, keys — are set once at acquire and then stable.
// The underlying ctx field may be replaced by SetContext, but only under the
// single-writer contract: middleware must call SetContext before c.Next(),
// never concurrently with handler-spawned goroutines that read c.Value.
type Context struct {
	ctx    context.Context
	Msg    *nats.Msg
	Params Params
	keys   map[string]any
	mu     sync.RWMutex

	chain *chainState
}

// chainState holds the per-request middleware-chain bookkeeping. It lives in
// a sync.Pool; nothing inside it is exposed via methods an outside goroutine
// would call (Next/Abort/IsAborted are handler-internal), so pool reuse is
// race-free w.r.t. external observers of *Context.
type chainState struct {
	handlers []HandlerFunc
	index    int
}

var chainPool = sync.Pool{
	New: func() any { return &chainState{} },
}

func acquireContext(ctx context.Context, msg *nats.Msg, params Params, handlers []HandlerFunc) *Context {
	cs := chainPool.Get().(*chainState)
	cs.handlers = handlers
	cs.index = -1
	return &Context{
		ctx:    ctx,
		Msg:    msg,
		Params: params,
		chain:  cs,
	}
}

func releaseContext(c *Context) {
	c.chain.handlers = nil
	c.chain.index = 0
	chainPool.Put(c.chain)
	// Nil out so Next/Abort/IsAborted panic loudly if a post-handler
	// goroutine calls them — otherwise it would silently read the next
	// request's chain state from the pool.
	c.chain = nil
	// c itself is left to GC. External ctx consumers may still hold it;
	// every field they can observe is stable from the moment of construction
	// (Msg, Params, keys); the underlying ctx may have been swapped by
	// SetContext during the chain but is no longer mutated once handlers return.
}

// NewContext creates a Context for testing handlers without a NATS connection.
func NewContext(params map[string]string) *Context {
	return &Context{
		ctx:    context.Background(),
		Params: NewParams(params),
		chain:  &chainState{index: -1},
	}
}

// context.Context implementation — delegates to c.ctx. Safe for async consumers
// provided no middleware calls SetContext concurrently; see SetContext doc.
func (c *Context) Deadline() (time.Time, bool) { return c.ctx.Deadline() }
func (c *Context) Done() <-chan struct{}       { return c.ctx.Done() }
func (c *Context) Err() error                  { return c.ctx.Err() }
func (c *Context) Value(key any) any           { return c.ctx.Value(key) }

// Chain methods are handler-internal. Calling them from a post-handler
// goroutine panics — chainState is pooled and would otherwise silently read
// the next request's state.
const chainAfterReleasePanic = "natsrouter: chain method called after handler chain ended; pass values out via c.Value/c.Get before returning"

// Next executes the next handler in the chain.
func (c *Context) Next() {
	if c.chain == nil {
		panic(chainAfterReleasePanic)
	}
	c.chain.index++
	for c.chain.index < len(c.chain.handlers) {
		c.chain.handlers[c.chain.index](c)
		c.chain.index++
	}
}

// Abort stops the middleware chain.
func (c *Context) Abort() {
	if c.chain == nil {
		panic(chainAfterReleasePanic)
	}
	c.chain.index = len(c.chain.handlers)
}

// IsAborted returns true if the chain was aborted.
func (c *Context) IsAborted() bool {
	if c.chain == nil {
		panic(chainAfterReleasePanic)
	}
	return c.chain.index >= len(c.chain.handlers)
}

// Set stores a key-value pair for downstream handlers.
func (c *Context) Set(key string, value any) {
	c.mu.Lock()
	if c.keys == nil {
		c.keys = make(map[string]any)
	}
	c.keys[key] = value
	c.mu.Unlock()
}

// Get returns a value by key and whether it was found.
func (c *Context) Get(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.keys == nil {
		return nil, false
	}
	val, ok := c.keys[key]
	return val, ok
}

// MustGet returns a value by key. Panics if not found.
func (c *Context) MustGet(key string) any {
	val, ok := c.Get(key)
	if !ok {
		panic("natsrouter: key " + key + " not found in context")
	}
	return val
}

// SetContext replaces the underlying context.Context.
//
// Single-writer contract: call only from middleware before c.Next() (racing
// with handler-spawned goroutines that read c.Value is unsafe — c.ctx is not
// guarded against concurrent writers).
//
// Cycle pitfall: never pass c (the *Context) as the parent of the new ctx.
// c.Value(k) delegates to c.ctx.Value(k), so a chain like
// context.WithValue(c, k, v) creates a ctx whose parent traverses back to c —
// any c.Value(otherKey) lookup then loops forever. Always derive from c.ctx
// (private to this package) or from a stable parent like context.Background().
// Example: ctx := natsutil.WithRequestID(c.ctx, reqID); c.SetContext(ctx).
func (c *Context) SetContext(ctx context.Context) {
	c.ctx = ctx
}

// WithLogValues enriches the ctx logger with key/value pairs for the errcode
// log line. Derives from c.ctx (avoids the SetContext Value-delegation cycle).
func (c *Context) WithLogValues(args ...any) {
	c.SetContext(errcode.WithLogValues(c.ctx, args...))
}

// Param returns a named parameter from the subject. Shortcut for c.Params.Get(key).
func (c *Context) Param(key string) string {
	return c.Params.Get(key)
}

// GetHeader returns the value of `key` from the inbound NATS message
// header. Returns "" if the message has no headers, no Msg at all
// (NewContext-constructed test context), or the key is absent.
// Shortcut for c.Msg.Header.Get(key); name and shape match gin's
// gin.Context.GetHeader.
//
// IMPORTANT — case sensitivity. Unlike net/http (which canonicalises
// header keys via textproto.CanonicalMIMEHeaderKey), NATS headers are
// CASE-SENSITIVE. The wire decoder preserves whatever case the sender
// used. So a sender that calls msg.Header.Set("authorization", token)
// will not be readable via GetHeader("Authorization") — those are two
// different keys to nats.go. Pick a canonical case (the project
// convention is the canonical "X-Request-ID" / "Authorization" form)
// and use it on both ends.
//
// IMPORTANT — request ID. GetHeader("X-Request-ID") returns the ID
// supplied on the INBOUND message, not necessarily the ID the router
// is using. RequestID middleware reads the inbound header and, if
// missing or invalid, mints a new ID and stores it via c.Set and on
// the underlying context. To get the ID the router actually uses,
// read c.Get("requestID") or natsutil.RequestIDFrom(c.Context()).
func (c *Context) GetHeader(key string) string {
	if c.Msg == nil {
		return ""
	}
	// nats.Header.Get already handles a nil receiver, so no second guard
	// is needed (it returns "" for nil maps).
	return c.Msg.Header.Get(key)
}

// ReplyJSON marshals v as JSON and sends it as the reply.
func (c *Context) ReplyJSON(v any) {
	// dev-only full-reply-payload capture (gated by DEBUG_LOG_PAYLOADS +
	// X-Debug-Payload); ShouldCapture guards the marshal so prod pays nothing.
	if logctx.ShouldCapture(c) {
		if b, err := json.Marshal(v); err == nil {
			logctx.CapturePayload(c, "reply", c.Msg.Subject, b)
		}
	}
	natsutil.ReplyJSON(c.Msg, v)
}
