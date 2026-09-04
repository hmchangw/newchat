package natsrouter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errnats"
	"github.com/hmchangw/chat/pkg/natsmetrics"
)

// Router manages NATS subscriptions with pattern-based routing and middleware.
type Router struct {
	nc         *o11ynats.Conn
	queue      string
	middleware []HandlerFunc
	// siteID labels client-facing requests whose subject pins the site to a
	// static token, leaving no {siteID} param. Configured by WithSiteID.
	siteID  string
	metrics natsmetrics.Publisher
	reply   responder

	// sem gates handler concurrency: every handler invocation acquires a
	// slot before running and releases it on return. cap(sem) is the
	// per-pod concurrency ceiling. Configured by WithMaxConcurrency.
	sem chan struct{}
	// wg tracks in-flight handler goroutines so Shutdown can wait for
	// them to finish.
	wg sync.WaitGroup
	// stopping gates the dispatch path; set by Shutdown before any drain
	// step. Late callbacks (subscription mid-drain, NATS internal buffer)
	// hit replyBusy instead of starting new handler goroutines that
	// would race teardown of caller-owned dependencies.
	stopping atomic.Bool

	mu   sync.Mutex
	subs []*nats.Subscription
	// methods maps each claimed rpc.method to the pattern that claimed it, so a
	// second route naming the same method is rejected at registration instead of
	// silently merging two routes into one series.
	methods map[natsmetrics.RPCMethod]string
}

// Option configures a Router on construction.
type Option func(*Router)

// WithMaxConcurrency sets the maximum number of in-flight handler
// invocations across all routes registered on this router. By default,
// no admission control is applied (unbounded spawn). Calling
// WithMaxConcurrency installs a semaphore of capacity n.
//
// Non-positive values are ignored and emit a Warn log so a
// misconfigured deployment (e.g. an env var defaulting to 0) doesn't
// silently disable admission control without trace. Once a semaphore
// is installed, a subsequent WithMaxConcurrency(0) does NOT remove it;
// non-positive values are unconditionally no-ops.
//
// If multiple WithMaxConcurrency options are supplied, the last call
// with a positive n takes effect. Saturation triggers a 503-style
// ErrUnavailable reply.
func WithMaxConcurrency(n int) Option {
	return func(r *Router) {
		if n > 0 {
			r.sem = make(chan struct{}, n)
			return
		}
		slog.Warn("natsrouter: WithMaxConcurrency ignored non-positive value; router remains in current admission state",
			"n", n,
			"sem_currently_installed", r.sem != nil)
	}
}

// WithSiteID supplies the service's own site for telemetry identity. A
// client-facing route whose subject carries a static site token has no
// {siteID} param to label the request with, and the caller's baggage is not
// trusted there — this is the trustworthy source for that value. Internal
// routes are unaffected: they keep the originating site propagated by the
// upstream hop, which for a federated event is not this site.
func WithSiteID(siteID string) Option {
	return func(r *Router) { r.siteID = siteID }
}

// WithMetrics enables bounded request/reply and response-publish metrics at
// the router boundary. It is opt-in so existing routers retain their current
// behavior until a service supplies its telemetry identity.
func WithMetrics(metrics natsmetrics.Publisher) Option {
	return func(r *Router) { r.metrics = metrics }
}

// New creates a Router with the given NATS connection and queue group.
// By default, the router spawns handlers unboundedly (no admission
// control). Use WithMaxConcurrency to opt into a concurrency cap.
func New(nc *o11ynats.Conn, queue string, opts ...Option) *Router {
	r := &Router{
		nc:      nc,
		queue:   queue,
		methods: map[natsmetrics.RPCMethod]string{},
	}
	for _, opt := range opts {
		opt(r)
	}
	r.reply = routerResponder{nc: nc, metrics: r.metrics}
	return r
}

type routerResponder struct {
	nc      *o11ynats.Conn
	metrics natsmetrics.Publisher
}

func (r routerResponder) Respond(ctx context.Context, msg *nats.Msg, data []byte) error {
	err := r.nc.Respond(ctx, msg, data)
	r.metrics.Failure(ctx, natsmetrics.DestinationClientResponse, natsmetrics.OperationClientResponse, err)
	return err
}

// Default returns a Router pre-configured with the recommended middleware
// stack: Recovery, RequestID, Logging — mirroring gin.Default()'s shape.
// Equivalent to:
//
//	r := New(nc, queue, opts...)
//	r.Use(Recovery(), RequestID(), Logging())
//
// Recovery is registered first (outermost) so it catches panics from
// RequestID and Logging themselves, not just from the handler. This
// differs from gin.Default(), which places Logger() first; gin's order
// is fine for HTTP because gin's Logger doesn't panic, but our
// outermost-Recovery posture is strictly stricter.
//
// HandlerTimeout is intentionally omitted because the deadline duration
// varies per service. Add it explicitly via r.Use(HandlerTimeout(d)).
// Logging measures via time.Since(start) in its post-c.Next() phase, so
// the logged duration captures the full chain regardless of where
// HandlerTimeout sits relative to Logging in the chain.
func Default(nc *o11ynats.Conn, queue string, opts ...Option) *Router {
	r := New(nc, queue, opts...)
	r.Use(Recovery(), RequestID(), Logging())
	return r
}

// replyBusy publishes an ErrUnavailable reply on m.Reply, used when the
// router's admission control rejects a message. For request/reply
// messages the caller observes the busy code and can retry. For
// fire-and-forget messages (empty Reply subject — typically
// RegisterVoid routes) the message is silently dropped at this point;
// we emit a Warn log so operators can correlate drops with the
// busy-reply rate.
func (r *Router) replyBusy(ctx context.Context, msg *nats.Msg) {
	if msg.Reply == "" {
		slog.Warn("natsrouter: dropped fire-and-forget message under saturation",
			"subject", msg.Subject)
		return
	}
	// Admission rejection is operational, not a request failure; ReplyQuiet skips Classify.
	if err := r.reply.Respond(ctx, msg, errnats.MarshalQuiet(errcode.Unavailable("service busy"))); err != nil {
		slog.ErrorContext(ctx, "error reply failed", "error", err, "subject", msg.Subject)
	}
}

// admit attempts to acquire an admission slot. The returned release
// function MUST be called exactly once per successful admit (typically
// via defer in the spawned handler goroutine).
//
// When admission control is disabled (r.sem == nil), admit always
// succeeds and release is a no-op. When admission control is enabled,
// admit performs a non-blocking semaphore acquire; on failure it
// returns admitted=false and a NIL release (caller must NOT call it).
//
// Why nil instead of a no-op closure on the rejected path: callers that
// blindly `defer release()` before the admitted-check would, with a
// no-op release, silently double-free the slot on the success path
// (release would fire twice — once via the misplaced defer and once
// via the correct one). Returning nil makes that mistake panic
// immediately, surfacing the misuse loudly.
//
// Pairing acquire+release in a single helper guarantees the cleanup
// defer is registered even on the unbounded path, eliminating the
// dual-`if sem != nil` pattern that previously coupled the acquire and
// release sites at a distance.
func (r *Router) admit() (admitted bool, release func()) {
	if r.sem == nil {
		return true, func() {}
	}
	select {
	case r.sem <- struct{}{}:
		return true, func() { <-r.sem }
	default:
		return false, nil
	}
}

// Use appends middleware to the router's chain.
func (r *Router) Use(mw ...HandlerFunc) {
	r.middleware = append(r.middleware, mw...)
}

// addRPCRoute registers a reply-bearing route under a declared, router-unique
// method. Both checks panic: a route is registered once at startup, and a
// service whose telemetry is mislabelled should fail to start rather than run
// blind.
func (r *Router) addRPCRoute(pattern string, method natsmetrics.RPCMethod, handlers []HandlerFunc) {
	if !method.Valid() {
		panic(fmt.Sprintf("natsrouter: route %s declares rpc method %q, which is not in the natsmetrics vocabulary", pattern, method))
	}
	r.mu.Lock()
	if claimed, dup := r.methods[method]; dup {
		r.mu.Unlock()
		panic(fmt.Sprintf("natsrouter: rpc method %q is already registered by %s; two routes sharing a method merge into one series", method, claimed))
	}
	if r.methods == nil {
		r.methods = map[natsmetrics.RPCMethod]string{}
	}
	r.methods[method] = pattern
	r.mu.Unlock()

	r.addRoute(pattern, method, true, handlers)
}

// addVoidRoute registers a fire-and-forget route. It has no reply subject, so
// there is no call to time and no method to name.
func (r *Router) addVoidRoute(pattern string, handlers []HandlerFunc) {
	r.addRoute(pattern, "", false, handlers)
}

// recordHandled reports one handler result, or nothing at all for a
// fire-and-forget route. Routed through one helper so the three record sites
// (stopping gate, admission rejection, handler completion) cannot disagree
// about whether a void route is recorded.
func (r *Router) recordHandled(ctx context.Context, rt route, started time.Time, result natsmetrics.RequestResult) {
	if !rt.recordRPC {
		return
	}
	r.metrics.HandledRequest(ctx, rt.method, time.Since(started), result)
}

func (r *Router) addRoute(pattern string, method natsmetrics.RPCMethod, recordRPC bool, handlers []HandlerFunc) {
	rt := parsePattern(pattern)
	rt.method = method
	rt.recordRPC = recordRPC
	all := make([]HandlerFunc, 0, len(r.middleware)+1+len(handlers))
	all = append(all, r.middleware...)
	// Identity enrichment is router plumbing, not an opt-in middleware. Keep it
	// immediately before the typed handler so every New/Default call path is
	// covered while Recovery, RequestID, Logging, and HandlerTimeout can wrap it.
	all = append(all, traceIdentity(r.siteID))
	all = append(all, handlers...)

	natsHandler := func(msgCtx context.Context, m *nats.Msg) {
		started := time.Now()
		// Stopping gate: reject before admit so Shutdown's contract holds
		// even if a callback fires mid-drain or after Shutdown's ctx expired.
		if r.stopping.Load() {
			r.replyBusy(msgCtx, m)
			r.recordHandled(msgCtx, rt, started, natsmetrics.RequestUnavailable)
			return
		}
		admitted, release := r.admit()
		if !admitted {
			r.replyBusy(msgCtx, m)
			r.recordHandled(msgCtx, rt, started, natsmetrics.RequestUnavailable)
			return
		}
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer release()
			// Process-safety backstop: catch any panic that bypassed
			// user-installed Recovery middleware. Recovery middleware (when
			// configured via r.Use) catches first and sends a structured
			// reply; this defer only fires if Recovery is absent or if a
			// panic somehow escapes it. Either way, the process survives
			// and the deferred semaphore/WG cleanup (registered earlier in
			// this goroutine; runs after this defer in LIFO order) still fires.
			c := acquireContext(msgCtx, m, rt.extractParams(m.Subject), all, r.reply)
			defer releaseContext(c)
			defer func() {
				r.recordHandled(msgCtx, rt, started, c.requestResult)
			}()
			defer func() {
				if rec := recover(); rec != nil {
					c.requestResult = natsmetrics.RequestInternal
					// Warn, not Error: a hit here means Recovery middleware
					// is misconfigured or absent (Recovery would have caught
					// it earlier and produced a structured ErrInternal
					// reply). The process survived, so the severity matches
					// "operator should fix the middleware setup", not
					// "production incident".
					slog.Warn("natsrouter: panic in handler caught by spawn backstop",
						"subject", m.Subject,
						"panic", rec,
						"stack", string(debug.Stack()))
					if m.Reply != "" {
						// Already logged via the Warn above; ReplyQuiet avoids a second line.
						if err := r.reply.Respond(msgCtx, m, errnats.MarshalQuiet(errcode.Internal("internal error"))); err != nil {
							slog.ErrorContext(msgCtx, "error reply failed", "error", err, "subject", m.Subject)
						}
					}
				}
			}()
			c.Next()
		}()
	}

	// context.Background is intentional: o11y/nats QueueSubscribe consults ctx
	// only as a registration-time guard (an already-cancelled ctx is rejected)
	// and never plumbs it into delivery. Subscription lifetime is managed by
	// Shutdown via Subscription.Drain, not ctx. Per-message trace context flows
	// from the inbound headers into the handler's ctx argument.
	sub, err := r.nc.QueueSubscribe(context.Background(), rt.natsSubject, r.queue, natsHandler)
	if err != nil {
		panic(fmt.Sprintf("natsrouter: subscribing to %s: %v", rt.natsSubject, err))
	}

	r.mu.Lock()
	r.subs = append(r.subs, sub)
	r.mu.Unlock()
}

// Shutdown drains every route registered through r and waits for in-flight
// handlers to finish or ctx to expire.
//
// After Shutdown returns, the router will not dispatch new requests. Calling
// Shutdown a second time is a no-op. This is independent of nc.Drain() — use
// Shutdown when you need to stop the router while keeping the NATS connection
// open for other work (e.g., publishing shutdown events).
//
// Returns ctx.Err() if handlers were still running when the deadline expired,
// combined with any error reported by Subscription.Drain().
func (r *Router) Shutdown(ctx context.Context) error {
	// Set stopping FIRST so any callback fired between now and full drain
	// (or after ctx expires) hits the rejection path in natsHandler.
	r.stopping.Store(true)

	r.mu.Lock()
	subs := r.subs
	r.subs = nil
	r.mu.Unlock()

	// Register close listeners BEFORE calling Drain so we don't miss the
	// event. nats.go fires SubscriptionClosed only after the per-sub
	// dispatch loop has fully exited — every callback that was ever going
	// to run has already returned by that point, so there is no "Add at
	// zero after Wait" window to guard against.
	closed := make([]<-chan nats.SubStatus, len(subs))
	for i, s := range subs {
		closed[i] = s.StatusChanged(nats.SubscriptionClosed)
	}

	var errs []error
	for _, s := range subs {
		if err := s.Drain(); err != nil {
			errs = append(errs, fmt.Errorf("draining %q: %w", s.Subject, err))
		}
	}

	// Wait for each subscription's dispatcher to finish. On ctx expiry,
	// record the error and stop waiting. allClosed tracks whether every
	// subscription confirmed close; if not, we MUST NOT enter r.wg.Wait()
	// below -- a still-draining subscription can fire natsHandler, which
	// calls r.wg.Add(1) concurrently with our Wait(). Per sync.WaitGroup
	// docs that's an Add+Wait race (panic if counter was 0 when Wait
	// started). When ctx expires we instead surface the error and let the
	// caller's deadline take precedence: any remaining handler goroutines
	// continue in the background until process exit.
	allClosed := true
closeLoop:
	for i, ch := range closed {
		select {
		case <-ch:
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("waiting for %q close: %w", subs[i].Subject, ctx.Err()))
			allClosed = false
			break closeLoop
		}
	}

	// Subscriptions are drained: no new natsHandler callbacks will fire,
	// so r.wg counter is now stable and Wait() is race-free. Skip this
	// block when allClosed is false (see comment above).
	if allClosed {
		wgDone := make(chan struct{})
		go func() {
			r.wg.Wait()
			close(wgDone)
		}()
		select {
		case <-wgDone:
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("waiting for in-flight handlers: %w", ctx.Err()))
		}
	}

	return errors.Join(errs...)
}
