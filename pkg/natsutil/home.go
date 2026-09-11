package natsutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/health"
)

// DefaultMaxPayload is the nats-server default max_payload (1 MiB). It is the
// floor MaxPayload assumes for a connection that has not yet learned the
// broker's real value.
const DefaultMaxPayload = 1024 * 1024

// MaxPayload is the broker's advertised max_payload for conn, or
// DefaultMaxPayload when it is not known yet. A connection from ConnectHome may
// still be dialing when a service sizes its page budgets and payload caps, and
// nats.go reports zero until the first INFO arrives. Zero would disable
// trimming outright, so a reply could then exceed the real limit and be refused;
// the server default is the conservative answer, and it is the real value in
// every deployment that has not raised it.
func MaxPayload(conn *o11ynats.Conn) int64 {
	if conn == nil || conn.NatsConn() == nil {
		return DefaultMaxPayload
	}
	if n := conn.NatsConn().MaxPayload(); n > 0 {
		return n
	}
	return DefaultMaxPayload
}

// DeferredBind is a home-lane bind that runs when the home connection is up.
//
// A JetStream consumer cannot be created on a connection that is still dialing:
// the API request sits in the reconnect buffer until its timeout and then
// fails. So a service whose home dial was lazy (see BuddyDialer.ConnectHome)
// hands its home-lane bind here and carries on; the bind runs the moment the
// connection reaches CONNECTED, retrying with backoff until it succeeds. Until
// then the lane is not Ready, which the service's readiness reports through
// LanesCheck.
type DeferredBind struct {
	mu      sync.Mutex
	stop    func()
	ready   bool
	stopped bool
	done    chan struct{}
}

// BindWhenConnected runs bind on conn's home lane as soon as conn is usable.
//
// If conn is already CONNECTED, bind runs synchronously and its error is
// returned: home is up, so a failed bind is a real fault and the caller treats
// it as fatal exactly as before. Otherwise bind is deferred to the connection's
// first CONNECTED transition and retried with backoff on failure — a cluster
// that has just returned may not have restored its streams yet, and a lane that
// never comes up is the outage all over again.
//
// bind returns the function that stops what it started (an iterator's Stop,
// say). Stop calls it once the bind has landed; a bind landing after Stop is
// stopped on the spot, so shutdown that begins mid-outage cannot leak a lane
// that binds a moment later.
func BindWhenConnected(ctx context.Context, conn *o11ynats.Conn, name string,
	bind func(context.Context) (func(), error),
) (*DeferredBind, error) {
	d := &DeferredBind{done: make(chan struct{})}
	nc := conn.NatsConn()
	if nc.Status() == nats.CONNECTED {
		stop, err := bind(ctx)
		if err != nil {
			close(d.done)
			return nil, fmt.Errorf("bind %s lane: %w", name, err)
		}
		d.landed(ctx, name, stop)
		close(d.done)
		return d, nil
	}

	// Registered before the status re-check, so a connect landing between the
	// two cannot be missed.
	ch := nc.StatusChanged(nats.CONNECTED)
	slog.WarnContext(ctx, "home lane deferred until the home cluster is reachable", "lane", name)
	go func() {
		defer close(d.done)
		defer nc.RemoveStatusListener(ch)
		if nc.Status() != nats.CONNECTED {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
			}
		}
		d.bindWithRetry(ctx, name, bind)
	}()
	return d, nil
}

// bindRetryMin and bindRetryMax bound the backoff between failed deferred
// binds. The first retry is quick because the common failure is a cluster that
// is up but still restoring; the cap keeps a persistent fault from being
// hammered.
const (
	bindRetryMin = time.Second
	bindRetryMax = 30 * time.Second
)

func (d *DeferredBind) bindWithRetry(ctx context.Context, name string, bind func(context.Context) (func(), error)) {
	wait := bindRetryMin
	for {
		stop, err := bind(ctx)
		if err == nil {
			d.landed(ctx, name, stop)
			return
		}
		slog.WarnContext(ctx, "home lane bind failed; retrying", "lane", name, "retry_in", wait, "error", err)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		wait = min(wait*2, bindRetryMax)
	}
}

// landed records a successful bind, or stops it immediately if Stop already ran.
func (d *DeferredBind) landed(ctx context.Context, name string, stop func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		if stop != nil {
			stop()
		}
		slog.InfoContext(ctx, "home lane bound after shutdown began; stopped", "lane", name)
		return
	}
	d.stop = stop
	d.ready = true
	slog.InfoContext(ctx, "home lane bound", "lane", name)
}

// Ready reports whether the lane is bound and serving. Nil-safe.
func (d *DeferredBind) Ready() bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ready
}

// Stop stops the bound lane, or marks a still-deferred bind so it stops itself
// on landing. Idempotent and nil-safe, so shutdown lists it unconditionally.
func (d *DeferredBind) Stop() {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return
	}
	d.stopped = true
	d.ready = false
	if d.stop != nil {
		d.stop()
	}
}

// Done reports whether the deferred watcher has exited — after the bind landed,
// or after the context was cancelled. For tests and for callers that want to
// know the goroutine is gone.
func (d *DeferredBind) Done() bool {
	select {
	case <-d.done:
		return true
	default:
		return false
	}
}

// errNoLane is LanesCheck's failure: nothing is consuming yet.
var errNoLane = errors.New("no lane bound")

// LanesCheck is the readiness check for a service with a lazily-dialed home:
// ready when at least one lane is serving. The plain NATS check reads
// RECONNECTING as healthy, which is exactly the state a pod is in while it has
// nothing bound at all, so it cannot express "waiting for a lane" on its own.
// bound are the per-lane predicates — a DeferredBind's Ready, a Lane's Bound.
func LanesCheck(bound ...func() bool) health.Check {
	return health.Check{Name: "lanes", Probe: func(context.Context) error {
		for _, b := range bound {
			if b() {
				return nil
			}
		}
		return errNoLane
	}}
}
