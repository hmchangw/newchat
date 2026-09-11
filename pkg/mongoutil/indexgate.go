package mongoutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/singleflight"
)

// IndexProbeTimeout bounds an on-demand ensure from a caller that states no deadline (a JetStream
// handler). Well under the 30s ACK_WAIT default, so a stalled createIndexes cannot hold a delivery
// past its ack deadline and have the server redeliver it while the attempt is still in flight.
const IndexProbeTimeout = 10 * time.Second

// indexRetryMin and indexRetryMax bound the backoff between probes after a failed ensure.
const (
	indexRetryMin = 5 * time.Second
	indexRetryMax = time.Minute
)

// IndexGate confirms an index on demand before a write that relies on it, for a degradable service
// that may resume before the index's owner has rebuilt it. All callers share one bounded, detached
// attempt (see wait); a failure is remembered so NAK redeliveries do not become a createIndexes storm.
type IndexGate struct {
	name   string // names the index in errors and logs
	ensure func(context.Context) error
	now    func() time.Time
	flight singleflight.Group
	ready  atomic.Bool

	// mu guards the remembered failure: written inside the flight, read by any caller's fast path.
	mu      sync.Mutex
	lastErr error
	retryAt time.Time
	backoff time.Duration
}

// NewIndexGate gates on ensure succeeding once; name identifies the index in errors and logs.
func NewIndexGate(name string, ensure func(context.Context) error) *IndexGate {
	return &IndexGate{name: name, ensure: ensure, now: time.Now}
}

// NewUniqueIndexGate gates on a unique index over keys on coll, created with the non-destructive
// EnsureIndex: a same-keys index with a different spec closes the gate until the owner repairs it.
func NewUniqueIndexGate(coll *mongo.Collection, keys bson.D) *IndexGate {
	fields := make([]string, 0, len(keys))
	for _, k := range keys {
		fields = append(fields, k.Key)
	}
	name := coll.Name() + " (" + strings.Join(fields, ",") + ")"
	return NewIndexGate(name, func(ctx context.Context) error {
		return EnsureIndex(ctx, coll, mongo.IndexModel{
			Keys:    keys,
			Options: options.Index().SetUnique(true),
		})
	})
}

// Ready returns nil once the index is confirmed, ensuring it if needed.
func (g *IndexGate) Ready(ctx context.Context) error {
	if g.ready.Load() {
		return nil
	}
	if err := g.wait(ctx); err != nil {
		return fmt.Errorf("confirm unique index %s: %w", g.name, err)
	}
	return nil
}

// wait returns the remembered failure inside its retry-after, else joins or leads one attempt. A waiter
// whose ctx ends is released while the attempt runs on for the rest, so one cancelled caller fails nobody.
func (g *IndexGate) wait(ctx context.Context) error {
	if err := g.remembered(); err != nil {
		return err
	}
	results := g.flight.DoChan("ensure", func() (any, error) {
		if g.ready.Load() {
			return nil, nil
		}
		if err := g.remembered(); err != nil {
			return nil, err
		}
		ensureCtx, cancel, callerBound := g.attemptContext(ctx)
		defer cancel()
		if err := g.ensure(ensureCtx); err != nil {
			// The caller's own budget running out says nothing about the index: do not close the gate on it.
			if !callerBound || !errors.Is(err, context.DeadlineExceeded) {
				g.remember(ctx, err)
			}
			return nil, err
		}
		g.ready.Store(true)
		return nil, nil
	})
	select {
	case r := <-results:
		return r.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// remembered returns the last failure while its retry-after has not elapsed.
func (g *IndexGate) remembered() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lastErr != nil && g.now().Before(g.retryAt) {
		return g.lastErr
	}
	return nil
}

// remember records a failure and schedules the next probe. An outage is re-probed every indexRetryMin
// (a failed command over a dead server is cheap, and a longer memo only delays recovery); a failure the
// server reported (a conflicting index, duplicate data) doubles up to indexRetryMax and is logged loudly,
// because it holds dependent writes until the owner service or an operator acts.
func (g *IndexGate) remember(ctx context.Context, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if isOutage(err) {
		g.backoff = indexRetryMin
	} else {
		g.backoff = min(max(2*g.backoff, indexRetryMin), indexRetryMax)
		slog.ErrorContext(ctx, "mongo: index cannot be confirmed; dependent writes are held until it is",
			"index", g.name, "retryAfter", g.backoff, "error", err)
	}
	g.lastErr = err
	g.retryAt = g.now().Add(g.backoff)
}

// isOutage reports whether err is MongoDB being unreachable or slow rather than a verdict on the index.
func isOutage(err error) bool {
	return mongo.IsNetworkError(err) || mongo.IsTimeout(err) || errors.Is(err, context.DeadlineExceeded)
}

// attemptContext detaches the caller's cancellation. A caller that states a deadline keeps it, capped at
// IndexEnsureTimeout (startup's shared budget); one that does not gets IndexProbeTimeout. callerBound
// reports that the deadline is the caller's own, so its expiry is not a verdict on the index.
func (g *IndexGate) attemptContext(ctx context.Context) (context.Context, context.CancelFunc, bool) {
	// Wall clock, not g.now: the driver enforces this deadline; g.now only paces the retry-after.
	deadline := time.Now().Add(IndexProbeTimeout)
	callerBound := false
	if d, ok := ctx.Deadline(); ok {
		deadline = time.Now().Add(IndexEnsureTimeout)
		if d.Before(deadline) {
			deadline, callerBound = d, true
		}
	}
	ensureCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	return ensureCtx, cancel, callerBound
}
