package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/jsretry"
)

// backlogCheckInterval throttles the consumer-info round trip on the drain path:
// the clear condition is checked at most this often, not once per persisted message.
const backlogCheckInterval = 5 * time.Second

// drainTailGrace is how long an unsettled redelivery set holds the marker once the
// undelivered backlog is empty. Elapsing it is the normal way a recovery ends, not an
// anomaly: settle calls OnWriteSuccess before Ack, so the very message observing the
// drained backlog is still in the consumer's pending set and ackPending is never 0
// there. The grace is what lets that resolve.
//
// It also bounds the pathological case: one message that can never be written would
// otherwise pin the site-wide marker forever, telling every client that history is
// incomplete.
//
// Derived from the retry schedule rather than written down, because a literal rots
// the moment anyone retunes that schedule — which already happened once: this was a
// hard-coded 5m "comfortably above the 2m tail" until the shared DefaultBackoff grew
// a 10m tail, leaving the grace *under* the interval it has to outlast. A marker
// cleared mid-drain tells every client history is complete while replays are still
// in flight, which is the exact failure the marker exists to prevent.
var drainTailGrace = deriveDrainTailGrace(jsretry.DefaultBackoff)

// minDrainTailGrace floors the derived value so a degenerate or very fast schedule
// still leaves room for one AckWait-sized settle after the backlog reads empty.
const minDrainTailGrace = 5 * time.Minute

// deriveDrainTailGrace allows two tail waits: one for the last in-flight redelivery
// to be served, one for it to settle and Ack. Jitter only shortens a served delay
// (equal jitter draws within [half, full]), so two full tails is an upper bound on
// the real interval and the grace cannot land under it.
func deriveDrainTailGrace(backoff []time.Duration) time.Duration {
	if len(backoff) == 0 {
		return minDrainTailGrace
	}
	if grace := 2 * backoff[len(backoff)-1]; grace > minDrainTailGrace {
		return grace
	}
	return minDrainTailGrace
}

// historyWriteError marks a failure of the Cassandra history write specifically,
// as opposed to the Mongo/user-lookup failures that share the handler's error
// return. Only this class sets the degraded marker, because only this class means
// history is behind.
type historyWriteError struct{ err error }

func (e historyWriteError) Error() string { return e.err.Error() }
func (e historyWriteError) Unwrap() error { return e.err }

func isHistoryWriteError(err error) bool {
	var h historyWriteError
	return errors.As(err, &h)
}

// backlogFunc reports the consumer's two backlogs: pending is what has not been
// delivered yet, ackPending is what has been delivered and not yet acked — the set
// still cycling through redelivery. Both must be settled before history has caught
// up; NumPending alone hits zero while the last batch is still replaying.
type backlogFunc func(ctx context.Context) (pending, ackPending uint64, err error)

// degradeTracker owns this pod's view of the site's history-degraded marker.
// The marker is shared in Mongo; the local flag is a hot-path cache kept fresh by
// write outcomes and a background refresh, so a marker set by a sibling pod is
// adopted here too.
type degradeTracker struct {
	store   DegradeStore
	siteID  string
	backlog backlogFunc
	metrics *metrics
	now     func() time.Time

	mu sync.RWMutex
	// degraded is the tracker's local view of the site's health.
	degraded bool
	// markerSynced is true once the shared store is known to reflect the current
	// degraded state — set by a successful Set/Clear, or by Refresh reading the
	// marker back. While degraded is true and markerSynced is false, a write to
	// the shared marker is still outstanding (its Set failed and hasn't been
	// retried yet); Refresh must not adopt a healthy remote read in that window,
	// or it would silently drop the local guarantee that a failed Set still
	// selects the degraded retry policy.
	markerSynced  bool
	lastBacklogAt time.Time
	// drainTailSince stamps when the undelivered backlog first read empty while
	// redeliveries were still in flight, bounding that hold (drainTailGrace).
	drainTailSince time.Time
}

func newDegradeTracker(store DegradeStore, siteID string, backlog backlogFunc, m *metrics, clock func() time.Time) *degradeTracker {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &degradeTracker{store: store, siteID: siteID, backlog: backlog, metrics: m, now: clock}
}

func (t *degradeTracker) Degraded() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.degraded
}

// markLocked flips the local health view. A transition into healthy also retires the
// drain-tail clock, so the next degrade measures its own tail instead of inheriting
// the previous one's start.
func (t *degradeTracker) markLocked(degraded bool) {
	t.degraded = degraded
	if !degraded {
		t.drainTailSince = time.Time{}
	}
}

// setSynced updates the local flag for a transition confirmed by the shared
// store itself — a successful Set/Clear, or a marker read back via Refresh.
func (t *degradeTracker) setSynced(degraded bool) {
	t.mu.Lock()
	t.markLocked(degraded)
	t.markerSynced = true
	t.mu.Unlock()
	t.metrics.setDegraded(degraded)
}

// OnWriteFailure marks the site degraded on the first history-write failure after a
// healthy period. The local flag flips even when the Mongo write fails, so a marker
// that could not be stored still holds the three consumers that read it.
// A failure that arrives while an earlier marker write is still unsynced retries
// that write rather than treating the site as already recorded.
//
// The write-failure counter is emitted by settle, not here: its class label is only
// knowable where the error is classified.
func (t *degradeTracker) OnWriteFailure(ctx context.Context) {
	t.mu.Lock()
	skipWrite := t.degraded && t.markerSynced
	t.markLocked(true)
	// A write failing again is the one real regression signal, so it — and nothing
	// else — restarts the drain tail. Without this the clock would keep running
	// through a resumed outage and could clear the marker mid-failure.
	t.drainTailSince = time.Time{}
	if !skipWrite {
		// A fresh transition into degraded (or a retry of one whose earlier Set
		// never landed) — the shared store does not yet reflect it. Cleared here,
		// in the same critical section that flips degraded, so a concurrent
		// Refresh can never observe (degraded=true, markerSynced=true) for a
		// write that hasn't actually succeeded.
		t.markerSynced = false
	}
	t.mu.Unlock()
	t.metrics.setDegraded(true)
	if skipWrite {
		return
	}

	since := t.now().UnixMilli()
	if err := t.store.Set(ctx, t.siteID, since); err != nil {
		slog.ErrorContext(ctx, "failed to set history degraded marker",
			"error", err, "site", t.siteID, "degraded_since", since)
		return
	}
	t.mu.Lock()
	if !t.degraded {
		// A concurrent OnWriteSuccess cleared the marker while this Set was in flight,
		// so the Set landed after the Clear and resurrected it in the shared store.
		// Stamping markerSynced here would leave (degraded=false, marker present), and
		// the next Refresh would read that marker back and re-adopt degraded for another
		// whole grace cycle. Undo the resurrection instead.
		t.mu.Unlock()
		if err := t.store.Clear(ctx, t.siteID); err != nil {
			slog.ErrorContext(ctx, "failed to undo a degraded marker that raced a recovery",
				"error", err, "site", t.siteID)
		}
		return
	}
	t.markerSynced = true
	t.mu.Unlock()
	slog.WarnContext(ctx, "history marked degraded", "site", t.siteID, "degraded_since", since)
}

// OnWriteSuccess clears the marker once writes succeed AND the consumer backlog is
// drained. Clearing on first success alone would be wrong: the drain is exactly when
// the quote re-projection needs the marker held, because parents are still replaying.
//
// "Drained" means both backlogs are settled — nothing undelivered and nothing still
// cycling through redelivery. NumPending alone reaches zero while the last in-flight
// batch is still replaying, which would clear the marker out from under exactly the
// messages the marker is protecting. The redelivery half of that hold is bounded by
// drainTailGrace so a permanently stuck message cannot pin the marker forever.
func (t *degradeTracker) OnWriteSuccess(ctx context.Context) {
	if !t.Degraded() {
		return // healthy hot path: no locks beyond the RLock above, no round trips
	}

	now := t.now()
	t.mu.Lock()
	if !t.lastBacklogAt.IsZero() && now.Sub(t.lastBacklogAt) < backlogCheckInterval {
		t.mu.Unlock()
		return
	}
	t.lastBacklogAt = now
	t.mu.Unlock()

	pending, ackPending, err := t.backlog(ctx)
	if err != nil {
		slog.WarnContext(ctx, "backlog check failed, holding degraded marker", "error", err, "site", t.siteID)
		return
	}
	if pending > 0 {
		// Still delivering, so the tail clock does not start yet — but a clock already
		// running is deliberately left alone. Arriving traffic is not evidence that the
		// drain regressed; on a site taking any load NumPending is non-zero constantly,
		// and restarting here meant the grace needed an uninterrupted run of samples it
		// would never get, pinning the marker (and incompleteSince, and badge
		// suppression) indefinitely after recovery. A genuine regression is a write
		// failing again, and OnWriteFailure is what restarts the clock.
		return
	}
	if ackPending > 0 {
		if !t.drainTailGraceElapsed(now) {
			return
		}
		// The expected end of a recovery, not a stuck consumer: this success has not
		// been acked yet, so it is itself counted in ackPending.
		slog.InfoContext(ctx, "history drain tail grace elapsed, clearing marker",
			"site", t.siteID, "ack_pending", ackPending, "tail_grace", drainTailGrace.String())
	}

	if err := t.store.Clear(ctx, t.siteID); err != nil {
		slog.ErrorContext(ctx, "failed to clear history degraded marker", "error", err, "site", t.siteID)
		return
	}
	t.setSynced(false)
	slog.InfoContext(ctx, "history degraded marker cleared", "site", t.siteID)
}

// drainTailGraceElapsed reports whether the in-flight redelivery set has held the
// marker for drainTailGrace. The first observation of an empty undelivered backlog
// starts the clock; a fresh undelivered backlog resets it (the drain is running
// again), so the grace always measures the current tail.
func (t *degradeTracker) drainTailGraceElapsed(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.drainTailSince.IsZero() {
		t.drainTailSince = now
		return false
	}
	return now.Sub(t.drainTailSince) >= drainTailGrace
}

// Refresh adopts the shared marker's current state, so a pod that saw no failures
// of its own still applies the degraded retry policy during an outage. It must not
// apply a read while a local degrade is unsynced: OnWriteFailure's failed Set has
// not yet landed in the shared store, so a Get here would read "healthy" and
// silently revert the local guarantee that a failed marker write still keeps the
// site degraded. The guard is checked both before and after the Get round trip —
// an OnWriteFailure can enter the unsynced state while the Get is in flight, and
// applying a now-stale healthy read would clobber it just the same.
func (t *degradeTracker) Refresh(ctx context.Context) {
	t.mu.RLock()
	unsynced := t.degraded && !t.markerSynced
	t.mu.RUnlock()
	if unsynced {
		return
	}

	marker, err := t.store.Get(ctx, t.siteID)
	if err != nil {
		slog.WarnContext(ctx, "history degraded marker refresh failed", "error", err, "site", t.siteID)
		return
	}

	t.mu.Lock()
	if t.degraded && !t.markerSynced {
		// OnWriteFailure raced this Get and is now retrying its own Set;
		// dropping this stale read leaves that in-flight write authoritative.
		t.mu.Unlock()
		return
	}
	degraded := marker != nil
	t.markLocked(degraded)
	t.markerSynced = true
	t.mu.Unlock()
	t.metrics.setDegraded(degraded)
}

// startDegradeRefresher polls the shared marker until the returned stop is called.
func startDegradeRefresher(ctx context.Context, t *degradeTracker, every time.Duration) func() {
	return startTicker(ctx, every, tickAfterInterval, t.Refresh)
}

// startDegradeTracker builds the tracker, adopts the site's current marker, and only
// then starts the background refresher.
//
// The adoption is synchronous on purpose. The refresher's first tick lands a full
// interval after it starts, so a tracker that only polls spends that interval
// believing the site is healthy — and a pod restarted mid-outage begins consuming
// inside it, replaying an outage's worth of events with the marker's protections
// off: unresolvable quotes dropped permanently instead of retried, hours-old thread
// badges re-notified. Shortening the interval does not fix it; a restart storm during
// an incident is exactly when it bites. Reading before the caller can consume does.
//
// A failed read is not fatal. The marker is advisory, and refusing to boot without it
// would trade a degraded recovery for a total one — Refresh logs the failure and the
// ticker retries.
func startDegradeTracker(ctx context.Context, store DegradeStore, siteID string,
	backlog backlogFunc, m *metrics, every time.Duration) (*degradeTracker, func()) {
	t := newDegradeTracker(store, siteID, backlog, m, nil)
	t.Refresh(ctx)
	return t, startDegradeRefresher(ctx, t, every)
}
