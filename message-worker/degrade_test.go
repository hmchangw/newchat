package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/histdegrade"
)

// fakeDegradeStore is an in-memory DegradeStore double.
type fakeDegradeStore struct {
	marker   *histdegrade.Marker
	setCalls atomic.Int64
	clears   atomic.Int64
	setErr   error
}

func (f *fakeDegradeStore) Set(_ context.Context, siteID string, since int64) error {
	f.setCalls.Add(1)
	if f.setErr != nil {
		return f.setErr
	}
	if f.marker == nil {
		f.marker = &histdegrade.Marker{SiteID: siteID, DegradedSince: since, UpdatedAt: since}
	}
	return nil
}

func (f *fakeDegradeStore) Clear(context.Context, string) error {
	f.clears.Add(1)
	f.marker = nil
	return nil
}

func (f *fakeDegradeStore) Get(context.Context, string) (*histdegrade.Marker, error) {
	return f.marker, nil
}

// newTestTracker builds a tracker over store with a real metrics instance. A nil
// clock freezes time at testClockStart — mirroring newDegradeTracker's own nil
// handling — so only the tests that advance time have to supply one.
func newTestTracker(t *testing.T, store DegradeStore, backlog backlogFunc, clock func() time.Time) *degradeTracker {
	t.Helper()
	m, err := newMetrics()
	require.NoError(t, err)
	if clock == nil {
		clock = func() time.Time { return testClockStart }
	}
	return newDegradeTracker(store, "site-a", backlog, m, clock)
}

// testClockStart is the instant the tracker tests' frozen clock reports.
var testClockStart = time.Unix(1700000000, 0).UTC()

func TestIsHistoryWriteError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
		{"wrapped history write error", fmt.Errorf("save message: %w", historyWriteError{errors.New("gocql timeout")}), true},
		{"double wrapped", fmt.Errorf("outer: %w", fmt.Errorf("save message: %w", historyWriteError{errors.New("x")})), true},
		{"unrelated wrapped error", fmt.Errorf("lookup user: %w", errors.New("mongo down")), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isHistoryWriteError(tt.err))
		})
	}
}

func TestDegradeTracker_OnWriteFailure_SetsMarkerOnce(t *testing.T) {
	store := &fakeDegradeStore{}
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) { return 500, 0, nil }, nil)

	assert.False(t, tr.Degraded())

	tr.OnWriteFailure(context.Background())
	assert.True(t, tr.Degraded())
	require.NotNil(t, store.marker)
	assert.Equal(t, int64(1700000000000), store.marker.DegradedSince)

	// Subsequent failures must not re-write the marker — it is a transition, not a per-message write.
	tr.OnWriteFailure(context.Background())
	tr.OnWriteFailure(context.Background())
	assert.Equal(t, int64(1), store.setCalls.Load())
}

func TestDegradeTracker_OnWriteSuccess_HoldsMarkerWhileBacklogRemains(t *testing.T) {
	store := &fakeDegradeStore{}
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) { return 4200, 0, nil }, nil)
	tr.OnWriteFailure(context.Background())

	tr.OnWriteSuccess(context.Background())

	assert.True(t, tr.Degraded(), "marker must be held during the drain")
	assert.Equal(t, int64(0), store.clears.Load())
}

// TestDegradeTracker_OnWriteSuccess_ClearsWhenBacklogDrained covers the
// fully-settled fast path (nothing pending, nothing unacked). Production rarely
// presents it — settle calls OnWriteSuccess before Ack, so the message doing the
// checking is itself unacked — but the branch is free and a sibling pod's success can
// still observe it.
func TestDegradeTracker_OnWriteSuccess_ClearsWhenBacklogDrained(t *testing.T) {
	store := &fakeDegradeStore{}
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) { return 0, 0, nil }, nil)
	tr.OnWriteFailure(context.Background())

	tr.OnWriteSuccess(context.Background())

	assert.False(t, tr.Degraded())
	assert.Equal(t, int64(1), store.clears.Load())
	assert.Nil(t, store.marker)
}

// TestDegradeTracker_OnWriteSuccess_HoldsMarkerWhileRedeliveriesInFlight covers the
// drain tail: NumPending reaches 0 while the last in-flight batch is still cycling
// through redelivery. Clearing there would switch the quote retry and the badge
// suppression off underneath exactly the messages that still need them.
func TestDegradeTracker_OnWriteSuccess_HoldsMarkerWhileRedeliveriesInFlight(t *testing.T) {
	store := &fakeDegradeStore{}
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) { return 0, 3, nil }, nil)
	tr.OnWriteFailure(context.Background())

	tr.OnWriteSuccess(context.Background())

	assert.True(t, tr.Degraded(), "an empty NumPending with redeliveries still in flight is not a drained backlog")
	assert.Equal(t, int64(0), store.clears.Load())
}

// TestDegradeTracker_OnWriteSuccess_ClearsOnceDrainTailGraceElapses covers the clear
// production actually takes: ackPending is non-zero (at minimum the checking message
// itself, which settle has not acked yet), so the marker clears when the grace
// elapses rather than on a fully-settled backlog. The same path bounds the pathological
// case — one message that can never be written would otherwise hold the site-wide
// marker forever, telling every client history is incomplete.
func TestDegradeTracker_OnWriteSuccess_ClearsOnceDrainTailGraceElapses(t *testing.T) {
	store := &fakeDegradeStore{}
	now := testClockStart
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) {
		return 0, 1, nil // never fully settles: the checking message is itself unacked
	}, func() time.Time { return now })

	tr.OnWriteFailure(context.Background())
	tr.OnWriteSuccess(context.Background())
	require.True(t, tr.Degraded(), "the tail hold starts on the first observation")

	now = now.Add(drainTailGrace)
	tr.OnWriteSuccess(context.Background())

	assert.False(t, tr.Degraded(), "a tail that never settles must not pin the marker indefinitely")
	assert.Equal(t, int64(1), store.clears.Load())
}

// TestDegradeTracker_OnWriteSuccess_DrainTailGraceRestartsOnNewBacklog guards the
// tail timer against a stale start: a fresh undelivered backlog means the drain is
// running again, so the grace must be measured from the next tail, not the old one.
func TestDegradeTracker_OnWriteSuccess_DrainTailGraceRestartsOnNewBacklog(t *testing.T) {
	store := &fakeDegradeStore{}
	now := testClockStart
	pending, ackPending := uint64(0), uint64(1)
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) {
		return pending, ackPending, nil
	}, func() time.Time { return now })

	tr.OnWriteFailure(context.Background())
	tr.OnWriteSuccess(context.Background()) // tail observed at t0

	now = now.Add(backlogCheckInterval)
	pending = 500 // a new wave arrives: the drain is not in its tail at all
	tr.OnWriteSuccess(context.Background())

	now = now.Add(drainTailGrace)
	pending = 0
	tr.OnWriteSuccess(context.Background())

	assert.True(t, tr.Degraded(), "the grace must run from the new tail, not the abandoned one")
	assert.Equal(t, int64(0), store.clears.Load())
}

func TestDegradeTracker_OnWriteSuccess_NoOpWhenHealthy(t *testing.T) {
	store := &fakeDegradeStore{}
	var pendingCalls atomic.Int64
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) {
		pendingCalls.Add(1)
		return 0, 0, nil
	}, nil)

	tr.OnWriteSuccess(context.Background())

	assert.Equal(t, int64(0), pendingCalls.Load(), "healthy hot path must not call consumer info")
	assert.Equal(t, int64(0), store.clears.Load())
}

func TestDegradeTracker_OnWriteSuccess_ThrottlesBacklogChecks(t *testing.T) {
	store := &fakeDegradeStore{}
	var pendingCalls atomic.Int64
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) {
		pendingCalls.Add(1)
		return 100, 0, nil
	}, nil)
	tr.OnWriteFailure(context.Background())

	for range 50 {
		tr.OnWriteSuccess(context.Background())
	}

	assert.Equal(t, int64(1), pendingCalls.Load(),
		"a frozen clock must yield exactly one backlog check, not one per message")
}

func TestDegradeTracker_OnWriteSuccess_PendingErrorHoldsMarker(t *testing.T) {
	store := &fakeDegradeStore{}
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) {
		return 0, 0, errors.New("consumer info unavailable")
	}, nil)
	tr.OnWriteFailure(context.Background())

	tr.OnWriteSuccess(context.Background())

	assert.True(t, tr.Degraded(), "an unreadable backlog must not clear the marker")
	assert.Equal(t, int64(0), store.clears.Load())
}

func TestDegradeTracker_OnWriteFailure_StoreErrorStillMarksLocally(t *testing.T) {
	store := &fakeDegradeStore{setErr: errors.New("mongo down")}
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) { return 0, 0, nil }, nil)

	tr.OnWriteFailure(context.Background())

	assert.True(t, tr.Degraded(),
		"a failed marker write must still select the degraded retry policy locally")
}

func TestDegradeTracker_Refresh_AdoptsRemoteMarker(t *testing.T) {
	store := &fakeDegradeStore{marker: &histdegrade.Marker{SiteID: "site-a", DegradedSince: 42}}
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) { return 0, 0, nil }, nil)

	assert.False(t, tr.Degraded())
	tr.Refresh(context.Background())
	assert.True(t, tr.Degraded(), "a marker set by a sibling pod must be adopted")

	store.marker = nil
	tr.Refresh(context.Background())
	assert.False(t, tr.Degraded())
}

func TestStartDegradeRefresher_StopTerminatesGoroutine(t *testing.T) {
	store := &fakeDegradeStore{}
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) { return 0, 0, nil }, nil)

	stop := startDegradeRefresher(context.Background(), tr, 5*time.Millisecond)
	stop() // must return promptly and not deadlock
}

// TestDegradeTracker_Refresh_DoesNotClearUnsyncedLocalDegrade covers the review
// finding: a failed marker Set must not be silently undone by a Refresh that
// reads the (nonexistent) shared marker as healthy. A later OnWriteFailure must
// retry the write rather than treat the site as already recorded.
//
// The leading Refresh models the state a running refresher actually produces
// in production — a healthy site whose most recent tick already confirmed
// markerSynced == true — rather than a freshly constructed, never-refreshed
// tracker (markerSynced == false at zero value). Starting from the zero value
// would pass even if OnWriteFailure never cleared markerSynced on a fresh
// degrade, since it starts false regardless; only starting from the
// post-Refresh synced-healthy state exercises the transition this test guards.
func TestDegradeTracker_Refresh_DoesNotClearUnsyncedLocalDegrade(t *testing.T) {
	store := &fakeDegradeStore{setErr: errors.New("mongo down")}
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) { return 0, 0, nil }, nil)

	// Steady-state healthy: store.Get returns nil, Refresh confirms markerSynced.
	tr.Refresh(context.Background())
	require.False(t, tr.Degraded())

	tr.OnWriteFailure(context.Background())
	require.True(t, tr.Degraded())
	require.Equal(t, int64(1), store.setCalls.Load())

	// store.Get still returns nil (no marker exists) because the earlier Set failed.
	tr.Refresh(context.Background())
	assert.True(t, tr.Degraded(),
		"a local degrade whose marker write never landed must not be cleared by Refresh reading a healthy remote")

	// A later failure must retry the marker write rather than treat the site as
	// already synced with the (nonexistent) shared marker.
	tr.OnWriteFailure(context.Background())
	assert.Equal(t, int64(2), store.setCalls.Load(),
		"OnWriteFailure must retry store.Set while the marker write is unsynced")
}

// TestDegradeTracker_OnWriteSuccess_ThrottleWindowExpires proves the throttle is a
// window, not a one-shot: with a frozen clock every prior test cannot distinguish
// "checked once, ever" from "checked once per backlogCheckInterval". Advancing the
// injected clock past the window must trigger a second backlog check.
func TestDegradeTracker_OnWriteSuccess_ThrottleWindowExpires(t *testing.T) {
	store := &fakeDegradeStore{}
	var pendingCalls atomic.Int64
	pending := uint64(100)
	now := testClockStart
	tr := newTestTracker(t, store, func(context.Context) (uint64, uint64, error) {
		pendingCalls.Add(1)
		return pending, 0, nil
	}, func() time.Time { return now })

	tr.OnWriteFailure(context.Background())
	tr.OnWriteSuccess(context.Background())
	assert.Equal(t, int64(1), pendingCalls.Load(), "the first check happens immediately after the failure")
	assert.True(t, tr.Degraded())

	// Still within the window: must not re-check.
	tr.OnWriteSuccess(context.Background())
	assert.Equal(t, int64(1), pendingCalls.Load(), "a check within the window must be throttled")

	now = now.Add(backlogCheckInterval)
	pending = 0
	tr.OnWriteSuccess(context.Background())

	assert.Equal(t, int64(2), pendingCalls.Load(), "a second check must fire once the window elapses")
	assert.False(t, tr.Degraded())
	assert.Equal(t, int64(1), store.clears.Load())
}
