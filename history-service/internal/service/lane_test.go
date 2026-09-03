package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

func laneTestConfig() *config.Config {
	return &config.Config{
		MessageHistoryFloorDays: 730, LargeRoomThreshold: 500,
		MaxPinnedPerRoom: 10, PinEnabled: true,
		PreviewWarmBackEnabled: true, PreviewWarmBackWorkers: 4, PreviewWarmBackQueue: 8,
	}
}

// newLaneService builds a service with nil stores for wiring tests and closes it
// with the test, so each case is two lines rather than four.
func newLaneService(t *testing.T, opts ...Option) *HistoryService {
	t.Helper()
	s := New(nil, nil, nil, nil, nil, nil, nil, nil, laneTestConfig(), opts...)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// Both lanes' services share one injected warm-back pool: it writes to Mongo,
// which is up whichever lane is serving, so a second set of workers and queue
// would be overhead for a lane that is idle almost always. Closing either
// service leaves the pool running — it is its creator's to close.
func TestNew_WithPreviewWarmer_SharesThePool(t *testing.T) {
	pool := NewPreviewWarmer(nil, 2, 4)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	home := newLaneService(t, WithPreviewWarmer(pool))
	failover := newLaneService(t, WithPreviewWarmer(pool))
	assert.Same(t, pool, home.warmer)
	assert.Same(t, pool, failover.warmer)

	require.NoError(t, failover.Close(context.Background()))
	pool.mu.RLock()
	closed := pool.closed
	pool.mu.RUnlock()
	assert.False(t, closed, "an injected pool must survive its borrowers' Close")
}

// Without the option each service starts and owns its pool, so a single-lane
// service — and every existing test — is unaffected.
func TestNew_WithoutPreviewWarmer_OwnsItsPool(t *testing.T) {
	first := newLaneService(t)
	second := newLaneService(t)
	firstPool, ok := first.warmer.(*PreviewWarmer)
	require.True(t, ok, "an enabled service must start a real pool")
	secondPool, ok := second.warmer.(*PreviewWarmer)
	require.True(t, ok)
	assert.NotSame(t, firstPool, secondPool)
	assert.True(t, first.ownsWarmer)
}

// PREVIEW_WARMBACK_ENABLED=false installs the no-op writer; an injected pool is
// the caller's decision and is still honoured, since main only builds one when
// the switch is on.
func TestNew_WarmBackDisabled_InstallsNoop(t *testing.T) {
	cfg := laneTestConfig()
	cfg.PreviewWarmBackEnabled = false
	s := New(nil, nil, nil, nil, nil, nil, nil, nil, cfg)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	_, isNoop := s.warmer.(nopPreviewWarmer)
	assert.True(t, isNoop)
	assert.True(t, s.ownsWarmer)
}

// lanePublisher records the subjects a service published to.
type lanePublisher struct{ subjects []string }

func (p *lanePublisher) Publish(_ context.Context, subj string, _ []byte, _ string) error {
	p.subjects = append(p.subjects, subj)
	return nil
}

func (p *lanePublisher) PublishMigration(ctx context.Context, subj string, data []byte, msgID string) error {
	return p.Publish(ctx, subj, data, msgID)
}

// An edit, delete, pin or reaction served on the failover lane must go to the
// standby canonical stream. Published to the live subject it would reach
// nothing: that stream lives on the cluster whose outage put the client on the
// failover lane in the first place.
func TestPublishCanonical_UsesTheServiceLane(t *testing.T) {
	events := []subject.CanonicalEvent{
		subject.CanonicalUpdated, subject.CanonicalDeleted,
		subject.CanonicalPinned, subject.CanonicalUnpinned, subject.CanonicalReacted,
	}
	for _, tc := range []struct {
		name string
		lane subject.Lane
	}{
		{"home lane", subject.LaneHome},
		{"failover lane", subject.LaneFailover},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub := &lanePublisher{}
			s := newLaneService(t, WithLane(tc.lane))
			s.publisher = pub

			var want []string
			for _, evt := range events {
				s.publishCanonicalBestEffort(natsrouter.NewContext(nil), "site-a", evt,
					&model.MessageEvent{Message: model.Message{ID: "m-1", RoomID: "r-1"}})
				want = append(want, tc.lane.MsgCanonical("site-a", evt))
			}
			assert.Equal(t, want, pub.subjects)
		})
	}
}

// The lane defaults to home, so a service built without the option — every
// single-site deployment — keeps publishing to the live canonical stream.
func TestNew_DefaultsToTheHomeLane(t *testing.T) {
	assert.Equal(t, subject.LaneHome, newLaneService(t).lane)
}
