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
		PreviewWarmBackWorkers: 4, PreviewWarmBackQueue: 8,
	}
}

// A site serves both its home and its failover lane from one process, and each
// lane needs its own service because their publishers go out on different
// connections. The preview warm-back pool is not per-lane, though: it writes to
// Mongo, which is still up. Left unshared, the failover lane would double this
// service's warm-back workers and queue for a lane that is idle almost always.
func TestNew_WithSharedWarmBack_ReusesThePool(t *testing.T) {
	home := New(nil, nil, nil, nil, nil, nil, nil, nil, laneTestConfig())
	t.Cleanup(func() { _ = home.Close(context.Background()) })

	failover := New(nil, nil, nil, nil, nil, nil, nil, nil, laneTestConfig(),
		WithSharedWarmBack(home))

	require.NotNil(t, home.warmer)
	assert.Same(t, home.warmer, failover.warmer, "the second lane must not start its own pool")
}

// Without the option each service owns its pool, so the sharing is opt-in and a
// single-lane service is unaffected.
func TestNew_WithoutSharedWarmBack_OwnsItsPool(t *testing.T) {
	first := New(nil, nil, nil, nil, nil, nil, nil, nil, laneTestConfig())
	t.Cleanup(func() { _ = first.Close(context.Background()) })
	second := New(nil, nil, nil, nil, nil, nil, nil, nil, laneTestConfig())
	t.Cleanup(func() { _ = second.Close(context.Background()) })

	require.NotNil(t, first.warmer)
	require.NotNil(t, second.warmer)
	assert.NotSame(t, first.warmer, second.warmer)
}

// Closing the borrowing service must not stop the pool out from under its owner:
// shutdown closes each service it built, and the home lane's warm-backs have to
// keep draining until the home service itself closes.
func TestClose_OnABorrowedPool_LeavesTheOwnerRunning(t *testing.T) {
	home := New(nil, nil, nil, nil, nil, nil, nil, nil, laneTestConfig())
	t.Cleanup(func() { _ = home.Close(context.Background()) })
	failover := New(nil, nil, nil, nil, nil, nil, nil, nil, laneTestConfig(),
		WithSharedWarmBack(home))

	require.NoError(t, failover.Close(context.Background()))

	home.warmer.mu.RLock()
	closed := home.warmer.closed
	home.warmer.mu.RUnlock()
	assert.False(t, closed, "the owner's pool must still accept warm-backs")
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
			s := New(nil, nil, nil, pub, nil, nil, nil, nil, laneTestConfig(), WithLane(tc.lane))
			t.Cleanup(func() { _ = s.Close(context.Background()) })

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
	s := New(nil, nil, nil, nil, nil, nil, nil, nil, laneTestConfig())
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	assert.Equal(t, subject.LaneHome, s.lane)
}
