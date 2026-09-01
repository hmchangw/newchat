package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// repairFixture is the minimum wiring persistMutatedPreview needs: it only ever touches
// the room repo, so the rest are inert mocks with no expectations.
type repairFixture struct {
	rooms *mocks.MockRoomRepository
	svc   *HistoryService
}

func newRepairFixture(t *testing.T) *repairFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	rooms := mocks.NewMockRoomRepository(ctrl)
	return &repairFixture{
		rooms: rooms,
		svc: closeOnCleanupIn(t, New(
			mocks.NewMockMessageRepository(ctrl),
			mocks.NewMockSubscriptionRepository(ctrl),
			rooms,
			mocks.NewMockEventPublisher(ctrl),
			mocks.NewMockThreadRoomRepository(ctrl),
			mocks.NewMockThreadSubscriptionRepository(ctrl),
			mocks.NewMockUserStore(ctrl),
			mocks.NewMockAppStore(ctrl),
			&config.Config{MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10},
		)),
	}
}

// deadlineOf fails the test rather than returning a zero time: an unbounded repair write
// is itself the bug this file guards against.
func deadlineOf(t *testing.T, ctx context.Context, what string) time.Time {
	t.Helper()
	dl, ok := ctx.Deadline()
	require.True(t, ok, "%s must run under a bounded context", what)
	return dl
}

// The mutation has already committed in Cassandra when the repair runs, so every extra
// second here is a second the client waits for an edit or delete that already succeeded.
// Giving the fallback its own fresh window doubles that wait precisely when Mongo is
// unwell — the case where the write it follows timed out rather than failed fast.
func TestPersistMutatedPreview_RepairSharesTheWriteBudget(t *testing.T) {
	f := newRepairFixture(t)
	at := time.Now().UTC()

	var writeDeadline, repairDeadline time.Time
	f.rooms.EXPECT().
		UpdatePreviewBody(gomock.Any(), "r1", gomock.Any(), "m2", at.UnixMilli()).
		DoAndReturn(func(ctx context.Context, _ string, _ models.PreviewMessage, _ string, _ int64) (bool, error) {
			writeDeadline = deadlineOf(t, ctx, "the preview body write")
			return false, errors.New("mongo down")
		})
	f.rooms.EXPECT().
		InvalidatePreviewKey(gomock.Any(), "r1", "m1", at.UnixMilli()).
		DoAndReturn(func(ctx context.Context, _, _ string, _ int64) error {
			repairDeadline = deadlineOf(t, ctx, "the preview key invalidation")
			return nil
		})

	w := &previewWalk{
		State:            previewFound,
		Preview:          models.PreviewMessage{MessageID: "m2"},
		NewestObservedID: "m2",
	}
	f.svc.persistMutatedPreview(natsrouter.NewContext(nil), "r1", "m1", w, at)

	assert.Equal(t, writeDeadline, repairDeadline,
		"the repair must inherit the write's deadline, not start a second full window")
}

// A guard rejection or a failed seal returns immediately, so the repair that follows still
// has nearly the whole budget. Sharing the context must not collapse into "no budget left".
func TestPersistMutatedPreview_FastWriteFailureLeavesRepairBudget(t *testing.T) {
	f := newRepairFixture(t)
	at := time.Now().UTC()

	// previewDegraded writes nothing and reports not-applied, the cheapest path to the repair.
	var repairRemaining time.Duration
	f.rooms.EXPECT().
		InvalidatePreviewKey(gomock.Any(), "r1", "m1", at.UnixMilli()).
		DoAndReturn(func(ctx context.Context, _, _ string, _ int64) error {
			repairRemaining = time.Until(deadlineOf(t, ctx, "the preview key invalidation"))
			return nil
		})

	f.svc.persistMutatedPreview(natsrouter.NewContext(nil), "r1", "m1",
		&previewWalk{State: previewDegraded}, at)

	assert.Greater(t, repairRemaining, warmBackTimeout/2,
		"a write that failed fast must leave the repair most of the budget")
}

// A hidden thread reply has no room-timeline presence, so there is nothing to repair and
// nothing to bound — the mocks assert this by having no expectations at all.
func TestPersistMutatedPreview_SkippedWalkTouchesNothing(t *testing.T) {
	f := newRepairFixture(t)
	f.svc.persistMutatedPreview(natsrouter.NewContext(nil), "r1", "m1",
		&previewWalk{State: previewSkipped}, time.Now().UTC())
}

// closeOnCleanupIn drains the service's background preview writer when the test ends. New
// starts those workers, so every construction needs a termination path. The in-package
// twin of closeOnCleanup in rooms_test.go; this file is untagged, so the integration
// build sees it too.
func closeOnCleanupIn(t *testing.T, svc *HistoryService) *HistoryService {
	t.Helper()
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	return svc
}
