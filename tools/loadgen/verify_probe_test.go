package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func at(sec int) time.Time { return time.Unix(int64(sec), 0).UTC() }

func TestProbeTracker_CompleteDelivery_NoViolations(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1", "u-2", "u-3"}, at(1))

	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordDelivery("u-2", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordDelivery("u-3", "m1", "room-small-000001", laneGlobal, at(2))

	assert.Empty(t, tr.Finalize())
}

func TestProbeTracker_SenderMustBeExpected(t *testing.T) {
	tr := NewProbeTracker()
	// Sender u-1 is in the expected set (broadcast-worker echoes) but never
	// receives its own message.
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1", "u-2"}, at(1))
	tr.RecordDelivery("u-2", "m1", "room-small-000001", laneGlobal, at(2))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMissingRecipient, vs[0].Kind)
	assert.Equal(t, []string{"u-1"}, vs[0].Users)
}

func TestProbeTracker_PartialDelivery_MissingRecipient(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1", "u-2", "u-3"}, at(1))
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMissingRecipient, vs[0].Kind)
	assert.Equal(t, "m1", vs[0].MsgID)
	assert.Equal(t, []string{"u-2", "u-3"}, vs[0].Users, "missing users must be sorted")
}

func TestProbeTracker_ZeroDelivery_TotalLoss(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1", "u-2"}, at(1))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindTotalLoss, vs[0].Kind,
		"zero recipients is total_loss, not missing_recipient — different investigation")
}

func TestProbeTracker_DuplicateWithinLane_IsViolation(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1"}, at(1))
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(3))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindDuplicateDelivery, vs[0].Kind)
	assert.Equal(t, []string{"u-1"}, vs[0].Users)
}

func TestProbeTracker_SameMsgOnBothLanes_IsNotDuplicate(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1"}, at(1))
	// Both lanes are subscribed to stay ROOM_SUBJECT_MODE-agnostic. One
	// arrival per lane is expected, not a duplicate.
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneLocal, at(2))

	assert.Empty(t, tr.Finalize())
}

func TestProbeTracker_UnknownMsgID_Ignored(t *testing.T) {
	tr := NewProbeTracker()
	// Untracked traffic (99% of the workload) must not allocate or panic.
	tr.RecordDelivery("u-9", "not-a-probe", "room-small-000001", laneGlobal, at(2))
	assert.Empty(t, tr.Finalize())
	assert.Zero(t, tr.Counts().Tracked)
}

func TestProbeTracker_Counts(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1"}, at(1))
	tr.RegisterProbe("m2", "room-small-000001", "u-1", 0, []string{"u-1"}, at(1))
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordSuppressed()
	tr.RecordSuppressed()

	c := tr.Counts()
	assert.Equal(t, 2, c.Tracked)
	assert.Equal(t, 2, c.Suppressed)
	assert.Equal(t, 1, c.Complete)
	assert.Equal(t, 1, c.TotalLoss)
}

func TestProbeTracker_ConcurrentDeliveries(t *testing.T) {
	tr := NewProbeTracker()
	expected := make([]string, 200)
	for i := range expected {
		expected[i] = fmtUserID(i)
	}
	tr.RegisterProbe("m1", "room-medium-000001", expected[0], 0, expected, at(1))

	var wg sync.WaitGroup
	for i := range expected {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			tr.RecordDelivery(u, "m1", "room-medium-000001", laneGlobal, at(2))
		}(expected[i])
	}
	wg.Wait()

	assert.Empty(t, tr.Finalize(), "all 200 concurrent deliveries must be recorded")
}
