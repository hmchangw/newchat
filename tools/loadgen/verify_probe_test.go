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

func TestProbeTracker_LeakageOnUserLane_IsViolation(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-dm-000001", "u-1", 0, []string{"u-1", "u-2"}, at(1))

	tr.RecordDelivery("u-1", "m1", "room-dm-000001", laneUser, at(2))
	tr.RecordDelivery("u-2", "m1", "room-dm-000001", laneUser, at(2))
	// u-9 is not a member of this DM.
	tr.RecordDelivery("u-9", "m1", "room-dm-000001", laneUser, at(2))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindUnexpectedRecipient, vs[0].Kind)
	assert.Equal(t, []string{"u-9"}, vs[0].Users)
}

func TestProbeTracker_LeakageOnRoomLane_IsIgnored(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1"}, at(1))

	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))
	// A non-member receiving on the room topic reflects who subscribed, which
	// loadgen itself controls (backend.creds has full chat.> permissions).
	// Treating it as leakage would test NATS ACLs, not the chat system.
	tr.RecordDelivery("u-9", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordDelivery("u-8", "m1", "room-small-000001", laneLocal, at(2))

	assert.Empty(t, tr.Finalize(),
		"room-lane delivery to a non-member must never be reported as leakage")
}

func TestProbeTracker_LeakageDoesNotCountAsDelivery(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-dm-000001", "u-1", 0, []string{"u-1", "u-2"}, at(1))

	tr.RecordDelivery("u-1", "m1", "room-dm-000001", laneUser, at(2))
	tr.RecordDelivery("u-9", "m1", "room-dm-000001", laneUser, at(2))

	vs := tr.Finalize()
	// u-2 is still missing; the leak must not paper over the gap.
	require.Len(t, vs, 2)
	kinds := []ViolationKind{vs[0].Kind, vs[1].Kind}
	assert.Contains(t, kinds, KindMissingRecipient)
	assert.Contains(t, kinds, KindUnexpectedRecipient)
}

func TestProbeTracker_RepeatedLeakFromSameUser_ReportedOnce(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-dm-000001", "u-1", 0, []string{"u-1", "u-2"}, at(1))
	tr.RecordDelivery("u-1", "m1", "room-dm-000001", laneUser, at(2))
	tr.RecordDelivery("u-2", "m1", "room-dm-000001", laneUser, at(2))
	tr.RecordDelivery("u-9", "m1", "room-dm-000001", laneUser, at(2))
	tr.RecordDelivery("u-9", "m1", "room-dm-000001", laneUser, at(3))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, []string{"u-9"}, vs[0].Users, "same leaking user must be deduped")
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

func TestShouldProbe_Deterministic(t *testing.T) {
	for seq := uint64(0); seq < 100; seq++ {
		a := shouldProbe(42, 7, seq, 0.5)
		b := shouldProbe(42, 7, seq, 0.5)
		assert.Equal(t, a, b, "same inputs must give the same answer at seq %d", seq)
	}
}

func TestShouldProbe_RateZeroNeverProbes(t *testing.T) {
	for seq := uint64(0); seq < 1000; seq++ {
		assert.False(t, shouldProbe(42, 1, seq, 0))
	}
}

func TestShouldProbe_RateOneAlwaysProbes(t *testing.T) {
	for seq := uint64(0); seq < 1000; seq++ {
		assert.True(t, shouldProbe(42, 1, seq, 1.0))
	}
}

func TestShouldProbe_ApproximatesRate(t *testing.T) {
	const n = 100000
	hits := 0
	for seq := uint64(0); seq < n; seq++ {
		if shouldProbe(42, 3, seq, 0.01) {
			hits++
		}
	}
	// 1% of 100k is 1000; allow generous slack for hash distribution.
	assert.InDelta(t, 1000, hits, 200, "observed rate %d/%d strays from 1%%", hits, n)
}

func TestShouldProbe_DiffersAcrossUsers(t *testing.T) {
	// Adjacent user indices must not produce identical probe streams,
	// otherwise probes cluster on the same senders.
	same := 0
	for seq := uint64(0); seq < 1000; seq++ {
		if shouldProbe(42, 1, seq, 0.1) == shouldProbe(42, 2, seq, 0.1) {
			same++
		}
	}
	assert.Less(t, same, 1000, "user 1 and user 2 produced identical probe streams")
}

func TestShouldProbe_DiffersAcrossSeeds(t *testing.T) {
	diff := 0
	for seq := uint64(0); seq < 1000; seq++ {
		if shouldProbe(1, 5, seq, 0.1) != shouldProbe(2, 5, seq, 0.1) {
			diff++
		}
	}
	assert.Positive(t, diff, "different seeds must produce different probe sets")
}
