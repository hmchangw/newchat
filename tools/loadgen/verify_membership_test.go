package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func modelForTest() *MembershipModel {
	prs := ProbeRoomSet{
		byRoom: map[string][]string{
			"room-small-000001": {"u-1", "u-2", "u-3"},
			"room-dm-000001":    {"u-1", "u-2"},
		},
	}
	m := NewMembershipModel(prs)
	m.SetSettle(5 * time.Second)
	return m
}

func TestMembershipModel_InitialEpochIsZero(t *testing.T) {
	m := modelForTest()
	assert.Equal(t, 0, m.Epoch("room-small-000001"))
	assert.Equal(t, []string{"u-1", "u-2", "u-3"}, m.Members("room-small-000001"))
}

func TestMembershipModel_AddBumpsEpochAndMembers(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))

	assert.Equal(t, 1, m.Epoch("room-small-000001"))
	assert.Equal(t, []string{"u-1", "u-2", "u-3", "u-9"}, m.Members("room-small-000001"))
}

func TestMembershipModel_RemoveBumpsEpochAndMembers(t *testing.T) {
	m := modelForTest()
	m.ApplyRemove("room-small-000001", "u-2", at(10))

	assert.Equal(t, 1, m.Epoch("room-small-000001"))
	assert.Equal(t, []string{"u-1", "u-3"}, m.Members("room-small-000001"))
}

func TestMembershipModel_ChangeIsScopedToOneRoom(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))

	assert.Equal(t, 0, m.Epoch("room-dm-000001"), "unrelated room epoch must not move")
	assert.Equal(t, []string{"u-1", "u-2"}, m.Members("room-dm-000001"))
}

func TestMembershipModel_SettleWindowOpensAndCloses(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))

	assert.True(t, m.InSettle("room-small-000001", at(11)), "inside the 5s window")
	assert.True(t, m.InSettle("room-small-000001", at(14)))
	assert.False(t, m.InSettle("room-small-000001", at(15)), "at the boundary the window is closed")
	assert.False(t, m.InSettle("room-small-000001", at(20)))
}

func TestMembershipModel_SettleIsPerRoom(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))

	assert.False(t, m.InSettle("room-dm-000001", at(11)),
		"a change in one room must not suspend probing in another")
}

func TestMembershipModel_OracleAgreement_NoViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))
	m.RecordOracle("room-small-000001", []string{"u-1", "u-2", "u-3", "u-9"}, 1)

	assert.Empty(t, m.Finalize())
}

func TestMembershipModel_OracleMissingAdd_IsViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))
	// subscription.list never picked up the add.
	m.RecordOracle("room-small-000001", []string{"u-1", "u-2", "u-3"}, 1)

	vs := m.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMembershipNotApplied, vs[0].Kind)
	assert.Equal(t, "room-small-000001", vs[0].RoomID)
	assert.Equal(t, []string{"u-9"}, vs[0].Users)
}

func TestMembershipModel_OracleStaleRemove_IsViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyRemove("room-small-000001", "u-2", at(10))
	// subscription.list still reports the removed member.
	m.RecordOracle("room-small-000001", []string{"u-1", "u-2", "u-3"}, 1)

	vs := m.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMembershipNotApplied, vs[0].Kind)
	assert.Equal(t, []string{"u-2"}, vs[0].Users)
}

func TestMembershipModel_AddedMemberSendRejected_IsViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))
	m.RecordOracle("room-small-000001", []string{"u-1", "u-2", "u-3", "u-9"}, 1)
	m.RecordSendResult("room-small-000001", "u-9", false, 1)

	vs := m.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMembershipAddIneffective, vs[0].Kind)
}

func TestMembershipModel_RemovedMemberSendAccepted_IsViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyRemove("room-small-000001", "u-2", at(10))
	m.RecordOracle("room-small-000001", []string{"u-1", "u-3"}, 1)
	m.RecordSendResult("room-small-000001", "u-2", true, 1)

	vs := m.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMembershipRemoveIneffective, vs[0].Kind,
		"a removed member whose send is still accepted means stale membership on the write path")
}

func TestMembershipModel_EffectiveChanges_NoViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))
	m.RecordOracle("room-small-000001", []string{"u-1", "u-2", "u-3", "u-9"}, 1)
	m.RecordSendResult("room-small-000001", "u-9", true, 1)

	m.ApplyRemove("room-dm-000001", "u-2", at(20))
	m.RecordOracle("room-dm-000001", []string{"u-1"}, 1)
	m.RecordSendResult("room-dm-000001", "u-2", false, 1)

	assert.Empty(t, m.Finalize())
}

func TestMembershipModel_MembersAtEpoch_ReturnsHistoricalSet(t *testing.T) {
	m := modelForTest()
	before := m.Members("room-small-000001")
	m.ApplyAdd("room-small-000001", "u-9", at(10))

	// A probe published at epoch 0 must be judged against epoch 0's set even
	// after the epoch advances.
	assert.Equal(t, before, m.MembersAtEpoch("room-small-000001", 0))
	assert.Equal(t, []string{"u-1", "u-2", "u-3", "u-9"}, m.MembersAtEpoch("room-small-000001", 1))
}

// The following tests supplement the 13 above (verbatim from the task brief)
// to reach the project's 90%+ coverage target on this file: unknown-room and
// out-of-range-epoch zero-value behavior, the ApplyAdd/ApplyRemove no-op on an
// unregistered room, and Counts, none of which the brief's 13 tests exercise.

func TestMembershipModel_UnknownRoom_ReturnsZeroValues(t *testing.T) {
	m := modelForTest()

	assert.Equal(t, 0, m.Epoch("room-does-not-exist"))
	assert.Nil(t, m.Members("room-does-not-exist"))
	assert.False(t, m.InSettle("room-does-not-exist", at(0)))
	assert.Nil(t, m.MembersAtEpoch("room-does-not-exist", 0))
}

func TestMembershipModel_MembersAtEpoch_OutOfRange_ReturnsNil(t *testing.T) {
	m := modelForTest()

	assert.Nil(t, m.MembersAtEpoch("room-small-000001", -1), "negative epoch")
	assert.Nil(t, m.MembersAtEpoch("room-small-000001", 1), "epoch never reached")
}

func TestMembershipModel_ApplyChange_UnknownRoom_IsNoop(t *testing.T) {
	m := modelForTest()

	m.ApplyAdd("room-does-not-exist", "u-9", at(10))
	m.ApplyRemove("room-does-not-exist", "u-1", at(10))

	assert.Equal(t, 0, m.Epoch("room-does-not-exist"))
	assert.Equal(t, ChangeCounts{}, m.Counts(), "no change should be recorded for an unknown room")
}

func TestMembershipModel_Counts(t *testing.T) {
	m := modelForTest()

	// Add: oracle agrees, send accepted -> applied and effective.
	m.ApplyAdd("room-small-000001", "u-9", at(10))
	m.RecordOracle("room-small-000001", []string{"u-1", "u-2", "u-3", "u-9"}, 1)
	m.RecordSendResult("room-small-000001", "u-9", true, 1)

	// Remove: oracle disagrees (stale), send never observed -> not applied,
	// and not counted effective since sendSeen is false.
	m.ApplyRemove("room-dm-000001", "u-2", at(20))
	m.RecordOracle("room-dm-000001", []string{"u-1", "u-2"}, 1)

	assert.Equal(t, ChangeCounts{
		Total: 2, Adds: 1, Removes: 1, Applied: 1, Effective: 1,
	}, m.Counts())
}

// TestMembershipModel_MembersAtEpoch_AlignsWithEachSequentialChange pins the
// history-indexing invariant directly, rather than relying on statement
// coverage to catch an off-by-one. An implementation that appends to history
// before incrementing epoch (or vice versa), or that seeds history empty
// instead of with the epoch-0 set, would misjudge every late-delivered probe
// while still executing every line in apply/MembersAtEpoch — so this test
// checks alignment at every epoch produced by a run of sequential changes,
// not just epoch 0 and 1. Adds and removes are interleaved so a transposition
// between epochs (not just a missing append) would also show up.
func TestMembershipModel_MembersAtEpoch_AlignsWithEachSequentialChange(t *testing.T) {
	m := modelForTest()

	epoch0 := m.MembersAtEpoch("room-small-000001", 0)
	require.Equal(t, []string{"u-1", "u-2", "u-3"}, epoch0)

	m.ApplyAdd("room-small-000001", "u-9", at(10))    // epoch 1: {u-1,u-2,u-3,u-9}
	m.ApplyRemove("room-small-000001", "u-2", at(20)) // epoch 2: {u-1,u-3,u-9}
	m.ApplyAdd("room-small-000001", "u-5", at(30))    // epoch 3: {u-1,u-3,u-5,u-9}

	require.Equal(t, 3, m.Epoch("room-small-000001"))

	assert.Equal(t, []string{"u-1", "u-2", "u-3"}, m.MembersAtEpoch("room-small-000001", 0),
		"epoch 0 must remain the seeded set even after later changes")
	assert.Equal(t, []string{"u-1", "u-2", "u-3", "u-9"}, m.MembersAtEpoch("room-small-000001", 1),
		"epoch 1 must be the set right after the first add, not epoch 0 or 2")
	assert.Equal(t, []string{"u-1", "u-3", "u-9"}, m.MembersAtEpoch("room-small-000001", 2),
		"epoch 2 must be the set right after the remove")
	assert.Equal(t, []string{"u-1", "u-3", "u-5", "u-9"}, m.MembersAtEpoch("room-small-000001", 3),
		"epoch 3 must be the set right after the second add, and must equal current Members")
	assert.Equal(t, m.Members("room-small-000001"), m.MembersAtEpoch("room-small-000001", 3),
		"current epoch's history entry must match Members")

	assert.Nil(t, m.MembersAtEpoch("room-small-000001", 4), "epoch beyond current must be nil, not panic")
	assert.Nil(t, m.MembersAtEpoch("room-small-000001", -1), "negative epoch must be nil, not panic")
}

// TestMembershipModel_Members_ReturnsCopyNotReference pins the copy contract
// documented on Members: mutating the returned slice must never be able to
// corrupt the model's internal state for a later caller.
func TestMembershipModel_Members_ReturnsCopyNotReference(t *testing.T) {
	m := modelForTest()

	got := m.Members("room-small-000001")
	got[0] = "tampered"

	assert.Equal(t, []string{"u-1", "u-2", "u-3"}, m.Members("room-small-000001"),
		"mutating a slice returned by Members must not affect the model")
}

// TestMembershipModel_MembersAtEpoch_ReturnsCopyNotReference pins the same
// copy contract for MembersAtEpoch, whose result is a past history entry —
// corrupting it would poison every future read of that epoch, including late
// probes judged against it.
func TestMembershipModel_MembersAtEpoch_ReturnsCopyNotReference(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))

	got := m.MembersAtEpoch("room-small-000001", 0)
	got[0] = "tampered"

	assert.Equal(t, []string{"u-1", "u-2", "u-3"}, m.MembersAtEpoch("room-small-000001", 0),
		"mutating a slice returned by MembersAtEpoch must not affect the model's stored history")
}
