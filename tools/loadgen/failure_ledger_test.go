package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailureLedger_FinalizesOnlyAfterEveryObservation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 2,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, ledger.Start(&failureOperation{
		ID: "message-1", Scenario: soakFailureScenario, Lane: "message_send",
		StartedAt: now, VerifyAfter: now.Add(10 * time.Second),
		Deadline: now.Add(time.Minute),
		Expected: []failureObserver{failureObserverAdmission, failureObserverHistory},
	}))

	finalized, err := ledger.Observe(
		"message-1", failureObserverAdmission, failureObservationGood, now.Add(time.Second),
	)
	require.NoError(t, err)
	assert.False(t, finalized)
	assert.Equal(t, 1, ledger.Snapshot().Active)

	finalized, err = ledger.Observe(
		"message-1", failureObserverHistory, failureObservationGood, now.Add(11*time.Second),
	)
	require.NoError(t, err)
	assert.True(t, finalized)

	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, uint64(1), snapshot.Results[failureResultGood])
}

func TestFailureLedger_RejectedAdmissionDoesNotBecomeMissingSideEffect(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&failureOperation{
		ID: "ambiguous-message", Scenario: soakFailureScenario, Lane: "message_send",
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Expected: []failureObserver{failureObserverAdmission, failureObserverHistory},
	}))

	_, err = ledger.Observe(
		"ambiguous-message", failureObserverAdmission, failureObservationBad, now,
	)
	require.NoError(t, err)

	finalized, err := ledger.Expire(now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, finalized)
	assert.Equal(
		t, uint64(1),
		ledger.Snapshot().Results[failureResultBad],
	)
}

func TestFailureLedger_ClaimsDueOperationOnceUntilReleased(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&failureOperation{
		ID: "message-1", Scenario: soakFailureScenario, Lane: "message_send",
		StartedAt: now, VerifyAfter: now.Add(10 * time.Second),
		Deadline: now.Add(time.Minute),
		Expected: []failureObserver{failureObserverAdmission, failureObserverHistory},
	}))

	_, ok := ledger.ClaimDue(now.Add(9 * time.Second))
	assert.False(t, ok)

	operation, ok := ledger.ClaimDue(now.Add(10 * time.Second))
	require.True(t, ok)
	assert.Equal(t, "message-1", operation.ID)
	_, ok = ledger.ClaimDue(now.Add(10 * time.Second))
	assert.False(t, ok)

	require.NoError(t, ledger.ReleaseClaim("message-1", now.Add(20*time.Second)))
	_, ok = ledger.ClaimDue(now.Add(19 * time.Second))
	assert.False(t, ok)
	_, ok = ledger.ClaimDue(now.Add(20 * time.Second))
	assert.True(t, ok)
}

func TestFailureLedger_CapacityFailureInvalidatesRun(t *testing.T) {
	now := time.Now().UTC()
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("message-1", now)))

	err = ledger.Start(testFailureOperation("message-2", now))
	require.Error(t, err)
	assert.ErrorIs(t, err, errFailureLedgerCapacity)
	assert.Equal(t, "capacity", ledger.Snapshot().InvalidReason)
}

func TestFailureLedger_FileWALRecoversUnresolvedOperation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 2, Journal: wal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("message-1", now)))
	_, err = ledger.Observe(
		"message-1", failureObserverAdmission, failureObservationGood, now.Add(time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, ledger.Close())

	reopenedWAL, err := openFailureWAL(path)
	require.NoError(t, err)
	recovered, err := newFailureLedger(failureLedgerConfig{
		Capacity: 2, Journal: reopenedWAL, Now: func() time.Time { return now.Add(2 * time.Second) },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })

	snapshot := recovered.Snapshot()
	assert.Equal(t, 1, snapshot.Active)
	assert.Equal(t, 1, snapshot.Recovered)
	operation, ok := recovered.ClaimDue(now.Add(10 * time.Second))
	require.True(t, ok)
	assert.Equal(t, "message-1", operation.ID)
}

func TestFailureLedger_CompactsFinalizedHistoryToActiveSet(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 3, CompactEvery: 1, Journal: wal,
		Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("completed", now)))
	require.NoError(t, ledger.Start(testFailureOperation("active", now)))
	_, err = ledger.Observe(
		"completed", failureObserverAdmission, failureObservationGood, now,
	)
	require.NoError(t, err)
	_, err = ledger.Observe(
		"completed", failureObserverHistory, failureObservationGood, now,
	)
	require.NoError(t, err)
	compactedSize := ledger.Snapshot().JournalBytes
	require.NoError(t, ledger.Close())

	reopenedWAL, err := openFailureWAL(path)
	require.NoError(t, err)
	recovered, err := newFailureLedger(failureLedgerConfig{
		Capacity: 3, CompactEvery: 1, Journal: reopenedWAL,
		Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	assert.Equal(t, 1, recovered.Snapshot().Active)
	assert.Less(t, compactedSize, int64(1000))
	operation, ok := recovered.ClaimDue(now.Add(10 * time.Second))
	require.True(t, ok)
	assert.Equal(t, "active", operation.ID)
}

func TestFailureLedger_WALAppendFailureDoesNotPublishOperation(t *testing.T) {
	wantErr := errors.New("disk full")
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1,
		Journal:  &failingFailureJournal{err: wantErr},
	})
	require.NoError(t, err)

	err = ledger.Start(testFailureOperation("message-1", time.Now().UTC()))
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, "wal", snapshot.InvalidReason)
}

func TestFailureWAL_ReplayIgnoresTornFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	require.NoError(t, wal.Append(&failureLedgerEvent{
		Type:      failureLedgerEventStarted,
		Operation: testFailureOperation("message-1", time.Now().UTC()),
		At:        time.Now().UTC(),
	}))
	require.NoError(t, wal.Close())
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = file.WriteString(`{"type":"observed","operationId":"message-1"`)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	reopened, err := openFailureWAL(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	events, err := reopened.Replay()
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, failureLedgerEventStarted, events[0].Type)
	require.NoError(t, reopened.Append(&failureLedgerEvent{
		Type: failureLedgerEventObserved, OperationID: "message-1",
		Observer: failureObserverAdmission, Observation: failureObservationGood,
		At: time.Now().UTC(),
	}))
	require.NoError(t, reopened.Close())
	reopenedAgain, err := openFailureWAL(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopenedAgain.Close()) })
	events, err = reopenedAgain.Replay()
	require.NoError(t, err)
	assert.Len(t, events, 2)
}

func testFailureOperation(id string, now time.Time) *failureOperation {
	return &failureOperation{
		ID: id, Scenario: soakFailureScenario, Lane: "message_send",
		StartedAt: now, VerifyAfter: now.Add(10 * time.Second),
		Deadline: now.Add(time.Minute),
		Expected: []failureObserver{failureObserverAdmission, failureObserverHistory},
	}
}

type failingFailureJournal struct {
	err error
}

func (j *failingFailureJournal) Replay() ([]failureLedgerEvent, error) { return nil, nil }
func (j *failingFailureJournal) Append(*failureLedgerEvent) error      { return j.err }
func (j *failingFailureJournal) Compact([]failureLedgerEvent) error    { return j.err }
func (j *failingFailureJournal) Size() int64                           { return 0 }
func (j *failingFailureJournal) Close() error                          { return nil }

func TestFailureObserverContract_DeclaresPerLaneObservers(t *testing.T) {
	contract := newFailureObserverContract(false)

	assert.Equal(t, 2, contract.SchemaVersion)
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverHistory},
		contract.Lanes[soakFailureLaneMessageSend])
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverRoomState},
		contract.Lanes[soakFailureLaneMemberMutation])
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverRoomState},
		contract.Lanes[soakFailureLaneRoomMutation])
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverRoomState},
		contract.Lanes[soakFailureLaneRoomCreate])
	require.NoError(t, validateFailureObserverContract(contract))
}

func TestFailureObserverContract_RecipientOnlyAffectsMessageLane(t *testing.T) {
	contract := newFailureObserverContract(true)

	assert.Contains(t, contract.Lanes[soakFailureLaneMessageSend], failureObserverRecipient)
	assert.NotContains(t, contract.Lanes[soakFailureLaneMemberMutation], failureObserverRecipient)
	require.NoError(t, validateFailureObserverContract(contract))
}

func TestFailureOperationMatchesObserverContract_UsesOperationLane(t *testing.T) {
	contract := newFailureObserverContract(false)
	operation := failureOperation{
		Scenario: soakFailureScenario,
		Lane:     soakFailureLaneMemberMutation,
		Expected: []failureObserver{failureObserverAdmission, failureObserverRoomState},
	}

	assert.True(t, failureOperationMatchesObserverContract(&operation, contract))

	operation.Lane = soakFailureLaneMessageSend
	assert.False(t, failureOperationMatchesObserverContract(&operation, contract))
}

func TestFailureWALPath_SeparatesRunAndEpoch(t *testing.T) {
	directory := t.TempDir()

	assert.Equal(t, filepath.Join(directory, "run-1.v2.wal"), failureWALPath(directory, "run-1", "v2"))
	assert.Equal(t, filepath.Join(directory, "run-1.v1.wal"), failureWALPath(directory, "run-1", ""))
}

func TestFailureLedger_StartsRoomLaneOperations(t *testing.T) {
	now := time.Date(2026, 8, 16, 4, 5, 6, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 4,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)

	for _, testCase := range []struct {
		name          string
		lane          string
		operationType failureOperationType
		effects       []failureExpectedEffect
	}{
		{
			name: "member add", lane: soakFailureLaneMemberMutation,
			operationType: failureOperationMemberAdd, effects: memberMutationExpectedEffects(),
		},
		{
			name: "room rename", lane: soakFailureLaneRoomMutation,
			operationType: failureOperationRoomRename,
			effects:       roomMutationExpectedEffects(failureOperationRoomRename),
		},
		{
			name: "mute toggle", lane: soakFailureLaneRoomMutation,
			operationType: failureOperationMuteToggle,
			effects:       roomMutationExpectedEffects(failureOperationMuteToggle),
		},
		{
			name: "room create", lane: soakFailureLaneRoomCreate,
			operationType: failureOperationRoomCreate, effects: roomCreateExpectedEffects(),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			operation := &failureOperation{
				SchemaVersion: 2, ID: "operation-" + string(testCase.operationType),
				RunID: "run-1", Scenario: soakFailureScenario, Lane: testCase.lane,
				OperationType: testCase.operationType, LifecycleState: failureOperationJournaled,
				StartedAt: now, VerifyAfter: now.Add(time.Second), Deadline: now.Add(time.Minute),
				Targets: map[string]string{"roomId": "room-1"},
				Effects: testCase.effects,
			}

			require.NoError(t, ledger.Start(operation))

			active, ok := ledger.Active(operation.ID)
			require.True(t, ok)
			assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverRoomState},
				active.Expected)
		})
	}
}

func TestValidateFailureOperation_RejectsUnknownOperationType(t *testing.T) {
	now := time.Date(2026, 8, 16, 4, 5, 6, 0, time.UTC)
	operation := &failureOperation{
		SchemaVersion: 2, ID: "operation-1", RunID: "run-1",
		Scenario: soakFailureScenario, Lane: soakFailureLaneMemberMutation,
		OperationType: "member_promote", LifecycleState: failureOperationJournaled,
		StartedAt: now, VerifyAfter: now.Add(time.Second), Deadline: now.Add(time.Minute),
		Targets: map[string]string{"roomId": "room-1"},
		Effects: memberMutationExpectedEffects(),
	}

	err := validateFailureOperation(operation)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
}

func TestFailureLedger_ClaimDueLanesKeepsLanesSeparate(t *testing.T) {
	now := time.Date(2026, 8, 16, 6, 0, 0, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 4, Now: func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, ledger.Start(&failureOperation{
		SchemaVersion: 2, ID: "message-1", RunID: "run-1",
		Scenario: soakFailureScenario, Lane: soakFailureLaneMessageSend,
		OperationType: failureOperationMessageCreate, LifecycleState: failureOperationJournaled,
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Targets: map[string]string{"messageId": "message-1"},
		Effects: messageCreateExpectedEffectsForObservers(false, 0, ""),
	}))
	require.NoError(t, ledger.Start(&failureOperation{
		SchemaVersion: 2, ID: "member-1", RunID: "run-1",
		Scenario: soakFailureScenario, Lane: soakFailureLaneMemberMutation,
		OperationType: failureOperationMemberAdd, LifecycleState: failureOperationJournaled,
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Targets: map[string]string{"roomId": "room-1"},
		Effects: memberMutationExpectedEffects(),
	}))

	claimed, ok := ledger.ClaimDueLanes(now, []string{soakFailureLaneMemberMutation})
	require.True(t, ok)
	assert.Equal(t, "member-1", claimed.ID,
		"a lane-scoped claim must never hand an operation to the wrong verifier")

	_, ok = ledger.ClaimDueLanes(now, []string{soakFailureLaneMemberMutation})
	assert.False(t, ok)

	remaining, ok := ledger.ClaimDue(now)
	require.True(t, ok)
	assert.Equal(t, "message-1", remaining.ID)
}

func TestFailureLedger_ClaimDueLanesRejectsAnEmptyLaneSet(t *testing.T) {
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)

	_, ok := ledger.ClaimDueLanes(time.Now(), nil)

	assert.False(t, ok)
}

func TestFailureLedger_RoomLaneIsClaimableFromItsVerifyTime(t *testing.T) {
	now := time.Date(2026, 8, 16, 6, 0, 0, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 2, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&failureOperation{
		SchemaVersion: 2, ID: "member-1", RunID: "run-1",
		Scenario: soakFailureScenario, Lane: soakFailureLaneMemberMutation,
		OperationType: failureOperationMemberAdd, LifecycleState: failureOperationJournaled,
		StartedAt: now, VerifyAfter: now.Add(10 * time.Second),
		Deadline: now.Add(10 * time.Minute),
		Targets:  map[string]string{"roomId": "room-1"},
		Effects:  memberMutationExpectedEffects(),
	}))

	_, ok := ledger.ClaimDueLanes(now.Add(time.Second), []string{soakFailureLaneMemberMutation})
	assert.False(t, ok, "the persist grace has not elapsed yet")

	claimed, ok := ledger.ClaimDueLanes(
		now.Add(11*time.Second), []string{soakFailureLaneMemberMutation},
	)
	require.True(t, ok)
	assert.Equal(t, "member-1", claimed.ID,
		"a query observer must poll from its verify time, not wait for the deadline")
}
