package failure

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// messageCreateExpectedEffects is the fully-enabled observer set. Production
// always derives the set from the runtime observer flags, so this shorthand
// exists only for tests that do not exercise those flags.
func TestFailureLedger_FinalizesOnlyAfterEveryObservation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := NewLedger(&LedgerConfig{
		Capacity: 2,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, ledger.Start(&Operation{
		ID: "message-1", Scenario: ScenarioMessageSoak, Lane: "message_send",
		StartedAt: now, VerifyAfter: now.Add(10 * time.Second),
		Deadline: now.Add(time.Minute),
		Expected: []Observer{ObserverAdmission, ObserverHistory},
	}))

	finalized, err := ledger.Observe(
		"message-1", ObserverAdmission, ObservationGood, now.Add(time.Second),
	)
	require.NoError(t, err)
	assert.False(t, finalized)
	assert.Equal(t, 1, ledger.Snapshot().Active)

	finalized, err = ledger.Observe(
		"message-1", ObserverHistory, ObservationGood, now.Add(11*time.Second),
	)
	require.NoError(t, err)
	assert.True(t, finalized)

	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, uint64(1), snapshot.Results[ResultGood])
}

func TestFailureLedger_RejectedAdmissionDoesNotBecomeMissingSideEffect(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := NewLedger(&LedgerConfig{
		Capacity: 1,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&Operation{
		ID: "ambiguous-message", Scenario: ScenarioMessageSoak, Lane: "message_send",
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Expected: []Observer{ObserverAdmission, ObserverHistory},
	}))

	_, err = ledger.Observe(
		"ambiguous-message", ObserverAdmission, ObservationBad, now,
	)
	require.NoError(t, err)

	finalizedIDs, err := ledger.Expire(now.Add(time.Minute))
	finalized := len(finalizedIDs)
	require.NoError(t, err)
	require.Equal(t, 1, finalized)
	assert.Equal(
		t, uint64(1),
		ledger.Snapshot().Results[ResultBad],
	)
}

func TestFailureLedger_ClaimsDueOperationOnceUntilReleased(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := NewLedger(&LedgerConfig{
		Capacity: 1,
		Now:      func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&Operation{
		ID: "message-1", Scenario: ScenarioMessageSoak, Lane: "message_send",
		StartedAt: now, VerifyAfter: now.Add(10 * time.Second),
		Deadline: now.Add(time.Minute),
		Expected: []Observer{ObserverAdmission, ObserverHistory},
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
	ledger, err := NewLedger(&LedgerConfig{Capacity: 1})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testLedgerOperation("message-1", now)))

	err = ledger.Start(testLedgerOperation("message-2", now))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrLedgerCapacity)
	assert.Equal(t, "capacity", ledger.Snapshot().InvalidReason)
}

func TestFailureLedger_FileWALRecoversUnresolvedOperation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := OpenWAL(path)
	require.NoError(t, err)
	ledger, err := NewLedger(&LedgerConfig{
		Capacity: 2, Journal: wal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testLedgerOperation("message-1", now)))
	_, err = ledger.Observe(
		"message-1", ObserverAdmission, ObservationGood, now.Add(time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, ledger.Close())

	reopenedWAL, err := OpenWAL(path)
	require.NoError(t, err)
	recovered, err := NewLedger(&LedgerConfig{
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
	wal, err := OpenWAL(path)
	require.NoError(t, err)
	ledger, err := NewLedger(&LedgerConfig{
		Capacity: 3, CompactEvery: 1, Journal: wal,
		Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testLedgerOperation("completed", now)))
	require.NoError(t, ledger.Start(testLedgerOperation("active", now)))
	_, err = ledger.Observe(
		"completed", ObserverAdmission, ObservationGood, now,
	)
	require.NoError(t, err)
	_, err = ledger.Observe(
		"completed", ObserverHistory, ObservationGood, now,
	)
	require.NoError(t, err)
	compactedSize := ledger.Snapshot().JournalBytes
	require.NoError(t, ledger.Close())

	reopenedWAL, err := OpenWAL(path)
	require.NoError(t, err)
	recovered, err := NewLedger(&LedgerConfig{
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
	ledger, err := NewLedger(&LedgerConfig{
		Capacity: 1,
		Journal:  &failingJournal{err: wantErr},
	})
	require.NoError(t, err)

	err = ledger.Start(testLedgerOperation("message-1", time.Now().UTC()))
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, "wal", snapshot.InvalidReason)
}

func TestFailureWAL_ReplayIgnoresTornFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := OpenWAL(path)
	require.NoError(t, err)
	require.NoError(t, wal.Append(&Event{
		Type:      EventStarted,
		Operation: testLedgerOperation("message-1", time.Now().UTC()),
		At:        time.Now().UTC(),
	}))
	require.NoError(t, wal.Close())
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = file.WriteString(`{"type":"observed","operationId":"message-1"`)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	reopened, err := OpenWAL(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	events, err := reopened.Replay()
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, EventStarted, events[0].Type)
	require.NoError(t, reopened.Append(&Event{
		Type: EventObserved, OperationID: "message-1",
		Observer: ObserverAdmission, Observation: ObservationGood,
		At: time.Now().UTC(),
	}))
	require.NoError(t, reopened.Close())
	reopenedAgain, err := OpenWAL(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopenedAgain.Close()) })
	events, err = reopenedAgain.Replay()
	require.NoError(t, err)
	assert.Len(t, events, 2)
}

func testLedgerOperation(id string, now time.Time) *Operation {
	return &Operation{
		ID: id, Scenario: ScenarioMessageSoak, Lane: "message_send",
		StartedAt: now, VerifyAfter: now.Add(10 * time.Second),
		Deadline: now.Add(time.Minute),
		Expected: []Observer{ObserverAdmission, ObserverHistory},
	}
}

type failingJournal struct {
	err error
}

func (j *failingJournal) Replay() ([]Event, error) { return nil, nil }
func (j *failingJournal) Append(*Event) error      { return j.err }
func (j *failingJournal) Compact([]Event) error    { return j.err }
func (j *failingJournal) Size() int64              { return 0 }
func (j *failingJournal) Close() error             { return nil }

func TestFailureObserverContract_DeclaresPerLaneObservers(t *testing.T) {
	contract := NewObserverContract(false, false)

	assert.Equal(t, 2, contract.SchemaVersion)
	assert.Equal(t, []Observer{ObserverAdmission, ObserverHistory},
		contract.Lanes[LaneMessageSend])
	assert.Equal(t, []Observer{ObserverAdmission, ObserverRoomState},
		contract.Lanes[LaneMemberMutation])
	assert.Equal(t, []Observer{ObserverAdmission, ObserverRoomState},
		contract.Lanes[LaneRoomMutation])
	assert.Equal(t, []Observer{ObserverAdmission, ObserverRoomState},
		contract.Lanes[LaneRoomCreate])
	require.NoError(t, ValidateObserverContract(contract))
}

func TestFailureObserverContract_RecipientOnlyAffectsMessageLane(t *testing.T) {
	contract := NewObserverContract(true, false)

	assert.Contains(t, contract.Lanes[LaneMessageSend], ObserverRecipient)
	assert.NotContains(t, contract.Lanes[LaneMemberMutation], ObserverRecipient)
	require.NoError(t, ValidateObserverContract(contract))
}

func TestFailureOperationMatchesObserverContract_UsesOperationLane(t *testing.T) {
	contract := NewObserverContract(false, false)
	operation := Operation{
		Scenario: ScenarioMessageSoak,
		Lane:     LaneMemberMutation,
		Expected: []Observer{ObserverAdmission, ObserverRoomState},
	}

	assert.True(t, OperationMatchesObserverContract(&operation, contract))

	operation.Lane = LaneMessageSend
	assert.False(t, OperationMatchesObserverContract(&operation, contract))
}

func TestFailureLedger_StartsRoomLaneOperations(t *testing.T) {
	now := time.Date(2026, 8, 16, 4, 5, 6, 0, time.UTC)
	for _, testCase := range []struct {
		name          string
		lane          string
		operationType OperationType
		effects       []ExpectedEffect
	}{
		{
			name: "member add", lane: LaneMemberMutation,
			operationType: OperationMemberAdd, effects: MemberMutationExpectedEffects(),
		},
		{
			name: "room rename", lane: LaneRoomMutation,
			operationType: OperationRoomRename,
			effects:       RoomMutationExpectedEffects(OperationRoomRename),
		},
		{
			name: "mute toggle", lane: LaneRoomMutation,
			operationType: OperationMuteToggle,
			effects:       RoomMutationExpectedEffects(OperationMuteToggle),
		},
		{
			name: "room create", lane: LaneRoomCreate,
			operationType: OperationRoomCreate, effects: RoomCreateExpectedEffects(),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ledger, err := NewLedger(&LedgerConfig{
				Capacity: 4,
				Now:      func() time.Time { return now },
			})
			require.NoError(t, err)

			operation := &Operation{
				SchemaVersion: 2, ID: "operation-" + string(testCase.operationType),
				RunID: "run-1", Scenario: ScenarioMessageSoak, Lane: testCase.lane,
				OperationType: testCase.operationType, LifecycleState: OperationJournaled,
				StartedAt: now, VerifyAfter: now.Add(time.Second), Deadline: now.Add(time.Minute),
				Targets: map[string]string{"roomId": "room-1"},
				Effects: testCase.effects,
			}

			require.NoError(t, ledger.Start(operation))

			active, ok := ledger.Active(operation.ID)
			require.True(t, ok)
			assert.Equal(t, []Observer{ObserverAdmission, ObserverRoomState},
				active.Expected)
		})
	}
}

func TestValidateFailureOperation_RejectsUnknownOperationType(t *testing.T) {
	now := time.Date(2026, 8, 16, 4, 5, 6, 0, time.UTC)
	operation := &Operation{
		SchemaVersion: 2, ID: "operation-1", RunID: "run-1",
		Scenario: ScenarioMessageSoak, Lane: LaneMemberMutation,
		OperationType: "member_promote", LifecycleState: OperationJournaled,
		StartedAt: now, VerifyAfter: now.Add(time.Second), Deadline: now.Add(time.Minute),
		Targets: map[string]string{"roomId": "room-1"},
		Effects: MemberMutationExpectedEffects(),
	}

	err := ValidateOperation(operation)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
}

func TestFailureLedger_ClaimDueLanesKeepsLanesSeparate(t *testing.T) {
	now := time.Date(2026, 8, 16, 6, 0, 0, 0, time.UTC)
	ledger, err := NewLedger(&LedgerConfig{
		Capacity: 4, Now: func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, ledger.Start(&Operation{
		SchemaVersion: 2, ID: "message-1", RunID: "run-1",
		Scenario: ScenarioMessageSoak, Lane: LaneMessageSend,
		OperationType: OperationMessageCreate, LifecycleState: OperationJournaled,
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Targets: map[string]string{"messageId": "message-1"},
		Effects: MessageCreateExpectedEffects(false, false, 0, ""),
	}))
	require.NoError(t, ledger.Start(&Operation{
		SchemaVersion: 2, ID: "member-1", RunID: "run-1",
		Scenario: ScenarioMessageSoak, Lane: LaneMemberMutation,
		OperationType: OperationMemberAdd, LifecycleState: OperationJournaled,
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Targets: map[string]string{"roomId": "room-1"},
		Effects: MemberMutationExpectedEffects(),
	}))

	claimed, ok := ledger.ClaimDueLanes(now, []string{LaneMemberMutation})
	require.True(t, ok)
	assert.Equal(t, "member-1", claimed.ID,
		"a lane-scoped claim must never hand an operation to the wrong verifier")

	_, ok = ledger.ClaimDueLanes(now, []string{LaneMemberMutation})
	assert.False(t, ok)

	remaining, ok := ledger.ClaimDue(now)
	require.True(t, ok)
	assert.Equal(t, "message-1", remaining.ID)
}

func TestFailureLedger_ClaimDueLanesRejectsAnEmptyLaneSet(t *testing.T) {
	ledger, err := NewLedger(&LedgerConfig{Capacity: 1})
	require.NoError(t, err)

	_, ok := ledger.ClaimDueLanes(time.Now(), nil)

	assert.False(t, ok)
}

func TestFailureLedger_RoomLaneIsClaimableFromItsVerifyTime(t *testing.T) {
	now := time.Date(2026, 8, 16, 6, 0, 0, 0, time.UTC)
	ledger, err := NewLedger(&LedgerConfig{
		Capacity: 2, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&Operation{
		SchemaVersion: 2, ID: "member-1", RunID: "run-1",
		Scenario: ScenarioMessageSoak, Lane: LaneMemberMutation,
		OperationType: OperationMemberAdd, LifecycleState: OperationJournaled,
		StartedAt: now, VerifyAfter: now.Add(10 * time.Second),
		Deadline: now.Add(10 * time.Minute),
		Targets:  map[string]string{"roomId": "room-1"},
		Effects:  MemberMutationExpectedEffects(),
	}))

	_, ok := ledger.ClaimDueLanes(now.Add(time.Second), []string{LaneMemberMutation})
	assert.False(t, ok, "the persist grace has not elapsed yet")

	claimed, ok := ledger.ClaimDueLanes(
		now.Add(11*time.Second), []string{LaneMemberMutation},
	)
	require.True(t, ok)
	assert.Equal(t, "member-1", claimed.ID,
		"a query observer must poll from its verify time, not wait for the deadline")
}
