package failure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailureLedger_InvalidationKeepsFirstCauseAndBoundsLaterReasons(t *testing.T) {
	journal := &memoryFailureJournal{}
	ledger, err := NewLedger(&LedgerConfig{Capacity: 4, Journal: journal})
	require.NoError(t, err)

	ledger.Invalidate(InvalidReasonReconcileCapacity)
	ledger.Invalidate(InvalidReasonWAL)
	ledger.Invalidate(InvalidReasonWAL)
	ledger.Invalidate("future_reason")

	assert.Equal(t, InvalidReasonReconcileCapacity, ledger.Snapshot().InvalidReason)
	assert.Equal(t, []string{InvalidReasonReconcileCapacity, InvalidReasonWAL, "other"}, invalidationReasons(journal.events))
	assert.Empty(t, ledger.UnpersistedInvalidations())
}

func TestFailureLedger_InvalidationReportsAndRetriesAnUnpersistedCause(t *testing.T) {
	journal := &memoryFailureJournal{refuseNext: true}
	ledger, err := NewLedger(&LedgerConfig{Capacity: 4, Journal: journal})
	require.NoError(t, err)

	ledger.Invalidate(InvalidReasonWAL)
	assert.Equal(t, []string{InvalidReasonWAL}, ledger.UnpersistedInvalidations())

	journal.refuseNext = false
	ledger.Invalidate(InvalidReasonCapacity)
	assert.Empty(t, ledger.UnpersistedInvalidations())
	assert.Equal(t, []string{InvalidReasonWAL, InvalidReasonCapacity}, invalidationReasons(journal.events))
}

func TestFailureLedger_ValidatesCapacityAndInvalidationReasons(t *testing.T) {
	_, err := NewLedger(&LedgerConfig{})
	require.ErrorContains(t, err, "capacity must be greater than zero")
	assert.True(t, ValidInvalidationReason(InvalidReasonWAL))
	assert.False(t, ValidInvalidationReason("unbounded_reason"))
}

func TestFailureLedger_ListsActiveOperationsInStableOrder(t *testing.T) {
	var nilLedger *Ledger
	assert.Nil(t, nilLedger.ActiveOperations())

	ledger, err := NewLedger(&LedgerConfig{Capacity: 2})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testLedgerOperation("b", ledger.now())))
	require.NoError(t, ledger.Start(testLedgerOperation("a", ledger.now())))

	operations := ledger.ActiveOperations()
	require.Len(t, operations, 2)
	assert.Equal(t, "a", operations[0].ID)
	assert.Equal(t, "b", operations[1].ID)
}

func invalidationReasons(events []Event) []string {
	reasons := make([]string, 0)
	for _, event := range events {
		if event.Type == EventInvalidated {
			reasons = append(reasons, event.InvalidReason)
		}
	}
	return reasons
}
