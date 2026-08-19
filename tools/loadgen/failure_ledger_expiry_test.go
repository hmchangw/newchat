package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startExpiredOperations(t *testing.T, ledger *failureLedger, count int, at time.Time) {
	t.Helper()
	for i := range count {
		operation := testFailureOperation(fmt.Sprintf("stale-%05d", i), at)
		operation.Deadline = at.Add(20 * time.Second)
		require.NoError(t, ledger.Start(operation))
	}
}

// A sweep used to walk the whole active set, writing two or three journal
// records for every expired operation while holding the ledger lock. At the
// capacity this ledger runs at that is a stall every lane feels, and the lanes
// that feel it worst are the ones with the largest replies. Bounding the batch
// makes the pause predictable; the backlog drains over the following sweeps.
func TestFailureLedger_ExpireRetiresAtMostOneBatchPerSweep(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1000, ExpireBatch: 25, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	startExpiredOperations(t, ledger, 100, now)

	finalized, err := ledger.Expire(now.Add(time.Minute))

	require.NoError(t, err)
	assert.Equal(t, 25, finalized)
	assert.Len(t, ledger.active, 75)
}

func TestFailureLedger_ExpireDrainsTheBacklogOverSuccessiveSweeps(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1000, ExpireBatch: 25, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	startExpiredOperations(t, ledger, 100, now)

	total := 0
	for range 4 {
		finalized, err := ledger.Expire(now.Add(time.Minute))
		require.NoError(t, err)
		total += finalized
	}

	assert.Equal(t, 100, total)
	assert.Empty(t, ledger.active)
}

func TestFailureLedger_ExpireIsUnboundedWhenNoBatchIsConfigured(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1000, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	startExpiredOperations(t, ledger, 60, now)

	finalized, err := ledger.Expire(now.Add(time.Minute))

	require.NoError(t, err)
	assert.Equal(t, 60, finalized)
}
