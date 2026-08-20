package main

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newExpiryTestEvidence pins the clock so retention can be asserted without
// sleeping.
func newExpiryTestEvidence(at *time.Time) *recipientEvidence {
	evidence := newRecipientEvidence(false)
	evidence.now = func() time.Time { return *at }
	return evidence
}

// The evidence map is emptied only by Finalize, which runs inside the
// reconciler. An operation that reaches its deadline without being reconciled
// is finalized by failureLedger.Expire — which drops it from the ledger and
// never tells the evidence, so its expectation stays for the life of the
// process. At 55 sends/s with 100 recipients each that is ~20M per-recipient
// route maps an hour, and it is what an 8GB pod dies of.
func TestRecipientEvidence_ExpiresExpectationsOlderThanTheCutoff(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	evidence := newExpiryTestEvidence(&clock)

	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op-old", Recipients: []string{"a", "b"}, Complete: true,
	}))
	clock = clock.Add(10 * time.Minute)
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op-recent", Recipients: []string{"c"}, Complete: true,
	}))

	expired := evidence.ExpireBefore(clock.Add(-5 * time.Minute))

	assert.Equal(t, 1, expired)
	assert.Equal(t, 1, evidence.Len(),
		"the entry older than the cutoff must go, the recent one must stay")
}

// Expiry has to release the observations too, not just the outer entry: the
// per-recipient route maps incrementRecipientRoute allocates are where the
// bytes actually are.
func TestRecipientEvidence_ExpiryReleasesObservedRoutes(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	evidence := newExpiryTestEvidence(&clock)
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op", Recipients: []string{"a", "b", "c"}, Complete: true,
	}))
	for _, recipient := range []string{"a", "b", "c"} {
		require.True(t, evidence.Observe("op", recipient))
	}

	require.Equal(t, 1, evidence.ExpireBefore(clock.Add(time.Minute)))

	assert.Zero(t, evidence.Len())
	assert.Equal(t, recipientEvidenceUntracked,
		evidence.ObserveDelivery("op", "a", "", "", recipientDeliveryRouteRoomGlobal),
		"a released expectation must not be resurrected by a late delivery")
}

// Nothing may be released before its operation could still be reconciled, or
// the sweep would turn a verifiable delivery into an unverified one.
func TestRecipientEvidence_KeepsExpectationsInsideTheRetentionWindow(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	evidence := newExpiryTestEvidence(&clock)
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op", Recipients: []string{"a"}, Complete: true,
	}))

	assert.Zero(t, evidence.ExpireBefore(clock.Add(-time.Nanosecond)))
	assert.Equal(t, 1, evidence.Len())
}

// A nil evidence is the observer-disabled case, which the sweep runs into on
// every tick of a default configuration.
func TestRecipientEvidence_ExpiryToleratesADisabledObserver(t *testing.T) {
	var evidence *recipientEvidence

	assert.Zero(t, evidence.ExpireBefore(time.Unix(1000, 0).UTC()))
	assert.Zero(t, evidence.Len())
}

// Expiry runs on the same sweep as the ledger's, so the evidence cannot outlive
// the operations it describes even when nothing reconciles.
func TestRunSoakFailureExpiry_ExpiresRecipientEvidenceOnEveryTick(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, Now: func() time.Time { return clock },
	})
	require.NoError(t, err)

	evidence := newExpiryTestEvidence(&clock)
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op", Recipients: []string{"a"}, Complete: true,
	}))
	require.Equal(t, 1, evidence.Len())

	ticks := make(chan time.Time, 1)
	ticks <- clock.Add(11 * time.Minute)
	close(ticks)
	runSoakFailureExpiry(context.Background(), ledger, evidence, 10*time.Minute, ticks, nil)

	assert.Zero(t, evidence.Len(),
		"the sweep that expires ledger operations must expire their evidence too")
}

// The retention window is the operation deadline, so an expectation whose
// operation is still live must survive the sweep.
func TestRunSoakFailureExpiry_KeepsEvidenceForStillLiveOperations(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, Now: func() time.Time { return clock },
	})
	require.NoError(t, err)

	evidence := newExpiryTestEvidence(&clock)
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op", Recipients: []string{"a"}, Complete: true,
	}))

	ticks := make(chan time.Time, 1)
	ticks <- clock.Add(time.Minute)
	close(ticks)
	runSoakFailureExpiry(context.Background(), ledger, evidence, 10*time.Minute, ticks, nil)

	assert.Equal(t, 1, evidence.Len(),
		"an operation one minute into a ten minute deadline is still verifiable")
}

// Without a gauge the only way to see this map growing again is a heap
// profile, which is how it went unnoticed for the whole investigation. A
// healthy run tracks the ledger's own in-flight count; a run climbing past it
// is this leaking again.
func TestRunSoakFailureExpiry_PublishesTheRetainedExpectationCount(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, Now: func() time.Time { return clock },
	})
	require.NoError(t, err)

	metrics := NewMetrics()
	evidence := newExpiryTestEvidence(&clock)
	for _, id := range []string{"op-1", "op-2", "op-3"} {
		require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
			OperationID: id, Recipients: []string{"a"}, Complete: true,
		}))
	}

	ticks := make(chan time.Time, 1)
	ticks <- clock.Add(time.Minute)
	close(ticks)
	runSoakFailureExpiry(context.Background(), ledger, evidence, 10*time.Minute, ticks, nil,
		withSoakFailureExpiryMetrics(metrics))

	assert.Equal(t, float64(3), testutil.ToFloat64(metrics.FailureRecipientExpectations))
}

// A zero retention makes the cutoff equal to now, which would release every
// expectation registered so far — including operations still inside their
// deadline, turning verifiable deliveries into unverified ones. Config
// validation forbids a zero SOAK_RECONCILE_DEADLINE, so this is a guard against
// a caller mistake rather than a reachable configuration, and the sweep must
// decline to expire rather than guess a window.
func TestRunSoakFailureExpiry_DoesNotExpireWithoutARetentionWindow(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, Now: func() time.Time { return clock },
	})
	require.NoError(t, err)

	evidence := newExpiryTestEvidence(&clock)
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op", Recipients: []string{"a"}, Complete: true,
	}))

	ticks := make(chan time.Time, 1)
	ticks <- clock.Add(time.Hour)
	close(ticks)
	runSoakFailureExpiry(context.Background(), ledger, evidence, 0, ticks, nil)

	assert.Equal(t, 1, evidence.Len(),
		"no retention window means no basis to expire, not expire everything")
}

// Registration time plus the *current* retention is not the same bound the
// ledger uses. The ledger honours each operation's persisted deadline, so an
// operation accepted under a 60m deadline and recovered into a process
// configured for 10m stays live in the ledger while a retention-only sweep
// releases its evidence after 10m — and a delivery arriving in between is
// treated as untracked. An expectation that knows its own deadline must be held
// to that one.
func TestRecipientEvidence_HoldsAnExpectationToItsOwnDeadline(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	evidence := newExpiryTestEvidence(&clock)
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "recovered", Recipients: []string{"a"}, Complete: true,
		Deadline: clock.Add(time.Hour),
	}))

	// A ten-minute retention would release a ten-minute-old registration.
	assert.Zero(t, evidence.ExpireBefore(clock.Add(-10*time.Minute)))
	assert.Equal(t, 1, evidence.Len(),
		"its own deadline outranks the sweep's retention window")

	clock = clock.Add(time.Hour + time.Second)
	assert.Equal(t, 1, evidence.ExpireBefore(clock.Add(-10*time.Minute)))
}

// The ledger's expiry is what finalizes the operation the evidence belongs to.
// If that write fails the operation is still live and still reconcilable, so
// releasing its evidence on the same tick destroys the only record of what was
// delivered.
func TestRunSoakFailureExpiry_KeepsEvidenceWhenLedgerExpiryFails(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 16, Now: func() time.Time { return clock },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Close())

	evidence := newExpiryTestEvidence(&clock)
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op", Recipients: []string{"a"}, Complete: true,
	}))

	var swept []error
	ticks := make(chan time.Time, 1)
	ticks <- clock.Add(time.Hour)
	close(ticks)
	runSoakFailureExpiry(context.Background(), ledger, evidence, 10*time.Minute, ticks,
		func(err error) { swept = append(swept, err) })

	require.NotEmpty(t, swept, "a failed ledger expiry must be reported")
	assert.Equal(t, 1, evidence.Len(),
		"evidence for an operation the ledger could not finalize must survive the tick")
}

// Gating expiry on the ledger reinstates the leak if the ledger keeps failing,
// so the map needs a ceiling of its own — the ledger has LEDGER_CAPACITY and
// this had nothing.
func TestRecipientEvidence_RefusesToGrowPastItsCapacity(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	evidence := newExpiryTestEvidence(&clock)
	evidence.capacity = 2

	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op-1", Recipients: []string{"a"}, Complete: true,
	}))
	require.NoError(t, evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op-2", Recipients: []string{"a"}, Complete: true,
	}))

	err := evidence.ExpectDelivery(&recipientExpectationConfig{
		OperationID: "op-3", Recipients: []string{"a"}, Complete: true,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "capacity")
	assert.Equal(t, 2, evidence.Len())
}
