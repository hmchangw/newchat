package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Reconcile lag is read against SOAK_RECONCILE_DEADLINE — the validity rule is
// that lag approaching the deadline means the window's unverified verdicts are
// not evidence. A histogram whose largest finite bucket sits at the deadline
// puts exactly that region in +Inf, where no quantile can resolve it, and a
// deadline configured higher loses the whole range.
func TestSoakReconcileLagBuckets_ResolveLagPastTheSupportedDeadline(t *testing.T) {
	metrics := NewMetrics()
	buckets := soakReconcileLagBuckets()

	require.NotEmpty(t, buckets)
	largest := buckets[len(buckets)-1]

	assert.GreaterOrEqual(t, largest, soakReconcileLagCeiling.Seconds(),
		"the largest finite bucket must cover the deadline the run supports")

	cfg := validSoakConfig(t)
	assert.Less(t, cfg.ReconcileDeadline.Seconds(), largest,
		"the default deadline must sit inside the resolvable range, not on its edge")

	// A lag at the default deadline lands in a finite bucket rather than +Inf.
	metrics.FailureReconcileLag.Observe(cfg.ReconcileDeadline.Seconds())
	assert.Equal(t, uint64(1), soakReconcileLagCount(t, metrics))
}

// A deadline past the resolvable range is not refused — the operator decides
// what a run is worth — but it must not be silent, because lag is the signal
// that says whether the window counted.
func TestWarnSoakReconcileLagRange_SaysWhenTheDeadlineOutrunsTheHistogram(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.ReconcileDeadline = soakReconcileLagCeiling + time.Minute

	output := captureSoakLog(t)
	warnSoakReconcileLagRange(&cfg)

	logged := output.String()
	require.Contains(t, logged, `"level":"WARN"`)
	assert.Contains(t, logged, "SOAK_RECONCILE_DEADLINE")
}

func TestWarnSoakReconcileLagRange_SaysNothingInsideTheRange(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.ReconcileDeadline = soakReconcileLagCeiling

	output := captureSoakLog(t)
	warnSoakReconcileLagRange(&cfg)

	assert.Empty(t, output.String())
}

// Both reconcile-configuration checks must be reachable from one startup entry
// point. A check that is defined and unit-tested but never called from startup
// warns nobody — which is what happened to the lag-range check until this test
// pinned the two together.
func TestWarnSoakReconcileConfig_ReportsCapacityAndLagRangeTogether(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.SendRate = 100
	cfg.ReadRate = 1
	cfg.ReconcileReadShare = 0.1
	cfg.ReconcileDeadline = soakReconcileLagCeiling + time.Minute

	output := captureSoakLog(t)
	warnSoakReconcileConfig(&cfg)

	logged := output.String()
	assert.Contains(t, logged, "below the capacity its observers demand",
		"the capacity floor must still be reported through the combined entry point")
	assert.Contains(t, logged, "outruns the lag histogram",
		"the lag-range check must be reachable from startup, not only from tests")
}
