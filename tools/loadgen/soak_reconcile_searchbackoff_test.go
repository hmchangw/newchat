package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The amplification this PR exists to bound is not specific to history: an
// answered search probe that does not find the message reschedules at the flat
// retry interval, so one message delayed in the index costs hundreds of
// full-text searches over its deadline. During an indexing backlog those
// probes saturate the reconcile share and newer messages expire unverified for
// want of a look — the same starvation the history poll was backed off for.
func TestSoakFailureReconciler_BacksOffASearchProbeThatFoundNothing(t *testing.T) {
	startedAt := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	const settle = 30 * time.Second
	const retry = time.Second

	// The schedule is anchored at the settle boundary, not at VerifyAfter: no
	// probe before it can succeed, so measuring elapsed wait from there is what
	// keeps the first probes close together instead of starting a doubling
	// sequence already 30 seconds wide.
	probeAt := func(t *testing.T, now time.Time) time.Time {
		t.Helper()
		ledger := newSoakSearchTestLedger(t, startedAt, now)
		reconciler := newSoakFailureReconciler(
			ledger,
			&fakeSoakFailureVerifier{
				results: []soakFailureHistoryResult{soakFailureHistoryFound},
			},
			retry, func() time.Time { return now },
			withSoakFailureSearchIndexProbe(&stubSoakSearchIndexProbe{
				result: soakSearchIndexMissing, queried: true, settle: settle,
			}),
		)
		_, _, err := reconciler.Try(context.Background())
		require.NoError(t, err)
		_, _, err = reconciler.Try(context.Background())
		require.NoError(t, err)
		operation := ledger.active["m1"]
		require.NotNil(t, operation)
		return operation.nextVerifyAt
	}

	t.Run("the first probe after the settle boundary keeps the flat interval", func(t *testing.T) {
		now := startedAt.Add(settle)
		assert.Equal(t, now.Add(retry), probeAt(t, now))
	})

	t.Run("a probe well past the boundary waits as long as it has waited", func(t *testing.T) {
		now := startedAt.Add(settle + 8*time.Second)
		assert.Equal(t, now.Add(8*time.Second), probeAt(t, now))
	})

	// The final probe stays inside the deadline: expiring without one more look
	// would report a message that reached the index late as missing.
	t.Run("the last probe is pulled back inside the deadline", func(t *testing.T) {
		now := startedAt.Add(55 * time.Second)
		assert.Equal(t, startedAt.Add(time.Minute-retry), probeAt(t, now))
	})
}
