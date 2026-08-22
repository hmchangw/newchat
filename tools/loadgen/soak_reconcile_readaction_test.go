package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scriptedReconcileTry struct {
	results []scriptedTryResult
	calls   int
}

type scriptedTryResult struct {
	handled   bool
	spentRead bool
	err       error
}

func (s *scriptedReconcileTry) Try(context.Context) (bool, bool, error) {
	s.calls++
	if len(s.results) == 0 {
		return false, false, nil
	}
	result := s.results[0]
	if len(s.results) > 1 {
		s.results = s.results[1:]
	}
	return result.handled, result.spentRead, result.err
}

// Try advances one observer per claim and a read action is the only way to take
// one, so a claim that costs no read still costs a callback. With the recipient
// observer on that is a second callback per message — 110 claims/s against the
// 110 actions the read lane has, the whole budget, with nothing left for the
// history poll's retries. Draining the free claims inside one admission is what
// keeps the callback budget matched to the claims that actually query.
func TestReconcileReadAction_DrainsFreeClaimsBeforeSpendingTheAction(t *testing.T) {
	t.Run("a free claim does not end the action", func(t *testing.T) {
		reconciler := &scriptedReconcileTry{results: []scriptedTryResult{
			{handled: true},                  // recipient finalize: no read
			{handled: true, spentRead: true}, // history poll: queried
		}}

		spent := reconcileReadAction(context.Background(), reconciler, newSoakShareGate(1))

		assert.True(t, spent, "the action ended on the claim that queried")
		assert.Equal(t, 2, reconciler.calls,
			"the free claim was drained inside the same admission")
	})

	t.Run("an action that only found free claims still reads", func(t *testing.T) {
		reconciler := &scriptedReconcileTry{results: []scriptedTryResult{
			{handled: true},
			{handled: false},
		}}

		spent := reconcileReadAction(context.Background(), reconciler, newSoakShareGate(1))

		assert.False(t, spent, "nothing queried, so the action performs its read")
		assert.Equal(t, 2, reconciler.calls)
	})

	t.Run("the drain is bounded so one action cannot monopolise the lane", func(t *testing.T) {
		reconciler := &scriptedReconcileTry{results: []scriptedTryResult{{handled: true}}}

		spent := reconcileReadAction(context.Background(), reconciler, newSoakShareGate(1))

		assert.False(t, spent)
		assert.Equal(t, soakReconcileFreeClaimBurst, reconciler.calls)
	})

	t.Run("a refused allowance takes no claim at all", func(t *testing.T) {
		reconciler := &scriptedReconcileTry{results: []scriptedTryResult{
			{handled: true, spentRead: true},
		}}
		gate := newSoakShareGate(0)
		require.False(t, gate.Allow(), "a zero share never admits reconciliation")

		spent := reconcileReadAction(context.Background(), reconciler, gate)

		assert.False(t, spent)
		assert.Zero(t, reconciler.calls)
	})

	// A claim that failed still resolved nothing and bought no read, so it must
	// not silently end the action — the error is logged and the drain continues
	// under its own bound.
	t.Run("a failing claim does not spend the action", func(t *testing.T) {
		reconciler := &scriptedReconcileTry{results: []scriptedTryResult{
			{handled: true, err: errors.New("release malformed failure operation")},
			{handled: false},
		}}

		spent := reconcileReadAction(context.Background(), reconciler, newSoakShareGate(1))

		assert.False(t, spent)
		assert.Equal(t, 2, reconciler.calls)
	})
}
