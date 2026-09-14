package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

// A cancelled caller is evidence about the CALLER, not about MongoDB — the call
// may not even have been issued. Counting it would let a burst of cancellations
// open the member breaker, and every room whose member list is not already warm
// in L2 loses its notifications with it.
func TestMemberBreakerFailure_CancelledCallerDoesNotTripBreaker(t *testing.T) {
	b := circuitbreaker.New(2, time.Minute,
		circuitbreaker.WithFailurePredicate(memberBreakerFailure))

	for range 5 {
		err := b.Do(func() error { return context.Canceled })
		require.ErrorIs(t, err, context.Canceled, "the cancellation must still reach the caller")
	}
	assert.Equal(t, circuitbreaker.StateClosed, b.State(),
		"a cancelled caller must not count as a Mongo failure")
}

// An unreachable MongoDB reports itself as a DeadlineExceeded from the driver's
// server-selection bound, so it must still count or the fence never engages.
func TestMemberBreakerFailure_DeadlineExceededTripsBreaker(t *testing.T) {
	b := circuitbreaker.New(2, time.Minute,
		circuitbreaker.WithFailurePredicate(memberBreakerFailure))

	for range 2 {
		require.ErrorIs(t, b.Do(func() error { return context.DeadlineExceeded }), context.DeadlineExceeded)
	}
	assert.Equal(t, circuitbreaker.StateOpen, b.State(),
		"an unreachable MongoDB must open the member breaker")
}

// Genuine infrastructure errors must still open the breaker.
func TestMemberBreakerFailure_InfraErrorTripsBreaker(t *testing.T) {
	b := circuitbreaker.New(2, time.Minute,
		circuitbreaker.WithFailurePredicate(memberBreakerFailure))

	boom := errors.New("connection refused")
	for range 2 {
		require.ErrorIs(t, b.Do(func() error { return boom }), boom)
	}
	assert.Equal(t, circuitbreaker.StateOpen, b.State())
}
