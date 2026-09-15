package main

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
)

func TestSettleBackoff(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want []time.Duration
	}{
		{
			name: "a shedding downstream gets the slow curve",
			err:  errcode.Unavailable("service busy"),
			want: jsretry.BackpressureBackoff,
		},
		{
			name: "an explicitly rate-limited downstream gets the slow curve",
			err:  errcode.TooManyRequests("slow down"),
			want: jsretry.BackpressureBackoff,
		},
		{
			name: "the shedding signal survives wrapping",
			err:  fmt.Errorf("fetch thread parent abc: %w", errcode.Unavailable("service busy")),
			want: jsretry.BackpressureBackoff,
		},
		{
			name: "an ordinary infra failure keeps the fan-out curve",
			err:  errors.New("mongo: connection reset"),
			want: jsretry.LowLatencyBackoff,
		},
		{
			name: "an internal error is not backpressure",
			err:  errcode.Internal("boom"),
			want: jsretry.LowLatencyBackoff,
		},
		{
			name: "a not-found is not backpressure",
			err:  errcode.NotFound("no such message"),
			want: jsretry.LowLatencyBackoff,
		},
		{
			name: "success keeps the default curve",
			err:  nil,
			want: jsretry.LowLatencyBackoff,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, settleBackoff(tt.err))
		})
	}
}

// TestSettleBackoff_SheddingDoesNotRetryFast is the regression guard for the
// amplification loop: history-service sheds with Unavailable, and retrying that
// at LowLatencyBackoff's 200ms first rung aims more load at the service that is
// already failing. Offered load must not rise as capacity falls.
func TestSettleBackoff_SheddingDoesNotRetryFast(t *testing.T) {
	shedding := settleBackoff(errcode.Unavailable("service busy"))
	require.NotEmpty(t, shedding)
	assert.GreaterOrEqual(t, shedding[0], 30*time.Second,
		"a downstream that just shed load must not be retried in under 30s")

	// The fan-out path keeps its near-immediate first retry for ordinary blips,
	// which is the whole reason broadcast-worker uses LowLatencyBackoff.
	ordinary := settleBackoff(errors.New("transient"))
	require.NotEmpty(t, ordinary)
	assert.Less(t, ordinary[0], time.Second,
		"an ordinary transient failure must still retry fast")
}

// TestSettleBackoff_TailMatchesDeliveryBudget pins the coupling that
// WithOutageRetryBudget depends on. MaxDeliver is derived once, from
// LowLatencyBackoff, and must remain large enough to cover the outage window for
// whichever schedule a message actually settles with. Both schedules share the
// same repeating tail, so the derived count stays valid; if that ever stops
// being true the budget must be re-derived from the slower schedule.
func TestSettleBackoff_TailMatchesDeliveryBudget(t *testing.T) {
	low := jsretry.LowLatencyBackoff
	back := jsretry.BackpressureBackoff
	require.NotEmpty(t, low)
	require.NotEmpty(t, back)
	assert.Equal(t, low[len(low)-1], back[len(back)-1],
		"schedules must share a repeating tail or MaxDeliver stops covering the outage window")
}
