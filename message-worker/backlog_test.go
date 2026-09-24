package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func consumerInfo(pending uint64, ackPending int) func(context.Context) (*jetstream.ConsumerInfo, error) {
	return func(context.Context) (*jetstream.ConsumerInfo, error) {
		return &jetstream.ConsumerInfo{NumPending: pending, NumAckPending: ackPending}, nil
	}
}

// The regression the retry lane introduced. settle clears the degraded marker on
// a successful write from EITHER lane, so a drain check that sees only the hot
// consumer reports history complete while escalated messages are still parked on
// RETRY-{siteID} — and clients are told their history is whole when it is not.
func TestHistoryBacklog_CountsTheRetryConsumer(t *testing.T) {
	tests := []struct {
		name           string
		hot            func(context.Context) (*jetstream.ConsumerInfo, error)
		retry          func(context.Context) (*jetstream.ConsumerInfo, error)
		wantPending    uint64
		wantAckPending uint64
	}{
		{
			name: "hot drained but the retry lane still holds work",
			hot:  consumerInfo(0, 0), retry: consumerInfo(7, 3),
			wantPending: 7, wantAckPending: 3,
		},
		{
			name: "both lanes drained",
			hot:  consumerInfo(0, 0), retry: consumerInfo(0, 0),
			wantPending: 0, wantAckPending: 0,
		},
		{
			name: "both lanes hold work",
			hot:  consumerInfo(2, 1), retry: consumerInfo(5, 4),
			wantPending: 7, wantAckPending: 5,
		},
		{
			name: "no retry consumer bound (teams mode, or lane unprovisioned)",
			hot:  consumerInfo(4, 2), retry: nil,
			wantPending: 4, wantAckPending: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pending, ackPending, err := historyBacklog(context.Background(), tt.hot, tt.retry)

			require.NoError(t, err)
			assert.Equal(t, tt.wantPending, pending)
			assert.Equal(t, tt.wantAckPending, ackPending)
		})
	}
}

// An unreadable backlog must not read as "drained": the marker would clear on the
// strength of a number nobody could fetch.
func TestHistoryBacklog_PropagatesEitherLookupFailure(t *testing.T) {
	boom := func(context.Context) (*jetstream.ConsumerInfo, error) {
		return nil, errors.New("consumer info unavailable")
	}

	t.Run("hot", func(t *testing.T) {
		_, _, err := historyBacklog(context.Background(), boom, consumerInfo(0, 0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "consumer info")
	})
	t.Run("retry", func(t *testing.T) {
		_, _, err := historyBacklog(context.Background(), consumerInfo(0, 0), boom)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "retry consumer info")
	})
}
