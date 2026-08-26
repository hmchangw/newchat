package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckStreamRetention covers the durability boundary MaxDeliver=-1 created.
//
// With no delivery cap there is nothing beneath the stream: a message that cannot
// be persisted stalls the consumer at MaxAckPending and the backlog accumulates on
// MESSAGES-CANONICAL. If its retention is shorter than the outage, JetStream drops
// those messages off the stream — the same loss this service exists to prevent, one
// layer down and more silent, because a retention discard logs nothing at all.
//
// pkg/stream sets only Name and Subjects, so retention is ops/IaC state the repo
// cannot see. This check is the service reading it back and saying so.
func TestCheckStreamRetention(t *testing.T) {
	const minAge = 24 * time.Hour

	tests := []struct {
		name           string
		cfg            *jetstream.StreamConfig
		minMaxAge      time.Duration
		wantSufficient bool
		wantReason     string
	}{
		{
			name:           "unlimited age is the safest setting, not the most suspicious",
			cfg:            &jetstream.StreamConfig{MaxAge: 0},
			minMaxAge:      minAge,
			wantSufficient: true,
		},
		{
			name:           "comfortably above the floor",
			cfg:            &jetstream.StreamConfig{MaxAge: 72 * time.Hour},
			minMaxAge:      minAge,
			wantSufficient: true,
		},
		{
			name:           "exactly at the floor is sufficient",
			cfg:            &jetstream.StreamConfig{MaxAge: minAge},
			minMaxAge:      minAge,
			wantSufficient: true,
		},
		{
			name:           "below the floor is the silent-loss configuration",
			cfg:            &jetstream.StreamConfig{MaxAge: time.Hour},
			minMaxAge:      minAge,
			wantSufficient: false,
			wantReason:     "MaxAge",
		},
		{
			name:           "a zero floor disables the check",
			cfg:            &jetstream.StreamConfig{MaxAge: time.Second},
			minMaxAge:      0,
			wantSufficient: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkStreamRetention(tt.cfg, tt.minMaxAge)
			assert.Equal(t, tt.wantSufficient, got.Sufficient)
			if tt.wantSufficient {
				assert.Empty(t, got.Reasons)
				return
			}
			require.NotEmpty(t, got.Reasons, "an insufficient verdict must say what to change")
			assert.Contains(t, got.Reasons[0], tt.wantReason)
		})
	}
}

// TestCheckStreamRetention_ReasonsAreActionable guards the one thing the operator
// reading this log line at 02:00 needs: the observed value and the required one.
func TestCheckStreamRetention_ReasonsAreActionable(t *testing.T) {
	got := checkStreamRetention(&jetstream.StreamConfig{MaxAge: 30 * time.Minute}, 24*time.Hour)
	require.False(t, got.Sufficient)
	require.Len(t, got.Reasons, 1)
	assert.Contains(t, got.Reasons[0], "30m0s", "the reason must name what the stream is actually set to")
	assert.Contains(t, got.Reasons[0], "24h0m0s", "and what it has to be raised to")
}

// TestReportStreamRetention_IsAdvisory pins the failure direction.
//
// A bad verdict must not stop the pod: refusing to boot trades a partial outage for
// a total one, and this service's whole design principle is that the inert direction
// wins. The verdict has to land somewhere alertable instead.
func TestReportStreamRetention_IsAdvisory(t *testing.T) {
	tests := []struct {
		name string
		info streamInfoFunc
		want int64
	}{
		{
			name: "sufficient retention reads 0",
			info: func(context.Context) (*jetstream.StreamInfo, error) {
				return &jetstream.StreamInfo{Config: jetstream.StreamConfig{MaxAge: 72 * time.Hour}}, nil
			},
			want: 0,
		},
		{
			name: "insufficient retention reads 1",
			info: func(context.Context) (*jetstream.StreamInfo, error) {
				return &jetstream.StreamInfo{Config: jetstream.StreamConfig{MaxAge: time.Minute}}, nil
			},
			want: 1,
		},
		{
			name: "an unreadable stream reads -1, never 0",
			info: func(context.Context) (*jetstream.StreamInfo, error) {
				return nil, errors.New("stream not found")
			},
			want: -1,
		},
		{
			name: "a nil StreamInfo is also unverified",
			info: func(context.Context) (*jetstream.StreamInfo, error) { return nil, nil },
			want: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := newMetrics()
			require.NoError(t, err)
			require.NotPanics(t, func() {
				reportStreamRetention(context.Background(), tt.info, "MESSAGES-CANONICAL-site-a", 24*time.Hour, m)
			})
			assert.Equal(t, tt.want, m.streamRetentionInsufficient.Load())
		})
	}
}

func TestReportStreamRetention_NilMetricsIsSafe(t *testing.T) {
	info := func(context.Context) (*jetstream.StreamInfo, error) {
		return &jetstream.StreamInfo{Config: jetstream.StreamConfig{MaxAge: time.Minute}}, nil
	}
	require.NotPanics(t, func() {
		reportStreamRetention(context.Background(), info, "MESSAGES-CANONICAL-site-a", 24*time.Hour, nil)
	})
}
