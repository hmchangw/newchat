package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

// The schedule itself is pkg/stream's contract; this pins the two overrides
// this worker adds on top of the shared defaults.
func TestBuildConsumerConfig(t *testing.T) {
	s := stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	}

	cc := buildConsumerConfig(s)

	assert.Equal(t, durableName, cc.Durable)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 512, cc.MaxWaiting)

	// Never drop a feed batch — this overrides whatever CONSUMER_MAX_DELIVER says.
	assert.Equal(t, -1, cc.MaxDeliver)
	// Strictly sequential: a quit cannot overtake the upsert before it.
	assert.Equal(t, 1, cc.MaxAckPending)

	assert.Equal(t, []time.Duration{
		30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
	}, cc.BackOff)
	assert.Equal(t, cc.AckWait, cc.BackOff[0])
}

// MaxDeliver=-1 exempts the schedule from the len(BackOff)<=MaxDeliver rule, so
// a step count above the caller's MaxDeliver must survive rather than be clamped.
func TestBuildConsumerConfig_UnlimitedDeliverySkipsTheClamp(t *testing.T) {
	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 2, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	})

	assert.Equal(t, -1, cc.MaxDeliver)
	assert.Len(t, cc.BackOff, 5)
}
