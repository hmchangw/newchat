package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestBuildConsumerConfig(t *testing.T) {
	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	}, "site-a")

	assert.Equal(t, "bot-message-worker", cc.Durable)
	assert.Equal(t, "chat.bot.canonical.site-a.created", cc.FilterSubject)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 6, cc.MaxDeliver)
	assert.Equal(t, 512, cc.MaxWaiting)
	assert.Equal(t, 1000, cc.MaxAckPending)
	assert.Equal(t, []time.Duration{
		30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
	}, cc.BackOff)
}

// Regression: the previous inline BackOff{1s,...} silently set the server-side
// AckWait to 1s (server/consumer.go:677-682), redelivering Cassandra writes
// while they were still in flight.
func TestBuildConsumerConfig_BackOffHeadEqualsAckWait(t *testing.T) {
	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait: 45 * time.Second, MaxDeliver: 6,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	}, "site-a")

	assert.Equal(t, cc.AckWait, cc.BackOff[0])
}
