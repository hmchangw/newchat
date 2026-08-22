package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestBuildConsumerConfig_UnlimitedDeliverAndDeliverNew(t *testing.T) {
	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		MaxWaiting:    512,
		MaxAckPending: 1000,
	}, "unread-worker", "chat.msg.canonical.site-a.>")

	assert.Equal(t, "unread-worker", cc.Durable)
	assert.Equal(t, "chat.msg.canonical.site-a.>", cc.FilterSubject)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	// Durable retry: a MongoDB outage must not exhaust MaxDeliver and silently
	// drop badges. Poison protection is classifyFlushErr, not MaxDeliver.
	assert.Equal(t, -1, cc.MaxDeliver)
	// New, not All: replaying the whole canonical stream on first deploy would
	// re-apply historical writes as one large burst for no benefit.
	assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 1000, cc.MaxAckPending)
}

func TestBuildConsumerConfig_BotModePrefixesDurable(t *testing.T) {
	assert.Equal(t, "bot-unread-worker", stream.PipelineBot.ConsumerName("unread-worker"))
	assert.Equal(t, "unread-worker", stream.PipelineUser.ConsumerName("unread-worker"))
}

func TestValidateFlushBudget(t *testing.T) {
	tests := []struct {
		name                       string
		interval, timeout, ackWait time.Duration
		wantErr                    bool
	}{
		{"defaults fit comfortably", 250 * time.Millisecond, 10 * time.Second, 30 * time.Second, false},
		// The budget is interval+timeout: a message waits out the interval in the
		// pending batch BEFORE its bounded flush even starts, so checking the
		// timeout alone lets a legal config still outlive AckWait.
		{"timeout alone fits but interval pushes it over", 25 * time.Second, 9 * time.Second, 30 * time.Second, true},
		{"exactly at the budget is not under it", 10 * time.Second, 20 * time.Second, 30 * time.Second, true},
		{"timeout alone exceeds ack wait", time.Second, 31 * time.Second, 30 * time.Second, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFlushBudget(tt.interval, tt.timeout, tt.ackWait)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
