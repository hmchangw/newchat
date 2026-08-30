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
	}, "roomlist-worker", "chat.msg.canonical.site-a.>")

	assert.Equal(t, "roomlist-worker", cc.Durable)
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
	assert.Equal(t, "bot-roomlist-worker", stream.PipelineBot.ConsumerName("roomlist-worker"))
	assert.Equal(t, "roomlist-worker", stream.PipelineUser.ConsumerName("roomlist-worker"))
}

func TestValidateFlushBudget(t *testing.T) {
	tests := []struct {
		name                       string
		interval, timeout, ackWait time.Duration
		wantErr                    bool
	}{
		{"defaults fit comfortably", 250 * time.Millisecond, 10 * time.Second, 30 * time.Second, false},
		// The budget is 2*timeout+interval: Run drives Flush SYNCHRONOUSLY, so a
		// message can wait out a stalled flush (timeout), then the next tick
		// (interval), then its own flush (timeout) before it is settled.
		// Charging one timeout admits configs that still outlive AckWait.
		{"timeout alone fits but interval pushes it over", 25 * time.Second, 9 * time.Second, 30 * time.Second, true},
		{"exactly at the budget is not under it", 10 * time.Second, 10 * time.Second, 30 * time.Second, true},
		{"timeout alone exceeds ack wait", time.Second, 31 * time.Second, 30 * time.Second, true},
		// The case the old arithmetic waved through: 20.25s looks safe against a
		// 30s AckWait, but a stalled flush plus the wait for the next tick plus
		// this message's own flush is 40.25s.
		{"a blocked flush precedes the wait this message must also serve", 250 * time.Millisecond, 20 * time.Second, 30 * time.Second, true},
		// A non-positive value passes the budget arithmetic trivially — 2*0+0 is
		// under any AckWait — and then fails at runtime, which is the one outcome
		// a config validator exists to prevent. A zero or negative interval
		// panics time.NewTicker in flusher.Run. A non-positive timeout is worse
		// because it is silent: context.WithTimeout hands every flush an
		// already-expired context, so no batch ever lands, nothing is ever
		// acked, and the consumer stalls with MaxDeliver=-1 holding the messages.
		{"zero interval panics the ticker", 0, 10 * time.Second, 30 * time.Second, true},
		{"negative interval panics the ticker", -time.Second, 10 * time.Second, 30 * time.Second, true},
		{"zero timeout expires every flush context", 250 * time.Millisecond, 0, 30 * time.Second, true},
		{"negative timeout expires every flush context", 250 * time.Millisecond, -time.Second, 30 * time.Second, true},
		// A negative timeout also drags the budget below zero, so the comparison
		// alone would wave through a config that can never flush.
		{"negative timeout large enough to fake a passing budget", 250 * time.Millisecond, -time.Hour, 30 * time.Second, true},
		{"zero ack wait leaves no budget at all", 250 * time.Millisecond, 10 * time.Second, 0, true},
		{"negative ack wait is not a deadline", 250 * time.Millisecond, 10 * time.Second, -time.Second, true},
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

// buildConsumerConfig overrides MaxDeliver to -1 (unlimited), so the derived
// BackOff must not be clamped to the configured CONSUMER_MAX_DELIVER — that cap
// no longer applies, and pkg/stream skips the clamp precisely for an unlimited
// cap. Before the fix the override ran AFTER DurableConsumerDefaults had already
// clamped, so lowering CONSUMER_MAX_DELIVER silently shortened server-side
// redelivery spacing while having no effect at all on the delivery cap it names.
func TestBuildConsumerConfig_BackOffIsNotClampedByTheOverriddenMaxDeliver(t *testing.T) {
	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait:       30 * time.Second,
		MaxDeliver:    2, // deliberately below BackOffSteps
		BackOffSteps:  5,
		BackOffFactor: 2,
		BackOffMax:    8 * time.Minute,
		MaxWaiting:    512,
		MaxAckPending: 1000,
	}, "roomlist-worker", "chat.msg.canonical.site-a.>")

	assert.Equal(t, -1, cc.MaxDeliver)
	assert.Len(t, cc.BackOff, 5, "an unlimited MaxDeliver must not clamp the backoff schedule")
	assert.Equal(t, 30*time.Second, cc.BackOff[0], "BackOff[0] overwrites AckWait server-side, so it must equal AckWait")
}
