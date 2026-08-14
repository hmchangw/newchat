package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

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
