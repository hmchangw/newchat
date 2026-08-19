package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestBuildConsumerConfig(t *testing.T) {
	w := stream.Resolve(stream.PipelineUser, "site-a")

	cc := buildConsumerConfig(stream.PipelineUser.ConsumerName("push-notification-service"),
		w.PushInputWildcard, 5)

	assert.Equal(t, "push-notification-service", cc.Durable)
	assert.Equal(t, "chat.server.notification.push.site-a.>", cc.FilterSubject)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 5, cc.MaxDeliver)
	assert.NotEmpty(t, cc.BackOff, "retry backoff must survive extraction")
}

// The failover lane reuses the same builder, so what needs asserting is that the
// two lanes get distinct durables and filters — a shared durable would have them
// clobber each other's cursor on a single-server dev NATS.
func TestFailoverConsumerConfig_DiffersFromPrimary(t *testing.T) {
	w := stream.Resolve(stream.PipelineUser, "site-a")

	primary := buildConsumerConfig(stream.PipelineUser.ConsumerName("push-notification-service"),
		w.PushInputWildcard, 5)
	failover := buildConsumerConfig(stream.PipelineUser.FailoverConsumerName("push-notification-service"),
		w.PushFailoverInputWildcard, 5)

	assert.Equal(t, "push-notification-service-failover", failover.Durable)
	assert.Equal(t, "chat.failover.push.site-a.>", failover.FilterSubject)
	assert.NotEqual(t, primary.Durable, failover.Durable)
	assert.NotEqual(t, primary.FilterSubject, failover.FilterSubject)
}
