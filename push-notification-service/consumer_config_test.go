package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

// The schedule itself is pkg/stream's contract; this only pins the wiring
// buildConsumerConfig adds on top of the shared defaults, per pipeline.
func TestBuildConsumerConfig(t *testing.T) {
	s := stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	}

	for _, mode := range []stream.Pipeline{stream.PipelineUser, stream.PipelineBot} {
		t.Run(string(mode), func(t *testing.T) {
			wiring := stream.Resolve(mode, "site-a")

			want := stream.DurableConsumerDefaults(s)
			want.Durable = mode.ConsumerName("push-notification-service")
			want.FilterSubject = wiring.PushInputWildcard

			assert.Equal(t, want, buildConsumerConfig(s, mode.ConsumerName("push-notification-service"), wiring.PushInputWildcard))
		})
	}
}

// The failover lane reuses the same builder, so what needs asserting is that the
// two lanes get distinct durables and filters — a shared durable would have them
// clobber each other's cursor on a single-server dev NATS.
func TestFailoverConsumerConfig_DiffersFromPrimary(t *testing.T) {
	w := stream.Resolve(stream.PipelineUser, "site-a")

	s := stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	}
	primary := buildConsumerConfig(s, stream.PipelineUser.ConsumerName("push-notification-service"),
		w.PushInputWildcard)
	failover := buildConsumerConfig(s, stream.PipelineUser.FailoverConsumerName("push-notification-service"),
		w.PushFailoverInputWildcard)

	assert.Equal(t, "push-notification-service-failover", failover.Durable)
	assert.Equal(t, "chat.failover.push.site-a.>", failover.FilterSubject)
	assert.NotEqual(t, primary.Durable, failover.Durable)
	assert.NotEqual(t, primary.FilterSubject, failover.FilterSubject)
}
