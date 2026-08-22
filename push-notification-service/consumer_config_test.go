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

			assert.Equal(t, want, buildConsumerConfig(s, mode, wiring.PushInputWildcard))
		})
	}
}
