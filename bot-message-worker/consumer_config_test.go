package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

// The schedule itself is pkg/stream's contract; this only pins the wiring
// buildConsumerConfig adds on top of the shared defaults.
func TestBuildConsumerConfig(t *testing.T) {
	s := stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	}

	want := stream.DurableConsumerDefaults(s)
	want.Durable = "bot-message-worker"
	want.FilterSubject = "chat.bot.canonical.site-a.created"

	assert.Equal(t, want, buildConsumerConfig(s, "site-a"))
}
