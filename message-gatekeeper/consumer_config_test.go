package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestBuildConsumerConfig(t *testing.T) {
	t.Run("propagates settings", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			MaxWaiting:    512,
			MaxAckPending: 1000,
		})

		assert.Equal(t, "message-gatekeeper", cc.Durable)
		assert.Equal(t, 1000, cc.MaxAckPending)
		assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
		assert.Equal(t, 30*time.Second, cc.AckWait)
		assert.Equal(t, 5, cc.MaxDeliver)
		assert.Equal(t, 512, cc.MaxWaiting)
		assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
	})

	t.Run("overrides flow through", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait:       45 * time.Second,
			MaxDeliver:    3,
			MaxWaiting:    256,
			MaxAckPending: 250,
		})

		assert.Equal(t, "message-gatekeeper", cc.Durable)
		assert.Equal(t, 250, cc.MaxAckPending)
		assert.Equal(t, 45*time.Second, cc.AckWait)
		assert.Equal(t, 3, cc.MaxDeliver)
		assert.Equal(t, 256, cc.MaxWaiting)
	})
}
