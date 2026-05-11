package stream_test

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestDurableConsumerDefaults(t *testing.T) {
	t.Run("propagates settings", func(t *testing.T) {
		s := stream.ConsumerSettings{
			AckWait:       45 * time.Second,
			MaxDeliver:    3,
			MaxWaiting:    256,
			MaxAckPending: 750,
		}
		cc := stream.DurableConsumerDefaults(s)

		assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy, "AckPolicy invariant")
		assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy, "DeliverPolicy invariant")
		assert.Equal(t, 45*time.Second, cc.AckWait)
		assert.Equal(t, 3, cc.MaxDeliver)
		assert.Equal(t, 256, cc.MaxWaiting)
		assert.Equal(t, 750, cc.MaxAckPending)

		assert.Empty(t, cc.Durable, "Durable must be set by caller")
		assert.Empty(t, cc.FilterSubjects, "FilterSubjects must be set by caller if needed")
	})

	t.Run("zero settings produce zero values", func(t *testing.T) {
		cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{})

		assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
		assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy)
		assert.Zero(t, cc.AckWait)
		assert.Zero(t, cc.MaxDeliver)
		assert.Zero(t, cc.MaxWaiting)
		assert.Zero(t, cc.MaxAckPending)
	})
}

func TestConsumerSettingsEnvDefaults(t *testing.T) {
	type holder struct {
		Consumer stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	}

	var h holder
	require.NoError(t, env.Parse(&h))

	assert.Equal(t, 30*time.Second, h.Consumer.AckWait)
	assert.Equal(t, 5, h.Consumer.MaxDeliver)
	assert.Equal(t, 512, h.Consumer.MaxWaiting)
	assert.Equal(t, 1000, h.Consumer.MaxAckPending)
}

func TestConsumerSettingsEnvOverrides(t *testing.T) {
	type holder struct {
		Consumer stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	}

	t.Setenv("CONSUMER_ACK_WAIT", "10s")
	t.Setenv("CONSUMER_MAX_DELIVER", "7")
	t.Setenv("CONSUMER_MAX_WAITING", "1024")
	t.Setenv("CONSUMER_MAX_ACK_PENDING", "250")

	var h holder
	require.NoError(t, env.Parse(&h))

	assert.Equal(t, 10*time.Second, h.Consumer.AckWait)
	assert.Equal(t, 7, h.Consumer.MaxDeliver)
	assert.Equal(t, 1024, h.Consumer.MaxWaiting)
	assert.Equal(t, 250, h.Consumer.MaxAckPending)
}
