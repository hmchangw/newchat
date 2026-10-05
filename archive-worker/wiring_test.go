package main

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/stream"
)

func TestConsumerConfig(t *testing.T) {
	s := stream.WithUnlimitedRedelivery(validConfig().Consumer)
	cc := consumerConfig("archive-worker-events", []string{"a.>", "b.>"}, s)
	assert.Equal(t, "archive-worker-events", cc.Durable)
	assert.Equal(t, []string{"a.>", "b.>"}, cc.FilterSubjects)
	assert.Equal(t, -1, cc.MaxDeliver, "events and members lanes redeliver without a cap")
	assert.Equal(t, 12000, cc.MaxAckPending)
}

func TestBlobConsumerSettings(t *testing.T) {
	t.Run("blob lane gets its own ack wait and a bounded outage budget", func(t *testing.T) {
		cfg := validConfig()
		cfg.Consumer.MaxDeliver = stream.DefaultMaxDeliver
		s := blobConsumerSettings(&cfg)
		assert.Equal(t, cfg.BlobAckWait, s.AckWait)
		assert.Greater(t, s.MaxDeliver, stream.DefaultMaxDeliver, "outage budget raises the cap")

		cc := consumerConfig(blobsDurable, nil, s)
		assert.Equal(t, s.MaxDeliver, cc.MaxDeliver)
		assert.NotEqual(t, -1, cc.MaxDeliver, "the blob lane is not unlimited")
		assert.Equal(t, cfg.BlobAckWait, cc.AckWait)
	})
	t.Run("an operator-set MAX_DELIVER wins over the budget", func(t *testing.T) {
		cfg := validConfig()
		cfg.Consumer.MaxDeliver = 9
		assert.Equal(t, 9, blobConsumerSettings(&cfg).MaxDeliver)
	})
}

func TestNewBlobLaneConfig(t *testing.T) {
	cfg := validConfig()
	cfg.Consumer.MaxDeliver = stream.DefaultMaxDeliver
	cfg.Consumer.HeartbeatMax = 7 * time.Minute
	cfg.Consumer.BackOffSteps = 5
	cfg.Consumer.BackOffFactor = 2
	cfg.Consumer.BackOffMax = 8 * time.Minute
	settings := blobConsumerSettings(&cfg)
	cc := consumerConfig(blobsDurable, nil, settings)

	got := newBlobLaneConfig(&cfg, &cc)
	assert.Equal(t, cc.MaxDeliver, got.maxDeliver, "wired from the consumer that was created")
	assert.Equal(t, cc.BackOff[0], got.ackWait, "heartbeat paces off the deadline the server enforces")
	assert.Equal(t, cfg.BlobAckWait, got.ackWait)
	assert.Equal(t, 7*time.Minute, got.heartbeatMax)
	assert.Equal(t, cfg.SiteID, got.site)
	assert.Equal(t, cfg.BlobMaxBytes, got.maxBytes)
	assert.Equal(t, cfg.BlobWorkers, got.workers)
	assert.Equal(t, auditarchive.DefaultChunkBytes, got.chunkBytes)
	assert.NotNil(t, got.now)
}

func TestLaneGroup(t *testing.T) {
	t.Run("stop reaches every lane and wait returns once all are done", func(t *testing.T) {
		g := newLaneGroup()
		started := make(chan struct{}, 2)
		for range 2 {
			g.start(context.Background(), func(_ context.Context, stop <-chan struct{}, done chan<- struct{}) {
				defer close(done)
				started <- struct{}{}
				<-stop
			})
		}
		<-started
		<-started
		g.stopAll()
		g.stopAll() // idempotent
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, g.wait(ctx))
	})
	t.Run("wait reports a lane that outlives the budget", func(t *testing.T) {
		g := newLaneGroup()
		release := make(chan struct{})
		g.start(context.Background(), func(_ context.Context, _ <-chan struct{}, done chan<- struct{}) {
			defer close(done)
			<-release
		})
		t.Cleanup(func() { close(release) })
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		err := g.wait(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("no lanes is an immediate success", func(t *testing.T) {
		require.NoError(t, newLaneGroup().wait(context.Background()))
	})
}

func TestLaneConsumerSettings(t *testing.T) {
	cfg := validConfig()
	cfg.Consumer.MaxDeliver = 4
	s := laneConsumerSettings(&cfg)
	assert.Equal(t, -1, s.MaxDeliver)
	assert.Equal(t, cfg.Consumer.AckWait, s.AckWait)
	assert.Equal(t, cfg.Consumer.MaxAckPending, s.MaxAckPending)
	assert.Equal(t, 4, cfg.Consumer.MaxDeliver, "the shared config is not mutated")
	assert.Equal(t, -1, consumerConfig(eventsDurable, nil, s).MaxDeliver)
}

func TestNewLaneConfig(t *testing.T) {
	cfg := validConfig()
	m := &metrics{}
	for _, name := range []string{"events", "members"} {
		t.Run(name, func(t *testing.T) {
			got := newLaneConfig(&cfg, name, m)
			assert.Equal(t, cfg.SiteID, got.site)
			assert.Equal(t, name, got.name)
			assert.Equal(t, cfg.FetchBatch, got.fetchBatch)
			assert.Equal(t, cfg.FillInterval, got.fillInterval)
			assert.Same(t, m, got.metrics)
			assert.NotNil(t, got.now)
		})
	}
}

func TestNewBlobLaneConfig_AckWait(t *testing.T) {
	cfg := validConfig()
	t.Run("backoff head wins over ack wait", func(t *testing.T) {
		cc := jetstream.ConsumerConfig{AckWait: time.Minute, BackOff: []time.Duration{3 * time.Minute, 6 * time.Minute}, MaxDeliver: 17}
		got := newBlobLaneConfig(&cfg, &cc)
		assert.Equal(t, 3*time.Minute, got.ackWait)
		assert.Equal(t, 17, got.maxDeliver)
	})
	t.Run("no backoff falls back to ack wait", func(t *testing.T) {
		cc := jetstream.ConsumerConfig{AckWait: time.Minute}
		assert.Equal(t, time.Minute, newBlobLaneConfig(&cfg, &cc).ackWait)
	})
}
