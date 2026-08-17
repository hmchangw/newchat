package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestBuildConsumerConfig(t *testing.T) {
	t.Run("propagates settings", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			MaxWaiting:    512,
			MaxAckPending: 1000,
		}, "default", "site-a")

		assert.Equal(t, "message-worker", cc.Durable)
		assert.Equal(t, 1000, cc.MaxAckPending)
		assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
		assert.Equal(t, 30*time.Second, cc.AckWait)
		assert.Equal(t, stream.OutageRetryMaxDeliver, cc.MaxDeliver,
			"a consumer left at the package default gets the outage budget, not ~2.6 minutes")
		assert.Equal(t, 512, cc.MaxWaiting)
		assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
	})

	t.Run("overrides flow through", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait:       45 * time.Second,
			MaxDeliver:    3,
			MaxWaiting:    256,
			MaxAckPending: 500,
		}, "default", "site-a")

		assert.Equal(t, "message-worker", cc.Durable)
		assert.Equal(t, 500, cc.MaxAckPending)
		assert.Equal(t, 45*time.Second, cc.AckWait)
		assert.Equal(t, 3, cc.MaxDeliver)
		assert.Equal(t, 256, cc.MaxWaiting)
	})

	t.Run("default mode filters to .created only, excludes edits/deletes/teams", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{}, "default", "site-a")
		assert.Empty(t, cc.FilterSubject, "single FilterSubject unset when using FilterSubjects")
		assert.Equal(t, []string{subject.MsgCanonicalCreated("site-a")}, cc.FilterSubjects)
	})

	t.Run("teams mode filters to the teams batch subject on its own durable", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{}, "teams", "site-a")
		assert.Equal(t, "message-worker-teams", cc.Durable)
		assert.Equal(t, []string{subject.MsgTeamsCanonicalBatch("site-a")}, cc.FilterSubjects)
	})
}
