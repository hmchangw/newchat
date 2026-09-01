package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestBuildConsumerConfig(t *testing.T) {
	t.Run("propagates settings", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait: 30 * time.Second,
			// The symbol, not a literal: the default moved 5→6 when main added
			// exponential backoff, and a literal here silently stopped matching
			// it, so the outage budget was no longer being applied under test.
			MaxDeliver:    stream.DefaultMaxDeliver,
			MaxWaiting:    512,
			MaxAckPending: 1000,
		}, "default", "site-a")

		assert.Equal(t, "message-worker", cc.Durable)
		assert.Equal(t, 1000, cc.MaxAckPending)
		assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
		assert.Equal(t, 30*time.Second, cc.AckWait)
		// Default mode takes stream.WithUnlimitedRedelivery, not the outage budget:
		// the give-up decision lives in settle.go, which drops only a request-class
		// failure that outlived its window. A finite cap — even the generous derived
		// one — would terminate messages behind that decision.
		assert.Equal(t, -1, cc.MaxDeliver,
			"default mode retries until the handler decides, never until a delivery count")
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
		assert.Equal(t, 256, cc.MaxWaiting)
	})

	t.Run("default mode retries forever regardless of the env cap", func(t *testing.T) {
		for _, envCap := range []int{0, 3, 5, 1000} {
			cc := buildConsumerConfig(stream.ConsumerSettings{MaxDeliver: envCap}, "default", "site-a")
			assert.Equal(t, -1, cc.MaxDeliver,
				"a terminating consumer drops history behind settle.go's back (env cap %d)", envCap)
		}
	})

	t.Run("teams mode keeps its finite cap", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{MaxDeliver: 3}, "teams", "site-a")
		assert.Equal(t, 3, cc.MaxDeliver)
	})

	t.Run("teams mode falls back to a finite cap when the service-wide env says -1", func(t *testing.T) {
		for _, envCap := range []int{-1, 0} {
			cc := buildConsumerConfig(stream.ConsumerSettings{MaxDeliver: envCap}, "teams", "site-a")
			assert.Equal(t, teamsMaxDeliver, cc.MaxDeliver,
				"teams settles through plain jsretry.Settle, so an infinite NAK would never give up (env cap %d)", envCap)
		}
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

func TestValidateConsumerConfig(t *testing.T) {
	tests := []struct {
		name       string
		mode       string
		maxDeliver int
		wantErr    bool
	}{
		{name: "default mode retrying forever", mode: "default", maxDeliver: -1},
		{name: "default mode with a finite cap", mode: "default", maxDeliver: 5, wantErr: true},
		{name: "default mode with the jetstream unset sentinel", mode: "default", maxDeliver: 0, wantErr: true},
		{name: "teams mode with a finite cap", mode: "teams", maxDeliver: 5},
		{name: "teams mode retrying forever", mode: "teams", maxDeliver: -1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConsumerConfig(&jetstream.ConsumerConfig{MaxDeliver: tt.maxDeliver}, tt.mode)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
