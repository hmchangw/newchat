package stream_test

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/jsretry"
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
		assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy, "DeliverPolicy invariant")
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
		assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
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
	assert.Equal(t, 6, h.Consumer.MaxDeliver)
	assert.Equal(t, 512, h.Consumer.MaxWaiting)
	assert.Equal(t, 1000, h.Consumer.MaxAckPending)
	assert.Equal(t, 5, h.Consumer.BackOffSteps)
	assert.InDelta(t, 2.0, h.Consumer.BackOffFactor, 0.0001)
	assert.Equal(t, 8*time.Minute, h.Consumer.BackOffMax)
}

func TestConsumerSettingsEnvOverrides(t *testing.T) {
	type holder struct {
		Consumer stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	}

	t.Setenv("CONSUMER_ACK_WAIT", "10s")
	t.Setenv("CONSUMER_MAX_DELIVER", "7")
	t.Setenv("CONSUMER_MAX_WAITING", "1024")
	t.Setenv("CONSUMER_MAX_ACK_PENDING", "250")
	t.Setenv("CONSUMER_BACKOFF_STEPS", "3")
	t.Setenv("CONSUMER_BACKOFF_FACTOR", "3")
	t.Setenv("CONSUMER_BACKOFF_MAX", "1m")

	var h holder
	require.NoError(t, env.Parse(&h))

	assert.Equal(t, 10*time.Second, h.Consumer.AckWait)
	assert.Equal(t, 7, h.Consumer.MaxDeliver)
	assert.Equal(t, 1024, h.Consumer.MaxWaiting)
	assert.Equal(t, 250, h.Consumer.MaxAckPending)
	assert.Equal(t, 3, h.Consumer.BackOffSteps)
	assert.InDelta(t, 3.0, h.Consumer.BackOffFactor, 0.0001)
	assert.Equal(t, time.Minute, h.Consumer.BackOffMax)
}

func TestDurableConsumerDefaults_BackOff(t *testing.T) {
	tests := []struct {
		name string
		s    stream.ConsumerSettings
		want []time.Duration
	}{
		{
			name: "agreed default schedule",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: 6,
				BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
			},
			want: []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute},
		},
		{
			name: "BackOffMax caps and every later step stays at the cap",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: 6,
				BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 2 * time.Minute,
			},
			want: []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 2 * time.Minute, 2 * time.Minute},
		},
		{
			name: "steps zero disables BackOff — the operator off-switch",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: 6,
				BackOffSteps: 0, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
			},
			want: nil,
		},
		{
			name: "steps beyond MaxDeliver are clamped — the server rejects the excess",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: 3,
				BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
			},
			want: []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute},
		},
		{
			name: "MaxDeliver 0 means unlimited, so no clamp",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: 0,
				BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
			},
			want: []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute},
		},
		{
			name: "MaxDeliver -1 exempts the clamp",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: -1,
				BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
			},
			want: []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute},
		},
		{
			name: "factor below 1 flattens rather than shrinking",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: 6,
				BackOffSteps: 3, BackOffFactor: 0.5, BackOffMax: 8 * time.Minute,
			},
			want: []time.Duration{30 * time.Second, 30 * time.Second, 30 * time.Second},
		},
		{
			name: "zero AckWait yields no schedule",
			s: stream.ConsumerSettings{
				AckWait: 0, MaxDeliver: 6,
				BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
			},
			want: nil,
		},
		{
			name: "zero BackOffMax leaves the schedule uncapped",
			s: stream.ConsumerSettings{
				AckWait: time.Second, MaxDeliver: 6,
				BackOffSteps: 4, BackOffFactor: 2, BackOffMax: 0,
			},
			want: []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cc := stream.DurableConsumerDefaults(tt.s)
			assert.Equal(t, tt.want, cc.BackOff)
		})
	}
}

// The whole point of deriving the schedule: nats-server overwrites AckWait with
// BackOff[0] (server/consumer.go:677-682), so if they ever disagreed the
// consumer would silently run a shorter AckWait than configured.
func TestDurableConsumerDefaults_BackOffHeadEqualsAckWait(t *testing.T) {
	for _, ackWait := range []time.Duration{time.Second, 30 * time.Second, 2 * time.Minute} {
		cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
			AckWait: ackWait, MaxDeliver: 6,
			BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
		})
		require.NotEmpty(t, cc.BackOff)
		assert.Equal(t, cc.AckWait, cc.BackOff[0], "BackOff[0] must equal AckWait for ackWait=%s", ackWait)
	}
}

// The server errors with JSConsumerMaxDeliverBackoffError when the schedule is
// longer than MaxDeliver (server/consumer.go:807, :2588).
func TestDurableConsumerDefaults_BackOffNeverExceedsMaxDeliver(t *testing.T) {
	for steps := 1; steps <= 12; steps++ {
		cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
			AckWait: 30 * time.Second, MaxDeliver: 6,
			BackOffSteps: steps, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
		})
		assert.LessOrEqual(t, len(cc.BackOff), 6, "steps=%d", steps)
	}
}

// BackOffMax below AckWait would otherwise truncate entry 0, and the server
// then adopts that shorter value as AckWait (consumer.go:677-682) — the exact
// bug this derivation exists to prevent, reachable via CONSUMER_BACKOFF_MAX.
func TestDurableConsumerDefaults_BackOffMaxBelowAckWait(t *testing.T) {
	cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6,
		BackOffSteps: 3, BackOffFactor: 2, BackOffMax: 5 * time.Second,
	})

	require.NotEmpty(t, cc.BackOff)
	assert.Equal(t, cc.AckWait, cc.BackOff[0], "AckWait must survive a too-small BackOffMax")
	assert.Equal(t, []time.Duration{30 * time.Second, 30 * time.Second, 30 * time.Second}, cc.BackOff)
}

// An unlimited MaxDeliver skips the clamp, so BackOffSteps is otherwise
// unbounded and allocates whatever an operator typed.
func TestDurableConsumerDefaults_StepsAreBounded(t *testing.T) {
	cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: -1,
		BackOffSteps: 1_000_000, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	})

	assert.LessOrEqual(t, len(cc.BackOff), 32, "an absurd step count must be bounded")
}

func TestWithOutageRetryBudget(t *testing.T) {
	tests := []struct {
		name string
		in   stream.ConsumerSettings
		want int
	}{
		{
			name: "package default is raised to the outage budget",
			in:   stream.ConsumerSettings{MaxDeliver: stream.DefaultMaxDeliver},
			want: jsretry.DeliveriesFor(jsretry.DefaultBackoff, stream.OutageRetryWindow),
		},
		{
			name: "an operator's higher value is left alone",
			in:   stream.ConsumerSettings{MaxDeliver: 100},
			want: 100,
		},
		{
			name: "unlimited is left alone",
			in:   stream.ConsumerSettings{MaxDeliver: -1},
			want: -1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, stream.WithOutageRetryBudget(tc.in, jsretry.DefaultBackoff).MaxDeliver)
		})
	}
}

// The budget must be derived from the schedule the consumer ACTUALLY settles
// with, and must survive jitter. This is the test that was missing: the old one
// walked DefaultBackoff's nominal entries, so it certified a budget for a
// consumer that settled with a different, much shorter schedule — and ignored
// that equal jitter can halve every wait.
func TestOutageRetryBudget_GuaranteesTheWindowForEverySchedule(t *testing.T) {
	for name, schedule := range map[string][]time.Duration{
		"DefaultBackoff":    jsretry.DefaultBackoff,
		"LowLatencyBackoff": jsretry.LowLatencyBackoff,
	} {
		t.Run(name, func(t *testing.T) {
			got := stream.WithOutageRetryBudget(
				stream.ConsumerSettings{MaxDeliver: stream.DefaultMaxDeliver}, schedule)

			assert.GreaterOrEqual(t,
				jsretry.MinWindow(schedule, got.MaxDeliver), stream.OutageRetryWindow,
				"budget of %d deliveries must ride out %s even when every wait lands at its jittered minimum",
				got.MaxDeliver, stream.OutageRetryWindow)
		})
	}
}

// A shorter tail needs more deliveries to cover the same window. If these ever
// match, the budget has stopped depending on the schedule.
func TestOutageRetryBudget_TracksTheSchedule(t *testing.T) {
	in := stream.ConsumerSettings{MaxDeliver: stream.DefaultMaxDeliver}
	slow := stream.WithOutageRetryBudget(in, jsretry.DefaultBackoff).MaxDeliver
	fast := stream.WithOutageRetryBudget(in, jsretry.LowLatencyBackoff).MaxDeliver

	assert.Greater(t, fast, slow,
		"LowLatencyBackoff opens with smaller waits, so it needs more deliveries for the same window")
}

// An explicitly-tuned budget is the operator's call and must survive.
func TestOutageRetryBudget_LeavesAnExplicitBudgetAlone(t *testing.T) {
	in := stream.ConsumerSettings{MaxDeliver: 3}
	assert.Equal(t, 3, stream.WithOutageRetryBudget(in, jsretry.DefaultBackoff).MaxDeliver)
}

// Other fields must pass through untouched.
func TestWithOutageRetryBudget_LeavesOtherSettings(t *testing.T) {
	in := stream.ConsumerSettings{AckWait: 42 * time.Second, MaxDeliver: stream.DefaultMaxDeliver, MaxWaiting: 7, MaxAckPending: 9}
	got := stream.WithOutageRetryBudget(in, jsretry.DefaultBackoff)
	assert.Equal(t, in.AckWait, got.AckWait)
	assert.Equal(t, in.MaxWaiting, got.MaxWaiting)
	assert.Equal(t, in.MaxAckPending, got.MaxAckPending)
}
