package retrylane_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/retrylane"
)

func TestSlowBackoffIsTheTailOfTheFullSchedule(t *testing.T) {
	got := retrylane.SlowBackoff(3, jsretry.DefaultBackoff)
	assert.Equal(t, []time.Duration{2 * time.Minute, 10 * time.Minute}, got,
		"the budget is relocated, not redefined: slow is the tail the fast rungs left")
}

func TestSlowBackoffTotalPlusFastEqualsOriginalBudget(t *testing.T) {
	var fast, slow time.Duration
	for _, d := range jsretry.DefaultBackoff[:3] {
		fast += d
	}
	for _, d := range retrylane.SlowBackoff(3, jsretry.DefaultBackoff) {
		slow += d
	}
	var full time.Duration
	for _, d := range jsretry.DefaultBackoff {
		full += d
	}
	assert.Equal(t, full, fast+slow, "total patience per message must be unchanged")
}

func TestSlowBackoffDegradesSafely(t *testing.T) {
	tests := []struct {
		name      string
		fastSteps int
		full      []time.Duration
	}{
		{"fastSteps beyond the schedule", 99, jsretry.DefaultBackoff},
		{"empty schedule", 3, nil},
		{"negative fastSteps", -1, jsretry.DefaultBackoff},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retrylane.SlowBackoff(tt.fastSteps, tt.full)
			require.NotEmpty(t, got, "an empty schedule would make jsretry fall back to a 1ms nak")
		})
	}
}

func TestConsumerConfigFiltersToItsOwnConsumer(t *testing.T) {
	s := retrylane.Settings{
		Enabled:   true,
		FastSteps: 3,
		Consumer: retrylane.ConsumerSettings{
			AckWait: 30 * time.Second, MaxDeliver: 3, MaxWaiting: 512,
			MaxAckPending: 4000, BackOffSteps: 3, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
		},
	}
	cfg := retrylane.ConsumerConfig("site1", "message-worker", &s)

	assert.Equal(t, "message-worker-retry", cfg.Durable)
	assert.Equal(t, []string{"chat.retry.site1.message-worker.>"}, cfg.FilterSubjects,
		"a retry consumer must never drain another service's escalations")
	assert.Equal(t, 4000, cfg.MaxAckPending,
		"the retry lane holds the long waits, so it needs its own large budget")
}

// wrapperConfig embeds Settings the way a service's own Config would, so
// env.Parse exercises the real default-resolution path instead of a struct
// literal — a struct literal is exactly the blind spot that let the retry
// lane silently inherit the hot lane's 1000-slot ack-pending budget: a nested
// stream.ConsumerSettings carries its own envDefault tags regardless of the
// outer struct, so it could never default to anything else under env.Parse.
type wrapperConfig struct {
	Retry retrylane.Settings `envPrefix:"RETRY_"`
}

func TestSettingsConsumerDefaultsThroughEnvParse(t *testing.T) {
	cfg, err := env.ParseAs[wrapperConfig]()
	require.NoError(t, err)

	assert.Equal(t, 4000, cfg.Retry.Consumer.MaxAckPending,
		"the retry lane must default to its own 4000 budget, not the hot lane's 1000")
	assert.Equal(t, 3, cfg.Retry.Consumer.MaxDeliver, "MaxDeliver counts retry-lane attempts only")
}

func TestSettingsConsumerOverrideSurvivesEnvParse(t *testing.T) {
	t.Setenv("RETRY_CONSUMER_MAX_ACK_PENDING", "250")

	cfg, err := env.ParseAs[wrapperConfig]()
	require.NoError(t, err)

	assert.Equal(t, 250, cfg.Retry.Consumer.MaxAckPending, "an operator override must survive")
}

func TestSkipMissingStream(t *testing.T) {
	tests := []struct {
		name    string
		enabled bool
		err     error
		want    bool
	}{
		{"disabled lane, stream absent", false, fmt.Errorf("create consumer: %w", jetstream.ErrStreamNotFound), true},
		{"disabled lane, bare not-found", false, jetstream.ErrStreamNotFound, true},
		{"enabled lane, stream absent", true, jetstream.ErrStreamNotFound, false},
		{"disabled lane, some other bind failure", false, errors.New("nats: connection closed"), false},
		{"disabled lane, consumer config rejected", false, jetstream.ErrConsumerNotFound, false},
		{"no error at all", false, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want,
				retrylane.SkipMissingStream(context.Background(), "RETRY-site1", tt.enabled, tt.err))
		})
	}
}

func TestSettingsConsumerMaxWorkersDefaultsThroughEnvParse(t *testing.T) {
	cfg, err := env.ParseAs[wrapperConfig]()
	require.NoError(t, err)

	assert.Equal(t, 10, cfg.Retry.Consumer.MaxWorkers,
		"spec §4: deliberately small — it doubles as the recovery-herd damper, "+
			"and the retry loop runs in the same process as the hot loop")
}

func TestSettingsConsumerMaxWorkersOverrideSurvivesEnvParse(t *testing.T) {
	t.Setenv("RETRY_CONSUMER_MAX_WORKERS", "42")

	cfg, err := env.ParseAs[wrapperConfig]()
	require.NoError(t, err)

	assert.Equal(t, 42, cfg.Retry.Consumer.MaxWorkers, "an operator override must survive")
}

func TestSettingsConsumerMaxWorkersIsNotTheHotLaneDefault(t *testing.T) {
	cfg, err := env.ParseAs[wrapperConfig]()
	require.NoError(t, err)

	assert.NotEqual(t, 100, cfg.Retry.Consumer.MaxWorkers,
		"reusing the hot loop's MAX_WORKERS default would make the process-wide "+
			"in-flight cap 2×MaxWorkers, reached precisely during an incident")
}
