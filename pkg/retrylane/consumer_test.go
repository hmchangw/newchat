package retrylane_test

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
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
	cfg := retrylane.ConsumerConfig("site1", "message-worker", s)

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
