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
	"github.com/hmchangw/chat/pkg/stream"
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
			MaxAckPending: 40000, BackOffSteps: 3, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
		},
	}
	cfg := retrylane.ConsumerConfig("site1", "message-worker", &s, retrylane.SlowBackoff(3, jsretry.DefaultBackoff))

	assert.Equal(t, "message-worker-retry", cfg.Durable)
	assert.Equal(t, []string{"chat.retry.site1.message-worker.>"}, cfg.FilterSubjects,
		"a retry consumer must never drain another service's escalations")
	assert.Equal(t, 40000, cfg.MaxAckPending,
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

	assert.Equal(t, 40000, cfg.Retry.Consumer.MaxAckPending,
		"the retry lane must default to its own 40000 budget, not the hot lane's 1000")
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

// Validate is the guard against a retry lane that is configured "on" but cannot
// do its job. Both failure modes are silent at runtime, which is why they are
// startup errors rather than clamps.
func TestSettingsValidate(t *testing.T) {
	base := func() retrylane.Settings {
		return retrylane.Settings{Enabled: true, FastSteps: 3,
			Consumer: retrylane.ConsumerSettings{MaxWorkers: 10}}
	}

	t.Run("accepts a sane configuration", func(t *testing.T) {
		s := base()
		assert.NoError(t, s.Validate())
	})

	// message-worker and notification-worker size their retry semaphore from this
	// value: make(chan struct{}, 0) is unbuffered, so the consume loop blocks on
	// its first send and nothing parked on RETRY-{siteID} ever drains again.
	t.Run("rejects a zero worker count", func(t *testing.T) {
		s := base()
		s.Consumer.MaxWorkers = 0
		require.Error(t, s.Validate())
		assert.Contains(t, s.Validate().Error(), "RETRY_CONSUMER_MAX_WORKERS")
	})

	t.Run("rejects a negative worker count", func(t *testing.T) {
		s := base()
		s.Consumer.MaxWorkers = -1
		assert.Error(t, s.Validate(), "make(chan struct{}, negative) panics outright")
	})

	// The retry consumer binds and drains regardless of Enabled — that asymmetry is
	// the rollback story — so an unusable worker count must fail even with the lane off.
	t.Run("checks the worker count even when the lane is disabled", func(t *testing.T) {
		s := base()
		s.Enabled, s.Consumer.MaxWorkers = false, 0
		assert.Error(t, s.Validate(), "a disabled lane still has to drain what is already parked")
	})

	// FastSteps <= 0 short-circuits Lane.shouldEscalate, so the lane reports itself
	// enabled and silently never escalates — the worst of both configurations.
	t.Run("rejects a non-positive fast-step split when enabled", func(t *testing.T) {
		s := base()
		s.FastSteps = 0
		require.Error(t, s.Validate())
		assert.Contains(t, s.Validate().Error(), "RETRY_LANE_FAST_STEPS")
	})

	t.Run("ignores the fast-step split when the lane is disabled", func(t *testing.T) {
		s := base()
		s.Enabled, s.FastSteps = false, 0
		assert.NoError(t, s.Validate(), "FastSteps is unused while the lane is off")
	})
}

// Relocating the wait must not shorten it. The hot consumer's MaxDeliver is
// derived from stream.OutageRetryWindow, but once a message escalates, the
// retry consumer's own MaxDeliver is the only budget left — at the package
// default of 3 that is ~12m against the hot lane's ~2h, so enabling the lane
// would make an outage lossier than leaving it off.
func TestConsumerConfigPreservesTheOutageRetryBudget(t *testing.T) {
	slow := retrylane.SlowBackoff(3, jsretry.DefaultBackoff) // {2m, 10m}

	t.Run("raises the package default to cover the outage window", func(t *testing.T) {
		s := retrylane.Settings{Enabled: true, FastSteps: 3,
			Consumer: retrylane.ConsumerSettings{MaxDeliver: 3, MaxWorkers: 10}}
		cc := retrylane.ConsumerConfig("site1", "message-worker", &s, slow)

		want := jsretry.DeliveriesFor(slow, stream.OutageRetryWindow)
		assert.Equal(t, want, cc.MaxDeliver,
			"the retry lane must carry the budget the hot lane handed it")
		assert.Greater(t, cc.MaxDeliver, 3, "the package default cannot ride out an hour")
	})

	// Same shape as stream.WithOutageRetryBudget: an operator who set the knob
	// explicitly wins, because only the untouched default is safe to reinterpret.
	t.Run("leaves an explicitly configured cap alone", func(t *testing.T) {
		s := retrylane.Settings{Enabled: true, FastSteps: 3,
			Consumer: retrylane.ConsumerSettings{MaxDeliver: 5, MaxWorkers: 10}}
		cc := retrylane.ConsumerConfig("site1", "message-worker", &s, slow)
		assert.Equal(t, 5, cc.MaxDeliver)
	})

	t.Run("never lowers the cap", func(t *testing.T) {
		s := retrylane.Settings{Enabled: true, FastSteps: 3,
			Consumer: retrylane.ConsumerSettings{MaxDeliver: 3, MaxWorkers: 10}}
		// A schedule whose tail alone already outlasts the window.
		long := []time.Duration{2 * stream.OutageRetryWindow}
		cc := retrylane.ConsumerConfig("site1", "message-worker", &s, long)
		assert.GreaterOrEqual(t, cc.MaxDeliver, 3)
	})
}
