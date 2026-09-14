package mongoutil

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
)

func TestBreakerConfig_Validate_AcceptsValid(t *testing.T) {
	require.NoError(t, BreakerConfig{Fails: 5, Cooldown: 10 * time.Second}.Validate(""))
	require.NoError(t, BreakerConfig{Fails: 0, Cooldown: 0}.Validate(""), "zero disables the breaker")
}

func TestBreakerConfig_Validate_RejectsNegatives(t *testing.T) {
	tests := []struct {
		name string
		cfg  BreakerConfig
		want string
	}{
		{"negative fails", BreakerConfig{Fails: -1, Cooldown: time.Second}, "HISTORY_MONGO_BREAKER_FAILS"},
		{"negative cooldown", BreakerConfig{Fails: 5, Cooldown: -time.Second}, "HISTORY_MONGO_BREAKER_COOLDOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate("HISTORY_")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestBreakerConfig_New_OpensAtThreshold(t *testing.T) {
	b := BreakerConfig{Fails: 2, Cooldown: time.Minute}.New(context.Background(), "test")
	boom := errors.New("boom")

	require.ErrorIs(t, b.Do(func() error { return boom }), boom)
	assert.Equal(t, circuitbreaker.StateClosed, b.State(), "one failure is under the budget")

	require.ErrorIs(t, b.Do(func() error { return boom }), boom)
	assert.Equal(t, circuitbreaker.StateOpen, b.State())
	assert.ErrorIs(t, b.Do(func() error { return nil }), circuitbreaker.ErrOpen, "an open breaker fences the call")
}

// A zero budget turns the protection off without unwiring it, so a deployment
// can disable fencing by config.
func TestBreakerConfig_New_ZeroFailsDisablesTheBreaker(t *testing.T) {
	b := BreakerConfig{Fails: 0, Cooldown: time.Minute}.New(context.Background(), "test")
	boom := errors.New("boom")

	for range 10 {
		require.ErrorIs(t, b.Do(func() error { return boom }), boom)
	}
	assert.Equal(t, circuitbreaker.StateClosed, b.State())
}

// New must not impose a failure predicate of its own: two call sites count
// ErrNoDocuments today and must keep doing so until they opt out explicitly.
func TestBreakerConfig_New_CountsEveryErrorByDefault(t *testing.T) {
	b := BreakerConfig{Fails: 1, Cooldown: time.Minute}.New(context.Background(), "test")

	require.ErrorIs(t, b.Do(func() error { return mongo.ErrNoDocuments }), mongo.ErrNoDocuments)

	assert.Equal(t, circuitbreaker.StateOpen, b.State())
}

func TestBreakerConfig_New_AppliesCallerOptions(t *testing.T) {
	b := BreakerConfig{Fails: 1, Cooldown: time.Minute}.New(context.Background(), "test",
		circuitbreaker.WithFailurePredicate(BreakerFailure()))

	require.ErrorIs(t, b.Do(func() error { return mongo.ErrNoDocuments }), mongo.ErrNoDocuments)

	assert.Equal(t, circuitbreaker.StateClosed, b.State(), "a healthy absence is not evidence the database is unwell")
}

func TestBreakerFailure(t *testing.T) {
	sentinel := errors.New("no such user")
	tests := []struct {
		name  string
		err   error
		extra []error
		want  bool
	}{
		{"nil is not a failure", nil, nil, false},
		{"ErrNoDocuments is always exempt", mongo.ErrNoDocuments, nil, false},
		{"wrapped ErrNoDocuments is exempt", fmt.Errorf("find room: %w", mongo.ErrNoDocuments), nil, false},
		{"context.Canceled is always exempt", context.Canceled, nil, false},
		{"an extra sentinel is exempt", sentinel, []error{sentinel}, false},
		{"an unlisted sentinel counts", sentinel, nil, true},
		{"DeadlineExceeded counts", context.DeadlineExceeded, nil, true},
		{"an unrecognised error counts", errors.New("connection reset"), nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, BreakerFailure(tt.extra...)(tt.err))
		})
	}
}
