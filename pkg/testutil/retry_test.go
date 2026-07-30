package testutil

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errFirstAttempt = errors.New("first attempt failed")
	errLaterAttempt = errors.New("later attempt failed")
)

func TestRetryUntil_SucceedsFirstAttempt(t *testing.T) {
	calls := 0
	err := retryUntil(time.Second, func() error {
		calls++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "a succeeding fn must not be retried")
}

func TestRetryUntil_SucceedsAfterTransientFailures(t *testing.T) {
	calls := 0
	err := retryUntil(5*time.Second, func() error {
		calls++
		if calls < 3 {
			return errFirstAttempt
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestRetryUntil_ReturnsLastErrorOnTimeout(t *testing.T) {
	const timeout = 250 * time.Millisecond
	calls := 0
	var lastCall time.Time
	start := time.Now()
	err := retryUntil(timeout, func() error {
		calls++
		lastCall = time.Now()
		if calls == 1 {
			return errFirstAttempt
		}
		return errLaterAttempt
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, errLaterAttempt, "must surface the last error")
	assert.NotErrorIs(t, err, errFirstAttempt, "must not surface the first error")
	assert.Greater(t, calls, 1, "should have retried before giving up")
	// A failure just before the deadline must not buy a full extra poll interval.
	assert.False(t, lastCall.After(start.Add(timeout+20*time.Millisecond)),
		"fn must not be attempted after the deadline")
}

// A non-positive timeout must still attempt once rather than never calling fn.
func TestRetryUntil_RunsAtLeastOnce(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{name: "zero timeout", timeout: 0},
		{name: "negative timeout", timeout: -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			err := retryUntil(tt.timeout, func() error {
				calls++
				return errFirstAttempt
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, errFirstAttempt)
			assert.Equal(t, 1, calls)
		})
	}
}
