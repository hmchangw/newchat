package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSyncMessagesFrom(t *testing.T) {
	t.Run("empty string disables filter (zero time)", func(t *testing.T) {
		got, err := parseSyncMessagesFrom("")
		require.NoError(t, err)
		assert.True(t, got.IsZero(), "empty input must yield zero time so the filter is disabled")
	})

	t.Run("valid YYYY-MM-DD parsed as UTC midnight", func(t *testing.T) {
		got, err := parseSyncMessagesFrom("2026-01-01")
		require.NoError(t, err)
		assert.Equal(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), got)
	})

	t.Run("rejects RFC3339 form", func(t *testing.T) {
		_, err := parseSyncMessagesFrom("2026-01-01T00:00:00Z")
		assert.Error(t, err)
	})

	t.Run("rejects garbage", func(t *testing.T) {
		_, err := parseSyncMessagesFrom("yesterday")
		assert.Error(t, err)
	})

	t.Run("rejects MM-DD-YYYY form", func(t *testing.T) {
		_, err := parseSyncMessagesFrom("01-15-2026")
		assert.Error(t, err)
	})
}
