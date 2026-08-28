package roomkeystore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// OpenMongo validates before touching the DB, so a nil database proves the guards short-circuit.
func TestOpenMongo_RejectsNonPositiveDurations(t *testing.T) {
	tests := []struct {
		name        string
		gracePeriod time.Duration
		retiredTTL  time.Duration
		wantErr     string
	}{
		{"zero grace period", 0, 20 * time.Minute, "room key grace period"},
		{"negative grace period", -time.Second, 20 * time.Minute, "room key grace period"},
		{"zero retired ttl", 24 * time.Hour, 0, "retired room key TTL"},
		{"negative retired ttl", 24 * time.Hour, -time.Second, "retired room key TTL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := OpenMongo(context.Background(), nil, tt.gracePeriod, tt.retiredTTL)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Nil(t, store, "no store may be returned alongside an error")
		})
	}
}

// The key.get that resolves a stamped version is served through OpenMongo, while
// broadcast-worker encrypts against its own handle. If the two disagree about
// falling back to a secondary, the producer keeps delivering messages whose keys
// the consumer cannot fetch — worse than both failing together.
func TestOpenMongo_ReadPreferenceOption(t *testing.T) {
	t.Run("defaults to primaryPreferred", func(t *testing.T) {
		cfg := newOpenConfig()
		require.NotNil(t, cfg.readPref)
		assert.Equal(t, readpref.PrimaryPreferredMode, cfg.readPref.Mode())
	})

	t.Run("explicit preference is honoured", func(t *testing.T) {
		cfg := newOpenConfig(WithKeyReadPreference(readpref.Primary()))
		require.NotNil(t, cfg.readPref)
		assert.Equal(t, readpref.PrimaryMode, cfg.readPref.Mode())
	})

	t.Run("nil preference falls back to the default rather than unsetting it", func(t *testing.T) {
		cfg := newOpenConfig(WithKeyReadPreference(nil))
		require.NotNil(t, cfg.readPref, "a nil option must not leave the handles on the driver default")
		assert.Equal(t, readpref.PrimaryPreferredMode, cfg.readPref.Mode())
	})
}
