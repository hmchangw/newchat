package roomkeystore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
