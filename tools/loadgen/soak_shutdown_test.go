package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The ledger refusing to close over an unpersisted invalidation only means
// something if the process says so. Logging the error and returning 0 leaves
// the run reporting success over evidence it could not make durable, which is
// the outcome the barrier exists to prevent.
func TestSoakShutdownExitCode(t *testing.T) {
	tests := []struct {
		name     string
		current  int
		closeErr error
		want     int
	}{
		{name: "a clean close leaves success alone", current: 0, closeErr: nil, want: 0},
		{
			name:     "a close that lost durability fails the run",
			current:  0,
			closeErr: errors.New("could not persist invalidation reconcile_capacity"),
			want:     2,
		},
		{
			name:     "an earlier failure keeps its own code",
			current:  1,
			closeErr: errors.New("could not persist invalidation wal"),
			want:     1,
		},
		{name: "a clean close does not rescue an earlier failure", current: 1, closeErr: nil, want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, soakShutdownExitCode(tc.current, tc.closeErr))
		})
	}
}
