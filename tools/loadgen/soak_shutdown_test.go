package main

import (
	"context"
	"errors"
	"fmt"
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

// A run the durability watch stopped leaves through the same cancellation an
// operator's SIGTERM uses, and cancellation is a normal stop. So a run
// truncated because the ledger could not record the verdict disqualifying its
// evidence reported success — and if the journal recovered before shutdown,
// Close succeeded too and nothing anywhere said the window was cut short.
// Kubernetes reads the exit code, not the log.
func TestSoakRunExitCode(t *testing.T) {
	stopped := fmt.Errorf("%w: %s", errSoakLedgerNotDurable, "reconcile_capacity, wal")

	tests := []struct {
		name   string
		runErr error
		cause  error
		want   int
	}{
		{name: "a completed run succeeds", runErr: nil, cause: nil, want: 0},
		{
			name:   "an operator stopping the run is still a normal stop",
			runErr: context.Canceled,
			cause:  context.Canceled,
			want:   0,
		},
		{
			name:   "a run the durability watch stopped fails",
			runErr: context.Canceled,
			cause:  stopped,
			want:   2,
		},
		{
			name:   "a workload failure fails",
			runErr: errors.New("send lane collapsed"),
			cause:  nil,
			want:   1,
		},
		{
			name:   "lost durability outranks the workload error it caused",
			runErr: errors.New("send lane collapsed"),
			cause:  stopped,
			want:   2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, soakRunExitCode(tc.runErr, tc.cause))
		})
	}
}
