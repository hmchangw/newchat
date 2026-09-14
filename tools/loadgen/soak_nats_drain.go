package main

import (
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

// drainSoakNATS flushes what loadgen has already published, but never past the
// lease boundary. Waiting is the right default: a pending publish belongs to an
// operation the ledger has recorded, so dropping it would report data loss
// loadgen itself caused. Once the lease is at risk that trade reverses, because
// continuing to emit is the one thing the stop boundary promised not to do --
// so the connection is closed and the interval is marked inconclusive rather
// than left looking like a clean run that lost messages.
// soakDrainableConn is the slice of *nats.Conn the drain needs, so both ways
// the flush can fail are reachable from a test without a broker.
type soakDrainableConn interface {
	Drain() error
	Close()
	ClosedHandler() nats.ConnHandler
	SetClosedHandler(nats.ConnHandler)
}

func drainSoakNATS(
	nc soakDrainableConn,
	budget time.Duration,
	invalidate func(string),
) {
	if nc == nil {
		return
	}
	if budget <= 0 {
		// Ordinary shutdown, unchanged: start the drain and let the process
		// finish on its own schedule. No lease is at stake, so a refusal here
		// says nothing about the system under test.
		if err := nc.Drain(); err != nil {
			slog.Error("drain Cassandra soak NATS connection", "error", err)
		}
		return
	}
	closed := make(chan struct{})
	previous := nc.ClosedHandler()
	nc.SetClosedHandler(func(conn *nats.Conn) {
		if previous != nil {
			previous(conn)
		}
		close(closed)
	})
	// Drain refuses outright on a closed or already-draining connection, which
	// leaves the pending publishes in the same unknown state as a drain that
	// ran out of budget. Both are the same fact about the evidence, so they get
	// the same answer.
	err := nc.Drain()
	if err == nil && waitSoakDrain(closed, budget) {
		return
	}
	slog.Error(
		"abandoned the soak NATS drain at the lease boundary",
		"budget", budget,
		"error", err,
		"consequence", "unflushed publishes are dropped; the interval is inconclusive",
	)
	if invalidate != nil {
		invalidate(invalidReasonLeaseAbort)
	}
	nc.Close()
}

func waitSoakDrain(done <-chan struct{}, budget time.Duration) bool {
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
