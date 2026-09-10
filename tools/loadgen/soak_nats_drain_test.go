package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

type fakeSoakDrainConn struct {
	drainErr        error
	completeOnDrain bool
	handler         nats.ConnHandler
	drains          int
	closes          int
}

func (c *fakeSoakDrainConn) ClosedHandler() nats.ConnHandler { return c.handler }

func (c *fakeSoakDrainConn) SetClosedHandler(handler nats.ConnHandler) { c.handler = handler }

func (c *fakeSoakDrainConn) Close() { c.closes++ }

func (c *fakeSoakDrainConn) Drain() error {
	c.drains++
	if c.drainErr != nil {
		return c.drainErr
	}
	if c.completeOnDrain && c.handler != nil {
		c.handler(nil)
	}
	return nil
}

func TestDrainSoakNATS_UnderALeaseBudget(t *testing.T) {
	t.Run("a completed drain leaves the evidence alone", func(t *testing.T) {
		conn := &fakeSoakDrainConn{completeOnDrain: true}
		reasons := []string{}

		drainSoakNATS(conn, time.Minute, func(r string) { reasons = append(reasons, r) })

		assert.Equal(t, 1, conn.drains)
		assert.Zero(t, conn.closes)
		assert.Empty(t, reasons)
	})

	t.Run("a drain past the budget is abandoned and invalidated", func(t *testing.T) {
		conn := &fakeSoakDrainConn{}
		reasons := []string{}

		drainSoakNATS(conn, time.Millisecond, func(r string) { reasons = append(reasons, r) })

		assert.Equal(t, 1, conn.closes)
		assert.Equal(t, []string{invalidReasonLeaseAbort}, reasons)
	})

	t.Run("a drain that cannot start is treated the same way", func(t *testing.T) {
		// Drain refuses on a closed or already-draining connection, so it never
		// flushes: the pending publishes are in the same unknown state as a
		// drain that ran out of budget, and the evidence has to say so.
		conn := &fakeSoakDrainConn{drainErr: nats.ErrConnectionClosed}
		reasons := []string{}

		drainSoakNATS(conn, time.Minute, func(r string) { reasons = append(reasons, r) })

		assert.Equal(t, 1, conn.closes)
		assert.Equal(t, []string{invalidReasonLeaseAbort}, reasons)
	})
}

func TestDrainSoakNATS_OrdinaryShutdownIsUnbounded(t *testing.T) {
	conn := &fakeSoakDrainConn{drainErr: nats.ErrConnectionClosed}
	reasons := []string{}

	drainSoakNATS(conn, 0, func(r string) { reasons = append(reasons, r) })

	// No lease to protect: the drain is started and the process moves on, and a
	// refusal here is not evidence about the system under test.
	assert.Equal(t, 1, conn.drains)
	assert.Zero(t, conn.closes)
	assert.Empty(t, reasons)
}

func TestWaitSoakDrain_BoundsOnlyWhenTheLeaseSaysSo(t *testing.T) {
	t.Run("a drain inside the budget reports completion", func(t *testing.T) {
		done := make(chan struct{})
		close(done)

		assert.True(t, waitSoakDrain(done, time.Minute))
	})

	t.Run("a drain past the budget is abandoned", func(t *testing.T) {
		// Never closed: the lease boundary, not the drain, has to end the wait.
		assert.False(t, waitSoakDrain(make(chan struct{}), time.Millisecond))
	})
}
