package natsutil_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// stubMsg is a tiny test double for the Acker/Naker interfaces. Records
// whether Ack/Nak was called and returns a configurable error.
type stubMsg struct {
	ackCalled bool
	nakCalled bool
	nakDelay  time.Duration
	ackErr    error
	nakErr    error
}

func (s *stubMsg) Ack() error {
	s.ackCalled = true
	return s.ackErr
}

func (s *stubMsg) NakWithDelay(d time.Duration) error {
	s.nakCalled = true
	s.nakDelay = d
	return s.nakErr
}

func TestAck_Success(t *testing.T) {
	msg := &stubMsg{}
	natsutil.Ack(msg, "handler succeeded")
	assert.True(t, msg.ackCalled, "Ack() should be invoked on the message")
}

func TestAck_ErrorIsLoggedNotReturned(t *testing.T) {
	// Ack is fire-and-forget by design — any error from msg.Ack() is logged
	// and swallowed so callers don't have to branch on it. The helper's
	// contract is "try to ack; if it fails, log it and move on."
	msg := &stubMsg{ackErr: errors.New("connection closed")}
	natsutil.Ack(msg, "filtered")
	assert.True(t, msg.ackCalled)
}

// The delay is mandatory, not optional: a NAK carrying no delay is queued for
// immediate redelivery by the server, which burns the consumer's delivery
// budget in milliseconds instead of spacing retries across an outage.
func TestNak_ForwardsDelay(t *testing.T) {
	msg := &stubMsg{}
	natsutil.Nak(msg, "handler error", 30*time.Second)
	assert.True(t, msg.nakCalled, "NakWithDelay() should be invoked on the message")
	assert.Equal(t, 30*time.Second, msg.nakDelay)
}

func TestNak_ErrorIsLoggedNotReturned(t *testing.T) {
	msg := &stubMsg{nakErr: errors.New("consumer deleted")}
	natsutil.Nak(msg, "bulk failure", time.Second)
	assert.True(t, msg.nakCalled)
}

// Compile-time checks that the stub implements the interfaces the helpers
// require — this is what lets production code pass `jetstream.Msg` and
// `oteljetstream.Msg` to the helpers without a wrapper.
var (
	_ natsutil.Acker = (*stubMsg)(nil)
	_ natsutil.Naker = (*stubMsg)(nil)
)
