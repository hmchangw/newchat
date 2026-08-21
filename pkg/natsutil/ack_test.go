package natsutil_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// stubMsg is a tiny test double for the Acker/Naker interfaces. Records
// whether Ack/Nak was called and returns a configurable error.
type stubMsg struct {
	ackCalled bool
	nakCalled bool
	ackErr    error
	nakErr    error
}

func (s *stubMsg) Ack() error {
	s.ackCalled = true
	return s.ackErr
}

func (s *stubMsg) Nak() error {
	s.nakCalled = true
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

// Compile-time checks that the stub implements the interfaces the helpers
// require — this is what lets production code pass `jetstream.Msg` and
// `oteljetstream.Msg` to the helpers without a wrapper.
var _ natsutil.Acker = (*stubMsg)(nil)
