package natsutil_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// stubMsg is a tiny test double for the Acker interface.
type stubMsg struct {
	ackCalled bool
	ackErr    error
}

func (s *stubMsg) Ack() error {
	s.ackCalled = true
	return s.ackErr
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

// Compile-time check that the stub satisfies Acker — this is what lets
// production code pass jetstream.Msg / oteljetstream.Msg without a wrapper.
var _ natsutil.Acker = (*stubMsg)(nil)
