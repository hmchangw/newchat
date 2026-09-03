package natsutil_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// stubJS satisfies only the one method the helper needs, which is the point of
// the helper taking a narrow interface: every service's test can be this small.
type stubJS struct {
	msgs []*nats.Msg
	err  error
}

func (s *stubJS) PublishMsg(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	s.msgs = append(s.msgs, msg)
	if s.err != nil {
		return nil, s.err
	}
	return &jetstream.PubAck{}, nil
}

type stubCore struct {
	msgs []*nats.Msg
	err  error
}

func (s *stubCore) PublishMsg(_ context.Context, msg *nats.Msg) error {
	s.msgs = append(s.msgs, msg)
	return s.err
}

// Seven services carried a copy of this closure; the helper is what lets them
// share one. Its contract: NewMsg headers are stamped, the msgID becomes the
// Nats-Msg-Id, and a failure is wrapped with the subject for triage.
func TestJetStreamPublishFunc(t *testing.T) {
	js := &stubJS{}
	publish := natsutil.JetStreamPublishFunc(js, natsmetrics.Publisher{})

	ctx := natsutil.WithRequestID(context.Background(), "01970a4f-8c2d-7c9a-abcd-e0123456789f")
	require.NoError(t, publish(ctx, "chat.outbox.a.b.member_added", []byte(`{}`), "dedup-1"))

	require.Len(t, js.msgs, 1)
	assert.Equal(t, "chat.outbox.a.b.member_added", js.msgs[0].Subject)
	assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f", js.msgs[0].Header.Get(natsutil.RequestIDHeader),
		"NewMsg must stamp the request id so the consumer's log lines correlate")
}

func TestJetStreamPublishFunc_WrapsErrorWithSubject(t *testing.T) {
	js := &stubJS{err: errors.New("no responders")}
	publish := natsutil.JetStreamPublishFunc(js, natsmetrics.Publisher{})

	err := publish(context.Background(), "chat.outbox.a.b.x", []byte(`{}`), "d")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "chat.outbox.a.b.x")
	assert.Contains(t, err.Error(), "no responders")
}

// A zero natsmetrics.Publisher is the documented "not instrumented" value; the
// helper must accept it rather than force every caller to wire metrics.
func TestJetStreamPublishFunc_ZeroMetricsIsSafe(t *testing.T) {
	js := &stubJS{err: errors.New("boom")}
	publish := natsutil.JetStreamPublishFunc(js, natsmetrics.Publisher{}, natsutil.WithPublishLabels(natsmetrics.DestinationOutbox, natsmetrics.OperationRecipientPublish))

	assert.Error(t, publish(context.Background(), "s", nil, "d"))
}

func TestCorePublishFunc(t *testing.T) {
	nc := &stubCore{}
	publish := natsutil.CorePublishFunc(nc, natsmetrics.Publisher{})

	require.NoError(t, publish(context.Background(), "chat.server.broadcast.a.thread.tcount", []byte(`{}`)))
	require.Len(t, nc.msgs, 1)
	assert.Equal(t, "chat.server.broadcast.a.thread.tcount", nc.msgs[0].Subject)
}

func TestCorePublishFunc_WrapsErrorWithSubject(t *testing.T) {
	nc := &stubCore{err: errors.New("closed")}
	publish := natsutil.CorePublishFunc(nc, natsmetrics.Publisher{})

	err := publish(context.Background(), "chat.user.alice.x", nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "chat.user.alice.x")
	assert.Contains(t, err.Error(), "closed")
}
