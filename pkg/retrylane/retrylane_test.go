package retrylane_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/retrylane"
)

// fakeMsg is a retrylane.Msg double recording ack/nak behaviour.
type fakeMsg struct {
	data         []byte
	subject      string
	headers      nats.Header
	stream       string
	seq          uint64
	numDelivered uint64
	metaErr      error

	acked    bool
	naked    bool
	nakDelay time.Duration
}

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return &jetstream.MsgMetadata{
		Stream:       m.stream,
		NumDelivered: m.numDelivered,
		Sequence:     jetstream.SequencePair{Stream: m.seq},
	}, nil
}
func (m *fakeMsg) Ack() error { m.acked = true; return nil }
func (m *fakeMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return nil
}
func (m *fakeMsg) Data() []byte         { return m.data }
func (m *fakeMsg) Subject() string      { return m.subject }
func (m *fakeMsg) Headers() nats.Header { return m.headers }

type capturedPublish struct {
	called  bool
	subject string
	data    []byte
	hdr     nats.Header
	msgID   string
	err     error
}

func (c *capturedPublish) fn() retrylane.PublishFunc {
	return func(_ context.Context, subj string, data []byte, hdr nats.Header, msgID string) error {
		c.called = true
		c.subject = subj
		c.data = data
		c.hdr = hdr
		c.msgID = msgID
		return c.err
	}
}

var testBackoff = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}

func newMsg(delivered uint64) *fakeMsg {
	return &fakeMsg{
		data:         []byte(`{"msg":"hello"}`),
		subject:      "chat.msg.canonical.site1.created",
		headers:      nats.Header{},
		stream:       "MESSAGES-CANONICAL-site1",
		seq:          42,
		numDelivered: delivered,
	}
}

func newLane(pub retrylane.PublishFunc) *retrylane.Lane {
	return &retrylane.Lane{
		Consumer:  "message-worker",
		SiteID:    "site1",
		Publish:   pub,
		Enabled:   true,
		FastSteps: 3,
	}
}

func TestSettleAcksOnSuccess(t *testing.T) {
	pub := &capturedPublish{}
	msg := newMsg(1)
	newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, nil)

	assert.True(t, msg.acked)
	assert.False(t, msg.naked)
	assert.False(t, pub.called, "success never escalates")
}

func TestSettleNaksBelowThreshold(t *testing.T) {
	for delivered := uint64(1); delivered <= 3; delivered++ {
		pub := &capturedPublish{}
		msg := newMsg(delivered)
		newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

		assert.True(t, msg.naked, "delivery %d is within the fast budget", delivered)
		assert.False(t, msg.acked)
		assert.False(t, pub.called)
		assert.Positive(t, msg.nakDelay, "a zero delay would be a bare nak")
	}
}

func TestSettleEscalatesAboveThreshold(t *testing.T) {
	pub := &capturedPublish{}
	msg := newMsg(4)
	newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	require.True(t, pub.called, "the fast budget is spent at delivery 4")
	assert.Equal(t, "chat.retry.site1.message-worker.slow", pub.subject)
	assert.Equal(t, []byte(`{"msg":"hello"}`), pub.data, "the body must be byte-identical")
	assert.Equal(t, "MESSAGES-CANONICAL-site1:42:message-worker", pub.msgID)
	assert.Equal(t, "message-worker", pub.hdr.Get(retrylane.HeaderConsumer))
	assert.True(t, msg.acked, "the hot lane releases the slot after a successful republish")
	assert.False(t, msg.naked)
}

func TestSettleNeverEscalatesPermanent(t *testing.T) {
	pub := &capturedPublish{}
	msg := newMsg(9)
	err := errcode.Permanent(errcode.BadRequest("malformed payload"))
	newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, err)

	assert.False(t, pub.called, "a permanent error can never succeed; escalating it wastes the lane")
	assert.True(t, msg.acked, "permanent stays an Ack-drop")
	assert.False(t, msg.naked)
}

func TestSettleNaksWhenPublishFails(t *testing.T) {
	pub := &capturedPublish{err: errors.New("stream unavailable")}
	msg := newMsg(4)
	newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	require.True(t, pub.called)
	assert.False(t, msg.acked, "acking after a failed republish would silently lose the message")
	assert.True(t, msg.naked, "fall back to in-place redelivery")
}

func TestSettleDisabledIsPureJsretry(t *testing.T) {
	pub := &capturedPublish{}
	lane := newLane(pub.fn())
	lane.Enabled = false
	msg := newMsg(9)
	lane.Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	assert.False(t, pub.called, "the flag is the rollback; disabled must behave exactly as before")
	assert.True(t, msg.naked)
	assert.False(t, msg.acked)
}

func TestSettleFallsBackWhenMetadataUnavailable(t *testing.T) {
	pub := &capturedPublish{}
	msg := newMsg(4)
	msg.metaErr = errors.New("no metadata")
	newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	assert.False(t, pub.called, "without NumDelivered we cannot know the budget is spent")
	assert.True(t, msg.naked)
}

func TestSettleWithoutPublishFuncDegradesToJsretry(t *testing.T) {
	lane := &retrylane.Lane{Consumer: "c", SiteID: "s", Enabled: true, FastSteps: 3}
	msg := newMsg(9)
	lane.Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	assert.True(t, msg.naked, "a misconfigured lane must degrade, never panic")
	assert.False(t, msg.acked)
}

func TestSettleCallsOnEscalateBeforeAck(t *testing.T) {
	pub := &capturedPublish{}
	msg := newMsg(4)
	lane := newLane(pub.fn())

	var calledBeforeAck bool
	lane.OnEscalate = func() { calledBeforeAck = !msg.acked }

	lane.Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	assert.True(t, calledBeforeAck,
		"the delivery must be labelled escalated before the Ack records it as a success")
}

func TestSettleDoesNotCallOnEscalateWhenPublishFails(t *testing.T) {
	pub := &capturedPublish{err: errors.New("stream unavailable")}
	msg := newMsg(4)
	lane := newLane(pub.fn())

	var called bool
	lane.OnEscalate = func() { called = true }

	lane.Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	assert.False(t, called, "nothing escalated, so there is nothing to label")
}

func TestWithEscalationHookDoesNotMutateTheOriginal(t *testing.T) {
	base := newLane((&capturedPublish{}).fn())
	derived := base.WithEscalationHook(func() {})

	assert.Nil(t, base.OnEscalate, "the shared lane is read by every message goroutine")
	assert.NotNil(t, derived.OnEscalate)
	assert.Equal(t, base.Consumer, derived.Consumer)
	assert.Equal(t, base.FastSteps, derived.FastSteps)
}

// captureLogs swaps the default logger for a JSON handler writing to a buffer,
// restoring it on cleanup. The retry lane logs through slog's default.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func TestSettleLogsASuccessfulEscalation(t *testing.T) {
	buf := captureLogs(t)
	pub := &capturedPublish{}
	lane := newLane(pub.fn())
	msg := newMsg(4)
	msg.headers.Set(natsutil.RequestIDHeader, "01970a4f-8c2d-7c9a-abcd-e0123456789f")

	ctx := natsutil.WithRequestID(context.Background(), "01970a4f-8c2d-7c9a-abcd-e0123456789f")
	lane.Settle(ctx, msg, testBackoff, errcode.NotFound("room not found"))

	require.True(t, msg.acked, "precondition: the escalation succeeded")
	var rec map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec),
		"on-call greps the logs for context on outcome=\"escalated\"; there must be a line to find")
	assert.Equal(t, "WARN", rec["level"])
	assert.Equal(t, "message-worker", rec["consumer"])
	assert.Equal(t, "chat.msg.canonical.site1.created", rec["origin_subject"])
	assert.Equal(t, float64(4), rec["attempt"])
	assert.Equal(t, "not_found", rec["reason"])
	assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f", rec["request_id"])
}

func TestSettleEscalationLogNeverCarriesTheBody(t *testing.T) {
	buf := captureLogs(t)
	pub := &capturedPublish{}
	lane := newLane(pub.fn())
	msg := newMsg(4)
	msg.data = []byte(`{"text":"my bank password is hunter2"}`)

	lane.Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	require.True(t, msg.acked)
	assert.NotContains(t, buf.String(), "hunter2", "the escalation log is bounded fields only")
	assert.NotContains(t, buf.String(), "mongo down", "the reason is a category, never the error text")
}

func TestSettleLogsNothingOnTheNonEscalatingPath(t *testing.T) {
	buf := captureLogs(t)
	pub := &capturedPublish{}
	lane := newLane(pub.fn())

	lane.Settle(context.Background(), newMsg(4), testBackoff, nil)

	assert.NotContains(t, buf.String(), "escalat", "an Ack is not an escalation")
}

func TestOriginSubject(t *testing.T) {
	withOrigin := nats.Header{}
	withOrigin.Set(retrylane.HeaderOriginSubject, "chat.msg.canonical.site1.created")

	tests := []struct {
		name     string
		headers  nats.Header
		fallback string
		want     string
	}{
		{
			name:     "retry-lane delivery classifies from the origin subject",
			headers:  withOrigin,
			fallback: "chat.retry.site1.message-worker.slow",
			want:     "chat.msg.canonical.site1.created",
		},
		{
			name:     "hot-lane delivery falls back to its own subject",
			headers:  nats.Header{},
			fallback: "chat.msg.canonical.site1.created",
			want:     "chat.msg.canonical.site1.created",
		},
		{
			name:     "nil headers fall back",
			headers:  nil,
			fallback: "chat.msg.canonical.site1.created",
			want:     "chat.msg.canonical.site1.created",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, retrylane.OriginSubject(tt.headers, tt.fallback))
		})
	}
}
