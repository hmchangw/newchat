package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// fakeInboxMsg is an inboxMsg test double recording how the event was settled.
// It deliberately implements NakWithDelay only — a delay-less Nak is not part
// of the interface, so the "instant redelivery burns MaxDeliver" regression
// cannot be reintroduced without changing the type.
type fakeInboxMsg struct {
	subject      string
	data         []byte
	headers      nats.Header
	numDelivered uint64
	metaErr      error

	acks      int
	nakDelays []time.Duration
	ackErr    error
	nakErr    error
}

func (m *fakeInboxMsg) Subject() string      { return m.subject }
func (m *fakeInboxMsg) Data() []byte         { return m.data }
func (m *fakeInboxMsg) Headers() nats.Header { return m.headers }
func (m *fakeInboxMsg) Ack() error           { m.acks++; return m.ackErr }

func (m *fakeInboxMsg) NakWithDelay(d time.Duration) error {
	m.nakDelays = append(m.nakDelays, d)
	return m.nakErr
}

func (m *fakeInboxMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return &jetstream.MsgMetadata{NumDelivered: m.numDelivered}, nil
}

// handlerFunc adapts a function to eventHandler.
type handlerFunc func(context.Context, []byte) error

func (f handlerFunc) HandleEvent(ctx context.Context, data []byte) error { return f(ctx, data) }

func newFakeMsg() *fakeInboxMsg {
	return &fakeInboxMsg{
		subject:      "chat.inbox.site-a.external.member_added",
		data:         []byte(`{"type":"member_added"}`),
		numDelivered: 1,
	}
}

func TestProcessEvent_Success_Acks(t *testing.T) {
	msg := newFakeMsg()
	var gotData []byte
	processEvent(context.Background(), handlerFunc(func(_ context.Context, data []byte) error {
		gotData = data
		return nil
	}), msg)

	assert.Equal(t, msg.data, gotData, "handler must receive the message payload")
	assert.Equal(t, 1, msg.acks, "a handled event must Ack exactly once")
	assert.Empty(t, msg.nakDelays, "success must not Nak")
}

func TestProcessEvent_TransientError_NaksWithBackoffDelay(t *testing.T) {
	// A transient failure must be redelivered with a DELAY. A delay-less Nak
	// triggers instant redelivery, which burns the consumer's whole delivery
	// budget within milliseconds and drops the federation event for good.
	tests := []struct {
		name         string
		numDelivered uint64
		wantDelay    time.Duration
	}{
		{name: "first delivery", numDelivered: 1, wantDelay: 1 * time.Second},
		{name: "second delivery", numDelivered: 2, wantDelay: 5 * time.Second},
		{name: "third delivery", numDelivered: 3, wantDelay: 30 * time.Second},
		{name: "fourth delivery", numDelivered: 4, wantDelay: 2 * time.Minute},
		{name: "beyond the schedule reuses the last entry", numDelivered: 9, wantDelay: 2 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := newFakeMsg()
			msg.numDelivered = tt.numDelivered
			processEvent(context.Background(), handlerFunc(func(context.Context, []byte) error {
				return errors.New("mongo unavailable")
			}), msg)

			require.Len(t, msg.nakDelays, 1, "a transient failure must Nak for redelivery")
			assert.Equal(t, tt.wantDelay, msg.nakDelays[0])
			assert.Positive(t, msg.nakDelays[0], "the Nak must carry a delay, never instant redelivery")
			assert.Zero(t, msg.acks, "a transient failure must not Ack — the event would be lost")
		})
	}
}

func TestProcessEvent_UnknownUserRace_IsRetriedNotDropped(t *testing.T) {
	// handleMemberAdded returns a plain error when a referenced account has not
	// replicated yet. That is a federation race, so it must redeliver — the
	// subscription replica is the only record of the room on this site.
	msg := newFakeMsg()
	processEvent(context.Background(), handlerFunc(func(context.Context, []byte) error {
		return errors.New("member_added references unknown users [alice] in room r1")
	}), msg)

	assert.Zero(t, msg.acks)
	require.Len(t, msg.nakDelays, 1)
	assert.Positive(t, msg.nakDelays[0])
}

func TestProcessEvent_PermanentError_Acks(t *testing.T) {
	msg := newFakeMsg()
	processEvent(context.Background(), handlerFunc(func(context.Context, []byte) error {
		return errcode.Permanent(errcode.BadRequest("unmarshal room_renamed payload"))
	}), msg)

	assert.Equal(t, 1, msg.acks, "a poison event must be Ack-dropped so redelivery stops")
	assert.Empty(t, msg.nakDelays, "a permanent failure must not be retried")
}

func TestProcessEvent_HandlerPanic_AcksAsPoisonDrop(t *testing.T) {
	msg := newFakeMsg()
	assert.NotPanics(t, func() {
		processEvent(context.Background(), handlerFunc(func(context.Context, []byte) error {
			panic("boom")
		}), msg)
	}, "a handler panic must be contained so the worker survives")

	assert.Equal(t, 1, msg.acks, "jobguard drops the poison message")
	assert.Empty(t, msg.nakDelays)
}

func TestProcessEvent_StampsRequestIDFromHeaders(t *testing.T) {
	const inbound = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	msg := newFakeMsg()
	msg.headers = nats.Header{natsutil.RequestIDHeader: []string{inbound}}

	var got string
	processEvent(context.Background(), handlerFunc(func(ctx context.Context, _ []byte) error {
		got = natsutil.RequestIDFromContext(ctx)
		return nil
	}), msg)

	assert.Equal(t, inbound, got, "the inbound request id must reach the handler context")
}

func TestProcessEvent_MissingMetadata_StillNaksWithDelay(t *testing.T) {
	msg := newFakeMsg()
	msg.metaErr = errors.New("not a jetstream message")
	processEvent(context.Background(), handlerFunc(func(context.Context, []byte) error {
		return errors.New("transient")
	}), msg)

	require.Len(t, msg.nakDelays, 1)
	assert.Positive(t, msg.nakDelays[0], "an unreadable delivery count must still back off")
}
