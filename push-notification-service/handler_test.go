package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

type fakeDispatcher struct {
	calls int32
	last  *model.PushNotificationEvent
	err   error
	perm  bool
}

func (f *fakeDispatcher) Dispatch(_ context.Context, evt *model.PushNotificationEvent) error {
	atomic.AddInt32(&f.calls, 1)
	f.last = evt
	if f.err != nil {
		if f.perm {
			return errcode.Permanent(errcode.Internal("bad device", errcode.WithCause(f.err)))
		}
		return f.err
	}
	return nil
}

type fakeJSMsg struct {
	data []byte
	acks int32
	naks int32
}

func (f *fakeJSMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (f *fakeJSMsg) Data() []byte                              { return f.data }
func (f *fakeJSMsg) Headers() nats.Header                      { return nil }
func (f *fakeJSMsg) Subject() string                           { return "" }
func (f *fakeJSMsg) Reply() string                             { return "" }
func (f *fakeJSMsg) Ack() error                                { atomic.AddInt32(&f.acks, 1); return nil }
func (f *fakeJSMsg) DoubleAck(_ context.Context) error         { return nil }
func (f *fakeJSMsg) Nak() error                                { atomic.AddInt32(&f.naks, 1); return nil }
func (f *fakeJSMsg) NakWithDelay(_ time.Duration) error        { atomic.AddInt32(&f.naks, 1); return nil }
func (f *fakeJSMsg) InProgress() error                         { return nil }
func (f *fakeJSMsg) Term() error                               { return nil }
func (f *fakeJSMsg) TermWithReason(_ string) error             { return nil }

func encode(t *testing.T, evt *model.PushNotificationEvent) []byte {
	t.Helper()
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	return data
}

func TestDispatchSuccessAcks(t *testing.T) {
	d := &fakeDispatcher{}
	h := newHandler(d)

	evt := &model.PushNotificationEvent{
		ID:       "m1-b0",
		RoomID:   "r1",
		Accounts: []string{"alice"},
		Data:     model.PushNotificationData{RoomID: "r1", MessageID: "m1", Type: "c"},
	}
	jsm := &fakeJSMsg{data: encode(t, evt)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, int32(1), atomic.LoadInt32(&d.calls))
	assert.Equal(t, []string{"alice"}, d.last.Accounts)
	assert.Equal(t, int32(1), atomic.LoadInt32(&jsm.acks))
}

func TestDispatchTransientNaks(t *testing.T) {
	d := &fakeDispatcher{err: errors.New("apns timeout")}
	h := newHandler(d)

	evt := &model.PushNotificationEvent{ID: "m1-b0", RoomID: "r1", Accounts: []string{"alice"}}
	jsm := &fakeJSMsg{data: encode(t, evt)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, int32(0), atomic.LoadInt32(&jsm.acks))
	assert.Equal(t, int32(1), atomic.LoadInt32(&jsm.naks))
}

func TestDispatchPermanentAcks(t *testing.T) {
	d := &fakeDispatcher{err: errors.New("device unregistered"), perm: true}
	h := newHandler(d)

	evt := &model.PushNotificationEvent{ID: "m1-b0", RoomID: "r1", Accounts: []string{"alice"}}
	jsm := &fakeJSMsg{data: encode(t, evt)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, int32(1), atomic.LoadInt32(&jsm.acks), "permanent errors ack-drop rather than retrying")
}

func TestMalformedJSONAcks(t *testing.T) {
	h := newHandler(&fakeDispatcher{})
	jsm := &fakeJSMsg{data: []byte(`{not json`)}
	h.HandleJetStreamMsg(context.Background(), jsm)
	assert.Equal(t, int32(1), atomic.LoadInt32(&jsm.acks))
}

func TestLogDispatcher_ReturnsNil(t *testing.T) {
	evt := &model.PushNotificationEvent{
		ID:       "m1-b0",
		RoomID:   "r1",
		Accounts: []string{"alice"},
		Data:     model.PushNotificationData{RoomID: "r1", MessageID: "m1", Type: "c"},
	}
	err := LogDispatcher{}.Dispatch(context.Background(), evt)
	assert.NoError(t, err)
}
