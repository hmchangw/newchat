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
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

type fakeStore struct {
	saveCalls      int32
	threadCalls    int32
	lastSaved      *model.Message
	lastThread     *model.Message
	lastThreadRoom string
	err            error
	permanentErr   bool
}

func (f *fakeStore) SaveMessage(_ context.Context, m *model.Message, _ string) error {
	atomic.AddInt32(&f.saveCalls, 1)
	f.lastSaved = m
	if f.err != nil {
		return maybePermanent(f.err, f.permanentErr)
	}
	return nil
}

func (f *fakeStore) SaveThreadMessage(_ context.Context, m *model.Message, _ string, threadRoomID string) error {
	atomic.AddInt32(&f.threadCalls, 1)
	f.lastThread = m
	f.lastThreadRoom = threadRoomID
	if f.err != nil {
		return maybePermanent(f.err, f.permanentErr)
	}
	return nil
}

func maybePermanent(err error, permanent bool) error {
	if permanent {
		return errcode.Permanent(errcode.Internal("schema violation", errcode.WithCause(err)))
	}
	return err
}

type fakeJSMsg struct {
	subject  string
	data     []byte
	acks     int32
	naks     int32
	nakDelay time.Duration
}

func (f *fakeJSMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (f *fakeJSMsg) Data() []byte                              { return f.data }
func (f *fakeJSMsg) Headers() nats.Header                      { return nil }
func (f *fakeJSMsg) Subject() string                           { return f.subject }
func (f *fakeJSMsg) Reply() string                             { return "" }
func (f *fakeJSMsg) Ack() error                                { atomic.AddInt32(&f.acks, 1); return nil }
func (f *fakeJSMsg) DoubleAck(_ context.Context) error         { return nil }
func (f *fakeJSMsg) Nak() error                                { atomic.AddInt32(&f.naks, 1); return nil }
func (f *fakeJSMsg) NakWithDelay(d time.Duration) error {
	atomic.AddInt32(&f.naks, 1)
	f.nakDelay = d
	return nil
}
func (f *fakeJSMsg) InProgress() error { return nil }
func (f *fakeJSMsg) Term() error       { return nil }
func (f *fakeJSMsg) TermWithReason(_ string) error {
	return nil
}

func encode(t *testing.T, m *model.Message) []byte {
	t.Helper()
	evt := model.MessageEvent{
		Event: model.EventCreated, Message: *m, SiteID: "site-a",
		Timestamp: m.CreatedAt.UnixMilli(),
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	return data
}

func TestHandleJetStreamMsg_MainRoomSuccess(t *testing.T) {
	store := &fakeStore{}
	h := newHandler(store, "site-a")

	msg := &model.Message{
		ID: "m1", RoomID: "r1", UserID: "bot-1", UserAccount: "myapp.bot",
		Content: "hi", CreatedAt: time.Now().UTC(),
	}
	jsm := &fakeJSMsg{subject: "chat.bot.canonical.site-a.created", data: encode(t, msg)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, int32(1), atomic.LoadInt32(&store.saveCalls), "SaveMessage called once")
	assert.Equal(t, int32(0), atomic.LoadInt32(&store.threadCalls))
	assert.Equal(t, int32(1), atomic.LoadInt32(&jsm.acks), "success acks the message")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jsm.naks))
	assert.Equal(t, "m1", store.lastSaved.ID)
}

func TestHandleJetStreamMsg_ThreadReplyRouted(t *testing.T) {
	store := &fakeStore{}
	h := newHandler(store, "site-a")

	msg := &model.Message{
		ID: "reply-1", RoomID: "r1", UserID: "bot-1",
		Content: "reply", CreatedAt: time.Now().UTC(),
		ThreadParentMessageID: "parent-msg",
	}
	jsm := &fakeJSMsg{data: encode(t, msg)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, int32(0), atomic.LoadInt32(&store.saveCalls))
	assert.Equal(t, int32(1), atomic.LoadInt32(&store.threadCalls), "SaveThreadMessage called once")
	assert.Equal(t, "r1", store.lastThreadRoom, "threadRoomID is the parent roomID for bot messages")
	assert.Equal(t, int32(1), atomic.LoadInt32(&jsm.acks))
}

func TestHandleJetStreamMsg_MalformedJSONAcks(t *testing.T) {
	store := &fakeStore{}
	h := newHandler(store, "site-a")

	jsm := &fakeJSMsg{data: []byte(`{not-json`)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, int32(0), atomic.LoadInt32(&store.saveCalls), "no write on malformed input")
	assert.Equal(t, int32(1), atomic.LoadInt32(&jsm.acks), "malformed JSON is permanent → ack-drop")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jsm.naks))
}

func TestHandleJetStreamMsg_TransientErrorNaks(t *testing.T) {
	store := &fakeStore{err: errors.New("cassandra timeout")}
	h := newHandler(store, "site-a")

	msg := &model.Message{ID: "m1", RoomID: "r1", CreatedAt: time.Now().UTC()}
	jsm := &fakeJSMsg{data: encode(t, msg)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, int32(0), atomic.LoadInt32(&jsm.acks), "transient must NOT ack")
	assert.Equal(t, int32(1), atomic.LoadInt32(&jsm.naks), "transient naks so JS backoff redelivers")
}

func TestHandleJetStreamMsg_PermanentErrorAcks(t *testing.T) {
	store := &fakeStore{err: errors.New("schema violation"), permanentErr: true}
	h := newHandler(store, "site-a")

	// Snapshot delta so subtests that share the counter don't cross-contaminate.
	before := testutil.ToFloat64(permanentErrorTotal)

	msg := &model.Message{ID: "m1", RoomID: "r1", CreatedAt: time.Now().UTC()}
	jsm := &fakeJSMsg{data: encode(t, msg)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, int32(1), atomic.LoadInt32(&jsm.acks), "permanent → ack-drop")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jsm.naks), "permanent must NOT nak")

	after := testutil.ToFloat64(permanentErrorTotal)
	assert.Equal(t, float64(1), after-before, "poison metric must bump exactly once")
}
