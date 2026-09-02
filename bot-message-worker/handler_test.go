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
	"github.com/hmchangw/chat/pkg/natsutil"
)

type fakeStore struct {
	saveCalls      int32
	threadCalls    int32
	lastSaved      *model.Message
	lastThread     *model.Message
	lastThreadRoom string
	lastCtx        context.Context // captured so tests can assert on request-id propagation
	err            error
	permanentErr   bool
}

func (f *fakeStore) SaveMessage(ctx context.Context, m *model.Message, _ string) error {
	atomic.AddInt32(&f.saveCalls, 1)
	f.lastCtx = ctx
	f.lastSaved = m
	if f.err != nil {
		return maybePermanent(f.err, f.permanentErr)
	}
	return nil
}

func (f *fakeStore) SaveThreadMessage(ctx context.Context, m *model.Message, _ string, threadRoomID string) error {
	atomic.AddInt32(&f.threadCalls, 1)
	f.lastCtx = ctx
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
	headers  nats.Header
	acks     int32
	naks     int32
	nakDelay time.Duration
}

func (f *fakeJSMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (f *fakeJSMsg) Data() []byte                              { return f.data }
func (f *fakeJSMsg) Headers() nats.Header                      { return f.headers }
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

// identityHeader builds the X-Bot-Identity header bot-message-handler stamps on
// every canonical publish.
func identityHeader(t *testing.T, id, account string) nats.Header {
	t.Helper()
	raw, err := json.Marshal(model.BotIdentity{ID: id, Account: account, SiteID: "site-a"})
	require.NoError(t, err)
	h := nats.Header{}
	h.Set(model.HeaderBotIdentity, string(raw))
	return h
}

// failureCount reads the per-bot failure counter for one label pair.
func failureCount(t *testing.T, account, outcome string) float64 {
	t.Helper()
	return testutil.ToFloat64(botFailureTotal.WithLabelValues(account, outcome))
}

func TestHandleJetStreamMsg_TransientFailureCountsAgainstSender(t *testing.T) {
	store := &fakeStore{err: errors.New("cassandra timeout")}
	h := newHandler(store, "site-a")

	before := failureCount(t, "payload.bot", "nak")
	msg := &model.Message{ID: "m1", RoomID: "r1", UserID: "bot-1", UserAccount: "payload.bot", CreatedAt: time.Now().UTC()}
	jsm := &fakeJSMsg{data: encode(t, msg)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, float64(1), failureCount(t, "payload.bot", "nak")-before,
		"a naked message is counted against the bot that sent it; with no header the payload is the fallback")
}

func TestHandleJetStreamMsg_PermanentFailureCountsAgainstSender(t *testing.T) {
	store := &fakeStore{err: errors.New("schema violation"), permanentErr: true}
	h := newHandler(store, "site-a")

	before := failureCount(t, "payload.bot", "permanent")
	msg := &model.Message{ID: "m1", RoomID: "r1", UserAccount: "payload.bot", CreatedAt: time.Now().UTC()}
	jsm := &fakeJSMsg{data: encode(t, msg)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, float64(1), failureCount(t, "payload.bot", "permanent")-before,
		"an ack-dropped poison message is counted against its sender")
}

func TestHandleJetStreamMsg_MalformedPayloadAttributesSenderFromHeader(t *testing.T) {
	store := &fakeStore{}
	h := newHandler(store, "site-a")

	before := failureCount(t, "header.bot", "malformed")
	jsm := &fakeJSMsg{data: []byte(`{not-json`), headers: identityHeader(t, "bot-1", "header.bot")}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, float64(1), failureCount(t, "header.bot", "malformed")-before,
		"the header is the only attribution available when the body cannot be decoded")
}

func TestHandleJetStreamMsg_HeaderIdentityWinsOverPayloadAccount(t *testing.T) {
	store := &fakeStore{err: errors.New("cassandra timeout")}
	h := newHandler(store, "site-a")

	before := failureCount(t, "header.bot", "nak")
	msg := &model.Message{ID: "m1", RoomID: "r1", UserAccount: "payload.bot", CreatedAt: time.Now().UTC()}
	jsm := &fakeJSMsg{data: encode(t, msg), headers: identityHeader(t, "bot-1", "header.bot")}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, float64(1), failureCount(t, "header.bot", "nak")-before,
		"the header is stamped from the authenticated identity, so it outranks the body")
}

func TestHandleJetStreamMsg_UnattributableFailureCountsAsUnknown(t *testing.T) {
	store := &fakeStore{}
	h := newHandler(store, "site-a")

	before := failureCount(t, "unknown", "malformed")
	jsm := &fakeJSMsg{data: []byte(`{not-json`)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, float64(1), failureCount(t, "unknown", "malformed")-before,
		"a failure with no identity anywhere is still counted, under a reserved label")
}

func TestHandleJetStreamMsg_SuccessRecordsNoFailure(t *testing.T) {
	store := &fakeStore{}
	h := newHandler(store, "site-a")

	before := failureCount(t, "payload.bot", "nak")
	msg := &model.Message{ID: "m1", RoomID: "r1", UserAccount: "payload.bot", CreatedAt: time.Now().UTC()}
	jsm := &fakeJSMsg{data: encode(t, msg)}
	h.HandleJetStreamMsg(context.Background(), jsm)

	assert.Equal(t, float64(0), failureCount(t, "payload.bot", "nak")-before,
		"a healthy bot must never appear in the failure counter")
}

func TestHandleJetStreamMsg_StampsRequestIDFromMessageHeader(t *testing.T) {
	store := &fakeStore{}
	h := newHandler(store, "site-a")

	const requestID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	hdr := identityHeader(t, "bot-1", "header.bot")
	hdr.Set(natsutil.RequestIDHeader, requestID)

	msg := &model.Message{ID: "m1", RoomID: "r1", UserAccount: "header.bot", CreatedAt: time.Now().UTC()}
	jsm := &fakeJSMsg{data: encode(t, msg), headers: hdr}
	h.HandleJetStreamMsg(context.Background(), jsm)

	require.NotNil(t, store.lastCtx)
	assert.Equal(t, requestID, natsutil.RequestIDFromContext(store.lastCtx),
		"the inbound request id must reach the handler ctx so worker logs correlate with the bot's API call")
}
