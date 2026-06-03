package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

type recordedPublish struct {
	subject string
	msgID   string
	headers nats.Header
	payload []byte
}

type fakePublisher struct {
	mu       sync.Mutex
	records  []recordedPublish
	failNext error
}

func (f *fakePublisher) PublishMsg(_ context.Context, msg *nats.Msg) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return err
	}
	hdrCopy := nats.Header{}
	for k, v := range msg.Header {
		hdrCopy[k] = append([]string(nil), v...)
	}
	f.records = append(f.records, recordedPublish{
		subject: msg.Subject,
		msgID:   msg.Header.Get("Nats-Msg-Id"),
		headers: hdrCopy,
		payload: append([]byte(nil), msg.Data...),
	})
	return nil
}

func TestMobileEmitter_PublishesGzippedBatch(t *testing.T) {
	pub := &fakePublisher{}
	em := newMobileEmitter(pub, "site-a", 0)
	evt := model.PushNotificationEvent{
		ID:       "m1-b0",
		Accounts: []string{"alice", "bob"},
		RoomID:   "r1",
		Body:     "hello",
	}
	require.NoError(t, em.Emit(context.Background(), evt))

	require.Len(t, pub.records, 1)
	r := pub.records[0]
	assert.Equal(t, "chat.server.notification.push.site-a.send", r.subject)
	assert.Equal(t, "m1-b0", r.msgID, "Nats-Msg-Id is the batch dedup key")
	assert.Equal(t, "gzip", r.headers.Get("Content-Encoding"))
	assert.Equal(t, "application/json", r.headers.Get("Content-Type"))

	// Payload must round-trip via the shared natsutil decoder so any consumer can use it.
	decoded, err := natsutil.DecodePayload(&nats.Msg{Data: r.payload, Header: r.headers})
	require.NoError(t, err)
	var got model.PushNotificationEvent
	require.NoError(t, json.Unmarshal(decoded, &got))
	assert.Equal(t, evt, got)
}

func TestMobileEmitter_PropagatesError(t *testing.T) {
	pub := &fakePublisher{failNext: errors.New("nats: full")}
	em := newMobileEmitter(pub, "site-a", 0)
	err := em.Emit(context.Background(), model.PushNotificationEvent{ID: "m1-b0", Accounts: []string{"bob"}})
	assert.Error(t, err)
}

func TestMobileEmitter_RejectsOversizedBatch(t *testing.T) {
	pub := &fakePublisher{}
	em := newMobileEmitter(pub, "site-a", 64) // absurdly low cap to force rejection
	err := em.Emit(context.Background(), model.PushNotificationEvent{
		ID:       "m1-b0",
		Accounts: []string{"alice", "bob", "carol", "dave"},
		Body:     "this body plus accounts and headers will gzip larger than 64 bytes",
		RoomID:   "r1",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds NATS max_payload")
	assert.Empty(t, pub.records, "oversized batch must not reach the publisher")
}
