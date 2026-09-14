package main

import (
	"context"
	"encoding/json"
	"math/rand" // #nosec G404 -- deterministic load-generator test input // nosemgrep: math-random-used
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

type soakReadCall struct {
	subject string
	data    []byte
}

type soakReadTransport struct {
	mu      sync.Mutex
	replies []soakRPCFakeReply
	calls   []soakReadCall
}

func (t *soakReadTransport) Request(
	_ context.Context,
	subject string,
	data []byte,
	_ time.Duration,
) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, soakReadCall{
		subject: subject,
		data:    append([]byte(nil), data...),
	})
	if len(t.replies) == 0 {
		return nil, assert.AnError
	}
	reply := t.replies[0]
	t.replies = t.replies[1:]
	return reply.data, reply.err
}

func (t *soakReadTransport) snapshot() []soakReadCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]soakReadCall(nil), t.calls...)
}

type soakReadRecorder struct {
	mu      sync.Mutex
	samples []soakReadSample
}

func (r *soakReadRecorder) Record(sample *soakReadSample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, *sample)
}

func (r *soakReadRecorder) snapshot() []soakReadSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]soakReadSample(nil), r.samples...)
}

func newTestSoakReader(
	transport soakRPCTransport,
	recorder soakReadSampleRecorder,
	catalog *soakCatalog,
) *soakReader {
	topology := soakTopology{Subscriptions: []model.Subscription{{
		RoomID: "room-1", IsSubscribed: true,
		User: model.SubscriptionUser{ID: "u-1", Account: "alice"},
	}}}
	return newSoakReader(soakReadConfig{
		SiteID: "site-1", PageLimit: 2, MaxPages: 10,
		RequestTimeout: time.Second,
	}, &topology, catalog, newSoakRPCClient(
		transport,
		soakRetryConfig{MaxAttempts: 1},
		&soakRecordingSleeper{},
		nil,
	), recorder, rand.New(rand.NewSource(1)), steppedSoakNow())
}

func steppedSoakNow() func() time.Time {
	current := time.Unix(100, 0)
	return func() time.Time {
		value := current
		current = current.Add(10 * time.Millisecond)
		return value
	}
}

func emptySoakReadCatalog() *soakCatalog {
	return newSoakCatalog(8, 100, 0, newFakeSoakClock(time.Unix(100, 0)))
}

func soakPinnedReply(
	t *testing.T,
	messageIDs []string,
	nextCursor string,
	hasNext bool,
) []byte {
	t.Helper()
	data, err := json.Marshal(soakListPinnedMessagesResponse{
		Messages:   soakReadWireMessages(messageIDs),
		NextCursor: nextCursor,
		HasNext:    hasNext,
	})
	require.NoError(t, err)
	return data
}

func soakReadWireMessages(messageIDs []string) []soakWireMessage {
	messages := make([]soakWireMessage, len(messageIDs))
	for index, messageID := range messageIDs {
		messages[index] = soakWireMessage{
			RoomID: "room-1", MessageID: messageID,
			CreatedAt: time.UnixMilli(int64(index + 1)),
		}
	}
	return messages
}

func acceptedSoakReadThread(
	t *testing.T,
	clock *fakeSoakClock,
	roomID string,
	messageID string,
) *soakCatalog {
	t.Helper()
	catalog := newSoakCatalog(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: messageID, RoomID: roomID, Author: "alice", Content: "hello",
		CreatedAt: clock.Now(), ThreadReplyLimit: 10,
	}))
	require.True(t, catalog.Accept(roomID, messageID))
	require.True(t, catalog.ReserveThreadReply(roomID, messageID))
	require.True(t, catalog.ConfirmThreadReply(roomID, messageID))
	return catalog
}
