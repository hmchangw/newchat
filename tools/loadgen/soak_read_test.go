package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestSoakReadPicker_UsesSeventyFiveFifteenTenMix(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	counts := map[soakReadKind]int{}
	const samples = 100000
	for range samples {
		counts[pickSoakReadKind(rng)]++
	}

	assert.InDelta(t, 0.75, float64(counts[soakReadHistory])/samples, 0.005)
	assert.InDelta(t, 0.15, float64(counts[soakReadThread])/samples, 0.005)
	assert.InDelta(t, 0.10, float64(counts[soakReadMessage])/samples, 0.005)
}

func TestSoakReader_LoadHistoryPaginatesWithStrictOldestBoundary(t *testing.T) {
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: soakHistoryReply(t, 300, 200)},
		{data: soakHistoryReply(t, 100, 50)},
		{data: soakHistoryReply(t)},
	}}
	recorder := &soakReadRecorder{}
	reader := newTestSoakReader(transport, recorder, emptySoakReadCatalog())

	outcome, err := reader.LoadHistory(context.Background(), "room-1")
	require.NoError(t, err)
	assert.Equal(t, 3, outcome.Pages)
	assert.Equal(t, 4, outcome.Messages)
	assert.False(t, outcome.Skipped)

	calls := transport.snapshot()
	require.Len(t, calls, 3)
	assert.Equal(t, subject.MsgHistory("alice", "room-1", "site-1"), calls[0].subject)
	var requests []soakLoadHistoryRequest
	for _, call := range calls {
		var request soakLoadHistoryRequest
		require.NoError(t, json.Unmarshal(call.data, &request))
		requests = append(requests, request)
	}
	assert.Nil(t, requests[0].Before)
	require.NotNil(t, requests[0].Meta)
	require.NotNil(t, requests[0].Meta.LastMsgAt)
	require.NotNil(t, requests[1].Before)
	assert.Equal(t, int64(199), *requests[1].Before)
	require.NotNil(t, requests[1].Meta)
	assert.Equal(t, requests[0].Meta.LastMsgAt, requests[1].Meta.LastMsgAt)
	require.NotNil(t, requests[2].Before)
	assert.Equal(t, int64(49), *requests[2].Before)

	samples := recorder.snapshot()
	require.Len(t, samples, 3)
	for _, sample := range samples {
		assert.Equal(t, soakRPCLoadHistory, sample.Action)
		assert.Empty(t, sample.ErrorClass)
		assert.Equal(t, 10*time.Millisecond, sample.Latency)
	}
	assert.Zero(t, samples[2].Messages)
}

func TestSoakReader_LoadHistoryStopsOnNonProgressingPage(t *testing.T) {
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: soakHistoryReply(t, 300, 200)},
		{data: soakHistoryReply(t, 250, 200)},
	}}
	recorder := &soakReadRecorder{}
	reader := newTestSoakReader(transport, recorder, emptySoakReadCatalog())

	_, err := reader.LoadHistory(context.Background(), "room-1")
	require.ErrorContains(t, err, "progress")
	assert.Len(t, transport.snapshot(), 2)
	samples := recorder.snapshot()
	require.Len(t, samples, 2)
	assert.Equal(t, soakErrorAssertion, samples[1].ErrorClass)
}

func TestSoakReader_ThreadCursorPagination(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedSoakReadThread(t, clock, "room-1", "parent")
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: soakThreadReply(t, []string{"reply-1"}, "cursor-2", true)},
		{data: soakThreadReply(t, []string{"reply-2"}, "", false)},
	}}
	recorder := &soakReadRecorder{}
	reader := newTestSoakReader(transport, recorder, catalog)

	outcome, err := reader.GetThreadMessages(context.Background(), "room-1")
	require.NoError(t, err)
	assert.Equal(t, 2, outcome.Pages)
	assert.Equal(t, 2, outcome.Messages)

	calls := transport.snapshot()
	require.Len(t, calls, 2)
	assert.Equal(t, subject.MsgThread("alice", "room-1", "site-1"), calls[0].subject)
	var first, second soakGetThreadMessagesRequest
	require.NoError(t, json.Unmarshal(calls[0].data, &first))
	require.NoError(t, json.Unmarshal(calls[1].data, &second))
	assert.Equal(t, "parent", first.ThreadMessageID)
	assert.Empty(t, first.Cursor)
	assert.Equal(t, "cursor-2", second.Cursor)
	assert.Len(t, recorder.snapshot(), 2)
}

func TestSoakReader_PinnedListCursorPaginationAndRepeatGuard(t *testing.T) {
	tests := []struct {
		name      string
		replies   []soakRPCFakeReply
		wantPages int
		wantErr   string
	}{
		{
			name: "advances cursor",
			replies: []soakRPCFakeReply{
				{data: soakPinnedReply(t, []string{"m-1"}, "cursor-2", true)},
				{data: soakPinnedReply(t, []string{"m-2"}, "", false)},
			},
			wantPages: 2,
		},
		{
			name: "rejects repeated cursor",
			replies: []soakRPCFakeReply{
				{data: soakPinnedReply(t, nil, "same", true)},
				{data: soakPinnedReply(t, nil, "same", true)},
			},
			wantPages: 2,
			wantErr:   "cursor",
		},
		{
			name: "rejects empty next cursor",
			replies: []soakRPCFakeReply{
				{data: soakPinnedReply(t, nil, "", true)},
			},
			wantPages: 1,
			wantErr:   "cursor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &soakReadTransport{replies: tt.replies}
			recorder := &soakReadRecorder{}
			reader := newTestSoakReader(transport, recorder, emptySoakReadCatalog())

			outcome, err := reader.ListPinnedMessages(context.Background(), "room-1")
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
				samples := recorder.snapshot()
				assert.Equal(t, soakErrorAssertion, samples[len(samples)-1].ErrorClass)
			}
			assert.Equal(t, tt.wantPages, outcome.Pages)
			for _, sample := range recorder.snapshot() {
				assert.Equal(t, soakRPCPinnedList, sample.Action)
			}
		})
	}
}

func TestSoakReader_PinnedListRehydratesCatalog(t *testing.T) {
	pinnedAt := time.Unix(101, 0)
	message := soakWireMessage{
		RoomID: "room-1", MessageID: "pinned-1",
		Sender: cassandra.Participant{Account: "alice"},
		Msg:    "owned by the soak room", CreatedAt: time.Unix(100, 0),
		PinnedAt: &pinnedAt,
	}
	data, err := json.Marshal(soakListPinnedMessagesResponse{
		Messages: []soakWireMessage{message},
	})
	require.NoError(t, err)
	transport := &soakReadTransport{
		replies: []soakRPCFakeReply{{data: data}},
	}
	catalog := emptySoakReadCatalog()
	reader := newTestSoakReader(transport, &soakReadRecorder{}, catalog)

	_, err = reader.ListPinnedMessages(context.Background(), "room-1")
	require.NoError(t, err)
	got, ok := catalog.Get("room-1", "pinned-1")
	require.True(t, ok)
	assert.True(t, got.Pinned)
	assert.Equal(t, "alice", got.Author)
}

func TestSoakReader_EmptyCatalogIsWarmupSkip(t *testing.T) {
	transport := &soakReadTransport{}
	recorder := &soakReadRecorder{}
	reader := newTestSoakReader(transport, recorder, emptySoakReadCatalog())

	thread, err := reader.GetThreadMessages(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, thread.Skipped)
	message, err := reader.GetMessageByID(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, message.Skipped)

	assert.Empty(t, transport.snapshot())
	samples := recorder.snapshot()
	require.Len(t, samples, 2)
	assert.True(t, samples[0].Skipped)
	assert.True(t, samples[1].Skipped)
	assert.Empty(t, samples[0].ErrorClass)
	assert.Empty(t, samples[1].ErrorClass)
}

func TestSoakReader_GetMessageDecodesPayloadAndRecordsEndpointLatency(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedSoakReadMessage(t, clock, "room-1", "message-1", "")
	response, err := json.Marshal(soakWireMessage{
		RoomID: "room-1", MessageID: "message-1", Msg: "hello",
		CreatedAt: time.UnixMilli(100),
	})
	require.NoError(t, err)
	transport := &soakReadTransport{replies: []soakRPCFakeReply{{data: response}}}
	recorder := &soakReadRecorder{}
	reader := newTestSoakReader(transport, recorder, catalog)

	outcome, err := reader.GetMessageByID(context.Background(), "room-1")
	require.NoError(t, err)
	assert.Equal(t, 1, outcome.Messages)
	assert.Equal(t, "message-1", outcome.MessageID)

	calls := transport.snapshot()
	require.Len(t, calls, 1)
	assert.Equal(t, subject.MsgGet("alice", "room-1", "site-1"), calls[0].subject)
	var request soakGetMessageByIDRequest
	require.NoError(t, json.Unmarshal(calls[0].data, &request))
	assert.Equal(t, "message-1", request.MessageID)

	samples := recorder.snapshot()
	require.Len(t, samples, 1)
	assert.Equal(t, soakRPCGetMessage, samples[0].Action)
	assert.Equal(t, 10*time.Millisecond, samples[0].Latency)
	assert.Equal(t, 1, samples[0].Messages)
}

func TestSoakReader_NormalEmptyResponseIsNotAnError(t *testing.T) {
	transport := &soakReadTransport{replies: []soakRPCFakeReply{{
		data: soakHistoryReply(t),
	}}}
	recorder := &soakReadRecorder{}
	reader := newTestSoakReader(transport, recorder, emptySoakReadCatalog())

	outcome, err := reader.LoadHistory(context.Background(), "room-1")
	require.NoError(t, err)
	assert.Zero(t, outcome.Messages)
	samples := recorder.snapshot()
	require.Len(t, samples, 1)
	assert.Empty(t, samples[0].ErrorClass)
}

func TestSoakReader_SelectsOnlySubscribedAccountForRoom(t *testing.T) {
	topology := soakTopology{
		Subscriptions: []model.Subscription{
			{RoomID: "room-1", IsSubscribed: false, User: model.SubscriptionUser{Account: "inactive"}},
			{RoomID: "room-2", IsSubscribed: true, User: model.SubscriptionUser{Account: "other"}},
			{RoomID: "room-1", IsSubscribed: true, User: model.SubscriptionUser{Account: "alice"}},
		},
	}
	transport := &soakReadTransport{replies: []soakRPCFakeReply{{
		data: soakHistoryReply(t),
	}}}
	reader := newSoakReader(soakReadConfig{
		SiteID: "site-1", PageLimit: 2, MaxPages: 10, RequestTimeout: time.Second,
	}, &topology, emptySoakReadCatalog(), newSoakRPCClient(
		transport,
		soakRetryConfig{MaxAttempts: 1},
		&soakRecordingSleeper{},
		nil,
	), &soakReadRecorder{}, rand.New(rand.NewSource(1)), steppedSoakNow())

	_, err := reader.LoadHistory(context.Background(), "room-1")
	require.NoError(t, err)
	calls := transport.snapshot()
	require.Len(t, calls, 1)
	assert.Equal(t, subject.MsgHistory("alice", "room-1", "site-1"), calls[0].subject)
}

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
		SiteID: "site-1", PageLimit: 2, MaxPages: 10, RequestTimeout: time.Second,
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

func acceptedSoakReadMessage(
	t *testing.T,
	clock *fakeSoakClock,
	roomID string,
	messageID string,
	threadParentID string,
) *soakCatalog {
	t.Helper()
	catalog := newSoakCatalog(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: messageID, RoomID: roomID, Author: "alice", Content: "hello",
		CreatedAt: clock.Now(), ThreadParentID: threadParentID,
		ThreadReplyLimit: 10,
	}))
	require.True(t, catalog.Accept(roomID, messageID))
	return catalog
}

// acceptedSoakReadThread returns a catalog whose message already carries a
// reply, so it is eligible for soakCatalogThreadRead. A zero-reply parent has
// no thread room and the reader skips it by design.
func acceptedSoakReadThread(
	t *testing.T,
	clock *fakeSoakClock,
	roomID string,
	messageID string,
) *soakCatalog {
	t.Helper()
	catalog := acceptedSoakReadMessage(t, clock, roomID, messageID, "")
	require.True(t, catalog.ReserveThreadReply(roomID, messageID))
	require.True(t, catalog.ConfirmThreadReply(roomID, messageID))
	return catalog
}

func soakHistoryReply(t *testing.T, timestamps ...int64) []byte {
	t.Helper()
	messages := make([]soakWireMessage, 0, len(timestamps))
	for index, timestamp := range timestamps {
		messages = append(messages, soakWireMessage{
			RoomID: "room-1", MessageID: soakReadMessageID(index),
			CreatedAt: time.UnixMilli(timestamp),
		})
	}
	data, err := json.Marshal(soakLoadHistoryResponse{Messages: messages})
	require.NoError(t, err)
	return data
}

func soakThreadReply(
	t *testing.T,
	messageIDs []string,
	nextCursor string,
	hasNext bool,
) []byte {
	t.Helper()
	messages := soakCassandraMessages(messageIDs)
	data, err := json.Marshal(soakGetThreadMessagesResponse{
		Messages: messages, NextCursor: nextCursor, HasNext: hasNext,
	})
	require.NoError(t, err)
	return data
}

func soakPinnedReply(
	t *testing.T,
	messageIDs []string,
	nextCursor string,
	hasNext bool,
) []byte {
	t.Helper()
	data, err := json.Marshal(soakListPinnedMessagesResponse{
		Messages:   soakCassandraMessages(messageIDs),
		NextCursor: nextCursor, HasNext: hasNext,
	})
	require.NoError(t, err)
	return data
}

func soakCassandraMessages(messageIDs []string) []soakWireMessage {
	messages := make([]soakWireMessage, len(messageIDs))
	for index, messageID := range messageIDs {
		messages[index] = soakWireMessage{
			RoomID: "room-1", MessageID: messageID,
			CreatedAt: time.UnixMilli(int64(index + 1)),
		}
	}
	return messages
}

func soakReadMessageID(index int) string {
	return string(rune('A'+index)) + "AAAAAAAAAAAAAAAAAAA"
}
