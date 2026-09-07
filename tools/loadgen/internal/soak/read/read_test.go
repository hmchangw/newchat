package read

import (
	"context"
	"encoding/json"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
	soakcatalog "github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/wire"
)

func TestSoakReadPicker_UsesSeventyFiveFifteenTenMix(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	counts := map[Kind]int{}
	const samples = 100000
	for range samples {
		counts[PickKind(rng)]++
	}

	assert.InDelta(t, 0.75, float64(counts[KindHistory])/samples, 0.005)
	assert.InDelta(t, 0.15, float64(counts[KindThread])/samples, 0.005)
	assert.InDelta(t, 0.10, float64(counts[KindMessage])/samples, 0.005)
}

func TestSoakReaderAndVerifier_ApplyDefaults(t *testing.T) {
	messageCatalog := soakcatalog.New(8, 100, 0, nil)
	reader := NewReader(Config{}, nil, messageCatalog, nil, nil, nil, nil)
	assert.Equal(t, 50, reader.cfg.PageLimit)
	assert.Equal(t, 100, reader.cfg.MaxPages)
	assert.Equal(t, 5*time.Second, reader.cfg.RequestTimeout)
	assert.NotNil(t, reader.rng)
	assert.NotNil(t, reader.now)

	verifier := NewVerifier(nil, messageCatalog, nil, nil, nil)
	assert.Equal(t, 50, verifier.cfg.PageLimit)
	assert.Equal(t, 20, verifier.cfg.MaxPages)
	assert.Equal(t, 5*time.Second, verifier.cfg.RequestTimeout)
	assert.NotNil(t, verifier.now)
}

func TestSoakSample_CountRowsMarksARealRowCount(t *testing.T) {
	sample := Sample{}
	sample.CountRows(7)
	assert.Equal(t, 7, sample.Messages)
	assert.True(t, sample.RowsCounted)
}

func TestSoakReader_RecordsRPCFailuresByEndpoint(t *testing.T) {
	tests := []struct {
		name   string
		invoke func(*Reader) (Outcome, error)
	}{
		{
			name: "load history",
			invoke: func(reader *Reader) (Outcome, error) {
				return reader.LoadHistory(context.Background(), "room-1")
			},
		},
		{
			name: "get thread",
			invoke: func(reader *Reader) (Outcome, error) {
				return reader.GetThreadMessages(context.Background(), "room-1")
			},
		},
		{
			name: "get message",
			invoke: func(reader *Reader) (Outcome, error) {
				return reader.GetMessageByID(context.Background(), "room-1")
			},
		},
		{
			name: "pinned list",
			invoke: func(reader *Reader) (Outcome, error) {
				return reader.ListPinnedMessages(context.Background(), "room-1")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeSoakClock(time.Unix(100, 0))
			messageCatalog := acceptedSoakReadThread(
				t,
				clock,
				"room-1",
				"AAAAAAAAAAAAAAAAAAAA",
			)
			recorder := &soakReadRecorder{}
			reader := newTestSoakReader(
				&soakReadTransport{replies: []soakRPCFakeReply{{
					data: []byte(`{"error":"denied","code":"forbidden"}`),
				}}},
				recorder,
				messageCatalog,
			)

			_, err := tt.invoke(reader)
			require.Error(t, err)
			require.Len(t, recorder.snapshot(), 1)
			assert.Equal(t, rpc.ErrorForbidden, recorder.snapshot()[0].ErrorClass)
		})
	}
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
	var requests []wire.LoadHistoryRequest
	for _, call := range calls {
		var request wire.LoadHistoryRequest
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
		assert.Equal(t, rpc.ActionLoadHistory, sample.Action)
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
	assert.Equal(t, rpc.ErrorAssertion, samples[1].ErrorClass)
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
	var first, second wire.GetThreadMessagesRequest
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
				assert.Equal(t, rpc.ErrorAssertion, samples[len(samples)-1].ErrorClass)
			}
			assert.Equal(t, tt.wantPages, outcome.Pages)
			for _, sample := range recorder.snapshot() {
				assert.Equal(t, rpc.ActionPinnedList, sample.Action)
			}
		})
	}
}

func TestSoakReader_PinnedListRehydratesCatalog(t *testing.T) {
	pinnedAt := time.Unix(101, 0)
	message := wire.Message{
		RoomID: "room-1", MessageID: "pinned-1",
		Sender: cassandra.Participant{Account: "alice"},
		Msg:    "owned by the soak room", CreatedAt: time.Unix(100, 0),
		PinnedAt: &pinnedAt,
	}
	data, err := json.Marshal(wire.ListPinnedMessagesResponse{
		Messages: []wire.Message{message},
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
	response, err := json.Marshal(wire.Message{
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
	var request wire.GetMessageByIDRequest
	require.NoError(t, json.Unmarshal(calls[0].data, &request))
	assert.Equal(t, "message-1", request.MessageID)

	samples := recorder.snapshot()
	require.Len(t, samples, 1)
	assert.Equal(t, rpc.ActionGetMessage, samples[0].Action)
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

func TestSoakReader_SelectsOnlyActiveAccountForRoom(t *testing.T) {
	topology := topology.Topology{
		ActiveUsers: []model.User{{ID: "u-alice", Account: "alice"}},
		Subscriptions: []model.Subscription{
			{RoomID: "room-1", User: model.SubscriptionUser{ID: "u-inactive", Account: "inactive"}},
			{RoomID: "room-2", User: model.SubscriptionUser{ID: "u-other", Account: "other"}},
			{RoomID: "room-1", User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}},
		},
	}
	transport := &soakReadTransport{replies: []soakRPCFakeReply{{
		data: soakHistoryReply(t),
	}}}
	reader := NewReader(Config{
		SiteID: "site-1", PageLimit: 2, MaxPages: 10, RequestTimeout: time.Second,
	}, &topology, emptySoakReadCatalog(), rpc.NewClient(
		transport,
		rpc.RetryConfig{MaxAttempts: 1},
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
	samples []Sample
}

func (r *soakReadRecorder) Record(sample *Sample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, *sample)
}

func (r *soakReadRecorder) snapshot() []Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Sample(nil), r.samples...)
}

func newTestSoakReader(
	transport rpc.Transport,
	recorder SampleRecorder,
	catalog *soakcatalog.Catalog,
) *Reader {
	topology := topology.Topology{Subscriptions: []model.Subscription{{
		RoomID: "room-1", IsSubscribed: true,
		User: model.SubscriptionUser{ID: "u-1", Account: "alice"},
	}}}
	return NewReader(Config{
		SiteID: "site-1", PageLimit: 2, MaxPages: 10, RequestTimeout: time.Second,
	}, &topology, catalog, rpc.NewClient(
		transport,
		rpc.RetryConfig{MaxAttempts: 1},
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

func emptySoakReadCatalog() *soakcatalog.Catalog {
	return soakcatalog.New(8, 100, 0, newFakeSoakClock(time.Unix(100, 0)))
}

func acceptedSoakReadMessage(
	t *testing.T,
	clock *fakeSoakClock,
	roomID string,
	messageID string,
	threadParentID string,
) *soakcatalog.Catalog {
	t.Helper()
	catalog := soakcatalog.New(8, 100, 0, clock)
	require.NoError(t, catalog.TrackPublished(&soakcatalog.Candidate{
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
) *soakcatalog.Catalog {
	t.Helper()
	catalog := acceptedSoakReadMessage(t, clock, roomID, messageID, "")
	require.True(t, catalog.ReserveThreadReply(roomID, messageID))
	require.True(t, catalog.ConfirmThreadReply(roomID, messageID))
	return catalog
}

func soakHistoryReply(t *testing.T, timestamps ...int64) []byte {
	t.Helper()
	messages := make([]wire.Message, 0, len(timestamps))
	for index, timestamp := range timestamps {
		messages = append(messages, wire.Message{
			RoomID: "room-1", MessageID: soakReadMessageID(index),
			CreatedAt: time.UnixMilli(timestamp),
		})
	}
	data, err := json.Marshal(wire.LoadHistoryResponse{Messages: messages})
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
	data, err := json.Marshal(wire.GetThreadMessagesResponse{
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
	data, err := json.Marshal(wire.ListPinnedMessagesResponse{
		Messages:   soakCassandraMessages(messageIDs),
		NextCursor: nextCursor, HasNext: hasNext,
	})
	require.NoError(t, err)
	return data
}

func soakCassandraMessages(messageIDs []string) []wire.Message {
	messages := make([]wire.Message, len(messageIDs))
	for index, messageID := range messageIDs {
		messages[index] = wire.Message{
			RoomID: "room-1", MessageID: messageID,
			CreatedAt: time.UnixMilli(int64(index + 1)),
		}
	}
	return messages
}

func soakReadMessageID(index int) string {
	return string(rune('A'+index)) + "AAAAAAAAAAAAAAAAAAA"
}
