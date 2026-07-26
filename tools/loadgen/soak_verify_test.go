package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestSoakVerifier_GetMessageChecksPresenceRoomAuthorAndContent(t *testing.T) {
	tests := []struct {
		name      string
		response  cassandra.Message
		wantClass soakVerifyClass
		wantField string
	}{
		{
			name: "matches",
			response: verifiedCassandraMessage(
				"room-1", "message-1", "alice", "original",
			),
			wantClass: soakVerifyOK,
		},
		{
			name: "wrong room",
			response: verifiedCassandraMessage(
				"room-2", "message-1", "alice", "original",
			),
			wantClass: soakVerifyMismatch,
			wantField: "room_id",
		},
		{
			name: "wrong author",
			response: verifiedCassandraMessage(
				"room-1", "message-1", "bob", "original",
			),
			wantClass: soakVerifyMismatch,
			wantField: "author",
		},
		{
			name: "wrong content",
			response: verifiedCassandraMessage(
				"room-1", "message-1", "alice", "different secret body",
			),
			wantClass: soakVerifyMismatch,
			wantField: "content",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeSoakClock(time.Unix(100, 0))
			catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
			data, err := json.Marshal(tt.response)
			require.NoError(t, err)
			transport := &soakReadTransport{replies: []soakRPCFakeReply{{data: data}}}
			recorder := &soakVerifyRecorder{}
			verifier := newTestSoakVerifier(catalog, transport, recorder)

			result := verifier.VerifyByID(context.Background(), "room-1", "message-1")
			assert.Equal(t, tt.wantClass, result.Class)
			assert.Equal(t, tt.wantField, result.Field)
			assert.Equal(t, "room-1", result.RoomID)
			assert.Equal(t, "message-1", result.MessageID)
			assert.NotContains(t, result.String(), "different secret body")

			calls := transport.snapshot()
			require.Len(t, calls, 1)
			assert.Equal(t, subject.MsgGet("alice", "room-1", "site-1"), calls[0].subject)
			assert.Equal(t, []soakVerifyResult{result}, recorder.snapshot())
		})
	}
}

func TestSoakVerifier_VerifiesEditedContent(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	require.True(t, catalog.MarkEdited("room-1", "message-1", "updated"))
	response := verifiedCassandraMessage("room-1", "message-1", "alice", "updated")
	editedAt := time.Unix(101, 0)
	response.EditedAt = &editedAt
	data, err := json.Marshal(response)
	require.NoError(t, err)
	verifier := newTestSoakVerifier(catalog, &soakReadTransport{
		replies: []soakRPCFakeReply{{data: data}},
	}, &soakVerifyRecorder{})

	result := verifier.VerifyByID(context.Background(), "room-1", "message-1")
	assert.Equal(t, soakVerifyOK, result.Class)
	assert.Equal(t, soakRPCEdit, result.ExpectedAction)
}

func TestSoakVerifier_VerifiesSoftDeletedStateWithoutComparingBody(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	require.True(t, catalog.MarkDeleted("room-1", "message-1"))
	response := verifiedCassandraMessage("room-1", "message-1", "alice", "")
	response.Deleted = true
	data, err := json.Marshal(response)
	require.NoError(t, err)
	verifier := newTestSoakVerifier(catalog, &soakReadTransport{
		replies: []soakRPCFakeReply{{data: data}},
	}, &soakVerifyRecorder{})

	result := verifier.VerifyByID(context.Background(), "room-1", "message-1")
	assert.Equal(t, soakVerifyOK, result.Class)
	assert.Equal(t, soakRPCDelete, result.ExpectedAction)
}

func TestSoakVerifier_LoadHistoryFindsMessageAcrossBeforePages(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	other := verifiedCassandraMessage("room-1", "other", "bob", "other")
	other.CreatedAt = time.UnixMilli(300)
	target := verifiedCassandraMessage("room-1", "message-1", "alice", "original")
	target.CreatedAt = time.UnixMilli(100)
	first, err := json.Marshal(soakLoadHistoryResponse{Messages: []soakWireMessage{
		soakVerifyToWireMessage(&other),
	}})
	require.NoError(t, err)
	second, err := json.Marshal(soakLoadHistoryResponse{Messages: []soakWireMessage{
		soakVerifyToWireMessage(&target),
	}})
	require.NoError(t, err)
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: first},
		{data: second},
	}}
	verifier := newTestSoakVerifier(catalog, transport, &soakVerifyRecorder{})

	result := verifier.VerifyHistory(context.Background(), "room-1", "message-1")
	assert.Equal(t, soakVerifyOK, result.Class)
	assert.Equal(t, 2, result.Pages)

	calls := transport.snapshot()
	require.Len(t, calls, 2)
	var request soakLoadHistoryRequest
	require.NoError(t, json.Unmarshal(calls[0].data, &request))
	require.NotNil(t, request.Before)
	assert.Equal(t, int64(100001), *request.Before)
	require.NotNil(t, request.Meta)
	require.NotNil(t, request.Meta.LastMsgAt)
	require.NoError(t, json.Unmarshal(calls[1].data, &request))
	require.NotNil(t, request.Before)
	assert.Equal(t, int64(299), *request.Before)
	require.NotNil(t, request.Meta)
}

func TestSoakVerifier_ResultClassesRemainDistinct(t *testing.T) {
	tests := []struct {
		name        string
		replies     []soakRPCFakeReply
		attempts    int
		wantClass   soakVerifyClass
		wantRPC     soakErrorClass
		wantRetries int
	}{
		{
			name: "missing",
			replies: []soakRPCFakeReply{{
				data: []byte(`{"error":"missing","code":"not_found"}`),
			}},
			attempts:  1,
			wantClass: soakVerifyMissing,
			wantRPC:   soakErrorNotFound,
		},
		{
			name:      "malformed",
			replies:   []soakRPCFakeReply{{data: []byte(`{`)}},
			attempts:  1,
			wantClass: soakVerifyMalformed,
			wantRPC:   soakErrorDecode,
		},
		{
			name: "retryable exhausted",
			replies: []soakRPCFakeReply{
				{err: nats.ErrTimeout},
				{err: nats.ErrTimeout},
			},
			attempts:    2,
			wantClass:   soakVerifyRetryable,
			wantRPC:     soakErrorTimeout,
			wantRetries: 1,
		},
		{
			name: "retry succeeds",
			replies: []soakRPCFakeReply{
				{err: nats.ErrNoResponders},
				{data: mustVerifiedMessageJSON(
					"room-1", "message-1", "alice", "original",
				)},
			},
			attempts:    2,
			wantClass:   soakVerifyOK,
			wantRetries: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeSoakClock(time.Unix(100, 0))
			catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
			transport := &soakReadTransport{replies: tt.replies}
			recorder := &soakVerifyRecorder{}
			verifier := newSoakVerifier(&soakVerifyConfig{
				SiteID: "site-1", PageLimit: 2, MaxPages: 5,
				RequestTimeout: time.Second,
			}, catalog, newSoakRPCClient(
				transport,
				soakRetryConfig{
					MaxAttempts: tt.attempts,
					MinBackoff:  time.Millisecond,
					MaxBackoff:  time.Millisecond,
				},
				&soakRecordingSleeper{},
				nil,
			), recorder, steppedSoakNow())

			result := verifier.VerifyByID(
				context.Background(),
				"room-1",
				"message-1",
			)
			assert.Equal(t, tt.wantClass, result.Class)
			assert.Equal(t, tt.wantRPC, result.RPCErrorClass)
			assert.Equal(t, tt.wantRetries, result.Retries)
			assert.Len(t, transport.snapshot(), tt.attempts)
		})
	}
}

func TestSoakVerifier_HistoryMissingAfterBoundedPages(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: soakHistoryReply(t, 300)},
		{data: soakHistoryReply(t, 200)},
	}}
	verifier := newTestSoakVerifier(catalog, transport, &soakVerifyRecorder{})
	verifier.cfg.MaxPages = 2

	result := verifier.VerifyHistory(context.Background(), "room-1", "message-1")
	assert.Equal(t, soakVerifyMissing, result.Class)
	assert.Equal(t, 2, result.Pages)
	assert.Len(t, transport.snapshot(), 2)
}

func TestSoakVerifier_SamplesOnlyPersistenceEligibleCatalogEntries(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := newSoakCatalog(8, 100, 10*time.Second, clock)
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: "message-1", RoomID: "room-1", Author: "alice",
		Content: "original", CreatedAt: clock.Now(),
	}))
	require.True(t, catalog.Accept("room-1", "message-1"))
	transport := &soakReadTransport{replies: []soakRPCFakeReply{{
		data: mustVerifiedMessageJSON(
			"room-1", "message-1", "alice", "original",
		),
	}}}
	verifier := newTestSoakVerifier(catalog, transport, &soakVerifyRecorder{})

	result := verifier.Sample(context.Background(), "room-1")
	assert.Equal(t, soakVerifySkipped, result.Class)
	assert.Empty(t, transport.snapshot())

	clock.Advance(10 * time.Second)
	result = verifier.Sample(context.Background(), "room-1")
	assert.Equal(t, soakVerifyOK, result.Class)
	assert.Len(t, transport.snapshot(), 1)
}

func TestSoakVerifier_SamplerIncludesTrackedMutationStates(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "immutable", "alice")
	acceptMutationCatalogMessage(t, catalog, clock, "edited", "alice")
	require.True(t, catalog.MarkEdited("room-1", "edited", "updated"))
	edited := verifiedCassandraMessage("room-1", "edited", "alice", "updated")
	editedAt := time.Unix(101, 0)
	edited.EditedAt = &editedAt
	editedData, err := json.Marshal(edited)
	require.NoError(t, err)
	transport := &soakReadTransport{replies: []soakRPCFakeReply{{
		data: editedData,
	}}}
	verifier := newTestSoakVerifier(catalog, transport, &soakVerifyRecorder{})
	verifier.sampleSequence = 9

	result := verifier.Sample(context.Background(), "room-1")
	assert.Equal(t, soakVerifyOK, result.Class)
	assert.Equal(t, soakRPCEdit, result.ExpectedAction)
}

type soakVerifyRecorder struct {
	mu      sync.Mutex
	results []soakVerifyResult
}

func (r *soakVerifyRecorder) Record(result *soakVerifyResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, *result)
}

func (r *soakVerifyRecorder) snapshot() []soakVerifyResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]soakVerifyResult(nil), r.results...)
}

func newTestSoakVerifier(
	catalog *soakCatalog,
	transport soakRPCTransport,
	recorder soakVerifyResultRecorder,
) *soakVerifier {
	return newSoakVerifier(&soakVerifyConfig{
		SiteID: "site-1", PageLimit: 2, MaxPages: 5,
		RequestTimeout: time.Second,
	}, catalog, newSoakRPCClient(
		transport,
		soakRetryConfig{
			MaxAttempts: 1,
			MinBackoff:  time.Millisecond,
			MaxBackoff:  time.Millisecond,
		},
		&soakRecordingSleeper{},
		nil,
	), recorder, steppedSoakNow())
}

func verifiedCassandraMessage(
	roomID string,
	messageID string,
	author string,
	content string,
) cassandra.Message {
	return cassandra.Message{
		RoomID: roomID, MessageID: messageID, Msg: content,
		CreatedAt: time.Unix(100, 0),
		Sender: cassandra.Participant{
			ID:      "user-" + author,
			Account: author,
		},
	}
}

func mustVerifiedMessageJSON(
	roomID string,
	messageID string,
	author string,
	content string,
) []byte {
	data, err := json.Marshal(verifiedCassandraMessage(
		roomID,
		messageID,
		author,
		content,
	))
	if err != nil {
		panic(err)
	}
	return data
}

func soakVerifyToWireMessage(message *cassandra.Message) soakWireMessage {
	return soakWireMessage{
		RoomID: message.RoomID, CreatedAt: message.CreatedAt,
		MessageID: message.MessageID, Sender: message.Sender, Msg: message.Msg,
		ThreadParentID: message.ThreadParentID, Deleted: message.Deleted,
		EditedAt: message.EditedAt, PinnedAt: message.PinnedAt,
	}
}
