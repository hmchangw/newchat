package read

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
	soakcatalog "github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/wire"
)

func TestSoakVerifier_GetMessageChecksPresenceRoomAuthorAndContent(t *testing.T) {
	tests := []struct {
		name      string
		response  cassandra.Message
		wantClass VerifyClass
		wantField VerifyField
	}{
		{
			name: "matches",
			response: verifiedCassandraMessage(
				"room-1", "message-1", "alice", "original",
			),
			wantClass: VerifyOK,
		},
		{
			name: "wrong room",
			response: verifiedCassandraMessage(
				"room-2", "message-1", "alice", "original",
			),
			wantClass: VerifyMismatch,
			wantField: "room_id",
		},
		{
			name: "wrong author",
			response: verifiedCassandraMessage(
				"room-1", "message-1", "bob", "original",
			),
			wantClass: VerifyMismatch,
			wantField: "author",
		},
		{
			name: "wrong content",
			response: verifiedCassandraMessage(
				"room-1", "message-1", "alice", "different secret body",
			),
			wantClass: VerifyMismatch,
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
			recorder := &verifyRecorder{}
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
			assert.Equal(t, []VerifyResult{result}, recorder.snapshot())
		})
	}
}

func TestSoakVerifyFields_AreClosedMetricVocabulary(t *testing.T) {
	for _, field := range []VerifyField{
		VerifyFieldNone,
		VerifyFieldMessageID,
		VerifyFieldRoomID,
		VerifyFieldAuthor,
		VerifyFieldDeleted,
		VerifyFieldContent,
		VerifyFieldEditedAt,
		VerifyFieldPagination,
	} {
		assert.True(t, ValidVerifyField(field), field)
	}
	assert.False(t, ValidVerifyField("unbounded"))
}

func TestSoakCompareVerifiedMessage_ClassifiesEveryMismatch(t *testing.T) {
	editedAt := time.Unix(101, 0).UTC()
	expected := soakcatalog.Message{
		Candidate: soakcatalog.Candidate{
			ID: "message-1", RoomID: "room-1", Author: "alice",
			ContentSHA256: soakcatalog.ContentDigest("hello"),
		},
		Edited: true,
	}
	valid := VerifyMessage{
		MessageID: "message-1", RoomID: "room-1",
		Sender: cassandra.Participant{ID: "user-alice", Account: "alice"},
		Msg:    "hello", EditedAt: &editedAt,
	}
	tests := []struct {
		name   string
		mutate func(*VerifyMessage)
		field  VerifyField
	}{
		{
			name: "message ID",
			mutate: func(actual *VerifyMessage) {
				actual.MessageID = "different"
			},
			field: VerifyFieldMessageID,
		},
		{
			name: "room ID",
			mutate: func(actual *VerifyMessage) {
				actual.RoomID = "different"
			},
			field: VerifyFieldRoomID,
		},
		{
			name: "author",
			mutate: func(actual *VerifyMessage) {
				actual.Sender.Account = "bob"
			},
			field: VerifyFieldAuthor,
		},
		{
			name: "deleted",
			mutate: func(actual *VerifyMessage) {
				actual.Deleted = true
			},
			field: VerifyFieldDeleted,
		},
		{
			name: "content",
			mutate: func(actual *VerifyMessage) {
				actual.Msg = "different"
			},
			field: VerifyFieldContent,
		},
		{
			name: "edited timestamp",
			mutate: func(actual *VerifyMessage) {
				actual.EditedAt = nil
			},
			field: VerifyFieldEditedAt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := valid
			tt.mutate(&actual)
			result := VerifyResult{}
			CompareVerifiedMessage(&result, &expected, &actual)
			assert.Equal(t, VerifyMismatch, result.Class)
			assert.Equal(t, tt.field, result.Field)
		})
	}

	result := VerifyResult{Field: "stale"}
	CompareVerifiedMessage(&result, &expected, &valid)
	assert.Equal(t, VerifyOK, result.Class)
	assert.Empty(t, result.Field)

	deletedExpected := expected
	deletedExpected.Deleted = true
	deletedActual := valid
	deletedActual.Deleted = true
	deletedActual.Msg = ""
	CompareVerifiedMessage(&result, &deletedExpected, &deletedActual)
	assert.Equal(t, VerifyOK, result.Class)
}

func TestSoakClassifyRPCError_CoversTerminalAndTransientClasses(t *testing.T) {
	tests := []struct {
		class rpc.ErrorClass
		want  VerifyClass
	}{
		{class: rpc.ErrorNotFound, want: VerifyMissing},
		{class: rpc.ErrorRequestEncode, want: VerifyMalformed},
		{class: rpc.ErrorResponseDecode, want: VerifyMalformed},
		{class: rpc.ErrorTimeout, want: VerifyRetryable},
		{class: rpc.ErrorForbidden, want: VerifyRPCError},
	}
	for _, tt := range tests {
		result := VerifyResult{}
		ClassifyRPCError(&result, tt.class, "")
		assert.Equal(t, tt.want, result.Class)
		assert.Equal(t, tt.class, result.RPCErrorClass)
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
	}, &verifyRecorder{})

	result := verifier.VerifyByID(context.Background(), "room-1", "message-1")
	assert.Equal(t, VerifyOK, result.Class)
	assert.Equal(t, rpc.ActionEdit, result.ExpectedAction)
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
	}, &verifyRecorder{})

	result := verifier.VerifyByID(context.Background(), "room-1", "message-1")
	assert.Equal(t, VerifyOK, result.Class)
	assert.Equal(t, rpc.ActionDelete, result.ExpectedAction)
}

func TestSoakVerifier_LoadHistoryFindsMessageAcrossBeforePages(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	other := verifiedCassandraMessage("room-1", "other", "bob", "other")
	other.CreatedAt = time.UnixMilli(300)
	target := verifiedCassandraMessage("room-1", "message-1", "alice", "original")
	target.CreatedAt = time.UnixMilli(100)
	first, err := json.Marshal(wire.LoadHistoryResponse{Messages: []wire.Message{
		soakVerifyToWireMessage(&other),
	}})
	require.NoError(t, err)
	second, err := json.Marshal(wire.LoadHistoryResponse{Messages: []wire.Message{
		soakVerifyToWireMessage(&target),
	}})
	require.NoError(t, err)
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: first},
		{data: second},
	}}
	verifier := newTestSoakVerifier(catalog, transport, &verifyRecorder{})

	result := verifier.VerifyHistory(context.Background(), "room-1", "message-1")
	assert.Equal(t, VerifyOK, result.Class)
	assert.Equal(t, 2, result.Pages)

	calls := transport.snapshot()
	require.Len(t, calls, 2)
	var request wire.LoadHistoryRequest
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
		wantClass   VerifyClass
		wantRPC     rpc.ErrorClass
		wantRetries int
	}{
		{
			name: "missing",
			replies: []soakRPCFakeReply{{
				data: []byte(`{"error":"missing","code":"not_found"}`),
			}},
			attempts:  1,
			wantClass: VerifyMissing,
			wantRPC:   rpc.ErrorNotFound,
		},
		{
			name:      "malformed",
			replies:   []soakRPCFakeReply{{data: []byte(`{`)}},
			attempts:  1,
			wantClass: VerifyMalformed,
			wantRPC:   rpc.ErrorResponseDecode,
		},
		{
			name: "retryable exhausted",
			replies: []soakRPCFakeReply{
				{err: nats.ErrTimeout},
				{err: nats.ErrTimeout},
			},
			attempts:    2,
			wantClass:   VerifyRetryable,
			wantRPC:     rpc.ErrorTimeout,
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
			wantClass:   VerifyOK,
			wantRetries: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeSoakClock(time.Unix(100, 0))
			catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
			transport := &soakReadTransport{replies: tt.replies}
			recorder := &verifyRecorder{}
			verifier := NewVerifier(&VerifyConfig{
				SiteID: "site-1", PageLimit: 2, MaxPages: 5,
				RequestTimeout: time.Second,
			}, catalog, rpc.NewClient(
				transport,
				rpc.RetryConfig{
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
	verifier := newTestSoakVerifier(catalog, transport, &verifyRecorder{})
	verifier.cfg.MaxPages = 2

	result := verifier.VerifyHistory(context.Background(), "room-1", "message-1")
	assert.Equal(t, VerifyMissing, result.Class)
	assert.Equal(t, 2, result.Pages)
	assert.Len(t, transport.snapshot(), 2)
}

func TestSoakVerifier_HistoryClassifiesRPCAndPaginationFailures(t *testing.T) {
	tests := []struct {
		name      string
		reply     soakRPCFakeReply
		wantClass VerifyClass
		wantField VerifyField
		wantRPC   rpc.ErrorClass
	}{
		{
			name:      "RPC timeout",
			reply:     soakRPCFakeReply{err: nats.ErrTimeout},
			wantClass: VerifyRetryable,
			wantRPC:   rpc.ErrorTimeout,
		},
		{
			name:      "non-progressing page",
			reply:     soakRPCFakeReply{data: soakHistoryReply(t, 100001)},
			wantClass: VerifyMismatch,
			wantField: VerifyFieldPagination,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeSoakClock(time.Unix(100, 0))
			messageCatalog := acceptedMutationMessage(
				t,
				clock,
				"message-1",
				"alice",
			)
			verifier := newTestSoakVerifier(
				messageCatalog,
				&soakReadTransport{replies: []soakRPCFakeReply{tt.reply}},
				&verifyRecorder{},
			)

			result := verifier.VerifyHistory(
				context.Background(),
				"room-1",
				"message-1",
			)
			assert.Equal(t, tt.wantClass, result.Class)
			assert.Equal(t, tt.wantField, result.Field)
			assert.Equal(t, tt.wantRPC, result.RPCErrorClass)
		})
	}
}

func TestSoakVerifier_MissingCatalogCandidatesAreSkipped(t *testing.T) {
	messageCatalog := soakcatalog.New(8, 100, 0, nil)
	verifier := NewVerifier(nil, messageCatalog, nil, nil, nil)

	byID := verifier.VerifyByID(context.Background(), "room-1", "missing")
	assert.Equal(t, VerifySkipped, byID.Class)
	history := verifier.VerifyHistory(context.Background(), "room-1", "missing")
	assert.Equal(t, VerifySkipped, history.Class)
}

func TestSoakVerifier_SamplesOnlyPersistenceEligibleCatalogEntries(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := soakcatalog.New(8, 100, 10*time.Second, clock)
	require.NoError(t, catalog.TrackPublished(&soakcatalog.Candidate{
		ID: "message-1", RoomID: "room-1", Author: "alice",
		Content: "original", CreatedAt: clock.Now(),
	}))
	require.True(t, catalog.Accept("room-1", "message-1"))
	transport := &soakReadTransport{replies: []soakRPCFakeReply{{
		data: mustVerifiedMessageJSON(
			"room-1", "message-1", "alice", "original",
		),
	}}}
	verifier := newTestSoakVerifier(catalog, transport, &verifyRecorder{})

	result := verifier.Sample(context.Background(), "room-1")
	assert.Equal(t, VerifySkipped, result.Class)
	assert.Empty(t, transport.snapshot())

	clock.Advance(10 * time.Second)
	result = verifier.Sample(context.Background(), "room-1")
	assert.Equal(t, VerifyOK, result.Class)
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
	verifier := newTestSoakVerifier(catalog, transport, &verifyRecorder{})
	verifier.sampleSequence = 9

	result := verifier.Sample(context.Background(), "room-1")
	assert.Equal(t, VerifyOK, result.Class)
	assert.Equal(t, rpc.ActionEdit, result.ExpectedAction)
}

type verifyRecorder struct {
	mu      sync.Mutex
	results []VerifyResult
}

func (r *verifyRecorder) Record(result *VerifyResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, *result)
}

func (r *verifyRecorder) snapshot() []VerifyResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]VerifyResult(nil), r.results...)
}

func newTestSoakVerifier(
	catalog *soakcatalog.Catalog,
	transport rpc.Transport,
	recorder VerifyResultRecorder,
) *Verifier {
	return NewVerifier(&VerifyConfig{
		SiteID: "site-1", PageLimit: 2, MaxPages: 5,
		RequestTimeout: time.Second,
	}, catalog, rpc.NewClient(
		transport,
		rpc.RetryConfig{
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

func soakVerifyToWireMessage(message *cassandra.Message) wire.Message {
	return wire.Message{
		RoomID: message.RoomID, CreatedAt: message.CreatedAt,
		MessageID: message.MessageID, Sender: message.Sender, Msg: message.Msg,
		ThreadParentID: message.ThreadParentID, Deleted: message.Deleted,
		EditedAt: message.EditedAt, PinnedAt: message.PinnedAt,
	}
}
