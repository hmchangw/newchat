package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
)

type soakRPCFakeReply struct {
	data []byte
	err  error
}

type soakRPCFakeTransport struct {
	mu      sync.Mutex
	replies []soakRPCFakeReply
	calls   int
}

func (f *soakRPCFakeTransport) Request(
	_ context.Context,
	_ string,
	_ []byte,
	_ time.Duration,
) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.replies) == 0 {
		return nil, fmt.Errorf("unexpected request")
	}
	reply := f.replies[0]
	f.replies = f.replies[1:]
	return reply.data, reply.err
}

func (f *soakRPCFakeTransport) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type soakRecordingSleeper struct {
	delays []time.Duration
}

func (s *soakRecordingSleeper) Sleep(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.delays = append(s.delays, delay)
	return nil
}

func TestClassifySoakRPCError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want soakErrorClass
	}{
		{"timeout context", context.DeadlineExceeded, soakErrorTimeout},
		{"timeout NATS", nats.ErrTimeout, soakErrorTimeout},
		{"no responders", nats.ErrNoResponders, soakErrorNoResponder},
		{"disconnected", nats.ErrDisconnected, soakErrorDisconnected},
		{"connection closed", nats.ErrConnectionClosed, soakErrorDisconnected},
		{"unavailable envelope", soakErrorEnvelope(`{"error":"later","code":"unavailable"}`), soakErrorUnavailable},
		{"internal envelope", soakErrorEnvelope(`{"error":"failed","code":"internal"}`), soakErrorInternal},
		{"not found envelope", soakErrorEnvelope(`{"error":"missing","code":"not_found"}`), soakErrorNotFound},
		{"forbidden envelope", soakErrorEnvelope(`{"error":"no","code":"forbidden"}`), soakErrorForbidden},
		{"bad request envelope", soakErrorEnvelope(`{"error":"bad","code":"bad_request"}`), soakErrorBadRequest},
		{"conflict envelope", soakErrorEnvelope(`{"error":"exists","code":"conflict"}`), soakErrorConflict},
		{"too many requests envelope", soakErrorEnvelope(`{"error":"slow down","code":"too_many_requests"}`), soakErrorUnavailable},
		{"unknown", errors.New("boom"), soakErrorInternal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, classifySoakRPCError(tt.err))
		})
	}
}

func TestSoakRPCClient_RetriesSafeTransientErrorsWithBoundedBackoff(t *testing.T) {
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{
		{err: nats.ErrNoResponders},
		{err: context.DeadlineExceeded},
		{data: []byte(`{"messageId":"m1"}`)},
	}}
	sleeper := &soakRecordingSleeper{}
	client := newSoakRPCClient(transport, soakRetryConfig{
		MaxAttempts: 4,
		MinBackoff:  100 * time.Millisecond,
		MaxBackoff:  150 * time.Millisecond,
		Jitter:      0,
	}, sleeper, func() float64 { return 0.5 })

	var response soakUnpinMessageResponse
	result, err := client.Call(context.Background(), soakRPCRequest{
		Action:    soakRPCGetMessage,
		Subject:   "chat.test",
		Body:      soakGetMessageByIDRequest{MessageID: "m1"},
		Timeout:   time.Second,
		RetryMode: soakRetrySafe,
	}, &response)

	require.NoError(t, err)
	assert.Equal(t, "m1", response.MessageID)
	assert.Equal(t, 3, result.Attempts)
	assert.Equal(t, 2, result.Retries)
	assert.Equal(t, []time.Duration{100 * time.Millisecond, 150 * time.Millisecond}, sleeper.delays)
}

func TestSoakRPCClient_JitterStaysWithinConfiguredRange(t *testing.T) {
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{
		{err: nats.ErrTimeout},
		{data: []byte(`{"messageId":"m1"}`)},
	}}
	sleeper := &soakRecordingSleeper{}
	client := newSoakRPCClient(transport, soakRetryConfig{
		MaxAttempts: 2,
		MinBackoff:  100 * time.Millisecond,
		MaxBackoff:  time.Second,
		Jitter:      0.20,
	}, sleeper, func() float64 { return 1 })

	_, err := client.Call(context.Background(), soakRPCRequest{
		Action:    soakRPCGetMessage,
		Subject:   "chat.test",
		Body:      soakGetMessageByIDRequest{MessageID: "m1"},
		Timeout:   time.Second,
		RetryMode: soakRetrySafe,
	}, &soakUnpinMessageResponse{})

	require.NoError(t, err)
	require.Len(t, sleeper.delays, 1)
	assert.Equal(t, 120*time.Millisecond, sleeper.delays[0])
}

func TestSoakRPCClient_DoesNotRetryTerminalEnvelope(t *testing.T) {
	for _, code := range []string{"bad_request", "forbidden", "not_found", "conflict"} {
		t.Run(code, func(t *testing.T) {
			transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{{
				data: []byte(`{"error":"terminal","code":"` + code + `"}`),
			}}}
			client := newSoakRPCClient(transport, soakRetryConfig{
				MaxAttempts: 3,
				MinBackoff:  time.Millisecond,
				MaxBackoff:  time.Second,
			}, &soakRecordingSleeper{}, nil)

			result, err := client.Call(context.Background(), soakRPCRequest{
				Action:    soakRPCEdit,
				Subject:   "chat.test",
				Body:      soakEditMessageRequest{MessageID: "m1", NewMsg: "new"},
				Timeout:   time.Second,
				RetryMode: soakRetrySafe,
			}, &soakEditMessageResponse{})

			require.Error(t, err)
			assert.Equal(t, 1, result.Attempts)
			assert.Equal(t, 0, result.Retries)
			assert.Equal(t, 1, transport.callCount())
		})
	}
}

func TestSoakRPCClient_ReportsRetryExhaustion(t *testing.T) {
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{
		{err: nats.ErrTimeout},
		{err: nats.ErrTimeout},
		{err: nats.ErrTimeout},
	}}
	client := newSoakRPCClient(transport, soakRetryConfig{
		MaxAttempts: 3,
		MinBackoff:  time.Millisecond,
		MaxBackoff:  time.Second,
	}, &soakRecordingSleeper{}, nil)

	result, err := client.Call(context.Background(), soakRPCRequest{
		Action:    soakRPCLoadHistory,
		Subject:   "chat.test",
		Body:      soakLoadHistoryRequest{Limit: 50},
		Timeout:   time.Second,
		RetryMode: soakRetrySafe,
	}, &soakLoadHistoryResponse{})

	require.Error(t, err)
	assert.ErrorIs(t, err, errSoakRetryExhausted)
	assert.ErrorIs(t, err, nats.ErrTimeout)
	assert.Equal(t, soakErrorTimeout, result.ErrorClass)
	assert.Equal(t, 3, result.Attempts)
	assert.Equal(t, 2, result.Retries)
}

func TestSoakRPCClient_ContextCancellationStopsImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	transport := &soakRPCFakeTransport{}
	sleeper := &soakRecordingSleeper{}
	client := newSoakRPCClient(transport, soakRetryConfig{
		MaxAttempts: 3,
		MinBackoff:  time.Millisecond,
		MaxBackoff:  time.Second,
	}, sleeper, nil)

	result, err := client.Call(ctx, soakRPCRequest{
		Action:    soakRPCLoadHistory,
		Subject:   "chat.test",
		Body:      soakLoadHistoryRequest{Limit: 50},
		Timeout:   time.Second,
		RetryMode: soakRetrySafe,
	}, &soakLoadHistoryResponse{})

	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, result.Attempts)
	assert.Zero(t, transport.callCount())
	assert.Empty(t, sleeper.delays)
}

func TestSoakRPCClient_ReactionTimeoutIsNotBlindlyRetried(t *testing.T) {
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{
		{err: nats.ErrTimeout},
		{data: []byte(`{"messageId":"m1","shortcode":":wave:","action":"added","reactedAt":1}`)},
	}}
	client := newSoakRPCClient(transport, soakRetryConfig{
		MaxAttempts: 3,
		MinBackoff:  time.Millisecond,
		MaxBackoff:  time.Second,
	}, &soakRecordingSleeper{}, nil)

	result, err := client.Call(context.Background(), soakRPCRequest{
		Action:    soakRPCReact,
		Subject:   "chat.test",
		Body:      soakReactMessageRequest{MessageID: "m1", Shortcode: ":wave:"},
		Timeout:   time.Second,
		RetryMode: soakRetryAmbiguous,
	}, &soakReactMessageResponse{})

	require.Error(t, err)
	assert.Equal(t, soakErrorAmbiguous, result.ErrorClass)
	assert.Equal(t, 1, result.Attempts)
	assert.Equal(t, 1, transport.callCount())
}

func TestSoakRPCClient_ReactionResolverControlsRetry(t *testing.T) {
	tests := []struct {
		name          string
		retryNeeded   bool
		wantCalls     int
		wantResolved  bool
		wantErr       bool
		secondReplies []soakRPCFakeReply
	}{
		{
			name:         "desired state already observed",
			retryNeeded:  false,
			wantCalls:    1,
			wantResolved: true,
		},
		{
			name:         "desired state absent so one retry is allowed",
			retryNeeded:  true,
			wantCalls:    2,
			wantResolved: false,
			secondReplies: []soakRPCFakeReply{{
				data: []byte(`{"messageId":"m1","shortcode":":wave:","action":"added","reactedAt":1}`),
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replies := append([]soakRPCFakeReply{{err: nats.ErrTimeout}}, tt.secondReplies...)
			transport := &soakRPCFakeTransport{replies: replies}
			client := newSoakRPCClient(transport, soakRetryConfig{
				MaxAttempts: 3,
				MinBackoff:  time.Millisecond,
				MaxBackoff:  time.Second,
			}, &soakRecordingSleeper{}, nil)

			result, err := client.Call(context.Background(), soakRPCRequest{
				Action:    soakRPCReact,
				Subject:   "chat.test",
				Body:      soakReactMessageRequest{MessageID: "m1", Shortcode: ":wave:"},
				Timeout:   time.Second,
				RetryMode: soakRetryAmbiguous,
				ResolveAmbiguity: func(context.Context) (bool, error) {
					return tt.retryNeeded, nil
				},
			}, &soakReactMessageResponse{})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantCalls, transport.callCount())
			assert.Equal(t, tt.wantResolved, result.AmbiguityResolved)
		})
	}
}

func TestSoakRPCClient_ClassifiesDecodeAndAssertionErrors(t *testing.T) {
	t.Run("decode", func(t *testing.T) {
		transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{{data: []byte(`{`)}}}
		client := newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil)
		result, err := client.Call(context.Background(), soakRPCRequest{
			Action: soakRPCGetMessage, Subject: "chat.test",
			Body: soakGetMessageByIDRequest{MessageID: "m1"}, Timeout: time.Second,
		}, &cannedSoakResponse{})
		require.Error(t, err)
		assert.Equal(t, soakErrorResponseDecode, result.ErrorClass)
	})

	t.Run("assertion", func(t *testing.T) {
		err := newSoakAssertionError("message content differs")
		assert.Equal(t, soakErrorAssertion, classifySoakRPCError(err))
	})
}

func TestSoakRPCActionAndErrorLabelsAreBounded(t *testing.T) {
	assert.True(t, validSoakRPCAction(soakRPCPinnedList))
	assert.False(t, validSoakRPCAction(soakRPCAction("room-123")))
	assert.True(t, validSoakErrorClass(soakErrorMutationTargetMissing))
	assert.False(t, validSoakErrorClass(soakErrorClass("arbitrary-error-text")))
}

type cannedSoakResponse struct {
	MessageID string `json:"messageId"`
}

func soakErrorEnvelope(payload string) error {
	var wire map[string]any
	if err := json.Unmarshal([]byte(payload), &wire); err != nil {
		panic(err)
	}
	return parseSoakErrorEnvelope([]byte(payload))
}

// history-service replies with a compact oversize envelope when a page would
// exceed the broker's max_payload (pkg/natsutil.oversizeEnvelope). It carries
// code=internal, so without a dedicated class it is indistinguishable from a
// server fault — and the operator cannot tell "lower --page-limit" from
// "the service is broken".
func TestClassifySoakRPCError_ResponseTooLargeIsItsOwnClass(t *testing.T) {
	oversize := []byte(`{"code":"internal","reason":"response_too_large","error":"response payload exceeds maximum size"}`)
	parsed, ok := errcode.Parse(oversize)
	require.True(t, ok, "the oversize envelope must be a parseable errcode envelope")

	assert.Equal(t, soakErrorResponseTooLarge, classifySoakRPCError(parsed))
}

// An ordinary internal error must keep its own class.
func TestClassifySoakRPCError_PlainInternalStaysInternal(t *testing.T) {
	plain, ok := errcode.Parse([]byte(`{"code":"internal","error":"boom"}`))
	require.True(t, ok)
	assert.Equal(t, soakErrorInternal, classifySoakRPCError(plain))
}

// The class has to be in the reported set or it is counted and never shown.
func TestSoakAllErrorClasses_IncludesResponseTooLarge(t *testing.T) {
	assert.Contains(t, soakAllErrorClasses[:], soakErrorResponseTooLarge)
}

// A class that aggregate reporting counts but validation rejects would be
// dropped somewhere between the two. Assert the two sets agree rather than
// spot-checking one entry.
func TestValidSoakErrorClass_AgreesWithTheReportedSet(t *testing.T) {
	for _, class := range soakAllErrorClasses {
		assert.True(t, validSoakErrorClass(class),
			"%q is reported but rejected by validation", class)
	}
}

func TestValidSoakRPCAction_AcceptsRoomAndMemberActions(t *testing.T) {
	for _, action := range []soakRPCAction{
		soakRPCMemberAdd, soakRPCMemberRemove, soakRPCRoomRename, soakRPCMuteToggle,
		soakRPCRoomCreate, soakRPCMemberList, soakRPCRoomsInfo,
		soakRPCSubscriptionList, soakRPCRoomStateRead,
	} {
		t.Run(string(action), func(t *testing.T) {
			assert.True(t, validSoakRPCAction(action))
		})
	}
}

// The two failures sit on opposite sides of the wire, so they must not share a
// class: one proves the request never left, the other proves the server replied.
func TestSoakRPCClient_ClassifiesEncodeAndDecodeSeparately(t *testing.T) {
	client := newSoakRPCClient(
		&soakRPCFakeTransport{}, soakRetryConfig{MaxAttempts: 1}, nil, nil,
	)

	result, err := client.Call(context.Background(), soakRPCRequest{
		Action: soakRPCGetMessage, Body: make(chan int),
	}, nil)
	require.Error(t, err)
	assert.Equal(t, soakErrorRequestEncode, result.ErrorClass)

	decoding := newSoakRPCClient(
		&soakRPCFakeTransport{replies: []soakRPCFakeReply{{data: []byte(`{"messageId":`)}}},
		soakRetryConfig{MaxAttempts: 1}, nil, nil,
	)
	var response soakGetMessageByIDRequest
	result, err = decoding.Call(context.Background(), soakRPCRequest{
		Action: soakRPCGetMessage,
	}, &response)
	require.Error(t, err)
	assert.Equal(t, soakErrorResponseDecode, result.ErrorClass)
}
