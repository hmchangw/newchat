package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// attrMap folds soakErrorAttrs' flat key/value slice into a map so a test can
// assert on presence and absence without depending on emission order.
func attrMap(t *testing.T, attrs []any) map[string]any {
	t.Helper()
	require.Zero(t, len(attrs)%2, "slog attrs must be key/value pairs")
	out := make(map[string]any, len(attrs)/2)
	for i := 0; i < len(attrs); i += 2 {
		key, ok := attrs[i].(string)
		require.True(t, ok, "attr key %d must be a string, got %T", i, attrs[i])
		out[key] = attrs[i+1]
	}
	return out
}

// The whole point of the carrier: a metric spike names the action, never the
// account. Without these attrs on the log line there is nothing to grep the
// server logs for.
func TestSoakErrorAttrs_CarriesTheRequestIdentity(t *testing.T) {
	cause := errors.New("nats request: context deadline exceeded")
	err := &soakRequestError{
		Action:   soakRPCSubscriptionList,
		Subject:  "chat.user.alice.request.user.tw01.subscription.list",
		Account:  "alice",
		RoomID:   "room-1",
		Class:    soakErrorResponseTooLarge,
		Reason:   soakErrorReason(soakReasonResponseTooLarge),
		Attempts: 3,
		Retries:  2,
		err:      cause,
	}

	got := attrMap(t, soakErrorAttrs(err))

	assert.Equal(t, string(soakRPCSubscriptionList), got["action"])
	assert.Equal(t, "alice", got["account"])
	assert.Equal(t, "room-1", got["room_id"])
	assert.Equal(t, "chat.user.alice.request.user.tw01.subscription.list", got["subject"])
	assert.Equal(t, string(soakErrorResponseTooLarge), got["error_class"])
	assert.Equal(t, string(soakReasonResponseTooLarge), got["error_reason"])
	assert.Equal(t, 3, got["attempts"])
	assert.Equal(t, 2, got["retries"])
	assert.Contains(t, got, "error")
}

// An absent field must not reach the log line as an empty string: a row of
// room_id="" on every user-read error is noise that trains readers to skip
// the field they will one day need.
func TestSoakErrorAttrs_OmitsEmptyFields(t *testing.T) {
	err := &soakRequestError{
		Action: soakRPCUserMe,
		Class:  soakErrorTimeout,
		err:    errors.New("boom"),
	}

	got := attrMap(t, soakErrorAttrs(err))

	assert.Equal(t, string(soakRPCUserMe), got["action"])
	assert.NotContains(t, got, "account")
	assert.NotContains(t, got, "room_id")
	assert.NotContains(t, got, "subject")
	assert.NotContains(t, got, "error_reason")
	assert.NotContains(t, got, "attempts")
	assert.NotContains(t, got, "retries")
}

// Lane callbacks log whatever error they are handed; ones that never went
// through the RPC client must still log their message rather than vanish.
func TestSoakErrorAttrs_FallsBackToTheBareError(t *testing.T) {
	err := errors.New("prepare soak room state pool")

	got := attrMap(t, soakErrorAttrs(err))

	assert.Equal(t, err, got["error"])
	assert.Len(t, got, 1)
}

func TestSoakErrorAttrs_NilErrorYieldsNoAttrs(t *testing.T) {
	assert.Empty(t, soakErrorAttrs(nil))
}

// The carrier is added by the RPC client and wrapped again by every lane on
// the way up, so it has to be findable through that wrapping.
func TestSoakErrorAttrs_FindsTheCarrierThroughWrapping(t *testing.T) {
	inner := &soakRequestError{
		Action:  soakRPCSubscriptionList,
		Account: "bob",
		Class:   soakErrorTimeout,
		err:     errors.New("deadline"),
	}
	wrapped := fmt.Errorf("issue %s request: %w", soakRPCSubscriptionList, inner)

	got := attrMap(t, soakErrorAttrs(wrapped))

	assert.Equal(t, "bob", got["account"])
	assert.Equal(t, wrapped, got["error"], "the outermost message is what a reader needs")
}

// Retry classification upstream keys on sentinel errors; the carrier must not
// break errors.Is by swallowing what it wraps.
func TestSoakRequestError_UnwrapsToItsCause(t *testing.T) {
	cause := fmt.Errorf("%w: %s", errSoakRetryExhausted, "subscription_list")
	err := &soakRequestError{Action: soakRPCSubscriptionList, err: cause}

	assert.ErrorIs(t, err, errSoakRetryExhausted)
	assert.Equal(t, cause, errors.Unwrap(err))
}

func TestSoakRequestError_ErrorKeepsTheCauseMessage(t *testing.T) {
	err := &soakRequestError{
		Action: soakRPCSubscriptionList,
		err:    errors.New("nats request: context deadline exceeded"),
	}

	assert.Contains(t, err.Error(), "subscription_list")
	assert.Contains(t, err.Error(), "nats request: context deadline exceeded")
}

// A carrier with no cause would be a programming error, but it must not panic
// a running soak: the log line degrades instead.
func TestSoakRequestError_SurvivesANilCause(t *testing.T) {
	err := &soakRequestError{Action: soakRPCSubscriptionList}

	assert.NotPanics(t, func() {
		_ = err.Error()
		_ = soakErrorAttrs(err)
	})
	assert.Nil(t, errors.Unwrap(err))
}

// --- carrier attachment at the RPC chokepoint ---

// newCarrierTestClient builds a client whose retries are instant, so a test can
// assert on the carrier without pacing a real backoff.
func newCarrierTestClient(transport soakRPCTransport, attempts int) *soakRPCClient {
	return newSoakRPCClient(transport, soakRetryConfig{
		MaxAttempts: attempts, MinBackoff: time.Millisecond,
		MaxBackoff: time.Millisecond, Jitter: 0,
	}, &soakRecordingSleeper{}, func() float64 { return 0 })
}

func TestSoakRPCClient_ExhaustedRetriesCarryTheRequestIdentity(t *testing.T) {
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{
		{err: context.DeadlineExceeded},
		{err: context.DeadlineExceeded},
	}}
	client := newCarrierTestClient(transport, 2)

	_, err := client.Call(context.Background(), soakRPCRequest{
		Action:    soakRPCSubscriptionList,
		Subject:   "chat.user.alice.request.user.tw01.subscription.list",
		Account:   "alice",
		RoomID:    "room-1",
		Timeout:   time.Second,
		RetryMode: soakRetrySafe,
	}, nil)

	require.Error(t, err)
	var carrier *soakRequestError
	require.ErrorAs(t, err, &carrier)
	assert.Equal(t, soakRPCSubscriptionList, carrier.Action)
	assert.Equal(t, "alice", carrier.Account)
	assert.Equal(t, "room-1", carrier.RoomID)
	assert.Equal(t, "chat.user.alice.request.user.tw01.subscription.list", carrier.Subject)
	assert.Equal(t, soakErrorTimeout, carrier.Class)
	assert.Equal(t, 2, carrier.Attempts)
	assert.Equal(t, 1, carrier.Retries)
}

// The ledger and the retry classifier both key on sentinels underneath the
// carrier; wrapping must not hide them.
func TestSoakRPCClient_CarrierPreservesTheRetrySentinel(t *testing.T) {
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{
		{err: context.DeadlineExceeded},
	}}
	client := newCarrierTestClient(transport, 1)

	_, err := client.Call(context.Background(), soakRPCRequest{
		Action: soakRPCSubscriptionList, Subject: "chat.test",
		Timeout: time.Second, RetryMode: soakRetrySafe,
	}, nil)

	assert.ErrorIs(t, err, errSoakRetryExhausted)
}

// The failure this whole change exists to make greppable: an oversize reply
// must land in the log with the account that could not load its sidebar.
func TestSoakRPCClient_OversizeReplyCarriesAccountAndReason(t *testing.T) {
	envelope := `{"code":"internal","error":"response payload exceeds maximum size","reason":"response_too_large"}`
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{
		{data: []byte(envelope)},
	}}
	client := newCarrierTestClient(transport, 1)

	_, err := client.Call(context.Background(), soakRPCRequest{
		Action:  soakRPCSubscriptionList,
		Subject: "chat.user.carol.request.user.tw01.subscription.list",
		Account: "carol",
		Timeout: time.Second, RetryMode: soakRetrySafe,
	}, nil)

	require.Error(t, err)
	var carrier *soakRequestError
	require.ErrorAs(t, err, &carrier)
	assert.Equal(t, "carol", carrier.Account)
	assert.Equal(t, soakErrorResponseTooLarge, carrier.Class)
	assert.Equal(t, soakErrorReason(soakReasonResponseTooLarge), carrier.Reason)

	got := attrMap(t, soakErrorAttrs(err))
	assert.Equal(t, "carol", got["account"])
	assert.Equal(t, string(soakErrorResponseTooLarge), got["error_class"])
}

// A body that never reached the wire has no reply to classify, but the action
// and account still identify what the run failed to do.
func TestSoakRPCClient_EncodeFailureStillCarriesTheAction(t *testing.T) {
	client := newCarrierTestClient(&soakRPCFakeTransport{}, 1)

	_, err := client.Call(context.Background(), soakRPCRequest{
		Action:  soakRPCSubscriptionList,
		Subject: "chat.test",
		Account: "dave",
		Body:    make(chan int), // channels are not JSON-encodable
		Timeout: time.Second,
	}, nil)

	require.Error(t, err)
	var carrier *soakRequestError
	require.ErrorAs(t, err, &carrier)
	assert.Equal(t, soakRPCSubscriptionList, carrier.Action)
	assert.Equal(t, "dave", carrier.Account)
	assert.Equal(t, soakErrorRequestEncode, carrier.Class)
}
