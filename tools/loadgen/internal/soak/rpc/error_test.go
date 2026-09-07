package rpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// attrMap folds ErrorAttrs' flat key/value slice into a map so a test can
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
	err := &RequestError{
		Action:   ActionSubscriptionList,
		Subject:  "chat.user.alice.request.user.tw01.subscription.list",
		Account:  "alice",
		RoomID:   "room-1",
		Class:    ErrorResponseTooLarge,
		Reason:   ErrorReason(ReasonResponseTooLarge),
		Attempts: 3,
		Retries:  2,
		Cause:    cause,
	}

	got := attrMap(t, ErrorAttrs(err))

	assert.Equal(t, string(ActionSubscriptionList), got["action"])
	assert.Equal(t, "alice", got["account"])
	assert.Equal(t, "room-1", got["room_id"])
	assert.Equal(t, "chat.user.alice.request.user.tw01.subscription.list", got["subject"])
	assert.Equal(t, string(ErrorResponseTooLarge), got["error_class"])
	assert.Equal(t, string(ReasonResponseTooLarge), got["error_reason"])
	assert.Equal(t, 3, got["attempts"])
	assert.Equal(t, 2, got["retries"])
	assert.Contains(t, got, "error")
}

// An absent field must not reach the log line as an empty string: a row of
// room_id="" on every user-read error is noise that trains readers to skip
// the field they will one day need.
func TestSoakErrorAttrs_OmitsEmptyFields(t *testing.T) {
	err := &RequestError{
		Action: ActionUserMe,
		Class:  ErrorTimeout,
		Cause:  errors.New("boom"),
	}

	got := attrMap(t, ErrorAttrs(err))

	assert.Equal(t, string(ActionUserMe), got["action"])
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

	got := attrMap(t, ErrorAttrs(err))

	assert.Equal(t, err, got["error"])
	assert.Len(t, got, 1)
}

func TestSoakErrorAttrs_NilErrorYieldsNoAttrs(t *testing.T) {
	assert.Empty(t, ErrorAttrs(nil))
}

// The carrier is added by the RPC client and wrapped again by every lane on
// the way up, so it has to be findable through that wrapping.
func TestSoakErrorAttrs_FindsTheCarrierThroughWrapping(t *testing.T) {
	inner := &RequestError{
		Action:  ActionSubscriptionList,
		Account: "bob",
		Class:   ErrorTimeout,
		Cause:   errors.New("deadline"),
	}
	wrapped := fmt.Errorf("issue %s request: %w", ActionSubscriptionList, inner)

	got := attrMap(t, ErrorAttrs(wrapped))

	assert.Equal(t, "bob", got["account"])
	assert.Equal(t, wrapped, got["error"], "the outermost message is what a reader needs")
}

// Retry classification upstream keys on sentinel errors; the carrier must not
// break errors.Is by swallowing what it wraps.
func TestSoakRequestError_UnwrapsToItsCause(t *testing.T) {
	cause := fmt.Errorf("%w: %s", ErrRetryExhausted, "subscription_list")
	err := &RequestError{Action: ActionSubscriptionList, Cause: cause}

	assert.ErrorIs(t, err, ErrRetryExhausted)
	assert.Equal(t, cause, errors.Unwrap(err))
}

// The carrier decorates a failure, it does not describe a new one. Naming the
// action here duplicates both the lane's own wrap and the action attr, giving
// "issue subscription_list request: subscription_list: ..." on every line.
func TestSoakRequestError_ErrorIsTheCauseVerbatim(t *testing.T) {
	cause := errors.New("nats request: context deadline exceeded")
	err := &RequestError{Action: ActionSubscriptionList, Cause: cause}

	assert.Equal(t, cause.Error(), err.Error())
}

// A carrier with no cause would be a programming error, but it must not panic
// a running soak: the log line degrades instead.
func TestSoakRequestError_SurvivesANilCause(t *testing.T) {
	err := &RequestError{Action: ActionSubscriptionList}

	assert.NotPanics(t, func() {
		_ = err.Error()
		_ = ErrorAttrs(err)
	})
	assert.Nil(t, errors.Unwrap(err))
}

// --- carrier attachment at the RPC chokepoint ---

// newCarrierTestClient builds a client whose retries are instant, so a test can
// assert on the carrier without pacing a real backoff.
func newCarrierTestClient(transport Transport, attempts int) *Client {
	return NewClient(transport, RetryConfig{
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

	_, err := client.Call(context.Background(), Request{
		Action:    ActionSubscriptionList,
		Subject:   "chat.user.alice.request.user.tw01.subscription.list",
		Account:   "alice",
		RoomID:    "room-1",
		Timeout:   time.Second,
		RetryMode: RetrySafe,
	}, nil)

	require.Error(t, err)
	var carrier *RequestError
	require.ErrorAs(t, err, &carrier)
	assert.Equal(t, ActionSubscriptionList, carrier.Action)
	assert.Equal(t, "alice", carrier.Account)
	assert.Equal(t, "room-1", carrier.RoomID)
	assert.Equal(t, "chat.user.alice.request.user.tw01.subscription.list", carrier.Subject)
	assert.Equal(t, ErrorTimeout, carrier.Class)
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

	_, err := client.Call(context.Background(), Request{
		Action: ActionSubscriptionList, Subject: "chat.test",
		Timeout: time.Second, RetryMode: RetrySafe,
	}, nil)

	assert.ErrorIs(t, err, ErrRetryExhausted)
}

// The failure this whole change exists to make greppable: an oversize reply
// must land in the log with the account that could not load its sidebar.
func TestSoakRPCClient_OversizeReplyCarriesAccountAndReason(t *testing.T) {
	envelope := `{"code":"internal","error":"response payload exceeds maximum size","reason":"response_too_large"}`
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{
		{data: []byte(envelope)},
	}}
	client := newCarrierTestClient(transport, 1)

	_, err := client.Call(context.Background(), Request{
		Action:  ActionSubscriptionList,
		Subject: "chat.user.carol.request.user.tw01.subscription.list",
		Account: "carol",
		Timeout: time.Second, RetryMode: RetrySafe,
	}, nil)

	require.Error(t, err)
	var carrier *RequestError
	require.ErrorAs(t, err, &carrier)
	assert.Equal(t, "carol", carrier.Account)
	assert.Equal(t, ErrorResponseTooLarge, carrier.Class)
	assert.Equal(t, ErrorReason(ReasonResponseTooLarge), carrier.Reason)

	got := attrMap(t, ErrorAttrs(err))
	assert.Equal(t, "carol", got["account"])
	assert.Equal(t, string(ErrorResponseTooLarge), got["error_class"])
}

// A body that never reached the wire has no reply to classify, but the action
// and account still identify what the run failed to do.
func TestSoakRPCClient_EncodeFailureStillCarriesTheAction(t *testing.T) {
	client := newCarrierTestClient(&soakRPCFakeTransport{}, 1)

	_, err := client.Call(context.Background(), Request{
		Action:  ActionSubscriptionList,
		Subject: "chat.test",
		Account: "dave",
		Body:    make(chan int), // channels are not JSON-encodable
		Timeout: time.Second,
	}, nil)

	require.Error(t, err)
	var carrier *RequestError
	require.ErrorAs(t, err, &carrier)
	assert.Equal(t, ActionSubscriptionList, carrier.Action)
	assert.Equal(t, "dave", carrier.Account)
	assert.Equal(t, ErrorRequestEncode, carrier.Class)
}

// --- a dead context still identifies the request ---

// soakIgnoringSleeper keeps backing off after the context dies, so a test can
// reach the guard at the top of the next attempt. The real sleeper returns the
// context error and short-circuits there.
type soakIgnoringSleeper struct{}

func (*soakIgnoringSleeper) Sleep(context.Context, time.Duration) error { return nil }

// The read recorder derives the outcome from the error class alone, so a
// failure that leaves the class empty is counted as a success. A context that
// died before Call reached the wire did exactly that: a bare error with a
// zero-valued result, logged without identity and tallied as a completed read.
func TestSoakRPCClient_CanceledContextCarriesTheRequestIdentity(t *testing.T) {
	transport := &soakRPCFakeTransport{}
	client := newCarrierTestClient(transport, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := client.Call(ctx, Request{
		Action:  ActionSubscriptionList,
		Subject: "chat.user.erin.request.user.tw01.subscription.list",
		Account: "erin", RoomID: "room-9",
		Timeout: time.Second, RetryMode: RetrySafe,
	}, nil)

	require.Error(t, err)
	assert.Zero(t, transport.callCount(), "a dead context must not reach the wire")
	var carrier *RequestError
	require.ErrorAs(t, err, &carrier)
	assert.Equal(t, "erin", carrier.Account)
	assert.Equal(t, "room-9", carrier.RoomID)
	assert.Equal(t, ErrorCanceled, carrier.Class)
	assert.Equal(t, ErrorCanceled, result.ErrorClass,
		"an empty class is what the recorder reads as a success")
}

// A deadline that elapsed before the call is a timeout, not a cancellation:
// the two say different things about the site under load.
func TestSoakRPCClient_ExpiredDeadlineIsClassifiedAsATimeout(t *testing.T) {
	client := newCarrierTestClient(&soakRPCFakeTransport{}, 1)
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()

	result, err := client.Call(ctx, Request{
		Action: ActionSubscriptionList, Subject: "chat.test",
		Account: "erin", Timeout: time.Second, RetryMode: RetrySafe,
	}, nil)

	require.Error(t, err)
	assert.Equal(t, ErrorTimeout, result.ErrorClass)
}

// The guard at the top of each attempt returned bare too. A soak torn down
// mid-retry takes this path, and it had already spent an attempt on the wire —
// so the identity has to survive without the cancellation overwriting why the
// operation actually failed.
func TestSoakRPCClient_CancellationBetweenAttemptsCarriesTheIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := &soakCancelingTransport{cancel: cancel, inner: &soakRPCFakeTransport{
		replies: []soakRPCFakeReply{{err: context.DeadlineExceeded}},
	}}
	client := NewClient(transport, RetryConfig{
		MaxAttempts: 3, MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond,
	}, &soakIgnoringSleeper{}, func() float64 { return 0 })

	result, err := client.Call(ctx, Request{
		Action: ActionSubscriptionList, Subject: "chat.test",
		Account: "frank", Timeout: time.Second, RetryMode: RetrySafe,
	}, nil)

	require.Error(t, err)
	var carrier *RequestError
	require.ErrorAs(t, err, &carrier)
	assert.Equal(t, "frank", carrier.Account)
	assert.Equal(t, ErrorTimeout, carrier.Class,
		"the attempt reached the wire and timed out; its effect is unknown, and "+
			"reporting the teardown instead would erase that")
	assert.Equal(t, 1, result.Attempts, "the spent attempt must still be reported")
}

// soakCancelingTransport kills the context once the request is on the wire, so
// the next attempt starts against a dead one.
type soakCancelingTransport struct {
	inner  Transport
	cancel context.CancelFunc
}

func (t *soakCancelingTransport) Request(
	ctx context.Context,
	requestSubject string,
	data []byte,
	timeout time.Duration,
) ([]byte, error) {
	defer t.cancel()
	return t.inner.Request(ctx, requestSubject, data, timeout)
}

// Keeping the failed attempt's class while reporting only the cancellation
// leaves the log line contradicting itself: error="context canceled" beside
// error_class=timeout, with the transport error that caused the retrying gone.
// Both causes have to survive, at both exits.
func TestSoakRPCClient_InterruptedRetryReportsBothCauses(t *testing.T) {
	for name, sleeper := range map[string]Sleeper{
		"canceled before the next attempt": &soakIgnoringSleeper{},
		"canceled during the backoff":      &soakRecordingSleeper{},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			transport := &soakCancelingTransport{cancel: cancel, inner: &soakRPCFakeTransport{
				replies: []soakRPCFakeReply{{err: nats.ErrTimeout}},
			}}
			client := NewClient(transport, RetryConfig{
				MaxAttempts: 3, MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond,
			}, sleeper, func() float64 { return 0 })

			result, err := client.Call(ctx, Request{
				Action: ActionSubscriptionList, Subject: "chat.test",
				Account: "grace", Timeout: time.Second, RetryMode: RetrySafe,
			}, nil)

			require.Error(t, err)
			assert.ErrorIs(t, err, context.Canceled, "why the retrying stopped")
			assert.ErrorIs(t, err, nats.ErrTimeout, "why the operation failed")
			assert.Equal(t, ErrorTimeout, result.ErrorClass,
				"the class must agree with the cause the message reports")

			got := attrMap(t, ErrorAttrs(err))
			assert.Equal(t, "grace", got["account"])
		})
	}
}

// With no attempt behind it there is no second cause to name, and the message
// must still say more than the bare context error.
func TestSoakRPCClient_CancellationBeforeAnyAttemptNamesTheAction(t *testing.T) {
	client := newCarrierTestClient(&soakRPCFakeTransport{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Call(ctx, Request{
		Action: ActionSubscriptionList, Subject: "chat.test",
		Account: "grace", Timeout: time.Second, RetryMode: RetrySafe,
	}, nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), string(ActionSubscriptionList))
	assert.Equal(t, 1, strings.Count(err.Error(), string(ActionSubscriptionList)),
		"Call names the action once; got %q", err.Error())
}

// soakCancelingSleeper completes its backoff but kills the context while doing
// so, which is the shutdown race that reaches the guard on the next attempt.
type soakCancelingSleeper struct{ cancel context.CancelFunc }

func (s *soakCancelingSleeper) Sleep(context.Context, time.Duration) error {
	s.cancel()
	return nil
}

// Retries is incremented after the backoff, before the next attempt's guard, so
// a run torn down in that window reported a retry that never reached the
// transport — and `loadgen_soak_retries_total` counted it.
func TestSoakRPCClient_CancellationAfterBackoffCountsNoRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{{err: nats.ErrTimeout}}}
	client := NewClient(transport, RetryConfig{
		MaxAttempts: 3, MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond,
	}, &soakCancelingSleeper{cancel: cancel}, func() float64 { return 0 })

	result, err := client.Call(ctx, Request{
		Action: ActionSubscriptionList, Subject: "chat.test",
		Account: "heidi", Timeout: time.Second, RetryMode: RetrySafe,
	}, nil)

	require.Error(t, err)
	assert.Equal(t, 1, transport.callCount(), "only the first attempt reached the wire")
	assert.Equal(t, 1, result.Attempts)
	assert.Zero(t, result.Retries, "a retry that never ran must not be counted")
}

// The counter still has to move when a retry does run, or the fix trades an
// over-count for an under-count.
func TestSoakRPCClient_AnExecutedRetryIsStillCounted(t *testing.T) {
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{
		{err: nats.ErrTimeout}, {err: nats.ErrTimeout},
	}}
	client := newCarrierTestClient(transport, 2)

	result, err := client.Call(context.Background(), Request{
		Action: ActionSubscriptionList, Subject: "chat.test",
		Account: "heidi", Timeout: time.Second, RetryMode: RetrySafe,
	}, nil)

	require.Error(t, err)
	assert.Equal(t, 2, result.Attempts)
	assert.Equal(t, 1, result.Retries, "Retries must stay Attempts-1")
}
