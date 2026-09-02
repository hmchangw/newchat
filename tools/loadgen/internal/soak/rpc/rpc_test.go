package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scriptedReply struct {
	data []byte
	err  error
}

type scriptedTransport struct {
	replies []scriptedReply
	calls   int
}

func (t *scriptedTransport) Request(
	_ context.Context,
	_ string,
	_ []byte,
	_ time.Duration,
) ([]byte, error) {
	reply := t.replies[t.calls]
	t.calls++
	return reply.data, reply.err
}

type recordingSleeper struct {
	delays []time.Duration
}

func (s *recordingSleeper) Sleep(_ context.Context, delay time.Duration) error {
	s.delays = append(s.delays, delay)
	return nil
}

func TestNewSoakRPCClient_Defaults(t *testing.T) {
	client := NewClient(&scriptedTransport{}, RetryConfig{}, nil, nil)

	assert.Equal(t, 1, client.retry.MaxAttempts)
	assert.Equal(t, 100*time.Millisecond, client.retry.MinBackoff)
	assert.Equal(t, client.retry.MinBackoff, client.retry.MaxBackoff)
	assert.NotNil(t, client.sleeper)
	assert.Equal(t, 0.5, client.random())
}

func TestSoakRPCClient_RetriesSafeTimeoutAndDecodesSuccess(t *testing.T) {
	transport := &scriptedTransport{replies: []scriptedReply{
		{err: nats.ErrTimeout},
		{data: []byte(`{"status":"ok"}`)},
	}}
	sleeper := &recordingSleeper{}
	client := NewClient(transport, RetryConfig{
		MaxAttempts: 2,
		MinBackoff:  time.Millisecond,
		MaxBackoff:  time.Millisecond,
	}, sleeper, func() float64 { return 0.5 })
	var response struct {
		Status string `json:"status"`
	}

	result, err := client.Call(context.Background(), Request{
		Action:    ActionLoadHistory,
		Subject:   "chat.test",
		Body:      struct{}{},
		Timeout:   time.Second,
		RetryMode: RetrySafe,
	}, &response)

	require.NoError(t, err)
	assert.Equal(t, "ok", response.Status)
	assert.Equal(t, 2, result.Attempts)
	assert.Equal(t, 1, result.Retries)
	assert.Equal(t, len(`{"status":"ok"}`), result.ReplyBytes)
	assert.Equal(t, []time.Duration{time.Millisecond}, sleeper.delays)
}

func TestSoakRPCPublicSurface_DelegatesWithoutChangingSemantics(t *testing.T) {
	actions := UserReadActions()
	require.NotEmpty(t, actions)
	actions[0] = Action("mutated-copy")
	assert.NotEqual(t, Action("mutated-copy"), UserReadActions()[0])

	assert.True(t, ValidAction(ActionLoadHistory))
	assert.False(t, ValidAction(Action("unknown")))
	assert.True(t, ValidErrorClass(ErrorTimeout))
	assert.False(t, ValidErrorClass(ErrorClass("unknown")))
	assert.True(t, ValidErrorReason(""))
	assert.True(t, ValidErrorReason(ErrorReasonUnknown))
	assert.True(t, ValidErrorReason(ErrorReason(ReasonResponseTooLarge)))
	assert.False(t, ValidErrorReason(ErrorReason("unknown_reason")))

	assert.Equal(t, ErrorAssertion, ClassifyError(NewAssertionError("different message")))
	parsed := ParseErrorEnvelope([]byte(
		`{"code":"forbidden","error":"denied","reason":"not_subscribed"}`,
	))
	require.Error(t, parsed)
	assert.Equal(t, ErrorForbidden, ClassifyError(parsed))
	assert.Equal(t, ErrorReason("not_subscribed"), ClassifyReason(parsed))
	assert.True(t, IsTransientError(ErrorTimeout))
	assert.False(t, IsTransientError(ErrorForbidden))

	attrs := ErrorAttrs(&RequestError{
		Action: ActionLoadHistory,
		Cause:  context.DeadlineExceeded,
	})
	assert.Contains(t, attrs, "action")

	require.NoError(t, (TimerSleeper{}).Sleep(context.Background(), 0))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, (TimerSleeper{}).Sleep(canceled, time.Hour), context.Canceled)
}
