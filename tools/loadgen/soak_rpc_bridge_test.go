package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

type soakRecordingSleeper struct {
	delays []time.Duration
}

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

func newCarrierTestClient(transport soakRPCTransport, attempts int) *soakRPCClient {
	return newSoakRPCClient(transport, soakRetryConfig{
		MaxAttempts: attempts,
		MinBackoff:  time.Millisecond,
		MaxBackoff:  time.Millisecond,
	}, &soakRecordingSleeper{}, func() float64 { return 0 })
}

func (s *soakRecordingSleeper) Sleep(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.delays = append(s.delays, delay)
	return nil
}

// Reporting stays in package main, so its error-class inventory remains an
// integration contract between the RPC package and the outer collector.
func TestSoakAllErrorClasses_IncludesResponseTooLarge(t *testing.T) {
	assert.Contains(t, soakAllErrorClasses[:], soakErrorResponseTooLarge)
}

func TestValidSoakErrorClass_AgreesWithTheReportedSet(t *testing.T) {
	for _, class := range soakAllErrorClasses {
		assert.True(t, validSoakErrorClass(class),
			"%q is reported but rejected by validation", class)
	}
}

// These two tests cross from the outer lane into the extracted RPC package,
// so they intentionally stay at the package-main integration boundary.
func TestSoakRoomReader_LaneFailureNamesTheActionOnce(t *testing.T) {
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{
		{err: nats.ErrTimeout},
	}}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 1)

	err := reader.SubscriptionList(context.Background())

	require.Error(t, err)
	assert.Equal(t, 1,
		strings.Count(err.Error(), string(soakRPCSubscriptionList)),
		"got %q", err.Error())
}

func TestSoakRoomReader_ACanceledReadIsRecordedAsAFailure(t *testing.T) {
	reader, _, recorder := newSoakRoomReadFixture(t, &soakRPCFakeTransport{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.Error(t, reader.SubscriptionList(ctx))

	require.Len(t, recorder.samples, 1)
	assert.Equal(t, soakErrorCanceled, recorder.samples[0].ErrorClass,
		"an empty class makes soakReadCollectorRecorder count this as succeeded")
}
