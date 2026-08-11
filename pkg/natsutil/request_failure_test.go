package natsutil_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsutil"
)

func TestRequestFailure(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantTyped  bool
		wantCode   errcode.Code
		wantReason errcode.Reason
	}{
		{
			name:       "no responders is unavailable",
			err:        nats.ErrNoResponders,
			wantTyped:  true,
			wantCode:   errcode.CodeUnavailable,
			wantReason: errcode.NatsNoResponders,
		},
		{
			name:       "timeout is unavailable",
			err:        nats.ErrTimeout,
			wantTyped:  true,
			wantCode:   errcode.CodeUnavailable,
			wantReason: errcode.NatsRequestTimeout,
		},
		{
			name:       "deadline exceeded is unavailable",
			err:        context.DeadlineExceeded,
			wantTyped:  true,
			wantCode:   errcode.CodeUnavailable,
			wantReason: errcode.NatsRequestTimeout,
		},
		{
			// Proves matching is errors.Is, not equality — the nats client
			// wraps its sentinels on some paths.
			name:       "wrapped no responders still matches",
			err:        fmt.Errorf("outer: %w", nats.ErrNoResponders),
			wantTyped:  true,
			wantCode:   errcode.CodeUnavailable,
			wantReason: errcode.NatsNoResponders,
		},
		{
			name:      "unrelated error stays a raw wrap",
			err:       errors.New("connection reset"),
			wantTyped: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := natsutil.RequestFailure("rooms-info rpc", tt.err)
			require.Error(t, got)
			require.Contains(t, got.Error(), "rooms-info rpc")

			var typed *errcode.Error
			ok := errors.As(got, &typed)
			require.Equal(t, tt.wantTyped, ok)
			if !tt.wantTyped {
				// The original error must remain unwrappable for callers
				// that inspect it further.
				require.ErrorIs(t, got, tt.err)
				return
			}
			require.Equal(t, tt.wantCode, typed.Code)
			require.Equal(t, tt.wantReason, typed.Reason)
		})
	}
}

func TestRequestFailure_NilReturnsNil(t *testing.T) {
	require.NoError(t, natsutil.RequestFailure("rooms-info rpc", nil))
}

// The cause is server-side only. If it ever reaches the wire envelope it would
// leak internal detail to clients, which is the one thing errcode guarantees
// against.
func TestRequestFailure_CauseNeverSerialised(t *testing.T) {
	err := natsutil.RequestFailure("rooms-info rpc", fmt.Errorf("dial 10.1.2.3:4222: %w", nats.ErrNoResponders))

	var typed *errcode.Error
	require.True(t, errors.As(err, &typed))

	data, mErr := json.Marshal(typed)
	require.NoError(t, mErr)
	require.NotContains(t, string(data), "10.1.2.3")
	require.NotContains(t, string(data), "no responders available")
}

// Proves the mapping fires on an error the real client produces, not just on a
// hand-constructed sentinel. A request to a subject nobody subscribes to
// returns ErrNoResponders, because responder detection is on by default.
//
// Uses the package's existing embedded-server helper, so this is a unit test:
// no Docker, and it runs everywhere make test runs.
func TestRequestFailure_RealNoResponders(t *testing.T) {
	nc := startTestNATSWithMaxPayload(t, 0) // 0 = leave the server default

	_, reqErr := nc.Request("nobody.is.listening.here", []byte("{}"), 2*time.Second)
	require.Error(t, reqErr)

	got := natsutil.RequestFailure("probe rpc", reqErr)

	var typed *errcode.Error
	require.True(t, errors.As(got, &typed), "expected a typed errcode, got %v", got)
	require.Equal(t, errcode.CodeUnavailable, typed.Code)
	require.Equal(t, errcode.NatsNoResponders, typed.Reason)
}
