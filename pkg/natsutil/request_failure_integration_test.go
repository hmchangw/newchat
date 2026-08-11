//go:build integration

package natsutil

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/testutil"
)

// Proves the mapping fires on an error the real client produces, not just on a
// hand-constructed sentinel. A request to a subject nobody subscribes to
// returns ErrNoResponders when the connection has responder detection on,
// which is the default.
func TestRequestFailure_RealNoResponders(t *testing.T) {
	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	_, reqErr := nc.Request("nobody.is.listening.here", []byte("{}"), 2*time.Second)
	require.Error(t, reqErr)

	got := RequestFailure("probe rpc", reqErr)

	var typed *errcode.Error
	require.True(t, errors.As(got, &typed), "expected a typed errcode, got %v", got)
	require.Equal(t, errcode.CodeUnavailable, typed.Code)
	require.Equal(t, errcode.NatsNoResponders, typed.Reason)
}
