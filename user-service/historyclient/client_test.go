package historyclient

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// startTestNATS spins up an embedded, in-process NATS server (no Docker) for
// request/reply unit tests.
func startTestNATS(t *testing.T) *o11ynats.Conn {
	t.Helper()
	opts := &natsserver.Options{Port: -1}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := o11ynats.Connect(context.Background(), ns.ClientURL(), noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

func TestRoomsGet_Hints(t *testing.T) {
	t.Run("non-empty hints are marshaled into the request body", func(t *testing.T) {
		nc := startTestNATS(t)

		lastMsgAt := int64(1234)
		var gotReq model.RoomsGetRequest
		sub, err := nc.Subscribe(context.Background(), subject.RoomsGet("site-a"), func(_ context.Context, m *nats.Msg) {
			require.NoError(t, json.Unmarshal(m.Data, &gotReq))
			out, _ := json.Marshal(model.RoomsGetResponse{})
			_ = m.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		hints := map[string]model.RoomTimeHint{"r1": {LastMsgAt: &lastMsgAt}}
		_, err = New(nc).RoomsGet(context.Background(), "site-a", []string{"r1"}, hints)
		require.NoError(t, err)

		require.Contains(t, gotReq.Hints, "r1")
		require.NotNil(t, gotReq.Hints["r1"].LastMsgAt)
		assert.Equal(t, lastMsgAt, *gotReq.Hints["r1"].LastMsgAt)
	})

	t.Run("nil hints are omitted from the request body", func(t *testing.T) {
		nc := startTestNATS(t)

		var gotRaw map[string]any
		sub, err := nc.Subscribe(context.Background(), subject.RoomsGet("site-a"), func(_ context.Context, m *nats.Msg) {
			require.NoError(t, json.Unmarshal(m.Data, &gotRaw))
			out, _ := json.Marshal(model.RoomsGetResponse{})
			_ = m.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc).RoomsGet(context.Background(), "site-a", []string{"r1"}, nil)
		require.NoError(t, err)

		_, present := gotRaw["hints"]
		assert.False(t, present, "nil hints must be omitted from the wire request")
	})

	t.Run("happy path — returns preview messages from responder", func(t *testing.T) {
		nc := startTestNATS(t)
		sub, err := nc.Subscribe(context.Background(), subject.RoomsGet("site-a"), func(_ context.Context, m *nats.Msg) {
			out, _ := json.Marshal(model.RoomsGetResponse{
				Rooms: map[string]model.PreviewMessage{"r1": {MessageID: "m1"}},
			})
			_ = m.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		out, err := New(nc).RoomsGet(context.Background(), "site-a", []string{"r1"}, nil)
		require.NoError(t, err)
		require.Contains(t, out, "r1")
		assert.Equal(t, "m1", out["r1"].MessageID)
	})

	t.Run("errcode reply — returns typed errcode error", func(t *testing.T) {
		nc := startTestNATS(t)
		sub, err := nc.Subscribe(context.Background(), subject.RoomsGet("site-a"), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.NotFound("room not found"))
			_ = m.Respond(data)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc).RoomsGet(context.Background(), "site-a", []string{"r1"}, nil)
		require.Error(t, err)
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, errcode.CodeNotFound, e.Code)
	})

	t.Run("no responder — returns error wrapping rooms-get rpc", func(t *testing.T) {
		nc := startTestNATS(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := New(nc).RoomsGet(ctx, "site-a", []string{"r1"}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rooms-get rpc")
	})
}
