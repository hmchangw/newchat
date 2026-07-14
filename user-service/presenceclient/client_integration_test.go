//go:build integration

package presenceclient

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/user-service/service"
)

// Compile-time assertion: `go vet -tags integration` fails if Client drifts from PresenceClient.
var _ service.PresenceClient = (*Client)(nil)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// dial returns a connected *otelnats.Conn backed by the shared test NATS
// server. The connection is drained on test cleanup.
func dial(t *testing.T) *otelnats.Conn {
	t.Helper()
	nc, err := otelnats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })
	return nc
}

func TestQueryPresence_Integration(t *testing.T) {
	t.Run("happy path — returns states from responder", func(t *testing.T) {
		nc := dial(t)

		sub, err := nc.Subscribe(subject.PresenceQueryBatchPeer("site-a"), func(m otelnats.Msg) {
			out, _ := json.Marshal(model.PresenceQueryResponse{
				States: []model.PresenceState{{Account: "alice", SiteID: "site-a", Status: model.StatusOnline}},
			})
			_ = m.Msg.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		states, err := New(nc).QueryPresence(context.Background(), "site-a", []string{"alice"})
		require.NoError(t, err)
		require.Len(t, states, 1)
		assert.Equal(t, "alice", states[0].Account)
		assert.Equal(t, model.StatusOnline, states[0].Status)
	})

	t.Run("errcode reply — returns typed errcode error", func(t *testing.T) {
		nc := dial(t)

		sub, err := nc.Subscribe(subject.PresenceQueryBatchPeer("site-a"), func(m otelnats.Msg) {
			data, _ := json.Marshal(errcode.BadRequest("batch exceeds max"))
			_ = m.Msg.Respond(data)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc).QueryPresence(context.Background(), "site-a", []string{"alice"})
		require.Error(t, err)
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, errcode.CodeBadRequest, e.Code)
	})

	t.Run("no responder — returns error wrapping presence-query rpc", func(t *testing.T) {
		nc := dial(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := New(nc).QueryPresence(ctx, "site-a", []string{"alice"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "presence-query rpc")
	})

	t.Run("siteID routing — targets the siteID param", func(t *testing.T) {
		nc := dial(t)

		// Responder on "site-b" subject proves the method routes on the siteID param.
		sub, err := nc.Subscribe(subject.PresenceQueryBatchPeer("site-b"), func(m otelnats.Msg) {
			out, _ := json.Marshal(model.PresenceQueryResponse{
				States: []model.PresenceState{{Account: "bob", SiteID: "site-b", Status: model.StatusAway}},
			})
			_ = m.Msg.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		states, err := New(nc).QueryPresence(context.Background(), "site-b", []string{"bob"})
		require.NoError(t, err)
		require.Len(t, states, 1)
		assert.Equal(t, model.StatusAway, states[0].Status)
	})

	t.Run("malformed reply — returns decode error", func(t *testing.T) {
		nc := dial(t)

		// A reply that is neither a parseable errcode envelope nor a valid
		// PresenceQueryResponse must surface the decode fall-through error.
		sub, err := nc.Subscribe(subject.PresenceQueryBatchPeer("site-a"), func(m otelnats.Msg) {
			_ = m.Msg.Respond([]byte(`[`))
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc).QueryPresence(context.Background(), "site-a", []string{"alice"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "decode presence-query response")
	})

	t.Run("unknown-code error envelope — relayed, not masked", func(t *testing.T) {
		nc := dial(t)

		sub, err := nc.Subscribe(subject.PresenceQueryBatchPeer("site-a"), func(m otelnats.Msg) {
			_ = m.Msg.Respond([]byte(`{"code":"upstream_only_code","error":"upstream boom"}`))
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc).QueryPresence(context.Background(), "site-a", []string{"alice"})
		require.Error(t, err)
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, "upstream boom", e.Message)
	})
}
