package presenceclient

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
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := o11ynats.Connect(context.Background(), ns.ClientURL(), noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

// stubResponder answers every request on subj with reply. Received messages are
// pushed onto the returned buffered channel so a test can assert on the wire
// request without sharing mutable state across goroutines.
func stubResponder(t *testing.T, nc *o11ynats.Conn, subj string, reply []byte) <-chan *nats.Msg {
	t.Helper()
	got := make(chan *nats.Msg, 8)
	sub, err := nc.Subscribe(context.Background(), subj, func(_ context.Context, m *nats.Msg) {
		got <- m
		_ = m.Respond(reply)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return got
}

// silentResponder subscribes without ever replying, so the caller's ctx
// deadline (not a bare missing subscriber) ends the request.
func silentResponder(t *testing.T, nc *o11ynats.Conn, subj string) {
	t.Helper()
	sub, err := nc.Subscribe(context.Background(), subj, func(_ context.Context, _ *nats.Msg) {})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// requireErrcode asserts err carries an *errcode.Error and returns it.
func requireErrcode(t *testing.T, err error) *errcode.Error {
	t.Helper()
	require.Error(t, err)
	var e *errcode.Error
	require.True(t, errors.As(err, &e), "want *errcode.Error, got %T: %v", err, err)
	return e
}

func TestNew(t *testing.T) {
	nc := startTestNATS(t)
	c := New(nc)
	require.NotNil(t, c)
	assert.Same(t, nc, c.nc)
}

func TestClient_QueryPresence_Subject(t *testing.T) {
	nc := startTestNATS(t)
	got := stubResponder(t, nc, "chat.server.request.presence.>", mustJSON(t, model.PresenceQueryResponse{}))

	_, err := New(nc).QueryPresence(context.Background(), "site-a", []string{"alice", "bob"})
	require.NoError(t, err)

	m := <-got
	assert.Equal(t, "chat.server.request.presence.site-a.query.batch", m.Subject)
	assert.Equal(t, subject.PresenceQueryBatchPeer("site-a"), m.Subject)

	var req model.PresenceQuery
	require.NoError(t, json.Unmarshal(m.Data, &req))
	assert.Equal(t, []string{"alice", "bob"}, req.Accounts)
}

func TestClient_QueryPresence_ReplyHandling(t *testing.T) {
	tests := []struct {
		name   string
		reply  []byte
		assert func(t *testing.T, states []model.PresenceState, err error)
	}{
		{
			name: "happy path decodes states",
			reply: mustJSON(t, model.PresenceQueryResponse{
				States: []model.PresenceState{
					{Account: "alice", SiteID: "site-a", Status: model.StatusOnline},
					{Account: "bob", SiteID: "site-a", Status: model.StatusAway},
				},
				Timestamp: 1717000000000,
			}),
			assert: func(t *testing.T, states []model.PresenceState, err error) {
				require.NoError(t, err)
				require.Len(t, states, 2)
				assert.Equal(t, "alice", states[0].Account)
				assert.Equal(t, model.StatusOnline, states[0].Status)
				assert.Equal(t, "bob", states[1].Account)
				assert.Equal(t, model.StatusAway, states[1].Status)
			},
		},
		{
			name:  "empty success envelope yields no states and no error",
			reply: []byte(`{}`),
			assert: func(t *testing.T, states []model.PresenceState, err error) {
				require.NoError(t, err)
				assert.Empty(t, states)
			},
		},
		{
			name:  "errcode envelope is relayed with code and reason intact",
			reply: mustJSON(t, errcode.BadRequest("batch exceeds max", errcode.WithReason(errcode.RequestIDRequired))),
			assert: func(t *testing.T, _ []model.PresenceState, err error) {
				e := requireErrcode(t, err)
				assert.Equal(t, errcode.CodeBadRequest, e.Code)
				assert.Equal(t, errcode.RequestIDRequired, e.Reason)
				assert.Equal(t, "batch exceeds max", e.Message)
			},
		},
		{
			name:  "unknown remote code is relayed, not masked",
			reply: []byte(`{"code":"upstream_only_code","error":"upstream boom"}`),
			assert: func(t *testing.T, _ []model.PresenceState, err error) {
				e := requireErrcode(t, err)
				assert.Equal(t, "upstream boom", e.Message)
				assert.Equal(t, errcode.Code("upstream_only_code"), e.Code)
			},
		},
		{
			name:  "non-JSON reply surfaces a wrapped decode error",
			reply: []byte(`[`),
			assert: func(t *testing.T, _ []model.PresenceState, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "decode presence-query response")
			},
		},
		{
			name:  "JSON of the wrong shape surfaces a wrapped decode error",
			reply: []byte(`{"states":"not-an-array"}`),
			assert: func(t *testing.T, _ []model.PresenceState, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "decode presence-query response")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nc := startTestNATS(t)
			stubResponder(t, nc, subject.PresenceQueryBatchPeer("site-a"), tc.reply)

			states, err := New(nc).QueryPresence(context.Background(), "site-a", []string{"alice"})
			tc.assert(t, states, err)
		})
	}
}

func TestClient_QueryPresence_EmptyAccounts(t *testing.T) {
	nc := startTestNATS(t)
	got := stubResponder(t, nc, subject.PresenceQueryBatchPeer("site-a"), mustJSON(t, model.PresenceQueryResponse{}))

	// The client does not short-circuit an empty batch — it still issues the RPC.
	states, err := New(nc).QueryPresence(context.Background(), "site-a", nil)
	require.NoError(t, err)
	assert.Empty(t, states)

	var raw map[string]any
	require.NoError(t, json.Unmarshal((<-got).Data, &raw))
	require.Contains(t, raw, "accounts")
	assert.Nil(t, raw["accounts"])
}

func TestClient_QueryPresence_SiteIDRouting(t *testing.T) {
	nc := startTestNATS(t)
	// Only site-b has a responder: the query must target the siteID argument.
	got := stubResponder(t, nc, subject.PresenceQueryBatchPeer("site-b"), mustJSON(t, model.PresenceQueryResponse{
		States: []model.PresenceState{{Account: "bob", SiteID: "site-b", Status: model.StatusAway}},
	}))

	states, err := New(nc).QueryPresence(context.Background(), "site-b", []string{"bob"})
	require.NoError(t, err)
	require.Len(t, states, 1)
	assert.Equal(t, model.StatusAway, states[0].Status)
	assert.Equal(t, "chat.server.request.presence.site-b.query.batch", (<-got).Subject)
}

func TestClient_QueryPresence_NoResponder(t *testing.T) {
	nc := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := New(nc).QueryPresence(ctx, "site-a", []string{"alice"})
	e := requireErrcode(t, err)
	assert.Equal(t, errcode.CodeUnavailable, e.Code)
	assert.Equal(t, errcode.NatsNoResponders, e.Reason)
	assert.Contains(t, e.Message, "presence-query rpc")
}

func TestClient_QueryPresence_Timeout(t *testing.T) {
	nc := startTestNATS(t)
	silentResponder(t, nc, subject.PresenceQueryBatchPeer("site-a"))

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, err := New(nc).QueryPresence(ctx, "site-a", []string{"alice"})
	e := requireErrcode(t, err)
	assert.Equal(t, errcode.CodeUnavailable, e.Code)
	assert.Equal(t, errcode.NatsRequestTimeout, e.Reason)
	assert.Contains(t, e.Message, "presence-query rpc")
}
