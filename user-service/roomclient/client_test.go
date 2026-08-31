package roomclient

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
	c := New(nc, "site-a")
	require.NotNil(t, c)
	assert.Same(t, nc, c.nc)
	assert.Equal(t, "site-a", c.siteID)
}

func TestClient_GetRoomsInfo_Subject(t *testing.T) {
	nc := startTestNATS(t)
	got := stubResponder(t, nc, "chat.server.request.room.>", mustJSON(t, model.RoomsInfoBatchResponse{}))

	_, err := New(nc, "site-a").GetRoomsInfo(context.Background(), "site-a", []string{"r1"})
	require.NoError(t, err)

	m := <-got
	assert.Equal(t, "chat.server.request.room.site-a.info.batch", m.Subject)
	assert.Equal(t, subject.RoomsInfoBatch("site-a"), m.Subject)

	var req model.RoomsInfoBatchRequest
	require.NoError(t, json.Unmarshal(m.Data, &req))
	assert.Equal(t, []string{"r1"}, req.RoomIDs)
}

func TestClient_GetRoomsInfo_ReplyHandling(t *testing.T) {
	lastMsgAt := int64(1717000000000)
	tests := []struct {
		name   string
		reply  []byte
		assert func(t *testing.T, rooms []model.RoomInfo, err error)
	}{
		{
			name: "happy path decodes rooms",
			reply: mustJSON(t, model.RoomsInfoBatchResponse{Rooms: []model.RoomInfo{
				{RoomID: "r1", Found: true, Name: "Eng", UserCount: 3, LastMsgAt: &lastMsgAt},
			}}),
			assert: func(t *testing.T, rooms []model.RoomInfo, err error) {
				require.NoError(t, err)
				require.Len(t, rooms, 1)
				assert.Equal(t, "r1", rooms[0].RoomID)
				assert.Equal(t, "Eng", rooms[0].Name)
				assert.Equal(t, 3, rooms[0].UserCount)
				require.NotNil(t, rooms[0].LastMsgAt)
				assert.Equal(t, lastMsgAt, *rooms[0].LastMsgAt)
			},
		},
		{
			name:  "empty success envelope yields no rooms and no error",
			reply: []byte(`{}`),
			assert: func(t *testing.T, rooms []model.RoomInfo, err error) {
				require.NoError(t, err)
				assert.Empty(t, rooms)
			},
		},
		{
			name:  "errcode envelope is relayed with code and reason intact",
			reply: mustJSON(t, errcode.NotFound("room not found", errcode.WithReason(errcode.RoomNotMember))),
			assert: func(t *testing.T, _ []model.RoomInfo, err error) {
				e := requireErrcode(t, err)
				assert.Equal(t, errcode.CodeNotFound, e.Code)
				assert.Equal(t, errcode.RoomNotMember, e.Reason)
				assert.Equal(t, "room not found", e.Message)
			},
		},
		{
			name:  "unknown remote code is relayed, not masked",
			reply: []byte(`{"code":"upstream_only_code","error":"upstream boom"}`),
			assert: func(t *testing.T, _ []model.RoomInfo, err error) {
				e := requireErrcode(t, err)
				assert.Equal(t, "upstream boom", e.Message)
				assert.Equal(t, errcode.Code("upstream_only_code"), e.Code)
			},
		},
		{
			name:  "non-JSON reply surfaces a wrapped decode error",
			reply: []byte("not json at all"),
			assert: func(t *testing.T, _ []model.RoomInfo, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "decode rooms-info response")
			},
		},
		{
			name:  "JSON of the wrong shape surfaces a wrapped decode error",
			reply: []byte(`{"rooms":"not-an-array"}`),
			assert: func(t *testing.T, _ []model.RoomInfo, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "decode rooms-info response")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nc := startTestNATS(t)
			stubResponder(t, nc, subject.RoomsInfoBatch("site-a"), tc.reply)

			rooms, err := New(nc, "site-a").GetRoomsInfo(context.Background(), "site-a", []string{"r1"})
			tc.assert(t, rooms, err)
		})
	}
}

func TestClient_GetRoomsMeta_SetsSkipKeys(t *testing.T) {
	nc := startTestNATS(t)
	got := stubResponder(t, nc, subject.RoomsInfoBatch("site-a"), mustJSON(t, model.RoomsInfoBatchResponse{}))
	c := New(nc, "site-a")

	_, err := c.GetRoomsMeta(context.Background(), "site-a", []string{"r1"})
	require.NoError(t, err)
	_, err = c.GetRoomsInfo(context.Background(), "site-a", []string{"r1"})
	require.NoError(t, err)

	var meta, info model.RoomsInfoBatchRequest
	require.NoError(t, json.Unmarshal((<-got).Data, &meta))
	require.NoError(t, json.Unmarshal((<-got).Data, &info))
	assert.True(t, meta.SkipKeys, "GetRoomsMeta must request skipKeys")
	assert.False(t, info.SkipKeys, "GetRoomsInfo must not request skipKeys")
}

func TestClient_GetRoomsMeta_ReplyHandling(t *testing.T) {
	t.Run("happy path decodes rooms", func(t *testing.T) {
		nc := startTestNATS(t)
		stubResponder(t, nc, subject.RoomsInfoBatch("site-a"), mustJSON(t, model.RoomsInfoBatchResponse{
			Rooms: []model.RoomInfo{{RoomID: "r1", Found: true, Name: "Eng"}},
		}))

		rooms, err := New(nc, "site-a").GetRoomsMeta(context.Background(), "site-a", []string{"r1"})
		require.NoError(t, err)
		require.Len(t, rooms, 1)
		assert.Equal(t, "Eng", rooms[0].Name)
	})

	t.Run("errcode envelope is relayed", func(t *testing.T) {
		nc := startTestNATS(t)
		stubResponder(t, nc, subject.RoomsInfoBatch("site-a"), mustJSON(t, errcode.Forbidden("nope")))

		_, err := New(nc, "site-a").GetRoomsMeta(context.Background(), "site-a", []string{"r1"})
		assert.Equal(t, errcode.CodeForbidden, requireErrcode(t, err).Code)
	})
}

func TestClient_GetRoomsInfo_EmptyRoomIDs(t *testing.T) {
	nc := startTestNATS(t)
	got := stubResponder(t, nc, subject.RoomsInfoBatch("site-a"), mustJSON(t, model.RoomsInfoBatchResponse{}))

	// The client does not short-circuit an empty batch — it still issues the RPC.
	rooms, err := New(nc, "site-a").GetRoomsInfo(context.Background(), "site-a", nil)
	require.NoError(t, err)
	assert.Empty(t, rooms)

	var raw map[string]any
	require.NoError(t, json.Unmarshal((<-got).Data, &raw))
	require.Contains(t, raw, "roomIds")
	assert.Nil(t, raw["roomIds"])
	assert.NotContains(t, raw, "skipKeys", "skipKeys is omitempty and must stay off the wire when false")
}

func TestClient_GetRoomsInfo_CrossSiteRouting(t *testing.T) {
	nc := startTestNATS(t)
	// Only site-b has a responder: proves the method routes on the siteID
	// argument rather than the client's own siteID.
	got := stubResponder(t, nc, subject.RoomsInfoBatch("site-b"), mustJSON(t, model.RoomsInfoBatchResponse{
		Rooms: []model.RoomInfo{{RoomID: "r2", Found: true, Name: "Remote"}},
	}))

	rooms, err := New(nc, "site-a").GetRoomsInfo(context.Background(), "site-b", []string{"r2"})
	require.NoError(t, err)
	require.Len(t, rooms, 1)
	assert.Equal(t, "Remote", rooms[0].Name)
	assert.Equal(t, "chat.server.request.room.site-b.info.batch", (<-got).Subject)
}

func TestClient_GetRoomsInfo_NoResponder(t *testing.T) {
	nc := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := New(nc, "site-a").GetRoomsInfo(ctx, "site-a", []string{"r1"})
	e := requireErrcode(t, err)
	assert.Equal(t, errcode.CodeUnavailable, e.Code)
	assert.Equal(t, errcode.NatsNoResponders, e.Reason)
	assert.Contains(t, e.Message, "rooms-info rpc")
}

func TestClient_GetRoomsInfo_Timeout(t *testing.T) {
	nc := startTestNATS(t)
	silentResponder(t, nc, subject.RoomsInfoBatch("site-a"))

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, err := New(nc, "site-a").GetRoomsInfo(ctx, "site-a", []string{"r1"})
	e := requireErrcode(t, err)
	assert.Equal(t, errcode.CodeUnavailable, e.Code)
	assert.Equal(t, errcode.NatsRequestTimeout, e.Reason)
	assert.Contains(t, e.Message, "rooms-info rpc")
}

func TestClient_GetThreadRoomInfoBatch_Subject(t *testing.T) {
	nc := startTestNATS(t)
	got := stubResponder(t, nc, "chat.server.request.room.>", mustJSON(t, model.ThreadRoomInfoBatchResponse{}))

	_, err := New(nc, "site-a").GetThreadRoomInfoBatch(context.Background(), "site-a", []string{"tr1"})
	require.NoError(t, err)

	m := <-got
	assert.Equal(t, "chat.server.request.room.site-a.thread.info.batch", m.Subject)
	var req model.ThreadRoomInfoBatchRequest
	require.NoError(t, json.Unmarshal(m.Data, &req))
	assert.Equal(t, []string{"tr1"}, req.ThreadRoomIDs)
}

func TestClient_GetThreadRoomInfoBatch_ReplyHandling(t *testing.T) {
	tests := []struct {
		name   string
		reply  []byte
		assert func(t *testing.T, threads []model.ThreadRoomInfo, err error)
	}{
		{
			name: "happy path decodes threads",
			reply: mustJSON(t, model.ThreadRoomInfoBatchResponse{Threads: []model.ThreadRoomInfo{
				{ThreadRoomID: "tr1", Found: true, LastMsgAt: 42},
			}}),
			assert: func(t *testing.T, threads []model.ThreadRoomInfo, err error) {
				require.NoError(t, err)
				require.Len(t, threads, 1)
				assert.Equal(t, "tr1", threads[0].ThreadRoomID)
				assert.Equal(t, int64(42), threads[0].LastMsgAt)
			},
		},
		{
			name:  "empty success envelope yields no threads",
			reply: []byte(`{}`),
			assert: func(t *testing.T, threads []model.ThreadRoomInfo, err error) {
				require.NoError(t, err)
				assert.Empty(t, threads)
			},
		},
		{
			name:  "errcode envelope is relayed",
			reply: mustJSON(t, errcode.BadRequest("bad batch", errcode.WithReason(errcode.RequestIDRequired))),
			assert: func(t *testing.T, _ []model.ThreadRoomInfo, err error) {
				e := requireErrcode(t, err)
				assert.Equal(t, errcode.CodeBadRequest, e.Code)
				assert.Equal(t, errcode.RequestIDRequired, e.Reason)
			},
		},
		{
			name:  "unknown remote code is relayed, not masked",
			reply: []byte(`{"code":"upstream_only_code","error":"upstream boom"}`),
			assert: func(t *testing.T, _ []model.ThreadRoomInfo, err error) {
				assert.Equal(t, "upstream boom", requireErrcode(t, err).Message)
			},
		},
		{
			name:  "non-JSON reply surfaces a wrapped decode error",
			reply: []byte(`[`),
			assert: func(t *testing.T, _ []model.ThreadRoomInfo, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "decode thread-room-info response")
			},
		},
		{
			name:  "JSON of the wrong shape surfaces a wrapped decode error",
			reply: []byte(`{"threads":7}`),
			assert: func(t *testing.T, _ []model.ThreadRoomInfo, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "decode thread-room-info response")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nc := startTestNATS(t)
			stubResponder(t, nc, subject.ThreadRoomInfoBatch("site-a"), tc.reply)

			threads, err := New(nc, "site-a").GetThreadRoomInfoBatch(context.Background(), "site-a", []string{"tr1"})
			tc.assert(t, threads, err)
		})
	}
}

func TestClient_GetThreadRoomInfoBatch_EmptyIDs(t *testing.T) {
	nc := startTestNATS(t)
	got := stubResponder(t, nc, subject.ThreadRoomInfoBatch("site-a"), mustJSON(t, model.ThreadRoomInfoBatchResponse{}))

	threads, err := New(nc, "site-a").GetThreadRoomInfoBatch(context.Background(), "site-a", []string{})
	require.NoError(t, err)
	assert.Empty(t, threads)

	var req model.ThreadRoomInfoBatchRequest
	require.NoError(t, json.Unmarshal((<-got).Data, &req))
	assert.Empty(t, req.ThreadRoomIDs)
}

func TestClient_GetThreadRoomInfoBatch_CrossSiteRouting(t *testing.T) {
	nc := startTestNATS(t)
	got := stubResponder(t, nc, subject.ThreadRoomInfoBatch("site-b"), mustJSON(t, model.ThreadRoomInfoBatchResponse{
		Threads: []model.ThreadRoomInfo{{ThreadRoomID: "tr2", Found: true}},
	}))

	threads, err := New(nc, "site-a").GetThreadRoomInfoBatch(context.Background(), "site-b", []string{"tr2"})
	require.NoError(t, err)
	require.Len(t, threads, 1)
	assert.Equal(t, "chat.server.request.room.site-b.thread.info.batch", (<-got).Subject)
}

func TestClient_GetThreadRoomInfoBatch_NoResponder(t *testing.T) {
	nc := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := New(nc, "site-a").GetThreadRoomInfoBatch(ctx, "site-a", []string{"tr1"})
	e := requireErrcode(t, err)
	assert.Equal(t, errcode.CodeUnavailable, e.Code)
	assert.Equal(t, errcode.NatsNoResponders, e.Reason)
	assert.Contains(t, e.Message, "thread-room-info rpc")
}

func TestClient_ClearAllThreadUnread_Subject(t *testing.T) {
	nc := startTestNATS(t)
	got := stubResponder(t, nc, "chat.server.request.room.>", mustJSON(t, model.RoomThreadReadAllResponse{}))

	require.NoError(t, New(nc, "site-a").ClearAllThreadUnread(context.Background(), "site-a", "alice"))

	m := <-got
	assert.Equal(t, "chat.server.request.room.site-a.thread.read.all", m.Subject)
	var req model.RoomThreadReadAllRequest
	require.NoError(t, json.Unmarshal(m.Data, &req))
	assert.Equal(t, "alice", req.Account)
}

func TestClient_ClearAllThreadUnread_ReplyHandling(t *testing.T) {
	tests := []struct {
		name   string
		reply  []byte
		assert func(t *testing.T, err error)
	}{
		{
			name:   "empty ack means success",
			reply:  mustJSON(t, model.RoomThreadReadAllResponse{}),
			assert: func(t *testing.T, err error) { require.NoError(t, err) },
		},
		{
			name:  "errcode envelope is relayed",
			reply: mustJSON(t, errcode.Internal("boom")),
			assert: func(t *testing.T, err error) {
				assert.Equal(t, errcode.CodeInternal, requireErrcode(t, err).Code)
			},
		},
		{
			name:  "unknown remote code is relayed, not masked",
			reply: []byte(`{"code":"upstream_only_code","error":"upstream boom"}`),
			assert: func(t *testing.T, err error) {
				assert.Equal(t, "upstream boom", requireErrcode(t, err).Message)
			},
		},
		{
			name: "non-JSON reply is treated as success",
			// The RPC carries no success payload, so the client only inspects the
			// reply for an error envelope; an undecodable body is not an error.
			reply:  []byte("garbage"),
			assert: func(t *testing.T, err error) { require.NoError(t, err) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nc := startTestNATS(t)
			stubResponder(t, nc, subject.RoomThreadReadAll("site-a"), tc.reply)

			tc.assert(t, New(nc, "site-a").ClearAllThreadUnread(context.Background(), "site-a", "alice"))
		})
	}
}

func TestClient_ClearAllThreadUnread_CrossSiteRouting(t *testing.T) {
	nc := startTestNATS(t)
	got := stubResponder(t, nc, subject.RoomThreadReadAll("site-b"), mustJSON(t, model.RoomThreadReadAllResponse{}))

	require.NoError(t, New(nc, "site-a").ClearAllThreadUnread(context.Background(), "site-b", "alice"))
	assert.Equal(t, "chat.server.request.room.site-b.thread.read.all", (<-got).Subject)
}

func TestClient_ClearAllThreadUnread_NoResponder(t *testing.T) {
	nc := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	e := requireErrcode(t, New(nc, "site-a").ClearAllThreadUnread(ctx, "site-a", "alice"))
	assert.Equal(t, errcode.CodeUnavailable, e.Code)
	assert.Equal(t, errcode.NatsNoResponders, e.Reason)
	assert.Contains(t, e.Message, "clear-all-thread-unread rpc")
}

func TestClient_CreateDMRoom_Subject(t *testing.T) {
	nc := startTestNATS(t)
	got := stubResponder(t, nc, "chat.server.request.room.>", mustJSON(t, model.SyncCreateDMReply{Success: true}))

	// The client's own siteID (not a parameter) selects the create-dm subject.
	_, err := New(nc, "site-a").CreateDMRoom(context.Background(), "alice", "bob", model.RoomTypeDM)
	require.NoError(t, err)

	m := <-got
	assert.Equal(t, "chat.server.request.room.site-a.create.dm", m.Subject)
	var req model.SyncCreateDMRequest
	require.NoError(t, json.Unmarshal(m.Data, &req))
	assert.Equal(t, model.RoomTypeDM, req.RoomType)
	assert.Equal(t, "alice", req.RequesterAccount)
	assert.Equal(t, "bob", req.OtherAccount)
}

func TestClient_CreateDMRoom_ReplyHandling(t *testing.T) {
	tests := []struct {
		name   string
		reply  []byte
		assert func(t *testing.T, sub model.Subscription, err error)
	}{
		{
			name: "happy path returns the subscription",
			reply: mustJSON(t, model.SyncCreateDMReply{
				Success:      true,
				Subscription: model.Subscription{ID: "sub-1", RoomID: "alicebob", SiteID: "site-a", Name: "bob"},
			}),
			assert: func(t *testing.T, sub model.Subscription, err error) {
				require.NoError(t, err)
				assert.Equal(t, "sub-1", sub.ID)
				assert.Equal(t, "alicebob", sub.RoomID)
				assert.Equal(t, "site-a", sub.SiteID)
				assert.Equal(t, "bob", sub.Name)
			},
		},
		{
			name:  "success=false without an envelope becomes an internal error",
			reply: mustJSON(t, model.SyncCreateDMReply{Success: false}),
			assert: func(t *testing.T, sub model.Subscription, err error) {
				e := requireErrcode(t, err)
				assert.Equal(t, errcode.CodeInternal, e.Code)
				assert.Equal(t, "create-dm reported failure", e.Message)
				assert.Equal(t, model.Subscription{}, sub)
			},
		},
		{
			name:  "errcode envelope is relayed with code and reason intact",
			reply: mustJSON(t, errcode.Conflict("DM already exists", errcode.WithReason(errcode.RoomSelfDM))),
			assert: func(t *testing.T, _ model.Subscription, err error) {
				e := requireErrcode(t, err)
				assert.Equal(t, errcode.CodeConflict, e.Code)
				assert.Equal(t, errcode.RoomSelfDM, e.Reason)
			},
		},
		{
			name:  "unknown remote code is relayed, not masked",
			reply: []byte(`{"code":"upstream_only_code","error":"upstream boom"}`),
			assert: func(t *testing.T, _ model.Subscription, err error) {
				assert.Equal(t, "upstream boom", requireErrcode(t, err).Message)
			},
		},
		{
			name:  "non-JSON reply surfaces a wrapped decode error",
			reply: []byte("nope"),
			assert: func(t *testing.T, _ model.Subscription, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "decode create-dm reply")
			},
		},
		{
			name:  "JSON of the wrong shape surfaces a wrapped decode error",
			reply: []byte(`{"success":"yes"}`),
			assert: func(t *testing.T, _ model.Subscription, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "decode create-dm reply")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nc := startTestNATS(t)
			stubResponder(t, nc, subject.RoomCreateDMSync("site-a"), tc.reply)

			sub, err := New(nc, "site-a").CreateDMRoom(context.Background(), "alice", "bob", model.RoomTypeDM)
			tc.assert(t, sub, err)
		})
	}
}

func TestClient_CreateDMRoom_NoResponder(t *testing.T) {
	nc := startTestNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := New(nc, "site-a").CreateDMRoom(ctx, "alice", "bob", model.RoomTypeDM)
	e := requireErrcode(t, err)
	assert.Equal(t, errcode.CodeUnavailable, e.Code)
	assert.Equal(t, errcode.NatsNoResponders, e.Reason)
	assert.Contains(t, e.Message, "create-dm rpc")
}
