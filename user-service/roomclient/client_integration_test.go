//go:build integration

package roomclient

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/user-service/service"
)

// Compile-time assertion: `go vet -tags integration` fails if Client drifts from RoomClient.
var _ service.RoomClient = (*Client)(nil)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// dial returns a connected *o11ynats.Conn backed by the shared test NATS
// server. The connection is drained on test cleanup.
func dial(t *testing.T) *o11ynats.Conn {
	t.Helper()
	nc, err := o11ynats.Connect(context.Background(), testutil.NATS(t), noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })
	return nc
}

func TestGetRoomsInfo_Integration(t *testing.T) {
	t.Run("happy path — returns rooms from responder", func(t *testing.T) {
		nc := dial(t)

		// Stand up a stub responder on the exact subject the client should publish on.
		sub, err := nc.Subscribe(context.Background(), subject.RoomsInfoBatch("site-a"), func(_ context.Context, m *nats.Msg) {
			out, _ := json.Marshal(model.RoomsInfoBatchResponse{
				Rooms: []model.RoomInfo{{RoomID: "r1", Found: true, Name: "Eng"}},
			})
			_ = m.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		rooms, err := New(nc, "site-a").GetRoomsInfo(context.Background(), "site-a", []string{"r1"})
		require.NoError(t, err)
		require.Len(t, rooms, 1)
		require.Equal(t, "Eng", rooms[0].Name)
	})

	t.Run("errcode reply — returns typed errcode error", func(t *testing.T) {
		nc := dial(t)

		sub, err := nc.Subscribe(context.Background(), subject.RoomsInfoBatch("site-a"), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.NotFound("room not found"))
			_ = m.Respond(data)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc, "site-a").GetRoomsInfo(context.Background(), "site-a", []string{"r1"})
		require.Error(t, err)
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, errcode.CodeNotFound, e.Code)
	})

	t.Run("no responder — returns error wrapping rooms-info rpc", func(t *testing.T) {
		nc := dial(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := New(nc, "site-a").GetRoomsInfo(ctx, "site-a", []string{"r1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rooms-info rpc")
	})

	t.Run("cross-site siteID routing — uses siteID param not c.siteID", func(t *testing.T) {
		nc := dial(t)

		// Responder on "site-b" subject proves the method routes on siteID param, not c.siteID.
		sub, err := nc.Subscribe(context.Background(), subject.RoomsInfoBatch("site-b"), func(_ context.Context, m *nats.Msg) {
			out, _ := json.Marshal(model.RoomsInfoBatchResponse{
				Rooms: []model.RoomInfo{{RoomID: "r2", Found: true, Name: "Remote"}},
			})
			_ = m.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		rooms, err := New(nc, "site-a").GetRoomsInfo(context.Background(), "site-b", []string{"r2"})
		require.NoError(t, err)
		require.Len(t, rooms, 1)
		assert.Equal(t, "Remote", rooms[0].Name)
	})

	t.Run("unknown-code error envelope — relayed, not masked", func(t *testing.T) {
		nc := dial(t)

		// A well-formed error envelope whose code is outside our closed set must be
		// relayed, not silently re-decoded as an empty success.
		sub, err := nc.Subscribe(context.Background(), subject.RoomsInfoBatch("site-a"), func(_ context.Context, m *nats.Msg) {
			_ = m.Respond([]byte(`{"code":"upstream_only_code","error":"upstream boom"}`))
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc, "site-a").GetRoomsInfo(context.Background(), "site-a", []string{"r1"})
		require.Error(t, err)
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, "upstream boom", e.Message)
	})
}

func TestGetThreadRoomInfoBatch_Integration(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		nc := dial(t)
		sub, err := nc.Subscribe(subject.ThreadRoomInfoBatch("site-a"), func(m otelnats.Msg) {
			out, _ := json.Marshal(model.ThreadRoomInfoBatchResponse{
				Threads: []model.ThreadRoomInfo{{ThreadRoomID: "tr1", Found: true, LastMsgAt: 42}},
			})
			_ = m.Msg.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		got, err := New(nc, "site-a").GetThreadRoomInfoBatch(context.Background(), "site-a", []string{"tr1"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, int64(42), got[0].LastMsgAt)
	})

	t.Run("errcode reply relayed", func(t *testing.T) {
		nc := dial(t)
		sub, err := nc.Subscribe(subject.ThreadRoomInfoBatch("site-a"), func(m otelnats.Msg) {
			data, _ := json.Marshal(errcode.BadRequest("bad"))
			_ = m.Msg.Respond(data)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc, "site-a").GetThreadRoomInfoBatch(context.Background(), "site-a", []string{"tr1"})
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, errcode.CodeBadRequest, e.Code)
	})

	t.Run("no responder — returns error wrapping thread-room-info rpc", func(t *testing.T) {
		nc := dial(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := New(nc, "site-a").GetThreadRoomInfoBatch(ctx, "site-a", []string{"tr1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "thread-room-info rpc")
	})

	t.Run("cross-site siteID routing — uses siteID param not c.siteID", func(t *testing.T) {
		nc := dial(t)

		// Responder on "site-b" subject proves the method routes on siteID param, not c.siteID.
		sub, err := nc.Subscribe(subject.ThreadRoomInfoBatch("site-b"), func(m otelnats.Msg) {
			out, _ := json.Marshal(model.ThreadRoomInfoBatchResponse{
				Threads: []model.ThreadRoomInfo{{ThreadRoomID: "tr2", Found: true, LastMsgAt: 99}},
			})
			_ = m.Msg.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		got, err := New(nc, "site-a").GetThreadRoomInfoBatch(context.Background(), "site-b", []string{"tr2"})
		require.NoError(t, err)
		require.Len(t, got, 1)
	})
}

func TestCreateDMRoom_Integration(t *testing.T) {
	t.Run("happy path — returns subscription from responder", func(t *testing.T) {
		nc := dial(t)

		// Stand up a stub responder on the exact subject the client should publish on.
		sub, err := nc.Subscribe(context.Background(), subject.RoomCreateDMSync("site-a"), func(_ context.Context, m *nats.Msg) {
			out, _ := json.Marshal(model.SyncCreateDMReply{
				Success:      true,
				Subscription: model.Subscription{ID: "new"},
			})
			_ = m.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		sub2, err := New(nc, "site-a").CreateDMRoom(context.Background(), "alice", "bob", model.RoomTypeDM)
		require.NoError(t, err)
		require.Equal(t, "new", sub2.ID)
	})

	t.Run("errcode reply — returns typed errcode error", func(t *testing.T) {
		nc := dial(t)

		sub, err := nc.Subscribe(context.Background(), subject.RoomCreateDMSync("site-a"), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.Conflict("DM room already exists"))
			_ = m.Respond(data)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc, "site-a").CreateDMRoom(context.Background(), "alice", "bob", model.RoomTypeDM)
		require.Error(t, err)
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, errcode.CodeConflict, e.Code)
	})

	t.Run("no responder — returns error wrapping create-dm rpc", func(t *testing.T) {
		nc := dial(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := New(nc, "site-a").CreateDMRoom(ctx, "alice", "bob", model.RoomTypeDM)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create-dm rpc")
	})

	t.Run("success=false reply — returns error", func(t *testing.T) {
		nc := dial(t)
		sub, err := nc.Subscribe(context.Background(), subject.RoomCreateDMSync("site-a"), func(_ context.Context, m *nats.Msg) {
			out, _ := json.Marshal(model.SyncCreateDMReply{Success: false}) // explicit not-success, no errcode envelope
			_ = m.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc, "site-a").CreateDMRoom(context.Background(), "alice", "bob", model.RoomTypeDM)
		require.Error(t, err)
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, errcode.CodeInternal, e.Code)
	})

	t.Run("unknown-code error envelope — relayed, not masked", func(t *testing.T) {
		nc := dial(t)

		// A foreign-code error envelope must surface as the original error rather
		// than collapse to the generic create-dm-failure backstop.
		sub, err := nc.Subscribe(context.Background(), subject.RoomCreateDMSync("site-a"), func(_ context.Context, m *nats.Msg) {
			_ = m.Respond([]byte(`{"code":"upstream_only_code","error":"upstream boom"}`))
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc, "site-a").CreateDMRoom(context.Background(), "alice", "bob", model.RoomTypeDM)
		require.Error(t, err)
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, "upstream boom", e.Message)
	})
}
