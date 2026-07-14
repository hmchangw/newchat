package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

type recordingHandler struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *recordingHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelInfo }

//nolint:gocritic // slog.Handler mandates the Record value parameter
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = nil
}
func (h *recordingHandler) hasLevel(l slog.Level) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.recs {
		if h.recs[i].Level == l {
			return true
		}
	}
	return false
}
func (h *recordingHandler) count(l slog.Level, msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for i := range h.recs {
		if h.recs[i].Level == l && h.recs[i].Message == msg {
			n++
		}
	}
	return n
}
func (h *recordingHandler) has(l slog.Level, msg string) bool { return h.count(l, msg) > 0 }

func installRecorder(t *testing.T) *recordingHandler {
	t.Helper()
	rec := &recordingHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(logctx.NewHandler(rec)))
	logctx.Configure(logctx.Config{Rate: 1e6, Burst: 1 << 20})
	t.Cleanup(func() {
		slog.SetDefault(prev)
		logctx.Configure(logctx.Config{Rate: 0, Burst: 0})
	})
	return rec
}

func admitRung(rung string) context.Context {
	return logctx.Admit(context.Background(), nats.Header{natsutil.DebugHeader: []string{rung}})
}

func TestPublishSubscriptionUpdates_TraceFanout(t *testing.T) {
	h := &Handler{siteID: "site-a", publish: func(context.Context, string, []byte, string) error { return nil }}
	subs := []*model.Subscription{
		{User: model.SubscriptionUser{ID: "u1", Account: "alice"}},
		{User: model.SubscriptionUser{ID: "u2", Account: "bob"}},
	}
	users := []*model.User{
		{ID: "u1", Account: "alice"},
		{ID: "u2", Account: "bob"},
	}
	rec := installRecorder(t)

	t.Run("trace: one delivery line per subscriber + the flow count", func(t *testing.T) {
		rec.reset()
		h.publishSubscriptionUpdates(admitRung("trace"), subs, users, "req-1")
		assert.Equal(t, 2, rec.count(logctx.LevelTrace, "room-worker subscription delivered"))
		assert.True(t, rec.has(logctx.LevelFlow, "room-worker subscription fan-out"))
	})

	t.Run("flow: count only, no per-subscriber lines", func(t *testing.T) {
		rec.reset()
		h.publishSubscriptionUpdates(admitRung("flow"), subs, users, "req-1")
		assert.True(t, rec.has(logctx.LevelFlow, "room-worker subscription fan-out"))
		assert.False(t, rec.hasLevel(logctx.LevelTrace))
	})

	t.Run("unadmitted: nothing", func(t *testing.T) {
		rec.reset()
		h.publishSubscriptionUpdates(context.Background(), subs, users, "req-1")
		assert.False(t, rec.hasLevel(logctx.LevelFlow))
		assert.False(t, rec.hasLevel(logctx.LevelTrace))
	})
}

func TestPublishAsyncJobResult_FlowBreadcrumb(t *testing.T) {
	h := &Handler{siteID: "site-a", publish: func(context.Context, string, []byte, string) error { return nil }}
	rec := installRecorder(t)

	ctx := natsutil.WithRequestID(admitRung("flow"), "req-1")
	h.publishAsyncJobResult(ctx, "alice", "room.create", "room-1", nil)
	assert.True(t, rec.has(logctx.LevelFlow, "room-worker async result"), "two-phase async result emits a flow terminal")
}

// room-worker's debug rung must carry decision detail, not jump flow->trace.
func TestProcessRemoveMember_DebugEdge(t *testing.T) {
	const roomID, account, siteID = "room-1", "alice", "site-a"
	setup := func(t *testing.T) (*Handler, []byte) {
		ctrl := gomock.NewController(t)
		store := NewMockSubscriptionStore(ctrl)
		userResult := &UserWithMembership{User: model.User{ID: "u1", Account: account, SiteID: siteID, EngName: "Alice", ChineseName: "愛麗絲"}}
		store.EXPECT().GetUserWithMembership(gomock.Any(), roomID, account).Return(userResult, nil).AnyTimes()
		store.EXPECT().DeleteSubscription(gomock.Any(), roomID, account).Return(int64(1), nil).AnyTimes()
		store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u1").Return(nil).AnyTimes()
		store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil).AnyTimes()
		store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return(nil, nil).AnyTimes()
		h := NewHandler(store, siteID, func(context.Context, string, []byte, string) error { return nil }, testKeyStore, testKeySender)
		req := model.RemoveMemberRequest{RoomID: roomID, Requester: account, Account: account, Timestamp: 1, RoomType: model.RoomTypeChannel}
		data, _ := json.Marshal(req)
		return h, data
	}

	t.Run("debug rung emits the remove-member decision edge", func(t *testing.T) {
		h, data := setup(t)
		rec := installRecorder(t)
		require.NoError(t, h.processRemoveMember(admitRung("debug"), data))
		assert.True(t, rec.has(slog.LevelDebug, "room-worker remove member"), "debug edge present")
	})

	t.Run("flow rung does not emit the debug edge", func(t *testing.T) {
		h, data := setup(t)
		rec := installRecorder(t)
		require.NoError(t, h.processRemoveMember(admitRung("flow"), data))
		assert.False(t, rec.hasLevel(slog.LevelDebug), "no debug edge below debug rung")
	})
}
