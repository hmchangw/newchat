package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

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

func TestHandler_DMFanout_DebugBreadcrumbs(t *testing.T) {
	msgTime := time.Date(2026, 3, 26, 11, 0, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event: model.EventCreated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{ID: "msg-1", RoomID: "dm-1", UserID: "alice-id", UserAccount: "alice", Content: "hey bob", CreatedAt: msgTime},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "dm-1", "msg-1", msgTime, false).Return(nil).AnyTimes()
	store.EXPECT().GetRoomMeta(gomock.Any(), "dm-1").Return(metaOf(testDMRoom), nil).AnyTimes()
	store.EXPECT().ListSubscriptions(gomock.Any(), "dm-1").Return(testDMSubs, nil).AnyTimes()
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil).AnyTimes()

	h := NewHandler(store, us, pub, NewMockRoomKeyProvider(ctrl), false)
	rec := installRecorder(t)

	t.Run("flow rung: fan-out outcome with recipient count, no debug/trace", func(t *testing.T) {
		rec.reset()
		require.NoError(t, h.HandleMessage(admitRung("flow"), data))
		assert.True(t, rec.has(logctx.LevelFlow, "broadcast fan-out"), "flow fan-out outcome present")
		assert.False(t, rec.hasLevel(slog.LevelDebug))
		assert.False(t, rec.hasLevel(logctx.LevelTrace))
	})

	t.Run("debug rung: adds routing decision, still no per-recipient trace", func(t *testing.T) {
		rec.reset()
		require.NoError(t, h.HandleMessage(admitRung("debug"), data))
		assert.True(t, rec.has(logctx.LevelFlow, "broadcast fan-out"))
		assert.True(t, rec.has(slog.LevelDebug, "broadcast routing"))
		assert.False(t, rec.hasLevel(logctx.LevelTrace), "per-recipient lines must stay off at debug")
	})

	t.Run("trace rung: one delivery line per recipient", func(t *testing.T) {
		rec.reset()
		require.NoError(t, h.HandleMessage(admitRung("trace"), data))
		assert.Equal(t, 2, rec.count(logctx.LevelTrace, "broadcast delivered"), "one trace line per DM recipient")
	})

	t.Run("unadmitted: nothing below INFO", func(t *testing.T) {
		rec.reset()
		require.NoError(t, h.HandleMessage(context.Background(), data))
		assert.False(t, rec.hasLevel(logctx.LevelFlow))
		assert.False(t, rec.hasLevel(slog.LevelDebug))
		assert.False(t, rec.hasLevel(logctx.LevelTrace))
	})
}

// The trace rung emits one line per recipient — the highest-cardinality
// emission in the feature — so explicitly lock that the message body never
// reaches any breadcrumb (message or attr).
func TestHandler_DMFanout_NoContentLeak(t *testing.T) {
	const secret = "SUPER-SECRET-BODY-DO-NOT-LOG"
	msgTime := time.Date(2026, 3, 26, 11, 0, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event: model.EventCreated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{ID: "msg-1", RoomID: "dm-1", UserID: "alice-id", UserAccount: "alice", Content: secret, CreatedAt: msgTime},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "dm-1", "msg-1", msgTime, false).Return(nil).AnyTimes()
	store.EXPECT().GetRoomMeta(gomock.Any(), "dm-1").Return(metaOf(testDMRoom), nil).AnyTimes()
	store.EXPECT().ListSubscriptions(gomock.Any(), "dm-1").Return(testDMSubs, nil).AnyTimes()
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil).AnyTimes()
	h := NewHandler(store, us, &mockPublisher{}, NewMockRoomKeyProvider(ctrl), false)

	rec := installRecorder(t)
	require.NoError(t, h.HandleMessage(admitRung("trace"), data)) // most verbose path
	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.NotEmpty(t, rec.recs, "trace run must emit breadcrumbs")
	for i := range rec.recs {
		assert.NotContains(t, rec.recs[i].Message, secret)
		rec.recs[i].Attrs(func(a slog.Attr) bool {
			assert.NotContains(t, a.Value.String(), secret, "attr %q leaked content", a.Key)
			return true
		})
	}
}
