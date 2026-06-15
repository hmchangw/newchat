package main

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
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
func (h *recordingHandler) has(l slog.Level, msg string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.recs {
		if h.recs[i].Level == l && h.recs[i].Message == msg {
			return true
		}
	}
	return false
}

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

// admittedCtx builds a natsrouter.Context whose underlying ctx has been admitted
// at the given rung — the state the RequestID middleware leaves for handlers.
func admittedCtx(rung string, params map[string]string) *natsrouter.Context {
	c := natsrouter.NewContext(params)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	if rung != "" {
		ctx = logctx.Admit(ctx, nats.Header{natsutil.DebugHeader: []string{rung}})
	}
	c.SetContext(ctx)
	return c
}

func TestCreateRoom_DebugBreadcrumbs(t *testing.T) {
	newHandler := func(t *testing.T) *Handler {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil).AnyTimes()
		store.EXPECT().GetUser(gomock.Any(), "bob").Return(bobUser(), nil).AnyTimes()
		store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "bob").Return(nil, model.ErrSubscriptionNotFound).AnyTimes()
		return &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
			publishToStream: func(context.Context, string, []byte, string) error { return nil }}
	}
	req := func() model.CreateRoomRequest { return model.CreateRoomRequest{Users: []string{"bob"}} }

	rec := installRecorder(t)

	t.Run("flow rung: emits the cross-service handoff breadcrumb", func(t *testing.T) {
		rec.reset()
		_, err := newHandler(t).createRoom(admittedCtx("flow", map[string]string{"account": "alice"}), req())
		require.NoError(t, err)
		assert.True(t, rec.has(logctx.LevelFlow, "room-service create handoff"), "flow handoff present")
		assert.False(t, rec.hasLevel(slog.LevelDebug))
	})

	t.Run("debug rung: adds the classification decision", func(t *testing.T) {
		rec.reset()
		_, err := newHandler(t).createRoom(admittedCtx("debug", map[string]string{"account": "alice"}), req())
		require.NoError(t, err)
		assert.True(t, rec.has(logctx.LevelFlow, "room-service create handoff"))
		assert.True(t, rec.has(slog.LevelDebug, "room-service createRoom classified"))
	})

	t.Run("unadmitted: nothing below INFO", func(t *testing.T) {
		rec.reset()
		_, err := newHandler(t).createRoom(admittedCtx("", map[string]string{"account": "alice"}), req())
		require.NoError(t, err)
		assert.False(t, rec.hasLevel(logctx.LevelFlow))
		assert.False(t, rec.hasLevel(slog.LevelDebug))
	})
}
