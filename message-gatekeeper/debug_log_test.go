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

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roommetacache"
)

// recordingHandler captures every record handed to it; it mimics a production
// JSON handler pinned at INFO so the logctx wrapper does the sub-INFO gating.
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

// installRecorder routes slog through a logctx-wrapped recorder and a permissive
// limiter for the duration of the test.
func installRecorder(t *testing.T) *recordingHandler {
	t.Helper()
	rec := &recordingHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(logctx.NewHandler(rec)))
	logctx.Configure(logctx.Config{Rate: 1e6, Burst: 1 << 20})
	t.Cleanup(func() {
		slog.SetDefault(prev)
		logctx.Configure(logctx.Config{Rate: 0, Burst: 0}) // deny for sibling tests
	})
	return rec
}

func admitRung(rung string) context.Context {
	return logctx.Admit(context.Background(), nats.Header{natsutil.DebugHeader: []string{rung}})
}

func TestHandler_processMessage_DebugBreadcrumbs(t *testing.T) {
	const (
		account = "alice"
		roomID  = "room-1"
		siteID  = "site-a"
		reqID   = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	)
	sub := &model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: account},
		RoomID: roomID,
		Roles:  []model.Role{model.RoleMember},
	}

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), account, roomID).Return(sub, nil).AnyTimes()
	store.EXPECT().GetRoomMeta(gomock.Any(), roomID).Return(roommetacache.Meta{ID: roomID, UserCount: 1}, nil).AnyTimes()

	h := NewHandler(store, nil, makePublishFunc(nil, nil), func(context.Context, *nats.Msg) error { return nil }, siteID, nil, 500)

	newReq := func() model.SendMessageRequest {
		return model.SendMessageRequest{ID: idgen.GenerateMessageID(), Content: "hello world", RequestID: reqID}
	}

	rec := installRecorder(t)

	t.Run("debug rung emits flow published AND debug decision breadcrumbs", func(t *testing.T) {
		rec.reset()
		req := newReq()
		_, err := h.processMessage(admitRung("debug"), account, roomID, siteID, &req)
		require.NoError(t, err)
		assert.True(t, rec.has(logctx.LevelFlow, "gatekeeper published to canonical"), "flow breadcrumb present at debug (cumulative)")
		assert.True(t, rec.has(slog.LevelDebug, "gatekeeper subscription resolved"), "debug decision breadcrumb present")
	})

	t.Run("flow rung emits the flow breadcrumb but no debug lines", func(t *testing.T) {
		rec.reset()
		req := newReq()
		_, err := h.processMessage(admitRung("flow"), account, roomID, siteID, &req)
		require.NoError(t, err)
		assert.True(t, rec.has(logctx.LevelFlow, "gatekeeper published to canonical"))
		assert.False(t, rec.hasLevel(slog.LevelDebug), "debug lines suppressed at flow rung")
	})

	t.Run("unadmitted context emits nothing below INFO", func(t *testing.T) {
		rec.reset()
		req := newReq()
		_, err := h.processMessage(context.Background(), account, roomID, siteID, &req)
		require.NoError(t, err)
		assert.False(t, rec.hasLevel(logctx.LevelFlow))
		assert.False(t, rec.hasLevel(slog.LevelDebug))
	})

	t.Run("breadcrumbs never carry message content", func(t *testing.T) {
		rec.reset()
		req := newReq()
		req.Content = "SUPER-SECRET-BODY-DO-NOT-LOG"
		_, err := h.processMessage(admitRung("debug"), account, roomID, siteID, &req)
		require.NoError(t, err)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		for i := range rec.recs {
			buf, err := json.Marshal(attrsOf(&rec.recs[i]))
			require.NoError(t, err)
			assert.NotContains(t, string(buf), "SUPER-SECRET-BODY", "breadcrumbs must be metadata-only")
			assert.NotContains(t, rec.recs[i].Message, "SUPER-SECRET-BODY")
		}
	})
}

// attrsOf collects a record's attributes into a flat map for content-safety assertions.
func attrsOf(r *slog.Record) map[string]any {
	m := map[string]any{}
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.Any()
		return true
	})
	return m
}
