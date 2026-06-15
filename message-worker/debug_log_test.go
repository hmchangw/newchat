package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// recordingHandler captures records; mimics a JSON handler pinned at INFO so the
// logctx wrapper performs the sub-INFO gating.
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

func admitRung(rung string) context.Context {
	return logctx.Admit(context.Background(), nats.Header{natsutil.DebugHeader: []string{rung}})
}

func TestHandler_processMessage_DebugBreadcrumbs(t *testing.T) {
	user := &model.User{ID: "u-1", Account: "alice", SiteID: "site-a", EngName: "Alice"}
	mkData := func(content string) []byte {
		evt := model.MessageEvent{
			Message: model.Message{ID: "msg-1", RoomID: "r1", UserID: "u-1", UserAccount: "alice", Content: content},
			SiteID:  "site-a",
		}
		b, _ := json.Marshal(evt)
		return b
	}

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	ts := NewMockThreadStore(ctrl)
	us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil).AnyTimes()
	store.EXPECT().SaveMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site-a").Return(nil).AnyTimes()

	h := NewHandler(store, us, ts, "site-a", func(context.Context, string, []byte, string) error { return nil })

	rec := installRecorder(t)

	t.Run("debug rung emits flow persisted AND debug decision breadcrumbs", func(t *testing.T) {
		rec.reset()
		require.NoError(t, h.processMessage(admitRung("debug"), mkData("hello")))
		assert.True(t, rec.has(logctx.LevelFlow, "message-worker persisted"), "flow breadcrumb present at debug")
		assert.True(t, rec.has(slog.LevelDebug, "message-worker mentions resolved"), "debug decision breadcrumb present")
	})

	t.Run("flow rung emits only the flow breadcrumb", func(t *testing.T) {
		rec.reset()
		require.NoError(t, h.processMessage(admitRung("flow"), mkData("hello")))
		assert.True(t, rec.has(logctx.LevelFlow, "message-worker persisted"))
		assert.False(t, rec.hasLevel(slog.LevelDebug))
	})

	t.Run("unadmitted context emits nothing below INFO", func(t *testing.T) {
		rec.reset()
		require.NoError(t, h.processMessage(context.Background(), mkData("hello")))
		assert.False(t, rec.hasLevel(logctx.LevelFlow))
		assert.False(t, rec.hasLevel(slog.LevelDebug))
	})

	t.Run("breadcrumbs never carry message content", func(t *testing.T) {
		rec.reset()
		require.NoError(t, h.processMessage(admitRung("debug"), mkData("SUPER-SECRET-BODY")))
		rec.mu.Lock()
		defer rec.mu.Unlock()
		for i := range rec.recs {
			assert.NotContains(t, rec.recs[i].Message, "SUPER-SECRET-BODY")
			rec.recs[i].Attrs(func(a slog.Attr) bool {
				assert.NotContains(t, a.Value.String(), "SUPER-SECRET-BODY", "attr %q leaked content", a.Key)
				return true
			})
		}
	})
}

// debugJSMsg is a minimal jetstream.Msg for exercising the full HandleJetStreamMsg
// path (received/persisted flow breadcrumbs incl. stream_wait_ms).
type debugJSMsg struct {
	subject string
	data    []byte
	headers nats.Header
	meta    *jetstream.MsgMetadata
	acked   bool
	naked   bool
}

func (m *debugJSMsg) Metadata() (*jetstream.MsgMetadata, error) { return m.meta, nil }
func (m *debugJSMsg) Data() []byte                              { return m.data }
func (m *debugJSMsg) Headers() nats.Header                      { return m.headers }
func (m *debugJSMsg) Subject() string                           { return m.subject }
func (m *debugJSMsg) Reply() string                             { return "" }
func (m *debugJSMsg) Ack() error                                { m.acked = true; return nil }
func (m *debugJSMsg) DoubleAck(context.Context) error           { m.acked = true; return nil }
func (m *debugJSMsg) Nak() error                                { m.naked = true; return nil }
func (m *debugJSMsg) NakWithDelay(time.Duration) error          { m.naked = true; return nil }
func (m *debugJSMsg) InProgress() error                         { return nil }
func (m *debugJSMsg) Term() error                               { return nil }
func (m *debugJSMsg) TermWithReason(string) error               { return nil }

func TestHandleJetStreamMsg_FlowBreadcrumbs(t *testing.T) {
	user := &model.User{ID: "u-1", Account: "alice", SiteID: "site-a", EngName: "Alice"}
	evt := model.MessageEvent{
		Message: model.Message{ID: "msg-1", RoomID: "r1", UserID: "u-1", UserAccount: "alice", Content: "hello"},
		SiteID:  "site-a",
	}
	data, _ := json.Marshal(evt)

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	ts := NewMockThreadStore(ctrl)
	us.EXPECT().FindUserByID(gomock.Any(), "u-1").Return(user, nil).AnyTimes()
	store.EXPECT().SaveMessage(gomock.Any(), gomock.Any(), gomock.Any(), "site-a").Return(nil).AnyTimes()
	h := NewHandler(store, us, ts, "site-a", func(context.Context, string, []byte, string) error { return nil })

	rec := installRecorder(t)
	msg := &debugJSMsg{
		subject: "chat.msg.canonical.site-a.created",
		data:    data,
		headers: nats.Header{natsutil.DebugHeader: []string{"flow"}},
		meta:    &jetstream.MsgMetadata{Timestamp: time.Now().Add(-95 * time.Millisecond)},
	}
	// Simulate the consumer-entry admission main.go performs.
	ctx := logctx.Admit(context.Background(), msg.Headers())
	h.HandleJetStreamMsg(ctx, msg)

	assert.True(t, msg.acked, "message must be acked on success")
	assert.True(t, rec.has(logctx.LevelFlow, "message-worker received"), "received breadcrumb present")
	assert.True(t, rec.has(logctx.LevelFlow, "message-worker persisted"), "persisted breadcrumb present")
}
