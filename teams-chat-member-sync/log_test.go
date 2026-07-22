package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/msgraph"
)

// recordingHandler captures emitted slog records for assertions.
type recordingHandler struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

//nolint:gocritic // slog.Handler mandates the Record value parameter
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// find returns the attributes of the first record at level whose message
// contains sub.
func (h *recordingHandler) find(level slog.Level, sub string) (map[string]any, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.recs {
		r := h.recs[i]
		if r.Level == level && strings.Contains(r.Message, sub) {
			attrs := make(map[string]any)
			r.Attrs(func(a slog.Attr) bool { attrs[a.Key] = a.Value.Any(); return true })
			return attrs, true
		}
	}
	return nil, false
}

func installRecorder(t *testing.T) *recordingHandler {
	t.Helper()
	rec := &recordingHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return rec
}

func TestRun_LogsBatchWritten(t *testing.T) {
	rec := installRecorder(t)
	s, chats, users, graph := newTestSyncer(t, 1, 500)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:g1", UpdatedAt: wtNow}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:g1").
		Return([]msgraph.ChatMemberDetail{member("u1"), member("u2")}, nil)
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]string{"u1": "alice", "u2": "bob"}, nil)
	chats.EXPECT().SetMembersSyncedBatch(gomock.Any(), gomock.Len(1), wtNow).Return(int64(1), nil)

	require.NoError(t, s.run(context.Background()))

	attrs, ok := rec.find(slog.LevelInfo, "batch written")
	require.True(t, ok, "an info log must record each flushed batch")
	assert.EqualValues(t, 1, attrs["chats"], "the log carries the batch size")
	assert.EqualValues(t, 1, attrs["chatsMatched"])
	assert.EqualValues(t, 0, attrs["chatsSuperseded"])
	assert.EqualValues(t, 2, attrs["members"], "the log carries the member count written")
}

func TestRun_LogsSupersededWarning(t *testing.T) {
	rec := installRecorder(t)
	s, chats, users, graph := newTestSyncer(t, 1, 500)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:g1", UpdatedAt: wtNow}, {ID: "19:g2", UpdatedAt: wtNow}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), gomock.Any()).
		Return([]msgraph.ChatMemberDetail{member("u1")}, nil).Times(2)
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]string{"u1": "a"}, nil).AnyTimes()
	chats.EXPECT().SetMembersSyncedBatch(gomock.Any(), gomock.Len(2), wtNow).Return(int64(1), nil)

	require.NoError(t, s.run(context.Background()))

	attrs, ok := rec.find(slog.LevelWarn, "superseded")
	require.True(t, ok, "a warn log must record superseded chats in the batch")
	assert.EqualValues(t, 1, attrs["chatsSuperseded"], "the log carries how many chats were superseded")
}
