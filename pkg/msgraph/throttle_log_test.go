package msgraph

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// find returns the first captured record at the given level whose message
// contains sub, plus its attributes flattened to a map.
func (h *recordingHandler) find(level slog.Level, sub string) (slog.Record, map[string]string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.recs {
		r := h.recs[i]
		if r.Level == level && strings.Contains(r.Message, sub) {
			attrs := make(map[string]string)
			r.Attrs(func(a slog.Attr) bool { attrs[a.Key] = a.Value.String(); return true })
			return r, attrs, true
		}
	}
	return slog.Record{}, nil, false
}

func installRecorder(t *testing.T) *recordingHandler {
	t.Helper()
	rec := &recordingHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return rec
}

func TestListUserChats_LogsThrottle(t *testing.T) {
	rec := installRecorder(t)
	tokenSrv := newChatsTokenServer(t)
	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0") // keep the test fast
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"value":[]}`))
	}))
	defer graphSrv.Close()

	_, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.NoError(t, err)

	_, attrs, ok := rec.find(slog.LevelWarn, "throttled")
	require.True(t, ok, "a WARN log must be emitted when Graph throttles a chats request")
	assert.Equal(t, "list user chats", attrs["operation"])
	assert.Equal(t, "429", attrs["status"])
	assert.Equal(t, "0", attrs["retryAfter"], "the Retry-After header value is logged")
	assert.Contains(t, attrs, "backoff", "the computed backoff must be logged")
	assert.Contains(t, attrs, "attempt")
}

func TestListChatMembers_LogsThrottle(t *testing.T) {
	rec := installRecorder(t)
	tokenSrv := newChatsTokenServer(t)
	var calls int
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable) // 503 is throttled too
			return
		}
		_, _ = w.Write([]byte(`{"value":[]}`))
	}))
	defer graphSrv.Close()

	c := NewChatMembersClient(
		Config{TenantID: "t", ClientID: "c", ClientSecret: "s"},
		WithTokenURL(tokenSrv.URL), WithBaseURL(graphSrv.URL),
	)
	_, err := c.ListChatMembers(context.Background(), "19:chat1")
	require.NoError(t, err)

	_, attrs, ok := rec.find(slog.LevelWarn, "throttled")
	require.True(t, ok, "a WARN log must be emitted when Graph throttles a members request")
	assert.Equal(t, "list chat members", attrs["operation"])
	assert.Equal(t, "503", attrs["status"])
}

func TestGetThrottled_NoLogOnSuccess(t *testing.T) {
	rec := installRecorder(t)
	tokenSrv := newChatsTokenServer(t)
	graphSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"value":[]}`))
	}))
	defer graphSrv.Close()

	_, err := newTestChats(tokenSrv.URL, graphSrv.URL).
		ListUserChats(context.Background(), "aad-user-1", chatsFrom, chatsTo)
	require.NoError(t, err)

	_, _, ok := rec.find(slog.LevelWarn, "throttled")
	assert.False(t, ok, "no throttle log when the request is not throttled")
}
