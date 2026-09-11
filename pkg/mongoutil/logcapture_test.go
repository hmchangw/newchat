package mongoutil

import (
	"context"
	"log/slog"
	"sync"
	"testing"
)

type logEntry struct {
	level slog.Level
	msg   string
	index string
}

// recordHandler captures each slog record's level, message and index attribute.
type recordHandler struct {
	mu      sync.Mutex
	entries []logEntry
}

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordHandler) Handle(_ context.Context, r slog.Record) error { //nolint:gocritic // slog.Handler's signature
	e := logEntry{level: r.Level, msg: r.Message}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "index" {
			e.index = a.Value.String()
		}
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, e)
	return nil
}
func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordHandler) WithGroup(string) slog.Handler      { return h }

// captureLogs installs a recordHandler as the default logger for the test.
func captureLogs(t *testing.T) *recordHandler {
	t.Helper()
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })
	h := &recordHandler{}
	slog.SetDefault(slog.New(h))
	return h
}

// at returns the captured entries logged at exactly level.
func (h *recordHandler) at(level slog.Level) []logEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []logEntry
	for _, e := range h.entries {
		if e.level == level {
			out = append(out, e)
		}
	}
	return out
}
