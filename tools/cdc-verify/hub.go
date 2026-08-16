package main

import (
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/hmchangw/chat/pkg/idgen"
)

// sseEvent is one frame pushed to every connected /api/events viewer.
type sseEvent struct {
	Kind string `json:"kind"` // "stats" | "result"
	Data any    `json:"data"` // StreamStats or CheckResult
}

const clientBuffer = 64

// hub is the single global broadcaster — every viewer sees the same pipeline.
// Frames are JSON-encoded once and fanned out as bytes: N viewers, one marshal.
type hub struct {
	mu      sync.RWMutex
	clients map[string]chan []byte
}

func newHub() *hub {
	return &hub{clients: make(map[string]chan []byte)}
}

func (h *hub) register() (string, <-chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := idgen.GenerateID()
	ch := make(chan []byte, clientBuffer)
	h.clients[id] = ch
	return id, ch
}

func (h *hub) unregister(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, id)
}

func (h *hub) clientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// broadcastStats/broadcastResult take payloads by value: StreamStats and
// CheckResult are value-passed package-wide.
//
//nolint:gocritic // see comment above
func (h *hub) broadcastStats(s StreamStats) { h.broadcast(sseEvent{Kind: "stats", Data: s}) }

//nolint:gocritic // see broadcastStats
func (h *hub) broadcastResult(r CheckResult) { h.broadcast(sseEvent{Kind: "result", Data: r}) }

// broadcast never blocks: a viewer that can't drain its buffer is disconnected
// (channel closed, handler returns) — the browser's EventSource reconnects and
// refetches full state on open, so nothing goes silently stale.
func (h *hub) broadcast(ev sseEvent) {
	b, err := json.Marshal(ev)
	if err != nil {
		slog.Error("marshal sse event", "error", err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, ch := range h.clients {
		select {
		case ch <- b:
		default:
			slog.Warn("disconnecting slow sse client", "client", id)
			close(ch)
			delete(h.clients, id)
		}
	}
}
