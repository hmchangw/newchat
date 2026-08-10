package main

import (
	"sync"

	"github.com/hmchangw/chat/pkg/idgen"
)

// sseEvent is one frame pushed to every connected /api/events viewer.
type sseEvent struct {
	Kind string `json:"kind"` // "stats" | "result"
	Data any    `json:"data"` // StreamStats or CheckResult
}

const clientBuffer = 64

// hub is the single global broadcaster: there is no per-session state, so
// every viewer sees the same pipeline of stats and result updates.
type hub struct {
	mu      sync.RWMutex
	clients map[string]chan sseEvent
}

func newHub() *hub {
	return &hub{clients: make(map[string]chan sseEvent)}
}

func (h *hub) register() (string, <-chan sseEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := idgen.GenerateID()
	ch := make(chan sseEvent, clientBuffer)
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

// broadcastStats and broadcastResult take their payload by value: StreamStats
// and CheckResult are passed by value package-wide, and the signature here is
// also fixed by the SSE hub's public contract (see handler_test.go).
//
//nolint:gocritic // see comment above
func (h *hub) broadcastStats(s StreamStats) { h.broadcast(sseEvent{Kind: "stats", Data: s}) }

//nolint:gocritic // see broadcastStats
func (h *hub) broadcastResult(r CheckResult) { h.broadcast(sseEvent{Kind: "result", Data: r}) }

// broadcast never blocks: a slow viewer loses intermediate frames, and the
// next /api/state reload or SSE frame catches it up.
func (h *hub) broadcast(ev sseEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.clients {
		select {
		case ch <- ev:
		default:
		}
	}
}
