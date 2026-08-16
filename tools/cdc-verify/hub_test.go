package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHub_RegisterUnregister(t *testing.T) {
	h := newHub()
	assert.Equal(t, 0, h.clientCount())

	id, ch := h.register()
	require.NotEmpty(t, id)
	require.NotNil(t, ch)
	assert.Equal(t, 1, h.clientCount())

	h.unregister(id)
	assert.Equal(t, 0, h.clientCount())
}

func TestHub_Broadcast_DeliversToClient(t *testing.T) {
	h := newHub()
	_, ch := h.register()

	h.broadcastStats(StreamStats{Stream: "S"})
	b := <-ch
	assert.Contains(t, string(b), `"kind":"stats"`)
}

// A client whose channel is full must not block the broadcast — and must not
// silently lose frames either: the hub disconnects it (closed channel), so the
// SSE handler returns and the browser reconnects with a full state refetch.
func TestHub_Broadcast_DisconnectsSlowClient(t *testing.T) {
	h := newHub()
	slow := make(chan []byte) // unbuffered, no reader → always full
	h.mu.Lock()
	h.clients["slow"] = slow
	h.mu.Unlock()

	done := make(chan struct{})
	go func() {
		h.broadcastResult(CheckResult{ID: "r1"})
		close(done)
	}()
	<-done // returns only if broadcast didn't block on the full channel
	assert.Equal(t, 0, h.clientCount(), "the slow client is removed")
	_, open := <-slow
	assert.False(t, open, "the slow client's channel is closed so its handler returns")

	// A healthy client registered afterwards still receives frames.
	_, ch := h.register()
	h.broadcastResult(CheckResult{ID: "r2"})
	b, open := <-ch
	require.True(t, open)
	assert.Contains(t, string(b), `"r2"`)
}

// A payload that cannot be JSON-marshaled hits the error branch and returns
// without fanning anything out.
func TestHub_Broadcast_MarshalError(t *testing.T) {
	h := newHub()
	_, ch := h.register()

	h.broadcast(sseEvent{Kind: "bad", Data: make(chan int)})

	select {
	case b := <-ch:
		t.Fatalf("expected no frame on marshal error, got %q", b)
	default:
	}
}
