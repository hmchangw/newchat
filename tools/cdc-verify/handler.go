package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type stateProvider interface {
	Snapshot() ([]CheckResult, []CheckResult, Counters)
}

type statsProvider interface {
	Last() StreamStats
}

// docInspector fetches the live source + destination view of one document —
// satisfied by *verifier.
type docInspector interface {
	Inspect(ctx context.Context, collection, docID string) (InspectResult, error)
}

type handler struct {
	hub       *hub
	results   stateProvider
	stats     statsProvider
	recentCap int
	pairs     []TargetPair
	inspector docInspector
}

func newHandler(hub *hub, results stateProvider, stats statsProvider, recentCap int,
	pairs []TargetPair, inspector docInspector,
) *handler {
	return &handler{hub: hub, results: results, stats: stats, recentCap: recentCap,
		pairs: pairs, inspector: inspector}
}

// inspect serves the live source/destination view for one document. Reads go
// straight to the stores at request time — nothing is cached from past checks.
func (h *handler) inspect(w http.ResponseWriter, r *http.Request) {
	collection := r.URL.Query().Get("collection")
	docID := r.URL.Query().Get("doc")
	if collection == "" || docID == "" {
		http.Error(w, "collection and doc query params are required", http.StatusBadRequest)
		return
	}
	res, err := h.inspector.Inspect(r.Context(), collection, docID)
	switch {
	case errors.Is(err, errUnmapped):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, res)
}

func (h *handler) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *handler) state(w http.ResponseWriter, _ *http.Request) {
	recent, failures, counters := h.results.Snapshot()
	writeJSON(w, map[string]any{
		"stats":     h.stats.Last(),
		"recent":    recent,
		"failures":  failures,
		"counters":  counters,
		"recentCap": h.recentCap,
		"pairs":     h.pairs,
	})
}

func (h *handler) failuresJSON(w http.ResponseWriter, _ *http.Request) {
	_, failures, _ := h.results.Snapshot()
	w.Header().Set("Content-Disposition", `attachment; filename=cdc-verify-failures.json`)
	writeJSON(w, failures)
}

func (h *handler) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	id, ch := h.hub.register()
	defer h.hub.unregister(id)
	flusher.Flush()

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case frame := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", frame)
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write json response", "error", err)
	}
}
