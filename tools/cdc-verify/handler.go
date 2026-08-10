package main

import (
	"encoding/json"
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

type handler struct {
	hub     *hub
	results stateProvider
	stats   statsProvider
}

func newHandler(hub *hub, results stateProvider, stats statsProvider) *handler {
	return &handler{hub: hub, results: results, stats: stats}
}

func (h *handler) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *handler) state(w http.ResponseWriter, _ *http.Request) {
	recent, failures, counters := h.results.Snapshot()
	writeJSON(w, map[string]any{
		"stats":    h.stats.Last(),
		"recent":   recent,
		"failures": failures,
		"counters": counters,
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
		case ev := <-ch:
			b, err := json.Marshal(ev)
			if err != nil {
				slog.Error("marshal sse event", "error", err)
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", b)
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
