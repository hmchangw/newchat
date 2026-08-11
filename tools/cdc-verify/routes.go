package main

import (
	"io/fs"
	"net/http"
)

func (h *handler) registerRoutes(mux *http.ServeMux) {
	staticRoot, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err) // embedded FS layout is fixed at build time
	}
	mux.Handle("GET /", http.FileServer(http.FS(staticRoot)))
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /api/state", h.state)
	mux.HandleFunc("GET /api/events", h.events)
	mux.HandleFunc("GET /api/inspect", h.inspect)
	mux.HandleFunc("GET /api/mapping", h.mapping)
	mux.HandleFunc("GET /failures.json", h.failuresJSON)
}
