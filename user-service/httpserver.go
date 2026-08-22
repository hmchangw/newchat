package main

import (
	"net/http"
	"time"

	"github.com/hmchangw/chat/user-service/config"
)

// newHTTPServer applies the listener tuning: bounded header reads, an idle
// window long enough for a desktop client to reuse connections, and a write
// deadline that outlives the handler budget. net/http starts that deadline when
// the request headers are read, so an equal value would cut a slow page
// mid-write. Config validation enforces the ordering.
func newHTTPServer(addr string, h http.Handler, cfg *config.HTTPConfig) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}
