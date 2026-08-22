package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/user-service/config"
)

func TestNewHTTPServer_Tuning(t *testing.T) {
	cfg := &config.HTTPConfig{Port: "8080", WriteTimeout: 35 * time.Second}
	srv := newHTTPServer(":8080", http.NotFoundHandler(), cfg)

	assert.Equal(t, ":8080", srv.Addr)
	assert.Equal(t, 5*time.Second, srv.ReadHeaderTimeout)
	assert.Equal(t, 10*time.Second, srv.ReadTimeout)
	assert.Equal(t, 120*time.Second, srv.IdleTimeout)
	assert.Equal(t, 16<<10, srv.MaxHeaderBytes)
}

// net/http starts the write clock when request headers are read, so the write
// deadline has to outlive the handler budget or a slow page is cut mid-write.
func TestNewHTTPServer_WriteTimeoutOutlivesHandlerBudget(t *testing.T) {
	cfg := &config.HTTPConfig{Port: "8080", HandlerTimeout: 30 * time.Second, WriteTimeout: 35 * time.Second}
	srv := newHTTPServer(":8080", http.NotFoundHandler(), cfg)

	assert.Equal(t, 35*time.Second, srv.WriteTimeout)
	assert.Greater(t, srv.WriteTimeout, cfg.HandlerTimeout)
}
