package main

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/user-service/config"
)

func TestNewHTTPServer_Tuning(t *testing.T) {
	cfg := &config.HTTPConfig{Port: "8080", HandlerTimeout: 30 * time.Second, WriteTimeout: 35 * time.Second}
	srv := newHTTPServer(":8080", http.NotFoundHandler(), cfg)

	assert.Equal(t, ":8080", srv.Addr)
	assert.Equal(t, 5*time.Second, srv.ReadHeaderTimeout)
	assert.Equal(t, 10*time.Second, srv.ReadTimeout)
	assert.Equal(t, 120*time.Second, srv.IdleTimeout)
	// net/http starts the write clock at header read, so this must outlive the
	// handler budget or a slow page is cut mid-write. config.Load enforces the ordering.
	assert.Equal(t, 35*time.Second, srv.WriteTimeout)
	assert.Equal(t, 16<<10, srv.MaxHeaderBytes)
}

// Readiness must fail as soon as the drain starts while liveness keeps answering:
// stopping the health server outright would fail liveness too and invite a
// SIGKILL mid-drain.
func TestDrainingCheck_FailsReadinessOnlyWhileDraining(t *testing.T) {
	var draining atomic.Bool
	check := drainingCheck(&draining)

	require.NoError(t, check.Probe(context.Background()), "ready before shutdown")

	draining.Store(true)
	assert.Error(t, check.Probe(context.Background()), "not ready once draining")
}
