//go:build integration

package main

import (
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

// TestRun_GracefulLifecycle boots the full service wiring against the shared
// test Mongo (cron scheduled but never firing, no Graph traffic), waits for
// the health listener, then drives the SIGTERM graceful-shutdown path.
func TestRun_GracefulLifecycle(t *testing.T) {
	uri := testutil.MongoURI(t)
	setRequiredEnv(t)
	t.Setenv("MONGO_READ_URI", uri)
	t.Setenv("MONGO_WRITE_URI", uri)
	t.Setenv("HEALTH_ADDR", "127.0.0.1:18099")
	t.Setenv("SYNC_CRON", "0 2 * * *")
	t.Setenv("RUN_ON_START", "false")

	// Subscribing a guard channel to SIGTERM disables the default terminate
	// action process-wide, so the kill below can never take down the test
	// binary even if it lands before run() registers its own handler.
	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGTERM)
	defer signal.Stop(guard)

	done := make(chan error, 1)
	go func() { done <- run() }()

	// The health listener is the last dependency started before run blocks in
	// shutdown.Wait, so a serving /healthz means wiring completed.
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://127.0.0.1:18099/healthz")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 100*time.Millisecond, "health listener never came up")

	// Readiness must see both Mongo clients.
	resp, err := http.Get("http://127.0.0.1:18099/readyz")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Re-send until run() observes it, closing the startup race between the
	// health listener and shutdown.Wait's own signal registration.
	deadline := time.After(30 * time.Second)
	for {
		require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))
		select {
		case err := <-done:
			require.NoError(t, err)
			return
		case <-deadline:
			t.Fatal("run did not shut down after SIGTERM")
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// TestRun_InvalidCronFailsAfterConnect exercises the startup error path where
// both Mongo clients are already connected: run must return the registration
// error (after disconnecting both clients) instead of hanging.
func TestRun_InvalidCronFailsAfterConnect(t *testing.T) {
	uri := testutil.MongoURI(t)
	setRequiredEnv(t)
	t.Setenv("MONGO_READ_URI", uri)
	t.Setenv("MONGO_WRITE_URI", uri)
	t.Setenv("SYNC_CRON", "not a cron")

	err := run()
	require.ErrorContains(t, err, "register sync cron")
}
