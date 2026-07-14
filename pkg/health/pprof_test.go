package health

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// get issues a GET against a health server bound on l and returns the status
// code, closing the body. It targets the in-process listener the test owns.
func getStatus(t *testing.T, base, path string) int {
	t.Helper()
	resp, err := http.Get(base + path) // #nosec G107 -- test requests its own in-process listener URL, not attacker-controlled
	require.NoError(t, err, "path %s", path)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode
}

func TestServeWithPprof_Enabled_ServesProfilingEndpoints(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	base := "http://" + l.Addr().String()

	stop := serveListenerWithOptions(l, time.Second, serverOptions{pprof: true})
	t.Cleanup(func() { _ = stop(context.Background()) })

	// Health endpoints remain available alongside the profiling surface.
	assert.Equal(t, http.StatusOK, getStatus(t, base, "/healthz"))
	// pprof index and a representative named profile are mounted.
	assert.Equal(t, http.StatusOK, getStatus(t, base, "/debug/pprof/"))
	assert.Equal(t, http.StatusOK, getStatus(t, base, "/debug/pprof/goroutine"))
}

func TestServeWithPprof_Disabled_NoProfilingEndpoints(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	base := "http://" + l.Addr().String()

	stop := serveListenerWithOptions(l, time.Second, serverOptions{pprof: false})
	t.Cleanup(func() { _ = stop(context.Background()) })

	// Health still works; pprof must be absent when not enabled.
	assert.Equal(t, http.StatusOK, getStatus(t, base, "/healthz"))
	assert.Equal(t, http.StatusNotFound, getStatus(t, base, "/debug/pprof/"))
}

func TestServe_DoesNotExposePprof(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	base := "http://" + l.Addr().String()

	stop := ServeListener(l, time.Second)
	t.Cleanup(func() { _ = stop(context.Background()) })

	assert.Equal(t, http.StatusNotFound, getStatus(t, base, "/debug/pprof/"))
}

func TestServeWithPprof_PublicWrapper(t *testing.T) {
	stop, err := ServeWithPprof("127.0.0.1:0", time.Second, false)
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))
}

// With pprof mounted the write timeout must be disabled, otherwise a CPU or
// trace profile (which streams for a client-chosen duration, e.g. seconds=30)
// is truncated mid-response by the hardened 10s write timeout.
func TestNewServer_PprofRelaxesWriteTimeout(t *testing.T) {
	withPprof := newServer(time.Second, serverOptions{pprof: true})
	assert.Equal(t, time.Duration(0), withPprof.WriteTimeout)

	// The default (pprof off) server keeps the hardened write timeout.
	hardened := newServer(time.Second, serverOptions{})
	assert.Positive(t, hardened.WriteTimeout)
}

func TestServeWithPprof_PortInUseReturnsError(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	_, err = ServeWithPprof(l.Addr().String(), time.Second, true)
	assert.Error(t, err)
}
