package main

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A soak pod that is being OOM-killed cannot be port-forwarded in time, so the
// profile has to already be on disk when it dies. The writer keeps a bounded
// number of files so a long run cannot fill the ledger volume.
func TestSoakHeapProfiler_WritesAProfileOnEveryTick(t *testing.T) {
	dir := t.TempDir()
	profiler, err := newSoakHeapProfiler(soakHeapProfileConfig{Dir: dir, Keep: 3})
	require.NoError(t, err)

	require.NoError(t, profiler.WriteProfile())
	require.NoError(t, profiler.WriteProfile())

	assert.Len(t, soakProfileFiles(t, dir), 2)
}

func TestSoakHeapProfiler_KeepsOnlyTheConfiguredNumberOfProfiles(t *testing.T) {
	dir := t.TempDir()
	profiler, err := newSoakHeapProfiler(soakHeapProfileConfig{Dir: dir, Keep: 2})
	require.NoError(t, err)

	for range 5 {
		require.NoError(t, profiler.WriteProfile())
	}

	assert.Len(t, soakProfileFiles(t, dir), 2,
		"a long run must not fill the volume with profiles")
}

func TestSoakHeapProfiler_IsDisabledWithoutADirectory(t *testing.T) {
	profiler, err := newSoakHeapProfiler(soakHeapProfileConfig{Keep: 3})

	require.NoError(t, err)
	assert.Nil(t, profiler, "no directory configured means no profiling")
}

func TestSoakHeapProfiler_RunWritesUntilTheContextEnds(t *testing.T) {
	dir := t.TempDir()
	profiler, err := newSoakHeapProfiler(soakHeapProfileConfig{Dir: dir, Keep: 4})
	require.NoError(t, err)

	ticks := make(chan time.Time, 2)
	ticks <- time.Unix(0, 0)
	ticks <- time.Unix(1, 0)
	close(ticks)

	profiler.Run(context.Background(), ticks)

	assert.Len(t, soakProfileFiles(t, dir), 2)
}

// The metrics endpoint is scraped by Prometheus; profiling lives on its own
// listener so enabling it never widens what the scrape target exposes.
func TestStartSoakPProfServer_IsOffByDefault(t *testing.T) {
	server, err := startSoakPProfServer("")

	require.NoError(t, err)
	assert.Nil(t, server)
}

func TestStartSoakPProfServer_ServesTheHeapProfile(t *testing.T) {
	server, err := startSoakPProfServer("127.0.0.1:0")
	require.NoError(t, err)
	require.NotNil(t, server)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	require.NotEmpty(t, server.Addr, "the listener must be bound before Serve starts")

	response, err := http.Get("http://" + server.Addr + "/debug/pprof/heap?debug=1")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, response.Body.Close()) })

	assert.Equal(t, http.StatusOK, response.StatusCode)
}

func soakProfileFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.pprof"))
	require.NoError(t, err)
	return matches
}

// The profiles exist so a pod that is being OOM-killed leaves evidence behind,
// which means the process that restarts must not treat the survivors as newer
// than what it writes. A counter that restarts at zero makes prune keep the
// stale pre-restart profiles and delete every fresh one — exactly inverting
// what the volume is for.
func TestSoakHeapProfiler_KeepsWritingPastTheProfilesItInherited(t *testing.T) {
	dir := t.TempDir()
	before, err := newSoakHeapProfiler(soakHeapProfileConfig{Dir: dir, Keep: 3})
	require.NoError(t, err)
	for range 5 {
		require.NoError(t, before.WriteProfile())
	}
	inherited := soakProfileFiles(t, dir)
	require.Len(t, inherited, 3)

	restarted, err := newSoakHeapProfiler(soakHeapProfileConfig{Dir: dir, Keep: 3})
	require.NoError(t, err)
	require.NoError(t, restarted.WriteProfile())

	after := soakProfileFiles(t, dir)
	require.Len(t, after, 3)
	assert.NotEqual(t, inherited, after,
		"the restarted process must keep its own profile, not delete it in favour of stale ones")
	assert.NotContains(t, after, inherited[0],
		"the oldest inherited profile must be the one that made way")
}

// The profiling endpoints are unauthenticated and expose heap, goroutine,
// command-line, CPU-profile and trace data. Binding every interface hands that
// to any reachable peer; loopback still serves kubectl port-forward, which
// reaches the pod's own network namespace.
func TestStartSoakPProfServer_RefusesToBindBeyondLoopback(t *testing.T) {
	for _, addr := range []string{":6060", "0.0.0.0:6060", "10.0.0.5:6060"} {
		t.Run(addr, func(t *testing.T) {
			server, err := startSoakPProfServer(addr)

			require.Error(t, err)
			assert.Nil(t, server)
		})
	}
}

func TestStartSoakPProfServer_AcceptsLoopbackForms(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:0", "localhost:0"} {
		t.Run(addr, func(t *testing.T) {
			server, err := startSoakPProfServer(addr)

			require.NoError(t, err)
			require.NotNil(t, server)
			t.Cleanup(func() { require.NoError(t, server.Close()) })
		})
	}
}

// A configured diagnostic that quietly does not exist is the failure this
// branch is about; the caller has to be able to tell the difference.
func TestStartSoakPProfServer_ReportsAListenFailure(t *testing.T) {
	occupied, err := startSoakPProfServer("127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, occupied.Close()) })

	server, err := startSoakPProfServer(occupied.Addr)

	require.Error(t, err)
	assert.Nil(t, server)
}

// Profiling responses stream for the requested duration, so the listener needs
// bounds that do not cut them off but do close stalled and idle connections.
func TestStartSoakPProfServer_BoundsEveryConnectionPhase(t *testing.T) {
	server, err := startSoakPProfServer("127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	assert.Positive(t, server.ReadHeaderTimeout)
	assert.Positive(t, server.ReadTimeout)
	assert.Positive(t, server.WriteTimeout)
	assert.Positive(t, server.IdleTimeout)
}
