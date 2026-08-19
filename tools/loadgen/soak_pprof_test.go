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
	assert.Nil(t, startSoakPProfServer(""))
}

func TestStartSoakPProfServer_ServesTheHeapProfile(t *testing.T) {
	server := startSoakPProfServer("127.0.0.1:0")
	require.NotNil(t, server)
	t.Cleanup(func() {
		_ = server.Close()
	})
	require.NotEmpty(t, server.Addr, "the listener must be bound before Serve starts")

	response, err := http.Get("http://" + server.Addr + "/debug/pprof/heap?debug=1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = response.Body.Close() })

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
