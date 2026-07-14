package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderPresenceConsole_Answer(t *testing.T) {
	results := []presenceStepResult{
		{N: 1000, P95Ms: 40, P99Ms: 90, Kind: verdictPass},
		{N: 2000, P95Ms: 250, P99Ms: 600, Kind: verdictTrip, Reasons: []string{"p99=600ms > 500"}},
	}
	var buf bytes.Buffer
	renderPresenceConsole(&buf, results)
	out := buf.String()
	assert.Contains(t, out, "ANSWER: N = 1000")
	assert.Contains(t, out, "Next limit: p99=600ms > 500")
}

func TestRenderPresenceConsole_NoPass(t *testing.T) {
	var buf bytes.Buffer
	renderPresenceConsole(&buf, []presenceStepResult{{N: 1000, Kind: verdictTrip, Reasons: []string{"x"}}})
	assert.Contains(t, buf.String(), "no step passed")
}

func TestWritePresenceCSV(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.csv")
	require.NoError(t, writePresenceCSV(path, []presenceStepResult{
		{N: 1000, EffectiveN: 1000, P50Ms: 10, P95Ms: 40, P99Ms: 90, ErrorRate: 0, Attempted: 500, Kind: verdictPass},
	}))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	assert.Equal(t, "n,effective_n,p50_ms,p95_ms,p99_ms,error_rate,attempted,failed,verdict,reasons", lines[0])
	assert.Contains(t, lines[1], "1000")
	assert.Contains(t, lines[1], "PASS")
}

func TestRenderStormConsole_Answer(t *testing.T) {
	results := []stormStepResult{
		{Fraction: 0.5, StormUsers: 500, RecoveryComplete: true, RecoveryMs: 3000, P99Ms: 200, Kind: verdictPass},
		{Fraction: 1.0, StormUsers: 1000, RecoveryComplete: true, RecoveryMs: 20000, P99Ms: 300, Kind: verdictTrip, Reasons: []string{"recovery=20s > 10s"}},
	}
	var buf bytes.Buffer
	renderStormConsole(&buf, results)
	out := buf.String()
	assert.Contains(t, out, "ANSWER: max survivable storm = 0.50")
	assert.Contains(t, out, "Next limit: recovery=20s > 10s")
}

func TestWriteStormCSV(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "storm.csv")
	require.NoError(t, writeStormCSV(path, []stormStepResult{
		{Fraction: 0.5, StormUsers: 500, RecoveryComplete: true, RecoveryMs: 3000, P99Ms: 200, ErrorRate: 0, Kind: verdictPass},
	}))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	assert.Equal(t, "fraction,storm_users,recovery_complete,recovery_ms,p99_ms,error_rate,verdict,reasons", lines[0])
	assert.Contains(t, lines[1], "0.50")
}
