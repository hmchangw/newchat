package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderCapacityConsole_AnswerLine(t *testing.T) {
	results := []capacityStepResult{
		{N: 10000, EffectiveN: 10000, ConnectP50Ms: 8, ConnectP95Ms: 40, ConnectP99Ms: 80, PingSustain: 1.0, Kind: verdictPass},
		{N: 20000, EffectiveN: 20000, ConnectP50Ms: 9, ConnectP95Ms: 60, ConnectP99Ms: 120, PingSustain: 1.0, Kind: verdictPass},
		{N: 50000, EffectiveN: 50000, FalseOfflines: 900, FalseOfflineRate: 0.018, PingSustain: 1.0,
			Kind: verdictTrip, Reasons: []string{"false_offline_rate=0.0180 > 0.0010 (900 users swept offline)"}},
	}
	var buf bytes.Buffer
	renderCapacityConsole(&buf, results)
	out := buf.String()
	assert.Contains(t, out, "MAX CONCURRENT ONLINE: 20000")
	assert.Contains(t, out, "Next limit:")
	assert.Contains(t, out, "false_offline_rate")
}

func TestRenderCapacityConsole_NoPass(t *testing.T) {
	results := []capacityStepResult{
		{N: 10000, EffectiveN: 5000, Kind: verdictInconclusive, Reasons: []string{"inconclusive: only 5000/10000 users activated"}},
	}
	var buf bytes.Buffer
	renderCapacityConsole(&buf, results)
	assert.Contains(t, buf.String(), "MAX CONCURRENT ONLINE: none")
}

func TestWriteCapacityCSV_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cap.csv")
	results := []capacityStepResult{
		{N: 20000, EffectiveN: 20000, StartedAt: time.Now(), ConnectP95Ms: 60, FalseOfflines: 0, PingSustain: 1.0, Kind: verdictPass},
		{N: 10000, EffectiveN: 10000, StartedAt: time.Now(), ConnectP95Ms: 40, FalseOfflines: 0, PingSustain: 1.0, Kind: verdictPass},
	}
	require.NoError(t, writeCapacityCSV(path, results))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	out := string(b)
	assert.Contains(t, out, "n,effective_n,")
	// Rows sorted ascending by N: 10000 before 20000.
	idx10 := bytes.Index(b, []byte("10000,10000"))
	idx20 := bytes.Index(b, []byte("20000,20000"))
	assert.True(t, idx10 >= 0 && idx20 >= 0 && idx10 < idx20)
}
