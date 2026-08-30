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

func TestRenderBotRoomConsole_AnswerIsLargestPassingSize(t *testing.T) {
	results := []BotRoomStepResult{
		{Size: 100, E2P95Ms: 20, Tripped: false},
		{Size: 500, E2P95Ms: 60, Tripped: false},
		{Size: 1000, E2P95Ms: 800, Tripped: true, TrippedReasons: []string{"E2E p95=800ms > 500ms"}},
	}
	var buf bytes.Buffer
	renderBotRoomConsole(&buf, results)
	out := buf.String()
	assert.Contains(t, out, "ANSWER: max room size = 500")
	assert.Contains(t, out, "Next limit: E2E p95=800ms > 500ms")
}

func TestRenderBotRoomConsole_NoStepPassed(t *testing.T) {
	results := []BotRoomStepResult{
		{Size: 100, Tripped: true, TrippedReasons: []string{"x"}},
	}
	var buf bytes.Buffer
	renderBotRoomConsole(&buf, results)
	assert.Contains(t, buf.String(), "ANSWER: no step passed")
}

func TestWriteBotRoomCSV(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.csv")
	results := []BotRoomStepResult{
		{Size: 100, Rooms: 4, TargetRate: 200, AchievedRate: 199, E2P95Ms: 20, Tripped: false},
	}
	require.NoError(t, writeBotRoomCSV(path, results))
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	assert.Equal(t, "size,rooms,target_rate,achieved_rate,e2_p50_ms,e2_p95_ms,e2_p99_ms,read_p95_ms,read_p99_ms,error_rate,attempted,failed,worst_durable,worst_pending_delta,tripped,inconclusive,reasons", lines[0])
	assert.Len(t, lines, 2)
}
