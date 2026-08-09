package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleResults() []rpsStepResult {
	return []rpsStepResult{
		{TargetRPS: 500, AchievedRPS: 499, ErrorRate: 0, Kind: verdictPass,
			Latencies: []seriesPercentile{{Name: "E1", Pct: Percentiles{P95: ms(20), P99: ms(40)}}}},
		{TargetRPS: 1000, AchievedRPS: 998, ErrorRate: 0, Kind: verdictPass,
			Latencies: []seriesPercentile{{Name: "E1", Pct: Percentiles{P95: ms(60), P99: ms(90)}}}},
		{TargetRPS: 2000, AchievedRPS: 1900, ErrorRate: 0.02, Kind: verdictTrip,
			WorstDurable: "broadcast-worker", WorstDelta: 1500,
			Latencies: []seriesPercentile{{Name: "E1", Pct: Percentiles{P95: ms(160), P99: ms(300)}}},
			Reasons:   []string{"E1 p95=160ms > 100ms", "broadcast-worker pending +1500 > +1000"}},
	}
}

func TestRenderRPSReport_ReportsLastPass(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, renderRPSReport(&buf, sampleResults(), "messages", "medium"))
	out := buf.String()
	assert.Contains(t, out, "ANSWER: max RPS = 1000")
	assert.Contains(t, out, "workload=messages")
	assert.Contains(t, out, "preset=medium")
	assert.Contains(t, out, "Next limit:")
	assert.Contains(t, out, "broadcast-worker pending +1500 > +1000")
	assert.Contains(t, out, "E1 p95") // dynamic series column header
}

// Dropped deliveries have to be visible in the report, not just in the verdict:
// a run that trips on missing replies is diagnosed very differently from one
// that trips on latency.
func TestRenderRPSReport_ShowsMissingRates(t *testing.T) {
	results := []rpsStepResult{
		{TargetRPS: 2000, AchievedRPS: 1990, Kind: verdictTrip,
			AttemptedOps: 10000, MissingReplies: 50, MissingBroadcasts: 120,
			MissingReplyRate: 0.005, MissingBroadcastRate: 0.012,
			Latencies: []seriesPercentile{{Name: "E1", Pct: Percentiles{P95: ms(20)}}},
			Reasons:   []string{"missing broadcast rate 1.200% > 1.000%"}},
	}
	var buf bytes.Buffer
	require.NoError(t, renderRPSReport(&buf, results, "messages", "medium"))
	out := buf.String()

	assert.Contains(t, out, "miss%", "table needs a missing-delivery column")
	assert.Contains(t, out, "0.500/1.200", "reply and broadcast miss rates shown separately")
}

func TestRenderRPSReport_ShowsEventRatio(t *testing.T) {
	results := []rpsStepResult{{
		TargetRPS: 100, AchievedRPS: 100, Kind: verdictPass,
		EventRatios: []eventRatioResult{{Name: "SLO-3", Good: 99, Valid: 100, Ratio: 0.99, Target: 0.99}},
	}}
	var buf bytes.Buffer
	require.NoError(t, renderRPSReport(&buf, results, "login", "medium"))

	out := buf.String()
	assert.Contains(t, out, "SLO-3 good%")
	assert.Contains(t, out, "99.000")
}

func TestWriteRPSCSV_CarriesEventRatioCounts(t *testing.T) {
	results := []rpsStepResult{{
		TargetRPS: 100, AchievedRPS: 100, Kind: verdictPass,
		EventRatios: []eventRatioResult{{Name: "SLO-3", Good: 99, Valid: 100, Ratio: 0.99, Target: 0.99}},
	}}
	var buf bytes.Buffer
	require.NoError(t, writeRPSCSV(&buf, results, nil))

	out := buf.String()
	assert.Contains(t, out, "SLO-3_good,SLO-3_valid,SLO-3_good_ratio,SLO-3_target")
	assert.Contains(t, out, ",99,100,0.990000,0.990000,")
}

func TestWriteRPSCSV_CarriesMissingColumns(t *testing.T) {
	results := []rpsStepResult{
		{TargetRPS: 1000, AchievedRPS: 990, Kind: verdictPass,
			AttemptedOps: 10000, MissingReplies: 7, MissingBroadcasts: 9,
			MissingReplyRate: 0.0007, MissingBroadcastRate: 0.0009},
	}
	var buf bytes.Buffer
	require.NoError(t, writeRPSCSV(&buf, results, nil))
	out := buf.String()

	assert.Contains(t, out, "missing_replies")
	assert.Contains(t, out, "missing_broadcasts")
	assert.Contains(t, out, "missing_reply_rate")
	assert.Contains(t, out, "missing_broadcast_rate")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2)
	assert.Contains(t, lines[1], ",7,")
	assert.Contains(t, lines[1], ",9,")
}

func TestRenderRPSReport_NoStepPassed(t *testing.T) {
	results := []rpsStepResult{{TargetRPS: 500, Kind: verdictTrip, Reasons: []string{"E1 p95=400ms > 100ms"}}}
	var buf bytes.Buffer
	require.NoError(t, renderRPSReport(&buf, results, "history", "history-medium"))
	assert.Contains(t, buf.String(), "ANSWER: no step passed")
	assert.NotContains(t, buf.String(), "Next limit:")
}

func TestLastPassRPS(t *testing.T) {
	assert.Equal(t, 1000, lastPassRPS(sampleResults()))
	assert.Equal(t, 0, lastPassRPS([]rpsStepResult{{Kind: verdictTrip}}))
}

func TestWriteRPSCSV(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeRPSCSV(&buf, sampleResults(), nil))
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 4) // header + 3 rows
	assert.Contains(t, lines[0], "target_rps")
	assert.Contains(t, lines[0], "achieved_rps")
	assert.Contains(t, lines[0], "E1_p95_ms")
	assert.Contains(t, lines[0], "saturation")
	assert.Contains(t, lines[0], "emit_underrun")
	assert.Contains(t, lines[0], "verdict")
	assert.Contains(t, lines[0], "bottleneck_component")
	assert.Contains(t, lines[3], "2000")
	assert.Contains(t, lines[3], "TRIP")
}

func TestFirstTrip_NoneTripped(t *testing.T) {
	results := []rpsStepResult{
		{Kind: verdictPass},
		{Kind: verdictPass},
	}
	assert.Nil(t, firstTrip(results))
}

func TestPctFor_AbsentSeries(t *testing.T) {
	r := &rpsStepResult{
		Latencies: []seriesPercentile{{Name: "E1", Pct: Percentiles{P95: ms(50)}}},
	}
	assert.Equal(t, Percentiles{}, pctFor(r, "E2"))
}

func TestRenderRPSReport_AllPassNoTrip(t *testing.T) {
	results := []rpsStepResult{
		{TargetRPS: 500, AchievedRPS: 499, Kind: verdictPass,
			Latencies: []seriesPercentile{{Name: "E1", Pct: Percentiles{P95: ms(20), P99: ms(40)}}}},
	}
	var buf bytes.Buffer
	require.NoError(t, renderRPSReport(&buf, results, "messages", "medium"))
	out := buf.String()
	assert.Contains(t, out, "ANSWER: max RPS = 500")
	assert.NotContains(t, out, "Next limit:")
}

func TestRenderRPSReport_MultiSeriesAlignment(t *testing.T) {
	results := []rpsStepResult{
		{TargetRPS: 500, AchievedRPS: 500, Kind: verdictPass, Latencies: []seriesPercentile{
			{Name: "E1", Pct: Percentiles{P95: ms(10), P99: ms(20)}},
			{Name: "E2", Pct: Percentiles{P95: ms(30), P99: ms(40)}},
		}},
		{TargetRPS: 1000, AchievedRPS: 1000, Kind: verdictPass, Latencies: []seriesPercentile{
			{Name: "E1", Pct: Percentiles{P95: ms(15), P99: ms(25)}},
		}},
	}
	var buf bytes.Buffer
	require.NoError(t, renderRPSReport(&buf, results, "messages", "medium"))
	out := buf.String()
	assert.Contains(t, out, "E1 p95")
	assert.Contains(t, out, "E2 p95")

	var csvBuf bytes.Buffer
	require.NoError(t, writeRPSCSV(&csvBuf, results, nil))
	lines := strings.Split(strings.TrimSpace(csvBuf.String()), "\n")
	require.Len(t, lines, 3) // header + 2 rows
	assert.Contains(t, lines[0], "E1_p95_ms,E1_p99_ms,E2_p95_ms,E2_p99_ms")
	cols := strings.Split(lines[2], ",") // step 1 row; E2 columns must be zero-filled
	require.GreaterOrEqual(t, len(cols), 6)
	assert.Equal(t, "0", cols[4]) // E2_p95_ms
	assert.Equal(t, "0", cols[5]) // E2_p99_ms
}

func TestRenderRPSReport_AppendsBottleneck(t *testing.T) {
	results := []rpsStepResult{
		{TargetRPS: 1000, Kind: verdictPass},
		{TargetRPS: 2000, Kind: verdictTrip, Reasons: []string{"E2 p95=143ms > 100ms"}},
	}
	bn := bottleneckVerdict{Component: "message-worker", Resource: "Cassandra", Confidence: "high", Determined: true,
		Reasons: []string{"message-worker consumer backlog grew"}}
	var sb strings.Builder
	require.NoError(t, renderRPSReportWithBottleneck(&sb, results, "messages", "medium", &bn))
	out := sb.String()
	assert.Contains(t, out, "ANSWER: max RPS = 1000")
	assert.Contains(t, out, "BOTTLENECK: message-worker (Cassandra-bound)")
}

func TestRenderRPSReport_NilBottleneckUnchanged(t *testing.T) {
	results := []rpsStepResult{{TargetRPS: 1000, Kind: verdictPass}}
	var sb strings.Builder
	require.NoError(t, renderRPSReportWithBottleneck(&sb, results, "messages", "medium", nil))
	assert.NotContains(t, sb.String(), "BOTTLENECK:")
}
