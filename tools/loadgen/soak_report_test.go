package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSoakReport_IncludesTargetsRatesErrorsAndDrift(t *testing.T) {
	start := time.Unix(100, 0)
	collector := NewSoakCollector(nil, start, 0, 100*time.Second)
	for range 10 {
		require.NoError(t, collector.Record(&soakOperationSample{
			Action: soakRPCReact, Outcome: soakOutcomeSucceeded,
			At: start.Add(10 * time.Second), Latency: 10 * time.Millisecond,
		}))
		require.NoError(t, collector.Record(&soakOperationSample{
			Action: soakRPCReact, Outcome: soakOutcomeFailed,
			ErrorClass: soakErrorTimeout, Retries: 1,
			At: start.Add(90 * time.Second), Latency: 100 * time.Millisecond,
		}))
	}
	snapshot := collector.Snapshot(start.Add(100 * time.Second))
	report := BuildSoakReport(
		&snapshot,
		map[soakRPCAction]float64{soakRPCReact: 100},
	)

	require.Len(t, report.Rows, 1)
	row := report.Rows[0]
	assert.Equal(t, soakRPCReact, row.Action)
	assert.Equal(t, float64(100), row.TargetRate)
	assert.InDelta(t, 0.2, row.AchievedRate, 0.001)
	assert.Equal(t, uint64(10), row.Failed)
	assert.Equal(t, uint64(10), row.Retries)
	assert.Equal(t, 90*time.Millisecond, row.P99Drift)
	assert.Equal(t, uint64(10), row.Errors[soakErrorTimeout])
}

func TestPrintSoakReport_PrintsRequiredPerRPCRowsAndFindings(t *testing.T) {
	snapshot := soakCollectorSnapshot{
		Actions: map[soakRPCAction]soakActionStats{
			soakRPCReact:      {Attempted: 1, Succeeded: 1},
			soakRPCEdit:       {Attempted: 1, Failed: 1},
			soakRPCDelete:     {Attempted: 1, Skipped: 1},
			soakRPCPin:        {Attempted: 1, Succeeded: 1},
			soakRPCUnpin:      {Attempted: 1, Succeeded: 1},
			soakRPCPinnedList: {Attempted: 1, Succeeded: 1},
		},
		MutationTargetMissing: 2,
		Verifications: map[soakRPCAction]map[soakVerifyClass]uint64{
			soakRPCGetMessage: {soakVerifyMismatch: 3},
		},
	}
	report := BuildSoakReport(&snapshot, nil)
	var output bytes.Buffer
	require.NoError(t, PrintSoakReport(&output, &report))

	text := output.String()
	for _, action := range []string{
		"reaction", "edit", "delete", "pin", "unpin", "pinned_list",
	} {
		assert.Contains(t, text, action)
	}
	assert.Contains(t, text, "mutation_target_missing")
	assert.Contains(t, text, "mismatch")
}

func TestBuildSoakReport_SortsRowsByAction(t *testing.T) {
	snapshot := soakCollectorSnapshot{
		Actions: map[soakRPCAction]soakActionStats{
			soakRPCUnpin: {Attempted: 1},
			soakRPCEdit:  {Attempted: 1},
			soakRPCReact: {Attempted: 1},
		},
	}
	report := BuildSoakReport(&snapshot, nil)
	require.Len(t, report.Rows, 3)
	assert.Equal(t, []soakRPCAction{
		soakRPCEdit, soakRPCReact, soakRPCUnpin,
	}, []soakRPCAction{
		report.Rows[0].Action,
		report.Rows[1].Action,
		report.Rows[2].Action,
	})
}
