package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailureEvidenceReport_GoldenAndAtomicArtifacts(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	report := failureEvidenceReport{
		SchemaVersion:  1,
		ManifestDigest: "manifest-sha",
		Window:         failureReportWindow{StartedAt: now, EndedAt: now.Add(time.Minute)},
		Lanes: []failureLaneReport{{
			Scenario: "F02", Lane: "message_send", Offered: 4, Eligible: 4,
			Terminals: failureTerminalCounts{Good: 2, Bad: 1, Unverified: 1},
			Latency:   failureLatencyReport{P50: "10ms", P95: "100ms", P99: "250ms", Max: "300ms"},
		}},
		Observers: []failureObserverReport{{
			Observer: failureObserverRecipient,
			Outcomes: failureObservationCounts{Good: 2, Unverified: 1, MissingAfterDeadline: 1},
		}},
		Gates:   []failureGate{{ID: "observer_health", Verdict: failureVerdictInconclusive, Reason: "observer gap"}},
		Verdict: failureVerdictInconclusive,
		Reasons: []string{"observer gap"},
	}
	directory := t.TempDir()
	sidecar, err := writeFailureSidecar(directory, "missing-recipients.json", []string{"bob", "alice", "alice"})
	require.NoError(t, err)
	report.Sidecars = []failureSidecarReference{sidecar}
	require.NoError(t, writeFailureEvidenceArtifacts(directory, &report))

	got, err := os.ReadFile(filepath.Join(directory, "report.json"))
	require.NoError(t, err)
	want, err := os.ReadFile(filepath.Join("testdata", "failure_report.golden.json"))
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got))
	summary, err := os.ReadFile(filepath.Join(directory, "summary.md"))
	require.NoError(t, err)
	assert.Contains(t, string(summary), "# Failure evidence report: INCONCLUSIVE")
	assert.Contains(t, string(summary), "observer gap")

	sidecarBytes, err := os.ReadFile(filepath.Join(directory, sidecar.Path))
	require.NoError(t, err)
	assert.Equal(t, "[\n  \"alice\",\n  \"bob\"\n]\n", string(sidecarBytes))
	assert.Equal(t, 2, sidecar.Count)
	assert.Len(t, sidecar.SHA256, 64)
}

func TestFailureEvidenceReport_RejectsAccountingMismatch(t *testing.T) {
	report := failureEvidenceReport{
		SchemaVersion: 1,
		Lanes: []failureLaneReport{{
			Scenario: "F02", Lane: "message_send", Eligible: 2,
			Terminals: failureTerminalCounts{Good: 1},
		}},
	}
	_, err := marshalFailureEvidenceReport(report)
	assert.ErrorContains(t, err, "eligible operation accounting")
}

func TestFailureEvidenceReport_BuildsEveryVerdictGateAndRetainsSidecars(t *testing.T) {
	start := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	manifest := validFailureManifest()
	run := &soakFailureEvidenceRun{
		Manifest: &manifest, ManifestPath: "run.manifest.json", ManifestDigest: "digest",
		Timeline: &failureTimeline{events: []failureFaultEvent{{
			SchemaVersion: 1, RunID: manifest.RunID, ScenarioID: manifest.ScenarioID,
			Sequence: 1, Type: failureEventBaselineStarted, At: start, Actor: "operator",
		}}},
		StartedAt: start,
	}
	healthy := failureObserverHealthSnapshot{
		Observer: failureObserverRecipient, Up: true, LastSuccess: start.Add(time.Minute),
		Intervals: []failureHealthInterval{{Start: start, End: start.Add(time.Minute), Up: true}},
	}
	soak := &soakCollectorSnapshot{Actions: map[soakRPCAction]soakActionStats{
		soakRPCSend: {Attempted: 2, Latency: soakLatencySummary{P50: time.Millisecond, P95: 2 * time.Millisecond, P99: 5 * time.Millisecond, Max: 7 * time.Millisecond}},
	}}
	consumers := []ConsumerStat{
		{Stream: "MESSAGES_CANONICAL_site-local", Durable: "message-worker", BaselinePending: 1, PeakPending: 3, FinalPending: 0, HasSample: true},
		{Stream: "MESSAGES_CANONICAL_site-local", Durable: "broadcast-worker", BaselinePending: 0, PeakPending: 2, FinalPending: 0, HasSample: true},
	}

	tests := []struct {
		name     string
		ledger   failureLedgerSnapshot
		health   failureObserverHealthSnapshot
		mutate   func(*soakFailureEvidenceRun)
		sidecars []failureSidecarReference
		wantGate string
		want     failureVerdict
	}{
		{
			name:   "complete loadgen evidence retains external boundary",
			ledger: failureLedgerSnapshot{Results: map[failureResult]uint64{failureResultGood: 2}},
			health: healthy, wantGate: "07_external_contracts", want: failureVerdictInconclusive,
		},
		{
			name:   "healthy authoritative missing keeps fail evidence",
			ledger: failureLedgerSnapshot{Results: map[failureResult]uint64{failureResultMissingAfterDeadline: 1}},
			health: healthy, wantGate: "06_correctness", want: failureVerdictInconclusive,
		},
		{
			name:   "ledger invalidation",
			ledger: failureLedgerSnapshot{InvalidReason: "wal", Results: map[failureResult]uint64{}},
			health: healthy, wantGate: "04_ledger", want: failureVerdictInconclusive,
		},
		{
			name:     "active reconciliation and observer gap",
			ledger:   failureLedgerSnapshot{Active: 2, Results: map[failureResult]uint64{}},
			health:   failureObserverHealthSnapshot{Observer: failureObserverRecipient},
			wantGate: "05_observer_health", want: failureVerdictInconclusive,
		},
		{
			name:     "recipient duplicate sidecar is a hard failure gate",
			ledger:   failureLedgerSnapshot{Results: map[failureResult]uint64{failureResultGood: 1}},
			health:   healthy,
			sidecars: []failureSidecarReference{{Path: "recipient-duplicate.json", Count: 1}},
			wantGate: "06_recipient_anomaly_recipient-duplicate.json", want: failureVerdictInconclusive,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testRun := *run
			if tc.mutate != nil {
				tc.mutate(&testRun)
			}
			report := buildSoakFailureEvidenceReport(
				&testRun, tc.ledger, soak, tc.health, consumers, tc.sidecars, start.Add(time.Minute),
			)
			assert.Equal(t, tc.want, report.Verdict)
			assert.Contains(t, gateIDs(report.Gates), tc.wantGate)
		})
	}

	nilRun := buildSoakFailureEvidenceReport(nil, failureLedgerSnapshot{}, nil, failureObserverHealthSnapshot{}, nil, nil, start)
	assert.Equal(t, failureVerdictInconclusive, nilRun.Verdict)
}

func TestFailureEvidenceReport_ConsumerAndAccountingEvidence(t *testing.T) {
	start := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	manifest := validFailureManifest()
	run := &soakFailureEvidenceRun{
		Manifest: &manifest, ManifestPath: "run.manifest.json", ManifestDigest: "digest",
		Timeline: &failureTimeline{events: []failureFaultEvent{{
			SchemaVersion: 1, RunID: manifest.RunID, ScenarioID: manifest.ScenarioID,
			Sequence: 1, Type: failureEventBaselineStarted, At: start, Actor: "operator",
		}}},
		StartedAt: start,
	}
	soak := &soakCollectorSnapshot{Actions: map[soakRPCAction]soakActionStats{
		soakRPCSend:        {Attempted: 2},
		soakRPCThreadReply: {Attempted: 1},
	}}
	health := failureObserverHealthSnapshot{
		Observer: failureObserverRecipient, Up: true, LastSuccess: start.Add(time.Minute),
		Intervals: []failureHealthInterval{{Start: start, End: start.Add(time.Minute), Up: true}},
	}
	consumers := []ConsumerStat{
		{
			Stream: "MESSAGES_CANONICAL_site-local", Durable: "message-worker",
			BaselinePending: 2, PeakPending: 5, FinalPending: 1, HasSample: true,
		},
		{
			Stream: "MESSAGES_CANONICAL_site-local", Durable: "broadcast-worker",
			HasSample: true,
		},
	}

	report := buildSoakFailureEvidenceReport(
		run,
		failureLedgerSnapshot{Results: map[failureResult]uint64{failureResultGood: 2}},
		soak,
		health,
		consumers,
		nil,
		start.Add(time.Minute),
	)

	require.Len(t, report.Consumers, 2)
	messageConsumer := report.Consumers[0]
	assert.Equal(t, "message-worker", messageConsumer.Durable)
	assert.Equal(t, uint64(2), messageConsumer.BaselinePending)
	assert.Equal(t, uint64(5), messageConsumer.PeakPending)
	assert.Equal(t, uint64(1), messageConsumer.FinalPending)
	assert.Contains(t, gateIDs(report.Gates), "04_accounting")
	assert.Contains(t, gateIDs(report.Gates), "05_consumer_health")
	assert.Equal(t, failureVerdictInconclusive, gateByID(t, report.Gates, "04_accounting").Verdict)
	assert.Equal(t, failureVerdictPass, gateByID(t, report.Gates, "05_consumer_health").Verdict)
	assert.Equal(t, failureVerdictInconclusive, gateByID(t, report.Gates, "02_topology").Verdict)
}

func TestFailureEvidenceReport_AccountingIncludesWarmupSends(t *testing.T) {
	start := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	manifest := validFailureManifest()
	run := &soakFailureEvidenceRun{
		Manifest: &manifest, ManifestPath: "run.manifest.json", ManifestDigest: "digest",
		Timeline: &failureTimeline{events: []failureFaultEvent{{
			SchemaVersion: 1, RunID: manifest.RunID, ScenarioID: manifest.ScenarioID,
			Sequence: 1, Type: failureEventBaselineStarted, At: start, Actor: "operator",
		}}},
		StartedAt: start,
	}
	soak := &soakCollectorSnapshot{
		Actions: map[soakRPCAction]soakActionStats{
			soakRPCSend:        {Attempted: 2},
			soakRPCThreadReply: {Attempted: 1},
		},
		WarmupActions: map[soakRPCAction]uint64{
			soakRPCSend:        1,
			soakRPCThreadReply: 2,
		},
	}
	health := failureObserverHealthSnapshot{
		Observer: failureObserverRecipient, Up: true, LastSuccess: start.Add(time.Minute),
		Intervals: []failureHealthInterval{{Start: start, End: start.Add(time.Minute), Up: true}},
	}
	consumers := []ConsumerStat{
		{Durable: "message-worker", HasSample: true},
		{Durable: "broadcast-worker", HasSample: true},
	}

	report := buildSoakFailureEvidenceReport(
		run,
		failureLedgerSnapshot{Results: map[failureResult]uint64{failureResultGood: 6}},
		soak,
		health,
		consumers,
		nil,
		start.Add(time.Minute),
	)

	require.Len(t, report.Lanes, 1)
	assert.Equal(t, uint64(6), report.Lanes[0].Offered)
	assert.Equal(t, failureVerdictPass, gateByID(t, report.Gates, "04_accounting").Verdict)
}

func TestFailureEvidenceReport_RecoveredOperationsInvalidatePerformanceInterval(t *testing.T) {
	start := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	manifest := validFailureManifest()
	run := &soakFailureEvidenceRun{
		Manifest: &manifest, ManifestPath: "run.manifest.json", ManifestDigest: "digest",
		Timeline: &failureTimeline{}, StartedAt: start,
	}

	report := buildSoakFailureEvidenceReport(
		run,
		failureLedgerSnapshot{
			Recovered: 1,
			Results:   map[failureResult]uint64{failureResultGood: 1},
		},
		&soakCollectorSnapshot{Actions: map[soakRPCAction]soakActionStats{
			soakRPCSend: {Attempted: 1},
		}},
		failureObserverHealthSnapshot{},
		nil,
		nil,
		start.Add(time.Minute),
	)

	gate := gateByID(t, report.Gates, "04_ledger")
	assert.Equal(t, failureVerdictInconclusive, gate.Verdict)
	assert.Contains(t, gate.Reason, "recovered")
}

func gateByID(t *testing.T, gates []failureGate, id string) failureGate {
	t.Helper()
	for _, gate := range gates {
		if gate.ID == id {
			return gate
		}
	}
	t.Fatalf("gate %q not found", id)
	return failureGate{}
}

func TestFailureEvidenceReport_FinalizesRawRecipientJournals(t *testing.T) {
	directory := t.TempDir()
	observer := newFailureRecipientObserver(
		nil, NewMetrics(), 1, time.Now, withFailureRecipientEvidenceDir(directory),
	)
	require.NoError(t, observer.appendRawEvidence("missing", "message-2", "bob"))
	require.NoError(t, observer.appendRawEvidence("missing", "message-1", "alice"))
	require.NoError(t, observer.appendRawEvidence("unverified", "message-3", ""))

	references, err := finalizeFailureRecipientSidecars(directory)
	require.NoError(t, err)
	require.Len(t, references, 2)
	assert.Equal(t, "recipient-missing.json", references[0].Path)
	assert.Equal(t, 2, references[0].Count)
	assert.Equal(t, "recipient-unverified.json", references[1].Path)
}

func gateIDs(gates []failureGate) []string {
	ids := make([]string, 0, len(gates))
	for _, gate := range gates {
		ids = append(ids, gate.ID)
	}
	return ids
}
