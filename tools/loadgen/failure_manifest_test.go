package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validFailureManifest() failureManifest {
	return failureManifest{
		SchemaVersion: 1, RunID: "nats-core-20260812-001", ScenarioID: "F02",
		GitSHA: "101a074c3fe06f298a5667ae8081a075428d4584", Environment: "staging",
		Seed: 42, TrafficProfile: "nats-core-message-v1",
		Topology:                failureTopology{NATSServers: 3, StreamReplicas: 3},
		RequiredObservers:       []failureObserver{failureObserverAdmission, failureObserverHistory, failureObserverRecipient},
		RequiredMetricContracts: []string{"nats-core-v1", "loadgen-health-v1"},
		CreatedAt:               time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
	}
}

func TestFailureManifest_CreateDurableIsImmutable(t *testing.T) {
	directory := t.TempDir()
	manifest := validFailureManifest()
	path, digest, err := createFailureManifest(directory, &manifest)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(directory, manifest.RunID+".manifest.json"), path)
	assert.Len(t, digest, 64)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	parsed, err := parseFailureManifest(data)
	require.NoError(t, err)
	assert.Equal(t, manifest.RunID, parsed.RunID)

	_, _, err = createFailureManifest(directory, &manifest)
	assert.Error(t, err)
}

func TestFailureManifest_EnsureRetainedCopySupportsRestartAndRejectsMutation(t *testing.T) {
	manifest := validFailureManifest()
	directory := t.TempDir()
	first, firstDigest, err := ensureFailureManifest(directory, &manifest)
	require.NoError(t, err)
	second, secondDigest, err := ensureFailureManifest(directory, &manifest)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, firstDigest, secondDigest)

	mutated := manifest
	mutated.GitSHA = "different"
	_, _, err = ensureFailureManifest(directory, &mutated)
	assert.ErrorContains(t, err, "does not match retained manifest")
}

func TestFailureEvidenceRun_InstallsManifestAndOpensTimeline(t *testing.T) {
	manifest := validFailureManifest()
	sourceBytes, err := json.Marshal(&manifest)
	require.NoError(t, err)
	sourcePath := filepath.Join(t.TempDir(), "source.json")
	require.NoError(t, os.WriteFile(sourcePath, sourceBytes, 0o600))
	ledgerDirectory := t.TempDir()
	cfg := validSoakConfig(t)
	cfg.RunID = manifest.RunID
	cfg.LedgerDir = ledgerDirectory
	cfg.FailureManifestPath = sourcePath

	run, err := openSoakFailureEvidence(&cfg, NewMetrics(), manifest.CreatedAt)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, run.Close()) })
	assert.Equal(t, manifest.RunID, run.Manifest.RunID)
	assert.NotEmpty(t, run.ManifestDigest)
	assert.FileExists(t, filepath.Join(ledgerDirectory, manifest.RunID+".manifest.json"))
	assert.FileExists(t, filepath.Join(ledgerDirectory, manifest.RunID+".fault-events.jsonl"))
}

func TestFailureEvidenceRun_RejectsUnsafeAndMismatchedInputs(t *testing.T) {
	_, err := openSoakFailureEvidence(nil, nil, time.Now())
	assert.Error(t, err)
	cfg := validSoakConfig(t)
	cfg.FailureManifestPath = filepath.Join(t.TempDir(), "missing.json")
	_, err = openSoakFailureEvidence(&cfg, nil, time.Now())
	assert.Error(t, err)

	manifest := validFailureManifest()
	encoded, err := json.Marshal(&manifest)
	require.NoError(t, err)
	source := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(source, encoded, 0o600))
	cfg.FailureManifestPath = source
	cfg.LedgerDir = t.TempDir()
	cfg.RunID = "different"
	_, err = openSoakFailureEvidence(&cfg, nil, time.Now())
	assert.ErrorContains(t, err, "does not match")

	cfg.RunID = manifest.RunID
	cfg.FailureTimelinePath = filepath.Join(t.TempDir(), "outside.jsonl")
	_, err = openSoakFailureEvidence(&cfg, nil, time.Now())
	assert.ErrorContains(t, err, "under SOAK_LEDGER_DIR")

	cfg.FailureTimelinePath = ""
	cfg.FailureReportDir = filepath.Join(t.TempDir(), "outside-report")
	_, err = openSoakFailureEvidence(&cfg, nil, time.Now())
	assert.ErrorContains(t, err, "failure report must be stored")

	invalidSource := filepath.Join(t.TempDir(), "invalid.json")
	require.NoError(t, os.WriteFile(invalidSource, []byte("{"), 0o600))
	cfg.FailureManifestPath = invalidSource
	cfg.FailureReportDir = ""
	_, err = openSoakFailureEvidence(&cfg, nil, time.Now())
	assert.Error(t, err)
	assert.True(t, failurePathWithin(cfg.LedgerDir, filepath.Join(cfg.LedgerDir, "child")))
	assert.False(t, failurePathWithin(cfg.LedgerDir, filepath.Join(t.TempDir(), "outside")))
	require.NoError(t, (*soakFailureEvidenceRun)(nil).Close())
}

func TestFailureEvidenceRun_RejectsCorruptRetainedEvidence(t *testing.T) {
	manifest := validFailureManifest()
	encoded, err := json.Marshal(&manifest)
	require.NoError(t, err)
	source := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(source, encoded, 0o600))

	t.Run("manifest", func(t *testing.T) {
		ledgerDirectory := t.TempDir()
		cfg := validSoakConfig(t)
		cfg.RunID = manifest.RunID
		cfg.LedgerDir = ledgerDirectory
		cfg.FailureManifestPath = source
		retained := filepath.Join(ledgerDirectory, manifest.RunID+".manifest.json")
		require.NoError(t, os.WriteFile(retained, []byte("{"), 0o600))

		_, err := openSoakFailureEvidence(&cfg, nil, time.Now())
		assert.Error(t, err)
	})

	t.Run("timeline", func(t *testing.T) {
		ledgerDirectory := t.TempDir()
		cfg := validSoakConfig(t)
		cfg.RunID = manifest.RunID
		cfg.LedgerDir = ledgerDirectory
		cfg.FailureManifestPath = source
		timeline := filepath.Join(ledgerDirectory, manifest.RunID+".fault-events.jsonl")
		require.NoError(t, os.WriteFile(timeline, []byte("not-json\n"), 0o600))

		_, err := openSoakFailureEvidence(&cfg, nil, time.Now())
		assert.Error(t, err)
	})
}

func TestFailureTimeline_AppendAndReplayDurably(t *testing.T) {
	manifest := validFailureManifest()
	path := filepath.Join(t.TempDir(), "timeline.jsonl")
	timeline, err := openFailureTimeline(path, &manifest)
	require.NoError(t, err)
	event := failureFaultEvent{SchemaVersion: 1, RunID: manifest.RunID, ScenarioID: manifest.ScenarioID, Sequence: 1, Type: failureEventBaselineStarted, At: manifest.CreatedAt, Actor: "operator"}
	require.NoError(t, timeline.Append(&event))
	require.NoError(t, timeline.Close())

	reopened, err := openFailureTimeline(path, &manifest)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	assert.Equal(t, []failureFaultEvent{event}, reopened.Events())
	assert.Error(t, reopened.Append(&event))
}

func TestFailureTimeline_RefreshesExternalFaultInjectorEvents(t *testing.T) {
	manifest := validFailureManifest()
	path := filepath.Join(t.TempDir(), "events.jsonl")
	timeline, err := openFailureTimeline(path, &manifest)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, timeline.Close()) })
	base := manifest.CreatedAt.Add(time.Second)
	require.NoError(t, timeline.Append(&failureFaultEvent{
		SchemaVersion: 1, RunID: manifest.RunID, ScenarioID: manifest.ScenarioID,
		Sequence: 1, Type: failureEventBaselineStarted, At: base, Actor: "operator",
	}))
	external := failureFaultEvent{
		SchemaVersion: 1, RunID: manifest.RunID, ScenarioID: manifest.ScenarioID,
		Sequence: 2, Type: failureEventFaultStarted, TargetKind: "nats_server",
		Target: "nats-1", FaultKind: "process_stop", At: base.Add(time.Second), Actor: "chaos",
	}
	encoded, err := json.Marshal(external)
	require.NoError(t, err)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = file.Write(append(encoded, '\n'))
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())

	require.NoError(t, timeline.Refresh())
	assert.Len(t, timeline.Events(), 2)
	assert.Error(t, timeline.Append(nil))
	assert.Nil(t, (*failureTimeline)(nil).Events())
	require.NoError(t, (*failureTimeline)(nil).Close())
}

func TestFailureManifest_ParseAndValidate(t *testing.T) {
	encoded, err := json.Marshal(validFailureManifest())
	require.NoError(t, err)
	manifest, err := parseFailureManifest(encoded)
	require.NoError(t, err)
	assert.Equal(t, "nats-core-20260812-001", manifest.RunID)
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverHistory, failureObserverRecipient}, manifest.RequiredObservers)
}

func TestFailureManifest_RejectsUnsafeAndUnknownValues(t *testing.T) {
	tests := map[string]func(*failureManifest){
		"unsafe run id":           func(m *failureManifest) { m.RunID = "../escape" },
		"unknown schema":          func(m *failureManifest) { m.SchemaVersion = 9 },
		"unknown observer":        func(m *failureManifest) { m.RequiredObservers = []failureObserver{"dynamic"} },
		"unknown metric contract": func(m *failureManifest) { m.RequiredMetricContracts = []string{"raw-user-input"} },
		"insufficient topology":   func(m *failureManifest) { m.Topology.NATSServers = 1 },
		"non UTC timestamp":       func(m *failureManifest) { m.CreatedAt = time.Now() },
		"unknown scenario":        func(m *failureManifest) { m.ScenarioID = "F99" },
		"unknown environment":     func(m *failureManifest) { m.Environment = "tenant-123" },
		"unknown traffic profile": func(m *failureManifest) { m.TrafficProfile = "dynamic" },
		"missing identity":        func(m *failureManifest) { m.GitSHA = "" },
		"missing observers":       func(m *failureManifest) { m.RequiredObservers = nil },
		"duplicate observer":      func(m *failureManifest) { m.RequiredObservers = append(m.RequiredObservers, failureObserverAdmission) },
		"duplicate contract": func(m *failureManifest) {
			m.RequiredMetricContracts = append(m.RequiredMetricContracts, "nats-core-v1")
		},
		"missing recipient": func(m *failureManifest) {
			m.RequiredObservers = []failureObserver{failureObserverAdmission, failureObserverHistory}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := validFailureManifest()
			mutate(&manifest)
			assert.Error(t, validateFailureManifest(&manifest))
		})
	}
	assert.Error(t, validateFailureManifest(nil))
	assert.Error(t, func() error { _, err := parseFailureManifest([]byte(`{"schemaVersion":1,"unknown":true}`)); return err }())
	encoded, err := json.Marshal(validFailureManifest())
	require.NoError(t, err)
	_, err = parseFailureManifest(append(encoded, []byte(` {}`)...))
	assert.Error(t, err)
}

func TestFaultTimeline_OrderingDuplicatesAndOverlap(t *testing.T) {
	manifest := validFailureManifest()
	base := manifest.CreatedAt
	events := []failureFaultEvent{
		{SchemaVersion: 1, RunID: manifest.RunID, ScenarioID: manifest.ScenarioID, Sequence: 1, Type: failureEventBaselineStarted, At: base, Actor: "operator"},
		{SchemaVersion: 1, RunID: manifest.RunID, ScenarioID: manifest.ScenarioID, Sequence: 2, Type: failureEventFaultStarted, TargetKind: "nats_server", Target: "nats-1", FaultKind: "process_stop", At: base.Add(time.Minute), Actor: "chaos"},
		{SchemaVersion: 1, RunID: manifest.RunID, ScenarioID: manifest.ScenarioID, Sequence: 3, Type: failureEventFaultRemoved, TargetKind: "nats_server", Target: "nats-1", FaultKind: "process_stop", At: base.Add(2 * time.Minute), Actor: "chaos"},
	}
	ordered, err := validateFailureTimeline(&manifest, events)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), ordered[2].Sequence)

	duplicate := append(append([]failureFaultEvent(nil), events...), events[2])
	assert.Error(t, func() error { _, err := validateFailureTimeline(&manifest, duplicate); return err }())

	overlap := append([]failureFaultEvent(nil), events[:2]...)
	overlap = append(overlap, failureFaultEvent{SchemaVersion: 1, RunID: manifest.RunID, ScenarioID: manifest.ScenarioID, Sequence: 3, Type: failureEventFaultStarted, TargetKind: "nats_server", Target: "nats-2", FaultKind: "process_stop", At: base.Add(90 * time.Second), Actor: "chaos"})
	assert.Error(t, func() error { _, err := validateFailureTimeline(&manifest, overlap); return err }())
	manifest.FaultPolicy.AllowCombined = true
	_, err = validateFailureTimeline(&manifest, overlap)
	assert.NoError(t, err)
}

func TestFaultTimeline_RejectsInvalidContractsAndAllowsDeclaredCombinedFaults(t *testing.T) {
	manifest := validFailureManifest()
	base := manifest.CreatedAt.Add(time.Second)
	valid := failureFaultEvent{
		SchemaVersion: 1, RunID: manifest.RunID, ScenarioID: manifest.ScenarioID,
		Sequence: 1, Type: failureEventFaultStarted, TargetKind: "nats_server",
		Target: "nats-1", FaultKind: "process_stop", At: base, Actor: "chaos",
	}
	tests := []struct {
		name   string
		mutate func(*failureFaultEvent)
	}{
		{name: "unknown event", mutate: func(event *failureFaultEvent) { event.Type = "future" }},
		{name: "wrong run", mutate: func(event *failureFaultEvent) { event.RunID = "other" }},
		{name: "missing target", mutate: func(event *failureFaultEvent) { event.Target = "" }},
		{name: "unknown target kind", mutate: func(event *failureFaultEvent) { event.TargetKind = "pod_uid" }},
		{name: "unknown fault kind", mutate: func(event *failureFaultEvent) { event.FaultKind = "raw-command" }},
		{name: "zero sequence", mutate: func(event *failureFaultEvent) { event.Sequence = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event := valid
			tc.mutate(&event)
			_, err := validateFailureTimeline(&manifest, []failureFaultEvent{event})
			assert.Error(t, err)
		})
	}

	remove := valid
	remove.Type = failureEventFaultRemoved
	_, err := validateFailureTimeline(&manifest, []failureFaultEvent{remove})
	assert.ErrorContains(t, err, "no matching active fault")

	manifest.FaultPolicy.AllowCombined = true
	second := valid
	second.Sequence = 2
	second.Target = "nats-2"
	second.At = base.Add(time.Second)
	_, err = validateFailureTimeline(&manifest, []failureFaultEvent{valid, second})
	require.NoError(t, err)
}
