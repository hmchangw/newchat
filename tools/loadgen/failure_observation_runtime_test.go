package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailureObservationRuntime_ObserverContractRejectsChangedConfiguration(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	cfg := validSoakConfig(t)
	cfg.LedgerDir = t.TempDir()
	cfg.RecipientObserverEnabled = false

	ledger, err := openSoakFailureLedger(&cfg, NewMetrics(), func() time.Time { return now })
	require.NoError(t, err)
	require.NoError(t, ledger.Close())

	cfg.RecipientObserverEnabled = true
	_, err = openSoakFailureLedger(&cfg, NewMetrics(), func() time.Time { return now })
	require.Error(t, err)
	assert.ErrorContains(t, err, "new SOAK_RUN_ID")
}

func TestFailureObservationRuntime_ReportsConfiguredAndDisabledObservers(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RecipientObserverEnabled = false
	metrics := NewMetrics()
	ledger, err := openSoakFailureLedger(&cfg, metrics, time.Now)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ledger.Close()) })

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureObserverConfigured.WithLabelValues(string(failureObserverAdmission)),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureObserverConfigured.WithLabelValues(string(failureObserverHistory)),
	))
	assert.Equal(t, float64(0), testutil.ToFloat64(
		metrics.FailureObserverConfigured.WithLabelValues(string(failureObserverRecipient)),
	))
}

func TestFailureObservationRuntime_LegacyWALAdoptsCompatibleContract(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	cfg := validSoakConfig(t)
	cfg.LedgerDir = t.TempDir()
	cfg.RecipientObserverEnabled = false
	path := filepath.Join(cfg.LedgerDir, cfg.RunID+".wal")
	legacy := `{"type":"started","operation":{"id":"legacy-1","scenario":"message_soak","lane":"message_send","startedAt":"2026-08-15T01:02:03Z","verifyAfter":"2026-08-15T01:02:03Z","deadline":"2026-08-15T01:03:03Z","expected":["admission","cassandra_history"]},"at":"2026-08-15T01:02:03Z"}` + "\n"
	require.NoError(t, os.WriteFile(path, []byte(legacy), 0o600))

	ledger, err := openSoakFailureLedger(&cfg, NewMetrics(), func() time.Time { return now })
	require.NoError(t, err)
	require.NoError(t, ledger.Close())

	encoded, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"observerContract"`)
}

func TestFailureObservationRuntime_LegacyPendingOperationRejectsNewRecipientMode(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	cfg := validSoakConfig(t)
	cfg.LedgerDir = t.TempDir()
	cfg.RecipientObserverEnabled = true
	path := filepath.Join(cfg.LedgerDir, cfg.RunID+".wal")
	legacy := `{"type":"started","operation":{"id":"legacy-1","scenario":"message_soak","lane":"message_send","startedAt":"2026-08-15T01:02:03Z","verifyAfter":"2026-08-15T01:02:03Z","deadline":"2026-08-15T01:03:03Z","expected":["admission","cassandra_history"]},"at":"2026-08-15T01:02:03Z"}` + "\n"
	require.NoError(t, os.WriteFile(path, []byte(legacy), 0o600))

	_, err := openSoakFailureLedger(&cfg, NewMetrics(), func() time.Time { return now })
	require.Error(t, err)
	assert.ErrorContains(t, err, "new SOAK_RUN_ID")
}

func TestSoakFailureTracker_RecipientEffectIsOptIn(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 2})
	require.NoError(t, err)
	pending := &soakPendingSend{
		MessageID: "message-1", RequestID: "request-1", PublishedAt: now,
		Target: soakSendTarget{RoomID: "room-1", Account: "alice", Recipients: []string{"alice", "bob"}},
	}

	tracker := newSoakFailureTracker(ledger, 0, time.Minute, func() time.Time { return now })
	require.NoError(t, tracker.Start(pending))
	operation, ok := ledger.Active(pending.MessageID)
	require.True(t, ok)
	assert.Equal(t, []failureObserver{failureObserverAdmission, failureObserverHistory}, operation.Expected)
	for _, effect := range operation.Effects {
		assert.NotEqual(t, failureObserverRecipient, effect.Observer)
	}
}

func TestFailureObservationRuntime_DisabledCreatesNoRecipientRuntime(t *testing.T) {
	runtime := newSoakFailureObservationRuntime(false, nil, NewMetrics(), 1, t.TempDir(), time.Now)
	assert.Nil(t, runtime.Recipient())
	require.NoError(t, runtime.StartRecipient(nil, nil, nil))
	require.NoError(t, runtime.Close())
}

func TestFailureObservationRuntime_WALFailureFallsBackWithoutStoppingTraffic(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocked, []byte("blocked"), 0o600))
	cfg := validSoakConfig(t)
	cfg.LedgerDir = filepath.Join(blocked, "ledger")

	ledger, degraded, err := openSoakFailureObservationLedger(
		&cfg,
		NewMetrics(),
		func() time.Time { return now },
	)
	require.NoError(t, err)
	require.NotNil(t, ledger)
	assert.True(t, degraded)
	assert.Equal(t, "wal", ledger.Snapshot().InvalidReason)
}
