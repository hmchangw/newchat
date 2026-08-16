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
	path := failureWALPath(cfg.LedgerDir, cfg.RunID, cfg.LedgerEpoch)
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
	path := failureWALPath(cfg.LedgerDir, cfg.RunID, cfg.LedgerEpoch)
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

func TestFailureObservationRuntime_EarlierEpochJournalIsRetainedNotReplayed(t *testing.T) {
	now := time.Date(2026, 8, 16, 1, 2, 3, 0, time.UTC)
	cfg := validSoakConfig(t)
	cfg.LedgerDir = t.TempDir()
	cfg.LedgerEpoch = "v1"
	metrics := NewMetrics()

	ledger, err := openSoakFailureLedger(&cfg, metrics, func() time.Time { return now })
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&failureOperation{
		SchemaVersion: 2, ID: "message-1", RunID: cfg.RunID,
		Scenario: soakFailureScenario, Lane: soakFailureLaneMessageSend,
		OperationType: failureOperationMessageCreate, LifecycleState: failureOperationJournaled,
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Targets: map[string]string{"messageId": "message-1"},
		Effects: messageCreateExpectedEffectsForObservers(false, 0, ""),
	}))
	require.NoError(t, ledger.Close())

	// A contract change is deployed as a new epoch rather than a new run ID, so
	// the topology stays seeded while the incompatible journal is left alone.
	cfg.LedgerEpoch = "v2"
	upgradedMetrics := NewMetrics()
	upgraded, err := openSoakFailureLedger(&cfg, upgradedMetrics, func() time.Time { return now })
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, upgraded.Close()) })

	assert.Equal(t, 0, upgraded.Snapshot().Recovered,
		"an earlier epoch's operations must not be replayed under a new contract")
	assert.Equal(t, float64(1), testutil.ToFloat64(upgradedMetrics.FailureAbandonedJournals))
	assert.FileExists(t, failureWALPath(cfg.LedgerDir, cfg.RunID, "v1"),
		"the earlier journal is retained evidence and must not be deleted")
}

func TestFailureObservationRuntime_SameEpochRecoversUnresolvedOperations(t *testing.T) {
	now := time.Date(2026, 8, 16, 1, 2, 3, 0, time.UTC)
	cfg := validSoakConfig(t)
	cfg.LedgerDir = t.TempDir()
	metrics := NewMetrics()

	ledger, err := openSoakFailureLedger(&cfg, metrics, func() time.Time { return now })
	require.NoError(t, err)
	require.NoError(t, ledger.Start(&failureOperation{
		SchemaVersion: 2, ID: "member-1", RunID: cfg.RunID,
		Scenario: soakFailureScenario, Lane: soakFailureLaneMemberMutation,
		OperationType: failureOperationMemberAdd, LifecycleState: failureOperationJournaled,
		StartedAt: now, VerifyAfter: now, Deadline: now.Add(time.Minute),
		Targets: map[string]string{"roomId": "room-1", "account": "user-9"},
		Effects: memberMutationExpectedEffects(),
	}))
	require.NoError(t, ledger.Close())

	recovered, err := openSoakFailureLedger(&cfg, NewMetrics(), func() time.Time { return now })
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })

	assert.Equal(t, 1, recovered.Snapshot().Recovered)
	operation, ok := recovered.Active("member-1")
	require.True(t, ok)
	assert.Equal(t, failureOperationMemberAdd, operation.OperationType)
}
