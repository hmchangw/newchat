package main

import (
	"math"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dashboardDispatchValid(actual, expected float64) bool {
	return expected > 0 && actual*100 >= expected*95
}

func dashboardUnverifiedInvalid(unverified, eligible int) bool {
	limit := max(3, int(math.Ceil(0.001*float64(eligible))))
	return unverified > limit
}

func dashboardRecovered(points []bool) bool {
	streak := 0
	for _, healthy := range points {
		if !healthy {
			streak = 0
			continue
		}
		streak++
		if streak >= 5 {
			return true
		}
	}
	return false
}

func TestFailureDashboardContract_ThresholdsAndRecoveryStreak(t *testing.T) {
	assert.True(t, dashboardDispatchValid(95, 100), "the exact 95 percent boundary is valid")
	assert.False(t, dashboardDispatchValid(94, 100))
	assert.False(t, dashboardUnverifiedInvalid(3, 1000))
	assert.True(t, dashboardUnverifiedInvalid(4, 1000))
	assert.False(t, dashboardUnverifiedInvalid(3, 10), "low-rate lanes retain the absolute floor")
	assert.True(t, dashboardUnverifiedInvalid(4, 10))
	assert.False(t, dashboardUnverifiedInvalid(10, 10000))
	assert.True(t, dashboardUnverifiedInvalid(11, 10000))
	assert.False(t, dashboardRecovered([]bool{true, true, false, true, true, true, true}))
	assert.True(t, dashboardRecovered([]bool{false, true, true, true, true, true}))
}

func TestFailureDashboardContract_DocPinsCadenceAndMissingSeriesSemantics(t *testing.T) {
	encoded, err := os.ReadFile("../../docs/load-testing/failure-testing/dashboard-evidence-contract.md")
	require.NoError(t, err)
	contract := string(encoded)
	for _, required := range []string{
		"Prometheus scrape interval: 30 seconds",
		"Evaluation lookback: 2 minutes",
		"Evaluation step: 1 minute",
		"Minimum samples per required series in each lookback: 3",
		"Recovery: 5 consecutive healthy evaluation points",
		"Minimum post-remediation evaluation window: 6 minutes",
		"A missing series is unknown, never zero",
		"or vector(0)",
		"Existing loadgen metrics",
		"Metrics added by this work",
		"Externally owned metrics",
		"NATS topology, leader, and quorum",
	} {
		assert.Contains(t, contract, required)
	}
}

func TestFailureDashboardContract_ObserverRatioUsesMatchingBoundedSelectors(t *testing.T) {
	encoded, err := os.ReadFile("../../docs/load-testing/failure-testing/dashboard-evidence-contract.md")
	require.NoError(t, err)
	contract := string(encoded)

	assert.Contains(t, contract,
		`loadgen_failure_observer_eligible_total{scenario="message_soak",lane="$lane",observer="$observer"}`,
	)
	assert.Contains(t, contract,
		`loadgen_failure_observations_total{scenario="message_soak",lane="$lane",observer="$observer",result="unverified"}`,
	)
}

func TestFailureDashboardContract_AckFloorStallIsDocumentedAsAProxy(t *testing.T) {
	encoded, err := os.ReadFile("../../docs/load-testing/failure-testing/dashboard-evidence-contract.md")
	require.NoError(t, err)
	contract := string(encoded)

	assert.Contains(t, contract, "loadgen_consumer_ack_floor_stall_seconds")
	assert.Contains(t, contract, "replace a true oldest-pending-age signal")
}

func TestFailureDashboardContract_UsesWorkloadOrientedScenario(t *testing.T) {
	encoded, err := os.ReadFile("../../docs/load-testing/failure-testing/dashboard-evidence-contract.md")
	require.NoError(t, err)
	contract := string(encoded)
	observerStart := strings.Index(contract, "## Observer validity")
	observerEnd := strings.Index(contract, "## Result interpretation")
	require.NotEqual(t, -1, observerStart)
	require.Greater(t, observerEnd, observerStart)
	observerQueries := contract[observerStart:observerEnd]

	assert.Contains(t, observerQueries, `scenario="message_soak"`)
	assert.NotContains(t, observerQueries, "cassandra_soak")
}

func TestFailureDashboardContract_BundledDashboardUsesCurrentMetricContract(t *testing.T) {
	encoded, err := os.ReadFile("deploy/grafana/dashboards/loadtest.json")
	require.NoError(t, err)
	dashboard := string(encoded)

	for _, query := range []string{
		`loadgen_failure_operations_total{scenario=\"message_soak\"}`,
		`loadgen_failure_inflight{scenario=\"message_soak\"}`,
		`loadgen_failure_observations_total{scenario=\"message_soak\"}`,
		"loadgen_failure_wal_flush_duration_seconds_bucket",
		"loadgen_failure_wal_flush_batch_size_sum",
		"loadgen_failure_wal_appends_total",
		"loadgen_failure_evidence_flush_duration_seconds_bucket",
	} {
		assert.Contains(t, dashboard, query)
	}
	assert.NotContains(t, dashboard, "cassandra_soak")
}

func TestFailureObservationDeploymentContract_AcceptsBoundedTestEnvironment(t *testing.T) {
	schema, err := os.ReadFile("deploy/k8s/values.schema.json")
	require.NoError(t, err)
	readme, err := os.ReadFile("README.md")
	require.NoError(t, err)

	assert.Contains(t, string(schema), `"enum": ["local", "test", "staging", "production"]`)
	assert.Contains(t, string(readme), "`local`, `test`, `staging`, or `production`")
	values, err := os.ReadFile("deploy/k8s/values.yaml")
	require.NoError(t, err)
	assert.Contains(t, string(values), "environment: staging")
}

func TestFailureDashboardContract_ObservabilityInventoryUsesSplitSaturationMetrics(t *testing.T) {
	encoded, err := os.ReadFile("../../docs/specs/o11y/storage-dependency-metrics.md")
	require.NoError(t, err)
	contract := string(encoded)

	assert.Contains(t, contract, "loadgen_soak_lane_saturation_total")
	assert.Contains(t, contract, "loadgen_soak_global_saturation_total")
	assert.NotContains(t, contract, "loadgen_soak_saturation_total")
}

func TestFailureRuntimeControlFollowUp_PinsAuthenticatedStatusAndPausedEvidence(t *testing.T) {
	encoded, err := os.ReadFile("../../docs/load-testing/failure-testing/runtime-control-api.md")
	require.NoError(t, err)
	contract := string(encoded)
	for _, required := range []string{
		"authenticated `GET /control/status`",
		"`loadgen_dispatch_enabled == 0`",
		"INCONCLUSIVE",
		"no accumulated deficit",
	} {
		assert.Contains(t, contract, required)
	}
}

func TestFailureObservationDeploymentContract_UsesIndependentRecipientObserverValues(t *testing.T) {
	schema, err := os.ReadFile("deploy/k8s/values.schema.json")
	require.NoError(t, err)
	configMap, err := os.ReadFile("deploy/k8s/templates/configmap.yaml")
	require.NoError(t, err)
	values := string(schema)
	environment := string(configMap)
	for _, required := range []string{
		`"recipientObserver"`, `"queue"`, `"connections"`,
		"SOAK_RECIPIENT_OBSERVER_ENABLED", "SOAK_RECIPIENT_OBSERVER_QUEUE",
		"SOAK_RECIPIENT_OBSERVER_CONNECTIONS",
	} {
		assert.Contains(t, values+environment, required)
	}
	for _, removed := range []string{
		"failureEvidence", "SOAK_FAILURE_MANIFEST_PATH",
		"SOAK_FAILURE_TIMELINE_PATH", "SOAK_FAILURE_REPORT_DIR",
	} {
		assert.NotContains(t, values+environment, removed)
	}
}

func TestFailureObservationScope_FormalCampaignArtifactsAreAbsent(t *testing.T) {
	// Scoped to loadgen's own files. The root Makefile used to be checked here
	// too, which made this package's tests depend on a file whose content has
	// nothing to do with loadgen's behaviour — a false alarm waiting for an
	// unrelated edit.
	paths := []string{
		"soak_config.go",
		"soak_main.go",
		"deploy/docker-compose.yml",
		"README.md",
	}
	for _, path := range paths {
		encoded, err := os.ReadFile(path)
		require.NoError(t, err)
		for _, removed := range []string{
			"SOAK_FAILURE_MANIFEST_PATH",
			"SOAK_FAILURE_TIMELINE_PATH",
			"SOAK_FAILURE_REPORT_DIR",
			"failureEvidence.enabled",
		} {
			assert.NotContains(t, string(encoded), removed, "path=%s", path)
		}
	}
	for _, removedPath := range []string{
		"failure_manifest.go",
		"failure_timeline.go",
		"failure_verdict.go",
		"failure_report.go",
		"failure_runtime.go",
		"deploy/k8s/templates/failure-manifest-configmap.yaml",
	} {
		_, err := os.Stat(removedPath)
		assert.ErrorIs(t, err, os.ErrNotExist, "path=%s", removedPath)
	}
}
