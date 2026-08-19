package main

import (
	"math"
	"os"
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
