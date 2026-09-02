package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const candidateDashboardPath = "deploy/grafana/dashboards/loadtest-campaign-v2.json"

type candidateDashboardTarget struct {
	Expr string `json:"expr"`
}

type candidateDashboardPanel struct {
	Title       string                     `json:"title"`
	Type        string                     `json:"type"`
	Collapsed   bool                       `json:"collapsed"`
	Targets     []candidateDashboardTarget `json:"targets"`
	Panels      []candidateDashboardPanel  `json:"panels"`
	FieldConfig struct {
		Defaults struct {
			NoValue    string `json:"noValue"`
			Thresholds struct {
				Steps []struct {
					Color string `json:"color"`
				} `json:"steps"`
			} `json:"thresholds"`
		} `json:"defaults"`
	} `json:"fieldConfig"`
}

type candidateDashboard struct {
	UID         string                    `json:"uid"`
	Title       string                    `json:"title"`
	Description string                    `json:"description"`
	Panels      []candidateDashboardPanel `json:"panels"`
	Templating  struct {
		List []struct {
			Name        string `json:"name"`
			IncludeAll  bool   `json:"includeAll"`
			Description string `json:"description"`
			Current     struct {
				Text  any `json:"text"`
				Value any `json:"value"`
			} `json:"current"`
		} `json:"list"`
	} `json:"templating"`
}

func loadCandidateDashboard(t *testing.T) (string, candidateDashboard, []string) {
	t.Helper()

	encoded, err := os.ReadFile(candidateDashboardPath)
	require.NoError(t, err)

	var dashboard candidateDashboard
	require.NoError(t, json.Unmarshal(encoded, &dashboard))

	queries := make([]string, 0, 128)
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case map[string]any:
			if expr, ok := typed["expr"].(string); ok {
				queries = append(queries, expr)
			}
			for _, child := range typed {
				walk(child)
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	var raw any
	require.NoError(t, json.Unmarshal(encoded, &raw))
	walk(raw)

	return string(encoded), dashboard, queries
}

func flattenCandidatePanels(panels []candidateDashboardPanel) []candidateDashboardPanel {
	flattened := make([]candidateDashboardPanel, 0, len(panels))
	for index := range panels {
		panel := &panels[index]
		flattened = append(flattened, *panel)
		flattened = append(flattened, flattenCandidatePanels(panel.Panels)...)
	}
	return flattened
}

func requireCandidatePanel(t *testing.T, panels []candidateDashboardPanel, title string) candidateDashboardPanel {
	t.Helper()

	flattened := flattenCandidatePanels(panels)
	for index := range flattened {
		if flattened[index].Title == title {
			return flattened[index]
		}
	}
	require.Failf(t, "candidate dashboard panel missing", "title=%q", title)
	return candidateDashboardPanel{}
}

func requireCandidateQuery(t *testing.T, queries []string, fragments ...string) {
	t.Helper()

	for _, query := range queries {
		matched := true
		for _, fragment := range fragments {
			if !strings.Contains(query, fragment) {
				matched = false
				break
			}
		}
		if matched {
			return
		}
	}
	require.Failf(t, "candidate dashboard query missing", "required fragments: %q", fragments)
}

func TestCandidateDashboardContract_IsIndependentFromLegacyDashboard(t *testing.T) {
	_, candidate, _ := loadCandidateDashboard(t)
	legacy, err := os.ReadFile("deploy/grafana/dashboards/loadtest.json")
	require.NoError(t, err)
	var legacyIdentity struct {
		UID   string `json:"uid"`
		Title string `json:"title"`
	}
	require.NoError(t, json.Unmarshal(legacy, &legacyIdentity))

	assert.Equal(t, "load-test-campaign-v2", candidate.UID)
	assert.Equal(t, "Load Test Campaign (Candidate)", candidate.Title)
	assert.NotEqual(t, legacyIdentity.UID, candidate.UID)
	assert.NotEqual(t, legacyIdentity.Title, candidate.Title)
}

func TestCandidateDashboardContract_HistoricalPanelsUseSelectedWindow(t *testing.T) {
	_, dashboard, _ := loadCandidateDashboard(t)
	coverage := requireCandidatePanel(t, dashboard.Panels, "Observation coverage")
	require.Len(t, coverage.Targets, 1)
	assert.Contains(t, coverage.Targets[0].Expr, "avg_over_time")
	assert.Contains(t, coverage.Targets[0].Expr, "$__range")
	assert.Contains(t, coverage.Targets[0].Expr, "up")

	attainment := requireCandidatePanel(t, dashboard.Panels, "Steady-state target attainment")
	require.Len(t, attainment.Targets, 1)
	assert.Contains(t, attainment.Targets[0].Expr, "quantile_over_time(0.5")
	assert.Contains(t, attainment.Targets[0].Expr, "[$__range:$__interval]")

	for _, title := range []string{
		"Soak run identity (selected window)",
		"Loadgen NATS connection (selected window)",
		"Failure observers (selected window)",
		"Soak control plane (selected window)",
		"Consumer telemetry (selected window)",
	} {
		panel := requireCandidatePanel(t, dashboard.Panels, title)
		require.NotEmpty(t, panel.Targets, "panel=%s", title)
		for _, target := range panel.Targets {
			assert.Contains(t, target.Expr, "_over_time(", "panel=%s query=%s", title, target.Expr)
			assert.Contains(t, target.Expr, "$__range", "panel=%s query=%s", title, target.Expr)
		}
	}
}

func TestCandidateDashboardContract_NoDataStatesExplainApplicability(t *testing.T) {
	encoded, dashboard, _ := loadCandidateDashboard(t)
	for _, title := range []string{
		"Soak run identity (selected window)",
		"Failure observers (selected window)",
		"Soak control plane (selected window)",
	} {
		panel := requireCandidatePanel(t, dashboard.Panels, title)
		assert.Contains(t, panel.FieldConfig.Defaults.NoValue, "SOAK ONLY", "panel=%s", title)
	}

	for _, title := range []string{
		"OOM-killed loadgen containers (Kubernetes only)",
		"Loadgen memory limit utilization (Kubernetes only)",
	} {
		panel := requireCandidatePanel(t, dashboard.Panels, title)
		assert.Contains(t, panel.FieldConfig.Defaults.NoValue, "KUBERNETES METRIC UNAVAILABLE", "panel=%s", title)
	}
	assert.Contains(t, encoded, "N/A (NO PENDING WORK)")
}

func TestCandidateDashboardContract_SoakMetricsRequireRunIdentity(t *testing.T) {
	_, dashboard, _ := loadCandidateDashboard(t)
	for _, title := range []string{
		"Soak control plane (selected window)",
		"Soak ledger WAL size and abandoned journals",
		"Soak control-plane freshness and attempts",
		"Soak ledger accounting gaps",
		"Soak reconcile capacity and lag",
		"Mutation targets missing",
		"Encryption preflight",
		"Operations recovered from the ledger",
		"Soak member candidate pool and room budget",
	} {
		panel := requireCandidatePanel(t, dashboard.Panels, title)
		require.NotEmpty(t, panel.Targets, "panel=%s", title)
		for _, target := range panel.Targets {
			assert.Contains(t, target.Expr, "and on()", "panel=%s query=%s", title, target.Expr)
			assert.Contains(t, target.Expr, "loadgen_run_info", "panel=%s query=%s", title, target.Expr)
		}
	}
}

func TestCandidateDashboardContract_ZeroStatesRequireObservedEvidence(t *testing.T) {
	_, dashboard, queries := loadCandidateDashboard(t)

	for _, metric := range []string{
		"loadgen_failure_invalidations_total",
		"loadgen_failure_operations_total",
		"loadgen_soak_verifications_total",
	} {
		requireCandidateQuery(t, queries, metric, "or on()", "loadgen_run_info")
	}

	for _, title := range []string{"Soak error reasons", "Errors and retries"} {
		panel := requireCandidatePanel(t, dashboard.Panels, title)
		require.NotEmpty(t, panel.Targets)
		for _, target := range panel.Targets {
			assert.Contains(t, target.Expr, "or on", "panel=%s query=%s", title, target.Expr)
		}
	}

	consumer := requireCandidatePanel(t, dashboard.Panels, "Consumer telemetry (selected window)")
	require.Len(t, consumer.Targets, 5)
	assert.Contains(t, consumer.Targets[4].Expr, "or on (durable)")
	assert.Contains(t, consumer.Targets[4].Expr, "-1 + 0 *")
}

func TestCandidateDashboardContract_DefaultVariablesSelectLocalMetrics(t *testing.T) {
	encoded, dashboard, _ := loadCandidateDashboard(t)
	assert.NotContains(t, encoded, `.*loadgen.*|`)
	assert.Contains(t, encoded, `"name": "pod"`)
	assert.Contains(t, encoded, `"query": ".*"`)

	for _, variable := range dashboard.Templating.List {
		if variable.Name == "loadgen_job" {
			assert.Equal(t, "loadgen", variable.Current.Text)
			assert.Equal(t, "loadgen", variable.Current.Value)
			return
		}
	}
	require.Fail(t, "loadgen_job variable missing")
}

func TestCandidateDashboardContract_UsesCurrentMetricsAndStableScopes(t *testing.T) {
	encoded, _, queries := loadCandidateDashboard(t)
	for _, fragments := range [][]string{
		{"loadgen_failure_operations_total", `scenario="message_soak"`},
		{"loadgen_failure_inflight", `scenario="message_soak"`},
		{"loadgen_failure_observer_eligible_total", `scenario="message_soak"`},
		{"loadgen_failure_wal_flush_duration_seconds_bucket"},
		{"loadgen_failure_evidence_flush_duration_seconds_bucket"},
		{"loadgen_failure_reconcile_claims_total"},
		{"loadgen_failure_reconcile_lag_seconds_bucket"},
		{"loadgen_failure_invalidations_total", `reason=~"reconcile_capacity|reconcile_lag_range|lease_abort"`},
		{"loadgen_mongo_probe_attempts_total", `outcome="error"`},
		{"loadgen_soak_heartbeat_attempts_total"},
	} {
		requireCandidateQuery(t, queries, fragments...)
	}
	assert.NotContains(t, encoded, "cassandra_soak")

	for _, metric := range []string{
		"process_resident_memory_bytes",
		"process_cpu_seconds_total",
		"process_open_fds",
		"go_memstats_heap_inuse_bytes",
		"go_memstats_next_gc_bytes",
		"go_goroutines",
	} {
		for _, query := range queries {
			if strings.Contains(query, metric) {
				assert.Contains(t, query, `job=~"$loadgen_job"`, "query=%s", query)
			}
		}
	}
}

func TestCandidateDashboardContract_OverviewPrioritizesCampaignDecisions(t *testing.T) {
	_, dashboard, queries := loadCandidateDashboard(t)
	panels := flattenCandidatePanels(dashboard.Panels)
	titles := make(map[string]struct{}, len(panels))
	for _, panel := range panels {
		titles[panel.Title] = struct{}{}
	}

	for _, title := range []string{
		"How to read this campaign",
		"Observation coverage",
		"Steady-state target attainment",
		"Evidence validity blockers",
		"Unrecovered impact",
		"Correctness violations",
		"Service pipeline throughput",
		"Service-side terminal failures",
		"Recovery: operations still in flight",
	} {
		assert.Contains(t, titles, title)
	}

	for _, metric := range []string{
		"message_gatekeeper_messages_total",
		"message_worker_persistence_total",
		"broadcast_worker_recipient_deliveries_total",
		"notification_worker_outcomes_total",
		"chat_nats_terminal_failures_total",
	} {
		requireCandidateQuery(t, queries, metric)
	}
}

func TestCandidateDashboardContract_DefaultViewCollapsesDiagnostics(t *testing.T) {
	_, dashboard, _ := loadCandidateDashboard(t)
	rows := make(map[string]candidateDashboardPanel)
	for _, panel := range dashboard.Panels {
		if panel.Type == "row" {
			rows[panel.Title] = panel
		}
	}

	for _, title := range []string{
		"2. Measurement validity (mode and environment specific)",
		"3. Soak correctness (soak mode only)",
		"4. Workload-specific diagnostics",
		"5. Consumer backlog details",
	} {
		row, ok := rows[title]
		if assert.True(t, ok, "row=%s", title) {
			assert.True(t, row.Collapsed, "row=%s", title)
			assert.NotEmpty(t, row.Panels, "collapsed rows must own their panels")
		}
	}

	correctness := requireCandidatePanel(t, dashboard.Panels, "3. Soak correctness (soak mode only)")
	for _, title := range []string{"Evidence validity blockers", "Unrecovered impact", "Correctness violations"} {
		panel := requireCandidatePanel(t, correctness.Panels, title)
		assert.Contains(t, panel.FieldConfig.Defaults.NoValue, "NOT APPLICABLE")
		require.NotEmpty(t, panel.FieldConfig.Defaults.Thresholds.Steps)
		assert.Equal(t, "gray", panel.FieldConfig.Defaults.Thresholds.Steps[0].Color)
	}
}

func TestCandidateDashboardContract_OperationalQueriesDoNotMisclassifyTraffic(t *testing.T) {
	_, dashboard, _ := loadCandidateDashboard(t)

	oom := requireCandidatePanel(t, dashboard.Panels, "OOM-killed loadgen containers (Kubernetes only)")
	require.Len(t, oom.Targets, 1)
	assert.Contains(t, oom.Targets[0].Expr, "and on (namespace, pod)")
	assert.Contains(t, oom.Targets[0].Expr, `process_resident_memory_bytes{job=~"$loadgen_job"}`)

	pipeline := requireCandidatePanel(t, dashboard.Panels, "Service pipeline throughput")
	require.Len(t, pipeline.Targets, 4)
	assert.Contains(t, pipeline.Targets[3].Expr, `notification_worker_outcomes_total{result="sent"}`)

	recovery := requireCandidatePanel(t, dashboard.Panels, "Recovery: operations still in flight")
	require.Len(t, recovery.Targets, 3)
	for _, target := range recovery.Targets[1:] {
		assert.Contains(t, target.Expr, "sum(max by (stream_name, consumer_name)", "query=%s", target.Expr)
	}

	terminal := requireCandidatePanel(t, dashboard.Panels, "Service-side terminal failures")
	require.Len(t, terminal.Targets, 1)
	assert.Contains(t, terminal.Targets[0].Expr, "sum by (service_name, consumer, reason)")
	assert.Contains(t, terminal.Targets[0].Expr, " and on() ", "the explicit zero remains fail-closed on the full message pipeline")
	assert.Contains(t, terminal.FieldConfig.Defaults.NoValue, "PIPELINE NOT OBSERVED")
}

func TestCandidateDashboardContract_TargetAndVariablesCannotSilentlyBroadenScope(t *testing.T) {
	_, dashboard, _ := loadCandidateDashboard(t)

	attainment := requireCandidatePanel(t, dashboard.Panels, "Steady-state target attainment")
	require.Len(t, attainment.Targets, 1)
	assert.Contains(t, attainment.Targets[0].Expr, "loadgen_soak_configured_rate")
	assert.Contains(t, attainment.Targets[0].Expr, "or vector($target_rps)")

	throughput := requireCandidatePanel(t, dashboard.Panels, "Target vs achieved throughput")
	require.Len(t, throughput.Targets, 2)
	assert.Contains(t, throughput.Targets[0].Expr, "loadgen_soak_configured_rate")
	assert.Contains(t, throughput.Targets[0].Expr, "or vector($target_rps)")

	for _, variable := range dashboard.Templating.List {
		switch variable.Name {
		case "loadgen_job":
			assert.False(t, variable.IncludeAll)
		case "pod":
			assert.NotContains(t, strings.ToLower(variable.Description), "trailing pipe")
		}
	}
}

func TestCandidateDashboardContract_RequiredDiagnosticsAreVisible(t *testing.T) {
	encoded, dashboard, queries := loadCandidateDashboard(t)

	for _, metric := range []string{
		"loadgen_failure_observer_queue_depth",
		"loadgen_nats_current_outage_seconds",
		"loadgen_soak_retries_total",
	} {
		requireCandidateQuery(t, queries, metric)
	}

	assert.NotContains(t, encoded, `phase=\"run\"`)
	requireCandidateQuery(t, queries, "loadgen_soak_errors_total", `phase="measured"`)
	requireCandidateQuery(t, queries, "loadgen_soak_operations_total", `phase="measured"`)
	requireCandidateQuery(t, queries, "loadgen_soak_error_reasons_total", `phase="measured"`)

	soakRow := requireCandidatePanel(t, dashboard.Panels, "3. Soak correctness (soak mode only)")
	assert.True(t, soakRow.Collapsed)
	assert.Contains(t, strings.ToLower(dashboard.Description), "per-site prometheus")
}
