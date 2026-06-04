# loadgen Bottleneck Attribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a `max-rps --workload=messages` ramp trips, append a `BOTTLENECK:` block to the verdict that names the culprit component and saturated resource, by fusing loadgen's per-stage signals with cAdvisor container CPU trends from Prometheus.

**Architecture:** A new attribution engine in `tools/loadgen` runs once, on the first tripped step. It reads the breaching step + the prior passing step (both now carrying their hold-window wall-clock times), walks a declarative messages stage-graph, and for each stage checks two predicates — *backing up* (durable backlog delta > 0 or its latency series breached the SLO) and *saturated* (CPU "knee": cores used barely rose between the passing and tripping windows even though offered RPS rose). The first stage that is both backing-up and saturated (or whose downstream dependency is saturated) is the culprit. Falls back to pure CPU ranking, then to `undetermined`. Purely additive — never fails the run.

**Tech Stack:** Go 1.25, `flag`-based CLI subcommands, Resty (via `pkg/restyutil`) for the Prometheus HTTP API, `caarlos0/env` for config, `testify` + `httptest` for tests. No new third-party dependencies.

---

## File Structure

**New files (all in `tools/loadgen/`, `package main`):**
- `promclient.go` / `promclient_test.go` — Prometheus `query_range` HTTP client; one `RangeQuery` method returning typed series.
- `stagegraph.go` / `stagegraph_test.go` — declarative messages pipeline stage-graph + dependency display names. Pure data.
- `identity.go` / `identity_test.go` — logical service name → cAdvisor PromQL selector (compose-service label, with short-ID fallback). Pure function.
- `attribution.go` / `attribution_test.go` — the engine: CPU-knee test, causality walk, fallback, undetermined. Takes a `promQuerier` interface so unit tests inject a fake.
- `attribution_report.go` / `attribution_report_test.go` — formats the `bottleneckVerdict` into the `BOTTLENECK:` block.

**Modified files:**
- `verdict.go` — add `HoldStart/HoldEnd time.Time` and `Pending []consumerPendingDelta` to `rpsStepResult`; populate in `evaluateRPSStep`.
- `ramp.go` — record each step's hold window wall-clock in `runRamp`.
- `main.go` — add `bottleneckConfig` to `config`.
- `maxrps.go` — build the engine after the ramp and pass its verdict to the reporter.
- `maxrps_report.go` — print the `BOTTLENECK:` block after `ANSWER:`; add culprit columns to the CSV trip row.
- `tools/loadgen/deploy/prometheus/prometheus.yml` — add a cAdvisor scrape job.
- `tools/loadgen/deploy/docker-compose.yml` + `README.md` — bring up cAdvisor; document the feature.

---

## Task 1: Carry the hold window + per-durable deltas on each step result

The engine needs (a) each step's measurement-window wall-clock times to query Prometheus over the right interval, and (b) the per-durable backlog deltas (today only the *worst* durable is kept). Add both to `rpsStepResult`.

**Files:**
- Modify: `tools/loadgen/verdict.go`
- Modify: `tools/loadgen/ramp.go`
- Test: `tools/loadgen/verdict_test.go` (add cases), `tools/loadgen/ramp_test.go` (add case)

- [ ] **Step 1: Write the failing test for the new result fields**

Add to `tools/loadgen/verdict_test.go`. This file currently imports only `testify/assert`; add `"github.com/stretchr/testify/require"` to its import block first.

```go
func TestEvaluateRPSStep_CopiesPendingAndWindow(t *testing.T) {
	in := &rpsStepInputs{
		TargetRPS:    1000,
		Hold:         30 * time.Second,
		AttemptedOps: 30000,
		Pending: []consumerPendingDelta{
			{Durable: "message-worker", Start: 0, End: 5000},
			{Durable: "broadcast-worker", Start: 0, End: 10},
		},
	}
	res := evaluateRPSStep(in, buildThresholds(100*time.Millisecond, 250*time.Millisecond, 0.001, 1000, 0.05))
	require.Len(t, res.Pending, 2)
	assert.Equal(t, "message-worker", res.Pending[0].Durable)
	assert.Equal(t, int64(5000), res.Pending[0].Delta())
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=loadgen`
Expected: FAIL — `res.Pending` undefined.

- [ ] **Step 3: Add the fields and populate them**

In `tools/loadgen/verdict.go`, add to the `rpsStepResult` struct (after `Reasons []string`):

```go
	// Pending carries every durable's backlog delta for the step (not just the
	// worst), so the bottleneck engine can map a delta to a pipeline stage.
	Pending []consumerPendingDelta
	// HoldStart/HoldEnd bound the step's measurement window in wall-clock time,
	// set by runRamp. Used to query container metrics over the same interval.
	HoldStart, HoldEnd time.Time
```

In `evaluateRPSStep`, right after the `res := rpsStepResult{...}` literal, add:

```go
	res.Pending = in.Pending
```

- [ ] **Step 4: Write the failing test for window capture in runRamp**

Add to `tools/loadgen/ramp_test.go` (reuse any existing fake `rpsWorkload`; if none exists, add this minimal one):

```go
type windowFakeWorkload struct{ hold time.Duration }

func (f windowFakeWorkload) Label() string { return "fake" }
func (f windowFakeWorkload) RunStep(ctx context.Context, rps int, warmup, hold time.Duration) (rpsStepInputs, error) {
	time.Sleep(2 * time.Millisecond) // simulate a measurement window
	return rpsStepInputs{TargetRPS: rps, Hold: hold, AttemptedOps: rps}, nil
}

func TestRunRamp_RecordsHoldWindow(t *testing.T) {
	results := runRamp(context.Background(), windowFakeWorkload{}, &rampConfig{
		Steps:      []int{100},
		Hold:       30 * time.Second,
		Thresholds: buildThresholds(time.Second, time.Second, 1, 1<<62, 0),
	})
	require.Len(t, results, 1)
	assert.False(t, results[0].HoldStart.IsZero(), "HoldStart should be set")
	assert.False(t, results[0].HoldEnd.IsZero(), "HoldEnd should be set")
	assert.True(t, results[0].HoldEnd.After(results[0].HoldStart))
}
```

- [ ] **Step 5: Run it to confirm it fails**

Run: `make test SERVICE=loadgen`
Expected: FAIL — `HoldStart`/`HoldEnd` are zero.

- [ ] **Step 6: Capture the window in runRamp**

In `tools/loadgen/ramp.go`, inside `runRamp`'s loop, replace:

```go
		in, err := w.RunStep(ctx, n, cfg.Warmup, cfg.Hold)
```

with:

```go
		stepStart := time.Now()
		in, err := w.RunStep(ctx, n, cfg.Warmup, cfg.Hold)
		stepEnd := time.Now()
```

and, right after `res := evaluateRPSStep(&in, cfg.Thresholds)`, add:

```go
		// RunStep does warmup then hold sequentially; approximate the hold
		// window as [start+warmup, end] so metric queries skip the ramp-up.
		res.HoldStart = stepStart.Add(cfg.Warmup)
		res.HoldEnd = stepEnd
```

- [ ] **Step 7: Run tests to confirm pass**

Run: `make test SERVICE=loadgen`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add tools/loadgen/verdict.go tools/loadgen/verdict_test.go tools/loadgen/ramp.go tools/loadgen/ramp_test.go
git commit -m "feat(loadgen): carry hold window + per-durable deltas on step result"
```

---

## Task 2: Prometheus range-query client

A thin client over the Prometheus HTTP API `GET /api/v1/query_range`, returning typed time-ordered samples per series. Built on the shared `restyutil.New`.

**Files:**
- Create: `tools/loadgen/promclient.go`
- Test: `tools/loadgen/promclient_test.go`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/promclient_test.go`:

```go
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPromClient_RangeQuery_ParsesMatrix(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/query_range", r.URL.Path)
		assert.NotEmpty(t, r.URL.Query().Get("query"))
		_, _ = w.Write([]byte(`{
			"status":"success",
			"data":{"resultType":"matrix","result":[
				{"metric":{"container_label_com_docker_compose_service":"cassandra"},
				 "values":[[100,"10.5"],[105,"11.0"]]}
			]}}`))
	}))
	defer srv.Close()

	c := newPromClient(srv.URL)
	start := time.Unix(100, 0)
	series, err := c.RangeQuery(context.Background(), `up`, start, start.Add(5*time.Second), 5*time.Second)
	require.NoError(t, err)
	require.Len(t, series, 1)
	assert.Equal(t, "cassandra", series[0].Labels["container_label_com_docker_compose_service"])
	require.Len(t, series[0].Samples, 2)
	assert.Equal(t, 10.5, series[0].Samples[0].V)
	assert.Equal(t, 11.0, series[0].Samples[1].V)
	assert.Equal(t, time.Unix(105, 0).UTC(), series[0].Samples[1].T.UTC())
}

func TestPromClient_RangeQuery_NonSuccessStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"boom"}`))
	}))
	defer srv.Close()

	_, err := newPromClient(srv.URL).RangeQuery(context.Background(), `up`, time.Unix(0, 0), time.Unix(5, 0), time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=loadgen`
Expected: FAIL — `newPromClient` undefined.

- [ ] **Step 3: Implement the client**

Create `tools/loadgen/promclient.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/restyutil"
)

// promSample is one (timestamp, value) point from a Prometheus matrix result.
type promSample struct {
	T time.Time
	V float64
}

// promSeries is one labelled time-series returned by a range query.
type promSeries struct {
	Labels  map[string]string
	Samples []promSample
}

// promClient queries the Prometheus HTTP API. It is the production promQuerier.
type promClient struct {
	rc *resty.Client
}

// newPromClient builds a client against a Prometheus base URL (e.g.
// "http://prometheus:9090"). A short timeout keeps a slow/missing Prometheus
// from stalling the end-of-run report.
func newPromClient(baseURL string) *promClient {
	return &promClient{rc: restyutil.New(baseURL, restyutil.WithTimeout(10*time.Second))}
}

// rangeQueryResponse mirrors the subset of the query_range payload we read.
type rangeQueryResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Values [][2]any          `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// RangeQuery runs a PromQL range query and returns one promSeries per result.
func (c *promClient) RangeQuery(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]promSeries, error) {
	resp, err := c.rc.R().
		SetContext(ctx).
		SetQueryParams(map[string]string{
			"query": query,
			"start": strconv.FormatInt(start.Unix(), 10),
			"end":   strconv.FormatInt(end.Unix(), 10),
			"step":  strconv.FormatFloat(step.Seconds(), 'f', -1, 64),
		}).
		Get("/api/v1/query_range")
	if err != nil {
		return nil, fmt.Errorf("query prometheus: %w", err)
	}

	var parsed rangeQueryResponse
	if err := json.Unmarshal(resp.Body(), &parsed); err != nil {
		return nil, fmt.Errorf("decode prometheus response: %w", err)
	}
	if parsed.Status != "success" {
		return nil, fmt.Errorf("prometheus query failed: %s", parsed.Error)
	}

	out := make([]promSeries, 0, len(parsed.Data.Result))
	for _, r := range parsed.Data.Result {
		s := promSeries{Labels: r.Metric}
		for _, v := range r.Values {
			ts, ok := v[0].(float64)
			if !ok {
				continue
			}
			raw, ok := v[1].(string)
			if !ok {
				continue
			}
			val, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				continue
			}
			s.Samples = append(s.Samples, promSample{T: time.Unix(int64(ts), 0).UTC(), V: val})
		}
		out = append(out, s)
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests to confirm pass**

Run: `make test SERVICE=loadgen`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/promclient.go tools/loadgen/promclient_test.go
git commit -m "feat(loadgen): add Prometheus range-query client"
```

---

## Task 3: Messages stage-graph

A declarative description of the messages pipeline: each stage's logical name, its cAdvisor compose-service container, the durable that fronts it (if any), the latency series that measures it (if any), and its downstream dependencies. Plus a display-name helper for dependencies.

**Files:**
- Create: `tools/loadgen/stagegraph.go`
- Test: `tools/loadgen/stagegraph_test.go`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/stagegraph_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessagesStageGraph_Shape(t *testing.T) {
	g := messagesStageGraph()
	require.Len(t, g, 3)
	assert.Equal(t, "message-gatekeeper", g[0].Name)
	assert.Equal(t, "E1", g[0].LatencySeries)
	assert.Empty(t, g[0].Durable)

	assert.Equal(t, "message-worker", g[1].Name)
	assert.Equal(t, "message-worker", g[1].Durable)
	assert.Equal(t, []string{"cassandra"}, g[1].DependsOn)

	assert.Equal(t, "broadcast-worker", g[2].Name)
	assert.Equal(t, "broadcast-worker", g[2].Durable)
	assert.Equal(t, "E2", g[2].LatencySeries)
}

func TestDependencyDisplayName(t *testing.T) {
	assert.Equal(t, "Cassandra", dependencyDisplayName("cassandra"))
	assert.Equal(t, "MongoDB", dependencyDisplayName("mongo"))
	assert.Equal(t, "valkey", dependencyDisplayName("valkey")) // unknown -> as-is
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=loadgen`
Expected: FAIL — `messagesStageGraph` undefined.

- [ ] **Step 3: Implement the stage-graph**

Create `tools/loadgen/stagegraph.go`:

```go
package main

// stage is one node in a workload's pipeline. The bottleneck engine walks
// stages in flow order, mapping loadgen's signals (durable backlog, latency
// series) and cAdvisor's container metrics onto each one.
type stage struct {
	Name          string   // logical component name, used in the verdict
	Container     string   // cAdvisor compose-service label value
	Durable       string   // durable consumer fronting this stage; "" if none
	LatencySeries string   // loadgen latency series measuring this stage; "" if none
	DependsOn     []string // downstream components this stage calls into
}

// messagesStageGraph describes the messages pipeline:
// publish -> message-gatekeeper -> MESSAGES_CANONICAL -> {message-worker (Cassandra),
// broadcast-worker (MongoDB membership + Valkey keys)}. E1 latency measures the
// gatekeeper front door; E2 is the end-to-end publish->broadcast time.
func messagesStageGraph() []stage {
	return []stage{
		{Name: "message-gatekeeper", Container: "message-gatekeeper", LatencySeries: "E1"},
		{Name: "message-worker", Container: "message-worker", Durable: "message-worker", DependsOn: []string{"cassandra"}},
		{Name: "broadcast-worker", Container: "broadcast-worker", Durable: "broadcast-worker", LatencySeries: "E2", DependsOn: []string{"mongo", "valkey"}},
	}
}

// dependencyDisplayName maps an internal dependency key to a human label for
// the verdict ("message-worker (Cassandra-bound)"). Unknown keys pass through.
func dependencyDisplayName(dep string) string {
	switch dep {
	case "cassandra":
		return "Cassandra"
	case "mongo":
		return "MongoDB"
	case "valkey":
		return "Valkey"
	default:
		return dep
	}
}
```

- [ ] **Step 4: Run tests to confirm pass**

Run: `make test SERVICE=loadgen`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/stagegraph.go tools/loadgen/stagegraph_test.go
git commit -m "feat(loadgen): add messages pipeline stage-graph"
```

---

## Task 4: Container identity resolver

Builds the PromQL metric selector for a logical service name. Prefers the cAdvisor compose-service label; if the operator supplied a `shortid:name` fallback map (for hosts where cAdvisor doesn't populate the label), it selects by the cgroup-path `id` instead.

**Files:**
- Create: `tools/loadgen/identity.go`
- Test: `tools/loadgen/identity_test.go`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/identity_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseContainerMap(t *testing.T) {
	m, err := parseContainerMap("0a1b2c3d4e5f:cassandra,deadbeef0000:mongo")
	require.NoError(t, err)
	assert.Equal(t, "0a1b2c3d4e5f", m["cassandra"])
	assert.Equal(t, "deadbeef0000", m["mongo"])
}

func TestParseContainerMap_Empty(t *testing.T) {
	m, err := parseContainerMap("")
	require.NoError(t, err)
	assert.Empty(t, m)
}

func TestParseContainerMap_Malformed(t *testing.T) {
	_, err := parseContainerMap("noseparator")
	require.Error(t, err)
}

func TestIdentityResolver_Selector(t *testing.T) {
	r := identityResolver{fallback: map[string]string{"cassandra": "0a1b2c3d4e5f"}}
	assert.Equal(t, `container_label_com_docker_compose_service="message-worker"`, r.selector("message-worker"))
	assert.Equal(t, `id=~".*0a1b2c3d4e5f.*"`, r.selector("cassandra"))
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=loadgen`
Expected: FAIL — `parseContainerMap` / `identityResolver` undefined.

- [ ] **Step 3: Implement the resolver**

Create `tools/loadgen/identity.go`:

```go
package main

import (
	"fmt"
	"strings"
)

// identityResolver maps a logical service name to a cAdvisor PromQL label
// selector. The fallback map (name -> 12-char container short-ID) is used on
// hosts where cAdvisor cannot populate the compose-service label.
type identityResolver struct {
	fallback map[string]string
}

// parseContainerMap parses "shortid:name,shortid2:name2" into a name->shortid
// map. An empty string yields an empty map.
func parseContainerMap(s string) (map[string]string, error) {
	out := map[string]string{}
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for _, pair := range strings.Split(s, ",") {
		id, name, ok := strings.Cut(strings.TrimSpace(pair), ":")
		if !ok || id == "" || name == "" {
			return nil, fmt.Errorf("bad container-map entry %q (want shortid:name)", pair)
		}
		out[name] = id
	}
	return out, nil
}

// selector returns the inner PromQL label matcher (no metric name, no braces)
// that identifies the given service's container.
func (r identityResolver) selector(service string) string {
	if id, ok := r.fallback[service]; ok {
		return fmt.Sprintf(`id=~".*%s.*"`, id)
	}
	return fmt.Sprintf(`container_label_com_docker_compose_service=%q`, service)
}
```

- [ ] **Step 4: Run tests to confirm pass**

Run: `make test SERVICE=loadgen`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/identity.go tools/loadgen/identity_test.go
git commit -m "feat(loadgen): add cAdvisor container identity resolver"
```

---

## Task 5: Attribution engine — CPU-knee helper

The saturation primitive. Given a service and two windows (passing step, tripping step), query the CPU usage counter over each window, compute cores-used as `(lastSample - firstSample) / windowSeconds`, and decide "saturated" = the trip-window cores barely rose over the pass-window cores (a plateau) while above an idle floor.

**Files:**
- Create: `tools/loadgen/attribution.go`
- Test: `tools/loadgen/attribution_test.go`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/attribution_test.go`:

```go
package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProm returns canned counter samples per service name. The key is matched
// as a substring of the query (the query embeds the service selector).
type fakeProm struct {
	// byService[service] = samples for the *next* RangeQuery whose query
	// mentions that service. Windows are distinguished by start time via fn.
	fn  func(query string, start, end time.Time) []promSample
	err error
}

func (f fakeProm) RangeQuery(_ context.Context, query string, start, end time.Time, _ time.Duration) ([]promSeries, error) {
	if f.err != nil {
		return nil, f.err
	}
	samples := f.fn(query, start, end)
	if samples == nil {
		return nil, nil
	}
	return []promSeries{{Labels: map[string]string{}, Samples: samples}}, nil
}

func counterSamples(start time.Time, startVal, cores float64, windowSec int) []promSample {
	// Linear counter: startVal at t0, rising `cores` per second for windowSec.
	return []promSample{
		{T: start, V: startVal},
		{T: start.Add(time.Duration(windowSec) * time.Second), V: startVal + cores*float64(windowSec)},
	}
}

func TestEngine_cpuCores(t *testing.T) {
	start := time.Unix(1000, 0)
	q := fakeProm{fn: func(_ string, s, _ time.Time) []promSample {
		return counterSamples(s, 100, 2.5, 30) // 2.5 cores over 30s
	}}
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	cores, reset, ok := eng.cpuCores(context.Background(), "message-worker", start, start.Add(30*time.Second))
	require.True(t, ok)
	assert.False(t, reset)
	assert.InDelta(t, 2.5, cores, 0.001)
}

func TestEngine_cpuCores_CounterReset(t *testing.T) {
	start := time.Unix(1000, 0)
	q := fakeProm{fn: func(_ string, s, _ time.Time) []promSample {
		return []promSample{{T: s, V: 500}, {T: s.Add(30 * time.Second), V: 3}} // dropped -> restart
	}}
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	_, reset, ok := eng.cpuCores(context.Background(), "x", start, start.Add(30*time.Second))
	require.True(t, ok)
	assert.True(t, reset)
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=loadgen`
Expected: FAIL — `newBottleneckEngine` undefined.

- [ ] **Step 3: Implement the engine scaffold + cpuCores**

Create `tools/loadgen/attribution.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

const (
	// cpuIdleFloorCores: a container using less than this over the trip window
	// is treated as idle and never blamed.
	cpuIdleFloorCores = 0.05
)

// promQuerier is the consumer-defined seam over Prometheus, so unit tests can
// inject a fake without a live server.
type promQuerier interface {
	RangeQuery(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]promSeries, error)
}

// bottleneckVerdict is the engine's output, rendered as the BOTTLENECK: block.
type bottleneckVerdict struct {
	Component  string   // culprit component, "" when undetermined
	Resource   string   // "CPU", a dependency display name, or "unknown"
	Confidence string   // "high" | "medium" | "low"
	Reasons    []string // human-readable causal lines
	Determined bool     // false -> render "undetermined (<reason>)"
}

// bottleneckEngine fuses loadgen signals with cAdvisor CPU trends.
type bottleneckEngine struct {
	q        promQuerier
	ident    identityResolver
	knee     float64       // max relative CPU rise still counted as a plateau
	step     time.Duration // PromQL query step
}

func newBottleneckEngine(q promQuerier, ident identityResolver, knee float64, step time.Duration) *bottleneckEngine {
	return &bottleneckEngine{q: q, ident: ident, knee: knee, step: step}
}

// cpuCores returns mean cores used by service over [start,end], derived from
// the CPU usage counter. reset=true when the counter dropped (container
// restart) — callers treat that as a memory/restart signal, not a CPU rate.
func (e *bottleneckEngine) cpuCores(ctx context.Context, service string, start, end time.Time) (cores float64, reset bool, ok bool) {
	query := fmt.Sprintf(`container_cpu_usage_seconds_total{%s}`, e.ident.selector(service))
	series, err := e.q.RangeQuery(ctx, query, start, end, e.step)
	if err != nil {
		slog.Warn("cpu query failed", "service", service, "error", err)
		return 0, false, false
	}
	// Sum across any matching cgroup series (cAdvisor may emit several).
	var first, last float64
	var t0, t1 time.Time
	var have bool
	for _, s := range series {
		if len(s.Samples) < 2 {
			continue
		}
		first += s.Samples[0].V
		last += s.Samples[len(s.Samples)-1].V
		t0 = s.Samples[0].T
		t1 = s.Samples[len(s.Samples)-1].T
		have = true
	}
	if !have || !t1.After(t0) {
		return 0, false, false
	}
	if last < first {
		return 0, true, true
	}
	return (last - first) / t1.Sub(t0).Seconds(), false, true
}
```

- [ ] **Step 4: Run tests to confirm pass**

Run: `make test SERVICE=loadgen`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/attribution.go tools/loadgen/attribution_test.go
git commit -m "feat(loadgen): add bottleneck engine CPU-knee primitive"
```

---

## Task 6: Attribution engine — causality walk

The decision logic. Given the tripping step result, the last passing step result, and the stage-graph, produce a `bottleneckVerdict` via the precedence: high (stage CPU-knee) → high (dependency CPU-knee) → medium (backs up, no knee) → low (pure CPU ranking) → undetermined.

**Files:**
- Modify: `tools/loadgen/attribution.go`
- Test: `tools/loadgen/attribution_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `tools/loadgen/attribution_test.go`:

```go
func tripResult(window time.Time) *rpsStepResult {
	return &rpsStepResult{
		TargetRPS: 2000,
		HoldStart: window, HoldEnd: window.Add(30 * time.Second),
		Latencies: []seriesPercentile{
			{Name: "E1", Pct: Percentiles{P95: 20 * time.Millisecond}},
			{Name: "E2", Pct: Percentiles{P95: 200 * time.Millisecond}},
		},
		Pending: []consumerPendingDelta{
			{Durable: "message-worker", Start: 0, End: 12000},
			{Durable: "broadcast-worker", Start: 0, End: 0},
		},
	}
}

func passResult(window time.Time) *rpsStepResult {
	return &rpsStepResult{
		TargetRPS: 1000,
		HoldStart: window, HoldEnd: window.Add(30 * time.Second),
	}
}

var slo = buildThresholds(100*time.Millisecond, 250*time.Millisecond, 0.001, 1000, 0.05)

// stageProm returns per-service cores keyed by service, with a plateau for
// services in `plateau` (same cores in both windows) and growth otherwise.
func stageProm(passT, tripT time.Time, plateau map[string]float64) fakeProm {
	return fakeProm{fn: func(query string, s, _ time.Time) []promSample {
		for svc, cores := range plateau {
			if strings.Contains(query, svc) {
				return counterSamples(s, 0, cores, 30) // same cores both windows -> plateau
			}
		}
		// non-plateau services: grow a lot from pass to trip window
		base := 0.2
		if s.Equal(tripT) {
			base = 2.0
		}
		return counterSamples(s, 0, base, 30)
	}}
}

func TestEngine_DependencyBound(t *testing.T) {
	passT, tripT := time.Unix(1000, 0), time.Unix(2000, 0)
	// message-worker backs up; cassandra CPU plateaus -> Cassandra-bound, high.
	q := stageProm(passT, tripT, map[string]float64{"cassandra": 3.8, "message-worker": 0.4})
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), tripResult(tripT), passResult(passT), messagesStageGraph(), slo)
	require.True(t, v.Determined)
	assert.Equal(t, "message-worker", v.Component)
	assert.Equal(t, "Cassandra", v.Resource)
	assert.Equal(t, "high", v.Confidence)
}

func TestEngine_StageCPUBound(t *testing.T) {
	passT, tripT := time.Unix(1000, 0), time.Unix(2000, 0)
	// E1 (gatekeeper) breaches and gatekeeper CPU plateaus -> CPU-bound, high.
	trip := tripResult(tripT)
	trip.Latencies[0].Pct.P95 = 150 * time.Millisecond // E1 over SLO
	trip.Pending[0].End = 0                            // no worker backlog
	q := stageProm(passT, tripT, map[string]float64{"message-gatekeeper": 4.0})
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), trip, passResult(passT), messagesStageGraph(), slo)
	require.True(t, v.Determined)
	assert.Equal(t, "message-gatekeeper", v.Component)
	assert.Equal(t, "CPU", v.Resource)
	assert.Equal(t, "high", v.Confidence)
}

func TestEngine_BacksUpNoKnee_Medium(t *testing.T) {
	passT, tripT := time.Unix(1000, 0), time.Unix(2000, 0)
	// worker backs up, but nothing plateaus (all CPU still rising) -> medium/unknown.
	q := stageProm(passT, tripT, nil)
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), tripResult(tripT), passResult(passT), messagesStageGraph(), slo)
	require.True(t, v.Determined)
	assert.Equal(t, "message-worker", v.Component)
	assert.Equal(t, "unknown", v.Resource)
	assert.Equal(t, "medium", v.Confidence)
}

func TestEngine_NoBackup_FallbackRanking_Low(t *testing.T) {
	passT, tripT := time.Unix(1000, 0), time.Unix(2000, 0)
	trip := tripResult(tripT)
	trip.Pending[0].End = 0 // nothing backs up, no latency breach
	// cassandra has the clearest plateau at the highest cores -> low-confidence pick.
	q := stageProm(passT, tripT, map[string]float64{"cassandra": 3.8})
	eng := newBottleneckEngine(q, identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), trip, passResult(passT), messagesStageGraph(), slo)
	require.True(t, v.Determined)
	assert.Equal(t, "cassandra", v.Component)
	assert.Equal(t, "low", v.Confidence)
}

func TestEngine_NoPassStep_Undetermined(t *testing.T) {
	eng := newBottleneckEngine(stageProm(time.Unix(1, 0), time.Unix(2, 0), nil), identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), tripResult(time.Unix(2000, 0)), nil, messagesStageGraph(), slo)
	assert.False(t, v.Determined)
	assert.Contains(t, v.Reasons[0], "no passing step")
}

func TestEngine_PromError_Undetermined(t *testing.T) {
	eng := newBottleneckEngine(fakeProm{err: assertAnErr{}}, identityResolver{}, 0.10, 5*time.Second)
	v := eng.Diagnose(context.Background(), tripResult(time.Unix(2000, 0)), passResult(time.Unix(1000, 0)), messagesStageGraph(), slo)
	assert.False(t, v.Determined)
}

type assertAnErr struct{}

func (assertAnErr) Error() string { return "prom down" }
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=loadgen`
Expected: FAIL — `Diagnose` undefined.

- [ ] **Step 3: Implement Diagnose + helpers**

Append to `tools/loadgen/attribution.go`:

```go
// saturated reports whether service plateaued between the pass and trip windows
// — cores stayed above the idle floor but rose by less than the knee fraction
// even though offered RPS rose. A counter reset also counts (restart under
// pressure). dataOK=false means we couldn't measure it.
func (e *bottleneckEngine) saturated(ctx context.Context, service string, pass, trip *rpsStepResult) (sat, dataOK bool) {
	tripCores, reset, okT := e.cpuCores(ctx, service, trip.HoldStart, trip.HoldEnd)
	if !okT {
		return false, false
	}
	if reset {
		return true, true
	}
	if tripCores < cpuIdleFloorCores {
		return false, true
	}
	passCores, _, okP := e.cpuCores(ctx, service, pass.HoldStart, pass.HoldEnd)
	if !okP || passCores <= 0 {
		// No baseline: treat a high absolute trip usage as saturated.
		return tripCores >= 1.0, true
	}
	rise := (tripCores - passCores) / passCores
	return rise < e.knee, true
}

// stageBackingUp reports whether a stage is accumulating backlog or breaching
// its latency SLO at the tripping step.
func stageBackingUp(st stage, trip *rpsStepResult, th rpsThresholds) bool {
	if st.Durable != "" {
		for _, p := range trip.Pending {
			if p.Durable == st.Durable && p.Delta() > 0 {
				return true
			}
		}
	}
	if st.LatencySeries != "" {
		for _, sp := range trip.Latencies {
			if sp.Name == st.LatencySeries && (sp.Pct.P95 > th.P95 || sp.Pct.P99 > th.P99) {
				return true
			}
		}
	}
	return false
}

// Diagnose applies the attribution precedence and returns a verdict. It never
// returns an error; measurement gaps degrade to a lower confidence or to
// undetermined.
func (e *bottleneckEngine) Diagnose(ctx context.Context, trip, pass *rpsStepResult, graph []stage, th rpsThresholds) bottleneckVerdict {
	if pass == nil {
		return bottleneckVerdict{Reasons: []string{"no passing step before breach; cannot compute CPU knee"}}
	}

	// Pass 1: first backing-up stage that is itself CPU-saturated -> high.
	for _, st := range graph {
		if !stageBackingUp(st, trip, th) {
			continue
		}
		if sat, ok := e.saturated(ctx, st.Container, pass, trip); ok && sat {
			return bottleneckVerdict{
				Component: st.Name, Resource: "CPU", Confidence: "high", Determined: true,
				Reasons: []string{
					fmt.Sprintf("%s is the first stage to back up", st.Name),
					fmt.Sprintf("%s CPU plateaued between %d and %d rps while load rose", st.Container, pass.TargetRPS, trip.TargetRPS),
				},
			}
		}
	}

	// Pass 2: first backing-up stage whose downstream dependency is saturated -> high.
	for _, st := range graph {
		if !stageBackingUp(st, trip, th) {
			continue
		}
		for _, dep := range st.DependsOn {
			if sat, ok := e.saturated(ctx, dep, pass, trip); ok && sat {
				return bottleneckVerdict{
					Component: st.Name, Resource: dependencyDisplayName(dep), Confidence: "high", Determined: true,
					Reasons: []string{
						fmt.Sprintf("%s consumer backlog grew (first stage to back up)", st.Name),
						fmt.Sprintf("%s CPU plateaued between %d and %d rps while load rose", dep, pass.TargetRPS, trip.TargetRPS),
					},
				}
			}
		}
	}

	// Pass 3: first backing-up stage, nothing saturated -> medium / unknown.
	for _, st := range graph {
		if stageBackingUp(st, trip, th) {
			return bottleneckVerdict{
				Component: st.Name, Resource: "unknown", Confidence: "medium", Determined: true,
				Reasons: []string{fmt.Sprintf("%s backs up but no resource knee found — likely I/O or lock wait", st.Name)},
			}
		}
	}

	// Pass 4: nothing backed up -> rank containers by clearest saturation -> low.
	if v, ok := e.fallbackRanking(ctx, pass, trip, graph); ok {
		return v
	}

	// Pass 5: nothing stands out.
	return bottleneckVerdict{Reasons: []string{"no stage backed up and no container saturated in the breach window"}}
}

// fallbackRanking picks the saturated container with the highest trip-window
// cores across all stages and their dependencies. Confidence low.
func (e *bottleneckEngine) fallbackRanking(ctx context.Context, pass, trip *rpsStepResult, graph []stage) (bottleneckVerdict, bool) {
	seen := map[string]bool{}
	var best string
	var bestCores float64
	consider := func(svc string) {
		if seen[svc] {
			return
		}
		seen[svc] = true
		sat, ok := e.saturated(ctx, svc, pass, trip)
		if !ok || !sat {
			return
		}
		cores, _, _ := e.cpuCores(ctx, svc, trip.HoldStart, trip.HoldEnd)
		if cores > bestCores {
			bestCores, best = cores, svc
		}
	}
	for _, st := range graph {
		consider(st.Container)
		for _, dep := range st.DependsOn {
			consider(dep)
		}
	}
	if best == "" {
		return bottleneckVerdict{}, false
	}
	return bottleneckVerdict{
		Component: best, Resource: "CPU", Confidence: "low", Determined: true,
		Reasons: []string{fmt.Sprintf("resource-ranking fallback: %s had the clearest CPU plateau (%.1f cores)", best, bestCores)},
	}, true
}
```

- [ ] **Step 4: Run tests to confirm pass**

Run: `make test SERVICE=loadgen`
Expected: PASS (all `TestEngine_*` cases).

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/attribution.go tools/loadgen/attribution_test.go
git commit -m "feat(loadgen): add bottleneck causality walk + fallback"
```

---

## Task 7: Render the BOTTLENECK block

Format a `bottleneckVerdict` into the text block appended under `ANSWER:`.

**Files:**
- Create: `tools/loadgen/attribution_report.go`
- Test: `tools/loadgen/attribution_report_test.go`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/attribution_report_test.go`:

```go
package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderBottleneck_Determined(t *testing.T) {
	var sb strings.Builder
	renderBottleneck(&sb, bottleneckVerdict{
		Component: "message-worker", Resource: "Cassandra", Confidence: "high", Determined: true,
		Reasons: []string{"message-worker consumer backlog grew", "cassandra CPU plateaued"},
	})
	out := sb.String()
	assert.Contains(t, out, "BOTTLENECK: message-worker (Cassandra-bound)")
	assert.Contains(t, out, "message-worker consumer backlog grew")
	assert.Contains(t, out, "confidence: high")
}

func TestRenderBottleneck_Undetermined(t *testing.T) {
	var sb strings.Builder
	renderBottleneck(&sb, bottleneckVerdict{Reasons: []string{"prometheus unreachable"}})
	assert.Contains(t, sb.String(), "BOTTLENECK: undetermined (prometheus unreachable)")
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=loadgen`
Expected: FAIL — `renderBottleneck` undefined.

- [ ] **Step 3: Implement the renderer**

Create `tools/loadgen/attribution_report.go`:

```go
package main

import (
	"fmt"
	"io"
	"strings"
)

// renderBottleneck writes the BOTTLENECK: block. For an undetermined verdict it
// writes a single line naming why; otherwise it names the culprit, the causal
// reasons, and the confidence.
func renderBottleneck(w io.Writer, v bottleneckVerdict) {
	if !v.Determined {
		reason := "no signal"
		if len(v.Reasons) > 0 {
			reason = v.Reasons[0]
		}
		fmt.Fprintf(w, "BOTTLENECK: undetermined (%s)\n", reason)
		return
	}
	fmt.Fprintf(w, "BOTTLENECK: %s (%s-bound)\n", v.Component, v.Resource)
	for _, r := range v.Reasons {
		fmt.Fprintf(w, "        %s\n", r)
	}
	fmt.Fprintf(w, "        confidence: %s\n", v.Confidence)
}

// bottleneckCSVColumns returns the trip-row culprit columns appended to the CSV.
func bottleneckCSVColumns(v bottleneckVerdict) []string {
	if !v.Determined {
		return []string{"undetermined", "", ""}
	}
	return []string{v.Component, strings.ToLower(v.Resource), v.Confidence}
}
```

- [ ] **Step 4: Run tests to confirm pass**

Run: `make test SERVICE=loadgen`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/attribution_report.go tools/loadgen/attribution_report_test.go
git commit -m "feat(loadgen): render BOTTLENECK verdict block"
```

---

## Task 8: Config — bottleneck settings

Add the bottleneck config (env-driven) to the loadgen `config` struct.

**Files:**
- Modify: `tools/loadgen/main.go`
- Test: covered indirectly; no new unit test (config is parsed by `caarlos0/env`, exercised in Task 9 wiring). Add a small parse test to satisfy coverage.

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/config_bottleneck_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBottleneckConfig_Defaults(t *testing.T) {
	var c bottleneckConfig
	// zero value should be safe; the wiring treats Enabled=false as off.
	assert.False(t, c.Enabled)
	assert.Equal(t, "", c.PromURL)
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=loadgen`
Expected: FAIL — `bottleneckConfig` undefined.

- [ ] **Step 3: Add the config type and field**

In `tools/loadgen/main.go`, add this type above `type config struct`:

```go
// bottleneckConfig tunes the max-rps(messages) bottleneck attribution. It is
// additive: when Enabled is false (or PromURL is empty) the run behaves exactly
// as before.
type bottleneckConfig struct {
	Enabled       bool          `env:"ENABLED"        envDefault:"true"`
	PromURL       string        `env:"PROM_URL"       envDefault:""`
	KneeTolerance float64       `env:"KNEE_TOLERANCE" envDefault:"0.10"`
	QueryStep     time.Duration `env:"QUERY_STEP"     envDefault:"5s"`
	ContainerMap  string        `env:"CONTAINER_MAP"  envDefault:""`
}
```

Add this field to the `config` struct (after `MessageBucketHours`):

```go
	Bottleneck bottleneckConfig `envPrefix:"BOTTLENECK_"`
```

- [ ] **Step 4: Run tests to confirm pass**

Run: `make test SERVICE=loadgen`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/main.go tools/loadgen/config_bottleneck_test.go
git commit -m "feat(loadgen): add bottleneck attribution config"
```

---

## Task 9: Wire the engine into max-rps and the report

Build the engine after the ramp (messages workload only, when enabled and a step tripped), run `Diagnose`, and thread the verdict into the report (stdout block + CSV trip-row columns).

**Files:**
- Modify: `tools/loadgen/maxrps.go`
- Modify: `tools/loadgen/maxrps_report.go`
- Test: `tools/loadgen/maxrps_report_test.go` (add a render-with-bottleneck case)

- [ ] **Step 1: Write the failing test**

Add to the existing `tools/loadgen/maxrps_report_test.go` (ensure its import block includes `strings`, `testing`, and `testify/assert` + `testify/require`):

```go
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
```

Ensure the file imports `strings`, `testing`, and the testify packages.

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=loadgen`
Expected: FAIL — `renderRPSReportWithBottleneck` undefined.

- [ ] **Step 3: Refactor the reporter to accept an optional verdict**

In `tools/loadgen/maxrps_report.go`, rename the body of `renderRPSReport` into a new function and keep a thin wrapper. Replace the existing `func renderRPSReport(...)` signature line:

```go
func renderRPSReport(w io.Writer, results []rpsStepResult, workload, preset string) error {
```

with:

```go
func renderRPSReport(w io.Writer, results []rpsStepResult, workload, preset string) error {
	return renderRPSReportWithBottleneck(w, results, workload, preset, nil)
}

func renderRPSReportWithBottleneck(w io.Writer, results []rpsStepResult, workload, preset string, bn *bottleneckVerdict) error {
```

Then, at the end of `renderRPSReportWithBottleneck`, immediately before the final `return nil`, add:

```go
	if bn != nil {
		renderBottleneck(w, *bn)
	}
```

- [ ] **Step 4: Wire the engine in maxrps.go**

In `tools/loadgen/maxrps.go`, replace this block:

```go
	if err := renderRPSReport(os.Stdout, results, w.Label(), presetID); err != nil {
		slog.Warn("render report", "error", err)
	}
```

with:

```go
	var bn *bottleneckVerdict
	if *workload == "messages" {
		if v := diagnoseBottleneck(ctx, cfg, results, thresholds); v != nil {
			bn = v
		}
	}
	if err := renderRPSReportWithBottleneck(os.Stdout, results, w.Label(), presetID, bn); err != nil {
		slog.Warn("render report", "error", err)
	}
```

Then add this helper at the bottom of `tools/loadgen/maxrps.go`:

```go
// diagnoseBottleneck runs the attribution engine for a messages ramp that
// tripped. Returns nil when disabled, unconfigured, or no step tripped — the
// report then prints normally with no BOTTLENECK line.
func diagnoseBottleneck(ctx context.Context, cfg *config, results []rpsStepResult, th rpsThresholds) *bottleneckVerdict {
	bc := cfg.Bottleneck
	if !bc.Enabled || bc.PromURL == "" {
		return nil
	}
	trip := firstTrip(results)
	if trip == nil {
		return nil
	}
	var pass *rpsStepResult
	for i := range results {
		if results[i].Kind == verdictPass {
			pass = &results[i]
		}
	}
	fallback, err := parseContainerMap(bc.ContainerMap)
	if err != nil {
		slog.Warn("bad BOTTLENECK_CONTAINER_MAP; ignoring", "error", err)
		fallback = map[string]string{}
	}
	eng := newBottleneckEngine(newPromClient(bc.PromURL), identityResolver{fallback: fallback}, bc.KneeTolerance, bc.QueryStep)
	v := eng.Diagnose(ctx, trip, pass, messagesStageGraph(), th)
	return &v
}
```

- [ ] **Step 5: Add the CSV trip-row columns**

In `tools/loadgen/maxrps.go`, the CSV is written via `writeRPSCSV(f, results)`. Extend it to pass the verdict. Replace `writeRPSCSV(f, results)` with `writeRPSCSV(f, results, bn)`.

In `tools/loadgen/maxrps_report.go`, change `func writeRPSCSV(w io.Writer, results []rpsStepResult) error {` to:

```go
func writeRPSCSV(w io.Writer, results []rpsStepResult, bn *bottleneckVerdict) error {
```

In its header slice, append the three culprit columns:

```go
	header = append(header, "error_rate", "attempted", "failed", "saturation", "worst_durable", "worst_pending_delta", "verdict", "reasons", "bottleneck_component", "bottleneck_resource", "bottleneck_confidence")
```

In the per-row loop, after the existing `row = append(row, ...)` that ends with `strings.Join(r.Reasons, "; ")`, add:

```go
		if bn != nil && r.Kind == verdictTrip {
			row = append(row, bottleneckCSVColumns(*bn)...)
		} else {
			row = append(row, "", "", "")
		}
```

- [ ] **Step 6: Run tests to confirm pass**

Run: `make test SERVICE=loadgen`
Expected: PASS.

- [ ] **Step 7: Lint, then commit**

Run: `make lint` (fixes/flags formatting and vet issues)
Then:

```bash
git add tools/loadgen/maxrps.go tools/loadgen/maxrps_report.go tools/loadgen/maxrps_report_test.go
git commit -m "feat(loadgen): wire bottleneck attribution into max-rps report"
```

---

## Task 10: Deploy wiring + docs

Add a cAdvisor scrape job to loadgen's deploy Prometheus, bring cAdvisor up in the deploy compose, and document the feature in the README.

**Files:**
- Modify: `tools/loadgen/deploy/prometheus/prometheus.yml`
- Modify: `tools/loadgen/deploy/docker-compose.yml`
- Modify: `tools/loadgen/README.md`

- [ ] **Step 1: Read the current deploy compose**

Run: `cat tools/loadgen/deploy/docker-compose.yml`
Note the existing `prometheus` and `loadgen` service definitions, the network name, and how env is passed to `loadgen`.

- [ ] **Step 2: Add the cAdvisor scrape job**

In `tools/loadgen/deploy/prometheus/prometheus.yml`, add under `scrape_configs:`:

```yaml
  - job_name: cadvisor
    static_configs:
      - targets: ["cadvisor:8080"]
```

- [ ] **Step 3: Add cAdvisor to the deploy compose**

In `tools/loadgen/deploy/docker-compose.yml`, add a `cadvisor` service mirroring the proven `tools/observability` posture (privileged, `cgroup: host`, host mounts), joined to the same network the loadgen overlay uses, and add `BOTTLENECK_PROM_URL=http://prometheus:9090` to the `loadgen` service's environment. Use this service block:

```yaml
  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.49.1
    privileged: true
    cgroup: host
    pid: host
    devices:
      - /dev/kmsg
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8080/healthz"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 5s
```

If the `prometheus` service has a `depends_on`, add `cadvisor` to it.

- [ ] **Step 4: Verify compose parses**

Run: `docker compose -f tools/loadgen/deploy/docker-compose.yml config >/dev/null && echo OK`
Expected: `OK` (no YAML/compose errors). If Docker is unavailable in the environment, skip and note it.

- [ ] **Step 5: Document the feature in the README**

In `tools/loadgen/README.md`, under the `max-rps` section, add a subsection:

```markdown
### Bottleneck attribution

When a `max-rps --workload=messages` ramp trips, loadgen appends a
`BOTTLENECK:` block naming the culprit component, the saturated resource,
and a confidence:

```text
ANSWER: max RPS = 2000 (workload=messages, preset=medium)
        Next limit: E2 p95=143ms > 100ms
BOTTLENECK: message-worker (Cassandra-bound)
        message-worker consumer backlog grew (first stage to back up)
        cassandra CPU plateaued between 1000 and 2000 rps while load rose
        confidence: high
```

It fuses loadgen's per-stage signals (E1/E2 latency, per-durable backlog)
with cAdvisor container CPU trends from Prometheus. The deploy stack brings
up cAdvisor automatically. Tunables (env, `BOTTLENECK_` prefix):

| Var | Default | Notes |
|-----|---------|-------|
| `BOTTLENECK_ENABLED` | `true` | Set `false` to disable; run behaves as before. |
| `BOTTLENECK_PROM_URL` | (set in compose) | Prometheus that scrapes cAdvisor. Empty = disabled. |
| `BOTTLENECK_KNEE_TOLERANCE` | `0.10` | Max relative CPU rise still counted as a plateau. |
| `BOTTLENECK_QUERY_STEP` | `5s` | PromQL step; match the scrape interval. |
| `BOTTLENECK_CONTAINER_MAP` | (empty) | `shortid:name,…` fallback when cAdvisor omits the compose-service label. |

The verdict is best-effort: if Prometheus is unreachable or the data is too
thin (e.g. the breach was on the first step), the line reads
`BOTTLENECK: undetermined (<reason>)` and the run still reports normally.
```

- [ ] **Step 6: Commit**

```bash
git add tools/loadgen/deploy/prometheus/prometheus.yml tools/loadgen/deploy/docker-compose.yml tools/loadgen/README.md
git commit -m "feat(loadgen): bring up cAdvisor for bottleneck attribution + docs"
```

---

## Task 11: Final verification

- [ ] **Step 1: Full unit suite with race detector**

Run: `make test SERVICE=loadgen`
Expected: PASS, no data races.

- [ ] **Step 2: Coverage check (≥80% on new files)**

Run:
```bash
go test -coverprofile=/tmp/loadgen.cov ./tools/loadgen/... >/dev/null 2>&1; go tool cover -func=/tmp/loadgen.cov | grep -E 'promclient|stagegraph|identity|attribution'
```
Expected: each new file's functions ≥ 80%. If any are below, add table cases to the corresponding `_test.go` (e.g. malformed Prometheus JSON in `promclient_test.go`, empty graph in `attribution_test.go`) and re-run.

- [ ] **Step 3: Lint + SAST**

Run: `make lint && make sast-gosec`
Expected: clean. (No `InsecureSkipVerify`/unsafe conversions were introduced; if gosec flags the cAdvisor host mounts they live in compose YAML, not Go, and are out of gosec's scope.)

- [ ] **Step 4: Confirm the build**

Run: `make build SERVICE=loadgen`
Expected: builds successfully.

- [ ] **Step 5: Push the branch**

```bash
git push -u origin claude/magical-ramanujan-gA5rC
```

---

## Self-Review Notes (author)

- **Spec coverage:** promclient (Task 2) ✓; stage-graph causality (Tasks 3, 6) ✓; identity resolution (Task 4) ✓; knee/plateau saturation given no CPU limits (Task 5) ✓; precedence high→high→medium→low→undetermined (Task 6) ✓; config (Task 8) ✓; additive output + CSV (Tasks 7, 9) ✓; deploy + docs (Task 10) ✓; fake-promclient-only testing, no integration test (Tasks 2, 5, 6) ✓.
- **Type consistency:** `bottleneckVerdict`, `bottleneckEngine`, `newBottleneckEngine`, `promQuerier`, `promSeries`/`promSample`, `identityResolver`, `stage`, `Diagnose`, `cpuCores`, `saturated`, `stageBackingUp`, `renderBottleneck`, `renderRPSReportWithBottleneck`, `diagnoseBottleneck`, `bottleneckConfig` are defined once and referenced consistently across tasks.
- **Memory/network signals:** the spec lists them as corroborating-only for v1; the implementation treats a CPU counter reset as the restart/memory proxy and otherwise leans on the CPU knee. Pure memory-bound-without-restart attribution is explicitly deferred (matches "corroborating only in v1").
