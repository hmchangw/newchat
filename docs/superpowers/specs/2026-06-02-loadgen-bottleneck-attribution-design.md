# loadgen Bottleneck Attribution (v1)

**Date:** 2026-06-02
**Status:** Design — approved for planning
**Scope:** `tools/loadgen` — messages workload + `max-rps(messages)` only

## Problem

When a load run breaches SLO, `loadgen` tells you *that* it broke (latency
p95/p99, error rate, or per-durable backlog growth) but not *which*
component is the bottleneck. Today a human cross-references loadgen's
verdict against the `tools/observability` Grafana dashboard (cAdvisor
container CPU/mem/net) and eyeballs the hot box. We want loadgen to fuse
those two signal sources automatically and name a culprit.

The raw signals already exist in two places:

- **loadgen** owns per-stage latency (E1/E2 split), per-durable consumer
  backlog deltas (`WorstDurable`/`WorstDelta`), and error classes.
- **cAdvisor** (in `tools/observability`) exposes per-container CPU, memory,
  and network for every container — Go services *and* dependencies
  (Mongo/Cassandra/Valkey/ES/NATS) — with no per-service instrumentation.

What's missing is the correlation layer that walks the known event flow and
attributes the breach to a stage and a resource.

## Goals / Non-goals

**Goals**
- On the breaching step of a `max-rps --workload=messages` ramp, append a
  `BOTTLENECK:` block to the existing verdict naming the culprit
  component, the saturated resource, a causal reason, and a confidence.
- Purely additive: when Prometheus is unreachable or data is thin, print
  `BOTTLENECK: undetermined (<reason>)` and never fail the run.

**Non-goals (v1)**
- history and members workloads (different stage graphs; history has no
  durable backlog signal). They get their own stage graphs later.
- Production use. Inherits the local-dev-only posture of cAdvisor.
- Absolute resource thresholds / cross-machine comparison.

## Integration point

The engine fires once, when the `max-rps` ramp identifies its first `TRIP`
step. At that moment loadgen already knows the breach reason, the hold
window `[start, end]`, and the breaching step's `rpsStepResult` plus the
prior steps' results. The engine takes the breach window, queries a
Prometheus that scrapes cAdvisor for per-container trends over the same
window (and the prior step's window, for the knee test), fuses them with
loadgen's owned stage signals, walks the messages stage graph, and appends:

```text
ANSWER: max RPS = 2000 (workload=messages, preset=medium)
        Next limit: E2 p95=143ms > 100ms
BOTTLENECK: message-worker (Cassandra-bound)
        message-worker consumer backlog +12k (first stage to back up)
        cassandra CPU plateaued at 3.8 cores across steps 1k→2k while load rose
        confidence: high
```

## Architecture & components

All new code lives in `tools/loadgen`, following the existing flat
`package main` layout. Each unit has one job:

- **`promclient.go`** — thin PromQL HTTP client over Resty (already in
  `go.mod`; CLAUDE.md mandates Resty for outbound HTTP with a timeout).
  One method: `RangeQuery(ctx, query, start, end, step) ([]series, error)`.
- **`stagegraph.go`** — declarative, ordered DAG of the messages pipeline.
  Each stage names its component(s), the cAdvisor container identity to
  query, and (where applicable) the durable consumer fronting it. Pure
  data + helpers, no I/O. The only workload-specific piece.
- **`identity.go`** — resolves a logical stage name → cAdvisor series
  selector, preferring the `container_label_com_docker_compose_service`
  label, falling back to a configured short-ID→name map. Pure function.
- **`attribution.go`** — the engine. Input: the breaching step's
  `rpsStepResult`, the prior steps' results (for the knee comparison), and
  a promclient (accepted as a **consumer-defined interface** so unit tests
  inject a fake — no real Prometheus in unit tests). Output:
  `bottleneckVerdict{Component, Resource, Reasons, Confidence}`.
- **`attribution_report.go`** — formats `bottleneckVerdict` into the
  `BOTTLENECK:` block, mirroring `maxrps_report.go`.

Wiring lives in existing `maxrps.go`: after the ramp finds the breaching
step, call the engine and hand its verdict to the reporter. New config is
parsed via `caarlos0/env` in `main.go` like everything else.

## Signals & the saturation definition

No container in the local stack has a CPU quota (only one `mem_limit`
exists), so container CPU% has no per-container denominator. "Saturated"
is therefore defined **relative to the ramp**, not to a limit.

Per-container series pulled over the breach window and the prior step's
window:

| Signal | PromQL (cAdvisor) | Bottleneck meaning |
|---|---|---|
| CPU cores | `rate(container_cpu_usage_seconds_total[…])` | **Knee/plateau**: usage stopped rising step-over-step while offered load rose → CPU-bound |
| CPU throttle | `rate(container_cpu_cfs_throttled_seconds_total[…])` | Non-zero only if limits ever added; strong direct signal when present |
| Memory | `container_memory_working_set_bytes` | Climb toward `mem_limit`, or a restart (counter reset) → memory-bound |
| Network | `rate(container_network_*_bytes_total[…])` | Corroborating only in v1 |

Plus loadgen's owned signals (no Prometheus needed): per-durable backlog
delta, E1/E2 latency split, error class.

**Knee test (core primitive).** A component is "saturated" if, between the
last `PASS` step and the breaching step, offered RPS rose materially but
its CPU did **not** rise (within `BOTTLENECK_KNEE_TOLERANCE`) — it
flat-lined while being asked to do more. This sidesteps the missing-quota
problem and is exactly what a bottleneck looks like.

## Attribution algorithm

1. **Causality walk (primary).** Walk the stage graph in flow order. For
   each stage evaluate two predicates: *backing up?* (its durable backlog
   delta > 0 or its latency series breached) and *saturated?* (knee test on
   its container). The culprit is the **first** stage that is both backing
   up **and** saturated → `<component> (<resource>-bound)`. Confidence
   **high**.
2. **Single-signal stages.** If a stage backs up but is **not** visibly
   saturated (e.g. waiting on Cassandra I/O with low CPU), attribute to its
   **downstream dependency** if that dependency *is* saturated
   (message-worker backs up + cassandra CPU knee → "message-worker,
   Cassandra-bound"). Confidence **high**. If neither the stage nor its
   dependency is saturated, name the backing-up stage with resource
   `unknown` (likely I/O / lock wait). Confidence **medium**.
3. **Saturation fallback (cAdvisor-led).** If stage signals are ambiguous
   (no clear first-backed-up stage), fall back to pure ranking: the
   container with the clearest knee / highest absolute core usage in the
   breach window wins. Confidence **low**, flagged as "resource-ranking
   fallback."
4. **Undetermined.** Prometheus unreachable, too few steps for a knee
   (e.g. breach on step 1), or nothing stands out →
   `BOTTLENECK: undetermined (<reason>)`. Never errors the run.

## Configuration

Env via `caarlos0/env`, prefixed, with defaults. `BOTTLENECK_PROM_URL` is
required only when attribution is enabled.

| Var | Default | Notes |
|---|---|---|
| `BOTTLENECK_ENABLED` | `true` | Master switch; off = today's behavior exactly |
| `BOTTLENECK_PROM_URL` | — | Prometheus that scrapes cAdvisor (e.g. `http://prometheus:9090`) |
| `BOTTLENECK_KNEE_TOLERANCE` | `0.10` | CPU rise below this fraction across the step = plateau |
| `BOTTLENECK_QUERY_STEP` | `5s` | Matches the scrape interval |
| `BOTTLENECK_CONTAINER_MAP` | — | Optional `shortid:name,…` identity fallback |

Deploy changes: add a cAdvisor scrape job to loadgen's deploy
`prometheus.yml`; update the deploy compose/README so `run-max-rps` brings
up cAdvisor alongside loadgen's Prometheus.

## Output

- The `BOTTLENECK:` block (above) appended after the existing
  `ANSWER:`/`Next limit:` lines.
- A bottleneck column added to the CSV when `--csv` is set.

## Error handling

- Prometheus errors/timeouts → `undetermined`, run still prints its normal
  verdict. The engine never returns an error that aborts the run.
- Counter resets (container restart) detected and treated as a
  memory-pressure signal, not a negative rate.
- Identity unresolved (no label, no map entry) → that container is reported
  by short-ID rather than dropped.

## Testing (TDD, ≥80% coverage)

- `attribution_test.go` — table-driven over synthetic `rpsStepResult`
  sequences + a fake promclient: CPU-knee on a worker;
  DB-bound-with-low-worker-CPU; ambiguous → fallback; too-few-steps →
  undetermined; prom-unreachable → undetermined.
- `stagegraph_test.go` / `identity_test.go` — pure-function tables (label
  present vs short-ID fallback).
- `attribution_report_test.go` — golden-string formatting incl.
  undetermined.

No integration test in v1: the fake promclient (injected via the
consumer-defined interface) covers the engine end-to-end at unit scope,
and there is no store/container dependency to exercise.
