# Design: `loadgen max-rps` — auto-find Max RPS under SLO

**Status:** Approved (brainstorming complete)
**Date:** 2026-05-28
**Scope:** `tools/loadgen/`

## 1. Background & goal

PR #234 ("feat(loadgen): daily-IM load scenario to find sustainable N") introduced a
**step-up-and-hold-under-SLO control loop**: it ramps a load parameter through a series of
steps, holds at each step, evaluates SLO signals, and reports the largest step value at which
all signals held (`ANSWER: N = …`).

This design applies the **same control loop** to a different axis: instead of ramping the
number of simulated users `N`, we ramp the **target request rate (RPS)** of the existing
**open-loop** load generators, to automatically find the **maximum RPS each workload can
sustain under an SLO**.

Two existing workloads are covered:

- **messages** — the `run` subcommand: an open-loop publisher into the messaging pipeline
  (`message-gatekeeper` → `MESSAGES_CANONICAL` → `message-worker` + `broadcast-worker`),
  measuring E1 (gatekeeper ack) and E2 (broadcast visibility) latency and sampling the
  `message-worker` / `broadcast-worker` consumer backlog.
- **history** — the `history-sustained` subcommand: an open-loop NATS request/reply workload
  against history-service's synchronous read handlers (`LoadHistory` + `GetThreadMessages`),
  backed by Cassandra + MongoDB. No JetStream consumer queue is involved.

### Relationship to PR #234 (decided)

PR #234's verdict / report / pending-poller code lives only on its unmerged branch
(`claude/gifted-rubin-ry8HI`); none of it is on `main`. This work is built **standalone on
`main`**, mirroring #234's design with **workload-agnostic helpers** and **`rps`-prefixed
identifiers** so there is:

- **no dependency** on the unmerged PR, and
- **no symbol collision** in `package main` whether #234 merges before or after this branch
  (#234 uses `Thresholds`, `StepResult`, `evaluateStep`, `percentile`, `parseStepList`,
  `renderConsole`, `writeDailyCSV`; this work uses `rpsThresholds`, `rpsStepResult`,
  `evaluateRPSStep`, `rpsPercentile`, `parseRPSSteps`, `renderRPSReport`, `writeRPSCSV`).

If/when #234 merges, converging the two implementations into shared helpers (or a small
`pkg/` library) is a mechanical refactor, not a rewrite.

## 2. CLI surface

```
loadgen max-rps --workload=messages|history --preset=<name> [flags]
```

| Flag | Default | Notes |
|------|---------|-------|
| `--workload` | `messages` | `messages` or `history` |
| `--preset` | (required) | an existing preset for the chosen workload (`BuiltinPreset` / `BuiltinHistoryPreset`) |
| `--steps` | messages `500,1k,2k,5k,10k` / history `200,500,1k,2k,5k` | explicit ordered RPS list; `k` suffix = ×1000 |
| `--warmup` | `10s` | per-step warmup (samples discarded) |
| `--hold` | `30s` | per-step measurement window |
| `--cooldown` | `5s` | per-step settle gap before next step |
| `--slo-p95` | `100ms` | applied to **every** gated latency series |
| `--slo-p99` | `250ms` | applied to **every** gated latency series |
| `--slo-error-rate` | `0.001` | `failed / attempted` (0.1%) |
| `--slo-pending-growth` | `1000` | **messages only**: per-durable end−start `NumPending` delta |
| `--rate-tolerance` | `0.05` | achieved-vs-target shortfall band for the INCONCLUSIVE guard |
| `--stop-on-trip` | `true` | stop the ramp at the first TRIP (does **not** stop on INCONCLUSIVE) |
| `--seed` | `42` | RNG seed (parity with existing subcommands) |
| `--csv` | "" | optional CSV output path |

Per-workload defaults for `--steps` are chosen because messages are fire-and-forget publishes
(can sustain high RPS) while history requests are bounded-concurrency request/reply (lower
ceiling). Both lists are fully overridable.

Validation: `--preset` required; `--steps` must parse to a non-empty ascending list of
positive ints; latency/error/tolerance thresholds must be > 0. `history` workload requires
`CASSANDRA_HOSTS` (same fail-fast as `history-sustained`).

## 3. Architecture

A generic engine drives a per-workload adapter. Everything lives in `tools/loadgen`
(`package main`), consistent with the existing flat loadgen layout.

### New files

- **`ramp.go`** — generic driver. `parseRPSSteps(string) ([]int, error)` (comma split,
  `k` suffix, ascending-positive validation); the step iterator that calls the adapter per
  step, applies `--stop-on-trip`, and tracks `lastPass`. Knows nothing about NATS/Mongo.
- **`verdict.go`** — `rpsThresholds`, `rpsStepInputs`, `rpsStepResult`, the pure
  `evaluateRPSStep(in rpsStepInputs, th rpsThresholds) rpsStepResult`, and `rpsPercentile`.
  Latency is modeled as **named series** so "E1+E2" and per-endpoint gate uniformly.
- **`maxrps_report.go`** — `renderRPSReport` (console table + `ANSWER:` line) and
  `writeRPSCSV` (one row per step).
- **`maxrps_messages.go`** — `messagesWorkload` adapter (implements `rpsWorkload`): reuses
  `Generator`, `Collector`, the E1/E2 subscriptions and `ConsumerSampler` to run the
  messaging pipeline at a given RPS for the hold window and harvest `rpsStepInputs`
  (E1+E2 latency series, attempted/failed counts, saturation count, achieved RPS, and
  consumer-pending deltas).
- **`maxrps_history.go`** — `historyWorkload` adapter: reuses `HistoryGenerator` and
  `HistoryCollector`; harvests per-endpoint latency series (LoadHistory, GetThreadMessages),
  error/timeout counts, saturation count, achieved RPS; **no** pending deltas.
- **`maxrps.go`** — `runMaxRPS(ctx, cfg, args)`: flag parsing, dependency wiring (NATS,
  Mongo, Valkey, and Cassandra for history), builds the adapter, runs the ramp, renders the
  report. Wired into `dispatch` in `main.go` as the `max-rps` case.

### The `rpsWorkload` interface (engine ↔ adapter seam)

```go
type rpsWorkload interface {
    // RunStep drives open-loop load at targetRPS. The engine handles phase
    // timing; RunStep blocks for (warmup+hold), resetting measurement at the
    // hold boundary, and returns the harvested inputs for this step.
    RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error)
    // Label is used in the ANSWER line / report header.
    Label() string
}
```

The engine owns warmup/hold/cooldown timing, `--stop-on-trip`, and `lastPass`; the adapter
owns "how to emit load and harvest a normalized result." This is the convergence seam that
maps onto #234's `envFactory`/`stepEnv` split.

### Normalized step inputs

```go
type latencySeries struct {
    Name    string          // "E1","E2"  OR  "history","thread"
    Samples []time.Duration
}

type rpsStepInputs struct {
    TargetRPS    int
    AchievedRPS  float64
    Latencies    []latencySeries
    AttemptedOps int
    FailedOps    int
    Saturation   int                     // open-loop self-saturation tally
    Pending      []consumerPendingDelta  // empty for history
}
```

## 4. Per-step lifecycle

For each RPS step, the engine runs:

```
activate rate  →  warmup  →  [hold start: reset collector + snapshot pending]
              →  hold (accumulate samples)
              →  [hold end: snapshot pending + harvest inputs]
              →  evaluate verdict  →  cooldown
```

NATS connection, subscriptions, consumer samplers, and the collector stay alive across
steps; each step simply re-points the generator at the new RPS. The run has no `--duration`
flag — its length is the sum over steps of `warmup + hold + cooldown`, plus an early stop on
the first TRIP when `--stop-on-trip` is set. A SIGINT/SIGTERM during any phase ends the run
after printing whatever results exist so far. A failed pending snapshot at either boundary
marks that step INCONCLUSIVE (cannot trust the backlog signal).

Measurement covers the **full hold window** (collector reset at hold start, read at hold
end). #234's documented "middle 60% of hold" window was never implemented and is unnecessary
here because the offered rate is stationary within a step.

## 5. SLO verdict

`evaluateRPSStep` applies this precedence (the **ordering is the key correctness point** and
deliberately differs from #234):

1. **TRIP** if any of:
   - any latency series p95 > `--slo-p95`, **or** any series p99 > `--slo-p99`;
   - error rate (`FailedOps / AttemptedOps`) > `--slo-error-rate`;
   - (messages only) any `consumerPendingDelta.Delta` > `--slo-pending-growth`.
   Each tripped condition appends a human-readable reason
   (e.g. `"E2 p95=143ms > 100ms"`, `"broadcast-worker pending +1240 > +1000"`).
2. **else INCONCLUSIVE** if `AchievedRPS < (1 − rateTolerance) × TargetRPS`
   (corroborated by a non-zero `Saturation` tally) — meaning *"the system looked healthy but
   the harness could not push the target rate, so the limiting factor is the load box, not
   the service under test."*
3. **else PASS** — record `lastPass = TargetRPS`.

### Why TRIP must precede the shortfall guard (differs from #234)

#234 evaluates its harness-health signal **first** and returns early, because its GC-pause /
goroutine-count proxy is independent of the server under test.

Our shortfall signal is **entangled** with server health: when the service saturates, it
backpressures the open-loop generator and `AchievedRPS` drops *because the server is slow*.
If the shortfall guard ran first, we would wrongly mark the very step that found the limit as
INCONCLUSIVE. Therefore server-induced backpressure (latency/pending/error over threshold)
must be classified as **TRIP**, and only a *healthy-but-cannot-push* step becomes
INCONCLUSIVE. This single rule is correct for both workloads:

- **messages** — publishes are fire-and-forget, so `AchievedRPS ≈ TargetRPS` almost always;
  the real ceiling shows up as consumer pending-growth and rising E2 latency → TRIP.
  INCONCLUSIVE here is rare (only if the NATS client/CPU can't emit fast enough).
- **history** — request/reply holds an in-flight slot until the reply, so as the server
  slows, slots fill, ticks drop, latency climbs → TRIP (correctly attributing the plateau to
  the server). A genuine box limit (healthy latency but can't push rate) → INCONCLUSIVE.

A real box-CPU signal (gopsutil) is a possible future corroborator but is unnecessary given
the shortfall rule.

## 6. Reporting

Console table, one row per step:

```
target_rps  achieved_rps  <per-series p95/p99>  err%  worst_pending_delta  verdict
```

followed by:

```
ANSWER: max RPS = <largest passing step> (workload=<messages|history>, preset=<name>)
        Next limit: <reasons of the FIRST tripped step>
```

`ANSWER: no step passed` when nothing held. CSV mirrors the table (one row per step) with
columns: `target_rps,achieved_rps,<series>_p95_ms,<series>_p99_ms,error_rate,attempted,
failed,worst_durable,worst_pending_delta,verdict,reasons`.

Exit code: reuse `DetermineExitCode` semantics — non-zero if no step passed or the run
errored; zero otherwise.

## 7. Testing (TDD — Red→Green→Refactor, commit per green step)

- **`verdict_test.go`** — table-driven `evaluateRPSStep`: PASS; TRIP on each signal (E1 p95,
  E2 p99, error rate, pending growth, per-endpoint latency); shortfall → INCONCLUSIVE;
  TRIP-beats-shortfall (high latency + low achieved → TRIP, not INCONCLUSIVE); boundary
  values (exactly at threshold); empty sample sets.
- **`ramp_test.go`** — `parseRPSSteps` (k-suffix, whitespace, bad tokens, non-ascending,
  empty); the step iterator against a **fake `rpsWorkload`**: stops on first TRIP, does
  **not** stop on INCONCLUSIVE, records every result, computes `lastPass` correctly.
- **`maxrps_report_test.go`** — console table format; ANSWER line for (some pass),
  (none pass), (last pass then trip); CSV header + row formatting.
- **Adapter pure-logic tests** — latency-series assembly and achieved-rate computation with
  fakes / no live NATS.
- **`integration_test.go`** (`//go:build integration`) — end-to-end `max-rps` 2-step ramp
  against testcontainers, reusing `pkg/testutil` (NATS for messages; NATS + Mongo + Cassandra
  for history), asserting a report is produced and the verdict classification is sane.

Coverage: meet the repo's 80% floor; target 90%+ on `verdict.go` / `ramp.go` (pure logic).

## 8. Deliverables beyond code

- `tools/loadgen/README.md` — a `max-rps` section (quick-start for both workloads,
  flag table, how to read the ANSWER line).
- `tools/loadgen/deploy/Makefile` — a `run-max-rps` target (parameterized by
  `WORKLOAD`/`PRESET`/`STEPS`).
- No `docs/client-api.md` change: this is tooling, not a client-facing handler.

## 9. Out of scope (YAGNI)

- Binary-search refinement between last-pass and first-trip (chose explicit-steps/last-pass).
- Auto-geometric ramp (`--rps-start/--rps-factor/--rps-max`).
- The `members` workload (could be a follow-up using the same engine).
- Cross-site / federation RPS.
- Real-CPU box-health via gopsutil.
- Grafana dashboard panels for the ramp.
- Per-user auth (keep the existing stub behavior of the underlying generators).
