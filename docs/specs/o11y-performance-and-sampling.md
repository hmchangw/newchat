# o11y — Performance & Sampling Design

What the o11y integration costs on the hot path, and how sampling controls it.
Consolidates the performance findings from the PR-1 branch review, adds a
repeatable in-process benchmark (with a **sampler-ratio** dimension the review
didn't cover), and specifies the sampling configuration to set before
production load.

> Status: DESIGN + micro-benchmark. Full-stack `tools/loadgen` A/B against a
> real environment is the Phase-4 acceptance item (§5) and is **not** run here.

---

## 1. TL;DR

- **Latency is not the risk.** Per-hop tracing cost is ~2µs; against real
  NATS + Mongo/Cassandra round-trips (ms) it disappears. o11y does **not** make
  requests slower for this workload.
- **The risks are volume and allocations:** with **no sampler set, everything is
  100% sampled** — ~10–20 spans/message platform-wide → collector/Tempo/network
  pressure and ~20–25KB extra garbage per message.
- **Export never blocks** message processing (BatchSpanProcessor drops on a full
  queue), so a dead collector degrades traces, not throughput.
- **Action:** set `OTEL_TRACES_SAMPLER=parentbased_traceidratio` +
  `OTEL_TRACES_SAMPLER_ARG` in deploy before production scale (§3). Everything
  else (probe-span filtering, `WithRequireParentSpan`) is already done in code.

---

## 2. Performance findings (from the PR-1 review, verified against o11y v0.8.0)

### 2.1 Export path is non-blocking — the load-bearing safety property
The SDK builds its tracer provider with the stock OTel `BatchSpanProcessor`
(queue 2048, batch 512, 5s schedule) that **drops spans when the queue is full
instead of blocking**, exporting OTLP/HTTP on a background goroutine. A dead or
slow collector cannot stall message processing — the failure mode is *silently
missing traces*, not latency. (Corollary: dashboard the dropped-span counter;
queue overflow is otherwise invisible.)

### 2.2 Hot-path per-message cost is µs-scale
Each hop pays: traceparent extract, span start/end, one histogram record
(atomic). Negligible against the workers' ms-scale DB I/O. The one spot worth
watching is **message-gatekeeper** — it only validates + republishes, so its
baseline work per message is the lowest and instrumentation's *relative* share
the highest (it also adopted sonic for GC budget, which span allocations claw
back).

### 2.3 Unset sampler = 100% sampling
`pkg/obs` delegates `OTEL_TRACES_SAMPLER[_ARG]` to the SDK and sets no default;
OTel then defaults to `parentbased_always_on`. No compose/deploy file sets it, so
today every message yields a full trace tree. Fine pre-production; the first knob
to turn before scale.

### 2.4 Confirmed non-issues
- **Logs:** the SDK slog handler only injects traceId/spanId from ctx then
  delegates to the JSON handler on **stdout** — no OTLP log export on the hot
  path.
- **Metrics:** Prometheus pull-based; one histogram record per op; label sets are
  fixed semconv (redis hook pre-builds pool attrs, gin uses route templates) —
  **no cardinality-explosion path**.
- **Valkey:** every worker call site sets `WithRequireParentSpan(true)` →
  unparented background probes short-circuit before span creation.
- **DB spans:** one span + one record per operation is noise against ms I/O.

### 2.5 Known waste (now addressed in code)
- `/healthz`+`/readyz` probe spans on the Gin services → **fixed**:
  `o11ygin.WithSkipPaths()` added to auth/portal/upload.
- OTLP retry/error log noise when no collector exists locally → addressed by the
  local o11y stack (`docs/specs/o11y-local-trace-verification.md` /
  `docker-local` collector compose).

---

## 3. Sampling design

### 3.1 What to set (deploy env, not code)
```bash
OTEL_TRACES_SAMPLER=parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG=0.1          # 10% — tune per environment
```
`pkg/obs` already forwards these to the SDK; **no code change** is required.
Recommended starting points:

| Environment | Ratio | Rationale |
|---|---|---|
| local / dev | `1.0` (or unset) | see every trace while developing |
| staging | `0.5` | enough volume to validate dashboards, half the cost |
| production | `0.1` → tune | start at 10%, lower if collector/Tempo pressure shows |

### 3.2 The detached-root caveat (important)
Every NATS hop is a **detached root + link** (not parent-child) — see
`o11y-trace-design.md` §0. `traceidratio` samples on the span's **own** trace ID,
so with detached roots **each hop is sampled independently**. Consequences:

- A single logical flow (browser → gatekeeper → 3 workers ≈ 5 traces) does **not**
  sample as a unit: at ratio `r`, each of its ~5 traces is independently kept with
  probability `r`, so at 10% you usually see **fragments** of a flow, not the
  whole constellation.
- `parentbased_*` only keeps a *within-service* subtree consistent (handler →
  its DB spans share one decision); it does **not** stitch across the link
  boundary because the link doesn't carry the parent's sampled flag as a parent.

To keep whole flows at low ratios, prefer one of (future, tracked in
`o11y-followups.md`):
1. **Tail sampling at the collector** (decide per-traceID-group after the fact) —
   the cleanest fix, keeps complete flows, zero app change.
2. A higher head ratio (accept the cost) until tail sampling exists.
3. Consistent head sampling by seeding each detached root's trace ID from the
   upstream (requires an o11y/nats option — not available today).

For pre-production, head `traceidratio` is fine; **record tail-sampling as the
production follow-up** rather than chasing consistent head sampling now.

### 3.3 Kill switch
Per-pillar toggles make any telemetry-suspected regression an **env-only
rollback**: `O11Y_TRACES_ENABLED=false` (and siblings) — no redeploy of code.
Document this in the runbook.

---

## 4. Benchmark — on / off / sampler-ratio

`pkg/obs/sampling_bench_test.go` measures **one NATS hop**: extract the upstream
traceparent, start a detached-root CONSUMER span with a link (the o11y/nats
model), stamp the 3 messaging attributes, inject traceparent out, end — under
four samplers. A drop exporter pays the BatchSpanProcessor enqueue cost of a
sampled span without a network collector.

```
go test ./pkg/obs/ -run '^$' -bench BenchmarkSpanHop -benchmem -count=5
```

### Results (Go 1.25, linux/amd64 ×4, median of 5; indicative — see caveats)

| Sampler | ns/op | B/op | allocs/op | vs off |
|---|---|---|---|---|
| **off** (NATS tracing disabled, noop tracer) | ~705 | 648 | 10 | — |
| **10%** (`parentbased_traceidratio=0.1`) | ~1,970 | 1,629 | 17 | +1.3µs |
| **1%** (`…=0.01`) | ~1,800 | 1,523 | 17 | +1.1µs |
| **100%** (`always_on` — worst case) | ~3,140 | 2,696 | 21 | +2.4µs |

Reading:
- **The ratio works:** 100% → 10% cuts ~37% of the time and ~40% of the bytes per
  hop. It reduces the **recording + export-enqueue** cost of sampled spans.
- **There is a fixed floor (~1.1µs / +7 allocs over off) paid even when a hop is
  *not* sampled** — the traceparent extract, the sampling decision, and the
  (non-recording) span-context object are unconditional. Sampling controls
  recording cost, not the span-machinery entry cost.
- **1% ≈ 10%** because at these ratios almost every hop is already non-sampled, so
  both sit near that floor; the delta from 10% is the occasional recorded span.
- Absolute ns are higher than the review's earlier per-hop figure (~2µs) because
  this bench includes `WithNewRoot` + a link + full extract/inject each iteration;
  the *shape* (off ≪ ratio ≪ 100%, allocation-dominated) is what matters.

### What this bench does NOT cover
- **OTLP export CPU** (protobuf marshal + HTTP on the background goroutine) —
  scales with *sampled* span volume; another reason to set a ratio. A
  throughput/cost concern, not request latency.
- **JetStream publish path** (needs an embedded `nats-server`): the review
  measured ~+4KB / +26 allocs per instrumented publish, latency within noise
  against an in-process PubAck.
- **Full-stack `tools/loadgen` A/B** — see §5.

---

## 5. Phase-4 acceptance: full-stack load A/B (to run on a real environment)

Not runnable in CI/sandbox (no Docker/registry). On a real stack:

1. Bring up the platform + the local o11y stack (collector/Tempo/Prometheus).
2. Run `tools/loadgen` `maxrps_messages` against **message-gatekeeper** twice,
   identical except the pillar toggle:
   - **off:** `O11Y_TRACES_ENABLED=false`
   - **on:** default (then repeat with `OTEL_TRACES_SAMPLER_ARG=0.1`)
3. Compare **max RPS** and **p99**; watch the BatchSpanProcessor dropped-span
   counter and collector CPU.
4. Expected from the mechanism analysis + §4: **max RPS and p99 within noise**;
   the measurable delta is fleet **allocation churn / GC** and collector-side
   cost, both governed by the sampler ratio.

Fill in when run:

| Metric | off | on (100%) | on (10%) |
|---|---|---|---|
| max RPS | _tbd_ | _tbd_ | _tbd_ |
| p99 (ms) | _tbd_ | _tbd_ | _tbd_ |
| alloc MB/s (fleet) | _tbd_ | _tbd_ | _tbd_ |
| dropped spans/s | 0 | _tbd_ | _tbd_ |

---

## 6. Summary of actions

| # | Action | Where | Status |
|---|---|---|---|
| 1 | `OTEL_TRACES_SAMPLER=parentbased_traceidratio` + arg | deploy env | **design (this doc)** — set before prod |
| 2 | Filter probe spans (`WithSkipPaths`) | auth/portal/upload | ✅ done |
| 3 | `WithRequireParentSpan(true)` for Valkey | all workers | ✅ done (pre-existing) |
| 4 | Dashboard the dropped-span counter | monitor stack | follow-up |
| 5 | Tail sampling at the collector (keep whole flows) | collector config | follow-up (`o11y-followups.md`) |
| 6 | Full-stack loadgen A/B (§5) | real env | Phase-4 acceptance |

See also: `docs/specs/o11y-trace-design.md` (§0 propagation model — why hops are
detached roots), `docs/specs/o11y-followups.md`,
`docs/specs/o11y-local-trace-verification.md`.
