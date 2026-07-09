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
  local o11y stack (`docs/specs/o11y/o11y-local-trace-verification.md` /
  `docker-local` collector compose).

---

## 3. Sampling design

### 3.1 What to set (deploy env)
```bash
OTEL_TRACES_SAMPLER=parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG=0.1          # 10% — tune per environment
```
`pkg/obs` reads these and maps them to the SDK sampler (`samplerOptions`):
`always_on`/unset → 100%; `always_off` → drop all; `traceidratio` → raw ratio;
`parentbased_traceidratio` → `ParentBased(TraceIDRatioBased)` (recommended).
The **o11y SDK does not read `OTEL_TRACES_SAMPLER` itself** — before this wiring,
setting the env silently had no effect (SDK default 100%). Recommended starting
points:

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

**Why standard fixes don't fully solve it.** Ground truth from
`otelnats` (`ConsumerContextWithDeliver`): the consumer `deliver` span is started
with an **empty parent** (a fresh root) and only a *link* to the origin — so it
gets a **new trace ID** and is sampled **independently** by the ratio; the
origin's sampled flag is **not** inherited across the link.

- **Head `traceidratio`, same rate everywhere → fragments.** Each hop is an
  independent coin flip; a 5-hop flow at 10% rarely survives whole.
- **Tail sampling at the collector does NOT fix this either.** The OTel
  `tail_sampling` processor groups by **trace ID**, but here each hop is a
  *different* trace ID joined only by span **links** — there is no standard
  policy that keeps a link-connected constellation together. (Tail sampling is
  still useful for *other* policies — keep-on-error, keep-slow — just not for
  "keep whole flows".)

**The real fix — consistent head sampling driven by the entry decision:** make
the detached `deliver` span **inherit the origin's sampled flag** (from the
incoming `traceparent`) instead of rolling the ratio afresh. Then one decision at
the true entry (browser / first backend hop) cascades through every hop's
`traceparent` → every detached root honors it → the **whole flow is kept or
dropped as a unit**, while each hop keeps its own clean trace ID. This requires
an **upstream change to `flywindy/o11y` / `otelnats`** — spec'd in
`docs/specs/o11y/o11y-upstream-sampling-requirement.md` and tracked in
`o11y-followups.md` (F2).

**Interim stance:** run **100%** at current pre-production volume; when volume
forces sampling before the upstream fix lands, accept fragmented flows at a
head ratio (navigate by links between the traces that *were* sampled) rather than
pretending tail sampling stitches them.

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
   - **on:** default (100%), then repeat with
     `OTEL_TRACES_SAMPLER=parentbased_traceidratio OTEL_TRACES_SAMPLER_ARG=0.1`
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
| 1 | Read `OTEL_TRACES_SAMPLER[_ARG]` → SDK sampler | `pkg/obs` | ✅ done (`samplerOptions`) |
| 2 | Set `OTEL_TRACES_SAMPLER=parentbased_traceidratio` + arg | deploy env | before prod scale |
| 3 | Filter probe spans (`WithSkipPaths`) | auth/portal/upload | ✅ done |
| 4 | `WithRequireParentSpan(true)` for Valkey | all workers | ✅ done (pre-existing) |
| 5 | Dashboard the dropped-span counter | monitor stack | follow-up |
| 6 | **Inherit sampled flag across the NATS link** (keep whole flows) | upstream `o11y`/`otelnats` | follow-up — `o11y-upstream-sampling-requirement.md` |
| 7 | Full-stack loadgen A/B (§5) | real env | Phase-4 acceptance |

See also: `docs/specs/o11y/o11y-trace-design.md` (§0 propagation model — why hops are
detached roots), `docs/specs/o11y/o11y-upstream-sampling-requirement.md` (the upstream
fix for whole-flow sampling), `docs/specs/o11y/o11y-followups.md`,
`docs/specs/o11y/o11y-local-trace-verification.md`.
