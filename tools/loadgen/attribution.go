package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

const (
	// cpuSaturatedFloorCores is the absolute CPU bar for blaming a container's
	// own CPU. Below this, a plateaued container is treated as NOT CPU-bound
	// (it is idle, or low-and-flat because it is waiting on a dependency / I/O),
	// so attribution falls through to the dependency or to "unknown". Roughly
	// one fully-utilised core; a host-relative heuristic since no CPU limits
	// are set on the local stack.
	cpuSaturatedFloorCores = 1.0
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
	q     promQuerier
	ident identityResolver
	knee  float64       // max relative CPU rise still counted as a plateau
	step  time.Duration // PromQL query step
}

// newBottleneckEngine builds an engine over a promQuerier and identity resolver.
// knee is the max relative CPU rise still treated as a plateau; step is the
// PromQL query step.
func newBottleneckEngine(q promQuerier, ident identityResolver, knee float64, step time.Duration) *bottleneckEngine {
	return &bottleneckEngine{q: q, ident: ident, knee: knee, step: step}
}

// saturated reports whether service is CPU-bound at the breach: its trip-window
// CPU is above the saturation floor AND plateaued (rose by less than the knee
// fraction) even though offered RPS rose. A counter reset also counts (restart
// under pressure). A container below the floor is NOT blamed — it is idle or
// low-and-flat because it is waiting on something downstream. dataOK=false means
// the measurement itself failed (e.g. Prometheus unreachable).
func (e *bottleneckEngine) saturated(ctx context.Context, service string, pass, trip *rpsStepResult) (sat, dataOK bool, tripCores float64) {
	tripCores, reset, okT := e.cpuCores(ctx, service, trip.HoldStart, trip.HoldEnd)
	if !okT {
		return false, false, 0
	}
	if reset {
		return true, true, tripCores
	}
	if tripCores < cpuSaturatedFloorCores {
		return false, true, tripCores
	}
	passCores, _, okP := e.cpuCores(ctx, service, pass.HoldStart, pass.HoldEnd)
	if !okP || passCores <= 0 {
		// No usable baseline (pass-window query failed or read zero), but
		// trip-window usage is already above the saturation floor -> treat as
		// saturated. Caveat: without a baseline we can't confirm a plateau, so a
		// busy-but-still-scaling container could be over-blamed here. Rare: it
		// needs the pass query to fail while the trip query succeeds against the
		// same Prometheus.
		return true, true, tripCores
	}
	rise := (tripCores - passCores) / passCores
	return rise < e.knee, true, tripCores
}

// stageBackingUp reports whether a stage is accumulating backlog or breaching
// its latency SLO at the tripping step.
func stageBackingUp(st *stage, trip *rpsStepResult, th rpsThresholds) bool {
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
// undetermined. Precedence: high (stage CPU) -> high (dependency CPU) ->
// medium (backs up, no knee) -> low (resource-ranking fallback) -> undetermined.
func (e *bottleneckEngine) Diagnose(ctx context.Context, trip, pass *rpsStepResult, graph []stage, th rpsThresholds) bottleneckVerdict {
	if pass == nil {
		return bottleneckVerdict{Reasons: []string{"no passing step before breach; cannot compute CPU knee"}}
	}

	// Evaluate each stage once: is it backing up, and (if so) is its own
	// container CPU-saturated? sawData tracks whether ANY CPU query returned
	// usable data — if every query failed, Prometheus is effectively
	// unreachable and we must not emit a resource verdict.
	type stageEval struct {
		st        stage
		backingUp bool
		satStage  bool
	}
	evals := make([]stageEval, 0, len(graph))
	sawData := false
	for _, st := range graph {
		ev := stageEval{st: st, backingUp: stageBackingUp(&st, trip, th)}
		if ev.backingUp {
			sat, ok, _ := e.saturated(ctx, st.Container, pass, trip)
			ev.satStage = sat
			sawData = sawData || ok
		}
		evals = append(evals, ev)
	}

	// Pass 1: first backing-up stage whose own CPU is saturated -> high.
	for _, ev := range evals {
		if ev.backingUp && ev.satStage {
			return bottleneckVerdict{
				Component: ev.st.Name, Resource: "CPU", Confidence: "high", Determined: true,
				Reasons: []string{
					fmt.Sprintf("%s is the first stage to back up", ev.st.Name),
					fmt.Sprintf("%s CPU plateaued between %d and %d rps while load rose", ev.st.Container, pass.TargetRPS, trip.TargetRPS),
				},
			}
		}
	}

	// Pass 2: first backing-up stage whose backing dependency is saturated -> high.
	for _, ev := range evals {
		if !ev.backingUp {
			continue
		}
		for _, dep := range ev.st.DependsOn {
			sat, ok, _ := e.saturated(ctx, dep, pass, trip)
			sawData = sawData || ok
			if sat {
				return bottleneckVerdict{
					Component: ev.st.Name, Resource: dependencyDisplayName(dep), Confidence: "high", Determined: true,
					Reasons: []string{
						fmt.Sprintf("%s consumer backlog grew (first stage to back up)", ev.st.Name),
						fmt.Sprintf("%s CPU plateaued between %d and %d rps while load rose", dep, pass.TargetRPS, trip.TargetRPS),
					},
				}
			}
		}
	}

	// Pass 3: a stage backs up but nothing is saturated. If we had resource
	// data, that points to I/O or lock wait (medium); if every CPU query
	// failed, we cannot attribute at all (undetermined).
	for _, ev := range evals {
		if ev.backingUp {
			if !sawData {
				return bottleneckVerdict{Reasons: []string{"prometheus unreachable; cannot attribute the breach"}}
			}
			return bottleneckVerdict{
				Component: ev.st.Name, Resource: "unknown", Confidence: "medium", Determined: true,
				Reasons: []string{fmt.Sprintf("%s backs up but no resource knee found — likely I/O or lock wait", ev.st.Name)},
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
		sat, ok, cores := e.saturated(ctx, svc, pass, trip)
		if !ok || !sat {
			return
		}
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
		Component: dependencyDisplayName(best), Resource: "CPU", Confidence: "low", Determined: true,
		Reasons: []string{fmt.Sprintf("resource-ranking fallback: %s had the clearest CPU plateau (%.1f cores)", best, bestCores)},
	}, true
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
		// cAdvisor emits its series aligned to the same scrape window, so the
		// timestamps from any one series suffice as the shared [t0,t1] divisor.
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
