package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"runtime/metrics"
	"sort"
	"strings"
	"sync"
	"time"
)

// ConsumerPendingDelta captures a single durable's pending-message count
// at the start and end of a hold window.
type ConsumerPendingDelta struct {
	Start int64
	End   int64
	Delta int64
}

// SelfMetrics describes the loadgen process's own resource state during
// the hold window. High values mean the load box is the bottleneck and
// the step is INCONCLUSIVE rather than PASS/TRIP.
type SelfMetrics struct {
	GCPauseP99Ms float64
	CPUPercent   float64
	Goroutines   int
}

// Thresholds are the per-signal cutoffs that decide PASS / TRIP / INCONCLUSIVE.
type Thresholds struct {
	P95LatencyMs        float64
	P99LatencyMs        float64
	ErrorRate           float64 // fraction (0.001 = 0.1%)
	PendingGrowth       int64
	GCPauseInconclusive float64
	CPUInconclusive     float64

	// ActionP95Ms and ActionP99Ms gate per-action latency. Empty map (or
	// missing key for a given action) means "don't gate this action".
	// Read in evaluateStep; defaults populated by defaultThresholds.
	//
	// Keys are stable action names from actionKind.String() — e.g.
	// "mark_read", "scroll_history", "member_add". These run in the
	// loadgen process: each sample is the wall-clock around the per-action
	// handler call, so the thresholds reflect *handler* latency (not the
	// publish→broadcast pipeline gated by P95LatencyMs / P99LatencyMs).
	ActionP95Ms map[string]float64
	ActionP99Ms map[string]float64
}

func defaultThresholds() Thresholds {
	return Thresholds{
		P95LatencyMs: 500, P99LatencyMs: 1000,
		ErrorRate: 0.001, PendingGrowth: 1000,
		GCPauseInconclusive: 50, CPUInconclusive: 80,
		// Per-action defaults reflect typical handler latencies for the
		// chat backend. They're observational floors — runs against
		// faster or slower infrastructure may want to tune via the
		// --action-p95/--action-p99 flags. Actions not listed here
		// (e.g. send, thread_reply) don't gate at this layer — sends
		// gate via the broadcast-latency p95/p99 above.
		ActionP95Ms: map[string]float64{
			"mark_read":         100,
			"refresh_room_list": 200,
			"scroll_history":    500,
			"member_add":        200,
			"mute_toggle":       100,
			"room_create":       500,
		},
		ActionP99Ms: map[string]float64{
			"mark_read":         250,
			"refresh_room_list": 500,
			"scroll_history":    1500,
			"member_add":        500,
			"mute_toggle":       250,
			"room_create":       1500,
		},
	}
}

// pendingGrowthExempt lists durables whose pending-growth is NOT a daily
// pass/trip signal. notification-worker drives push-notification delivery,
// where delivery delay is tolerated by design — a notification backlog must
// not fail an otherwise-healthy run. Its pending is still surfaced in the
// report (worst-pending-delta column) for observability, and a mid-hold
// disappearance still trips, since that's an availability failure rather than
// a tolerated delay.
var pendingGrowthExempt = map[string]bool{
	"notification-worker": true,
}

// stepInputs is everything evaluateStep needs to produce a verdict.
type stepInputs struct {
	N               int
	EffectiveN      int // count of users actually activated (may be < N)
	StartedAt       time.Time
	HoldDuration    time.Duration
	LatencySamples  []float64            // milliseconds (broadcast latency)
	ActionSamplesMs map[string][]float64 // per-action wall-clock latency in ms
	AttemptedOps    int64
	FailedOps       int64
	ConsumerPending map[string]ConsumerPendingDelta
	ServiceErrors   map[string]int64
	Self            SelfMetrics
}

// ActionLatencyStats summarises one action kind's wall-clock latency
// distribution over the hold window. Surfaced in the report so the
// operator can see per-handler timing (sendMessage, scrollHistory,
// memberAdd, etc.) in addition to the system-wide broadcast latency.
// Does not feed the verdict — kept observational so the PASS/TRIP
// criteria stay focused on the messaging-pipeline SLO.
type ActionLatencyStats struct {
	Count int
	P50Ms float64
	P95Ms float64
	P99Ms float64
}

// PresenceObsStats is the observational presence summary for one daily step.
// It NEVER affects the verdict — evaluateStep does not read it. Latency is a
// single combined figure over connect (hello->online) and activity
// (setAway->away/online) transitions; pings are no-ops and contribute only to
// attempted/error accounting.
type PresenceObsStats struct {
	P50Ms     float64
	P95Ms     float64
	P99Ms     float64
	Attempted int64
	Failed    int64
	ErrorRate float64
}

// StepResult is the verdict for a single ramp step.
type StepResult struct {
	N                     int
	EffectiveN            int // users actually activated; differs from N when pools fill up
	StartedAt             time.Time
	HoldDuration          time.Duration
	P50LatencyMs          float64
	P95LatencyMs          float64
	P99LatencyMs          float64
	ErrorRate             float64
	AttemptedOps          int64
	FailedOps             int64
	ConsumerPending       map[string]ConsumerPendingDelta
	ServiceErrorIncreases map[string]int64
	LoadgenSelfMetrics    SelfMetrics
	ActionLatencies       map[string]ActionLatencyStats
	// Presence is non-nil only when daily ran with --presence. Observational.
	Presence       *PresenceObsStats
	Tripped        bool
	Inconclusive   bool
	TrippedReasons []string
}

// summariseActions reduces the per-action latency sample slices to
// Count + P50 + P95 + P99 stats so StepResult can carry a compact
// per-handler breakdown without holding the raw samples.
func summariseActions(samples map[string][]float64) map[string]ActionLatencyStats {
	if len(samples) == 0 {
		return nil
	}
	out := make(map[string]ActionLatencyStats, len(samples))
	for name, ss := range samples {
		out[name] = ActionLatencyStats{
			Count: len(ss),
			P50Ms: percentile(ss, 0.50),
			P95Ms: percentile(ss, 0.95),
			P99Ms: percentile(ss, 0.99),
		}
	}
	return out
}

// percentile returns the value at quantile p using ceil-based nearest-rank
// (the standard for "what's the p99 of my samples"). Floor-based indexing
// systematically under-reports for small sample counts — e.g. p99 of 50
// samples with floor → cp[48] (true p98), with ceil → cp[49] (true p99).
func percentile(samples []float64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	cp := make([]float64, len(samples))
	copy(cp, samples)
	sort.Float64s(cp)
	idx := int(math.Ceil(p*float64(len(cp)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

//nolint:gocritic // hugeParam: pure-function signature is intentional; the per-step copy cost is negligible.
func evaluateStep(in stepInputs, th Thresholds) StepResult {
	r := StepResult{
		N: in.N, EffectiveN: in.EffectiveN,
		StartedAt: in.StartedAt, HoldDuration: in.HoldDuration,
		AttemptedOps: in.AttemptedOps, FailedOps: in.FailedOps,
		ConsumerPending:       in.ConsumerPending,
		ServiceErrorIncreases: in.ServiceErrors,
		LoadgenSelfMetrics:    in.Self,
		P50LatencyMs:          percentile(in.LatencySamples, 0.50),
		P95LatencyMs:          percentile(in.LatencySamples, 0.95),
		P99LatencyMs:          percentile(in.LatencySamples, 0.99),
		ActionLatencies:       summariseActions(in.ActionSamplesMs),
	}
	if in.AttemptedOps > 0 {
		r.ErrorRate = float64(in.FailedOps) / float64(in.AttemptedOps)
	}

	// Inconclusive overrides trip. Reserved for situations where the
	// verdict signals can't be trusted: load box saturated, no traffic
	// generated, or far fewer users active than nominal.
	if in.Self.GCPauseP99Ms > th.GCPauseInconclusive || in.Self.CPUPercent > th.CPUInconclusive {
		r.Inconclusive = true
		r.TrippedReasons = append(r.TrippedReasons,
			fmt.Sprintf("inconclusive: gc=%.1fms cpu=%.1f%%", in.Self.GCPauseP99Ms, in.Self.CPUPercent))
		return r
	}
	if in.AttemptedOps == 0 {
		// No actions emitted — publisher conn failed, emitters not wired,
		// or zero hold duration. A "PASS" here would be a silent lie.
		r.Inconclusive = true
		r.TrippedReasons = append(r.TrippedReasons,
			"inconclusive: zero actions attempted (publisher down or emitters not wired)")
		return r
	}
	if in.N > 0 && in.EffectiveN > 0 && float64(in.EffectiveN)/float64(in.N) < 0.95 {
		// More than 5% of nominal N never came online. The result doesn't
		// reflect "N users at sustained load"; report Inconclusive so the
		// operator knows to fix pool config before trusting the verdict.
		r.Inconclusive = true
		r.TrippedReasons = append(r.TrippedReasons,
			fmt.Sprintf("inconclusive: only %d/%d users activated (pool caps too low)", in.EffectiveN, in.N))
		return r
	}

	if r.P95LatencyMs > th.P95LatencyMs {
		r.Tripped = true
		r.TrippedReasons = append(r.TrippedReasons,
			fmt.Sprintf("p95=%.0fms > %.0f", r.P95LatencyMs, th.P95LatencyMs))
	}
	if r.P99LatencyMs > th.P99LatencyMs {
		r.Tripped = true
		r.TrippedReasons = append(r.TrippedReasons,
			fmt.Sprintf("p99=%.0fms > %.0f", r.P99LatencyMs, th.P99LatencyMs))
	}
	if r.ErrorRate > th.ErrorRate {
		r.Tripped = true
		r.TrippedReasons = append(r.TrippedReasons,
			fmt.Sprintf("error_rate=%.4f > %.4f", r.ErrorRate, th.ErrorRate))
	}
	for durable, d := range in.ConsumerPending {
		switch {
		case d.Delta > th.PendingGrowth && !pendingGrowthExempt[durable]:
			r.Tripped = true
			r.TrippedReasons = append(r.TrippedReasons,
				fmt.Sprintf("%s pending +%d > +%d", durable, d.Delta, th.PendingGrowth))
		case d.End == 0 && d.Start > 0:
			// Durable disappeared mid-window — the consumer crashed or was
			// deleted. Trip regardless of PendingGrowth threshold.
			r.Tripped = true
			r.TrippedReasons = append(r.TrippedReasons,
				fmt.Sprintf("%s disappeared mid-hold (had %d pending at start)", durable, d.Start))
		}
	}
	for svc, n := range in.ServiceErrors {
		if n > 0 {
			r.Tripped = true
			r.TrippedReasons = append(r.TrippedReasons,
				fmt.Sprintf("%s errors +%d", svc, n))
		}
	}
	// Per-action latency gates. Each gated action contributes at most two
	// trip reasons (p95 and p99). Walk allActionKinds for stable ordering
	// so reason output doesn't depend on map iteration.
	for _, k := range allActionKinds {
		name := k.String()
		s, ok := r.ActionLatencies[name]
		if !ok || s.Count == 0 {
			continue
		}
		if cap, ok := th.ActionP95Ms[name]; ok && s.P95Ms > cap {
			r.Tripped = true
			r.TrippedReasons = append(r.TrippedReasons,
				fmt.Sprintf("%s p95=%.0fms > %.0f", name, s.P95Ms, cap))
		}
		if cap, ok := th.ActionP99Ms[name]; ok && s.P99Ms > cap {
			r.Tripped = true
			r.TrippedReasons = append(r.TrippedReasons,
				fmt.Sprintf("%s p99=%.0fms > %.0f", name, s.P99Ms, cap))
		}
	}
	return r
}

// snapshotSelfMetrics samples loadgen-process resource counters.
// CPU% is approximate (delta of cumulative CPU time / wall-clock since last call).
func snapshotSelfMetrics() SelfMetrics {
	g := runtime.NumGoroutine()
	gcP99 := readGCPauseP99Ms()
	cpu := readCPUPercent()
	return SelfMetrics{
		GCPauseP99Ms: gcP99,
		CPUPercent:   cpu,
		Goroutines:   g,
	}
}

var (
	gcLastNumGC uint32 //nolint:unused // reserved for future delta tracking
	gcMu        sync.Mutex
)

func readGCPauseP99Ms() float64 {
	gcMu.Lock()
	defer gcMu.Unlock()
	samples := []metrics.Sample{{Name: "/gc/pauses:seconds"}}
	metrics.Read(samples)
	if samples[0].Value.Kind() != metrics.KindFloat64Histogram {
		return 0
	}
	h := samples[0].Value.Float64Histogram()
	if len(h.Counts) == 0 {
		return 0
	}
	var total uint64
	for _, c := range h.Counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := total * 99 / 100
	var acc uint64
	for i, c := range h.Counts {
		acc += c
		if acc >= target {
			return h.Buckets[i] * 1000
		}
	}
	return 0
}

// readCPUPercent is disabled. The previous goroutine-count proxy
// (NumGoroutine/5000 × 100) tripped INCONCLUSIVE at any scale above ~4k
// users since startEmitter launches one goroutine per user — exactly the
// scale this tool is designed to test. A real CPU sample (gopsutil or
// /proc/self/stat deltas) is the right fix, deferred to a follow-up; for
// now the CPU check is effectively off and INCONCLUSIVE relies on the GC
// pause signal alone.
func readCPUPercent() float64 {
	return 0
}

// diffPending computes per-durable Start/End/Delta from two snapshots.
// Walks both maps: durables that appeared mid-window are counted with
// Start=0 (positive Delta), and durables that disappeared mid-window
// (consumer crashed, was deleted) are surfaced with End=0 (negative
// Delta) so evaluateStep can flag the disappearance instead of silently
// dropping the signal.
func diffPending(start, end map[string]int64) map[string]ConsumerPendingDelta {
	out := make(map[string]ConsumerPendingDelta, len(end)+len(start))
	for durable, e := range end {
		s := start[durable]
		out[durable] = ConsumerPendingDelta{Start: s, End: e, Delta: e - s}
	}
	for durable, s := range start {
		if _, present := end[durable]; present {
			continue
		}
		// Disappeared mid-window — surface the loss so it can trip.
		out[durable] = ConsumerPendingDelta{Start: s, End: 0, Delta: -s}
	}
	return out
}

// pollPending queries the NATS monitoring endpoint /jsz?consumers=true and
// returns a map of durable name -> NumPending. Retries transient failures
// with short backoff so a flaky monitoring endpoint doesn't poison a step.
func pollPending(ctx context.Context, jszURL string) (map[string]int64, error) {
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}
		out, err := pollPendingOnce(ctx, jszURL)
		if err == nil {
			return out, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("pollPending after %d attempts: %w", maxAttempts, lastErr)
}

// pollPendingClient has an explicit per-request timeout so a hung NATS
// monitoring endpoint can't wedge the whole run waiting on the operator's
// run-level ctx (which typically has no deadline for exploratory sweeps).
var pollPendingClient = &http.Client{Timeout: 5 * time.Second}

func pollPendingOnce(ctx context.Context, jszURL string) (map[string]int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, jszURL+"?consumers=true", nil)
	if err != nil {
		return nil, fmt.Errorf("build jsz request: %w", err)
	}
	resp, err := pollPendingClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jsz GET: %w", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccountDetails []struct {
			StreamDetail []struct {
				ConsumerDetail []struct {
					Name       string `json:"name"`
					NumPending int64  `json:"num_pending"`
				} `json:"consumer_detail"`
			} `json:"stream_detail"`
		} `json:"account_details"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("jsz decode: %w", err)
	}
	out := make(map[string]int64)
	for _, a := range body.AccountDetails {
		for _, s := range a.StreamDetail {
			for _, c := range s.ConsumerDetail {
				out[c.Name] = c.NumPending
			}
		}
	}
	return out, nil
}

// serviceScraper fetches /metrics from each service URL and returns a map of
// service -> delta in slog_errors_total since the previous call.
// First call returns zeros and records baselines.
type serviceScraper struct {
	mu       sync.Mutex
	baseline map[string]float64
}

func newServiceScraper() *serviceScraper {
	return &serviceScraper{baseline: make(map[string]float64)}
}

func (s *serviceScraper) Scrape(ctx context.Context, urls map[string]string) (map[string]int64, error) {
	out := make(map[string]int64, len(urls))
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, url := range urls {
		v, err := scrapeErrorCounter(ctx, url)
		if err != nil {
			out[name] = 0 // tolerate missing
			continue
		}
		prev, ok := s.baseline[name]
		s.baseline[name] = v
		if !ok {
			out[name] = 0
			continue
		}
		out[name] = int64(v - prev)
	}
	return out, nil
}

func scrapeErrorCounter(ctx context.Context, url string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("build metrics request %s: %w", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("metrics GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("metrics read %s: %w", url, err)
	}
	return sumCounterFamily(string(body), "slog_errors_total"), nil
}

func sumCounterFamily(body, family string) float64 {
	var sum float64
	for _, line := range strings.Split(body, "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		if !strings.HasPrefix(line, family) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		var v float64
		if _, err := fmt.Sscanf(fields[len(fields)-1], "%f", &v); err != nil {
			continue // skip unparseable line
		}
		sum += v
	}
	return sum
}
