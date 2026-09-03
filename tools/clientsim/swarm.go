package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// runnable is one simulated client from the swarm's point of view;
// simClient is the production implementation, tests inject fakes.
type runnable interface {
	run(ctx context.Context) error
	close()
}

type clientFactory func(account string) (runnable, error)

// swarmShutdownTimeout bounds the drain on ctx cancel; it stays under the
// repo's 25s shutdown budget so Kubernetes never SIGKILLs mid-drain.
// var (not const) so the drain-timeout path is testable.
var swarmShutdownTimeout = 20 * time.Second

// errDrainTimeout is returned when clients failed to exit within the
// shutdown budget — the process must not report a clean run.
var errDrainTimeout = errors.New("swarm drain timed out; some clients did not exit within the shutdown budget")

// maxStartAttempts bounds restarts per account, so one permanently broken
// account cannot spin for the length of a soak. It — not the refill pace —
// is what bounds the load a dead dependency sees. The resulting fleet
// shortfall is what the readiness gate reports.
const maxStartAttempts = 5

// instance identifies one started client. The epoch is what lets a retired
// client's exit report be told apart from its replacement's: without it,
// churn's own teardown notification arrives after the new client is
// registered and tears that one down instead.
type instance struct {
	account string
	epoch   uint64
}

// rampBudget paces starts across ticks. Above the batch threshold a tick
// starts more than one client, and the fractional remainder is CARRIED rather
// than rounded: rounding each tick up to a whole client overshoots by up to a
// full extra start per millisecond, so 1001/s would ramp at 2000/s — twice the
// rate the operator configured, which is a different test than the one they
// asked for.
type rampBudget struct {
	perTick float64
	ticks   int64
	issued  int64
}

// take reports how many clients this tick may start. The count comes from the
// CUMULATIVE target rather than a running fractional credit: repeatedly adding
// a fraction that binary floating point cannot represent (1.001, say) drifts,
// and at 1001/s that drift loses a whole connection over a one-second ramp.
func (r *rampBudget) take() int {
	r.ticks++
	want := int64(math.Round(float64(r.ticks) * r.perTick))
	n := want - r.issued
	r.issued = want
	if n < 0 {
		return 0
	}
	return int(n)
}

// rampPacing splits a connects/sec rate into a ticker interval and a per-tick
// budget. Below 1000/s each tick starts exactly one client; above it the tick
// is pinned to 1ms and the budget carries the remainder.
func rampPacing(rampRate float64) (time.Duration, *rampBudget) {
	interval := time.Duration(float64(time.Second) / rampRate)
	if interval >= time.Millisecond {
		return interval, &rampBudget{perTick: 1}
	}
	return time.Millisecond, &rampBudget{perTick: rampRate / 1000}
}

// orderIndex is the churn-pick population: a slice for O(1) random selection
// plus the account->position map that makes removal O(1) too. The map is the
// point — scanning the slice per removal made a full-fleet shutdown quadratic,
// and at 30k clients that is ~4.5e8 string comparisons against a 20s drain
// budget. Not internally synchronized; the swarm's mu guards it.
type orderIndex struct {
	accts []string
	at    map[string]int
}

func newOrderIndex() *orderIndex { return &orderIndex{at: map[string]int{}} }

func (o *orderIndex) add(account string) {
	if _, ok := o.at[account]; ok {
		return
	}
	o.at[account] = len(o.accts)
	o.accts = append(o.accts, account)
}

// remove swaps the last element into the hole and repairs ITS index — getting
// that wrong silently drops a different account on some later removal.
func (o *orderIndex) remove(account string) {
	i, ok := o.at[account]
	if !ok {
		return
	}
	last := len(o.accts) - 1
	o.accts[i] = o.accts[last]
	o.at[o.accts[i]] = i
	o.accts = o.accts[:last]
	delete(o.at, account)
}

func (o *orderIndex) len() int { return len(o.accts) }

// accounts returns a copy, so a caller iterating it cannot be invalidated by
// a concurrent removal.
func (o *orderIndex) accounts() []string {
	out := make([]string, len(o.accts))
	copy(out, o.accts)
	return out
}

func (o *orderIndex) pick(randN func(int) int) string {
	if len(o.accts) == 0 {
		return ""
	}
	return o.accts[randN(len(o.accts))]
}

// runSwarm ramps clients at rampRate connects/sec (per replica — cluster
// rate is rampRate × replicas), optionally churns clients at churnRate
// cycles/sec, and drains everything when ctx ends. Rates above 1000/s are
// honored by starting batches per 1ms tick instead of silently clamping.
func runSwarm(ctx context.Context, accounts []string, rampRate, churnRate float64, factory clientFactory) error {
	if rampRate <= 0 {
		rampRate = 1
	}
	interval, budget := rampPacing(rampRate)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		running  = map[string]runnable{}
		cancels  = map[string]context.CancelFunc{}
		order    = newOrderIndex() // O(1) random churn picks AND O(1) removal
		pending  []string          // accounts awaiting a restart (failed start, or early exit)
		attempts = map[string]int{}
		epochs   = map[string]uint64{} // epoch of the instance currently registered
		epoch    uint64
	)

	// exited carries accounts whose run() returned while the swarm is still
	// up — auth/connect/walk failures. Without this they stay parked in the
	// running map and, with the default CHURN_RATE=0, are never retried, so
	// the fleet silently shrinks for the rest of the soak.
	exited := make(chan instance, len(accounts)+1)

	start := func(account string) bool {
		mu.Lock()
		_, alreadyRunning := running[account]
		mu.Unlock()
		if alreadyRunning {
			// Never orphan a live client by overwriting its cancel func.
			slog.Warn("start skipped; account already running", "account", account)
			return true
		}
		// Counted before the factory runs, so a factory that fails every time
		// still exhausts its budget instead of retrying forever.
		attempts[account]++
		client, err := factory(account)
		if err != nil {
			slog.Warn("start client", "account", account, "error", err)
			return false
		}
		clientCtx, cancel := context.WithCancel(ctx)
		epoch++
		ep := epoch
		mu.Lock()
		running[account] = client
		cancels[account] = cancel
		epochs[account] = ep
		order.add(account)
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := client.run(clientCtx); err != nil {
				slog.Warn("client exited", "account", account, "error", err)
			}
			// Report the exit unless the whole swarm is shutting down, where
			// every client returns by design and nothing is left to consume.
			select {
			case exited <- instance{account: account, epoch: ep}:
			case <-ctx.Done():
			}
		}()
		return true
	}

	stopOne := func(account string) {
		mu.Lock()
		client := running[account]
		cancel := cancels[account]
		delete(running, account)
		delete(cancels, account)
		delete(epochs, account)
		order.remove(account)
		mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if client != nil {
			client.close()
		}
	}

	// One ticker paces both the initial ramp and refills. A fleet that took
	// N seconds to build should not need hours to rebuild after a dependency
	// blips; the per-account attempt cap, not a slow tick, is what protects
	// the dependency.
	rampTicker := time.NewTicker(interval)
	defer rampTicker.Stop()

	// requeue schedules another start attempt while the account still has
	// attempts left; past the cap it is abandoned and shows up as a gap in
	// clientsim_conns_ready.
	requeue := func(account string) {
		if attempts[account] >= maxStartAttempts {
			slog.Warn("client abandoned after repeated start failures",
				"account", account, "attempts", attempts[account])
			return
		}
		pending = append(pending, account)
	}

	// Same pacing as the ramp: clamping to one cycle per 1ms tick silently ran
	// a configured 5000/s at 1000/s.
	var churnCh <-chan time.Time
	churnBudget := &rampBudget{}
	if churnRate > 0 {
		var churnInterval time.Duration
		churnInterval, churnBudget = rampPacing(churnRate)
		churnTicker := time.NewTicker(churnInterval)
		defer churnTicker.Stop()
		churnCh = churnTicker.C
	}

	next := 0
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-rampTicker.C:
			// Refills first: restore what the fleet lost before growing it.
			slots := budget.take()
			for ; slots > 0 && len(pending) > 0; slots-- {
				account := pending[0]
				pending = pending[1:]
				if !start(account) {
					requeue(account)
				}
			}
			for ; slots > 0 && next < len(accounts); slots-- {
				if !start(accounts[next]) {
					requeue(accounts[next])
				}
				next++
			}
		case inst := <-exited:
			mu.Lock()
			current, stillRegistered := epochs[inst.account]
			mu.Unlock()
			if !stillRegistered || current != inst.epoch {
				continue // a retired instance reporting in; its slot already moved on
			}
			stopOne(inst.account)
			requeue(inst.account)
		case <-churnCh:
			// Cycle random running clients through a full disconnect +
			// re-auth + rejoin, modeling mobile clients dropping.
			for n := churnBudget.take(); n > 0; n-- {
				mu.Lock()
				pick := order.pick(secureIntN)
				mu.Unlock()
				if pick == "" {
					break
				}
				// A churn restart is a deliberate cycle, not a failure: reset
				// the attempt budget so long soaks do not exhaust it.
				stopOne(pick)
				delete(attempts, pick)
				if !start(pick) {
					requeue(pick)
				}
			}
		}
	}

	mu.Lock()
	accountsToStop := order.accounts()
	mu.Unlock()
	for _, account := range accountsToStop {
		stopOne(account)
	}

	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()
	timeout := time.NewTimer(swarmShutdownTimeout)
	defer timeout.Stop()
	select {
	case <-drained:
		return nil
	case <-timeout.C:
		slog.Warn("swarm drain timed out", "budget", swarmShutdownTimeout.String())
		return errDrainTimeout
	}
}

// runSummary is the digested end-of-run state; printSummary logs it and
// tests assert on it directly.
type runSummary struct {
	Attrs    []any
	Degraded bool
}

// summarize gathers the registry into a loadgen-style summary: delivered
// per lane, disconnects per reason, latency percentile estimates, and the
// loss-visibility counters that mark the window degraded.
func summarize(m *metrics, runID, configDigest string, target int) (runSummary, error) {
	families, err := m.Registry.Gather()
	if err != nil {
		return runSummary{}, fmt.Errorf("gather metrics for summary: %w", err)
	}
	s := runSummary{Attrs: []any{"runId", runID, "configDigest", configDigest,
		"target", target, "readyPeak", m.readyPeak.Load(),
		"readyMin", m.readyMin.Load(), "readyAtDrain", m.readyAtDrain.Load()}}
	var degradedEvidence float64
	for _, fam := range families {
		if len(fam.GetMetric()) == 0 {
			continue
		}
		switch fam.GetName() {
		case "clientsim_msgs_delivered_total":
			for _, metric := range fam.GetMetric() {
				lane := "unknown"
				for _, lp := range metric.GetLabel() {
					if lp.GetName() == "lane" {
						lane = lp.GetValue()
					}
				}
				s.Attrs = append(s.Attrs, "delivered_"+lane, metric.GetCounter().GetValue())
			}
		case "clientsim_disconnects_total":
			for _, metric := range fam.GetMetric() {
				reason := "unknown"
				for _, lp := range metric.GetLabel() {
					if lp.GetName() == "reason" {
						reason = lp.GetValue()
					}
				}
				s.Attrs = append(s.Attrs, "disconnects_"+reason, metric.GetCounter().GetValue())
			}
		case "clientsim_conns_ready_peak":
			s.Attrs = append(s.Attrs, "conns_ready_peak", fam.GetMetric()[0].GetGauge().GetValue())
		case "clientsim_reconnects_total":
			s.Attrs = append(s.Attrs, "reconnects", fam.GetMetric()[0].GetCounter().GetValue())
		case "clientsim_auth_failures_total":
			v := fam.GetMetric()[0].GetCounter().GetValue()
			s.Attrs = append(s.Attrs, "auth_failures", v)
		case "clientsim_broadcast_to_client_latency_seconds", "clientsim_canonical_to_client_latency_seconds":
			h := fam.GetMetric()[0].GetHistogram()
			s.Attrs = append(s.Attrs,
				fam.GetName()+"_count", h.GetSampleCount(),
				fam.GetName()+"_p50", histQuantile(h, 0.50),
				fam.GetName()+"_p95", histQuantile(h, 0.95),
				fam.GetName()+"_p99", histQuantile(h, 0.99))
			// A quantile that lands in the +Inf bucket is reported as the top
			// finite bound — correct, and the same thing histogram_quantile
			// does, but it reads like a measured latency. Printing the
			// overflow count beside it is what stops a clamped p99 from being
			// mistaken for the real tail.
			if over := overTopBucket(h); over > 0 {
				s.Attrs = append(s.Attrs, fam.GetName()+"_over_top_bucket", over)
			}
		case "clientsim_decode_failures_total", "clientsim_invalid_timestamp_total", "clientsim_slow_consumer_events_total":
			v := fam.GetMetric()[0].GetCounter().GetValue()
			degradedEvidence += v
			s.Attrs = append(s.Attrs, fam.GetName(), v)
		}
	}
	s.Degraded = degradedEvidence > 0
	s.Attrs = append(s.Attrs, "degraded", s.Degraded)
	return s, nil
}

// histQuantile linearly interpolates a quantile from cumulative buckets —
// the same estimate Prometheus's histogram_quantile would produce.
func histQuantile(h *dto.Histogram, q float64) float64 {
	total := float64(h.GetSampleCount())
	if total == 0 {
		return 0
	}
	rank := q * total
	var prevCount, prevBound float64
	for _, b := range h.GetBucket() {
		count := float64(b.GetCumulativeCount())
		bound := b.GetUpperBound()
		if count >= rank {
			// count == prevCount is unreachable: rank is positive (every
			// caller passes a positive quantile over a non-empty histogram),
			// so an equal pair at or above rank would have returned on the
			// previous bucket, and on the first bucket prevCount is 0 while
			// count must exceed a positive rank.
			return prevBound + (bound-prevBound)*(rank-prevCount)/(count-prevCount)
		}
		prevCount, prevBound = count, bound
	}
	return prevBound
}

// overTopBucket counts samples above the highest finite bucket: the dto
// carries only the explicit buckets, so this is the total minus the last
// cumulative count.
func overTopBucket(h *dto.Histogram) uint64 {
	buckets := h.GetBucket()
	if len(buckets) == 0 {
		return h.GetSampleCount()
	}
	return h.GetSampleCount() - buckets[len(buckets)-1].GetCumulativeCount()
}

// printSummary logs the end-of-run summary and returns it. Any loss-counter
// increment marks the window degraded — surfaced, never silently averaged in.
func printSummary(m *metrics, runID, configDigest string, target int) (runSummary, error) {
	s, err := summarize(m, runID, configDigest, target)
	if err != nil {
		// Returned, not swallowed: a zero summary would silently report
		// degraded=false and defeat CLIENTSIM_FAIL_ON_DEGRADED.
		return runSummary{}, err
	}
	slog.Info("clientsim run summary", s.Attrs...)
	return s, nil
}

// readyGate is the run's validity check: did the harness reach the requested
// fleet and recover it before shutdown? The pre-drain snapshot preserves the
// terminal state that SIGTERM-driven cleanup would otherwise erase.
//
// Deliberately separate from the degraded flag. Loss counters describe the
// system under test — in a failure test they are the result, not a fault —
// whereas a fleet that never came up means the numbers describe nothing.
func readyGate(m *metrics, target int, minRatio float64) error {
	if minRatio <= 0 || target == 0 {
		return nil
	}
	// The atomic, not the exported gauge: the gauge is Set outside the
	// compare-and-swap, so a descheduled goroutine can publish a stale lower
	// peak. Only the atomic is exact, and the gate must not fail a run that
	// actually met the floor.
	peak := float64(m.readyPeak.Load())
	want := minRatio * float64(target)
	if peak < want {
		return fmt.Errorf("fleet never reached the readiness floor: peak ready %.0f of %d target (%.1f%%), need %.1f%%",
			peak, target, 100*peak/float64(target), 100*minRatio)
	}
	if !m.readyCaptured.Load() {
		return errors.New("fleet readiness snapshot was not captured before drain")
	}
	atDrain := float64(m.readyAtDrain.Load())
	if atDrain < want {
		return fmt.Errorf("fleet did not recover the readiness floor before shutdown: ready %.0f of %d target (%.1f%%), need %.1f%%",
			atDrain, target, 100*atDrain/float64(target), 100*minRatio)
	}
	return nil
}
