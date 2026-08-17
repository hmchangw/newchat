package main

import (
	"context"
	"sync"
	"time"
)

const (
	// minEmitInterval is the floor for the open-loop ticker. A single
	// time.Ticker driven at sub-millisecond intervals can't be honored by the Go
	// runtime — and its buffer-of-1 silently drops ticks — which capped achieved
	// RPS at a few kHz no matter the target. Clamping the tick to this floor and
	// releasing a batch of events per tick removes that ceiling.
	minEmitInterval = 2 * time.Millisecond

	// pacerMaxBurstIntervals bounds how far a single tick may catch up after a
	// late wakeup, in units of the nominal per-tick batch. Catch-up within this
	// bound absorbs scheduler jitter; the excess is reported as emit underrun
	// (the load box could not keep schedule) instead of being released as one
	// thundering burst against the system under test.
	pacerMaxBurstIntervals = 8
)

// pacer converts a target rate (events/sec) into per-tick batch counts on a
// coarse, reliably-schedulable interval. It is driven by wall-clock time rather
// than by counting ticks, so dropped/delayed ticker fires surface as measurable
// emit underrun rather than silently lowering the achieved rate.
//
// pacer is not safe for concurrent use; a single dispatch goroutine owns it.
type pacer struct {
	rate     float64
	interval time.Duration
	perTick  float64 // nominal events released per on-schedule tick
	maxBurst float64 // catch-up ceiling for a single tick
	start    time.Time
	emitted  float64 // events accounted (released + forgiven underrun) so far
}

// newPacer builds a pacer for rate events/sec, measuring elapsed time from
// start. The ticker interval is the natural 1s/rate, clamped up to
// minEmitInterval so the runtime can honor it.
func newPacer(rate int, start time.Time) *pacer {
	return newRatePacer(float64(rate), start)
}

func newRatePacer(rate float64, start time.Time) *pacer {
	interval := time.Duration(float64(time.Second) / rate)
	if interval < minEmitInterval {
		interval = minEmitInterval
	}
	perTick := rate * interval.Seconds()
	return &pacer{
		rate:     rate,
		interval: interval,
		perTick:  perTick,
		maxBurst: perTick * pacerMaxBurstIntervals,
		start:    start,
	}
}

// tick computes, for a wakeup at now, how many events to release (emit) to track
// the target rate and how many were due but skipped because the wakeup arrived
// too late to catch up within the burst bound (underrun). Both counts are
// accounted internally so the long-run release rate tracks the target without
// re-counting skipped events on later ticks.
func (p *pacer) tick(now time.Time) (emit, underrun int) {
	intended := p.rate * now.Sub(p.start).Seconds()
	deficit := intended - p.emitted
	if deficit <= 0 {
		return 0, 0
	}
	if deficit > p.maxBurst {
		underrun = int(deficit - p.maxBurst)
		deficit = p.maxBurst
	}
	emit = int(deficit)
	p.emitted += float64(emit + underrun)
	return emit, underrun
}

// serialDispatch ticks at the natural 1s/rate interval and runs do once per
// tick on the ticker goroutine. Legacy path (MaxInFlight == 0): no batching, so
// it cannot ramp past single-ticker resolution — retained for bisection only.
func serialDispatch(ctx context.Context, rate int, do func(context.Context)) {
	interval := time.Second / time.Duration(rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			do(ctx)
		}
	}
}

// pacedDispatch drives a batched pacer into a bounded worker pool until ctx
// cancels, so achieved rate is not capped by single-ticker resolution. do runs
// per released event on a pool goroutine; recordUnderrun tallies events the
// pacer could not release on schedule (per tick) and recordSaturation tallies
// events dropped because the in-flight pool was full (per event). On ctx
// cancellation it drains in-flight work up to drainGracePeriod.
func pacedDispatch(
	ctx context.Context, rate, maxInFlight int,
	recordUnderrun func(int), recordSaturation func(), do func(context.Context),
) {
	pacedDispatchRate(
		ctx,
		float64(rate),
		maxInFlight,
		recordUnderrun,
		recordSaturation,
		do,
	)
}

// pacedDispatchRate preserves the open-loop invariant: downstream action
// latency never blocks the pacing clock or lowers offered load. Every due event
// is either dispatched immediately or reported as saturation; capacity is
// never awaited and missed work is never released later as a catch-up burst.
func pacedDispatchRate(
	ctx context.Context,
	rate float64,
	maxInFlight int,
	recordUnderrun func(int),
	recordSaturation func(),
	do func(context.Context),
) {
	p := newRatePacer(rate, time.Now())
	tick := time.NewTicker(p.interval)
	defer tick.Stop()
	pacedDispatchRateWithTicks(
		ctx,
		p,
		tick.C,
		maxInFlight,
		recordUnderrun,
		recordSaturation,
		do,
	)
}

func pacedDispatchRateWithTicks(
	ctx context.Context,
	p *pacer,
	ticks <-chan time.Time,
	maxInFlight int,
	recordUnderrun func(int),
	recordSaturation func(),
	do func(context.Context),
) {
	sem := make(chan struct{}, maxInFlight)
	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(drainGracePeriod):
			}
			return
		case now := <-ticks:
			emit, underrun := p.tick(now)
			recordUnderrun(underrun)
			for i := 0; i < emit; i++ {
				select {
				case sem <- struct{}{}:
					wg.Add(1)
					go func() {
						defer func() {
							<-sem
							wg.Done()
						}()
						do(ctx)
					}()
				default:
					recordSaturation()
				}
			}
		}
	}
}
