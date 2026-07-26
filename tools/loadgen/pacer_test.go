package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPacer_ClampsIntervalToFloor(t *testing.T) {
	// 1s/100000 = 10us, below the floor: the ticker interval must clamp so the
	// Go runtime can actually honor it.
	p := newPacer(100000, time.Unix(0, 0))
	assert.Equal(t, minEmitInterval, p.interval)
}

func TestPacer_LowRateUsesNaturalInterval(t *testing.T) {
	// 1s/200 = 5ms, comfortably above the floor: keep the natural interval and
	// release one event per tick (no regression from the old single-ticker).
	start := time.Unix(0, 0)
	p := newPacer(200, start)
	assert.Equal(t, 5*time.Millisecond, p.interval)
	e, u := p.tick(start.Add(p.interval))
	assert.Equal(t, 1, e)
	assert.Equal(t, 0, u)
}

func TestPacer_HighRateBatchesPerTick(t *testing.T) {
	// At 100k rps the interval clamps to the floor, so a single on-schedule tick
	// must release a whole batch — this is what removes the per-tick ceiling.
	start := time.Unix(0, 0)
	p := newPacer(100000, start)
	e, u := p.tick(start.Add(p.interval))
	assert.Equal(t, 0, u)
	assert.Greater(t, e, 1, "high rate must batch multiple events per tick")
	assert.InDelta(t, p.perTick, float64(e), 1)
}

func TestPacer_SteadyStateTracksTargetRate(t *testing.T) {
	start := time.Unix(0, 0)
	p := newPacer(10000, start)
	var total, under int
	now := start
	for now.Sub(start) < time.Second {
		now = now.Add(p.interval)
		e, u := p.tick(now)
		total += e
		under += u
	}
	assert.InDelta(t, 10000, total, p.perTick+1, "should release ~target events over one second")
	assert.Equal(t, 0, under, "on-schedule ticks must not underrun")
}

func TestPacer_NoEmitBeforeIntervalElapses(t *testing.T) {
	start := time.Unix(0, 0)
	p := newPacer(1000, start)
	e, u := p.tick(start) // no time has elapsed
	assert.Equal(t, 0, e)
	assert.Equal(t, 0, u)
}

func TestPacer_LateTickReportsUnderrun(t *testing.T) {
	// A full-second stall at 10k rps is far beyond the burst cap: the pacer must
	// release at most the cap and report the rest as underrun rather than fire a
	// thundering catch-up burst.
	start := time.Unix(0, 0)
	p := newPacer(10000, start)
	e, u := p.tick(start.Add(time.Second))
	assert.Greater(t, u, 0, "a long stall must surface as emit underrun")
	assert.LessOrEqual(t, float64(e), p.maxBurst+1, "emit is bounded by the burst cap")
	assert.Greater(t, u, e, "most of a 1s/10k-rps stall is underrun, not emit")
}

func TestPacer_JitterWithinBurstCapCatchesUp(t *testing.T) {
	// A wakeup a few intervals late but within the burst cap is normal scheduler
	// jitter: catch up fully, do not report underrun.
	start := time.Unix(0, 0)
	p := newPacer(10000, start)
	e, u := p.tick(start.Add(4 * p.interval))
	assert.Equal(t, 0, u, "jitter within the burst cap must not be counted as underrun")
	assert.InDelta(t, 4*p.perTick, float64(e), 1)
}

func TestPacer_AccumulatesFractionalRemainder(t *testing.T) {
	// 1s/333 truncates to a 3ms interval, making perTick ~0.999 — a fractional
	// batch. The carried remainder must keep the long-run total on target rather
	// than systematically dropping the fraction.
	start := time.Unix(0, 0)
	p := newPacer(333, start)
	var total int
	now := start
	for i := 0; i < 1000; i++ {
		now = now.Add(p.interval)
		e, _ := p.tick(now)
		total += e
	}
	elapsed := now.Sub(start).Seconds()
	assert.InDelta(t, 333*elapsed, float64(total), 2)
}

func TestRatePacer_SubHertzEmitsAtConfiguredCadence(t *testing.T) {
	start := time.Unix(0, 0)
	p := newRatePacer(0.2, start)

	assert.Equal(t, 5*time.Second, p.interval)
	emit, underrun := p.tick(start.Add(5 * time.Second))
	assert.Equal(t, 1, emit)
	assert.Zero(t, underrun)
	emit, underrun = p.tick(start.Add(10 * time.Second))
	assert.Equal(t, 1, emit)
	assert.Zero(t, underrun)
}
