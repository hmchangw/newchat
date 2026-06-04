package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"
)

// rpsWorkload is the engine<->adapter seam. RunStep drives open-loop load at
// targetRPS, owning its own warmup/hold measurement boundaries, and returns the
// normalized inputs for the hold window. The engine owns cooldown and stop-on-trip.
type rpsWorkload interface {
	RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error)
	Label() string
}

// rampConfig parameterizes a ramp.
type rampConfig struct {
	Steps                  []int
	Warmup, Hold, Cooldown time.Duration
	Thresholds             rpsThresholds
	StopOnTrip             bool
}

// parseRPSSteps parses a comma-separated, strictly-ascending list of positive
// RPS values. A trailing "k" multiplies by 1000 (e.g. "5k" -> 5000).
func parseRPSSteps(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	prev := 0
	for _, raw := range parts {
		tok := strings.TrimSpace(raw)
		if tok == "" {
			return nil, fmt.Errorf("empty step in %q", s)
		}
		mult := 1
		if strings.HasSuffix(tok, "k") || strings.HasSuffix(tok, "K") {
			mult = 1000
			tok = tok[:len(tok)-1]
		}
		n, err := strconv.Atoi(strings.TrimSpace(tok))
		if err != nil {
			return nil, fmt.Errorf("bad step %q: %w", raw, err)
		}
		if mult > 1 && n > math.MaxInt/mult {
			return nil, fmt.Errorf("step %q overflows int", raw)
		}
		n *= mult
		if n <= 0 {
			return nil, fmt.Errorf("step must be > 0, got %d", n)
		}
		if n <= prev {
			return nil, fmt.Errorf("steps must be strictly ascending, got %d after %d", n, prev)
		}
		prev = n
		out = append(out, n)
	}
	return out, nil
}

// waitOrCancel sleeps for d or returns early with ctx.Err() if ctx is cancelled.
func waitOrCancel(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// runRamp executes each step in order. It stops early on the first TRIP when
// StopOnTrip is set (an INCONCLUSIVE step never stops the ramp), and on ctx
// cancellation, returning whatever results were gathered.
func runRamp(ctx context.Context, w rpsWorkload, cfg *rampConfig) []rpsStepResult {
	var results []rpsStepResult
	for i, n := range cfg.Steps {
		if ctx.Err() != nil {
			break
		}
		stepStart := time.Now()
		in, err := w.RunStep(ctx, n, cfg.Warmup, cfg.Hold)
		stepEnd := time.Now()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			slog.Warn("step run failed", "rps", n, "error", err)
			break
		}
		res := evaluateRPSStep(&in, cfg.Thresholds)
		// RunStep does warmup then hold sequentially; approximate the hold
		// window as [start+warmup, end] so metric queries skip the ramp-up.
		// Note: stepEnd includes any post-hold drain the adapter performs, so
		// HoldEnd may trail the true hold end by the drain duration.
		res.HoldStart = stepStart.Add(cfg.Warmup)
		res.HoldEnd = stepEnd
		results = append(results, res)
		slog.Info("step complete", "rps", n, "verdict", res.Kind.String(),
			"achieved", res.AchievedRPS, "reasons", res.Reasons)
		if cfg.StopOnTrip && res.Kind == verdictTrip {
			break
		}
		if i < len(cfg.Steps)-1 {
			if err := waitOrCancel(ctx, cfg.Cooldown); err != nil {
				break
			}
		}
	}
	return results
}

// maxRPSExitCode returns 0 if any step PASSed, else 1.
func maxRPSExitCode(results []rpsStepResult) int {
	for i := range results {
		if results[i].Kind == verdictPass {
			return 0
		}
	}
	return 1
}
