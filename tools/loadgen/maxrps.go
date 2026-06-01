package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"
)

func defaultSteps(workload string) string {
	if workload == "history" {
		return "200,500,1000,2000,5000"
	}
	return "500,1000,2000,5000,10000"
}

func buildThresholds(p95, p99 time.Duration, errRate float64, pendingGrowth uint64, rateTol float64) rpsThresholds {
	return rpsThresholds{P95: p95, P99: p99, ErrorRate: errRate, PendingGrowth: pendingGrowth, RateTolerance: rateTol}
}

// runMaxRPS parses flags, builds the workload adapter, runs the ramp and prints
// the report. Returns the process exit code.
func runMaxRPS(ctx context.Context, cfg *config, args []string) int {
	fs := flag.NewFlagSet("max-rps", flag.ExitOnError)
	workload := fs.String("workload", "messages", "messages|history")
	preset := fs.String("preset", "", "preset name")
	seed := fs.Int64("seed", 42, "RNG seed")
	stepsFlag := fs.String("steps", "", "ascending RPS list, e.g. 500,1k,2k,5k,10k (default depends on workload)")
	warmup := fs.Duration("warmup", 10*time.Second, "per-step warmup (samples discarded)")
	hold := fs.Duration("hold", 30*time.Second, "per-step measurement window")
	cooldown := fs.Duration("cooldown", 5*time.Second, "per-step settle gap")
	sloP95 := fs.Duration("slo-p95", 100*time.Millisecond, "p95 latency SLO (all gated series)")
	sloP99 := fs.Duration("slo-p99", 250*time.Millisecond, "p99 latency SLO (all gated series)")
	sloErr := fs.Float64("slo-error-rate", 0.001, "max error rate (failed/attempted)")
	sloPending := fs.Uint64("slo-pending-growth", 1000, "max per-durable pending growth (messages only)")
	rateTol := fs.Float64("rate-tolerance", 0.05, "achieved-vs-target shortfall band for INCONCLUSIVE")
	stopOnTrip := fs.Bool("stop-on-trip", true, "stop the ramp at the first TRIP")
	inject := fs.String("inject", "frontdoor", "messages only: frontdoor|canonical")
	// history-only tunables (ignored for messages):
	mixFlag := fs.String("mix", "history:80,thread:20", "history only: endpoint mix")
	beforeModeFlag := fs.String("before-mode", "open:70,scrollback:30", "history only: before-cursor mix")
	scrollbackPages := fs.Int("scrollback-pages", 5, "history only: pages per scrollback chain")
	pageLimit := fs.Int("page-limit", 20, "history only: page limit")
	requestTimeout := fs.Duration("request-timeout", 5*time.Second, "history only: per-request timeout")
	csvPath := fs.String("csv", "", "optional CSV output path")
	_ = fs.Parse(args)

	if *preset == "" {
		fmt.Fprintln(os.Stderr, "--preset required")
		return 2
	}
	stepsStr := *stepsFlag
	if stepsStr == "" {
		stepsStr = defaultSteps(*workload)
	}
	steps, err := parseRPSSteps(stepsStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad --steps: %v\n", err)
		return 2
	}
	thresholds := buildThresholds(*sloP95, *sloP99, *sloErr, *sloPending, *rateTol)

	var (
		w        rpsWorkload
		cleanup  func()
		presetID string
	)
	switch *workload {
	case "messages":
		p, ok := BuiltinPreset(*preset)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown preset: %s\n", *preset)
			return 2
		}
		injectMode, err := ParseInjectMode(*inject)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			return 2
		}
		mw, clean, err := newMessagesWorkload(ctx, cfg, &p, injectMode, *seed)
		if err != nil {
			slog.Error("init messages workload", "error", err)
			return 1
		}
		w, cleanup, presetID = mw, clean, p.Name
	case "history":
		p, ok := BuiltinHistoryPreset(*preset)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown history preset: %s\n", *preset)
			return 2
		}
		mix, err := ParseEndpointMix(*mixFlag)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			return 2
		}
		beforeMode, err := ParseBeforeMode(*beforeModeFlag)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			return 2
		}
		if *scrollbackPages <= 0 {
			fmt.Fprintln(os.Stderr, "--scrollback-pages must be > 0")
			return 2
		}
		if *pageLimit <= 0 {
			fmt.Fprintln(os.Stderr, "--page-limit must be > 0")
			return 2
		}
		if *requestTimeout <= 0 {
			fmt.Fprintln(os.Stderr, "--request-timeout must be > 0")
			return 2
		}
		hw, clean, err := newHistoryWorkload(ctx, cfg, &p, *seed, historyWorkloadParams{
			Mix: mix, BeforeMode: beforeMode, ScrollbackPages: *scrollbackPages,
			PageLimit: *pageLimit, RequestTimeout: *requestTimeout,
		})
		if err != nil {
			slog.Error("init history workload", "error", err)
			return 1
		}
		w, cleanup, presetID = hw, clean, p.Name
	default:
		fmt.Fprintf(os.Stderr, "unknown workload: %s\n", *workload)
		return 2
	}
	defer cleanup()

	results := runRamp(ctx, w, &rampConfig{
		Steps: steps, Warmup: *warmup, Hold: *hold, Cooldown: *cooldown,
		Thresholds: thresholds, StopOnTrip: *stopOnTrip,
	})

	if err := renderRPSReport(os.Stdout, results, w.Label(), presetID); err != nil {
		slog.Warn("render report", "error", err)
	}
	if *csvPath != "" {
		f, err := os.Create(*csvPath)
		if err != nil {
			slog.Error("create csv", "error", err)
		} else {
			if err := writeRPSCSV(f, results); err != nil {
				slog.Error("write csv", "error", err)
			}
			_ = f.Close()
		}
	}
	return maxRPSExitCode(results)
}
