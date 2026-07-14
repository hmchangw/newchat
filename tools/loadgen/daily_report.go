package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// renderConsole writes a human-readable step-by-step table plus the ANSWER
// line (largest passing N) to w. When EffectiveN differs materially from N,
// the discrepancy is annotated so an operator doesn't read "N=20000 PASS"
// when only half the users were actually active.
func renderConsole(w io.Writer, results []StepResult) {
	fmt.Fprintln(w, "N        p50    p95    p99    err%    worst-pending-delta             verdict")
	var lastPass int
	for i := range results {
		r := &results[i]
		var verdict string
		switch {
		case r.Inconclusive:
			verdict = "INCONCLUSIVE"
		case r.Tripped:
			verdict = "TRIP"
		default:
			verdict = "PASS"
			lastPass = r.N
		}
		worst := worstPending(r.ConsumerPending)
		nLabel := strconv.Itoa(r.N)
		if r.EffectiveN > 0 && r.EffectiveN != r.N {
			nLabel = fmt.Sprintf("%d(%d)", r.N, r.EffectiveN)
		}
		fmt.Fprintf(w, "%-8s %-6.0f %-6.0f %-6.0f %-7.2f%% %-30s %s\n",
			nLabel, r.P50LatencyMs, r.P95LatencyMs, r.P99LatencyMs,
			r.ErrorRate*100, worst, verdict)
		if (r.Tripped || r.Inconclusive) && len(r.TrippedReasons) > 0 {
			fmt.Fprintf(w, "    reasons: %s\n", joinReasons(r.TrippedReasons))
		}
		if len(r.ActionLatencies) > 0 {
			fmt.Fprintf(w, "    actions: %s\n", formatActionLatencies(r.ActionLatencies))
		}
		if r.Presence != nil {
			p := r.Presence
			fmt.Fprintf(w, "    presence: p50=%.0f p95=%.0f p99=%.0f err%%=%.3f (attempted=%d failed=%d)\n",
				p.P50Ms, p.P95Ms, p.P99Ms, p.ErrorRate*100, p.Attempted, p.Failed)
		}
	}
	fmt.Fprintln(w)
	if lastPass > 0 {
		fmt.Fprintf(w, "ANSWER: N = %d (last passing step)\n", lastPass)
		for i := range results {
			if results[i].Tripped {
				fmt.Fprintf(w, "        Next limit: %s\n", joinReasons(results[i].TrippedReasons))
				break
			}
		}
	} else {
		fmt.Fprintln(w, "ANSWER: no step passed")
	}
}

func worstPending(m map[string]ConsumerPendingDelta) string {
	var worstName string
	var worstDelta int64
	for name, d := range m {
		if d.Delta > worstDelta {
			worstDelta = d.Delta
			worstName = name
		}
	}
	if worstName == "" {
		return "-"
	}
	return fmt.Sprintf("%s +%d", worstName, worstDelta)
}

func joinReasons(rs []string) string {
	return strings.Join(rs, "; ")
}

// formatActionLatencies renders per-action stats on a single line in
// canonical action order. Skips actions with zero samples so the line
// stays readable when only a subset fired during the hold.
//
// Example: "send n=8920 p50=12 p95=180 p99=320 | scroll_history n=540 p50=8 p95=42 p99=95"
func formatActionLatencies(stats map[string]ActionLatencyStats) string {
	var parts []string
	for _, k := range allActionKinds {
		name := k.String()
		s, ok := stats[name]
		if !ok || s.Count == 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s n=%d p50=%.0f p95=%.0f p99=%.0f",
			name, s.Count, s.P50Ms, s.P95Ms, s.P99Ms))
	}
	return strings.Join(parts, " | ")
}

// writeDailyCSV writes one row per StepResult, sorted ascending by N.
func writeDailyCSV(path string, results []StepResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"n", "effective_n", "started_at", "p50_ms", "p95_ms", "p99_ms",
		"error_rate", "attempted_ops", "failed_ops",
		"worst_durable", "worst_pending_delta",
		"tripped", "inconclusive", "tripped_reasons",
	}
	// Per-action columns in stable order: <name>_count, _p50_ms, _p95_ms, _p99_ms.
	// Every step writes every column even when count=0, so the schema is
	// fixed across the file and downstream tools can column-index reliably.
	for _, k := range allActionKinds {
		name := k.String()
		header = append(header,
			name+"_count", name+"_p50_ms", name+"_p95_ms", name+"_p99_ms")
	}
	header = append(header,
		"presence_p50_ms", "presence_p95_ms", "presence_p99_ms",
		"presence_attempted", "presence_failed", "presence_error_rate")
	if err := w.Write(header); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	rs := make([]StepResult, len(results))
	copy(rs, results)
	sort.Slice(rs, func(i, j int) bool { return rs[i].N < rs[j].N })

	for i := range rs {
		r := &rs[i]
		worstName, worstDelta := "", int64(0)
		for name, d := range r.ConsumerPending {
			if d.Delta > worstDelta {
				worstDelta, worstName = d.Delta, name
			}
		}
		row := []string{
			strconv.Itoa(r.N),
			strconv.Itoa(r.EffectiveN),
			r.StartedAt.UTC().Format("2006-01-02T15:04:05Z"),
			fmt.Sprintf("%.0f", r.P50LatencyMs),
			fmt.Sprintf("%.0f", r.P95LatencyMs),
			fmt.Sprintf("%.0f", r.P99LatencyMs),
			fmt.Sprintf("%.6f", r.ErrorRate),
			strconv.FormatInt(r.AttemptedOps, 10),
			strconv.FormatInt(r.FailedOps, 10),
			worstName,
			strconv.FormatInt(worstDelta, 10),
			strconv.FormatBool(r.Tripped),
			strconv.FormatBool(r.Inconclusive),
			joinReasons(r.TrippedReasons),
		}
		for _, k := range allActionKinds {
			s := r.ActionLatencies[k.String()]
			row = append(row,
				strconv.Itoa(s.Count),
				fmt.Sprintf("%.0f", s.P50Ms),
				fmt.Sprintf("%.0f", s.P95Ms),
				fmt.Sprintf("%.0f", s.P99Ms),
			)
		}
		var pP50, pP95, pP99, pErr float64
		var pAtt, pFail int64
		if r.Presence != nil {
			pP50, pP95, pP99 = r.Presence.P50Ms, r.Presence.P95Ms, r.Presence.P99Ms
			pAtt, pFail, pErr = r.Presence.Attempted, r.Presence.Failed, r.Presence.ErrorRate
		}
		row = append(row,
			fmt.Sprintf("%.0f", pP50), fmt.Sprintf("%.0f", pP95), fmt.Sprintf("%.0f", pP99),
			strconv.FormatInt(pAtt, 10), strconv.FormatInt(pFail, 10), fmt.Sprintf("%.6f", pErr))
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	return nil
}
