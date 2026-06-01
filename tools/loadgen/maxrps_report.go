package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
)

// lastPassRPS returns the largest TargetRPS whose step PASSed, or 0 if none.
// Assumes results are in ascending step order.
func lastPassRPS(results []rpsStepResult) int {
	last := 0
	for i := range results {
		if results[i].Kind == verdictPass {
			last = results[i].TargetRPS
		}
	}
	return last
}

// firstTrip returns the first tripped step, or nil if none tripped.
func firstTrip(results []rpsStepResult) *rpsStepResult {
	for i := range results {
		if results[i].Kind == verdictTrip {
			return &results[i]
		}
	}
	return nil
}

// seriesNames returns the ordered union of latency-series names across results.
func seriesNames(results []rpsStepResult) []string {
	var names []string
	seen := map[string]bool{}
	for i := range results {
		for _, sp := range results[i].Latencies {
			if !seen[sp.Name] {
				seen[sp.Name] = true
				names = append(names, sp.Name)
			}
		}
	}
	return names
}

// pctFor returns the percentiles for a named series in a result (zero if absent).
func pctFor(r *rpsStepResult, name string) Percentiles {
	for _, sp := range r.Latencies {
		if sp.Name == name {
			return sp.Pct
		}
	}
	return Percentiles{}
}

// renderRPSReport writes the per-step table and the ANSWER line.
func renderRPSReport(w io.Writer, results []rpsStepResult, workload, preset string) error {
	fmt.Fprintf(w, "=== loadgen max-rps complete (workload=%s, preset=%s) ===\n\n", workload, preset)
	names := seriesNames(results)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	header := []string{"target_rps", "achieved_rps"}
	for _, n := range names {
		header = append(header, n+" p95", n+" p99")
	}
	header = append(header, "err%", "worst_pending", "verdict")
	fmt.Fprintln(tw, strings.Join(header, "\t"))

	for i := range results {
		r := &results[i]
		row := []string{strconv.Itoa(r.TargetRPS), fmt.Sprintf("%.0f", r.AchievedRPS)}
		for _, n := range names {
			p := pctFor(r, n)
			row = append(row, p.P95.String(), p.P99.String())
		}
		pending := "-"
		if r.WorstDurable != "" {
			pending = fmt.Sprintf("%s +%d", r.WorstDurable, r.WorstDelta)
		}
		row = append(row, fmt.Sprintf("%.3f", r.ErrorRate*100), pending, r.Kind.String())
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush table: %w", err)
	}

	fmt.Fprintln(w)
	pass := lastPassRPS(results)
	if pass == 0 {
		fmt.Fprintf(w, "ANSWER: no step passed (workload=%s, preset=%s)\n", workload, preset)
		return nil
	}
	fmt.Fprintf(w, "ANSWER: max RPS = %d (workload=%s, preset=%s)\n", pass, workload, preset)
	if trip := firstTrip(results); trip != nil {
		fmt.Fprintf(w, "        Next limit: %s\n", strings.Join(trip.Reasons, "; "))
	}
	return nil
}

// writeRPSCSV writes one row per step. Series percentile columns are emitted in
// the union order of series names across all steps.
func writeRPSCSV(w io.Writer, results []rpsStepResult) error {
	cw := csv.NewWriter(w)
	names := seriesNames(results)

	header := []string{"target_rps", "achieved_rps"}
	for _, n := range names {
		header = append(header, n+"_p95_ms", n+"_p99_ms")
	}
	header = append(header, "error_rate", "attempted", "failed", "saturation", "worst_durable", "worst_pending_delta", "verdict", "reasons")
	if err := cw.Write(header); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	for i := range results {
		r := &results[i]
		row := []string{strconv.Itoa(r.TargetRPS), fmt.Sprintf("%.1f", r.AchievedRPS)}
		for _, n := range names {
			p := pctFor(r, n)
			row = append(row,
				strconv.FormatInt(p.P95.Milliseconds(), 10),
				strconv.FormatInt(p.P99.Milliseconds(), 10))
		}
		row = append(row,
			strconv.FormatFloat(r.ErrorRate, 'f', 6, 64),
			strconv.Itoa(r.AttemptedOps), strconv.Itoa(r.FailedOps), strconv.Itoa(r.Saturation),
			r.WorstDurable, strconv.FormatInt(r.WorstDelta, 10),
			r.Kind.String(), strings.Join(r.Reasons, "; "))
		if err := cw.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}
	return nil
}
