package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
)

func renderPresenceConsole(w io.Writer, results []presenceStepResult) {
	fmt.Fprintln(w, "N        p50    p95    p99    err%    verdict")
	var lastPass int
	for i := range results {
		r := &results[i]
		nLabel := strconv.Itoa(r.N)
		if r.EffectiveN > 0 && r.EffectiveN != r.N {
			nLabel = fmt.Sprintf("%d(%d)", r.N, r.EffectiveN)
		}
		if r.Kind == verdictPass {
			lastPass = r.N
		}
		fmt.Fprintf(w, "%-8s %-6.0f %-6.0f %-6.0f %-7.3f%% %s\n",
			nLabel, r.P50Ms, r.P95Ms, r.P99Ms, r.ErrorRate*100, r.Kind)
		if r.Kind != verdictPass && len(r.Reasons) > 0 {
			fmt.Fprintf(w, "    reasons: %s\n", joinReasons(r.Reasons))
		}
	}
	fmt.Fprintln(w)
	if lastPass > 0 {
		fmt.Fprintf(w, "ANSWER: N = %d (last passing step)\n", lastPass)
		for i := range results {
			if results[i].Kind == verdictTrip {
				fmt.Fprintf(w, "        Next limit: %s\n", joinReasons(results[i].Reasons))
				break
			}
		}
	} else {
		fmt.Fprintln(w, "ANSWER: no step passed")
	}
}

func writePresenceCSV(path string, results []presenceStepResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"n", "effective_n", "p50_ms", "p95_ms", "p99_ms", "error_rate", "attempted", "failed", "verdict", "reasons"}); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	rs := make([]presenceStepResult, len(results))
	copy(rs, results)
	sort.Slice(rs, func(i, j int) bool { return rs[i].N < rs[j].N })
	for i := range rs {
		r := &rs[i]
		row := []string{
			strconv.Itoa(r.N), strconv.Itoa(r.EffectiveN),
			fmt.Sprintf("%.0f", r.P50Ms), fmt.Sprintf("%.0f", r.P95Ms), fmt.Sprintf("%.0f", r.P99Ms),
			fmt.Sprintf("%.6f", r.ErrorRate),
			strconv.FormatInt(r.Attempted, 10), strconv.FormatInt(r.Failed, 10),
			r.Kind.String(), joinReasons(r.Reasons),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	return nil
}

func renderStormConsole(w io.Writer, results []stormStepResult) {
	fmt.Fprintln(w, "fraction users  recovery   p99    err%    verdict")
	var lastPass float64
	for i := range results {
		r := &results[i]
		if r.Kind == verdictPass {
			lastPass = r.Fraction
		}
		rec := fmt.Sprintf("%.0fms", r.RecoveryMs)
		if !r.RecoveryComplete {
			rec = "INCOMPLETE"
		}
		fmt.Fprintf(w, "%-8.2f %-6d %-10s %-6.0f %-7.3f%% %s\n",
			r.Fraction, r.StormUsers, rec, r.P99Ms, r.ErrorRate*100, r.Kind)
		if r.Kind != verdictPass && len(r.Reasons) > 0 {
			fmt.Fprintf(w, "    reasons: %s\n", joinReasons(r.Reasons))
		}
	}
	fmt.Fprintln(w)
	if lastPass > 0 {
		fmt.Fprintf(w, "ANSWER: max survivable storm = %.2f (largest passing fraction)\n", lastPass)
		for i := range results {
			if results[i].Kind == verdictTrip {
				fmt.Fprintf(w, "        Next limit: %s\n", joinReasons(results[i].Reasons))
				break
			}
		}
	} else {
		fmt.Fprintln(w, "ANSWER: no storm fraction survived")
	}
}

func writeStormCSV(path string, results []stormStepResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"fraction", "storm_users", "recovery_complete", "recovery_ms", "p99_ms", "error_rate", "verdict", "reasons"}); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	for i := range results {
		r := &results[i]
		row := []string{
			fmt.Sprintf("%.2f", r.Fraction), strconv.Itoa(r.StormUsers),
			strconv.FormatBool(r.RecoveryComplete), fmt.Sprintf("%.0f", r.RecoveryMs),
			fmt.Sprintf("%.0f", r.P99Ms), fmt.Sprintf("%.6f", r.ErrorRate),
			r.Kind.String(), joinReasons(r.Reasons),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	return nil
}
