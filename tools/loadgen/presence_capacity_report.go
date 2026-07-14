package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
)

func renderCapacityConsole(w io.Writer, results []capacityStepResult) {
	fmt.Fprintln(w, "N        cP50   cP95   cP99   false_off  ping%   verdict")
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
		fmt.Fprintf(w, "%-8s %-6.0f %-6.0f %-6.0f %-10d %-7.1f %s\n",
			nLabel, r.ConnectP50Ms, r.ConnectP95Ms, r.ConnectP99Ms,
			r.FalseOfflines, r.PingSustain*100, r.Kind)
		if r.Kind != verdictPass && len(r.Reasons) > 0 {
			fmt.Fprintf(w, "    reasons: %s\n", joinReasons(r.Reasons))
		}
	}
	fmt.Fprintln(w)
	if lastPass > 0 {
		fmt.Fprintf(w, "MAX CONCURRENT ONLINE: %d (last passing step)\n", lastPass)
		for i := range results {
			if results[i].Kind == verdictTrip {
				fmt.Fprintf(w, "        Next limit: %s\n", joinReasons(results[i].Reasons))
				break
			}
		}
	} else {
		fmt.Fprintln(w, "MAX CONCURRENT ONLINE: none (no step passed)")
	}
}

func writeCapacityCSV(path string, results []capacityStepResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	header := []string{
		"n", "effective_n", "started_at",
		"connect_p50_ms", "connect_p95_ms", "connect_p99_ms", "connect_error_rate",
		"false_offlines", "false_offline_rate", "ping_sustain", "verdict", "reasons",
	}
	if err := w.Write(header); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	rs := make([]capacityStepResult, len(results))
	copy(rs, results)
	sort.Slice(rs, func(i, j int) bool { return rs[i].N < rs[j].N })
	for i := range rs {
		r := &rs[i]
		row := []string{
			strconv.Itoa(r.N), strconv.Itoa(r.EffectiveN),
			r.StartedAt.UTC().Format("2006-01-02T15:04:05Z"),
			fmt.Sprintf("%.0f", r.ConnectP50Ms), fmt.Sprintf("%.0f", r.ConnectP95Ms),
			fmt.Sprintf("%.0f", r.ConnectP99Ms), fmt.Sprintf("%.6f", r.ConnectErrorRate),
			strconv.Itoa(r.FalseOfflines), fmt.Sprintf("%.6f", r.FalseOfflineRate),
			fmt.Sprintf("%.4f", r.PingSustain), r.Kind.String(), joinReasons(r.Reasons),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	return nil
}
