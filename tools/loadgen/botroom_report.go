package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func renderBotRoomConsole(w io.Writer, results []BotRoomStepResult) {
	fmt.Fprintln(w, "size     rooms  rate    e2_p50  e2_p95  e2_p99  err%    worst-pending          verdict")
	for i := range results {
		r := &results[i]
		verdict := "PASS"
		if r.Inconclusive {
			verdict = "INCONCLUSIVE"
		} else if r.Tripped {
			verdict = "TRIP"
		}
		fmt.Fprintf(w, "%-8d %-6d %-7d %-7.0f %-7.0f %-7.0f %-7.3f %-22s %s\n",
			r.Size, r.Rooms, r.TargetRate, r.E2P50Ms, r.E2P95Ms, r.E2P99Ms,
			r.ErrorRate*100, worstBotRoomPending(r.ConsumerPending), verdict)
		if len(r.TrippedReasons) > 0 {
			fmt.Fprintf(w, "    reasons: %s\n", strings.Join(r.TrippedReasons, "; "))
		}
	}

	best := -1
	var firstTrip *BotRoomStepResult
	for i := range results {
		if !results[i].Tripped && !results[i].Inconclusive {
			if results[i].Size > best {
				best = results[i].Size
			}
		}
		if results[i].Tripped && firstTrip == nil {
			firstTrip = &results[i]
		}
	}
	if best < 0 {
		fmt.Fprintln(w, "\nANSWER: no step passed")
		return
	}
	fmt.Fprintf(w, "\nANSWER: max room size = %d\n", best)
	if firstTrip != nil && len(firstTrip.TrippedReasons) > 0 {
		fmt.Fprintf(w, "        Next limit: %s\n", firstTrip.TrippedReasons[0])
	}
}

func worstBotRoomPending(m map[string]ConsumerPendingDelta) string {
	wd, wdelta := worstDurable(m)
	if wd == "" {
		return "-"
	}
	return fmt.Sprintf("%s +%d", wd, wdelta)
}

func writeBotRoomCSV(path string, results []BotRoomStepResult) error {
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer func() { _ = f.Close() }()
	cw := csv.NewWriter(f)
	defer cw.Flush()
	header := []string{
		"size", "rooms", "target_rate", "achieved_rate",
		"e2_p50_ms", "e2_p95_ms", "e2_p99_ms", "read_p95_ms", "read_p99_ms",
		"error_rate", "attempted", "failed",
		"worst_durable", "worst_pending_delta", "tripped", "inconclusive", "reasons",
	}
	if err := cw.Write(header); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	for i := range results {
		r := &results[i]
		wd, wdelta := worstDurable(r.ConsumerPending)
		row := []string{
			strconv.Itoa(r.Size), strconv.Itoa(r.Rooms), strconv.Itoa(r.TargetRate),
			strconv.FormatFloat(r.AchievedRate, 'f', 1, 64),
			strconv.FormatFloat(r.E2P50Ms, 'f', 1, 64),
			strconv.FormatFloat(r.E2P95Ms, 'f', 1, 64),
			strconv.FormatFloat(r.E2P99Ms, 'f', 1, 64),
			strconv.FormatFloat(r.ReadP95Ms, 'f', 1, 64),
			strconv.FormatFloat(r.ReadP99Ms, 'f', 1, 64),
			strconv.FormatFloat(r.ErrorRate, 'f', 6, 64),
			strconv.FormatInt(r.Attempted, 10), strconv.FormatInt(r.Failed, 10),
			wd, strconv.FormatInt(wdelta, 10),
			strconv.FormatBool(r.Tripped), strconv.FormatBool(r.Inconclusive),
			strings.Join(r.TrippedReasons, " | "),
		}
		if err := cw.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	return nil
}

func worstDurable(m map[string]ConsumerPendingDelta) (string, int64) {
	worst := ""
	var worstDelta int64
	for durable, d := range m {
		if worst == "" || d.Delta > worstDelta {
			worst = durable
			worstDelta = d.Delta
		}
	}
	return worst, worstDelta
}
