package main

import "fmt"

type scenarioResult struct {
	Scenario string    `json:"scenario"`
	Kind     string    `json:"kind"`
	Fix      string    `json:"fix"`
	Points   []outcome `json:"points"`
}

type report struct {
	Topology       string           `json:"topology"`
	SubURL         string           `json:"subUrl"`
	PubURL         string           `json:"pubUrl"`
	Iterations     int              `json:"iterations"`
	RenderMS       int              `json:"renderMs"`
	InterestWindow interestWindow   `json:"interestWindow"`
	Scenarios      []scenarioResult `json:"scenarios"`
}

func (r *report) print() {
	fmt.Printf("\n=== roomrace: %s ===\n", r.Topology)
	fmt.Printf("client B  -> %s\n", r.SubURL)
	fmt.Printf("services  -> %s\n", r.PubURL)
	fmt.Printf("iterations per point: %d   client render delay: %dms\n\n", r.Iterations, r.RenderMS)

	iw := r.InterestWindow
	fmt.Printf("subscription interest window (Subscribe() -> publisher actually reaches it)\n")
	fmt.Printf("  samples %d, delivered %d | median %dus  p95 %dus  max %dus | Flush() RTT median %dus\n\n",
		iw.Samples, iw.Delivered, iw.MedianMicros, iw.P95Micros, iw.MaxMicros, iw.FlushRTTMicro)

	if len(r.Scenarios) == 0 {
		return
	}
	header := r.Scenarios[0].Points
	fmt.Printf("%-46s %-22s", "scenario", "fix")
	for _, p := range header {
		fmt.Printf(" %7s", fmt.Sprintf("+%dms", p.SendMS))
	}
	fmt.Printf("\n%s\n", dashes(46+1+22+8*len(header)))

	for _, s := range r.Scenarios {
		fmt.Printf("%-46s %-22s", trunc(s.Scenario, 46), trunc(s.Fix, 22))
		for _, p := range s.Points {
			fmt.Printf(" %6.0f%%", p.MissRate*100)
		}
		fmt.Println()
	}
	fmt.Printf("\n(cell = %% of first messages user B never saw; columns = delay between\n")
	fmt.Printf(" subscription.update and the first message)\n\n")

	fmt.Printf("%-46s %8s %8s %8s %8s %8s\n", "detail (totals across all delays)", "live", "recover", "missed", "dropped", "dupes")
	fmt.Printf("%s\n", dashes(46+5*9))
	for _, s := range r.Scenarios {
		var live, rec, miss, drop, dup int
		for _, p := range s.Points {
			live += p.Live
			rec += p.Recovered
			miss += p.Missed
			drop += p.Dropped
			dup += p.Duplicates
		}
		fmt.Printf("%-46s %8d %8d %8d %8d %8d\n", trunc(s.Scenario, 46), live, rec, miss, drop, dup)
	}
	fmt.Println()
}

func dashes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '-'
	}
	return string(b)
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "~"
}
