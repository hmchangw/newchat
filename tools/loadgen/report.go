package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
	"text/tabwriter"
	"time"
)

// Percentiles holds summary latency percentiles.
type Percentiles struct {
	P50, P95, P99, Max time.Duration
}

// ComputePercentiles returns P50/P95/P99/max of samples. Empty input -> zeros.
// Input does not need to be sorted on entry.
func ComputePercentiles(samples []time.Duration) Percentiles {
	if len(samples) == 0 {
		return Percentiles{}
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pick := func(q float64) time.Duration {
		idx := int(float64(len(sorted)-1) * q)
		return sorted[idx]
	}
	return Percentiles{
		P50: pick(0.50),
		P95: pick(0.95),
		P99: pick(0.99),
		Max: sorted[len(sorted)-1],
	}
}

// ConsumerStat captures the min/peak/final snapshot of a single durable.
type ConsumerStat struct {
	Stream          string
	Durable         string
	BaselinePending uint64
	MinPending      uint64
	PeakPending     uint64
	FinalPending    uint64
	PeakAckPending  uint64
	// Redelivered is the final (at-shutdown) value of NumRedelivered, not a cumulative total.
	Redelivered  uint64
	HasSample    bool
	SampleErrors uint64
}

// Summary is the full end-of-run report.
type Summary struct {
	Preset, Site, Inject string
	Seed                 int64
	TargetRate           int
	ActualRate           float64
	Duration, Warmup     time.Duration
	Sent                 int // total across warmup + measured
	SentMeasured         int // post-warmup only; the denominator for E1/E2 comparisons
	PublishErrors        int
	GatekeeperErrors     int
	MissingReplies       int
	MissingBroadcasts    int
	E1                   Percentiles
	E2                   Percentiles
	E1Count, E2Count     int
	Consumers            []ConsumerStat
}

// PrintSummary writes the terminal summary to w using text/tabwriter.
func PrintSummary(w io.Writer, s *Summary) error {
	fmt.Fprintln(w, "=== loadgen run complete ===")
	fmt.Fprintf(w, "preset: %s    seed: %d    site: %s\n", s.Preset, s.Seed, s.Site)
	fmt.Fprintf(w, "duration: %s (warmup: %s, measured: %s)    inject: %s\n",
		s.Duration, s.Warmup, s.Duration-s.Warmup, s.Inject)
	fmt.Fprintf(w, "target rate: %d msg/s    actual rate: %.1f msg/s\n\n", s.TargetRate, s.ActualRate)

	fmt.Fprintln(w, "publish results")
	fmt.Fprintf(w, "  sent (total):     %d\n", s.Sent)
	fmt.Fprintf(w, "  sent (measured):  %d   ← compared to E1/E2 counts below\n", s.SentMeasured)
	fmt.Fprintf(w, "  publish errors:    %d\n", s.PublishErrors)
	fmt.Fprintf(w, "  gatekeeper errors: %d\n", s.GatekeeperErrors)
	fmt.Fprintf(w, "  missing replies:   %d\n", s.MissingReplies)
	fmt.Fprintf(w, "  missing broadcasts:%d\n\n", s.MissingBroadcasts)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "latency (measured window only)")
	fmt.Fprintln(tw, "metric\tcount\tp50\tp95\tp99\tmax")
	fmt.Fprintf(tw, "E1 gatekeeper\t%d\t%s\t%s\t%s\t%s\n", s.E1Count, s.E1.P50, s.E1.P95, s.E1.P99, s.E1.Max)
	fmt.Fprintf(tw, "E2 broadcast\t%d\t%s\t%s\t%s\t%s\n", s.E2Count, s.E2.P50, s.E2.P95, s.E2.P99, s.E2.Max)
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush latency table: %w", err)
	}

	fmt.Fprintln(w)
	if len(s.Consumers) > 0 {
		fmt.Fprintf(w, "consumer lag (%s)\n", s.Consumers[0].Stream)
		tw2 := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw2, "durable\thas_sample\tsample_errors\tbaseline_pending\tmin_pending\tpeak_pending\tfinal_pending\tpeak_ack_pending\tredelivered")
		for i := range s.Consumers {
			c := &s.Consumers[i]
			fmt.Fprintf(tw2, "%s\t%t\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
				c.Durable, c.HasSample, c.SampleErrors, c.BaselinePending,
				c.MinPending, c.PeakPending, c.FinalPending, c.PeakAckPending, c.Redelivered)
		}
		if err := tw2.Flush(); err != nil {
			return fmt.Errorf("flush consumer table: %w", err)
		}
	}
	return nil
}

// CSVSample is one row in the per-sample CSV dump.
type CSVSample struct {
	TimestampNs int64
	RequestID   string
	Metric      string
	LatencyNs   int64
}

// WriteCSV writes a header and one row per sample.
// csv.Writer buffers internally; individual Write calls never return errors —
// errors surface only via cw.Error() after Flush.
func WriteCSV(w io.Writer, rows []CSVSample) error {
	cw := csv.NewWriter(w)
	// Errors are intentionally discarded here: csv.Writer buffers all writes
	// and accumulates the first error internally. cw.Error() below is the
	// canonical way to retrieve it after Flush.
	_ = cw.Write([]string{"timestamp_ns", "request_id", "metric", "latency_ns"})
	for i := range rows {
		r := &rows[i]
		_ = cw.Write([]string{
			strconv.FormatInt(r.TimestampNs, 10),
			r.RequestID, r.Metric,
			strconv.FormatInt(r.LatencyNs, 10),
		})
	}
	cw.Flush()
	return cw.Error()
}

// DetermineExitCode returns 0 if error count is within 0.1% of sent.
// With sent == 0, any error is a failure.
func DetermineExitCode(sent, errs int) int {
	if sent == 0 {
		if errs == 0 {
			return 0
		}
		return 1
	}
	// 0.1% tolerance inclusive: errs * 1000 <= sent
	if errs*1000 <= sent {
		return 0
	}
	return 1
}
