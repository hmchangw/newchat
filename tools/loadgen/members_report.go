package main

import (
	"fmt"
	"io"
	"sort"
	"text/tabwriter"
	"time"
)

// MembersSummary is the sustained-mode end-of-run report.
type MembersSummary struct {
	Preset, Site, Inject, Shape string
	Seed                        int64
	TargetRate                  int
	ActualRate                  float64
	Duration, Warmup            time.Duration
	UsersPerAdd                 int
	Sent                        int
	SentMeasured                int
	PublishErrors               int
	RoomServiceErrors           int
	MissingReplies              int
	MissingEvents               int
	E1                          Percentiles
	E2                          Percentiles
	E1Count, E2Count            int
	Consumers                   []ConsumerStat
}

// PrintMembersSummary writes the sustained-mode summary to w.
func PrintMembersSummary(w io.Writer, s *MembersSummary) error {
	fmt.Fprintln(w, "=== loadgen members-sustained complete ===")
	fmt.Fprintf(w, "preset: %s    seed: %d    site: %s\n", s.Preset, s.Seed, s.Site)
	fmt.Fprintf(w, "duration: %s (warmup: %s, measured: %s)    inject: %s    shape: %s\n",
		s.Duration, s.Warmup, s.Duration-s.Warmup, s.Inject, s.Shape)
	fmt.Fprintf(w, "target rate: %d req/s    actual rate: %.1f req/s\n", s.TargetRate, s.ActualRate)

	fmt.Fprintln(w, "\npublish results")
	fmt.Fprintf(w, "  users per add:    %d\n", s.UsersPerAdd)
	fmt.Fprintf(w, "  sent (total):     %d\n", s.Sent)
	fmt.Fprintf(w, "  sent (measured):  %d   ← compared to E1/E2 counts below\n", s.SentMeasured)
	fmt.Fprintf(w, "  publish errors:    %d\n", s.PublishErrors)
	fmt.Fprintf(w, "  room-service errors: %d\n", s.RoomServiceErrors)
	fmt.Fprintf(w, "  missing replies:   %d\n", s.MissingReplies)
	fmt.Fprintf(w, "  missing member events:%d\n\n", s.MissingEvents)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "latency (measured window only)")
	fmt.Fprintln(tw, "metric\tcount\tp50\tp95\tp99\tmax")
	fmt.Fprintf(tw, "E1 reply\t%d\t%s\t%s\t%s\t%s\n", s.E1Count, s.E1.P50, s.E1.P95, s.E1.P99, s.E1.Max)
	fmt.Fprintf(tw, "E2 member-event\t%d\t%s\t%s\t%s\t%s\n", s.E2Count, s.E2.P50, s.E2.P95, s.E2.P99, s.E2.Max)
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush latency table: %w", err)
	}

	if len(s.Consumers) > 0 {
		fmt.Fprintf(w, "\nconsumer lag (%s)\n", s.Consumers[0].Stream)
		tw2 := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw2, "durable\tmin_pending\tpeak_pending\tfinal_pending\tpeak_ack_pending\tredelivered")
		for i := range s.Consumers {
			c := &s.Consumers[i]
			fmt.Fprintf(tw2, "%s\t%d\t%d\t%d\t%d\t%d\n",
				c.Durable, c.MinPending, c.PeakPending, c.FinalPending, c.PeakAckPending, c.Redelivered)
		}
		if err := tw2.Flush(); err != nil {
			return fmt.Errorf("flush consumer table: %w", err)
		}
	}
	return nil
}

// SizeBucket holds latency aggregates for one (lower, upper) room-size range.
type SizeBucket struct {
	Lower, Upper int
	Count        int
	E1, E2       Percentiles
}

// CapacitySummary is the capacity-mode end-of-run report.
type CapacitySummary struct {
	Preset, Site, Inject, Shape string
	Seed                        int64
	UsersPerAdd, TargetSize     int
	PublishErrors               int
	Timeouts                    int
	Buckets                     []SizeBucket
	FinalSizes                  map[string]int
}

// PrintCapacitySummary writes the capacity-mode summary to w.
func PrintCapacitySummary(w io.Writer, s *CapacitySummary) error {
	fmt.Fprintln(w, "=== loadgen members-capacity complete ===")
	fmt.Fprintf(w, "preset: %s    seed: %d    site: %s\n", s.Preset, s.Seed, s.Site)
	fmt.Fprintf(w, "inject: %s    shape: %s    users per add: %d    target size: %d\n\n",
		s.Inject, s.Shape, s.UsersPerAdd, s.TargetSize)
	fmt.Fprintf(w, "publish errors: %d    timeouts: %d\n\n", s.PublishErrors, s.Timeouts)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "size_bucket\tcount\te1_p50\te1_p99\te2_p50\te2_p99")
	for _, b := range s.Buckets {
		fmt.Fprintf(tw, "%d-%d\t%d\t%s\t%s\t%s\t%s\n",
			b.Lower, b.Upper, b.Count, b.E1.P50, b.E1.P99, b.E2.P50, b.E2.P99)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush bucket table: %w", err)
	}

	fmt.Fprintln(w, "\nfinal sizes")
	ids := make([]string, 0, len(s.FinalSizes))
	for id := range s.FinalSizes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	tw2 := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw2, "room_id\tfinal_size")
	for _, id := range ids {
		fmt.Fprintf(tw2, "%s\t%d\n", id, s.FinalSizes[id])
	}
	if err := tw2.Flush(); err != nil {
		return fmt.Errorf("flush final sizes table: %w", err)
	}
	return nil
}

// BucketIndex returns the index of the bucket whose [lower, upper) contains
// size, or -1 if size < edges[0] or size >= edges[len-1]. edges must be
// strictly increasing.
func BucketIndex(size int, edges []int) int {
	if len(edges) < 2 {
		return -1
	}
	if size < edges[0] || size >= edges[len(edges)-1] {
		return -1
	}
	for i := 0; i < len(edges)-1; i++ {
		if size >= edges[i] && size < edges[i+1] {
			return i
		}
	}
	return -1
}
