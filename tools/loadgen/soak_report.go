package main

import (
	"fmt"
	"io"
	"sort"
	"text/tabwriter"
	"time"
)

type soakReportRow struct {
	Action       soakRPCAction
	TargetRate   float64
	AchievedRate float64
	Attempted    uint64
	Succeeded    uint64
	Failed       uint64
	Skipped      uint64
	Retries      uint64
	Latency      soakLatencySummary
	EarlyP99     time.Duration
	LateP99      time.Duration
	P99Drift     time.Duration
	Errors       map[soakErrorClass]uint64
}

type soakReport struct {
	Rows                  []soakReportRow
	MutationTargetMissing uint64
	Verifications         map[soakRPCAction]map[soakVerifyClass]uint64
	MeasuredDuration      time.Duration
}

func BuildSoakReport(
	snapshot *soakCollectorSnapshot,
	targetRates map[soakRPCAction]float64,
) soakReport {
	if snapshot == nil {
		return soakReport{}
	}
	report := soakReport{
		Rows:                  make([]soakReportRow, 0, len(snapshot.Actions)),
		MutationTargetMissing: snapshot.MutationTargetMissing,
		Verifications:         snapshot.Verifications,
		MeasuredDuration:      snapshot.MeasuredDuration,
	}
	for action, stats := range snapshot.Actions {
		report.Rows = append(report.Rows, soakReportRow{
			Action:       action,
			TargetRate:   targetRates[action],
			AchievedRate: stats.AchievedRate,
			Attempted:    stats.Attempted,
			Succeeded:    stats.Succeeded,
			Failed:       stats.Failed,
			Skipped:      stats.Skipped,
			Retries:      stats.Retries,
			Latency:      stats.Latency,
			EarlyP99:     stats.EarlyP99,
			LateP99:      stats.LateP99,
			P99Drift:     stats.P99Drift,
			Errors:       snapshot.Errors[action],
		})
	}
	sort.Slice(report.Rows, func(i, j int) bool {
		return report.Rows[i].Action < report.Rows[j].Action
	})
	return report
}

func PrintSoakReport(w io.Writer, report *soakReport) error {
	if report == nil {
		return fmt.Errorf("soak report is required")
	}
	fmt.Fprintln(w, "=== Cassandra Run A soak summary ===")
	fmt.Fprintf(w, "measured duration: %s\n\n", report.MeasuredDuration)
	writer := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(
		writer,
		"action\ttarget/s\tachieved/s\tattempted\tsucceeded\tfailed\tskipped\tretries\tp50\tp95\tp99\tearly_p99\tlate_p99\tdrift",
	)
	for i := range report.Rows {
		row := &report.Rows[i]
		fmt.Fprintf(
			writer,
			"%s\t%.2f\t%.2f\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.Action,
			row.TargetRate,
			row.AchievedRate,
			row.Attempted,
			row.Succeeded,
			row.Failed,
			row.Skipped,
			row.Retries,
			row.Latency.P50,
			row.Latency.P95,
			row.Latency.P99,
			row.EarlyP99,
			row.LateP99,
			row.P99Drift,
		)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush soak RPC table: %w", err)
	}

	fmt.Fprintf(
		w,
		"\nmutation_target_missing: %d\n",
		report.MutationTargetMissing,
	)
	if len(report.Verifications) > 0 {
		fmt.Fprintln(w, "read-back verification")
		actions := make([]string, 0, len(report.Verifications))
		byName := make(map[string]map[soakVerifyClass]uint64)
		for action, classes := range report.Verifications {
			name := string(action)
			actions = append(actions, name)
			byName[name] = classes
		}
		sort.Strings(actions)
		for _, action := range actions {
			classes := make([]string, 0, len(byName[action]))
			for class := range byName[action] {
				classes = append(classes, string(class))
			}
			sort.Strings(classes)
			for _, class := range classes {
				fmt.Fprintf(
					w,
					"  %s %s: %d\n",
					action,
					class,
					byName[action][soakVerifyClass(class)],
				)
			}
		}
	}
	return nil
}
