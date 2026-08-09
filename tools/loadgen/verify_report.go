package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// consoleViolationCap bounds terminal output. Full detail goes to --json.
const consoleViolationCap = 10

// VerifyReport is the full run summary, rendered to console and JSON.
type VerifyReport struct {
	ProbeRooms     int          `json:"probeRooms"`
	ProbeMembers   int          `json:"probeMembers"`
	DirectPoolSize int          `json:"directPoolSize"`
	ReserveSize    int          `json:"reserveSize"`
	BackgroundSize int          `json:"backgroundSize"`
	Counts         ProbeCounts  `json:"counts"`
	Changes        ChangeCounts `json:"changes"`
	Result         VerifyResult `json:"result"`
}

// renderVerifyConsole formats the operator-facing summary. Never includes
// message content — IDs only.
func renderVerifyConsole(rep VerifyReport) string { //nolint:gocritic // hugeParam: VerifyReport is 192 bytes, but the by-value signature is fixed by this plan's brief and its pinned test call sites (e.g. renderVerifyConsole(reportForTest())), not by any interface conformance
	var b strings.Builder

	fmt.Fprintf(&b, "probe rooms: %d / %d members / direct pool %d (%d reserve)\n",
		rep.ProbeRooms, rep.ProbeMembers, rep.DirectPoolSize, rep.ReserveSize)
	fmt.Fprintf(&b, "background:  %d users on multiplex\n", rep.BackgroundSize)
	fmt.Fprintf(&b, "probes:      %d tracked / %d suppressed (settle window)\n",
		rep.Counts.Tracked, rep.Counts.Suppressed)
	fmt.Fprintf(&b, "delivery:    %d complete / %d partial / %d total-loss\n",
		rep.Counts.Complete, rep.Counts.Partial, rep.Counts.TotalLoss)
	fmt.Fprintf(&b, "leakage:     %d unexpected recipients (user lane)\n", rep.Counts.Leaked)
	fmt.Fprintf(&b, "membership:  %d changes (%d add, %d remove) / %d applied / %d effective\n",
		rep.Changes.Total, rep.Changes.Adds, rep.Changes.Removes,
		rep.Changes.Applied, rep.Changes.Effective)

	if len(rep.Result.Reasons) > 0 {
		b.WriteString("\nREASONS\n")
		for _, r := range rep.Result.Reasons {
			fmt.Fprintf(&b, "  %s\n", r)
		}
	}

	if n := len(rep.Result.Violations); n > 0 {
		shown := n
		if shown > consoleViolationCap {
			shown = consoleViolationCap
		}
		fmt.Fprintf(&b, "\nVIOLATIONS (showing %d of %d)\n", shown, n)
		for _, v := range rep.Result.Violations[:shown] {
			fmt.Fprintf(&b, "  %-30s room=%s", v.Kind, v.RoomID)
			if v.MsgID != "" {
				fmt.Fprintf(&b, " msg=%s", v.MsgID)
			}
			if len(v.Users) > 0 {
				fmt.Fprintf(&b, " users=%s", strings.Join(v.Users, ","))
			}
			if v.Detail != "" {
				fmt.Fprintf(&b, "\n      %s", v.Detail)
			}
			b.WriteString("\n")
		}
	}

	fmt.Fprintf(&b, "\nVERDICT: %s\n", rep.Result.Verdict)
	return b.String()
}

// renderVerifyJSON emits the full report, uncapped. This is the artifact
// diffed and grepped across runs, so it must carry every violation the
// console cap trims — see TestRenderVerifyJSON_CarriesAllViolations.
func renderVerifyJSON(rep VerifyReport) ([]byte, error) { //nolint:gocritic // hugeParam: VerifyReport is 192 bytes, but the by-value signature is fixed by this plan's brief and its pinned test call sites (e.g. renderVerifyJSON(reportForTest())), not by any interface conformance
	out, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal verify report: %w", err)
	}
	return out, nil
}
