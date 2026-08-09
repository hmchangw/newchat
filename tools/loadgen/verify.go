package main

import (
	"context"
	"flag"
	"fmt"
	"time"
)

// verifyConfig is the parsed CLI surface for the verify scenario.
type verifyConfig struct {
	Preset             string
	Users              int
	ProbeRooms         int
	ReserveUsers       int
	MemberChurn        float64
	Settle             time.Duration
	Warmup             time.Duration
	Steady             time.Duration
	Drain              time.Duration
	ProbeRate          float64
	MinProbes          int
	LargeRoomThreshold int
	Lane               string
	DirectOnly         bool
	Seed               int64
	JSONPath           string
}

// parseVerifyFlags parses the verify subcommand's flags and validates them.
func parseVerifyFlags(args []string) (verifyConfig, error) {
	var vc verifyConfig
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.StringVar(&vc.Preset, "preset", "daily-heavy", "daily-light | daily-heavy | daily-power")
	fs.IntVar(&vc.Users, "users", 0, "total activation count (0 = preset default)")
	fs.IntVar(&vc.ProbeRooms, "probe-rooms", 50, "number of probe rooms")
	fs.IntVar(&vc.ReserveUsers, "reserve-users", 200, "direct-connected floaters for membership changes")
	fs.Float64Var(&vc.MemberChurn, "member-churn", 0.2, "membership changes per probe room per minute (0 disables)")
	fs.DurationVar(&vc.Settle, "settle", 5*time.Second, "post-change quiet window per room")
	fs.DurationVar(&vc.Warmup, "warmup", 30*time.Second, "pre-measurement settle")
	fs.DurationVar(&vc.Steady, "steady", 120*time.Second, "probe-generating window")
	fs.DurationVar(&vc.Drain, "drain", 30*time.Second, "post-quiesce wait for in-flight probes")
	fs.Float64Var(&vc.ProbeRate, "probe-rate", 0.01, "fraction of probe-room sends tracked")
	fs.IntVar(&vc.MinProbes, "min-probes", 50, "below this, the verdict is INCONCLUSIVE")
	fs.IntVar(&vc.LargeRoomThreshold, "large-room-threshold", 500, "must match the gatekeeper's setting")
	fs.StringVar(&vc.Lane, "lane", "both", "global | local | both")
	fs.BoolVar(&vc.DirectOnly, "direct-only", false, "disable multiplex; every user gets a dedicated conn")
	fs.Int64Var(&vc.Seed, "seed", 42, "drives fixtures, probe rooms, and probe selection")
	fs.StringVar(&vc.JSONPath, "json", "", "full violation detail output path")

	if err := fs.Parse(args); err != nil {
		return vc, fmt.Errorf("parse verify flags: %w", err)
	}
	switch vc.Lane {
	case "global", "local", "both":
	default:
		return vc, fmt.Errorf("invalid --lane %q: want global, local, or both", vc.Lane)
	}
	if vc.ProbeRate < 0 || vc.ProbeRate > 1 {
		return vc, fmt.Errorf("invalid --probe-rate %v: want a fraction in [0,1]", vc.ProbeRate)
	}
	if vc.ProbeRooms <= 0 {
		return vc, fmt.Errorf("invalid --probe-rooms %d: want a positive count", vc.ProbeRooms)
	}
	if vc.MinProbes < 1 {
		return vc, fmt.Errorf(
			"invalid --min-probes %d: must be at least 1 — at 0 the probe floor in evaluateVerify "+
				"(tracked < min-probes) can never fire, so a run that tracked zero probes would "+
				"report PASS instead of INCONCLUSIVE", vc.MinProbes)
	}
	return vc, nil
}

// preflightVerify fails fast on configurations that would produce phantom
// violations. Seconds spent here beat a ten-minute run reporting a fake bug.
func preflightVerify(_ context.Context, vc verifyConfig, prs ProbeRoomSet, directSize int) error { //nolint:gocritic // hugeParam: verifyConfig is 152 bytes, but the by-value signature is fixed by this task's brief and its pinned test call sites (e.g. preflightVerify(t.Context(), vc, prs, 1)), not by any interface conformance
	for i := range prs.Rooms {
		r := &prs.Rooms[i]
		if r.UserCount >= vc.LargeRoomThreshold {
			return fmt.Errorf(
				"probe room %s has %d members, at or above --large-room-threshold=%d: "+
					"the gatekeeper rejects non-thread sends from member-role users there, "+
					"which is indistinguishable from message loss",
				r.ID, r.UserCount, vc.LargeRoomThreshold)
		}
	}
	if directSize < len(prs.Members) {
		return fmt.Errorf(
			"direct pool holds %d of %d probe-room members: a member without a dedicated "+
				"connection cannot be observed, which would corrupt the completeness verdict",
			directSize, len(prs.Members))
	}
	return nil
}

// orderForActivation puts designated users first so they land in the direct
// pool, preserving the original relative order of everyone else. With an empty
// designated set the input order is returned unchanged, which is exactly
// daily's existing behaviour.
func orderForActivation(users []*userState, designated []string) []string {
	want := make(map[string]struct{}, len(designated))
	for _, id := range designated {
		want[id] = struct{}{}
	}

	first := make([]string, 0, len(designated))
	rest := make([]string, 0, len(users))
	for _, u := range users {
		if _, ok := want[u.ID]; ok {
			first = append(first, u.ID)
			continue
		}
		rest = append(rest, u.ID)
	}
	return append(first, rest...)
}
