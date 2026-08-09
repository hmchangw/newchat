package main

import "fmt"

// Verdict is the run outcome.
type Verdict string

const (
	VerdictPass         Verdict = "PASS"
	VerdictFail         Verdict = "FAIL"
	VerdictInconclusive Verdict = "INCONCLUSIVE"
)

// ExitCode maps a verdict to a process exit code so the command can be
// scripted without parsing stdout.
func (v Verdict) ExitCode() int {
	switch v {
	case VerdictPass:
		return 0
	case VerdictFail:
		return 1
	default:
		return 2
	}
}

// VerifyInputs is everything the evaluator needs.
type VerifyInputs struct {
	Violations []Violation
	Counts     ProbeCounts
	Changes    ChangeCounts
	MinProbes  int
	// MultiplexDrops is reported as load context only — it does not gate the
	// verdict. See evaluateVerify.
	MultiplexDrops    int64
	DroppedRecipients int
	ReadbackErr       error
	OracleErr         error
	Cancelled         bool
	GCPauseP99        float64
	GCPauseMax        float64
}

// VerifyResult is the evaluated outcome plus human-readable reasons.
type VerifyResult struct {
	Verdict    Verdict     `json:"verdict"`
	Reasons    []string    `json:"reasons,omitempty"`
	Violations []Violation `json:"violations,omitempty"`
}

// evaluateVerify decides PASS / FAIL / INCONCLUSIVE.
//
// INCONCLUSIVE overrides both others: it means the signals cannot be trusted,
// so reporting PASS or FAIL would be a lie either way. A violation only means
// something if we can attribute it to the system under test — if the harness
// itself dropped data or lost a connection, or a supporting query failed, the
// run cannot prove the system did anything wrong (or right).
//
// Membership churn (Changes) is deliberately NOT considered here: churn
// legitimately changes the expected recipient set and is not, on its own,
// evidence that measurement was untrustworthy.
//
// Multiplex drops (MultiplexDrops) are deliberately NOT considered here either,
// though they were originally. The multiplex pool's per-user inbox channels are
// write-only by design — nothing in the tree ever receives from them, so a full
// inbox is their normal steady state under background load, and `daily` has
// always dropped there without caring. Meanwhile preflightVerify refuses to
// start unless every probe-room member is in the *direct* pool. A multiplex
// user is therefore never an expected probe recipient, and a multiplex drop
// cannot affect probe accounting at all: the loss is on a population probes
// never touch, which makes it perfectly distinguishable from a delivery bug.
// Gating on it made PASS unreachable in the default configuration — with
// `daily-heavy` roughly 7000 users sit on multiplex, so drops are certain
// within seconds. The count is still reported as load context (VerifyReport).
func evaluateVerify(in VerifyInputs) VerifyResult { //nolint:gocritic // hugeParam: VerifyInputs is 192 bytes, but the by-value signature is fixed by this plan's brief and its pinned test call sites (e.g. evaluateVerify(passingInputs())), not by any interface conformance
	var reasons []string

	if in.DroppedRecipients > 0 {
		reasons = append(reasons, fmt.Sprintf(
			"%d tracked recipient connection dropped mid-run — non-delivery cannot be attributed to the system",
			in.DroppedRecipients))
	}
	if in.ReadbackErr != nil {
		reasons = append(reasons, fmt.Sprintf("readback query failed: %v", in.ReadbackErr))
	}
	if in.OracleErr != nil {
		reasons = append(reasons, fmt.Sprintf("membership oracle query failed: %v", in.OracleErr))
	}
	if in.Counts.Tracked < in.MinProbes {
		reasons = append(reasons, fmt.Sprintf(
			"only %d probes tracked, below --min-probes=%d (%d suppressed by settle windows)",
			in.Counts.Tracked, in.MinProbes, in.Counts.Suppressed))
	}
	if in.Cancelled {
		reasons = append(reasons, "run cancelled before completion")
	}
	if in.GCPauseMax > 0 && in.GCPauseP99 > in.GCPauseMax {
		reasons = append(reasons, fmt.Sprintf(
			"loadgen GC pause p99 %.1fms exceeds %.1fms — the load box was saturated",
			in.GCPauseP99, in.GCPauseMax))
	}

	if len(reasons) > 0 {
		return VerifyResult{Verdict: VerdictInconclusive, Reasons: reasons, Violations: in.Violations}
	}
	if len(in.Violations) > 0 {
		return VerifyResult{Verdict: VerdictFail, Violations: in.Violations}
	}
	return VerifyResult{Verdict: VerdictPass}
}
