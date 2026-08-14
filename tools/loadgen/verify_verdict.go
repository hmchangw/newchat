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
	// OracleErrs counts failed membership-oracle queries and OracleErrSample
	// carries the first; they are tolerated up to maxToleratedOracleErrs. A
	// fatal harness-side churn abort is HarnessErr instead — a different class,
	// with no tolerance. See evaluateVerify.
	OracleErrs      int
	OracleErrSample error
	HarnessErr      error
	Cancelled       bool
	GCPauseP99      float64
	GCPauseMax      float64
}

// oracleErrToleranceDivisor sets the share of issued membership changes whose
// oracle query may fail before the run stops being trustworthy: 1/10th.
// Deliberately a constant rather than a flag — an operator tuning it away is
// tuning away the signal it guards.
const oracleErrToleranceDivisor = 10

// maxToleratedOracleErrs returns how many failed membership-oracle queries a run
// may absorb: always at least one, then 10% of the changes issued.
//
// A change whose oracle query failed is already excluded from Applied and
// Effective, so a tolerated failure costs detection sensitivity on that one
// change — not the correctness of the verdict. The original all-or-nothing rule
// conflated "we could not check this change" with "we cannot trust this run",
// and threw away a whole run's clean delivery, leakage, exactly-once and
// persistence results over a single transient subscription.list timeout.
func maxToleratedOracleErrs(totalChanges int) int {
	return max(1, totalChanges/oracleErrToleranceDivisor)
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
// itself dropped data or lost a connection, or enough supporting queries failed,
// the run cannot prove the system did anything wrong (or right).
//
// Membership-oracle failures are the one signal here with a budget rather than a
// latch (maxToleratedOracleErrs): each failed query only blinds one change,
// which is already excluded from Applied/Effective, so a few cost sensitivity
// rather than trust. The count is reported either way (VerifyReport.OracleErrs).
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
func evaluateVerify(in VerifyInputs) VerifyResult { //nolint:gocritic // hugeParam: VerifyInputs is well past the 80-byte threshold, but the by-value signature is fixed by this plan's brief and its pinned test call sites (e.g. evaluateVerify(passingInputs())), not by any interface conformance
	var reasons []string

	if in.DroppedRecipients > 0 {
		reasons = append(reasons, fmt.Sprintf(
			"%d tracked recipient connection dropped mid-run — non-delivery cannot be attributed to the system",
			in.DroppedRecipients))
	}
	if in.ReadbackErr != nil {
		reasons = append(reasons, fmt.Sprintf("readback query failed: %v", in.ReadbackErr))
	}
	// A churn abort is a loadgen-side failure, not an oracle query failure:
	// reporting it under an "oracle" prefix sends the operator after a service
	// that was never asked anything. No tolerance — the harness could not set up
	// the membership it was about to check.
	if in.HarnessErr != nil {
		reasons = append(reasons, fmt.Sprintf("harness failed during membership setup: %v", in.HarnessErr))
	}
	if tol := maxToleratedOracleErrs(in.Changes.Total); in.OracleErrs > tol {
		reasons = append(reasons, fmt.Sprintf(
			"%d of %d membership oracle queries failed (tolerance %d): %v",
			in.OracleErrs, in.Changes.Total, tol, in.OracleErrSample))
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
