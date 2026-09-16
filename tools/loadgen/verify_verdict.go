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
	// SlowConsumers counts slow-consumer errors raised on recipient connections.
	// nats.go drops deliveries when a subscription's bounded pending queue
	// overflows, and a dropped delivery is indistinguishable downstream from one
	// the system never sent — Finalize would report the gap as
	// missing_recipient or total_loss, attributing a harness fault to the system.
	SlowConsumers int
	ReadbackErr   error
	// OracleErrs counts changes an oracle failed to resolve — subscription.list
	// erroring, or the authorization probe answering nothing — and
	// OracleErrSample carries the first; they are tolerated up to
	// maxToleratedOracleErrs. A change is resolved only when BOTH oracles
	// answered, so a half-observed change lands here rather than reading as a
	// clean result with the effectiveness assertion silently skipped. A fatal
	// harness-side churn abort is HarnessErr instead — a different class, with
	// no tolerance. See evaluateVerify.
	OracleErrs      int
	OracleErrSample error
	// ChangesUnobserved counts membership changes that were issued but still
	// inside their settle window when driveChurn left its loop, so neither
	// oracle ever ran for them. They share OracleErrs' tolerance budget: both
	// mean "this change's outcome is unknown". See evaluateVerify.
	ChangesUnobserved int
	// ChurnRequested reports whether --member-churn asked for any churn at all.
	// The evaluator cannot infer it from Changes: zero changes is the expected
	// result of --member-churn=0 and a failed run otherwise.
	ChurnRequested bool
	HarnessErr     error
	Cancelled      bool
	GCPauseP99     float64
	GCPauseMax     float64
}

// oracleErrToleranceDivisor sets the share of issued membership changes whose
// oracle query may fail before the run stops being trustworthy: 1/10th.
// Deliberately a constant rather than a flag — an operator tuning it away is
// tuning away the signal it guards.
const oracleErrToleranceDivisor = 10

// maxToleratedOracleErrs returns how many membership changes a run may leave
// unresolved: always at least one, then 10% of the changes issued.
//
// A change that could not be resolved is already excluded from Applied and
// Effective, so a tolerated one costs detection sensitivity on that one change —
// not the correctness of the verdict. The original all-or-nothing rule
// conflated "we could not check this change" with "we cannot trust this run",
// and threw away a whole run's clean delivery, leakage, exactly-once and
// persistence results over a single transient subscription.list timeout.
//
// Two distinct failures spend this one budget, because they say the same thing
// about the verdict: an oracle that did not answer (VerifyInputs.OracleErrs —
// subscription.list erroring, or the authorization probe being unobservable,
// since a change is resolved only when BOTH answered) and a change dropped
// still inside its settle window when the steady window closed
// (VerifyInputs.ChangesUnobserved). Splitting the budget would let a run hide
// nine of each behind two separate sub-tolerances.
func maxToleratedOracleErrs(totalChanges int) int {
	return max(1, totalChanges/oracleErrToleranceDivisor)
}

// membershipNotExercisedReason explains a run that asked for churn and resolved
// nothing. The two ways to get there need different advice, so they read
// differently: nothing issued is a configuration problem, while issued-but-
// unresolved points at the oracle or a window that closed too early. The
// zero-issued text is built from the tailroom constants so the operator-facing
// advice cannot drift from churnTailroom's actual arithmetic.
func membershipNotExercisedReason(total int) string {
	if total == 0 {
		return fmt.Sprintf(
			"membership churn was requested but no change was issued — the membership dimension was not exercised: "+
				"--steady must exceed the churn tailroom (max(%s, --settle + %s observation budget)), "+
				"or every add/remove was rejected by the server",
			verifyChurnTailroom, verifyChurnObservation)
	}
	return fmt.Sprintf(
		"membership churn issued %d changes and resolved none of them — the membership dimension was not "+
			"exercised: every change's outcome was unknown (its oracle query failed, or it was still "+
			"inside its settle window when the steady window ended), so none was checked either way",
		total)
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
// Unresolved membership changes are the one signal here with a budget rather
// than a latch (maxToleratedOracleErrs): each one only blinds a single change,
// which is already excluded from Applied/Effective, so a few cost sensitivity
// rather than trust. The counts are reported either way (VerifyReport).
//
// A membership *epoch* change (Changes.Adds/Removes) is deliberately NOT
// considered here: churn legitimately changes the expected recipient set and is
// not, on its own, evidence that measurement was untrustworthy. A run that
// resolved no change at all is, when churn was requested — that is the floor,
// not the churn.
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
	if in.SlowConsumers > 0 {
		reasons = append(reasons, fmt.Sprintf(
			"%d slow consumer error on recipient connections — nats.go dropped deliveries harness-side, "+
				"so a missing delivery cannot be attributed to the system",
			in.SlowConsumers))
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
	// One budget, two spenders: a failed oracle query and a change dropped
	// before its settle window elapsed both leave that change's outcome unknown.
	// The reason breaks them back down because the remedies differ — retry the
	// run versus widen --steady.
	unresolved := in.OracleErrs + in.ChangesUnobserved
	if tol := maxToleratedOracleErrs(in.Changes.Total); unresolved > tol {
		reason := fmt.Sprintf(
			"%d of %d membership changes unresolved (tolerance %d): %d oracle queries failed, %d never observed",
			unresolved, in.Changes.Total, tol, in.OracleErrs, in.ChangesUnobserved)
		if in.OracleErrSample != nil {
			reason = fmt.Sprintf("%s: %v", reason, in.OracleErrSample)
		}
		reasons = append(reasons, reason)
	}
	// The membership analogue of the --min-probes floor, and a stronger statement
	// than the tolerance rule above rather than a special case of it. The
	// tolerance asks "were most changes resolved?"; the floor asks "was ANY
	// change resolved?" — because a change whose outcome nobody learned teaches
	// exactly as much as a change never issued. Asking only whether a change was
	// *issued* left a hole at Total=1: the tolerance is max(1, …), so one
	// unresolved change sits inside the budget and the run reports PASS having
	// resolved nothing at all.
	//
	// Clamped rather than trusted: Total comes from MembershipModel and the
	// unresolved counters from verifyRun, so a future divergence must not let a
	// negative difference read as "resolved something" and switch the floor off.
	resolved := max(0, in.Changes.Total-unresolved)
	if in.ChurnRequested && resolved == 0 {
		reasons = append(reasons, membershipNotExercisedReason(in.Changes.Total))
	}
	if in.Counts.Tracked < in.MinProbes {
		reasons = append(reasons, fmt.Sprintf(
			"only %d probes tracked, below --min-probes=%d (%d suppressed by membership churn)",
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
