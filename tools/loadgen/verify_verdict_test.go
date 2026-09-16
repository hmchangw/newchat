package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func passingInputs() VerifyInputs {
	return VerifyInputs{
		Counts:     ProbeCounts{Tracked: 100, Complete: 100},
		MinProbes:  50,
		GCPauseP99: 5,
		GCPauseMax: 50,
	}
}

func TestEvaluateVerify_Clean_IsPass(t *testing.T) {
	r := evaluateVerify(passingInputs())
	assert.Equal(t, VerdictPass, r.Verdict)
	assert.Empty(t, r.Reasons)
	assert.Equal(t, 0, r.Verdict.ExitCode())
}

func TestEvaluateVerify_AnyViolation_IsFail(t *testing.T) {
	in := passingInputs()
	in.Violations = []Violation{{Kind: KindMissingRecipient, MsgID: "m1"}}

	r := evaluateVerify(in)
	assert.Equal(t, VerdictFail, r.Verdict)
	assert.Equal(t, 1, r.Verdict.ExitCode())
}

func TestEvaluateVerify_MembershipViolation_IsFail(t *testing.T) {
	in := passingInputs()
	in.Violations = []Violation{{Kind: KindMembershipRemoveIneffective, RoomID: "r1"}}

	assert.Equal(t, VerdictFail, evaluateVerify(in).Verdict)
}

// TestEvaluateVerify_MultiplexDrops_DoNotGateVerdict pins the fix for the
// unreachable-PASS bug. The multiplex pool's per-user inbox channels are
// write-only by design — nothing ever receives from them, so filling and
// dropping is their normal steady state under background load. Preflight
// refuses to start unless every probe-room member is in the *direct* pool, so a
// multiplex user is never an expected probe recipient and a multiplex drop
// cannot touch probe accounting. Gating on it made PASS unreachable in the
// default configuration (~7000 multiplex users under `daily-heavy`).
func TestEvaluateVerify_MultiplexDrops_DoNotGateVerdict(t *testing.T) {
	in := passingInputs()
	in.MultiplexDrops = 4096

	r := evaluateVerify(in)
	assert.Equal(t, VerdictPass, r.Verdict,
		"probe recipients are guaranteed direct-pool by preflight, so a multiplex drop is irrelevant to probe accounting")
	assert.Empty(t, r.Reasons)
	assert.Equal(t, 0, r.Verdict.ExitCode())
}

func TestEvaluateVerify_MultiplexDropsWithViolations_IsFail(t *testing.T) {
	in := passingInputs()
	in.MultiplexDrops = 4096
	in.Violations = []Violation{{Kind: KindMissingRecipient, MsgID: "m1"}}

	assert.Equal(t, VerdictFail, evaluateVerify(in).Verdict,
		"a background-pool drop must not downgrade a real delivery violation to INCONCLUSIVE")
}

func TestEvaluateVerify_InconclusiveOverridesFail(t *testing.T) {
	in := passingInputs()
	in.Violations = []Violation{{Kind: KindMissingRecipient, MsgID: "m1"}}
	in.DroppedRecipients = 1

	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict,
		"a lost recipient connection means the missing delivery cannot be attributed to the system")
}

func TestEvaluateVerify_TooFewProbes_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.Counts = ProbeCounts{Tracked: 10, Complete: 10}

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	assert.Contains(t, r.Reasons[0], "min-probes")
}

func TestEvaluateVerify_TrackedEqualsMinProbes_IsPass(t *testing.T) {
	in := passingInputs()
	in.Counts = ProbeCounts{Tracked: 50, Complete: 50}
	in.MinProbes = 50

	assert.Equal(t, VerdictPass, evaluateVerify(in).Verdict,
		"tracked meeting the floor exactly satisfies --min-probes")
}

func TestEvaluateVerify_TrackedOneBelowMinProbes_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.Counts = ProbeCounts{Tracked: 49, Complete: 49}
	in.MinProbes = 50

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	assert.Contains(t, r.Reasons[0], "min-probes")
}

func TestEvaluateVerify_SuppressedProbesDoNotCountTowardFloor(t *testing.T) {
	in := passingInputs()
	in.Counts = ProbeCounts{Tracked: 10, Complete: 10, Suppressed: 500}

	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict,
		"suppressed probes must not disguise a starved run as covered")
}

func TestEvaluateVerify_ReadbackError_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.ReadbackErr = errors.New("history-service unreachable")

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	assert.Contains(t, r.Reasons[0], "readback")
}

// TestEvaluateVerify_OracleErrsBelowTolerance_IsPass pins the tolerance rule: a
// change whose oracle query failed is already excluded from Applied/Effective,
// so a handful of failures costs detection sensitivity on those changes, not
// trust in the whole run. The old all-or-nothing rule threw away every clean
// delivery, leakage, exactly-once and persistence result over one blip.
func TestEvaluateVerify_OracleErrsBelowTolerance_IsPass(t *testing.T) {
	in := passingInputs()
	in.Changes = ChangeCounts{Total: 22, Adds: 12, Removes: 10, Applied: 21, Effective: 21}
	in.OracleErrs = 1
	in.OracleErrSample = errors.New("subscription.list timeout")

	r := evaluateVerify(in)
	assert.Equal(t, VerdictPass, r.Verdict)
	assert.Empty(t, r.Reasons, "a tolerated oracle failure must not add a reason either")
}

func TestEvaluateVerify_OracleErrsAboveTolerance_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.Changes = ChangeCounts{Total: 22, Adds: 12, Removes: 10, Applied: 17, Effective: 17}
	in.OracleErrs = 5
	in.OracleErrSample = errors.New("subscription.list timeout")

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	require.Len(t, r.Reasons, 1)
	assert.Contains(t, r.Reasons[0], "5 of 22",
		"the reason must distinguish one blip from a service that was down all run")
	assert.Contains(t, r.Reasons[0], "tolerance 2")
	assert.Contains(t, r.Reasons[0], "subscription.list timeout")
}

// TestEvaluateVerify_OracleErrsAtTolerance_IsPass pins the boundary from both
// sides: exactly maxToleratedOracleErrs passes, one more does not.
func TestEvaluateVerify_OracleErrsAtTolerance_IsPass(t *testing.T) {
	in := passingInputs()
	in.Changes = ChangeCounts{Total: 22, Adds: 12, Removes: 10}
	in.OracleErrSample = errors.New("subscription.list timeout")

	in.OracleErrs = 2
	assert.Equal(t, VerdictPass, evaluateVerify(in).Verdict,
		"22/10 = 2 tolerated failures, so 2 is still inside the budget")

	in.OracleErrs = 3
	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict,
		"one past the budget must trip")
}

// TestEvaluateVerify_OneOracleErrIsAlwaysForgiven pins the max(1, …) floor: with
// few changes the 10% share rounds to zero, and a single blip would otherwise
// discard the run.
func TestEvaluateVerify_OneOracleErrIsAlwaysForgiven(t *testing.T) {
	in := passingInputs()
	in.Changes = ChangeCounts{Total: 3, Adds: 2, Removes: 1}
	in.OracleErrs = 1
	in.OracleErrSample = errors.New("subscription.list timeout")

	assert.Equal(t, VerdictPass, evaluateVerify(in).Verdict)

	in.OracleErrs = 2
	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict)
}

func TestEvaluateVerify_NoChurnNoOracleErrs_IsPass(t *testing.T) {
	in := passingInputs()
	in.Changes = ChangeCounts{}
	in.OracleErrs = 0

	r := evaluateVerify(in)
	assert.Equal(t, VerdictPass, r.Verdict, "a run without churn must be unaffected by the tolerance rule")
	assert.Empty(t, r.Reasons)
}

// TestEvaluateVerify_HarnessErr_IsInconclusive pins the split between a real
// oracle query failure (transient, tolerable) and a fatal churn abort (a
// loadgen-side failure that is never tolerable, and must not be reported under
// an "oracle" prefix that points the operator at the wrong service).
func TestEvaluateVerify_HarnessErr_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.Changes = ChangeCounts{Total: 22, Adds: 12, Removes: 10}
	in.HarnessErr = errors.New("membership churn aborted, added user is unobservable: subscribe u-1 to r-1")

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	require.Len(t, r.Reasons, 1)
	assert.Contains(t, r.Reasons[0], "harness")
	assert.NotContains(t, r.Reasons[0], "oracle",
		"a churn abort is a harness failure, not an oracle query failure")
	assert.Contains(t, r.Reasons[0], "unobservable")
}

func TestEvaluateVerify_HarnessErrWithToleratedOracleErrs_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.Changes = ChangeCounts{Total: 22, Adds: 12, Removes: 10}
	in.OracleErrs = 1
	in.OracleErrSample = errors.New("subscription.list timeout")
	in.HarnessErr = errors.New("subscribe u-1 to r-1: no such user")

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict,
		"a harness error has no tolerance budget regardless of the oracle count")
	require.Len(t, r.Reasons, 1)
}

// TestEvaluateVerify_ChurnRequestedButNothingIssued_IsInconclusive pins the
// membership floor — the analogue of --min-probes for the churn dimension.
// churnTailroom(settle) can exceed --steady (at --steady=30s --settle=20s it
// does), which makes issueUntil already past when driveChurn starts and leaves
// Changes.Total at zero. Without this branch the run reports PASS having never
// added or removed a single member: a check silently not performed, reported as
// clean, which is exactly the failure class this tool exists to catch.
func TestEvaluateVerify_ChurnRequestedButNothingIssued_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{}

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	require.Len(t, r.Reasons, 1)
	assert.Contains(t, r.Reasons[0], "membership dimension was not exercised")
	assert.Contains(t, r.Reasons[0], "--steady",
		"the reason must name the likely cause so the operator can fix the flags")
	assert.Contains(t, r.Reasons[0], "tailroom")
	assert.Contains(t, r.Reasons[0], "--settle")
	assert.Contains(t, r.Reasons[0], "rejected",
		"server-side rejection is the other way Total stays at zero")
}

// TestEvaluateVerify_MembershipFloor_FiresDespiteViolations pins that the floor
// is a trust signal like --min-probes, not a tiebreak: a run that never
// exercised membership cannot be reported as FAIL-on-delivery-only either,
// because one of its dimensions was never measured.
func TestEvaluateVerify_MembershipFloor_FiresDespiteViolations(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Violations = []Violation{{Kind: KindMissingRecipient, MsgID: "m1"}}

	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict)
}

// TestEvaluateVerify_ChurnNotRequestedNoChanges_IsPass pins the other side:
// with --member-churn=0 a zero change count is the expected outcome and must
// stay PASS-able.
func TestEvaluateVerify_ChurnNotRequestedNoChanges_IsPass(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = false
	in.Changes = ChangeCounts{}

	r := evaluateVerify(in)
	assert.Equal(t, VerdictPass, r.Verdict,
		"--member-churn=0 legitimately issues no changes")
	assert.Empty(t, r.Reasons)
}

func TestEvaluateVerify_ChurnRequestedAndResolved_NoFloorReason(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{Total: 1, Adds: 1, Applied: 1, Effective: 1}

	assert.Equal(t, VerdictPass, evaluateVerify(in).Verdict,
		"a single resolved change clears the floor — the floor asks whether the dimension produced any signal")
}

// TestEvaluateVerify_ChurnIssuedButNoneResolved_IsInconclusive pins the floor's
// real question: not "was a change issued?" but "was a change *resolved*?".
// Issuing a change whose outcome nobody learned teaches exactly as much as
// issuing none, so it must reach the same verdict.
func TestEvaluateVerify_ChurnIssuedButNoneResolved_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{Total: 5, Adds: 3, Removes: 2}
	in.ChangesUnobserved = 5

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	joined := strings.Join(r.Reasons, "\n")
	assert.Contains(t, joined, "membership dimension was not exercised")
	assert.Contains(t, joined, "resolved none",
		"the reason must point at the unknown outcomes, not at --steady: nothing here is a flag problem")
	assert.NotContains(t, joined, "--settle",
		"the tailroom advice belongs to the zero-issued case only")
}

// TestEvaluateVerify_SingleUnresolvedChange_IsInconclusive pins the exact hole
// the weaker floor left. Total=1 makes the tolerance max(1, 0) = 1, so a single
// unresolved change sits inside the budget and the tolerance rule stays silent —
// yet the run resolved nothing at all. Only the floor catches this.
func TestEvaluateVerify_SingleUnresolvedChange_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{Total: 1, Adds: 1}
	in.ChangesUnobserved = 1

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict,
		"one change, unresolved, is exactly as much membership signal as no change at all")
	require.Len(t, r.Reasons, 1,
		"the tolerance rule must stay silent here — 1 is inside max(1, 1/10), which is why the floor is needed")
	assert.Contains(t, r.Reasons[0], "resolved none")
}

func TestEvaluateVerify_SingleUnresolvedOracleQuery_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{Total: 1, Adds: 1}
	in.OracleErrs = 1
	in.OracleErrSample = errors.New("subscription.list timeout")

	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict,
		"the floor counts resolution, so it must not care which of the two spenders consumed it")
}

// TestEvaluateVerify_PartiallyResolvedChurn_ClearsFloor pins the boundary: one
// resolved change out of many is a thin signal, but it is a signal, and the
// tolerance rule is what judges thinness. The floor only asks for non-zero.
func TestEvaluateVerify_PartiallyResolvedChurn_ClearsFloor(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{Total: 22, Adds: 12, Removes: 10, Applied: 1, Effective: 1}
	in.ChangesUnobserved = 21

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	require.Len(t, r.Reasons, 1,
		"21 of 22 unresolved trips the tolerance; the floor stays silent because one change was resolved")
	assert.Contains(t, r.Reasons[0], "unresolved (tolerance 2)")
	assert.NotContains(t, r.Reasons[0], "not exercised")
}

// TestEvaluateVerify_MembershipFloor_ClampsDisagreeingCounters pins the guard on
// the subtraction. Total and the unresolved counters are produced by different
// code paths (MembershipModel vs verifyRun); if they ever disagree, an unsigned
// reading of a negative resolved count must not turn the floor off.
func TestEvaluateVerify_MembershipFloor_ClampsDisagreeingCounters(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{Total: 2, Adds: 2}
	in.ChangesUnobserved = 3

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	assert.Contains(t, strings.Join(r.Reasons, "\n"), "not exercised",
		"more unresolved than issued still means nothing was resolved")
}

// TestEvaluateVerify_UnobservedChangesBelowTolerance_IsPass pins that an
// unharvested change is folded into the same budget as a failed oracle query:
// both mean "this change's outcome is unknown", and a couple of unknowns cost
// sensitivity rather than trust.
func TestEvaluateVerify_UnobservedChangesBelowTolerance_IsPass(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{Total: 22, Adds: 12, Removes: 10, Applied: 21, Effective: 21}
	in.ChangesUnobserved = 1

	r := evaluateVerify(in)
	assert.Equal(t, VerdictPass, r.Verdict)
	assert.Empty(t, r.Reasons)
}

// TestEvaluateVerify_UnobservedChangesAtTolerance_IsPass pins the boundary from
// both sides with no oracle failures in play.
func TestEvaluateVerify_UnobservedChangesAtTolerance_IsPass(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{Total: 22, Adds: 12, Removes: 10}

	in.ChangesUnobserved = 2
	assert.Equal(t, VerdictPass, evaluateVerify(in).Verdict,
		"22/10 = 2 unresolved changes tolerated, so 2 is still inside the budget")

	in.ChangesUnobserved = 3
	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict,
		"one past the budget must trip")
}

// TestEvaluateVerify_UnobservedChangesAboveTolerance_IsInconclusive pins the
// truncated-run case: driveChurn dropped still-pending changes on ctx.Done, so
// Applied and Effective read short of Total with no violation — visually
// identical to a real membership_not_applied. A silent PASS there is the tool
// lying about a check it never performed.
func TestEvaluateVerify_UnobservedChangesAboveTolerance_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{Total: 22, Adds: 12, Removes: 10, Applied: 18, Effective: 18}
	in.ChangesUnobserved = 4

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	require.Len(t, r.Reasons, 1)
	assert.Contains(t, r.Reasons[0], "4 of 22")
	assert.Contains(t, r.Reasons[0], "tolerance 2")
	assert.Contains(t, r.Reasons[0], "4 never observed")
	assert.NotContains(t, r.Reasons[0], ": <nil>",
		"the sample clause must be dropped when no oracle query ever failed")
}

// TestEvaluateVerify_OracleErrsPlusUnobserved_ExceedTolerance is the point of
// folding the two counts: neither 3 oracle failures nor 4 unharvested changes
// trips the budget alone at Total=100, but together they mean 7 of 100 changes
// have no known outcome, and the run is no more trustworthy for the split.
func TestEvaluateVerify_OracleErrsPlusUnobserved_ExceedTolerance(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{Total: 22, Adds: 12, Removes: 10, Applied: 15, Effective: 15}
	in.OracleErrs = 1
	in.OracleErrSample = errors.New("subscription.list timeout")
	in.ChangesUnobserved = 1

	assert.Equal(t, VerdictPass, evaluateVerify(in).Verdict,
		"1 + 1 is exactly the 22/10 budget")

	in.ChangesUnobserved = 2
	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict,
		"neither count alone exceeds the budget; their sum does, and the sum is what is unknown")
	require.Len(t, r.Reasons, 1)
	assert.Contains(t, r.Reasons[0], "3 of 22")
}

// TestEvaluateVerify_UnresolvedReason_BreaksDownBothCounts pins that the
// operator can tell a flaky oracle apart from a truncated run without opening
// the JSON artifact — the remedies are different (retry vs widen --steady).
func TestEvaluateVerify_UnresolvedReason_BreaksDownBothCounts(t *testing.T) {
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = ChangeCounts{Total: 22, Adds: 12, Removes: 10, Applied: 15, Effective: 15}
	in.OracleErrs = 3
	in.OracleErrSample = errors.New("subscription.list timeout")
	in.ChangesUnobserved = 4

	r := evaluateVerify(in)
	require.Len(t, r.Reasons, 1)
	assert.Equal(t,
		"7 of 22 membership changes unresolved (tolerance 2): "+
			"3 oracle queries failed, 4 never observed: subscription.list timeout",
		r.Reasons[0])
}

func TestMaxToleratedOracleErrs(t *testing.T) {
	tests := []struct {
		name    string
		changes int
		want    int
	}{
		{name: "no changes still forgives one", changes: 0, want: 1},
		{name: "below ten forgives one", changes: 9, want: 1},
		{name: "ten forgives one", changes: 10, want: 1},
		{name: "twenty-two forgives two", changes: 22, want: 2},
		{name: "hundred forgives ten", changes: 100, want: 10},
		{name: "negative is clamped", changes: -5, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, maxToleratedOracleErrs(tt.changes))
		})
	}
}

func TestEvaluateVerify_RecipientDropped_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.DroppedRecipients = 1

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	assert.Contains(t, r.Reasons[0], "connection dropped")
}

func TestEvaluateVerify_EpochChangeAloneIsNotInconclusive(t *testing.T) {
	in := passingInputs()
	in.Changes = ChangeCounts{Total: 12, Adds: 7, Removes: 5, Applied: 12, Effective: 12}

	assert.Equal(t, VerdictPass, evaluateVerify(in).Verdict,
		"membership churn legitimately alters the expected set")
}

func TestEvaluateVerify_ContextCancelled_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.Cancelled = true

	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict)
}

func TestEvaluateVerify_GCPressure_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.GCPauseP99 = 120

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	assert.Contains(t, r.Reasons[0], "GC")
}

func TestEvaluateVerify_AllReasonsCollected(t *testing.T) {
	in := passingInputs()
	in.DroppedRecipients = 2
	in.Cancelled = true
	in.GCPauseP99 = 120

	r := evaluateVerify(in)
	assert.Len(t, r.Reasons, 3, "every failing signal must be reported, not just the first")
}
