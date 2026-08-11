package main

import (
	"errors"
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
