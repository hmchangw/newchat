package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestEvaluateVerify_OracleError_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.OracleErr = errors.New("subscription.list timeout")

	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict)
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
