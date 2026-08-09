package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func reportForTest() VerifyReport {
	return VerifyReport{
		ProbeRooms:     9,
		ProbeMembers:   171,
		DirectPoolSize: 191,
		ReserveSize:    20,
		BackgroundSize: 7383,
		Counts:         ProbeCounts{Tracked: 412, Suppressed: 18, Complete: 410, Partial: 2},
		Changes:        ChangeCounts{Total: 24, Adds: 14, Removes: 10, Applied: 24, Effective: 24},
		Result: VerifyResult{
			Verdict: VerdictFail,
			Violations: []Violation{
				{Kind: KindMissingRecipient, MsgID: "m1", RoomID: "r1", Users: []string{"u-1", "u-2"}},
			},
		},
	}
}

func TestRenderVerifyConsole_ShowsVerdict(t *testing.T) {
	out := renderVerifyConsole(reportForTest())
	assert.Contains(t, out, "VERDICT: FAIL")
}

func TestRenderVerifyConsole_ShowsCoverage(t *testing.T) {
	out := renderVerifyConsole(reportForTest())
	assert.Contains(t, out, "probe rooms:")
	assert.Contains(t, out, "412 tracked")
	assert.Contains(t, out, "18 suppressed")
}

func TestRenderVerifyConsole_ShowsViolationDetail(t *testing.T) {
	out := renderVerifyConsole(reportForTest())
	assert.Contains(t, out, "missing_recipient")
	assert.Contains(t, out, "m1")
	assert.Contains(t, out, "u-1")
}

func TestRenderVerifyConsole_ShowsDetailLine(t *testing.T) {
	rep := reportForTest()
	rep.Result.Violations = []Violation{
		{
			Kind: KindMissingRecipient, MsgID: "m1", RoomID: "r1", Users: []string{"u-1"},
			Detail: "reached 3 of 5 expected recipients",
		},
	}

	out := renderVerifyConsole(rep)
	assert.Contains(t, out, "reached 3 of 5 expected recipients")
	assert.Contains(t, out, "\n      reached 3 of 5 expected recipients")
}

func TestRenderVerifyConsole_CapsViolationsAtTen(t *testing.T) {
	rep := reportForTest()
	rep.Result.Violations = nil
	for i := 0; i < 25; i++ {
		rep.Result.Violations = append(rep.Result.Violations, Violation{
			Kind: KindMissingRecipient, MsgID: fmtUserID(i), RoomID: "r1",
		})
	}

	out := renderVerifyConsole(rep)
	assert.Contains(t, out, "showing 10 of 25")
	assert.Equal(t, 10, strings.Count(out, "missing_recipient"))
}

func TestRenderVerifyConsole_ShowsInconclusiveReasons(t *testing.T) {
	rep := reportForTest()
	rep.Result = VerifyResult{Verdict: VerdictInconclusive, Reasons: []string{"3 multiplex drop(s) recorded"}}

	out := renderVerifyConsole(rep)
	assert.Contains(t, out, "VERDICT: INCONCLUSIVE")
	assert.Contains(t, out, "multiplex drop")
}

func TestRenderVerifyConsole_PassHasNoViolationBlock(t *testing.T) {
	rep := reportForTest()
	rep.Result = VerifyResult{Verdict: VerdictPass}

	out := renderVerifyConsole(rep)
	assert.Contains(t, out, "VERDICT: PASS")
	assert.NotContains(t, out, "VIOLATIONS")
}

func TestRenderVerifyJSON_RoundTrips(t *testing.T) {
	raw, err := renderVerifyJSON(reportForTest())
	require.NoError(t, err)

	var back VerifyReport
	require.NoError(t, json.Unmarshal(raw, &back))
	assert.Equal(t, VerdictFail, back.Result.Verdict)
	require.Len(t, back.Result.Violations, 1)
	assert.Equal(t, []string{"u-1", "u-2"}, back.Result.Violations[0].Users)
}

func TestRenderVerifyJSON_CarriesAllViolations(t *testing.T) {
	rep := reportForTest()
	rep.Result.Violations = nil
	for i := 0; i < 25; i++ {
		rep.Result.Violations = append(rep.Result.Violations, Violation{
			Kind: KindMissingRecipient, MsgID: fmtUserID(i),
		})
	}

	raw, err := renderVerifyJSON(rep)
	require.NoError(t, err)

	var back VerifyReport
	require.NoError(t, json.Unmarshal(raw, &back))
	assert.Len(t, back.Result.Violations, 25, "JSON must not be capped like the console")
}
