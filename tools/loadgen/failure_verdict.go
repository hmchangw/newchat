package main

import (
	"slices"
)

type failureVerdict string

const (
	failureVerdictPass         failureVerdict = "PASS"
	failureVerdictFail         failureVerdict = "FAIL"
	failureVerdictInconclusive failureVerdict = "INCONCLUSIVE"
)

type failureGate struct {
	ID        string         `json:"id"`
	Verdict   failureVerdict `json:"verdict"`
	Reason    string         `json:"reason,omitempty"`
	Input     string         `json:"input,omitempty"`
	Threshold string         `json:"threshold,omitempty"`
	Observed  string         `json:"observed,omitempty"`
}

type failureVerdictResult struct {
	Verdict failureVerdict `json:"verdict"`
	Reasons []string       `json:"reasons"`
	Gates   []failureGate  `json:"gates"`
}

func evaluateFailureVerdict(gates []failureGate) failureVerdictResult {
	result := failureVerdictResult{Verdict: failureVerdictPass, Gates: append([]failureGate(nil), gates...)}
	for _, gate := range gates {
		if gate.Reason != "" {
			result.Reasons = append(result.Reasons, gate.Reason)
		}
		if gate.Verdict == failureVerdictInconclusive {
			result.Verdict = failureVerdictInconclusive
		} else if gate.Verdict == failureVerdictFail && result.Verdict == failureVerdictPass {
			result.Verdict = failureVerdictFail
		}
	}
	slices.Sort(result.Reasons)
	slices.SortFunc(result.Gates, func(a, b failureGate) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	return result
}
