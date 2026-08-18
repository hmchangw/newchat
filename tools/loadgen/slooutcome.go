package main

import (
	"net/http"

	"github.com/hmchangw/chat/pkg/errcode"
)

// sloOutcome is the error-budget eligibility class of one measured attempt,
// implementing the table in docs/load-testing/common/sli-slo.md §0.1. It is shared by
// every workload that scores an SLO so the rule has exactly one definition:
// two copies would let an HTTP journey and a NATS journey drift into scoring
// the same failure differently.
type sloOutcome int

const (
	// outcomeGood is a success: counted in both numerator and denominator.
	outcomeGood sloOutcome = iota
	// outcomeFailed is our fault — the request was legitimate and expected to
	// succeed. Stays in the denominator and burns budget.
	outcomeFailed
	// outcomeExcluded is a legitimate client-side outcome. Removed from valid
	// events entirely: never good, never a failure. §0.1 is explicit that these
	// leave the denominator rather than counting as successes.
	outcomeExcluded
)

func (o sloOutcome) String() string {
	switch o {
	case outcomeGood:
		return "good"
	case outcomeFailed:
		return "failed"
	default:
		return "excluded"
	}
}

// classifyHTTPStatus maps an HTTP response (or a transport failure) onto an
// eligibility class. transportErr takes precedence: a status is meaningless
// when the request never completed.
func classifyHTTPStatus(status int, transportErr bool) sloOutcome {
	switch {
	case transportErr:
		return outcomeFailed
	case status >= 200 && status < 300:
		return outcomeGood
	case status == http.StatusTooManyRequests:
		return outcomeFailed
	case status >= 400 && status < 500:
		return outcomeExcluded
	default:
		return outcomeFailed
	}
}

// classifyErrcode maps the Code from a NATS error envelope onto the same
// classes. Anything outside the excluded set — including an unrecognized or
// empty Code — burns budget, mirroring Code.HTTPStatus mapping unknowns to 500
// so a misclassification never reads as healthy.
func classifyErrcode(code errcode.Code) sloOutcome {
	switch code {
	case errcode.CodeBadRequest, errcode.CodeUnauthenticated, errcode.CodeForbidden,
		errcode.CodeNotFound, errcode.CodeConflict:
		return outcomeExcluded
	default:
		return outcomeFailed
	}
}
