package main

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/errcode"
)

func TestClassifyHTTPStatus(t *testing.T) {
	tests := []struct {
		name   string
		status int
		err    bool
		want   sloOutcome
	}{
		{"ok", http.StatusOK, false, outcomeGood},
		{"created", http.StatusCreated, false, outcomeGood},
		{"bad request excluded", http.StatusBadRequest, false, outcomeExcluded},
		{"unauthorized excluded", http.StatusUnauthorized, false, outcomeExcluded},
		{"forbidden excluded", http.StatusForbidden, false, outcomeExcluded},
		{"not found excluded", http.StatusNotFound, false, outcomeExcluded},
		{"conflict excluded", http.StatusConflict, false, outcomeExcluded},
		{"429 burns budget", http.StatusTooManyRequests, false, outcomeFailed},
		{"500 burns budget", http.StatusInternalServerError, false, outcomeFailed},
		{"503 burns budget", http.StatusServiceUnavailable, false, outcomeFailed},
		{"504 burns budget", http.StatusGatewayTimeout, false, outcomeFailed},
		{"transport error burns budget", 0, true, outcomeFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, classifyHTTPStatus(tc.status, tc.err))
		})
	}
}

// The wire-side twin of the HTTP table: the errcode Codes map onto the same
// three classes, so both transports score a journey the same way.
func TestClassifyErrcode(t *testing.T) {
	tests := []struct {
		name string
		code errcode.Code
		want sloOutcome
	}{
		{"bad request excluded", errcode.CodeBadRequest, outcomeExcluded},
		{"unauthenticated excluded", errcode.CodeUnauthenticated, outcomeExcluded},
		{"forbidden excluded", errcode.CodeForbidden, outcomeExcluded},
		{"not found excluded", errcode.CodeNotFound, outcomeExcluded},
		{"conflict excluded", errcode.CodeConflict, outcomeExcluded},
		{"too many requests burns budget", errcode.CodeTooManyRequests, outcomeFailed},
		{"unavailable burns budget", errcode.CodeUnavailable, outcomeFailed},
		{"internal burns budget", errcode.CodeInternal, outcomeFailed},
		// An unrecognized code must never be optimistically excluded — the same
		// reasoning as Code.HTTPStatus mapping unknowns to 500.
		{"unknown code burns budget", errcode.Code("something_new"), outcomeFailed},
		{"empty code burns budget", errcode.Code(""), outcomeFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, classifyErrcode(tc.code))
		})
	}
}

// Both classifiers agree on every code that has an HTTP twin. If they ever
// drift, one transport would score a journey differently from the other.
func TestClassifiers_AgreeAcrossTransports(t *testing.T) {
	for _, code := range []errcode.Code{
		errcode.CodeBadRequest, errcode.CodeUnauthenticated, errcode.CodeForbidden,
		errcode.CodeNotFound, errcode.CodeConflict, errcode.CodeTooManyRequests,
		errcode.CodeUnavailable, errcode.CodeInternal,
	} {
		t.Run(string(code), func(t *testing.T) {
			assert.Equal(t,
				classifyHTTPStatus(code.HTTPStatus(), false),
				classifyErrcode(code),
				"HTTP and errcode classification must agree for %s", code)
		})
	}
}
