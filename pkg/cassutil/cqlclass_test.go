package cassutil

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

// fakeCQLError is a gocql.RequestError double. gocql v1.7.0 returns its
// unexported errorFrame value for the codes we care about most (Invalid, Syntax,
// Unauthorized, Config) — there is no exported concrete type to construct — so
// the classifier matches the exported RequestError interface and the tests drive
// it through a local implementation of that same interface.
type fakeCQLError struct {
	code int
	msg  string
}

func (e fakeCQLError) Code() int       { return e.code }
func (e fakeCQLError) Message() string { return e.msg }
func (e fakeCQLError) Error() string   { return e.msg }

func cqlErr(code int) error {
	return fakeCQLError{code: code, msg: fmt.Sprintf("cql error 0x%x", code)}
}

// storeErr stands in for the error type a caller wraps a driver failure in
// (message-worker's historyWriteError): classification must traverse it.
type storeErr struct{ err error }

func (e storeErr) Error() string { return e.err.Error() }
func (e storeErr) Unwrap() error { return e.err }

func TestClassifyCQL(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want CQLClass
		why  string
	}{
		// Request class: deterministic for this statement against our static
		// prepared statements, so retrying forever would loop a message that can
		// never be written.
		{name: "invalid is request class", err: cqlErr(gocql.ErrCodeInvalid), want: CQLRequest,
			why: "oversized mutation / null clustering key: this message will never be accepted"},
		{name: "syntax is request class", err: cqlErr(gocql.ErrCodeSyntax), want: CQLRequest,
			why: "our statements are static, so a syntax error is a code bug, not a transient"},

		// The two lines that matter most. Both look permanent per message and are
		// in fact site-wide ops faults: a rotated credential and a keyspace
		// missing at a new site. Dropping on either would destroy EVERY message
		// for the duration of the misconfiguration, which is strictly worse than
		// the silent loss of one outage's messages.
		{name: "unauthorized is infra class", err: cqlErr(gocql.ErrCodeUnauthorized), want: CQLInfra,
			why: "a rotated credential affects every message — dropping would destroy the whole feed"},
		{name: "config error is infra class", err: cqlErr(gocql.ErrCodeConfig), want: CQLInfra,
			why: "a keyspace missing at a new site affects every message — dropping would destroy the whole feed"},

		{name: "server error is infra class", err: cqlErr(gocql.ErrCodeServer), want: CQLInfra},
		{name: "unavailable is infra class", err: cqlErr(gocql.ErrCodeUnavailable), want: CQLInfra},
		{name: "overloaded is infra class", err: cqlErr(gocql.ErrCodeOverloaded), want: CQLInfra},
		{name: "bootstrapping is infra class", err: cqlErr(gocql.ErrCodeBootstrapping), want: CQLInfra},
		{name: "write timeout is infra class", err: cqlErr(gocql.ErrCodeWriteTimeout), want: CQLInfra},
		{name: "read timeout is infra class", err: cqlErr(gocql.ErrCodeReadTimeout), want: CQLInfra},
		{name: "write failure is infra class", err: cqlErr(gocql.ErrCodeWriteFailure), want: CQLInfra},
		{name: "read failure is infra class", err: cqlErr(gocql.ErrCodeReadFailure), want: CQLInfra},
		{name: "truncate is infra class", err: cqlErr(gocql.ErrCodeTruncate), want: CQLInfra},
		{name: "credentials is infra class", err: cqlErr(gocql.ErrCodeCredentials), want: CQLInfra,
			why: "another shape of the rotated-credential fault"},
		{name: "unprepared is infra class", err: cqlErr(gocql.ErrCodeUnprepared), want: CQLInfra,
			why: "the driver re-prepares and retries; transient by construction"},
		{name: "cas write unknown is infra class", err: cqlErr(gocql.ErrCodeCASWriteUnknown), want: CQLInfra},
		{name: "protocol error is infra class", err: cqlErr(gocql.ErrCodeProtocol), want: CQLInfra},
		{name: "already exists is infra class", err: cqlErr(gocql.ErrCodeAlreadyExists), want: CQLInfra},
		{name: "function failure is infra class", err: cqlErr(gocql.ErrCodeFunctionFailure), want: CQLInfra},

		// Typed driver errors that carry a real code, constructed the way gocql
		// itself does for these frames.
		{name: "typed unavailable is infra class", err: &gocql.RequestErrUnavailable{}, want: CQLInfra},
		{name: "typed write timeout is infra class", err: &gocql.RequestErrWriteTimeout{}, want: CQLInfra},

		// Driver-level sentinels carry no CQL code at all.
		{name: "no connections is infra class", err: gocql.ErrNoConnections, want: CQLInfra},
		{name: "connection closed is infra class", err: gocql.ErrConnectionClosed, want: CQLInfra},
		{name: "timeout with no response is infra class", err: gocql.ErrTimeoutNoResponse, want: CQLInfra},
		{name: "deadline exceeded is infra class", err: context.DeadlineExceeded, want: CQLInfra},
		{name: "canceled is infra class", err: context.Canceled, want: CQLInfra},

		// The safety default: anything unrecognised retries, never drops.
		{name: "unknown error is infra class", err: errors.New("boom"), want: CQLInfra},
		{name: "unknown cql code is infra class", err: cqlErr(0x9999), want: CQLInfra},
		{name: "nil is infra class", err: nil, want: CQLInfra},

		// A store wraps every gocql error with %w, and the caller sees it through
		// its own error type, so classification must traverse the whole chain.
		{name: "wrapped invalid classifies through errors.As",
			err:  fmt.Errorf("save message m1: %w", storeErr{fmt.Errorf("gocql: %w", cqlErr(gocql.ErrCodeInvalid))}),
			want: CQLRequest},
		{name: "wrapped unavailable classifies through errors.As",
			err:  fmt.Errorf("save message m1: %w", storeErr{cqlErr(gocql.ErrCodeUnavailable)}),
			want: CQLInfra},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ClassifyCQL(tt.err), tt.why)
		})
	}
}

func TestCQLClass_String(t *testing.T) {
	assert.Equal(t, "infra", CQLInfra.String())
	assert.Equal(t, "request", CQLRequest.String())
	assert.Equal(t, "infra", CQLClass(99).String(), "an unnamed class must not read as droppable")
}

func TestCQLCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "invalid", err: cqlErr(gocql.ErrCodeInvalid), want: "invalid"},
		{name: "syntax", err: cqlErr(gocql.ErrCodeSyntax), want: "syntax"},
		{name: "wrapped invalid", err: fmt.Errorf("save: %w", cqlErr(gocql.ErrCodeInvalid)), want: "invalid"},
		{name: "another cql code", err: cqlErr(gocql.ErrCodeUnavailable), want: "other"},
		{name: "no cql code", err: gocql.ErrNoConnections, want: "none"},
		{name: "nil", err: nil, want: "none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The label must stay inside this bounded set: it rides a metric
			// attribute, where the error text would be a cardinality bomb.
			got := CQLCode(tt.err)
			assert.Equal(t, tt.want, got)
			assert.Contains(t, []string{"invalid", "syntax", "other", "none"}, got)
		})
	}
}
