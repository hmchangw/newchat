package cassutil

import (
	"errors"

	"github.com/gocql/gocql"
)

// CQLClass distinguishes Cassandra failures that will never succeed for the
// statement that produced them from those that mean the cluster cannot serve the
// write right now. It is what a worker's give-up decision turns on: infra-class
// failures retry forever, request-class failures retry to a deadline and are
// then dropped.
type CQLClass int

const (
	CQLInfra   CQLClass = iota // retry forever: the cluster cannot serve the write
	CQLRequest                 // message-specific: retried to a deadline, then dropped
)

// String is the bounded metric label for the class.
// Anything other than CQLRequest reports "infra", so a class value that has not
// been taught to this method can never read as droppable.
func (c CQLClass) String() string {
	if c == CQLRequest {
		return "request"
	}
	return "infra"
}

// ClassifyCQL decides whether err is specific to the statement that produced it.
//
// Only two CQL codes qualify. Both are deterministic for a given statement
// against the static prepared statements our services issue, so no amount of
// retrying changes the outcome:
//
//   - Invalid (0x2200) — oversized mutation, null clustering/partition key,
//     invalid collection contents.
//   - Syntax (0x2000) — our statements are static, so this is a code bug; a
//     deadline-then-drop beats an infinite loop.
//
// Everything else is infra class, and two codes are infra class *specifically*
// even though they look permanent per statement: Unauthorized (0x2100) is a
// rotated credential and ConfigError (0x2300) is a keyspace missing at a new
// site. Both fail EVERY write, so dropping on them would destroy the site's
// whole feed for the duration of a misconfiguration — strictly worse than
// silently losing the messages of one outage. Unprepared (0x2500) is transient
// because the driver re-prepares, and the driver-level sentinels
// (ErrNoConnections, ErrConnectionClosed, ErrTimeoutNoResponse) plus context
// deadlines carry no CQL code at all and fall through the default.
//
// Anything unrecognised — including a nil error and a code this switch has never
// seen — is infra class. Unknown errors retry; they never drop.
func ClassifyCQL(err error) CQLClass {
	var reqErr gocql.RequestError
	if !errors.As(err, &reqErr) {
		return CQLInfra
	}
	switch reqErr.Code() {
	case gocql.ErrCodeInvalid, gocql.ErrCodeSyntax:
		return CQLRequest
	default:
		return CQLInfra
	}
}

// CQLCode names err's CQL error code for a drop metric or a drop log line.
// The range is deliberately tiny — the two droppable codes, "other" for any
// other coded request error, "none" when the error carries no CQL code — because
// it rides a metric attribute, where the error text would be a cardinality bomb.
func CQLCode(err error) string {
	var reqErr gocql.RequestError
	if !errors.As(err, &reqErr) {
		return "none"
	}
	switch reqErr.Code() {
	case gocql.ErrCodeInvalid:
		return "invalid"
	case gocql.ErrCodeSyntax:
		return "syntax"
	default:
		return "other"
	}
}
