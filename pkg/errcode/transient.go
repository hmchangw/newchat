package errcode

import "errors"

// IsTransient reports whether err is a retryable infrastructure failure rather
// than a permanent domain answer. A typed *Error is transient only for the
// unavailable and internal categories — history-service collapses a Cassandra
// read failure to internal, so internal must stay retryable. Every other
// category (not_found, forbidden, bad_request, …) is a settled answer that will
// not change on retry. A non-errcode error is an unclassified infra failure
// (unmarshal, transport) and counts as transient.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var ee *Error
	if errors.As(err, &ee) {
		return ee.Code == CodeUnavailable || ee.Code == CodeInternal
	}
	return true
}
