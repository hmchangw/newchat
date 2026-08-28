package errcode

import "errors"

// ErrPermanent is the sentinel callers match via errors.Is to detect a
// non-retryable job failure. Wrap with Permanent to mark one.
var ErrPermanent = errors.New("permanent")

// PermanentError marks an *Error as non-retryable: JetStream consumers Ack
// (drop) rather than Nak. Permanence is INDEPENDENT of category — an Internal
// can be permanent; a retryable infra error stays unwrapped.
type PermanentError struct{ ec *Error }

// Permanent wraps an *Error as a non-retryable failure. Panics on nil — a
// caller with no classified error to wrap is a programmer bug.
func Permanent(ec *Error) *PermanentError {
	if ec == nil {
		panic("errcode.Permanent: nil *Error")
	}
	return &PermanentError{ec: ec}
}

// Error returns the wrapped *Error's message.
func (p *PermanentError) Error() string { return p.ec.Error() }

// Unwrap exposes the wrapped *Error (and, transitively, its WithCause cause).
func (p *PermanentError) Unwrap() error { return p.ec }

// Is matches the ErrPermanent sentinel so callers branch on permanence without
// importing the concrete type.
func (p *PermanentError) Is(target error) bool { return target == ErrPermanent }

// IsPermanent reports whether err's chain carries a *PermanentError, returning
// the wrapped *Error. Returns (nil, false) for any non-permanent error.
func IsPermanent(err error) (*Error, bool) {
	var p *PermanentError
	if errors.As(err, &p) {
		return p.ec, true
	}
	return nil, false
}

// Terminal reports whether err carries a typed *Error whose outcome cannot
// change on retry, returning that *Error. Use it in a JetStream worker to
// decide Ack-drop vs Nak on a REMOTE reply: a not_found/forbidden/bad_request
// from the service you called reads the same on every redelivery, so retrying
// it only holds an ack-pending slot until MaxDeliver drops the message anyway.
//
// The split is "a fact about THIS message" vs "a state of the world". Facts
// (not_found, forbidden, bad_request, conflict) are terminal. States are not:
// Unavailable and Internal because the remote may recover (history-service
// collapses a Cassandra read failure to internal), TooManyRequests because
// "retry shortly" must never mean "drop" — that is what jsretry's
// BackpressureBackoff exists for — and Unauthenticated because a credential
// problem hits every message at once, so dropping them is mass data loss rather
// than poison rejection. A non-errcode error is an infra failure (timeout,
// unmarshal) and is likewise transient.
//
// Terminal classifies; it does not wrap. Pair it with Permanent at the call
// site so the worker keeps its own message and terminal metric:
//
//	if ee, terminal := errcode.Terminal(err); terminal {
//	    return errcode.Permanent(ee)
//	}
func Terminal(err error) (*Error, bool) {
	var ee *Error
	if !errors.As(err, &ee) {
		return nil, false
	}
	switch ee.Code {
	case CodeUnavailable, CodeInternal, CodeTooManyRequests, CodeUnauthenticated:
		return nil, false
	default:
		return ee, true
	}
}
