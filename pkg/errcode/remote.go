package errcode

import "fmt"

// FromReply returns the error a remote reply carries, or nil when the payload
// is not an error envelope. It is the only correct way to read a reply from
// another service: Parse and Code.Valid answer different questions, and every
// call site that combined them by hand got one of the two wrong.
//
//	if err := errcode.FromReply(msg.Data); err != nil {
//		return nil, err
//	}
//
// Parse reports whether the payload *is* an error envelope — any envelope, with
// any code. Code.Valid reports whether this build's closed set recognises the
// code. Neither answers "did the call fail", and combining them wrong fails in
// opposite directions:
//
//   - `ok && code.Valid()` treats an unrecognised code as a success. The reply
//     falls through to a decoder, JSON's ignore-unknown-fields default yields a
//     zero-value response, the caller returns nil, and the call records a
//     success sample. A newer peer, or a cross-site one, silently turns remote
//     failures into empty results.
//   - `ok` alone relays an unrecognised code onward as a typed *Error, into
//     writers and constructors that assume the closed set. errcode.New panics
//     on a non-canonical Code.
//
// So: an envelope is always a failure, and an unrecognised code is a failure
// that must not be relayed typed. Callers that need to branch on a specific
// code use errors.As, which finds nothing for an unrecognised one — which is
// the point.
func FromReply(data []byte) error {
	e, ok := Parse(data)
	if !ok {
		return nil
	}
	if !e.Code.Valid() {
		return fmt.Errorf("remote returned unknown error code %q: %s", e.Code, e.Message)
	}
	return e
}
