package errcode

import (
	"encoding/json"
	"errors"
	"fmt"
)

// FromReply returns the error a remote reply carries, or nil when the payload
// is not an error envelope. It is the only correct way to read a reply from
// another service: Parse and Code.Valid answer different questions, and every
// call site that combined them by hand got one of the two wrong.
//
//	if err := errcode.FromReply(msg.Data); err != nil {
//		return nil, err
//	}
//
// Parse reports whether the payload decodes *into this build's Error* — which
// is not the same as whether it is an envelope. Code.Valid reports whether this
// build's closed set recognises the code. Neither answers "did the call fail",
// and combining them wrong fails in three directions:
//
//   - `ok && code.Valid()` treats an unrecognised code as a success. The reply
//     falls through to a decoder, JSON's ignore-unknown-fields default yields a
//     zero-value response, the caller returns nil, and the call records a
//     success sample. A newer peer, or a cross-site one, silently turns remote
//     failures into empty results.
//   - `ok` alone relays an unrecognised code onward as a typed *Error, into
//     writers and constructors that assume the closed set. errcode.New panics
//     on a non-canonical Code.
//   - `ok` alone *also* misses the envelope entirely when any field fails to
//     decode — Metadata is map[string]string, so a single numeric value sinks
//     the whole unmarshal — landing back in the first failure by another route.
//
// So the envelope is recognised by the presence of its discriminator — the
// "error" key — before anything is decoded, and that answer never depends on
// whether the rest of the payload fits this build's struct. An envelope is
// always a failure; a code this build does not know, and a payload it cannot
// fully decode, are both failures that must not be relayed typed. Callers that
// need to branch on a specific code use errors.As, which finds nothing for
// either — which is the point.
//
// Having no message to relay is not a reason to report success, so the
// discriminator is the key, not the message. The one line that needs drawing is
// which values mean "no error": null and "" both do — {"data":…,"error":null}
// is a common success shape — while an array, object, number or bool spells no
// such thing and is a foreign or corrupt envelope. Only a payload that is not
// JSON at all falls through untouched, for the success decoder to report.
func FromReply(data []byte) error {
	// Deliberately a RawMessage: it separates "no error key" from "an error key
	// this build cannot read", which a string field collapses into one.
	var envelope struct {
		Message json.RawMessage `json:"error"`
	}
	//nolint:nilerr // a non-JSON payload is not this build's to interpret; the caller's decoder reports it
	if err := json.Unmarshal(data, &envelope); err != nil || len(envelope.Message) == 0 {
		return nil
	}

	var message string
	if err := json.Unmarshal(envelope.Message, &message); err != nil {
		return errors.New("remote returned a malformed error envelope")
	}
	if message == "" { // null decodes to "" too, and both mean "no error"
		return nil
	}

	e, ok := Parse(data)
	if !ok {
		return fmt.Errorf("remote returned an error this build cannot decode: %s", message)
	}
	if !e.Code.Valid() {
		return fmt.Errorf("remote returned unknown error code %q: %s", e.Code, e.Message)
	}
	return e
}
