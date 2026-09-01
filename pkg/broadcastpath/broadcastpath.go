// Package broadcastpath classifies a message by the fan-out route it will take,
// so the metric that counts messages at the gatekeeper and the metric that counts
// enqueues at broadcast-worker are counted under one definition.
//
// The two are the denominator and the numerator of SLO-1b. They are emitted by
// different services, one upstream of the other, and a ratio whose halves
// disagree about which messages belong in it is worse than no ratio at all — it
// reads green while the disagreement, not the system, moves it. Keeping the rule
// here rather than as a copy in each service is what stops that drifting, for the
// same reason model.IsHiddenThreadReply lives in pkg/model rather than in the
// three services that branch on it.
//
// The label is the fan-out route, not the room type. A channel thread reply is a
// channel room that routes to per-account thread fan-out and never reaches
// publishChannelEvent, so a room-type denominator would count a message whose
// numerator can never fire.
package broadcastpath

import "github.com/hmchangw/chat/pkg/model"

// Path is the closed set of fan-out routes. It is a metric label value, so the
// set is closed by construction: an unrecognised input yields Unknown rather
// than a new series.
type Path string

const (
	// RoomSubject is one publish to the room's event subject, which NATS fans
	// out to the room's subscribers.
	RoomSubject Path = "room_subject"
	// Thread is per-account fan-out to a hidden thread reply's subscribers. It
	// bypasses the room subject entirely.
	Thread Path = "thread"
	// DM is per-account fan-out for a DM or bot-DM room.
	DM Path = "dm"
	// Unknown is the fail-open value, and two very different things produce it:
	// the room-meta lookup failed, or the room carries a type nothing here
	// recognises. The second is not merely unmeasured — broadcast-worker's
	// dispatch has no branch for it and drops the message — so Unknown is a
	// validity signal rather than a bucket, and the log line beside it is what
	// separates the two. See docs/specs/o11y/nats-metrics-contract.md §13.3.
	Unknown Path = "unknown"
)

// All enumerates every Path, so attribute sets can be precomputed over the
// closed space and a typo is a compile error rather than a stray series.
var All = []Path{RoomSubject, Thread, DM, Unknown}

// Valid reports whether p is one of the enumerated paths.
func (p Path) Valid() bool {
	switch p {
	case RoomSubject, Thread, DM, Unknown:
		return true
	default:
		return false
	}
}

// Classify returns the route a message takes, mirroring broadcast-worker's
// dispatch in handleCreated.
//
// tShow must be the *normalized* value — req.TShow && threadParentMessageID !=
// "" — because that is what lands on the canonical message the worker later
// classifies. Passing the raw request field misclassifies a tShow=true send that
// carries no thread parent.
//
// roomType may be empty or unrecognised, which is how a caller reports a failed
// room-meta lookup: the result is Unknown, never an error. A metric must not be
// able to fail a message.
func Classify(threadParentMessageID string, tShow bool, roomType model.RoomType) Path {
	// The thread test comes first and the room type gets no vote: a hidden
	// thread reply in a channel room routes to thread fan-out.
	if model.IsHiddenThreadReply(threadParentMessageID, tShow) {
		return Thread
	}
	switch roomType {
	case model.RoomTypeChannel:
		return RoomSubject
	case model.RoomTypeDM, model.RoomTypeBotDM:
		return DM
	default:
		return Unknown
	}
}
