package natsmetrics

import "strings"

// EventTypeFromSubject derives the bounded event_type from a NATS subject's
// last token.
//
// Every consumer of a stream must classify the same way: message-worker,
// broadcast-worker, and notification-worker all read MESSAGES-CANONICAL, and a
// campaign that cannot join their series on event_type cannot show a message
// moving from accepted to persisted to delivered. The subject already carries
// the verb (chat.msg.canonical.{site}.created, ….msg.send), so this needs no
// payload parse — which also keeps classification off the dispatch path.
func EventTypeFromSubject(subj string) EventType {
	tail := subj
	if idx := strings.LastIndexByte(subj, '.'); idx >= 0 {
		tail = subj[idx+1:]
	}
	// The Teams migration lane spells its verb "batch"; every other subject
	// tail already matches the shared event vocabulary.
	if tail == "batch" {
		return EventTeamsBatch
	}
	return NormalizeEventType(tail)
}
