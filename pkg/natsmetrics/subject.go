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

// RoomEventTypeFromSubject maps ROOMS and ROOMS-TEAMS subjects onto a closed
// room/member vocabulary. Site IDs and room IDs never become metric labels.
func RoomEventTypeFromSubject(subj string) EventType {
	if strings.HasPrefix(subj, "chat.teams.room.canonical.") {
		if strings.HasSuffix(subj, ".create") {
			return EventRoomCreate
		}
		return EventUnknown
	}
	if !strings.HasPrefix(subj, "chat.room.canonical.") {
		return EventUnknown
	}
	switch {
	case strings.HasSuffix(subj, ".event.member.muted"):
		return EventMemberMuted
	case strings.HasSuffix(subj, ".member.add"):
		return EventMemberAdd
	case strings.HasSuffix(subj, ".member.remove"):
		return EventMemberRemove
	case strings.HasSuffix(subj, ".room.rename"):
		return EventRoomRename
	case strings.HasSuffix(subj, ".create"):
		return EventRoomCreate
	default:
		return EventUnknown
	}
}

// PublishLabelsFromSubject maps a publish destination to closed labels. It is
// intentionally conservative: a new subject family remains unknown until it
// receives an explicit bounded mapping.
func PublishLabelsFromSubject(subj string) (DestinationKind, PublishOperation) {
	switch {
	case strings.HasPrefix(subj, "chat.msg.canonical."):
		return DestinationCanonical, OperationCanonicalPublish
	case strings.HasPrefix(subj, "chat.room.canonical."):
		operation := OperationRoomPublish
		if isMemberEventAt(strings.Split(subj, "."), 4) {
			operation = OperationMemberPublish
		}
		return DestinationRoomCanonical, operation
	case strings.HasPrefix(subj, "chat.teams.room.canonical."):
		operation := OperationRoomPublish
		if isMemberEventAt(strings.Split(subj, "."), 5) {
			operation = OperationMemberPublish
		}
		return DestinationRoomCanonical, operation
	case strings.HasPrefix(subj, "chat.outbox."):
		return DestinationOutbox, OperationOutboxPublish
	case strings.HasPrefix(subj, "chat.inbox."):
		tokens := strings.Split(subj, ".")
		if len(tokens) > 4 && strings.HasPrefix(tokens[4], "member_") {
			return DestinationInbox, OperationMemberPublish
		}
		return DestinationInbox, OperationRoomPublish
	case strings.HasPrefix(subj, "chat.room.") && strings.HasSuffix(subj, ".event"),
		strings.HasPrefix(subj, "chat.local.room.") && strings.HasSuffix(subj, ".event"):
		return DestinationRoomEvent, OperationRoomPublish
	case strings.HasPrefix(subj, "chat.room.") && strings.HasSuffix(subj, ".event.member"),
		strings.HasPrefix(subj, "chat.local.room.") && strings.HasSuffix(subj, ".event.member"):
		return DestinationMemberEvent, OperationMemberPublish
	case strings.HasPrefix(subj, "chat.user.") && (strings.HasSuffix(subj, ".event.room") || strings.HasSuffix(subj, ".event.room.update")):
		return DestinationRecipientEvent, OperationRoomPublish
	case strings.HasPrefix(subj, "chat.user.") && strings.HasSuffix(subj, ".event.subscription.update"):
		return DestinationRecipientEvent, OperationMemberPublish
	case strings.HasPrefix(subj, "chat.user.") && strings.HasSuffix(subj, ".event.room.key"):
		return DestinationRecipientEvent, OperationRoomPublish
	case strings.HasPrefix(subj, "chat.user.") && strings.Contains(subj, ".response."):
		return DestinationClientResponse, OperationClientResponse
	case strings.HasPrefix(subj, "chat.hr.") && strings.HasSuffix(subj, ".users.upsert"):
		return DestinationUserSync, OperationTeamsUserUpsert
	default:
		return DestinationUnknown, OperationUnknown
	}
}

func isMemberEventAt(tokens []string, eventIndex int) bool {
	if len(tokens) <= eventIndex {
		return false
	}
	if tokens[eventIndex] == "member" {
		return true
	}
	return tokens[eventIndex] == "event" && len(tokens) > eventIndex+1 && tokens[eventIndex+1] == "member"
}
