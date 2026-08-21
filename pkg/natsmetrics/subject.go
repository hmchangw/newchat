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

// RequestOperationFromSubject maps concrete request subjects to the bounded
// operation vocabulary used by natsrouter. Dynamic account, room, and site
// tokens are inspected only for shape and never become labels.
func RequestOperationFromSubject(subj string) Operation {
	tokens := strings.Split(subj, ".")
	// Suffix matching alone would classify any subject that happens to end in a
	// request-shaped tail, so it is gated on the subject's family token. Every
	// other label in this file is anchored to fixed token positions for the same
	// reason.
	isRoomRequest := isRoomFamilyRequest(tokens)
	switch {
	case strings.HasPrefix(subj, "chat.server.request.history."):
		return OperationHistoryRead
	case strings.HasPrefix(subj, "chat.server.request.thread."):
		return OperationHistoryRead
	case isRequestFamily(tokens, "msg") || isMigrationFamily(tokens, "msg"):
		switch {
		case hasAnySuffix(subj, ".msg.edit", ".msg.delete", ".msg.pin", ".msg.unpin", ".msg.react"):
			return OperationHistoryMutation
		case hasAnySuffix(subj, ".msg.history", ".msg.next", ".msg.surrounding", ".msg.get", ".msg.get.ids", ".msg.pinned.list", ".msg.thread", ".msg.thread.parent"):
			return OperationHistoryRead
		}
	case isRequestFamily(tokens, "teams"):
		return OperationTeamsRoom
	case isRoomRequest && hasAnySuffix(subj, ".member.list", ".member.statuses", ".subscription.mentionable", ".members"):
		return OperationMemberRead
	case isRoomRequest && hasAnySuffix(subj, ".member.add", ".member.remove", ".member.role-update", ".mute.toggle"):
		return OperationMemberMutation
	case isRoomRequest && hasAnySuffix(subj, ".key.get", ".open", ".app.tabs", ".app.cmd-menu"):
		return OperationRoomRead
	case isRoomRequest && hasAnySuffix(subj, ".room.rename", ".create", ".chat.move", ".favorite.toggle", ".message.read", ".message.read-receipt", ".message.thread.read"):
		return OperationRoomMutation
	case strings.HasPrefix(subj, "chat.server.request.room.") && hasAnySuffix(subj, ".info.batch", ".thread.info.batch"):
		return OperationRoomRead
	case strings.HasPrefix(subj, "chat.server.request.room."):
		return OperationRoomMutation
	default:
		return OperationUnknown
	}
	return OperationUnknown
}

// isRoomFamilyRequest reports whether tokens name a client request in one of
// room-service's families: chat.user.{account}.request.room.… or
// chat.user.{account}.request.orgs.….
//
// This gates the room/member suffix rules, which would otherwise cross service
// boundaries. That is not hypothetical: user-service's
// chat.user.{account}.request.user.{site}.chatlist.section.create ends in
// ".create" and was recorded as operation "room_mutation", merging its traffic
// into room-service's series. Every other service's subjects stay "unknown"
// until the operation vocabulary is extended for them deliberately — a wrong
// bounded label is worse than an honest unknown, because a wrong one is
// silently aggregated with real traffic.
//
// Server-to-server requests are deliberately not covered here: the
// chat.server.request.* cases below classify them by prefix, and no server
// subject in pkg/subject ends in one of the room/member tails.
func isRoomFamilyRequest(tokens []string) bool {
	if len(tokens) < 5 || tokens[0] != "chat" || tokens[1] != "user" || tokens[3] != "request" {
		return false
	}
	return tokens[4] == "room" || tokens[4] == "orgs"
}

func isRequestFamily(tokens []string, family string) bool {
	if len(tokens) < 5 || tokens[0] != "chat" || tokens[1] != "user" || tokens[3] != "request" {
		return false
	}
	if tokens[4] == family {
		return true
	}
	return len(tokens) >= 8 && tokens[4] == "room" && tokens[7] == family
}

func isMigrationFamily(tokens []string, family string) bool {
	return len(tokens) >= 5 && tokens[0] == "chat" && tokens[1] == "migration" &&
		tokens[2] == "internal" && tokens[4] == family
}

// PublishLabelsFromSubject maps a publish destination to closed labels. It is
// intentionally conservative: a new subject family remains unknown until it
// receives an explicit bounded mapping.
func PublishLabelsFromSubject(subj string) (DestinationKind, Operation) {
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

func hasAnySuffix(value string, suffixes ...string) bool {
	for _, suffix := range suffixes {
		if strings.HasSuffix(value, suffix) {
			return true
		}
	}
	return false
}
