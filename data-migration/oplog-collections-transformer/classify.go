package main

import (
	"strings"

	"github.com/hmchangw/chat/pkg/model"
)

// deletedNamePrefix is the source app's soft-delete marker: "deleting" a room renames it (and the
// denormalized name on every subscription to it) to "Del-"+name — there is no delete flag and no
// row removal. The destination honors the same marker: user-service filters ^Del- rooms out of
// every subscription read.
const deletedNamePrefix = "Del-"

// dmSourceType is the source room type for a direct message.
const dmSourceType = "d"

// isSoftDeletedRecord reports whether a source room or subscription doc carries the soft-delete
// marker on either name field. DMs are exempt: for t:"d" both fields hold the PEER's username and
// display name (see memberAddedEvent), so there the prefix marks a user, not a deletion.
func isSoftDeletedRecord(t string, names ...string) bool {
	if t == dmSourceType {
		return false
	}
	for _, n := range names {
		if strings.HasPrefix(n, deletedNamePrefix) {
			return true
		}
	}
	return false
}

// roomClass is the result of classifying a source room.
type roomClass struct {
	Type     model.RoomType // valid only when !Excluded
	Excluded bool
	Reason   string // exclusion reason (for metrics), set only when Excluded
}

// classifyRoom maps a source room type t (+ prid/teamId/participant/bot signals) to a destination
// RoomType or an exclusion (group_dm/livechat/voip/unknown_type). p+prid→discussion, team→channel. §4.2.
func classifyRoom(t string, hasPrid, hasTeamID bool, hasBot bool, participantCount int) roomClass {
	// hasTeamID is accepted for caller clarity and future use; c/p branch already returns channel
	// regardless of teamId, so no separate branch is needed here.
	_ = hasTeamID
	switch t {
	case "c", "p":
		if t == "p" && hasPrid {
			return roomClass{Type: model.RoomTypeDiscussion}
		}
		return roomClass{Type: model.RoomTypeChannel}
	case dmSourceType:
		if participantCount > 2 {
			return roomClass{Excluded: true, Reason: "group_dm"}
		}
		if hasBot {
			return roomClass{Type: model.RoomTypeBotDM}
		}
		return roomClass{Type: model.RoomTypeDM}
	case "l":
		return roomClass{Excluded: true, Reason: "livechat"}
	case "v":
		return roomClass{Excluded: true, Reason: "voip"}
	default:
		return roomClass{Excluded: true, Reason: "unknown_type"}
	}
}
