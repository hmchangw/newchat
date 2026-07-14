package main

import "github.com/hmchangw/chat/pkg/model"

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
	case "d":
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
