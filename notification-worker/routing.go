package main

import (
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

// EligibleForPush is Stage 3 of the fan-out pipeline (pure CPU, no I/O).
// Bots are always excluded; DMs and @mentions bypass the large-room throttle.
func EligibleForPush(m *roomsubcache.Member, roomType model.RoomType, isLargeRoom, mentioned bool) bool {
	if m.IsBot {
		return false
	}
	if isDirect(roomType) {
		return true
	}
	if mentioned {
		return true
	}
	return !isLargeRoom
}

func isDirect(t model.RoomType) bool {
	return t == model.RoomTypeDM || t == model.RoomTypeBotDM
}
