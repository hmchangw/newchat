package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

func TestEligibleForPush(t *testing.T) {
	tests := []struct {
		name      string
		member    roomsubcache.Member
		roomType  model.RoomType
		isLarge   bool
		mentioned bool
		want      bool
	}{
		{name: "dm always", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeDM, want: true},
		{name: "botdm always", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeBotDM, want: true},
		{name: "small channel non-mention", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeChannel, isLarge: false, mentioned: false, want: true},
		{name: "small channel mention", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeChannel, isLarge: false, mentioned: true, want: true},
		{name: "large channel non-mention dropped", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeChannel, isLarge: true, mentioned: false, want: false},
		{name: "large channel mention pushed", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeChannel, isLarge: true, mentioned: true, want: true},
		{name: "bot never", member: roomsubcache.Member{Account: "bot", IsBot: true}, roomType: model.RoomTypeDM, want: false},
		{name: "bot in mention dropped", member: roomsubcache.Member{Account: "bot", IsBot: true}, roomType: model.RoomTypeChannel, mentioned: true, want: false},
		{name: "discussion small non-mention", member: roomsubcache.Member{Account: "a"}, roomType: model.RoomTypeDiscussion, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EligibleForPush(&tt.member, tt.roomType, tt.isLarge, tt.mentioned)
			assert.Equal(t, tt.want, got)
		})
	}
}
