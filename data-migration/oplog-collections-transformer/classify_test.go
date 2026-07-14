package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

func TestClassifyRoom(t *testing.T) {
	tests := []struct {
		name             string
		t                string
		hasPrid          bool
		hasTeamID        bool
		hasBot           bool
		participantCount int
		wantType         model.RoomType
		wantExcluded     bool
		wantReason       string
	}{
		{
			name:             "c → channel",
			t:                "c",
			hasPrid:          false,
			hasTeamID:        false,
			hasBot:           false,
			participantCount: 0,
			wantType:         model.RoomTypeChannel,
			wantExcluded:     false,
		},
		{
			name:             "p no prid → channel",
			t:                "p",
			hasPrid:          false,
			hasTeamID:        false,
			hasBot:           false,
			participantCount: 0,
			wantType:         model.RoomTypeChannel,
			wantExcluded:     false,
		},
		{
			name:             "p with prid → discussion",
			t:                "p",
			hasPrid:          true,
			hasTeamID:        false,
			hasBot:           false,
			participantCount: 0,
			wantType:         model.RoomTypeDiscussion,
			wantExcluded:     false,
		},
		{
			name:             "d 2 participants no bot → dm",
			t:                "d",
			hasPrid:          false,
			hasTeamID:        false,
			hasBot:           false,
			participantCount: 2,
			wantType:         model.RoomTypeDM,
			wantExcluded:     false,
		},
		{
			name:             "d 2 participants with bot → botDM",
			t:                "d",
			hasPrid:          false,
			hasTeamID:        false,
			hasBot:           true,
			participantCount: 2,
			wantType:         model.RoomTypeBotDM,
			wantExcluded:     false,
		},
		{
			name:             "d 3 participants → excluded group_dm",
			t:                "d",
			hasPrid:          false,
			hasTeamID:        false,
			hasBot:           false,
			participantCount: 3,
			wantExcluded:     true,
			wantReason:       "group_dm",
		},
		{
			name:             "l → excluded livechat",
			t:                "l",
			hasPrid:          false,
			hasTeamID:        false,
			hasBot:           false,
			participantCount: 0,
			wantExcluded:     true,
			wantReason:       "livechat",
		},
		{
			name:             "v → excluded voip",
			t:                "v",
			hasPrid:          false,
			hasTeamID:        false,
			hasBot:           false,
			participantCount: 0,
			wantExcluded:     true,
			wantReason:       "voip",
		},
		{
			name:             "unknown type x → excluded unknown_type",
			t:                "x",
			hasPrid:          false,
			hasTeamID:        false,
			hasBot:           false,
			participantCount: 0,
			wantExcluded:     true,
			wantReason:       "unknown_type",
		},
		{
			name:             "c with teamId → channel (team rooms as plain channel)",
			t:                "c",
			hasPrid:          false,
			hasTeamID:        true,
			hasBot:           false,
			participantCount: 0,
			wantType:         model.RoomTypeChannel,
			wantExcluded:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyRoom(tc.t, tc.hasPrid, tc.hasTeamID, tc.hasBot, tc.participantCount)
			assert.Equal(t, tc.wantExcluded, got.Excluded)
			if tc.wantExcluded {
				assert.Equal(t, tc.wantReason, got.Reason)
			} else {
				assert.Equal(t, tc.wantType, got.Type)
				assert.Empty(t, got.Reason)
			}
		})
	}
}
