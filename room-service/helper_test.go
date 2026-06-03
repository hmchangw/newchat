package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

func TestHasRole(t *testing.T) {
	tests := []struct {
		name   string
		roles  []model.Role
		target model.Role
		want   bool
	}{
		{"owner in [owner]", []model.Role{model.RoleOwner}, model.RoleOwner, true},
		{"member in [member]", []model.Role{model.RoleMember}, model.RoleMember, true},
		{"owner not in [member]", []model.Role{model.RoleMember}, model.RoleOwner, false},
		{"member not in [owner]", []model.Role{model.RoleOwner}, model.RoleMember, false},
		{"empty roles", []model.Role{}, model.RoleOwner, false},
		{"nil roles", nil, model.RoleOwner, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasRole(tt.roles, tt.target)
			if got != tt.want {
				t.Errorf("hasRole(%v, %q) = %v, want %v", tt.roles, tt.target, got, tt.want)
			}
		})
	}
}

func TestIsBot(t *testing.T) {
	tests := []struct {
		name    string
		account string
		want    bool
	}{
		{"bot suffix", "helper.bot", true},
		{"bot prefix", "p_scheduler", true},
		{"normal user", "alice", false},
		{"contains bot but not suffix", "botmaster", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isBot(tt.account))
		})
	}
}

func TestFilterBots(t *testing.T) {
	input := []string{"alice", "helper.bot", "bob", "p_scheduler"}
	got := filterBots(input)
	assert.Equal(t, []string{"alice", "bob"}, got)
}

func TestFilterBots_AllBots(t *testing.T) {
	input := []string{"helper.bot", "p_scheduler"}
	got := filterBots(input)
	assert.Nil(t, got)
}

func TestDedup(t *testing.T) {
	input := []string{"a", "b", "a", "c", "b"}
	got := dedup(input)
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestDedup_Empty(t *testing.T) {
	got := dedup([]string{})
	assert.Nil(t, got)
}

// TestSentinelCodesAndReasons verifies each migrated sentinel carries the
// category (and where applicable the reason) from the plan's mapping table.
// This replaces the deleted sanitizeError suite.
func TestSentinelCodesAndReasons(t *testing.T) {
	cases := []struct {
		name   string
		err    *errcode.Error
		code   errcode.Code
		reason errcode.Reason
	}{
		{"invalid role", errInvalidRole, errcode.CodeBadRequest, ""},
		{"only owners", errOnlyOwners, errcode.CodeForbidden, errcode.RoomNotOwner},
		{"only owners can remove", errOnlyOwnersCanRemove, errcode.CodeForbidden, errcode.RoomNotOwner},
		{"only owners can add to restricted", errOnlyOwnersCanAddToRes, errcode.CodeForbidden, errcode.RoomNotOwner},
		{"already owner", errAlreadyOwner, errcode.CodeConflict, errcode.RoomAlreadyOwner},
		{"not owner", errNotOwner, errcode.CodeForbidden, errcode.RoomNotOwner},
		{"cannot demote last", errCannotDemoteLast, errcode.CodeConflict, errcode.RoomCannotDemoteLastOwner},
		{"room type guard", errRoomTypeGuard, errcode.CodeBadRequest, errcode.RoomNonChannelOperation},
		{"add members channel only", errAddMembersChannelOnly, errcode.CodeBadRequest, errcode.RoomNonChannelOperation},
		{"target not member", errTargetNotMember, errcode.CodeBadRequest, errcode.RoomTargetNotMember},
		{"not room member", errNotRoomMember, errcode.CodeForbidden, errcode.RoomNotMember},
		{"invalid thread id", errInvalidThreadID, errcode.CodeBadRequest, ""},
		{"thread sub not found", errThreadSubNotFound, errcode.CodeNotFound, ""},
		{"promote requires individual", errPromoteRequiresIndividual, errcode.CodeBadRequest, errcode.RoomPromoteRequiresIndividual},
		{"empty create request", errEmptyCreateRequest, errcode.CodeBadRequest, ""},
		{"self dm", errSelfDM, errcode.CodeBadRequest, errcode.RoomSelfDM},
		{"bot in channel", errBotInChannel, errcode.CodeBadRequest, errcode.RoomBotInChannel},
		{"bot not available", errBotNotAvailable, errcode.CodeNotFound, errcode.RoomBotNotAvailable},
		{"invalid user data", errInvalidUserData, errcode.CodeBadRequest, ""},
		{"channel name required", errChannelNameRequired, errcode.CodeBadRequest, ""},
		{"channel name too long", errChannelNameTooLong, errcode.CodeBadRequest, ""},
		{"message not found", errMessageNotFound, errcode.CodeNotFound, ""},
		{"message room mismatch", errMessageRoomMismatch, errcode.CodeBadRequest, ""},
		{"not message sender", errNotMessageSender, errcode.CodeForbidden, ""},
		{"remove target ambiguous", errRemoveTargetAmbiguous, errcode.CodeBadRequest, ""},
		{"cannot remove last member", errCannotRemoveLastMember, errcode.CodeConflict, errcode.RoomLastMemberCannotRemove},
		{"last owner cannot leave", errLastOwnerCannotLeave, errcode.CodeConflict, errcode.RoomLastOwnerCannotLeave},
		{"org member cannot leave solo", errOrgMemberCannotLeaveSolo, errcode.CodeForbidden, ""},
		{"room id mismatch", errRoomIDMismatch, errcode.CodeBadRequest, ""},
		{"remove channel only", errRemoveChannelOnly, errcode.CodeBadRequest, errcode.RoomNonChannelOperation},
		{"list limit invalid", errListLimitInvalid, errcode.CodeBadRequest, ""},
		{"list offset invalid", errListOffsetInvalid, errcode.CodeBadRequest, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.code, tc.err.Code)
			assert.Equal(t, tc.reason, tc.err.Reason)
		})
	}
}

func TestNewSentinelErrorsExist(t *testing.T) {
	assert.Equal(t, "request must include at least one of users, orgs, channels, or name", errEmptyCreateRequest.Error())
	assert.Equal(t, "cannot create a DM with yourself", errSelfDM.Error())
	assert.Equal(t, "bots cannot be added to a channel", errBotInChannel.Error())
	assert.Equal(t, "bot not available", errBotNotAvailable.Error())
	assert.Equal(t, "user is missing required name fields", errInvalidUserData.Error())
	assert.Equal(t, "channel name is required", errChannelNameRequired.Error())
	assert.Equal(t, "channel name must be at most 100 characters", errChannelNameTooLong.Error())
}

func TestStripAccount(t *testing.T) {
	tests := map[string]struct {
		in      []string
		account string
		want    []string
	}{
		"present":        {[]string{"alice", "bob", "carol"}, "bob", []string{"alice", "carol"}},
		"absent":         {[]string{"alice", "carol"}, "bob", []string{"alice", "carol"}},
		"first":          {[]string{"alice", "bob"}, "alice", []string{"bob"}},
		"only-element":   {[]string{"alice"}, "alice", []string{}},
		"multiple-occur": {[]string{"alice", "alice", "bob"}, "alice", []string{"bob"}},
		"empty":          {[]string{}, "alice", []string{}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := stripAccount(tc.in, tc.account)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsPlatformAdmin(t *testing.T) {
	tests := []struct {
		name string
		user *model.User
		want bool
	}{
		{"nil", nil, false},
		{"empty roles", &model.User{Account: "alice"}, false},
		{"user only", &model.User{Account: "a", Roles: []model.UserRole{model.UserRoleUser}}, false},
		{"admin", &model.User{Account: "a", Roles: []model.UserRole{model.UserRoleAdmin}}, true},
		{"mixed", &model.User{Account: "a", Roles: []model.UserRole{model.UserRoleUser, model.UserRoleAdmin}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { assert.Equal(t, tt.want, isPlatformAdmin(tt.user)) })
	}
}

func TestIsURLSafeIDToken(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty string", "", false},
		{"simple alphanumeric", "site-a", true},
		{"lowercase", "roomabc123", true},
		{"uppercase", "SiteA", true},
		{"underscore", "room_id", true},
		{"dot", "v1.2.3", true},
		{"tilde", "abc~def", true},
		{"hyphen", "room-id-123", true},
		{"question mark", "room?id", false},
		{"hash", "room#id", false},
		{"slash", "room/id", false},
		{"asterisk", "room*", false},
		{"greater than", "room>id", false},
		{"less than", "room<id", false},
		{"space", "room id", false},
		{"percent", "room%20id", false},
		{"at sign", "room@id", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isURLSafeIDToken(tt.input))
		})
	}
}

func TestDetermineRoomType(t *testing.T) {
	tests := []struct {
		name string
		req  model.CreateRoomRequest
		want model.RoomType
	}{
		{
			name: "single user no name → DM",
			req:  model.CreateRoomRequest{Users: []string{"bob"}},
			want: model.RoomTypeDM,
		},
		{
			name: "single bot user no name → botDM",
			req:  model.CreateRoomRequest{Users: []string{"helper.bot"}},
			want: model.RoomTypeBotDM,
		},
		{
			name: "single user with name → channel",
			req:  model.CreateRoomRequest{Name: "general", Users: []string{"bob"}},
			want: model.RoomTypeChannel,
		},
		{
			name: "multiple users no name → channel",
			req:  model.CreateRoomRequest{Users: []string{"bob", "charlie"}},
			want: model.RoomTypeChannel,
		},
		{
			name: "name only no users → channel",
			req:  model.CreateRoomRequest{Name: "general"},
			want: model.RoomTypeChannel,
		},
		{
			name: "org only → channel",
			req:  model.CreateRoomRequest{Orgs: []string{"eng"}},
			want: model.RoomTypeChannel,
		},
		{
			name: "channel ref only → channel",
			req:  model.CreateRoomRequest{Channels: []model.ChannelRef{{RoomID: "r1", SiteID: "site-a"}}},
			want: model.RoomTypeChannel,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := determineRoomType(&tt.req)
			assert.Equal(t, tt.want, got)
		})
	}
}
