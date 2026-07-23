package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

func TestEligibleSoakUsers_ExcludesUnsafeAccounts(t *testing.T) {
	users := []model.User{
		{ID: "u-1", Account: "alice", SiteID: "site-a"},
		{ID: "u-2", Account: "disabled", SiteID: "site-a", Deactivated: true},
		{ID: "u-3", Account: "", SiteID: "site-a"},
		{ID: "u-4", Account: "bad.account", SiteID: "site-a"},
		{ID: "u-5", Account: "load.bot", SiteID: "site-a"},
		{ID: "u-6", Account: "p_platform", SiteID: "site-a"},
		{ID: "u-7", Account: "role-bot", SiteID: "site-a", Roles: []model.UserRole{model.UserRoleBot}},
		{ID: "u-8", Account: "role-admin", SiteID: "site-a", Roles: []model.UserRole{model.UserRoleAdmin}},
		{ID: "u-9", Account: "other-site", SiteID: "site-b"},
		{ID: "", Account: "missing-id", SiteID: "site-a"},
	}

	got := eligibleSoakUsers(users, "site-a")

	require.Len(t, got, 1)
	assert.Equal(t, "alice", got[0].Account)
}

func TestSelectSoakUsers_CapsBorrowedPoolAndSelectsActiveDeterministically(t *testing.T) {
	users := makeSoakUsers(20005, "site-a")

	borrowedA, activeA, err := selectSoakUsers(users, "site-a", 20000, 2000, 42)
	require.NoError(t, err)
	borrowedB, activeB, err := selectSoakUsers(users, "site-a", 20000, 2000, 42)
	require.NoError(t, err)
	_, activeC, err := selectSoakUsers(users, "site-a", 20000, 2000, 43)
	require.NoError(t, err)

	assert.Len(t, borrowedA, 20000)
	assert.Len(t, activeA, 2000)
	assert.Equal(t, borrowedA, borrowedB)
	assert.Equal(t, activeA, activeB)
	assert.NotEqual(t, activeA, activeC)
}

func TestSelectSoakUsers_RejectsInsufficientEligibleUsers(t *testing.T) {
	_, _, err := selectSoakUsers(makeSoakUsers(3, "site-a"), "site-a", 10, 4, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "active users")
}

func TestBuildSoakTopology_ChannelDMSplitMembershipAndRoles(t *testing.T) {
	users := makeSoakUsers(12, "site-a")
	original := append([]model.User(nil), users...)
	cfg := validSoakConfig(t)
	cfg.MaxUsers = 12
	cfg.ActiveUsers = 8
	cfg.RoomCount = 10
	cfg.ChannelRatio = 0.3
	cfg.ChannelMembers = 4
	cfg.ReactionsPerHotMessage = 4

	topology, err := buildSoakTopology(users, &cfg, "site-a", 17, newSequenceSoakIDs())
	require.NoError(t, err)

	var channels, dms int
	dmIDs := make(map[string]struct{})
	subscriptionsByRoom := make(map[string][]model.Subscription)
	for _, sub := range topology.Subscriptions {
		subscriptionsByRoom[sub.RoomID] = append(subscriptionsByRoom[sub.RoomID], sub)
	}
	for _, room := range topology.Rooms {
		subs := subscriptionsByRoom[room.ID]
		assert.Equal(t, len(subs), room.UserCount)
		switch room.Type {
		case model.RoomTypeChannel:
			channels++
			require.Len(t, subs, 4)
			owners := 0
			for _, sub := range subs {
				if assert.Len(t, sub.Roles, 1) && sub.Roles[0] == model.RoleOwner {
					owners++
				}
			}
			assert.Equal(t, 1, owners)
		case model.RoomTypeDM:
			dms++
			require.Len(t, subs, 2)
			assert.NotEqual(t, subs[0].User.ID, subs[1].User.ID)
			assert.Equal(t, idgen.BuildDMRoomID(subs[0].User.ID, subs[1].User.ID), room.ID)
			_, duplicate := dmIDs[room.ID]
			assert.False(t, duplicate)
			dmIDs[room.ID] = struct{}{}
			for _, sub := range subs {
				assert.Equal(t, []model.Role{model.RoleMember}, sub.Roles)
			}
		default:
			t.Fatalf("unexpected room type %q", room.Type)
		}
	}

	assert.Equal(t, 3, channels)
	assert.Equal(t, 7, dms)
	assert.Equal(t, original, users, "borrowed user values must remain unchanged")
}

func TestBuildSoakTopology_EveryActiveUserHasWritableRoom(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.MaxUsers = 10
	cfg.ActiveUsers = 10
	cfg.RoomCount = 5
	cfg.ChannelRatio = 0.2
	cfg.ChannelMembers = 2
	cfg.ReactionsPerHotMessage = 2

	topology, err := buildSoakTopology(makeSoakUsers(10, "site-a"), &cfg, "site-a", 5, newSequenceSoakIDs())
	require.NoError(t, err)

	hasRoom := make(map[string]bool)
	for _, sub := range topology.Subscriptions {
		hasRoom[sub.User.ID] = true
	}
	for _, user := range topology.ActiveUsers {
		assert.True(t, hasRoom[user.ID], "active user %s has no writable room", user.ID)
	}
}

func TestBuildSoakTopology_IsDeterministicWithSeededIdentitySource(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.MaxUsers = 20
	cfg.ActiveUsers = 10
	cfg.RoomCount = 12
	cfg.ChannelRatio = 0.25
	cfg.ChannelMembers = 5
	cfg.ReactionsPerHotMessage = 5
	users := makeSoakUsers(20, "site-a")

	a, err := buildSoakTopology(users, &cfg, "site-a", 99, newSequenceSoakIDs())
	require.NoError(t, err)
	b, err := buildSoakTopology(users, &cfg, "site-a", 99, newSequenceSoakIDs())
	require.NoError(t, err)

	assert.Equal(t, a, b)
}

func TestBuildSoakTopology_RejectsImpossibleRoomShape(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.MaxUsers = 3
	cfg.ActiveUsers = 3
	cfg.RoomCount = 4
	cfg.ChannelRatio = 0
	cfg.ChannelMembers = 2
	cfg.ReactionsPerHotMessage = 2

	_, err := buildSoakTopology(makeSoakUsers(3, "site-a"), &cfg, "site-a", 1, newSequenceSoakIDs())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unique DM")
}

func TestProductionSoakIDs_UseProjectIdentityFormats(t *testing.T) {
	ids := newProductionSoakIDs()

	assert.True(t, idgen.IsValidMessageID(ids.channelRoomID()))
	assert.True(t, idgen.IsValidUUIDv7(ids.subscriptionID()))
}

func makeSoakUsers(count int, siteID string) []model.User {
	users := make([]model.User, count)
	for i := range users {
		users[i] = model.User{
			ID:      fmt.Sprintf("u-%05d", i),
			Account: fmt.Sprintf("user-%05d", i),
			SiteID:  siteID,
			Roles:   []model.UserRole{model.UserRoleUser},
		}
	}
	return users
}

func newSequenceSoakIDs() *soakIDs {
	var room, subscription int
	return &soakIDs{
		channelRoomID: func() string {
			room++
			return fmt.Sprintf("channel-%03d", room)
		},
		subscriptionID: func() string {
			subscription++
			return fmt.Sprintf("subscription-%05d", subscription)
		},
	}
}
