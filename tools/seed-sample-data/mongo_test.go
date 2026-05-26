package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSeedIDs_UsersMatchesBuildUsers(t *testing.T) {
	got := usersIDs()
	assert.Len(t, got, 10)
	assert.Contains(t, got, "u-alice")
	assert.Contains(t, got, "u-judy")
}

func TestSeedIDs_RoomsMatchesBuildRooms(t *testing.T) {
	got := roomIDs()
	assert.Len(t, got, 6)
	assert.Contains(t, got, "r-general")
	assert.Contains(t, got, "r-remote-announce")
}

func TestSeedIDs_AllCollectionsExposeIDs(t *testing.T) {
	assert.Len(t, usersIDs(), 10)
	assert.Len(t, roomIDs(), 6)
	assert.Len(t, subscriptionIDs(), 23)
	assert.Len(t, roomMemberIDs(), 19)
	assert.Len(t, messageIDs(), 23)
	assert.Len(t, threadRoomIDs(), 1)
	assert.Len(t, threadSubscriptionIDs(), 2)
}
