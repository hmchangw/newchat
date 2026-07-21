package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func TestSetMembersSyncedUpdate(t *testing.T) {
	now := time.Date(2026, 7, 15, 3, 4, 5, 0, time.UTC)
	members := []model.TeamsChatMember{
		{ID: "u1", Account: "alice", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "guest", Account: ""},
	}
	update := setMembersSyncedUpdate(members, now)

	set, ok := update["$set"].(bson.M)
	require.True(t, ok, "update must have a $set bson.M")
	assert.Equal(t, members, set["members"])
	assert.Equal(t, true, set["needCreateRoom"])
	assert.Equal(t, false, set["needMemberSync"])
	assert.Equal(t, now, set["updatedAt"])
	assert.Len(t, set, 4, "$set writes exactly members, needCreateRoom, needMemberSync, updatedAt")
}
