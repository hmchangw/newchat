package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

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

func TestSetMembersSyncedModels(t *testing.T) {
	now := time.Date(2026, 7, 15, 3, 4, 5, 0, time.UTC)
	seen1 := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	seen2 := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	members1 := []model.TeamsChatMember{{ID: "u1", Account: "alice"}}
	updates := []ChatMembersUpdate{
		{ChatID: "19:g1", SeenUpdatedAt: seen1, Members: members1},
		{ChatID: "19:g2", SeenUpdatedAt: seen2, Members: nil},
	}

	models := setMembersSyncedModels(updates, now)
	require.Len(t, models, 2, "one conditional update per chat")

	m0, ok := models[0].(*mongo.UpdateOneModel)
	require.True(t, ok, "each model is a single-document conditional update")
	assert.Equal(t, bson.M{"_id": "19:g1", "updatedAt": seen1}, m0.Filter, "filter carries the optimistic-concurrency token")
	assert.Equal(t, setMembersSyncedUpdate(members1, now), m0.Update)

	m1, ok := models[1].(*mongo.UpdateOneModel)
	require.True(t, ok)
	assert.Equal(t, bson.M{"_id": "19:g2", "updatedAt": seen2}, m1.Filter)
}
