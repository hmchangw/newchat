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

var upsertNow = time.Date(2026, 7, 14, 3, 4, 5, 0, time.UTC)

func asUpdateOne(t *testing.T, m mongo.WriteModel) *mongo.UpdateOneModel {
	t.Helper()
	u, ok := m.(*mongo.UpdateOneModel)
	require.True(t, ok, "chatUpsertModel must return *mongo.UpdateOneModel")
	require.NotNil(t, u.Upsert)
	require.True(t, *u.Upsert)
	return u
}

func TestChatUpsertModel_Group_SplitsSetAndSetOnInsert(t *testing.T) {
	c := model.TeamsChat{
		ID: "19:g1", Name: "Topic", ChatType: "group",
		CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members:             []model.TeamsChatMember{{ID: "u1", Account: "alice"}},
		SiteID:              "site-a",
		UpdatedAt:           upsertNow,
		NeedMemberSync:      true,
	}
	u := asUpdateOne(t, chatUpsertModel(c))
	assert.Equal(t, bson.M{"_id": "19:g1"}, u.Filter)

	update, ok := u.Update.(bson.M)
	require.True(t, ok)
	soi, ok := update["$setOnInsert"].(bson.M)
	require.True(t, ok)
	assert.Equal(t, bson.M{
		"createdDateTime": c.CreatedDateTime,
		"siteID":          "site-a",
	}, soi, "siteID and createdDateTime are insert-only")

	set, ok := update["$set"].(bson.M)
	require.True(t, ok)
	assert.Equal(t, "Topic", set["name"])
	assert.Equal(t, "group", set["chatType"])
	assert.Equal(t, c.LastUpdatedDateTime, set["lastUpdatedDateTime"])
	assert.Equal(t, c.Members, set["members"])
	assert.Equal(t, true, set["needMemberSync"])
	assert.Equal(t, upsertNow, set["updatedAt"], "$set writes the chat's build-time UpdatedAt stamp")
	assert.NotContains(t, set, "siteID", "$set must never touch siteID")
	assert.NotContains(t, set, "createdDateTime")
}

func TestChatUpsertModel_OneOnOne_AllSetOnInsert(t *testing.T) {
	c := model.TeamsChat{
		ID: "19:one1", Name: "", ChatType: model.TeamsChatTypeOneOnOne,
		CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members:             []model.TeamsChatMember{{ID: "u1", Account: "alice"}, {ID: "u2", Account: "bob"}},
		SiteID:              "site-b",
		UpdatedAt:           upsertNow,
		NeedMemberSync:      false,
	}
	u := asUpdateOne(t, chatUpsertModel(c))
	update, ok := u.Update.(bson.M)
	require.True(t, ok)
	assert.NotContains(t, update, "$set", "oneOnOne must never modify an existing doc")
	soi, ok := update["$setOnInsert"].(bson.M)
	require.True(t, ok)
	assert.Equal(t, bson.M{
		"name":                "",
		"chatType":            model.TeamsChatTypeOneOnOne,
		"createdDateTime":     c.CreatedDateTime,
		"lastUpdatedDateTime": c.LastUpdatedDateTime,
		"members":             c.Members,
		"siteID":              "site-b",
		"updatedAt":           upsertNow,
		"needMemberSync":      false,
	}, soi)
}
