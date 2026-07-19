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

// TestChatUpsertModel_Group_SplitsSetAndSetOnInsert covers the defer path: a
// non-oneOnOne chat with needMemberSync=true (roster too large to trust the
// inline expansion) is handed to teams-chat-member-sync, so members are not
// written and needCreateRoom stays insert-only.
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
		"siteId":          "site-a",
		"needCreateRoom":  false,
	}, soi, "siteID, createdDateTime, and needCreateRoom are insert-only (never clobbered on re-sync)")

	set, ok := update["$set"].(bson.M)
	require.True(t, ok)
	assert.Equal(t, "Topic", set["name"])
	assert.Equal(t, "group", set["chatType"])
	assert.Equal(t, c.LastUpdatedDateTime, set["lastUpdatedDateTime"])
	assert.Equal(t, true, set["needMemberSync"], "re-set on every re-sync to re-trigger member sync on chat changes")
	assert.Equal(t, upsertNow, set["updatedAt"], "$set writes the chat's build-time UpdatedAt stamp")
	assert.NotContains(t, set, "members", "group members are owned by teams-chat-member-sync, not this sync")
	assert.NotContains(t, set, "needCreateRoom", "needCreateRoom is $setOnInsert so a re-sync can't clobber member-sync's flag")
	assert.NotContains(t, set, "siteId", "$set must never touch siteID")
	assert.NotContains(t, set, "createdDateTime")
}

func TestChatUpsertModel_SmallGroup_FinalizesInline(t *testing.T) {
	// A non-oneOnOne chat with needMemberSync=false is a small chat whose inline
	// $expand=members roster was complete, so teams-chat-sync finalizes it itself.
	c := model.TeamsChat{
		ID: "19:g2", Name: "Small", ChatType: "group",
		CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members:             []model.TeamsChatMember{{ID: "u1", Account: "alice"}, {ID: "u2", Account: "bob"}},
		SiteID:              "site-a",
		UpdatedAt:           upsertNow,
		NeedMemberSync:      false,
	}
	u := asUpdateOne(t, chatUpsertModel(c))
	assert.Equal(t, bson.M{"_id": "19:g2"}, u.Filter)

	update, ok := u.Update.(bson.M)
	require.True(t, ok)

	soi, ok := update["$setOnInsert"].(bson.M)
	require.True(t, ok)
	assert.Equal(t, bson.M{
		"createdDateTime": c.CreatedDateTime,
		"siteId":          "site-a",
	}, soi, "only createdDateTime and siteID stay insert-only on the inline-finalize path")
	assert.NotContains(t, soi, "needCreateRoom", "needCreateRoom is $set:true here, not $setOnInsert")

	set, ok := update["$set"].(bson.M)
	require.True(t, ok)
	assert.Equal(t, "Small", set["name"])
	assert.Equal(t, "group", set["chatType"])
	assert.Equal(t, c.LastUpdatedDateTime, set["lastUpdatedDateTime"])
	assert.Equal(t, upsertNow, set["updatedAt"])
	assert.Equal(t, c.Members, set["members"], "the complete inline roster is written directly")
	assert.Equal(t, true, set["needCreateRoom"], "a small chat is immediately room-ready")
	assert.Equal(t, false, set["needMemberSync"], "member sync is skipped for inline-finalized chats")
	assert.NotContains(t, set, "siteId", "$set must never touch siteID")
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
		"siteId":              "site-b",
		"updatedAt":           upsertNow,
		"needMemberSync":      false,
		"needCreateRoom":      true,
	}, soi)
}
