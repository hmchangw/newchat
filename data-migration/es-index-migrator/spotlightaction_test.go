package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestBuildSpotlightAction(t *testing.T) {
	joinedAt := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	sub := model.Subscription{
		RoomID: "room1", SiteID: "site-a", Name: "general", RoomType: model.RoomTypeChannel,
		User: model.SubscriptionUser{Account: "alice"}, JoinedAt: joinedAt,
	}

	action, err := buildSpotlightAction(sub, "spotlight-a-v1")

	require.NoError(t, err)
	assert.Equal(t, searchengine.ActionIndex, action.Action)
	assert.Equal(t, "spotlight-a-v1", action.Index)
	assert.Equal(t, "alice_room1", action.DocID)
	assert.Equal(t, joinedAt.UnixMilli(), action.Version)

	var doc searchindex.SpotlightDoc
	require.NoError(t, json.Unmarshal(action.Doc, &doc))
	assert.Equal(t, "general", doc.RoomName)
	assert.Equal(t, "channel", doc.RoomType)
}

func TestBuildSpotlightAction_MissingRoomIDIsAnError(t *testing.T) {
	_, err := buildSpotlightAction(model.Subscription{User: model.SubscriptionUser{Account: "alice"}}, "spotlight-a-v1")
	require.Error(t, err)
}

func TestBuildSpotlightAction_MissingAccountIsAnError(t *testing.T) {
	_, err := buildSpotlightAction(model.Subscription{RoomID: "room1"}, "spotlight-a-v1")
	require.Error(t, err)
}
