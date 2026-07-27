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

func TestBuildUserRoomAction_Unrestricted(t *testing.T) {
	joinedAt := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	sub := model.Subscription{
		RoomID: "room1", User: model.SubscriptionUser{Account: "alice"}, JoinedAt: joinedAt,
	}

	action, err := buildUserRoomAction(sub, "user-room-a")

	require.NoError(t, err)
	assert.Equal(t, searchengine.ActionUpdate, action.Action)
	assert.Equal(t, "user-room-a", action.Index)
	assert.Equal(t, "alice", action.DocID)
	assert.Zero(t, action.Version, "user-room actions carry no external version — the script's own LWW guard is the ordering mechanism")

	var decoded struct {
		Script struct {
			ID     string         `json:"id"`
			Params map[string]any `json:"params"`
		} `json:"script"`
	}
	require.NoError(t, json.Unmarshal(action.Doc, &decoded))
	assert.Equal(t, searchindex.AddRoomScriptID, decoded.Script.ID)
	assert.Equal(t, "room1", decoded.Script.Params["rid"])
	assert.InDelta(t, 0, decoded.Script.Params["hss"], 0)
}

func TestBuildUserRoomAction_Restricted(t *testing.T) {
	joinedAt := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	hss := joinedAt.Add(-24 * time.Hour)
	sub := model.Subscription{
		RoomID: "room1", User: model.SubscriptionUser{Account: "alice"},
		JoinedAt: joinedAt, HistorySharedSince: &hss,
	}

	action, err := buildUserRoomAction(sub, "user-room-a")

	require.NoError(t, err)
	var decoded struct {
		Script struct {
			Params map[string]any `json:"params"`
		} `json:"script"`
	}
	require.NoError(t, json.Unmarshal(action.Doc, &decoded))
	assert.InDelta(t, hss.UnixMilli(), decoded.Script.Params["hss"], 0)
}

func TestBuildUserRoomAction_BotSubscriptionIsSkipped(t *testing.T) {
	sub := model.Subscription{
		RoomID: "room1", User: model.SubscriptionUser{Account: "helper.bot", IsBot: true}, JoinedAt: time.Now(),
	}

	action, err := buildUserRoomAction(sub, "user-room-a")

	require.NoError(t, err)
	assert.Equal(t, searchengine.BulkAction{}, action, "bot subscriptions must not be fanned into the user-room index (matches search-sync-worker's live BuildAction)")
}

func TestBuildUserRoomAction_MissingRoomIDIsAnError(t *testing.T) {
	_, err := buildUserRoomAction(model.Subscription{User: model.SubscriptionUser{Account: "alice"}}, "user-room-a")
	require.Error(t, err)
}
