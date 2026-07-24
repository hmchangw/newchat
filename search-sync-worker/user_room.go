package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

// userRoomCollection implements Collection for the user-room access-control index (per-user rooms
// array). Safe across multiple pods on the same consumer: painless scripts apply a params.ts timestamp guard (ctx.op='none' on stale), converging on last-write-wins regardless of arrival order.
type userRoomCollection struct {
	inboxMemberCollection
	indexName string
}

func newUserRoomCollection(indexName string) *userRoomCollection {
	return &userRoomCollection{indexName: indexName}
}

func (c *userRoomCollection) ConsumerName() string {
	return "user-room-sync"
}

func (c *userRoomCollection) TemplateName() string {
	return searchindex.UserRoomTemplateName(c.indexName)
}

func (c *userRoomCollection) TemplateBody() json.RawMessage {
	return searchindex.UserRoomTemplateBody(c.indexName)
}

// StoredScripts registers the add/remove painless scripts as ES stored scripts; BuildAction
// references them by id so fan-out member updates don't repeat the full source per action.
func (c *userRoomCollection) StoredScripts() map[string]json.RawMessage {
	return map[string]json.RawMessage{
		searchindex.AddRoomScriptID:    searchindex.StoredScriptBody(searchindex.AddRoomScript),
		searchindex.RemoveRoomScriptID: searchindex.StoredScriptBody(searchindex.RemoveRoomScript),
	}
}

// BuildAction fans a member_added/member_removed event into one ES update per account (bulk
// invites yield N doc updates from one event); restricted rooms route into restrictedRooms{}, read alongside rooms[] by search-service.
func (c *userRoomCollection) BuildAction(data []byte) ([]searchengine.BulkAction, error) {
	evt, payload, err := parseMemberEvent(data)
	if err != nil {
		return nil, err
	}
	if payload.RoomID == "" {
		return nil, fmt.Errorf("build user-room action: missing roomId")
	}
	if len(payload.Accounts) == 0 {
		return nil, fmt.Errorf("build user-room action: empty accounts")
	}

	ts := evt.Timestamp
	roomID := payload.RoomID
	// Translate *int64 → painless-safe int64 (sentinel contract lives on addRoomScript).
	var hss int64
	if payload.HistorySharedSince != nil {
		hss = *payload.HistorySharedSince
	}
	actions := make([]searchengine.BulkAction, 0, len(payload.Accounts))
	for i, account := range payload.Accounts {
		if account == "" {
			return nil, fmt.Errorf("build user-room action: empty account at index %d", i)
		}
		// Bot accounts don't search; skip so they don't inflate the per-user access-control view.
		if model.IsBot(account) {
			continue
		}

		switch evt.Type {
		case model.InboxMemberAdded:
			actions = append(actions, searchengine.BulkAction{
				Action: searchengine.ActionUpdate,
				Index:  c.indexName,
				DocID:  account,
				Doc:    searchindex.BuildAddRoomUpdateBody(account, roomID, ts, hss),
			})
		case model.InboxMemberRemoved:
			actions = append(actions, searchengine.BulkAction{
				Action: searchengine.ActionUpdate,
				Index:  c.indexName,
				DocID:  account,
				Doc:    searchindex.BuildRemoveRoomUpdateBody(roomID, ts),
			})
		default:
			return nil, fmt.Errorf("build user-room action: unsupported event type %q", evt.Type)
		}
	}
	return actions, nil
}
