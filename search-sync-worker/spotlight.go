package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

// spotlightCollection implements Collection for spotlight room-typeahead
// search. Documents are per (user, room) pair — one doc for every account
// that holds a subscription to a given room — so the search service can
// filter by userAccount and match on roomName. Doc IDs are synthesized as
// `{account}_{roomID}` since the INBOX payload doesn't carry subscription IDs.
type spotlightCollection struct {
	inboxMemberCollection
	indexName string
	devMode   bool
}

func newSpotlightCollection(indexName string, devMode bool) *spotlightCollection {
	return &spotlightCollection{indexName: indexName, devMode: devMode}
}

func (c *spotlightCollection) ConsumerName() string {
	return "spotlight-sync"
}

func (c *spotlightCollection) TemplateName() string {
	return searchindex.SpotlightTemplateName(c.indexName)
}

func (c *spotlightCollection) TemplateBody() json.RawMessage {
	return searchindex.SpotlightTemplateBody(c.indexName, c.devMode)
}

// BuildAction fans a member_added / member_removed event out into one ES
// action per account in the payload. Bulk invites produce N spotlight docs
// from a single event; single-user invites produce one.
//
// All actions in the returned slice carry the same external Version
// (evt.Timestamp) because they all represent the same logical event — if the
// event is redelivered, every action 409s uniformly and is treated as a
// successful idempotent replay.
//
// Restricted rooms are indexed the same as unrestricted rooms. Spotlight
// is a room-name typeahead over rooms the user belongs to — the HSS /
// restricted-rooms distinction is a MESSAGE-content access-control
// concern, enforced at query time by search-service's Clauses A/B against
// the messages index. Room-name discovery has no such boundary: a user
// who joined a restricted room must still be able to find it by name.
func (c *spotlightCollection) BuildAction(data []byte) ([]searchengine.BulkAction, error) {
	evt, payload, err := parseMemberEvent(data)
	if err != nil {
		return nil, err
	}
	if payload.RoomID == "" {
		return nil, fmt.Errorf("build spotlight action: missing roomId")
	}
	if len(payload.Accounts) == 0 {
		return nil, fmt.Errorf("build spotlight action: empty accounts")
	}

	actions := make([]searchengine.BulkAction, 0, len(payload.Accounts))
	for i, account := range payload.Accounts {
		if account == "" {
			return nil, fmt.Errorf("build spotlight action: empty account at index %d", i)
		}
		docID := fmt.Sprintf("%s_%s", account, payload.RoomID)

		switch evt.Type {
		case model.InboxMemberAdded:
			doc := newSpotlightSearchIndex(account, payload)
			body, err := json.Marshal(doc)
			if err != nil {
				return nil, fmt.Errorf("marshal spotlight doc: %w", err)
			}
			actions = append(actions, searchengine.BulkAction{
				Action:  searchengine.ActionIndex,
				Index:   c.indexName,
				DocID:   docID,
				Version: evt.Timestamp,
				Doc:     body,
			})
		case model.InboxMemberRemoved:
			actions = append(actions, searchengine.BulkAction{
				Action:  searchengine.ActionDelete,
				Index:   c.indexName,
				DocID:   docID,
				Version: evt.Timestamp,
			})
		default:
			return nil, fmt.Errorf("build spotlight action: unsupported event type %q", evt.Type)
		}
	}
	return actions, nil
}

func newSpotlightSearchIndex(account string, evt *model.InboxMemberEvent) searchindex.SpotlightDoc {
	var joinedAt int64
	if evt.JoinedAt > 0 {
		joinedAt = evt.JoinedAt
	}
	return searchindex.NewSpotlightDoc(searchindex.SpotlightFields{
		UserAccount: account,
		RoomID:      evt.RoomID,
		RoomName:    evt.RoomName,
		RoomType:    string(evt.RoomType),
		SiteID:      evt.SiteID,
		JoinedAt:    convertJoinedAt(joinedAt),
	})
}

// convertJoinedAt converts a Unix millisecond timestamp to a UTC time.Time,
// or returns a zero time.Time if the timestamp is 0.
func convertJoinedAt(joinedAtMs int64) time.Time {
	if joinedAtMs > 0 {
		return time.UnixMilli(joinedAtMs).UTC()
	}
	return time.Time{}
}
