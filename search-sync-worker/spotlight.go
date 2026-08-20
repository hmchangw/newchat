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

// MappingUpdate backfills the SpotlightDoc properties (incl. the new `origin`
// keyword) onto the already-created, non-rolled spotlight index — the template
// alone only covers new indices. Without this, `origin` lands in `_source` but
// stays unindexed on the existing index, so search-service's origin filter
// silently no-ops. Overrides the embedded inboxMemberCollection no-op.
func (c *spotlightCollection) MappingUpdate() (string, json.RawMessage) {
	// Error discarded: input is a static map of literals, marshal cannot fail.
	body, _ := json.Marshal(map[string]any{"properties": searchindex.EsPropertiesFromStruct[searchindex.SpotlightDoc]()})
	return c.indexName, body
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
		// A joinedAt refresh re-indexes the same doc with the corrected joinedAt;
		// the event carries roomName/roomType so the full re-index preserves them,
		// and its newer timestamp wins the version guard.
		case model.InboxMemberAdded, model.InboxMemberJoinedAtRefreshed:
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

// BuildByQuery handles room_renamed: roomName lives on every member's spotlight
// doc (`{account}_{roomId}`), and the rename payload carries no account list, so
// a single _update_by_query keyed on roomId re-indexes them all. Returns ok=false
// for member events, which fall through to BuildAction's bulk path.
func (c *spotlightCollection) BuildByQuery(data []byte) (string, json.RawMessage, bool, error) {
	var evt model.InboxEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return "", nil, false, fmt.Errorf("unmarshal inbox event: %w", err)
	}
	if evt.Type != model.InboxRoomRenamed {
		return "", nil, false, nil
	}
	var p model.RoomRenamedInboxPayload
	if err := json.Unmarshal(evt.Payload, &p); err != nil {
		return "", nil, false, fmt.Errorf("unmarshal room_renamed payload: %w", err)
	}
	if p.RoomID == "" {
		return "", nil, false, fmt.Errorf("build spotlight rename: missing roomId")
	}
	if p.NewName == "" {
		return "", nil, false, fmt.Errorf("build spotlight rename: missing newName")
	}
	if p.Timestamp <= 0 {
		return "", nil, false, fmt.Errorf("build spotlight rename: missing timestamp")
	}
	body, err := buildSpotlightRenameByQuery(p.RoomID, p.NewName, p.Timestamp)
	if err != nil {
		return "", nil, false, fmt.Errorf("build spotlight rename: %w", err)
	}
	return c.indexName, body, true, nil
}

// buildSpotlightRenameByQuery is the _update_by_query that sets roomName on every
// doc of the renamed room, guarded by roomNameUpdatedAt so an out-of-order rename
// can't overwrite a newer name: only a strictly-newer ts wins, and it stamps the
// clock it just advanced. A stale delivery is a no-op (updated:0), converging on
// last-write-wins regardless of arrival order.
//
// Residual (bounded, documented): a member added *concurrently* with a rename can
// have its doc created (from the member event's name snapshot) only after the
// rename's update-by-query already ran on the then-absent doc, so that one new
// member can see the pre-rename name until the next event touches their doc.
// Closing it fully needs query-time room-name resolution (a room doc + join),
// out of scope for this typeahead cache — spotlight is derived, not source of truth.
func buildSpotlightRenameByQuery(roomID, newName string, ts int64) (json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{
		"query": map[string]any{"term": map[string]any{"roomId": roomID}},
		"script": map[string]any{
			"lang": "painless",
			// >= (not >) so two renames sharing a millisecond both apply (last
			// processed wins the tie) instead of dropping the second, and a
			// redelivery of the same rename re-applies its own name idempotently.
			"source": "long stored = ctx._source.roomNameUpdatedAt == null ? 0L : ((Number)ctx._source.roomNameUpdatedAt).longValue(); " +
				"if (params.ts >= stored) { ctx._source.roomName = params.name; ctx._source.roomNameUpdatedAt = params.ts; } else { ctx.op = 'noop'; }",
			"params": map[string]any{"name": newName, "ts": ts},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal update-by-query: %w", err)
	}
	return body, nil
}

func newSpotlightSearchIndex(account string, evt *model.InboxMemberEvent) searchindex.SpotlightDoc {
	return searchindex.NewSpotlightDoc(searchindex.SpotlightFields{
		UserAccount: account,
		RoomID:      evt.RoomID,
		RoomName:    evt.RoomName,
		RoomType:    string(evt.RoomType),
		SiteID:      evt.SiteID,
		JoinedAt:    convertJoinedAt(evt.JoinedAt),
		// Stamp the name's LWW clock so a later rename (higher ts) wins and this
		// doc's own name can't be reverted by an older rename delivered late.
		RoomNameUpdatedAt: evt.Timestamp,
		Origin:            evt.Origin,
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
