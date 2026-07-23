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
	return fmt.Sprintf("%s_template", searchindex.StripVersionBase(c.indexName))
}

func (c *spotlightCollection) TemplateBody() json.RawMessage {
	return spotlightTemplateBody(c.indexName, c.devMode)
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
		// Bots aren't searchable principals — never index them. Removals still fall
		// through to clean up a stale doc (idempotent 404 on a never-indexed doc — see isBulkItemSuccess).
		if (model.IsBot(account) || model.IsPlatformAdminAccount(account)) && evt.Type == model.InboxMemberAdded {
			continue
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

// SpotlightSearchIndex defines the Elasticsearch document structure for the
// spotlight index. One doc per (user, room) pair.
type SpotlightSearchIndex struct {
	UserAccount string    `json:"userAccount" es:"keyword"`
	RoomID      string    `json:"roomId"      es:"keyword"`
	RoomName    string    `json:"roomName"    es:"search_as_you_type,custom_analyzer"`
	RoomType    string    `json:"roomType"    es:"keyword"`
	SiteID      string    `json:"siteId"      es:"keyword"`
	JoinedAt    time.Time `json:"joinedAt"    es:"date"`
}

func newSpotlightSearchIndex(account string, evt *model.InboxMemberEvent) SpotlightSearchIndex {
	var joinedAt time.Time
	if evt.JoinedAt > 0 {
		joinedAt = time.UnixMilli(evt.JoinedAt).UTC()
	}
	return SpotlightSearchIndex{
		UserAccount: account,
		RoomID:      evt.RoomID,
		RoomName:    evt.RoomName,
		RoomType:    string(evt.RoomType),
		SiteID:      evt.SiteID,
		JoinedAt:    joinedAt,
	}
}

// spotlightTemplateBody builds the ES index template. The wildcard
// index_patterns lets a single template cover the current versioned
// index and any future reindex targets.
func spotlightTemplateBody(indexName string, devMode bool) json.RawMessage {
	shards := 3
	replicas := 1
	if devMode {
		shards = 1
		replicas = 0
	}
	tmpl := map[string]any{
		"index_patterns": []string{fmt.Sprintf("%s-*", searchindex.StripVersionBase(indexName))},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   shards,
					"number_of_replicas": replicas,
				},
				"analysis": map[string]any{
					"analyzer": map[string]any{
						"custom_analyzer": map[string]any{
							"type":      "custom",
							"tokenizer": "custom_tokenizer",
							"filter":    []string{"lowercase"},
						},
					},
					"tokenizer": map[string]any{
						// Whitespace tokenizer only supports max_token_length
						// (default 255). `token_chars` is valid on ngram /
						// edge_ngram tokenizers, not whitespace — sending it
						// here would reject the UpsertTemplate request.
						"custom_tokenizer": map[string]any{
							"type": "whitespace",
						},
					},
				},
			},
			"mappings": map[string]any{
				"dynamic":    false,
				"properties": esPropertiesFromStruct[SpotlightSearchIndex](),
			},
		},
	}
	// tmpl is built entirely from map/slice/string/int literals that are
	// always JSON-marshalable, so the error cannot occur in practice.
	data, _ := json.Marshal(tmpl)
	return data
}
