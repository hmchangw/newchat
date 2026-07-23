package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
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
	return fmt.Sprintf("%s_template", c.indexName)
}

func (c *userRoomCollection) TemplateBody() json.RawMessage {
	return userRoomTemplateBody(c.indexName)
}

// StoredScripts registers the add/remove painless scripts as ES stored scripts; BuildAction
// references them by id so fan-out member updates don't repeat the full source per action.
func (c *userRoomCollection) StoredScripts() map[string]json.RawMessage {
	return map[string]json.RawMessage{
		addRoomScriptID:    storedScriptBody(addRoomScript),
		removeRoomScriptID: storedScriptBody(removeRoomScript),
	}
}

// storedScriptBody wraps a painless source string in the `PUT /_scripts/{id}` request envelope ES expects.
func storedScriptBody(source string) json.RawMessage {
	body := map[string]any{
		"script": map[string]any{
			"lang":   "painless",
			"source": source,
		},
	}
	// Inputs are literal string/map values that always marshal cleanly.
	data, _ := json.Marshal(body)
	return data
}

// addRoomScriptID / removeRoomScriptID are the ES stored-script ids for addRoomScript/removeRoomScript,
// referenced instead of inlining ~600 bytes per action; bump the suffix if a script's source changes incompatibly during a rolling deploy.
const (
	addRoomScriptID    = "search-sync-user-room-add-v1"
	removeRoomScriptID = "search-sync-user-room-remove-v1"
)

// addRoomScript/removeRoomScript apply application-level LWW via params.ts, skipping stale writes
// via ctx.op='none'. addRoomScript also routes by params.hss (>0 → restrictedRooms{}, <=0 → rooms[]) — Go passes hss=0 for nil HistorySharedSince, so publishers MUST emit nil, never &0, on the wire.
const (
	addRoomScript = `if (ctx._source.roomTimestamps == null) { ctx._source.roomTimestamps = [:]; } ` +
		`if (ctx._source.rooms == null) { ctx._source.rooms = []; } ` +
		`if (ctx._source.restrictedRooms == null) { ctx._source.restrictedRooms = [:]; } ` +
		`long stored = ctx._source.roomTimestamps.containsKey(params.rid) ` +
		`? ((Number)ctx._source.roomTimestamps.get(params.rid)).longValue() : 0L; ` +
		`if (params.ts > stored) { ` +
		`if (params.hss > 0) { ` +
		`ctx._source.restrictedRooms[params.rid] = params.hss; ` +
		`ctx._source.rooms.removeIf(r -> r == params.rid); ` +
		`} else { ` +
		`if (!ctx._source.rooms.contains(params.rid)) { ctx._source.rooms.add(params.rid); } ` +
		`ctx._source.restrictedRooms.remove(params.rid); ` +
		`} ` +
		`ctx._source.roomTimestamps.put(params.rid, params.ts); ` +
		`ctx._source.updatedAt = params.now; ` +
		`} else { ctx.op = 'none'; }`

	removeRoomScript = `if (ctx._source.roomTimestamps == null) { ctx._source.roomTimestamps = [:]; } ` +
		`long stored = ctx._source.roomTimestamps.containsKey(params.rid) ` +
		`? ((Number)ctx._source.roomTimestamps.get(params.rid)).longValue() : 0L; ` +
		`if (params.ts > stored) { ` +
		`if (ctx._source.rooms != null) { ` +
		`int idx = ctx._source.rooms.indexOf(params.rid); ` +
		`if (idx >= 0) { ctx._source.rooms.remove(idx); } } ` +
		`if (ctx._source.restrictedRooms != null) { ctx._source.restrictedRooms.remove(params.rid); } ` +
		`ctx._source.roomTimestamps.put(params.rid, params.ts); ` +
		`} else { ctx.op = 'none'; }`
)

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
		// Bots are channel members but not searchable principals — never index
		// them. Removals still fall through: a user-room doc indexed by legacy
		// behavior (or during a rolling deploy) must be cleaned up, and the
		// remove-path update is idempotent (404 document_missing_exception on a
		// never-indexed doc is a benign ack — see isBulkItemSuccess).
		if (model.IsBot(account) || model.IsPlatformAdminAccount(account)) && evt.Type == model.InboxMemberAdded {
			continue
		}

		switch evt.Type {
		case model.InboxMemberAdded:
			body, err := buildAddRoomUpdateBody(account, roomID, ts, hss)
			if err != nil {
				return nil, err
			}
			actions = append(actions, searchengine.BulkAction{
				Action: searchengine.ActionUpdate,
				Index:  c.indexName,
				DocID:  account,
				Doc:    body,
			})
		case model.InboxMemberRemoved:
			body, err := buildRemoveRoomUpdateBody(roomID, ts)
			if err != nil {
				return nil, err
			}
			actions = append(actions, searchengine.BulkAction{
				Action: searchengine.ActionUpdate,
				Index:  c.indexName,
				DocID:  account,
				Doc:    body,
			})
		default:
			return nil, fmt.Errorf("build user-room action: unsupported event type %q", evt.Type)
		}
	}
	return actions, nil
}

// userRoomUpsertDoc is the full document inserted on a user's first room (no prior user-room
// entry). Rooms holds unrestricted IDs; RestrictedRooms maps rid→historySharedSince; RoomTimestamps seeds the LWW guard.
type userRoomUpsertDoc struct {
	UserAccount     string           `json:"userAccount"`
	Rooms           []string         `json:"rooms"`
	RestrictedRooms map[string]int64 `json:"restrictedRooms"`
	RoomTimestamps  map[string]int64 `json:"roomTimestamps"`
	CreatedAt       string           `json:"createdAt"`
	UpdatedAt       string           `json:"updatedAt"`
}

func buildAddRoomUpdateBody(account, roomID string, ts, hss int64) (json.RawMessage, error) {
	now := time.UnixMilli(ts).UTC().Format(time.RFC3339Nano)

	// Seed the upsert document so the first-insert shape matches the painless-updated shape:
	// restricted rooms go straight into restrictedRooms{}, unrestricted into rooms[].
	upsert := userRoomUpsertDoc{
		UserAccount:     account,
		Rooms:           []string{},
		RestrictedRooms: map[string]int64{},
		RoomTimestamps:  map[string]int64{roomID: ts},
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if hss > 0 {
		upsert.RestrictedRooms[roomID] = hss
	} else {
		upsert.Rooms = []string{roomID}
	}

	body := map[string]any{
		"script": map[string]any{
			"id": addRoomScriptID,
			"params": map[string]any{
				"rid": roomID,
				"ts":  ts,
				"hss": hss,
				"now": now,
			},
		},
		"upsert": upsert,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal add room update: %w", err)
	}
	return data, nil
}

func buildRemoveRoomUpdateBody(roomID string, ts int64) (json.RawMessage, error) {
	body := map[string]any{
		"script": map[string]any{
			"id": removeRoomScriptID,
			"params": map[string]any{
				"rid": roomID,
				"ts":  ts,
			},
		},
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal remove room update: %w", err)
	}
	return data, nil
}

// userRoomTemplateBody builds the ES index template for user-room; index_patterns is the exact
// configured index name so a custom USER_ROOM_INDEX still maps correctly, and roomTimestamps is `flattened` to avoid per-key mapping bloat.
func userRoomTemplateBody(indexName string) json.RawMessage {
	tmpl := map[string]any{
		"index_patterns": []string{indexName},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   1,
					"number_of_replicas": 1,
				},
			},
			"mappings": map[string]any{
				"dynamic": false,
				"properties": map[string]any{
					"userAccount": map[string]any{"type": "keyword"},
					"rooms": map[string]any{
						"type": "text",
						"fields": map[string]any{
							"keyword": map[string]any{"type": "keyword", "ignore_above": 256},
						},
					},
					// restrictedRooms is a rid→historySharedSince map; `flattened` keeps the mapping stable regardless of rid count — same approach as roomTimestamps.
					"restrictedRooms": map[string]any{"type": "flattened"},
					"roomTimestamps":  map[string]any{"type": "flattened"},
					"createdAt":       map[string]any{"type": "date"},
					"updatedAt":       map[string]any{"type": "date"},
				},
			},
		},
	}
	data, _ := json.Marshal(tmpl)
	return data
}
