package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// userRoomCollection implements Collection for the user-room access-control
// index. It maintains a per-user rooms array (one doc per user) used by the
// search service as a terms filter on message search queries.
//
// Concurrency model: the collection is safe to run with multiple search-sync
// worker pods sharing the same durable consumer. Out-of-order delivery of
// (added, removed) pairs for the same (user, room) is handled by an
// application-level timestamp guard inside the painless scripts — each update
// carries the InboxEvent timestamp in `params.ts` and compares against the
// per-room stored timestamp in `ctx._source.roomTimestamps`, skipping the
// write (via `ctx.op = 'none'`) if the incoming event is stale. Concurrent
// writers from different pods serialize on the primary shard's per-doc lock,
// so the guard converges on last-write-wins-by-event-timestamp regardless of
// physical arrival order.
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

// StoredScripts registers the add/remove painless scripts as ES stored
// scripts. BuildAction references them by id so fan-out member updates don't
// repeat the full source per action.
func (c *userRoomCollection) StoredScripts() map[string]json.RawMessage {
	return map[string]json.RawMessage{
		addRoomScriptID:    storedScriptBody(addRoomScript),
		removeRoomScriptID: storedScriptBody(removeRoomScript),
	}
}

// storedScriptBody wraps a painless source string in the `PUT /_scripts/{id}`
// request envelope ES expects.
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

// addRoomScript / removeRoomScript implement application-level last-write-wins
// on (user, room) using `params.ts` (InboxEvent.Timestamp in millis). Stale
// events short-circuit via `ctx.op = 'none'` which tells ES to skip the write
// entirely — no version bump, no disk I/O.
//
// addRoomScript additionally routes by `params.hss`:
//   - hss > 0 → rid lives in `restrictedRooms{rid: hss}` and is removed from `rooms[]`
//   - hss <= 0 → rid lives in `rooms[]` and is removed from `restrictedRooms{}`
//
// This makes admin-driven restriction transitions atomic inside a single
// update: the rid always ends up in exactly one of the two slots.
//
// Painless lacks nullable primitives in script params, so the Go side passes
// `hss = 0` for unrestricted and `hss = *event.HistorySharedSince` otherwise.
// The Go↔painless contract: publishers MUST emit nil for unrestricted rooms
// on the wire — a `&0` is treated as unrestricted by this script and would be
// a silent contract violation.
// addRoomScriptID / removeRoomScriptID are the ES stored-script ids under
// which addRoomScript / removeRoomScript are registered at startup. Bulk
// member updates reference these ids instead of inlining the ~600-byte
// source per action, so an N-account fan-out ships one id reference per
// action rather than N copies of the script body. If a script's source ever
// changes incompatibly during a rolling deploy, bump the id suffix so old and
// new pods don't share a single mutated definition.
const (
	addRoomScriptID    = "search-sync-user-room-add-v1"
	removeRoomScriptID = "search-sync-user-room-remove-v1"
)

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

// BuildAction fans a member_added / member_removed event out into one ES
// update per account in the payload. Bulk invites produce N distinct
// user-room doc updates from a single event (each account touches a
// different user's doc, keyed by account).
//
// Restricted rooms (HistorySharedSince != nil on the event) are routed into
// `restrictedRooms{}` on the user-room doc — the search service reads both
// `rooms[]` and `restrictedRooms{}` directly from ES at query time.
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

// userRoomUpsertDoc is the full document inserted when the user has no prior
// user-room entry (i.e., the first time a room is added for this user).
//
// Rooms holds unrestricted room IDs; RestrictedRooms maps rid →
// historySharedSince (millis) for rooms the user joined with a history
// restriction. RoomTimestamps seeds the per-room LWW timestamp guard used
// uniformly across both paths.
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

	// Seed the upsert document so the first-insert shape matches the
	// painless-updated shape: restricted rooms go straight into
	// restrictedRooms{}, unrestricted rooms go straight into rooms[].
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

// userRoomTemplateBody builds the ES index template for the user-room
// collection. The `index_patterns` field is set to the exact configured
// index name so a custom USER_ROOM_INDEX value still receives the correct
// mapping. The `roomTimestamps` field is mapped as `flattened` so new
// roomIds don't balloon the mapping with per-key dynamic sub-fields.
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
					// restrictedRooms is a rid → historySharedSince (millis)
					// map. `flattened` keeps the mapping stable regardless of
					// how many restricted rids show up — same approach as
					// roomTimestamps.
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
