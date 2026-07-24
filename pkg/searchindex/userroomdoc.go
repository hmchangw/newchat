package searchindex

import (
	"encoding/json"
	"time"
)

// AddRoomScriptID / RemoveRoomScriptID are the ES stored-script ids under
// which AddRoomScript / RemoveRoomScript are registered. Bulk member
// updates reference these ids instead of inlining the full source per
// action. If a script's source ever changes incompatibly during a rolling
// deploy, bump the id suffix so old and new pods don't share a single
// mutated definition.
const (
	AddRoomScriptID    = "search-sync-user-room-add-v1"
	RemoveRoomScriptID = "search-sync-user-room-remove-v1"
)

// AddRoomScript / RemoveRoomScript implement application-level last-write-wins
// on (user, room) using params.ts (an event timestamp in millis). A stale
// call short-circuits via ctx.op = 'none', which tells ES to skip the write
// entirely — no version bump, no disk I/O.
//
// AddRoomScript additionally routes by params.hss:
//   - hss > 0  → rid lives in restrictedRooms{rid: hss} and is removed from rooms[]
//   - hss <= 0 → rid lives in rooms[] and is removed from restrictedRooms{}
//
// Painless lacks nullable primitives in script params: callers MUST pass
// hss = 0 for an unrestricted room, never a nil-shaped sentinel — a caller
// that ever passes hss for a room with no restriction must pass literal 0.
const (
	AddRoomScript = `if (ctx._source.roomTimestamps == null) { ctx._source.roomTimestamps = [:]; } ` +
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

	RemoveRoomScript = `if (ctx._source.roomTimestamps == null) { ctx._source.roomTimestamps = [:]; } ` +
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

// UserRoomUpsertDoc is the full document inserted when the user has no
// prior user-room entry (the first time a room is added for this user).
// Rooms holds unrestricted room IDs; RestrictedRooms maps rid ->
// historySharedSince (millis) for rooms joined with a history restriction.
// RoomTimestamps seeds the per-room LWW timestamp guard used uniformly by
// both scripts.
type UserRoomUpsertDoc struct {
	UserAccount     string           `json:"userAccount"`
	Rooms           []string         `json:"rooms"`
	RestrictedRooms map[string]int64 `json:"restrictedRooms"`
	RoomTimestamps  map[string]int64 `json:"roomTimestamps"`
	CreatedAt       string           `json:"createdAt"`
	UpdatedAt       string           `json:"updatedAt"`
}

// StoredScriptBody wraps a Painless source string in the
// `PUT /_scripts/{id}` request envelope ES expects.
func StoredScriptBody(source string) json.RawMessage {
	body, _ := json.Marshal(map[string]any{
		"script": map[string]string{"lang": "painless", "source": source},
	})
	return body
}

// BuildAddRoomUpdateBody builds the ActionUpdate body for adding rid to a
// user's user-room doc at timestamp ts (millis), with hss the room's
// HistorySharedSince in millis (0 for unrestricted). The upsert seed makes
// the first-insert document shape match what the script itself would
// produce on a subsequent update.
func BuildAddRoomUpdateBody(account, rid string, ts, hss int64) json.RawMessage {
	now := time.UnixMilli(ts).UTC().Format(time.RFC3339Nano)
	upsert := UserRoomUpsertDoc{
		UserAccount:     account,
		Rooms:           []string{},
		RestrictedRooms: map[string]int64{},
		RoomTimestamps:  map[string]int64{rid: ts},
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if hss > 0 {
		upsert.RestrictedRooms[rid] = hss
	} else {
		upsert.Rooms = []string{rid}
	}

	body, _ := json.Marshal(map[string]any{
		"script": map[string]any{
			"id":     AddRoomScriptID,
			"params": map[string]any{"rid": rid, "ts": ts, "hss": hss, "now": now},
		},
		"upsert": upsert,
	})
	return body
}

// BuildRemoveRoomUpdateBody builds the ActionUpdate body for removing rid
// from a user's user-room doc at timestamp ts (millis). No upsert seed —
// removing from a nonexistent doc is the document_missing_exception 404
// case searchengine.IsBulkItemSuccess treats as benign, matching how
// search-sync-worker's own adapter calls this same script.
func BuildRemoveRoomUpdateBody(rid string, ts int64) json.RawMessage {
	body, _ := json.Marshal(map[string]any{
		"script": map[string]any{
			"id":     RemoveRoomScriptID,
			"params": map[string]any{"rid": rid, "ts": ts},
		},
	})
	return body
}
