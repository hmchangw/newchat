package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

// roomType filter values accepted on SearchRoomsRequest.RoomType.
const (
	roomTypeAll     = "all"
	roomTypeChannel = "channel"
	roomTypeDM      = "dm"
	roomTypeApp     = "app"
)

// buildRoomQuery composes the ES `_search` body for a subscription
// search against the spotlight index. memberRoomIDs, when non-nil, scopes
// results to that set (resolved upstream via subscription.getChannels for
// req.Members) — an empty non-nil slice means "no room matched all the
// requested members" and the query is built to match nothing.
// It returns a user-facing *errcode.Error on invalid/unsupported roomType
// values and a plain error on marshalling failures.
func buildRoomQuery(req model.SearchRoomsRequest, account string, memberRoomIDs []string) (json.RawMessage, error) {
	roomTypeFilter, rerr := roomTypeFilterClause(req.RoomType)
	if rerr != nil {
		return nil, rerr
	}

	filters := []any{
		map[string]any{"term": map[string]any{"userAccount": account}},
	}
	if roomTypeFilter != nil {
		filters = append(filters, roomTypeFilter)
	}
	if memberRoomIDs != nil {
		filters = append(filters, map[string]any{"terms": map[string]any{"roomId": memberRoomIDs}})
	}

	query := map[string]any{"filter": filters}
	if req.Query != "" {
		query["must"] = []any{
			map[string]any{
				"multi_match": map[string]any{
					"query":    req.Query,
					"type":     "bool_prefix",
					"operator": "AND",
					"fields":   []string{"roomName"},
				},
			},
		}
	}

	body := map[string]any{
		"from":             req.Offset,
		"size":             req.Size,
		"track_total_hits": true,
		"query":            map[string]any{"bool": query},
		"sort": []any{
			"_score",
			map[string]any{"joinedAt": map[string]any{"order": "desc"}},
		},
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal subscription query: %w", err)
	}
	return data, nil
}

// roomTypeFilterClause translates the request-level roomType into an ES term
// filter on `roomType`. The filter values match the strings written to the
// spotlight index by search-sync-worker (the model.RoomType values
// themselves). Returns (nil, nil) for "" and "all" which need no extra
// filter; returns errcode.BadRequest for "app" (MVP-unsupported) and any unknown
// value.
func roomTypeFilterClause(roomType string) (map[string]any, *errcode.Error) {
	switch roomType {
	case "", roomTypeAll:
		return nil, nil
	case roomTypeChannel:
		return map[string]any{"term": map[string]any{"roomType": string(model.RoomTypeChannel)}}, nil
	case roomTypeDM:
		return map[string]any{"term": map[string]any{"roomType": string(model.RoomTypeDM)}}, nil
	case roomTypeApp:
		return nil, errcode.BadRequest("invalid roomType: app is not supported")
	default:
		return nil, errcode.BadRequest(fmt.Sprintf("invalid roomType: %s", roomType))
	}
}
