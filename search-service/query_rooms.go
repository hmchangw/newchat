package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// roomType filter values accepted on SearchRoomsRequest.RoomType.
const (
	roomTypeAll     = "all"
	roomTypeChannel = "channel"
	roomTypeDM      = "dm"
	roomTypeApp     = "app"
)

// buildRoomQuery composes the ES `_search` body for a subscription
// search against the spotlight index. It returns a *natsrouter.RouteError
// (user-facing) on invalid/unsupported roomType values and a plain error on
// marshalling failures.
func buildRoomQuery(req model.SearchRoomsRequest, account string) (json.RawMessage, error) {
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

	body := map[string]any{
		"from":             req.Offset,
		"size":             req.Size,
		"track_total_hits": true,
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{
						"multi_match": map[string]any{
							"query":    req.Query,
							"type":     "bool_prefix",
							"operator": "AND",
							"fields":   []string{"roomName"},
						},
					},
				},
				"filter": filters,
			},
		},
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
// filter; returns ErrBadRequest for "app" (MVP-unsupported) and any unknown
// value.
func roomTypeFilterClause(roomType string) (map[string]any, *natsrouter.RouteError) {
	switch roomType {
	case "", roomTypeAll:
		return nil, nil
	case roomTypeChannel:
		return map[string]any{"term": map[string]any{"roomType": string(model.RoomTypeChannel)}}, nil
	case roomTypeDM:
		return map[string]any{"term": map[string]any{"roomType": string(model.RoomTypeDM)}}, nil
	case roomTypeApp:
		return nil, natsrouter.ErrBadRequest("invalid roomType: app is not supported")
	default:
		return nil, natsrouter.ErrBadRequest(fmt.Sprintf("invalid roomType: %s", roomType))
	}
}
