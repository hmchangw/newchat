package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// subscriptionFilters extracts the filter array from the ES bool query.
func subscriptionFilters(t *testing.T, q map[string]any) []any {
	t.Helper()
	return q["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
}

func TestBuildSubscriptionQuery_RoomTypeAll(t *testing.T) {
	req := model.SearchRoomsRequest{Query: "general", Size: 10, Offset: 0}
	raw, err := buildRoomQuery(req, "alice")
	require.NoError(t, err)

	q := parseQuery(t, raw)
	assert.Equal(t, float64(0), q["from"])
	assert.Equal(t, float64(10), q["size"])
	assert.Equal(t, true, q["track_total_hits"])

	filters := subscriptionFilters(t, q)
	require.Len(t, filters, 1)
	account := filters[0].(map[string]any)["term"].(map[string]any)["userAccount"]
	assert.Equal(t, "alice", account)
}

func TestBuildSubscriptionQuery_RoomTypeExplicitAll(t *testing.T) {
	req := model.SearchRoomsRequest{Query: "general", RoomType: "all"}
	raw, err := buildRoomQuery(req, "alice")
	require.NoError(t, err)

	filters := subscriptionFilters(t, parseQuery(t, raw))
	require.Len(t, filters, 1) // only userAccount, no roomType filter
}

func TestBuildSubscriptionQuery_RoomTypeChannel(t *testing.T) {
	req := model.SearchRoomsRequest{Query: "general", RoomType: "channel"}
	raw, err := buildRoomQuery(req, "alice")
	require.NoError(t, err)

	filters := subscriptionFilters(t, parseQuery(t, raw))
	require.Len(t, filters, 2)
	roomType := filters[1].(map[string]any)["term"].(map[string]any)["roomType"]
	assert.Equal(t, string(model.RoomTypeChannel), roomType)
}

func TestBuildSubscriptionQuery_RoomTypeDM(t *testing.T) {
	req := model.SearchRoomsRequest{Query: "alice", RoomType: "dm"}
	raw, err := buildRoomQuery(req, "alice")
	require.NoError(t, err)

	filters := subscriptionFilters(t, parseQuery(t, raw))
	require.Len(t, filters, 2)
	roomType := filters[1].(map[string]any)["term"].(map[string]any)["roomType"]
	assert.Equal(t, string(model.RoomTypeDM), roomType)
}

func TestBuildSubscriptionQuery_RoomTypeAppRejected(t *testing.T) {
	req := model.SearchRoomsRequest{Query: "bot", RoomType: "app"}
	_, err := buildRoomQuery(req, "alice")
	require.Error(t, err)

	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr), "expected RouteError")
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
	assert.Contains(t, rerr.Message, "invalid roomType")
}

func TestBuildSubscriptionQuery_UnknownRoomTypeRejected(t *testing.T) {
	req := model.SearchRoomsRequest{Query: "x", RoomType: "orb"}
	_, err := buildRoomQuery(req, "alice")
	require.Error(t, err)

	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
	assert.Contains(t, rerr.Message, "invalid roomType")
}

func TestBuildSubscriptionQuery_SortByScoreThenJoinedAtDesc(t *testing.T) {
	req := model.SearchRoomsRequest{Query: "x"}
	raw, err := buildRoomQuery(req, "alice")
	require.NoError(t, err)

	sort := parseQuery(t, raw)["sort"].([]any)
	require.Len(t, sort, 2)
	assert.Equal(t, "_score", sort[0])
	joinedAt := sort[1].(map[string]any)["joinedAt"].(map[string]any)
	assert.Equal(t, "desc", joinedAt["order"])
}

func TestBuildSubscriptionQuery_QueryFieldFlowsToESBody(t *testing.T) {
	req := model.SearchRoomsRequest{Query: "engineering", Size: 5}
	raw, err := buildRoomQuery(req, "alice")
	require.NoError(t, err)

	q := parseQuery(t, raw)
	must := q["query"].(map[string]any)["bool"].(map[string]any)["must"].([]any)
	require.Len(t, must, 1)
	mm := must[0].(map[string]any)["multi_match"].(map[string]any)
	assert.Equal(t, "engineering", mm["query"], "Query field value must be passed as the multi_match query")
}
