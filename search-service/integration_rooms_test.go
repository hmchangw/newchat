//go:build integration

package main

// Integration tests for search.rooms (real ES + shared NATS + Valkey).

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Per-test spotlight index against shared ES.
type roomsFixture struct {
	clientNATS     *nats.Conn
	esURL          string
	spotlightIndex string
}

func setupRoomsFixture(t *testing.T) *roomsFixture {
	t.Helper()
	esURL := testutil.Elasticsearch(t)
	spotlightIndex := testutil.ElasticsearchIndex(t, "spotlight")
	putTestSpotlightIndex(t, esURL, spotlightIndex)

	engine, err := searchengine.New(context.Background(), searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err, "build searchengine for subs fixture")

	cache := newValkeyCache(valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t)))
	t.Cleanup(func() { testutil.FlushValkey(t) })
	h := newHandler(newESStore(engine, testUserRoomIndex), nil, nil, cache, handlerConfig{
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            365 * 24 * time.Hour,
		RequestTimeout:          5 * time.Second,
		SpotlightReadPattern:    spotlightIndex,
	})
	clientNC := setupRouter(t, testQueueGroupSubs, h.Register)
	return &roomsFixture{clientNATS: clientNC, esURL: esURL, spotlightIndex: spotlightIndex}
}

func putTestSpotlightIndex(t *testing.T, esURL, index string) {
	t.Helper()
	body := map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
			"refresh_interval":   "1s",
		},
		"mappings": map[string]any{
			"dynamic": false,
			"properties": map[string]any{
				"roomId": map[string]any{"type": "keyword"},
				"roomName": map[string]any{
					"type": "search_as_you_type",
				},
				"roomType":    map[string]any{"type": "keyword"},
				"userAccount": map[string]any{"type": "keyword"},
				"siteId":      map[string]any{"type": "keyword"},
				"joinedAt":    map[string]any{"type": "date"},
			},
		},
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPut, esURL+"/"+index, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := testHTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	require.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated,
		"create spotlight index: status=%d body=%s", resp.StatusCode, b)
}

func TestIntegration_SearchRooms_HappyPath(t *testing.T) {
	f := setupRoomsFixture(t)

	const account = "alice"
	now := time.Now().UTC()

	seedDoc(t, f.esURL, f.spotlightIndex, "spot-r1", map[string]any{
		"roomId":      "r1",
		"roomName":    "engineering-announcements",
		"roomType":    "channel",
		"userAccount": account,
		"siteId":      "site-local",
		"joinedAt":    now.Add(-48 * time.Hour).Format(time.RFC3339),
	})
	seedDoc(t, f.esURL, f.spotlightIndex, "spot-r2", map[string]any{
		"roomId":      "r2",
		"roomName":    "engineering-random",
		"roomType":    "channel",
		"userAccount": account,
		"siteId":      "site-local",
		"joinedAt":    now.Add(-24 * time.Hour).Format(time.RFC3339),
	})
	// A matching room owned by a different account. With the Mongo
	// hydration removed, the spotlight userAccount term filter is the
	// sole access boundary — this must not leak into alice's results.
	seedDoc(t, f.esURL, f.spotlightIndex, "spot-r3", map[string]any{
		"roomId":      "r3",
		"roomName":    "engineering-secret",
		"roomType":    "channel",
		"userAccount": "mallory",
		"siteId":      "site-local",
		"joinedAt":    now.Add(-12 * time.Hour).Format(time.RFC3339),
	})

	reqBytes, err := json.Marshal(model.SearchRoomsRequest{Query: "engineering"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchRooms(account), reqBytes, 10*time.Second)
	require.NoError(t, err)

	var resp model.SearchRoomsResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))

	require.Len(t, resp.Rooms, 2, "both rooms matching 'engineering' must be returned")
	byID := map[string]model.SearchRoom{}
	for _, r := range resp.Rooms {
		byID[r.RoomID] = r
	}
	assert.Equal(t, model.SearchRoom{RoomID: "r1", Name: "engineering-announcements", RoomType: "channel", SiteID: "site-local"}, byID["r1"])
	assert.Equal(t, model.SearchRoom{RoomID: "r2", Name: "engineering-random", RoomType: "channel", SiteID: "site-local"}, byID["r2"])
	_, leaked := byID["r3"]
	assert.False(t, leaked, "rooms owned by another account must not leak")
}

func TestIntegration_SearchRooms_RoomTypeChannelFilter(t *testing.T) {
	f := setupRoomsFixture(t)

	const account = "bob"
	now := time.Now().UTC()

	seedDoc(t, f.esURL, f.spotlightIndex, "spot-b-r1", map[string]any{
		"roomId":      "b-r1",
		"roomName":    "bob-alice",
		"roomType":    "dm",
		"userAccount": account,
		"siteId":      "site-local",
		"joinedAt":    now.Add(-1 * time.Hour).Format(time.RFC3339),
	})
	seedDoc(t, f.esURL, f.spotlightIndex, "spot-b-r2", map[string]any{
		"roomId":      "b-r2",
		"roomName":    "bob-channel",
		"roomType":    "channel",
		"userAccount": account,
		"siteId":      "site-local",
		"joinedAt":    now.Add(-2 * time.Hour).Format(time.RFC3339),
	})

	reqBytes, err := json.Marshal(model.SearchRoomsRequest{Query: "bob", RoomType: "channel"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchRooms(account), reqBytes, 10*time.Second)
	require.NoError(t, err)

	var resp model.SearchRoomsResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))

	require.Len(t, resp.Rooms, 1)
	assert.Equal(t, model.SearchRoom{RoomID: "b-r2", Name: "bob-channel", RoomType: "channel", SiteID: "site-local"}, resp.Rooms[0],
		"only the channel room must match roomType=channel filter")
}

func TestIntegration_SearchRooms_EmptyQueryReturnsBadRequest(t *testing.T) {
	f := setupRoomsFixture(t)

	reqBytes, err := json.Marshal(model.SearchRoomsRequest{Query: ""})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchRooms("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var envelope model.ErrorResponse
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	require.NotEmpty(t, envelope.Error)
	assert.Equal(t, natsrouter.CodeBadRequest, envelope.Code)
}

func TestIntegration_SearchRooms_RoomTypeAppReturnsBadRequest(t *testing.T) {
	f := setupRoomsFixture(t)

	reqBytes, err := json.Marshal(model.SearchRoomsRequest{Query: "x", RoomType: "app"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchRooms("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var envelope model.ErrorResponse
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	require.NotEmpty(t, envelope.Error)
	assert.Equal(t, natsrouter.CodeBadRequest, envelope.Code)
	assert.Contains(t, envelope.Error, "invalid roomType")
}
