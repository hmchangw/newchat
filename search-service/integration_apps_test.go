//go:build integration

package main

// Integration tests for search.apps (Mongo + NATS; ES/Valkey stubbed).

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

type appsFixture struct {
	clientNATS *nats.Conn
	mongoDB    *mongo.Database
}

func setupAppsFixture(t *testing.T) *appsFixture {
	t.Helper()
	mongoDB := testutil.MongoDB(t, "search_service_test")
	h := newHandler(&fakeStore{}, newMongoStore(mongoDB), nil, newFakeCache(), handlerConfig{
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            365 * 24 * time.Hour,
		RequestTimeout:          5 * time.Second,
		SpotlightReadPattern:    "spotlight-*",
	})
	clientNATS := setupRouter(t, testQueueGroup, h.Register)
	return &appsFixture{clientNATS: clientNATS, mongoDB: mongoDB}
}

func TestIntegration_SearchApps_PrototypePipeline(t *testing.T) {
	f := setupAppsFixture(t)
	ctx := context.Background()

	_, err := f.mongoDB.Collection("apps").InsertMany(ctx, []any{
		map[string]any{"_id": "a1", "name": "Weather Alpha", "assistant": map[string]any{"enabled": true, "name": "weather.bot"}},
		map[string]any{"_id": "a2", "name": "Weatherly", "assistant": map[string]any{"enabled": false, "name": "weatherly.bot"}},
		map[string]any{"_id": "a3", "name": "Calendar"},
	})
	require.NoError(t, err)

	reqBytes, err := json.Marshal(model.SearchAppsRequest{Query: "weather"})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchApps("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var resp model.SearchAppsResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))

	require.Len(t, resp.Apps, 2, "two apps match the 'weather' regex")
	names := []string{resp.Apps[0].Name, resp.Apps[1].Name}
	assert.Contains(t, names, "Weather Alpha")
	assert.Contains(t, names, "Weatherly")
}

func TestIntegration_SearchApps_AssistantEnabledFilter(t *testing.T) {
	f := setupAppsFixture(t)
	ctx := context.Background()

	_, err := f.mongoDB.Collection("apps").InsertMany(ctx, []any{
		map[string]any{"_id": "a1", "name": "Weather Alpha", "assistant": map[string]any{"enabled": true, "name": "weather.bot"}},
		map[string]any{"_id": "a2", "name": "Weatherly", "assistant": map[string]any{"enabled": false, "name": "weatherly.bot"}},
	})
	require.NoError(t, err)

	enabled := true
	reqBytes, err := json.Marshal(model.SearchAppsRequest{
		Query:            "weather",
		AssistantEnabled: &enabled,
	})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchApps("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var resp model.SearchAppsResponse
	require.NoError(t, json.Unmarshal(msg.Data, &resp))

	require.Len(t, resp.Apps, 1)
	assert.Equal(t, "Weather Alpha", resp.Apps[0].Name)
}

func TestIntegration_SearchApps_EmptyQueryReturnsBadRequest(t *testing.T) {
	f := setupAppsFixture(t)

	reqBytes, err := json.Marshal(model.SearchAppsRequest{Query: ""})
	require.NoError(t, err)

	msg, err := f.clientNATS.Request(subject.SearchApps("alice"), reqBytes, 5*time.Second)
	require.NoError(t, err)

	var envelope model.ErrorResponse
	require.NoError(t, json.Unmarshal(msg.Data, &envelope))
	require.NotEmpty(t, envelope.Error)
	assert.Equal(t, natsrouter.CodeBadRequest, envelope.Code)
}
