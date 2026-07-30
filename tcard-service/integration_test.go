//go:build integration

package main

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestMongoCardStore_ListCards(t *testing.T) {
	db := testutil.MongoDB(t, "tcard")
	store := newMongoCardStore(db)
	ctx := context.Background()

	// Schemaless template docs round-trip verbatim (minus _id and path); a doc
	// missing a string path or _tcardVersion is skipped, not fatal.
	_, err := db.Collection("cards").InsertMany(ctx, []any{
		bson.M{
			"_id": "c-home", "path": "home", "_tcardVersion": "v1",
			"title": "Home",
			"layout": bson.M{
				"columns": 2,
				"widgets": bson.A{
					bson.M{"kind": "news", "limit": 5},
					bson.M{"kind": "weather", "unit": "celsius"},
				},
			},
			"enabled": true,
		},
		bson.M{"_id": "c-profile", "path": "profile", "_tcardVersion": "v2", "title": "Profile"},
		bson.M{"_id": "c-no-path", "_tcardVersion": "v1", "title": "orphan template"},
		bson.M{"_id": "c-bad-path", "path": 42, "_tcardVersion": "v1", "title": "numeric path"},
		bson.M{"_id": "c-no-version", "path": "settings", "title": "no version"},
	})
	require.NoError(t, err)

	cards, err := store.ListCards(ctx)
	require.NoError(t, err)
	require.Len(t, cards, 2, "docs missing a string path or _tcardVersion are skipped")

	byKey := make(map[string]card, len(cards))
	for _, c := range cards {
		byKey[c.Path+"@"+c.CardVersion] = c
	}

	home, ok := byKey["home@v1"]
	require.True(t, ok)
	assert.JSONEq(t, `{
		"_tcardVersion": "v1",
		"title": "Home",
		"layout": {
			"columns": 2,
			"widgets": [
				{"kind": "news", "limit": 5},
				{"kind": "weather", "unit": "celsius"}
			]
		},
		"enabled": true
	}`, string(home.Template), "payload is the whole doc minus _id and path, keeping _tcardVersion")
	assert.NotContains(t, string(home.Template), `"path"`, "the routing key path must not leak into the payload")

	profile, ok := byKey["profile@v2"]
	require.True(t, ok)
	assert.JSONEq(t, `{"_tcardVersion": "v2", "title": "Profile"}`, string(profile.Template))
	assert.NotContains(t, string(profile.Template), "_id", "Mongo-internal _id must not leak into the payload")
}

func TestMongoCardStore_ListCards_EmptyCollection(t *testing.T) {
	db := testutil.MongoDB(t, "tcard")
	store := newMongoCardStore(db)

	cards, err := store.ListCards(context.Background())
	require.NoError(t, err)
	assert.Empty(t, cards)
}

func TestMongoCardStore_EnsureIndexes_UniquePathVersion(t *testing.T) {
	db := testutil.MongoDB(t, "tcard")
	store := newMongoCardStore(db)
	ctx := context.Background()

	require.NoError(t, store.EnsureIndexes(ctx))
	require.NoError(t, store.EnsureIndexes(ctx), "EnsureIndexes must stay idempotent")

	_, err := db.Collection("cards").InsertOne(ctx, bson.M{"path": "home", "_tcardVersion": "v1", "title": "first"})
	require.NoError(t, err)

	// Same (path, _tcardVersion) is a duplicate and must be rejected.
	_, err = db.Collection("cards").InsertOne(ctx, bson.M{"path": "home", "_tcardVersion": "v1", "title": "dup"})
	require.Error(t, err, "a second doc with the same (path, _tcardVersion) must be rejected")

	// Same path, different _tcardVersion is allowed — versions coexist.
	_, err = db.Collection("cards").InsertOne(ctx, bson.M{"path": "home", "_tcardVersion": "v2", "title": "next version"})
	require.NoError(t, err, "a new _tcardVersion for an existing path must be accepted")
}

// TestRefreshEndToEnd drives the real store through the HTTP refresh handler:
// docs inserted after the first refresh appear after the next one.
func TestRefreshEndToEnd(t *testing.T) {
	db := testutil.MongoDB(t, "tcard")
	store := newMongoCardStore(db)
	ctx := context.Background()

	_, err := db.Collection("cards").InsertOne(ctx, bson.M{"path": "home", "_tcardVersion": "v1", "title": "Home"})
	require.NoError(t, err)

	cache := newCardCache()
	r := setupRouter(t, NewCardHandler(cache, store))

	w := doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh")
	require.Equal(t, http.StatusOK, w.Code)

	w = doRequest(t, r, http.MethodGet, "/api/v1/cards/home@v1.template.json")
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"_tcardVersion":"v1","title":"Home"}`, w.Body.String())

	// New doc lands in Mongo → invisible until the next refresh.
	_, err = db.Collection("cards").InsertOne(ctx, bson.M{"path": "profile", "_tcardVersion": "v1", "title": "Profile"})
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound,
		doRequest(t, r, http.MethodGet, "/api/v1/cards/profile@v1.template.json").Code)

	require.Equal(t, http.StatusOK, doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh").Code)
	w = doRequest(t, r, http.MethodGet, "/api/v1/cards/profile@v1.template.json")
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"_tcardVersion":"v1","title":"Profile"}`, w.Body.String())
}

// TestGetTemplateEndToEnd drives the GET wildcard against a real store: a migrated
// doc serves without its bookkeeping fields, and the wildcard lists each depth.
func TestGetTemplateEndToEnd(t *testing.T) {
	db := testutil.MongoDB(t, "tcard")
	store := newMongoCardStore(db)
	ctx := context.Background()

	_, err := db.Collection("cards").InsertOne(ctx, bson.M{
		"_id": "c-legacy", "migratedAt": "2026-01-02T03:04:05Z",
		"path": "greetings/en/welcome", "_tcardVersion": "1.0.0", "type": "AdaptiveCard",
		"body": bson.A{bson.M{"type": "TextBlock", "id": "greeting", "text": "Hi"}},
	})
	require.NoError(t, err)

	cache := newCardCache()
	r := setupRouter(t, NewCardHandler(cache, store))
	require.Equal(t, http.StatusOK, doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh").Code)

	// The bookkeeping fields must be absent from the cached snapshot itself,
	// not merely filtered on the way out.
	cached, ok := cache.Get("greetings/en/welcome", "1.0.0")
	require.True(t, ok)
	assert.NotContains(t, string(cached), "migratedAt", "migratedAt must never enter the cache")
	assert.NotContains(t, string(cached), "_id")

	w := doRequest(t, r, http.MethodGet, "/api/v1/cards/greetings/en/welcome@1.0.0.template.json")
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{
		"_tcardVersion":"1.0.0","type":"AdaptiveCard",
		"body":[{"type":"TextBlock","id":"greeting","text":"Hi"}]
	}`, w.Body.String(), "an element id inside body is template content and survives")

	assert.NotContains(t, w.Body.String(), "migratedAt")
	assert.NotContains(t, w.Body.String(), "_id")

	// The same wildcard route lists folders and cards at each depth.
	assert.JSONEq(t, `{"statusCode":200,"cards":[],"folders":["greetings"]}`,
		doRequest(t, r, http.MethodGet, "/api/v1/cards/").Body.String())
	assert.JSONEq(t, `{"statusCode":200,"cards":[],"folders":["greetings/en"]}`,
		doRequest(t, r, http.MethodGet, "/api/v1/cards/greetings").Body.String())
	assert.JSONEq(t, `{"statusCode":200,"cards":["greetings/en/welcome@1.0.0"],"folders":[]}`,
		doRequest(t, r, http.MethodGet, "/api/v1/cards/greetings/en").Body.String())
}

// TestValidateEndToEnd drives POST /validate against a real Mongo-backed cache:
// ordering is judged from the loaded snapshot and nothing is written back.
func TestValidateEndToEnd(t *testing.T) {
	db := testutil.MongoDB(t, "tcard")
	store := newMongoCardStore(db)
	ctx := context.Background()

	_, err := db.Collection("cards").InsertOne(ctx,
		bson.M{"path": "onboard/en/welcome", "_tcardVersion": "1.0.0", "title": "Welcome"})
	require.NoError(t, err)

	r := setupRouter(t, NewCardHandler(newCardCache(), store))
	mk := func(path, version string) string {
		return `{"path":"` + path + `","_tcardVersion":"` + version + `","type":"AdaptiveCard",` +
			`"schema":"http://adaptivecards.io/schemas/adaptive-card.json","version":"1.5",` +
			`"body":[{"type":"TextBlock","text":"Hi"}],"cardUsage":"greeting"}`
	}

	// Until the cache loads there is no snapshot to order against.
	require.Equal(t, http.StatusServiceUnavailable,
		doJSON(t, r, http.MethodPost, "/api/v1/cards/validate", mk("onboard/en/welcome", "1.1.0")).Code)

	require.Equal(t, http.StatusOK, doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh").Code)

	// Strictly higher than the stored 1.0.0 → valid.
	w := doJSON(t, r, http.MethodPost, "/api/v1/cards/validate", mk("onboard/en/welcome", "1.1.0"))
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"success":true}`, w.Body.String())

	// An equal or lower _tcardVersion for the same path is a conflict.
	require.Equal(t, http.StatusConflict,
		doJSON(t, r, http.MethodPost, "/api/v1/cards/validate", mk("onboard/en/welcome", "1.0.0")).Code)
	require.Equal(t, http.StatusConflict,
		doJSON(t, r, http.MethodPost, "/api/v1/cards/validate", mk("onboard/en/welcome", "0.9.0")).Code)

	// A path with no cached versions has nothing to beat.
	require.Equal(t, http.StatusOK,
		doJSON(t, r, http.MethodPost, "/api/v1/cards/validate", mk("onboard/fr/bienvenue", "0.0.1")).Code)

	// Nothing was written: the validated card is neither stored nor servable.
	n, err := db.Collection("cards").CountDocuments(ctx, bson.M{"path": "onboard/en/welcome"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "validate must not insert")
	assert.Equal(t, http.StatusNotFound,
		doRequest(t, r, http.MethodGet, "/api/v1/cards/onboard/en/welcome@1.1.0.template.json").Code)
}
