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

// TestRegisterEndToEnd drives POST /register through the real store: a valid
// card inserts and is servable at once; the semver ordering rule is enforced.
func TestRegisterEndToEnd(t *testing.T) {
	db := testutil.MongoDB(t, "tcard")
	store := newMongoCardStore(db)
	require.NoError(t, store.EnsureIndexes(context.Background()))

	r := setupRouter(t, NewCardHandler(newCardCache(), store))
	mk := func(path, version string) string {
		// maxLines is a 2^53+1 int — it must round-trip without float64 rounding.
		return `{"path":"` + path + `","_tcardVersion":"` + version + `","type":"AdaptiveCard",` +
			`"schema":"http://adaptivecards.io/schemas/adaptive-card.json","version":"1.5",` +
			`"body":[{"type":"TextBlock","text":"Hi","maxLines":9007199254740993}],"cardUsage":"greeting"}`
	}

	// Load the (empty) cache so registered cards land in a live snapshot.
	require.Equal(t, http.StatusOK, doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh").Code)

	// Valid card → 201, then immediately servable with _id and path stripped.
	w := doJSON(t, r, http.MethodPost, "/api/v1/cards/register", mk("onboard/en/welcome", "1.0.0"))
	require.Equal(t, http.StatusCreated, w.Code)
	assert.JSONEq(t, `{"success":true}`, w.Body.String())

	got := doRequest(t, r, http.MethodGet, "/api/v1/cards/onboard/en/welcome@1.0.0.template.json")
	require.Equal(t, http.StatusOK, got.Code)
	assert.JSONEq(t, `{
		"_tcardVersion":"1.0.0","type":"AdaptiveCard",
		"schema":"http://adaptivecards.io/schemas/adaptive-card.json","version":"1.5",
		"body":[{"type":"TextBlock","text":"Hi","maxLines":9007199254740993}],"cardUsage":"greeting"
	}`, got.Body.String())

	// A lower or equal _tcardVersion for the same path is a conflict.
	require.Equal(t, http.StatusConflict,
		doJSON(t, r, http.MethodPost, "/api/v1/cards/register", mk("onboard/en/welcome", "1.0.0")).Code)
	require.Equal(t, http.StatusConflict,
		doJSON(t, r, http.MethodPost, "/api/v1/cards/register", mk("onboard/en/welcome", "0.9.0")).Code)

	// A strictly higher _tcardVersion succeeds and is servable.
	require.Equal(t, http.StatusCreated,
		doJSON(t, r, http.MethodPost, "/api/v1/cards/register", mk("onboard/en/welcome", "1.1.0")).Code)
	require.Equal(t, http.StatusOK,
		doRequest(t, r, http.MethodGet, "/api/v1/cards/onboard/en/welcome@1.1.0.template.json").Code)

	// The registered cards appear in the directory listing.
	list := doRequest(t, r, http.MethodGet, "/api/v1/cards/onboard/en")
	require.Equal(t, http.StatusOK, list.Code)
	assert.JSONEq(t, `{"statusCode":200,"cards":["onboard/en/welcome@1.0.0","onboard/en/welcome@1.1.0"],"folders":[]}`,
		list.Body.String())
}
