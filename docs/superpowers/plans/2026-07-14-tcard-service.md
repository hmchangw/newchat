# tcard-service (path, cardVersion) Addressing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change the already-shipped `tcard-service` so cards are addressed by `(path, cardVersion)` and the served payload is pure template content (the whole document minus `_id` and `path`).

**Architecture:** This is a *delta* against the existing service (`tcard-service/`, flat `package main`, Gin HTTP + in-memory `atomic.Pointer` cache over MongoDB `cards`). Tasks 1–2 (done): (1) the store reads `cardVersion`, drops `path` from the payload, and enforces a compound unique index; (2) the cache keys by `(path, cardVersion)` and the GET handler parses `{path}@{cardVersion}.template.json`. Task 3 adds the `POST /register` write path (validation + insert + cache reload); it adds one file (`semver.go`), a route, and two `CardStore` methods (so the mock is regenerated) — still no third-party dependency.

**Tech Stack:** Go 1.25, Gin, `go.mongodb.org/mongo-driver/v2` (bson), `pkg/errcode`/`errhttp`, `go.uber.org/mock` + `testify` (unit), `testcontainers` via `pkg/testutil` (integration).

## Global Constraints

- Follow the Red-Green-Refactor TDD cycle (CLAUDE.md §4): write/adjust the failing test first, confirm it fails, then implement.
- Use `make` targets only — never raw `go` (CLAUDE.md §2). Unit: `make test SERVICE=tcard-service`. Integration (Docker required): `make test-integration SERVICE=tcard-service`. Lint: `make lint`.
- Every commit must leave BOTH `make test SERVICE=tcard-service` and `make lint` green. Task 1 and Task 2 each also keep `make test-integration SERVICE=tcard-service` green.
- `cardVersion` is a **string** end-to-end; no numeric parsing or ordering.
- Errors use `pkg/errcode` constructors + `errhttp.Write` (CLAUDE.md §3); never log-and-return.
- Structured `log/slog` only; never log full payloads.
- Not `docs/client-api.md`-scoped (no `chat.user.*` NATS subject, not an auth-service HTTP route) — that file is NOT touched.
- The `CardStore` interface signature is unchanged (`ListCards(ctx) ([]card, error)`), so `mock_store_test.go` needs no regeneration; `make generate SERVICE=tcard-service` is a no-op here.

---

## File Structure

Only these files change — all already exist:

| File | Responsibility | Change |
|---|---|---|
| `tcard-service/store.go` | `card` type + `CardStore` interface | Add `CardVersion string` to `card`. |
| `tcard-service/store_mongo.go` | Mongo `CardStore` impl | `EnsureIndexes`: compound unique `(path, cardVersion)`. `ListCards`: read `cardVersion`, drop `path` from payload. |
| `tcard-service/cache.go` | in-memory snapshot | Key by `cardKey{path, cardVersion}`; `Get(path, cardVersion)`; `replace` skips rows missing either key part. |
| `tcard-service/handler.go` | HTTP handlers | `HandleGetTemplate`: parse `{path}@{cardVersion}.template.json`, version required. |
| `tcard-service/integration_test.go` | store + end-to-end integration tests | Fixtures gain `cardVersion`; assert `path` stripped; compound-index test; end-to-end URLs. |
| `tcard-service/cache_test.go` | cache unit tests | Versioned fixtures; `Get(path, cardVersion)` call sites; same-path-different-version case. |
| `tcard-service/handler_test.go` | handler unit tests | `@`-form URLs; new `400` cases (missing version). |

Unchanged (do not edit): `main.go`, `routes.go`, `mock_store_test.go`, `deploy/*`, `docker-local/compose.services.yaml`.

---

## Task 1: Store — read `cardVersion`, strip `path`, compound unique index

**Files:**
- Modify: `tcard-service/store.go`
- Modify: `tcard-service/store_mongo.go`
- Test: `tcard-service/integration_test.go`

**Interfaces:**
- Consumes: existing `CardStore.ListCards(ctx context.Context) ([]card, error)` (signature unchanged).
- Produces:
  - `card` struct with fields `Path string`, `CardVersion string`, `Template json.RawMessage`.
  - `(*mongoCardStore).ListCards` returns one `card` per document that has a non-empty string `path` **and** a non-empty string `cardVersion`; `Template` is the document rendered to relaxed extended JSON with `_id` and `path` removed (and `cardVersion` retained).
  - `(*mongoCardStore).EnsureIndexes` creates a unique compound index on `(path, cardVersion)`.

- [ ] **Step 1: Rewrite the store integration tests to the new expectations (Red)**

Replace `TestMongoCardStore_ListCards`, `TestMongoCardStore_EnsureIndexes_UniquePath`, and `TestRefreshEndToEnd` in `tcard-service/integration_test.go` with the versions below. `TestMongoCardStore_ListCards_EmptyCollection` and `TestMain` are unchanged — leave them as-is.

```go
func TestMongoCardStore_ListCards(t *testing.T) {
	db := testutil.MongoDB(t, "tcard")
	store := newMongoCardStore(db)
	ctx := context.Background()

	// Card docs are schemaless templates: nested objects, arrays, mixed
	// scalar types all round-trip verbatim (minus _id and path). A doc is
	// keyed by (path, cardVersion); a doc missing a string path OR a string
	// cardVersion is skipped, not fatal.
	_, err := db.Collection("cards").InsertMany(ctx, []any{
		bson.M{
			"_id": "c-home", "path": "home", "cardVersion": "v1",
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
		bson.M{"_id": "c-profile", "path": "profile", "cardVersion": "v2", "title": "Profile"},
		bson.M{"_id": "c-no-path", "cardVersion": "v1", "title": "orphan template"},
		bson.M{"_id": "c-bad-path", "path": 42, "cardVersion": "v1", "title": "numeric path"},
		bson.M{"_id": "c-no-version", "path": "settings", "title": "no version"},
	})
	require.NoError(t, err)

	cards, err := store.ListCards(ctx)
	require.NoError(t, err)
	require.Len(t, cards, 2, "docs missing a string path or cardVersion are skipped")

	byKey := make(map[string]card, len(cards))
	for _, c := range cards {
		byKey[c.Path+"@"+c.CardVersion] = c
	}

	home, ok := byKey["home@v1"]
	require.True(t, ok)
	assert.JSONEq(t, `{
		"cardVersion": "v1",
		"title": "Home",
		"layout": {
			"columns": 2,
			"widgets": [
				{"kind": "news", "limit": 5},
				{"kind": "weather", "unit": "celsius"}
			]
		},
		"enabled": true
	}`, string(home.Template), "payload is the whole doc minus _id and path, keeping cardVersion")
	assert.NotContains(t, string(home.Template), `"path"`, "the routing key path must not leak into the payload")

	profile, ok := byKey["profile@v2"]
	require.True(t, ok)
	assert.JSONEq(t, `{"cardVersion": "v2", "title": "Profile"}`, string(profile.Template))
	assert.NotContains(t, string(profile.Template), "_id", "Mongo-internal _id must not leak into the payload")
}

func TestMongoCardStore_EnsureIndexes_UniquePathVersion(t *testing.T) {
	db := testutil.MongoDB(t, "tcard")
	store := newMongoCardStore(db)
	ctx := context.Background()

	require.NoError(t, store.EnsureIndexes(ctx))

	_, err := db.Collection("cards").InsertOne(ctx, bson.M{"path": "home", "cardVersion": "v1", "title": "first"})
	require.NoError(t, err)

	// Same (path, cardVersion) is a duplicate and must be rejected.
	_, err = db.Collection("cards").InsertOne(ctx, bson.M{"path": "home", "cardVersion": "v1", "title": "dup"})
	require.Error(t, err, "a second doc with the same (path, cardVersion) must be rejected")

	// Same path, different cardVersion is allowed — versions coexist.
	_, err = db.Collection("cards").InsertOne(ctx, bson.M{"path": "home", "cardVersion": "v2", "title": "next version"})
	require.NoError(t, err, "a new cardVersion for an existing path must be accepted")
}

// TestRefreshEndToEnd drives the real store through the HTTP refresh handler:
// docs inserted after the first refresh appear after the next one. (Request
// URLs stay path-only here; Task 2 switches them to the {path}@{cardVersion}
// form once the handler and cache are version-aware.)
func TestRefreshEndToEnd(t *testing.T) {
	db := testutil.MongoDB(t, "tcard")
	store := newMongoCardStore(db)
	ctx := context.Background()

	_, err := db.Collection("cards").InsertOne(ctx, bson.M{"path": "home", "cardVersion": "v1", "title": "Home"})
	require.NoError(t, err)

	cache := newCardCache()
	r := setupRouter(t, NewCardHandler(cache, store))

	w := doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh")
	require.Equal(t, http.StatusOK, w.Code)

	w = doRequest(t, r, http.MethodGet, "/api/v1/cards/home.template.json")
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"cardVersion":"v1","title":"Home"}`, w.Body.String())

	// New doc lands in Mongo → invisible until the next refresh.
	_, err = db.Collection("cards").InsertOne(ctx, bson.M{"path": "profile", "cardVersion": "v1", "title": "Profile"})
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound,
		doRequest(t, r, http.MethodGet, "/api/v1/cards/profile.template.json").Code)

	require.Equal(t, http.StatusOK, doRequest(t, r, http.MethodPost, "/api/v1/cards/refresh").Code)
	w = doRequest(t, r, http.MethodGet, "/api/v1/cards/profile.template.json")
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"cardVersion":"v1","title":"Profile"}`, w.Body.String())
}
```

- [ ] **Step 2: Run integration tests to verify they fail**

Run: `make test-integration SERVICE=tcard-service`
Expected: FAIL — build error `card.CardVersion undefined` (the struct field doesn't exist yet) and, once that compiles, assertion failures because `path` is still in the payload and the index is still `path`-only.

- [ ] **Step 3: Add `CardVersion` to the `card` struct**

In `tcard-service/store.go`, replace the `card` struct with:

```go
// card is one row of the load-time template cache built from the cards
// collection: the lookup key (path + cardVersion) plus the template content
// rendered as JSON. Card documents are schemaless templates authored per
// deployment, so the content stays raw JSON rather than a typed struct.
type card struct {
	Path        string          `json:"path"`
	CardVersion string          `json:"cardVersion"`
	Template    json.RawMessage `json:"template"`
}
```

Leave the `//go:generate` directive and the `CardStore` interface unchanged.

- [ ] **Step 4: Rewrite `EnsureIndexes` and `ListCards`**

In `tcard-service/store_mongo.go`, replace `EnsureIndexes` and `ListCards` with:

```go
// EnsureIndexes enforces (path, cardVersion) uniqueness so two documents
// cannot claim one template version — the cache would otherwise serve
// whichever happened to come first in cursor order. The `version` field is a
// data-type version, unrelated to document identity, and is not indexed.
func (s *mongoCardStore) EnsureIndexes(ctx context.Context) error {
	if _, err := s.cards.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "path", Value: 1}, {Key: "cardVersion", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("ensure cards (path, cardVersion) unique index: %w", err)
	}
	return nil
}

// ListCards returns every card template document, keyed by (path, cardVersion).
// Card docs are schemaless templates, so each is decoded as an ordered
// document and rendered to relaxed extended JSON rather than a typed struct.
// The served payload excludes _id (Mongo-internal, dropped via projection) and
// path (the routing key, dropped here); cardVersion is retained as content. A
// doc missing a non-empty string path or cardVersion cannot be keyed and is
// skipped with a warning.
func (s *mongoCardStore) ListCards(ctx context.Context) ([]card, error) {
	proj := options.Find().SetProjection(bson.D{{Key: "_id", Value: 0}})
	cursor, err := s.cards.Find(ctx, bson.D{}, proj)
	if err != nil {
		return nil, fmt.Errorf("find cards: %w", err)
	}
	defer cursor.Close(ctx)

	var cards []card
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode card document: %w", err)
		}

		var path, cardVersion string
		payload := make(bson.D, 0, len(doc))
		for _, e := range doc {
			switch e.Key {
			case "path":
				path, _ = e.Value.(string)
				// path is the routing key, not template content — drop it.
			case "cardVersion":
				cardVersion, _ = e.Value.(string)
				payload = append(payload, e)
			default:
				payload = append(payload, e)
			}
		}
		if path == "" || cardVersion == "" {
			slog.Warn("card document missing a string path or cardVersion, skipping",
				"path", path, "cardVersion", cardVersion)
			continue
		}

		tmpl, err := bson.MarshalExtJSON(payload, false, false)
		if err != nil {
			return nil, fmt.Errorf("render card %q@%q to JSON: %w", path, cardVersion, err)
		}
		cards = append(cards, card{Path: path, CardVersion: cardVersion, Template: tmpl})
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate cards: %w", err)
	}
	return cards, nil
}
```

Imports in `store_mongo.go` are unchanged (`bson`, `mongo`, `options`, `fmt`, `slog`, `context` all still used).

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test-integration SERVICE=tcard-service`
Expected: PASS — `TestMongoCardStore_ListCards`, `TestMongoCardStore_EnsureIndexes_UniquePathVersion`, `TestMongoCardStore_ListCards_EmptyCollection`, `TestRefreshEndToEnd` all green.

Run: `make test SERVICE=tcard-service`
Expected: PASS — unit tests are unaffected (they build `card` values with only `Path`/`Template`; `CardVersion` defaults to `""`, and the cache still keys by `path` until Task 2).

Run: `make lint`
Expected: PASS — 0 issues.

- [ ] **Step 6: Commit**

```bash
git add tcard-service/store.go tcard-service/store_mongo.go tcard-service/integration_test.go
git commit -m "feat(tcard-service): key cards by (path, cardVersion) in the store

Read cardVersion, drop the routing key path from the served payload
(keeping cardVersion), and make the unique index compound on
(path, cardVersion). The data-type version field stays unindexed."
```

---

## Task 2: Cache + handler — composite key and `{path}@{cardVersion}` routing

**Files:**
- Modify: `tcard-service/cache.go`
- Modify: `tcard-service/handler.go`
- Test: `tcard-service/cache_test.go`
- Test: `tcard-service/handler_test.go`
- Test: `tcard-service/integration_test.go` (end-to-end URLs only)

**Interfaces:**
- Consumes: `card{Path, CardVersion, Template}` from Task 1.
- Produces:
  - `cardKey struct { path, cardVersion string }` (unexported, in `cache.go`).
  - `(*cardCache).Get(path, cardVersion string) (json.RawMessage, bool)`.
  - `(*cardCache).replace(cards []card) int` keys by `cardKey` (first occurrence of a `(path, cardVersion)` wins). Non-empty `path`/`cardVersion` is already guaranteed by the store, so `replace` only dedups.
  - `HandleGetTemplate` serves `GET /api/v1/cards/{path}@{cardVersion}.template.json`: `400` on missing `.template.json` suffix, missing `@`, or empty `path`/`cardVersion`; `404` on cache miss; `200` otherwise.

- [ ] **Step 1: Update the cache unit tests (Red)**

In `tcard-service/cache_test.go`, replace the fixtures block (the `var ( homeCard … profileCard … )`) with versioned cards whose templates carry `cardVersion` and no `path`:

```go
var (
	homeCard = card{
		Path: "home", CardVersion: "v1",
		Template: json.RawMessage(`{"cardVersion":"v1","title":"Home","widgets":["news","weather"]}`),
	}
	profileCard = card{
		Path: "profile", CardVersion: "v2",
		Template: json.RawMessage(`{"cardVersion":"v2","title":"Profile"}`),
	}
)
```

Then update every cache lookup call site in this file to pass the version:

- `TestCardCache_EmptyUntilLoaded`: `cache.Get("home", "v1")`
- `TestCardCache_Load`: `cache.Get("home", "v1")`, `cache.Get("profile", "v2")`, and the miss `cache.Get("missing", "v1")`
- `TestCardCache_LoadErrorKeepsPreviousEntries`: `cache.Get("home", "v1")`
- `TestCardCache_EmptyLoadIsReady`: `cache.Get("home", "v1")`
- `TestCardCache_RefreshDropsRemovedCards`: `cache.Get("profile", "v2")` (dropped) and `cache.Get("home", "v1")` (kept)
- `TestCardCache_ConcurrentReadDuringLoad`: `cache.Get("home", "v1")`

Replace `TestCardCache_DuplicatePathSkippedKeepsRest` with a version-aware duplicate test plus a coexistence test:

```go
func TestCardCache_DuplicateKeySkippedKeepsRest(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	dupHome := card{Path: "home", CardVersion: "v1", Template: json.RawMessage(`{"title":"Impostor"}`)}
	// Same (path, cardVersion): the first occurrence wins; the later duplicate
	// is skipped (with a warning) rather than rejecting the whole snapshot.
	store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard, profileCard, dupHome}, nil)

	cache := newCardCache()
	n, err := cache.Load(context.Background(), store)
	require.NoError(t, err, "a duplicate row must be skipped, not reject the whole snapshot")
	assert.Equal(t, 2, n)

	got, ok := cache.Get("home", "v1")
	require.True(t, ok)
	assert.JSONEq(t, string(homeCard.Template), string(got), "the first occurrence wins")
	_, ok = cache.Get("profile", "v2")
	assert.True(t, ok, "non-duplicate rows are still published")
}

func TestCardCache_SamePathDifferentVersionsCoexist(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	homeV2 := card{Path: "home", CardVersion: "v2", Template: json.RawMessage(`{"cardVersion":"v2","title":"Home v2"}`)}
	store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard, homeV2}, nil)

	cache := newCardCache()
	n, err := cache.Load(context.Background(), store)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "two versions of one path are distinct entries")

	v1, ok := cache.Get("home", "v1")
	require.True(t, ok)
	assert.JSONEq(t, string(homeCard.Template), string(v1))
	v2, ok := cache.Get("home", "v2")
	require.True(t, ok)
	assert.JSONEq(t, string(homeV2.Template), string(v2))
}
```

- [ ] **Step 2: Update the handler unit tests (Red)**

In `tcard-service/handler_test.go`, switch every template URL to the `{path}@{cardVersion}.template.json` form and replace the error table.

Call-site URL changes:
- `TestHandleRefresh_HappyPath`: the servable check → `GET /api/v1/cards/home@v1.template.json`
- `TestHandleRefresh_PicksUpNewDocs`: both `profile` GETs → `/api/v1/cards/profile@v2.template.json`
- `TestHandleRefresh_StoreErrorKeepsServingPrevious`: `GET /api/v1/cards/home@v1.template.json`
- `TestHandleGetTemplate_HappyPath`: `GET /api/v1/cards/home@v1.template.json`
- `TestHandleGetTemplate_NotReadyIsNotFound`: `GET /api/v1/cards/home@v1.template.json`

`TestHandleRefresh_GetIsNotRouted` is unchanged: `GET /api/v1/cards/refresh` still falls through to the `:file` route and 400s (no `.template.json` suffix).

Replace `TestHandleGetTemplate_Errors` with:

```go
func TestHandleGetTemplate_Errors(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		wantCode int
		wantErr  errcode.Code
	}{
		{
			name:     "unknown (path, cardVersion) is not found",
			target:   "/api/v1/cards/missing@v9.template.json",
			wantCode: http.StatusNotFound,
			wantErr:  errcode.CodeNotFound,
		},
		{
			name:     "known path with unknown version is not found",
			target:   "/api/v1/cards/home@v9.template.json",
			wantCode: http.StatusNotFound,
			wantErr:  errcode.CodeNotFound,
		},
		{
			name:     "filename without the .template.json suffix is a bad request",
			target:   "/api/v1/cards/home@v1.json",
			wantCode: http.StatusBadRequest,
			wantErr:  errcode.CodeBadRequest,
		},
		{
			name:     "missing version (no @) is a bad request",
			target:   "/api/v1/cards/home.template.json",
			wantCode: http.StatusBadRequest,
			wantErr:  errcode.CodeBadRequest,
		},
		{
			name:     "empty path is a bad request",
			target:   "/api/v1/cards/@v1.template.json",
			wantCode: http.StatusBadRequest,
			wantErr:  errcode.CodeBadRequest,
		},
		{
			name:     "empty version is a bad request",
			target:   "/api/v1/cards/home@.template.json",
			wantCode: http.StatusBadRequest,
			wantErr:  errcode.CodeBadRequest,
		},
		{
			name:     "empty stem is a bad request",
			target:   "/api/v1/cards/.template.json",
			wantCode: http.StatusBadRequest,
			wantErr:  errcode.CodeBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := setupRouter(t, NewCardHandler(cacheWith(homeCard), nil))
			w := doRequest(t, r, http.MethodGet, tt.target)
			require.Equal(t, tt.wantCode, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), tt.wantErr)
		})
	}
}
```

- [ ] **Step 3: Point the end-to-end integration test at `@`-form URLs (Red for integration)**

In `tcard-service/integration_test.go`, in `TestRefreshEndToEnd`, change only the three template GET URLs (the inserted docs and payload assertions from Task 1 stay):
- `"/api/v1/cards/home.template.json"` → `"/api/v1/cards/home@v1.template.json"`
- both `"/api/v1/cards/profile.template.json"` → `"/api/v1/cards/profile@v1.template.json"`

Also update the comment above the function to drop the "Task 2 switches them" note (the switch is now done):

```go
// TestRefreshEndToEnd drives the real store through the HTTP refresh handler:
// docs inserted after the first refresh appear after the next one.
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `make test SERVICE=tcard-service`
Expected: FAIL — `cache.Get` is called with 2 args but still takes 1 (build error), and once that is addressed the `@`-URL handler tests fail because `HandleGetTemplate` doesn't parse `@` yet.

- [ ] **Step 5: Rewrite the cache to key by `(path, cardVersion)`**

In `tcard-service/cache.go`, replace the `cardCache` type declaration, `Get`, and `replace` with the versions below. `newCardCache`, `Ready`, `Load`, `RefreshLoop`, `parseRefreshAt`, and `nextDailyRefresh` are unchanged.

```go
// cardKey identifies a cached template by its routing key: the card path plus
// the document version. A struct key avoids any delimiter-collision risk a
// concatenated string key would carry.
type cardKey struct {
	path        string
	cardVersion string
}

// cardCache is the in-memory (path, cardVersion) → template snapshot of the
// cards collection. The whole map is swapped wholesale by Load (at startup, on
// the periodic refresh, and on every /api/v1/cards/refresh call) — entries
// have no TTL, and a card deleted upstream drops out on the next load.
//
// The snapshot lives in an atomic.Pointer: reads (Get/Ready) are a single
// lock-free load of an immutable map, while a refresh swaps in a freshly
// built map with one atomic store. The map is never mutated in place, so
// readers never need a lock. Concurrent Loads are safe: each builds its own
// map and the last store wins.
type cardCache struct {
	entries atomic.Pointer[map[cardKey]json.RawMessage]
}

// Get returns the template document for (path, cardVersion).
func (c *cardCache) Get(path, cardVersion string) (json.RawMessage, bool) {
	m := c.entries.Load()
	if m == nil {
		return nil, false
	}
	tmpl, ok := (*m)[cardKey{path: path, cardVersion: cardVersion}]
	return tmpl, ok
}
```

Replace `replace` with:

```go
// replace builds a snapshot keyed by (path, cardVersion) and swaps it in
// atomically. A duplicate (path, cardVersion) is skipped with a warning
// rather than rejecting the whole snapshot; the first occurrence wins. The
// store guarantees non-empty path/cardVersion, so no empty-key guard is
// needed here. Returns the number of cards published.
func (c *cardCache) replace(cards []card) int {
	entries := make(map[cardKey]json.RawMessage, len(cards))
	for _, card := range cards {
		key := cardKey{path: card.Path, cardVersion: card.CardVersion}
		if _, dup := entries[key]; dup {
			slog.Warn("duplicate path in cards collection, skipping",
				"path", card.Path, "cardVersion", card.CardVersion)
			continue
		}
		entries[key] = card.Template
	}
	c.entries.Store(&entries)
	return len(entries)
}
```

`cache.go` imports are unchanged (`encoding/json`, `log/slog`, `sync/atomic`, `context`, `fmt`, `time` all still used).

- [ ] **Step 6: Rewrite `HandleGetTemplate` to parse `{path}@{cardVersion}`**

In `tcard-service/handler.go`, update the `templateSuffix` doc comment and replace `HandleGetTemplate`. `HandleRefresh`, `HandleHealth`, and `HandleReady` are unchanged.

```go
// templateSuffix is the filename suffix the template route requires:
// GET /api/v1/cards/{path}@{cardVersion}.template.json serves the card
// identified by (path, cardVersion).
const templateSuffix = ".template.json"
```

```go
// HandleGetTemplate serves the cached card identified by the {path} and
// {cardVersion} in /api/v1/cards/{path}@{cardVersion}.template.json — a single
// lock-free lookup, no Mongo read. The version is required: a request without
// an "@" separator is a bad request, never a "latest wins" guess.
func (h *CardHandler) HandleGetTemplate(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	file := c.Param("file")
	spec := strings.TrimSuffix(file, templateSuffix)
	if spec == file {
		errhttp.Write(ctx, c, errcode.BadRequest("card template file must be named {path}@{cardVersion}"+templateSuffix))
		return
	}
	at := strings.LastIndex(spec, "@")
	if at < 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("card template request must include a version: {path}@{cardVersion}"+templateSuffix))
		return
	}
	path, cardVersion := spec[:at], spec[at+1:]
	if path == "" || cardVersion == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("card template path and cardVersion must both be non-empty"))
		return
	}

	tmpl, ok := h.cache.Get(path, cardVersion)
	if !ok {
		errhttp.Write(ctx, c, errcode.NotFound("card template not found"))
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", tmpl)
}
```

`handler.go` imports are unchanged (`net/http`, `strings`, `gin`, `errcode`, `errhttp` all still used).

- [ ] **Step 7: Run tests to verify they pass**

Run: `make test SERVICE=tcard-service`
Expected: PASS — all cache and handler unit tests green, including the new missing-version `400` and same-path-different-version cases.

Run: `make test-integration SERVICE=tcard-service`
Expected: PASS — `TestRefreshEndToEnd` resolves `home@v1`/`profile@v1`.

Run: `make lint`
Expected: PASS — 0 issues.

- [ ] **Step 8: Commit**

```bash
git add tcard-service/cache.go tcard-service/handler.go tcard-service/cache_test.go tcard-service/handler_test.go tcard-service/integration_test.go
git commit -m "feat(tcard-service): address cards by {path}@{cardVersion} on GET

Cache keys by (path, cardVersion); GET /api/v1/cards/{path}@{cardVersion}
.template.json requires the version (no @ -> 400), returns 404 on a miss.
Two versions of one path now coexist in the cache."
```

---

## Task 3: `POST /api/v1/cards/register` — validate, insert, refresh

**Goal (delta):** Add a write endpoint that validates a card, inserts it, and reloads the cache so it is servable at once. See the spec's "Card registration" section for the authoritative rules.

**Files:**
- Create: `tcard-service/semver.go` (pure `a.b.c` parse/compare) + `tcard-service/semver_test.go`
- Modify: `tcard-service/store.go` (`CardStore` gains `GetCard` + `ListVersions` + `InsertCard`; new `cardDoc` type; `ErrDuplicateCard`)
- Modify: `tcard-service/store_mongo.go` (implement `GetCard`, `ListVersions`, `InsertCard`; shared `docToCard` render helper)
- Modify: `tcard-service/cache.go` (`Add` — copy-on-write one card into the snapshot)
- Modify: `tcard-service/handler.go` (`reqCtx` helper; `HandleRegister` binds `cardDoc` directly + `validateRegister` + `isHighest`)
- Modify: `tcard-service/routes.go` (register the POST route)
- Regenerate: `tcard-service/mock_store_test.go` via `make generate SERVICE=tcard-service` (the `CardStore` interface changed — this is NO LONGER a no-op)
- Test: `tcard-service/handler_test.go`, `tcard-service/integration_test.go`

**Interfaces produced:**
- `CardStore` adds `ListVersions(ctx, path string) ([]string, error)` and `InsertCard(ctx, doc *cardDoc) error`.
- `cardDoc{Path, CardVersion string; CardUsage json.RawMessage; Type, Schema, Version string; Body json.RawMessage}` — json-tagged; it doubles as the bound `POST /register` body (no separate request type).
- `ErrDuplicateCard` sentinel; `InsertCard` returns it (wrapped-comparable via `errors.Is`) on a duplicate-key error.
- `parseSemver(s string) (semver, bool)`, `(semver).greater(o semver) bool`, `isHighest(v string, existing []string) bool`.
- `HandleRegister(*gin.Context)`: `201 {"success":true}`; `400` on field/format; `409` on not-highest or duplicate; `500` on infra.

**Post-review design updates (authoritative: the spec's "Card registration" section + the shipped code — the illustrative code blocks below predate these):**
- Serviceability: register does **not** full-reload. After insert it fetches the one card (`GetCard`) and copy-on-writes it into the snapshot (`cache.Add`, **serialized with `Load` on a cache write mutex** so neither reverts the other; reads stay lock-free); a fetch-back miss or not-yet-loaded cache logs and still returns `201` (card appears on next refresh).
- Validation adds: `path` must not contain `/`; `body` must be a **non-empty array**; `cardVersion` semver rejects **leading zeros**.
- Concurrency: the highest-check and insert are **not** serialized — two concurrent same-path registers can both insert **different** versions (accepted; admin-driven, ~5–10/week); the compound unique index still blocks an exact `(path, cardVersion)` duplicate (one `409`).
- DRY: `reqCtx(c)` helper replaces the repeated request-id logging context; `docToCard` is shared by `ListCards` and `GetCard`.

- [ ] **Step 1: semver helper (Red → Green)**

`semver_test.go` (table): valid `1.2.3`; invalid — `""`, `1.2`, `1.2.3.4`, `1.2.x`, `1..3`, `-1.0.0`, `+1.0.0`, `1.2.3-beta`; ordering — `greater` true for `1.0.1>1.0.0`, `1.1.0>1.0.9`, `2.0.0>1.9.9`, false for equal and lower; `isHighest("1.0.1", ["1.0.0","bogus"])` true (non-semver ignored), `isHighest("1.0.0", ["1.0.0"])` false.

`semver.go`:

```go
package main

import (
	"strconv"
	"strings"
)

// semver is a parsed a.b.c version.
type semver struct{ major, minor, patch int }

// parseSemver parses "a.b.c" (three all-digit parts); ok is false otherwise.
func parseSemver(s string) (semver, bool) {
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return semver{}, false
	}
	var n [3]int
	for i, p := range parts {
		if p == "" || !allDigits(p) {
			return semver{}, false
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			return semver{}, false
		}
		n[i] = v
	}
	return semver{n[0], n[1], n[2]}, true
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// greater reports whether s > o in semver order.
func (s semver) greater(o semver) bool {
	if s.major != o.major {
		return s.major > o.major
	}
	if s.minor != o.minor {
		return s.minor > o.minor
	}
	return s.patch > o.patch
}

// isHighest reports whether v is strictly greater than every well-formed
// version in existing; non-semver existing versions are ignored.
func isHighest(v string, existing []string) bool {
	nv, ok := parseSemver(v)
	if !ok {
		return false
	}
	for _, e := range existing {
		if ev, ok := parseSemver(e); ok && !nv.greater(ev) {
			return false
		}
	}
	return true
}
```

- [ ] **Step 2: store interface + insert/version query (Red via integration)**

`store.go` — add to the file:

```go
// cardDoc is the POST /register payload (json tags only, never bson-marshaled;
// InsertCard builds the stored document explicitly).
type cardDoc struct {
	Path        string          `json:"path"`
	CardVersion string          `json:"cardVersion"`
	CardUsage   json.RawMessage `json:"cardUsage"`
	Type        string          `json:"type"`
	Schema      string          `json:"schema"`
	Version     string          `json:"version"`
	Body        json.RawMessage `json:"body"`
}

// ErrDuplicateCard is returned by InsertCard when (path, cardVersion) exists.
var ErrDuplicateCard = errors.New("card already exists")
```

Extend `CardStore`:

```go
type CardStore interface {
	ListCards(ctx context.Context) ([]card, error)
	ListVersions(ctx context.Context, path string) ([]string, error)
	InsertCard(ctx context.Context, doc *cardDoc) error
}
```

`store_mongo.go`:

```go
// ListVersions returns the cardVersion of every document for path.
func (s *mongoCardStore) ListVersions(ctx context.Context, path string) ([]string, error) {
	proj := options.Find().SetProjection(bson.D{{Key: "cardVersion", Value: 1}, {Key: "_id", Value: 0}})
	cursor, err := s.cards.Find(ctx, bson.D{{Key: "path", Value: path}}, proj)
	if err != nil {
		return nil, fmt.Errorf("find card versions: %w", err)
	}
	defer cursor.Close(ctx)
	var versions []string
	for cursor.Next(ctx) {
		if v, ok := cursor.Current.Lookup("cardVersion").StringValueOK(); ok {
			versions = append(versions, v)
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate card versions: %w", err)
	}
	return versions, nil
}

// InsertCard writes one validated card, mapping a duplicate key to ErrDuplicateCard.
func (s *mongoCardStore) InsertCard(ctx context.Context, doc *cardDoc) error {
	body, err := jsonToBSON(doc.Body)
	if err != nil {
		return fmt.Errorf("decode body: %w", err)
	}
	d := bson.M{
		"path": doc.Path, "cardVersion": doc.CardVersion,
		"type": doc.Type, "schema": doc.Schema, "version": doc.Version, "body": body,
	}
	if len(doc.CardUsage) > 0 {
		usage, err := jsonToBSON(doc.CardUsage)
		if err != nil {
			return fmt.Errorf("decode cardUsage: %w", err)
		}
		d["cardUsage"] = usage
	}
	if _, err := s.cards.InsertOne(ctx, d); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrDuplicateCard
		}
		return fmt.Errorf("insert card: %w", err)
	}
	return nil
}

// jsonToBSON turns raw JSON into a value the mongo driver can marshal.
func jsonToBSON(raw json.RawMessage) (any, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("unmarshal json: %w", err)
	}
	return v, nil
}
```

Run `make generate SERVICE=tcard-service` (regenerates the mock for the two new methods), then `make test-integration SERVICE=tcard-service` for the integration cases in Step 4.

- [ ] **Step 3: register handler + validation (Red → Green, unit)**

`handler.go` — add:

```go
const (
	registerType    = "AdaptiveCard"
	registerSchema  = "http://adaptivecards.io/schemas/adaptive-card.json"
	registerVersion = "1.5"
)

// HandleRegister validates a card, inserts it, and reloads the cache so it is
// servable at once. 400 on field/format, 409 on not-highest/duplicate.
func (h *CardHandler) HandleRegister(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	var doc cardDoc
	if err := c.ShouldBindJSON(&doc); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid card JSON"))
		return
	}
	if err := validateRegister(&doc); err != nil {
		errhttp.Write(ctx, c, err)
		return
	}

	versions, err := h.store.ListVersions(ctx, doc.Path)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("list card versions: %w", err))
		return
	}
	if !isHighest(doc.CardVersion, versions) {
		errhttp.Write(ctx, c, errcode.Conflict("cardVersion must be the highest for this path"))
		return
	}

	if err := h.store.InsertCard(ctx, &doc); err != nil {
		if errors.Is(err, ErrDuplicateCard) {
			errhttp.Write(ctx, c, errcode.Conflict("card already exists for this (path, cardVersion)"))
			return
		}
		errhttp.Write(ctx, c, err)
		return
	}

	// Reload so the card is servable at once; a reload failure is logged, not
	// surfaced (the card is persisted and appears on the next refresh).
	if _, err := h.cache.Load(ctx, h.store); err != nil {
		slog.Warn("card registered but cache reload failed",
			"path", doc.Path, "cardVersion", doc.CardVersion, "error", err)
	}
	c.JSON(http.StatusCreated, gin.H{"success": true})
}

// validateRegister runs the field/format checks (spec checks 1-5).
func validateRegister(doc *cardDoc) error {
	switch {
	case doc.Path == "":
		return errcode.BadRequest("path is required")
	case doc.CardVersion == "":
		return errcode.BadRequest("cardVersion is required")
	case doc.Type == "":
		return errcode.BadRequest("type is required")
	case doc.Schema == "":
		return errcode.BadRequest("schema is required")
	case doc.Version == "":
		return errcode.BadRequest("version is required")
	case len(doc.Body) == 0 || string(doc.Body) == "null":
		return errcode.BadRequest("body is required")
	}
	if _, ok := parseSemver(doc.CardVersion); !ok {
		return errcode.BadRequest("cardVersion must be a semantic version a.b.c")
	}
	if doc.Type != registerType {
		return errcode.BadRequest(`type must be "AdaptiveCard"`)
	}
	if doc.Schema != registerSchema {
		return errcode.BadRequest("schema must be " + registerSchema)
	}
	if doc.Version != registerVersion {
		return errcode.BadRequest(`version must be "1.5"`)
	}
	return nil
}
```

`routes.go` — add the route (before the `:file` GET is fine; POST vs GET never collide):

```go
	r.POST("/api/v1/cards/register", h.HandleRegister)
```

`handler_test.go` — table-driven: `201` happy path (mock `ListVersions` → `nil`, `InsertCard` → `nil`, cache reload mocked via `ListCards`); `400` for each of missing-`path`, missing-`body`, non-semver `cardVersion` (`1.2`), wrong `type`, wrong `schema`, wrong `version`; `409` when `ListVersions` returns a higher/equal version; `409` when `InsertCard` returns `ErrDuplicateCard`. Assert error codes via `errtest.AssertCode`.

- [ ] **Step 4: integration (Red → Green)**

`integration_test.go` — with a real store: register a valid card (`201`), then GET `{path}@{cardVersion}.template.json` returns it (`200`, payload minus `_id`/`path`); registering a lower/equal `cardVersion` for that path returns `409`; a higher one succeeds.

- [ ] **Step 5: verify + commit**

`make generate SERVICE=tcard-service` (if not already run), `make test SERVICE=tcard-service`, `make lint`, `make test-integration SERVICE=tcard-service` — all green. Every new comment ≤2 lines. Commit:

```bash
git add tcard-service/
git commit -m "feat(tcard-service): POST /api/v1/cards/register with validation"
```

---

## Notes for the implementer

- `TestRefreshEndToEnd` is edited in both tasks by design: Task 1 gives its fixtures a `cardVersion` and drops `path` from the payload assertions (keeping path-only URLs, which still resolve while the cache is version-unaware); Task 2 flips only its request URLs to the `@`-form. This keeps every commit green.
- If `make test-integration` cannot reach Docker in your environment, the store changes (Task 1) and the end-to-end URL flip (Task 2 Step 3) are the only integration-covered pieces; everything else is verified by `make test`. Do not skip integration silently — report it if Docker is unavailable.
