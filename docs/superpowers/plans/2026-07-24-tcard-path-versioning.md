# tcard-service `_tcardVersion` + slash paths + listings Implementation Plan

> **SUPERSEDED (historical working artifact).** This plan was fully executed, but three
> post-review product decisions changed the final behavior; the design spec
> (`docs/superpowers/specs/2026-07-24-tcard-path-versioning-design.md`) is authoritative:
> 1. Exact card path without a version → **400** BadRequest (Task 6 here says 404).
> 2. A never-loaded cache → **404** "no paths or cards exist" for every listing (this plan's
>    `handleList` has no `Ready()` gate).
> 3. **No legacy-index drop** (Task 2 here is reverted): the old `(path, cardVersion)` index
>    was removed manually from the production collection; `EnsureIndexes` only creates the
>    `(path, _tcardVersion)` unique index.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the card version field to `_tcardVersion` on every wire/storage surface, make `GET /api/v1/cards/...` match slash-containing card paths, and add cache-derived directory listings when the URL lacks the `.template.json` suffix.

**Architecture:** All changes inside `tcard-service/` (flat `package main`). The Gin route becomes a wildcard (`*file`); the handler branches on the `.template.json` suffix — template flow (unchanged semantics) vs a new listing flow computed by scanning the existing in-memory cache snapshot. The Mongo store reads/writes `_tcardVersion` and `EnsureIndexes` drops the legacy `(path, cardVersion)` index.

**Tech Stack:** Go 1.25, Gin, mongo-driver v2, testify, go.uber.org/mock (mocks unchanged — the `CardStore` interface signatures do not change), testcontainers via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-07-24-tcard-path-versioning-design.md` (approved).

## Global Constraints

- Always use `make` targets, never raw `go` commands: `make test SERVICE=tcard-service`, `make test-integration SERVICE=tcard-service`, `make fmt`, `make lint`, `make sast`.
- TDD Red-Green-Refactor for every task; run the failing test before implementing.
- Coverage floor 80% (target 90%+) for the package.
- Errors: Tier-1 errcode discipline — handlers return `errcode.BadRequest`/`NotFound`/`Conflict` via `errhttp.Write`; infra failures stay raw `fmt.Errorf("...: %w", err)`. Never log AND return.
- Go identifiers (`CardVersion`, `cardKey.cardVersion`) keep their names — only JSON/BSON/wire names change to `_tcardVersion`.
- No `docs/client-api.md` change (tcard-service is not a client-facing NATS handler nor auth-service HTTP).
- `make generate` NOT needed — the `CardStore` interface is unchanged.
- Branch: `claude/tcard-service-path-versioning-7xmv9w`. Commit after each task. Committer already configured (`Claude <noreply@anthropic.com>`).
- Every commit message ends with the two trailer lines already used on this branch (Co-Authored-By + Claude-Session).
- Integration tests require Docker. If Docker is unavailable in the environment, still write the integration tests, run `go vet`-level compile via `make lint`, and note the skip in the commit message; run them before push if possible.

---

### Task 1: Rename the stored field to `_tcardVersion` (store layer)

**Files:**
- Create: `tcard-service/store_mongo_test.go` (unit tests for `docToCard`)
- Modify: `tcard-service/store.go` (struct tags)
- Modify: `tcard-service/store_mongo.go` (all field references + index keys)
- Modify: `tcard-service/integration_test.go` (inserted docs + expectations)
- Modify: `tcard-service/cache_test.go` (fixture template content only)

**Interfaces:**
- Consumes: existing `docToCard(doc bson.D) (card, bool, error)`, `card` struct.
- Produces: store reads/writes BSON field `_tcardVersion`; `docToCard` keys on `_tcardVersion`; unique index `(path, _tcardVersion)` (name `path_1__tcardVersion_1`). Later tasks rely on served template payloads embedding `"_tcardVersion"`.

- [ ] **Step 1: Write the failing unit test** — create `tcard-service/store_mongo_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestDocToCard(t *testing.T) {
	tests := []struct {
		name        string
		doc         bson.D
		wantOK      bool
		wantPath    string
		wantVersion string
		wantJSON    string
	}{
		{
			name: "keys on _tcardVersion, strips path, keeps _tcardVersion in payload",
			doc: bson.D{
				{Key: "path", Value: "greetings/en/welcome"},
				{Key: "_tcardVersion", Value: "1.0.0"},
				{Key: "title", Value: "Hi"},
			},
			wantOK: true, wantPath: "greetings/en/welcome", wantVersion: "1.0.0",
			wantJSON: `{"_tcardVersion":"1.0.0","title":"Hi"}`,
		},
		{
			name: "legacy cardVersion key is no longer recognized",
			doc: bson.D{
				{Key: "path", Value: "greetings/en/welcome"},
				{Key: "cardVersion", Value: "1.0.0"},
			},
			wantOK: false,
		},
		{
			name:   "missing path is skipped",
			doc:    bson.D{{Key: "_tcardVersion", Value: "1.0.0"}, {Key: "title", Value: "x"}},
			wantOK: false,
		},
		{
			name:   "missing _tcardVersion is skipped",
			doc:    bson.D{{Key: "path", Value: "a/b/c"}, {Key: "title", Value: "x"}},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, ok, err := docToCard(tt.doc)
			require.NoError(t, err)
			require.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				return
			}
			assert.Equal(t, tt.wantPath, c.Path)
			assert.Equal(t, tt.wantVersion, c.CardVersion)
			assert.JSONEq(t, tt.wantJSON, string(c.Template))
		})
	}
}
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `make test SERVICE=tcard-service`
Expected: FAIL — `TestDocToCard` subtests "keys on _tcardVersion..." (ok=false because current code looks for `cardVersion`) and "legacy cardVersion key..." (ok=true under current code).

- [ ] **Step 3: Implement the rename in `store_mongo.go`**

In `EnsureIndexes`, change the index keys and error text:

```go
	if _, err := s.cards.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "path", Value: 1}, {Key: "_tcardVersion", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("ensure cards (path, _tcardVersion) unique index: %w", err)
	}
```

In `ListCards`, update the skip warning:

```go
			slog.Warn("card document missing a string path or _tcardVersion, skipping")
```

In `GetCard`, change the filter:

```go
	filter := bson.D{{Key: "path", Value: path}, {Key: "_tcardVersion", Value: cardVersion}}
```

In `docToCard`, change the switch case (comment updates too):

```go
		case "_tcardVersion":
			cardVersion, _ = e.Value.(string)
			payload = append(payload, e)
```

In `ListVersions`, change projection and lookup:

```go
	proj := options.Find().SetProjection(bson.D{{Key: "_tcardVersion", Value: 1}, {Key: "_id", Value: 0}})
	...
		if v, ok := cursor.Current.Lookup("_tcardVersion").StringValueOK(); ok {
```

In `InsertCard`, change the stored key:

```go
	d := bson.M{
		"path": doc.Path, "_tcardVersion": doc.CardVersion,
		"type": doc.Type, "schema": doc.Schema, "version": doc.Version, "body": body,
	}
```

In `store.go`, update the `card` struct tags (the `cardDoc` JSON tag is Task 3, do NOT touch it here):

```go
type card struct {
	Path        string          `json:"path" bson:"path"`
	CardVersion string          `json:"_tcardVersion" bson:"_tcardVersion"`
	Template    json.RawMessage `json:"template" bson:"template"`
}
```

- [ ] **Step 4: Update the unit fixtures in `cache_test.go`** — the template content mirrors what `docToCard` now emits:

```go
var (
	homeCard = card{
		Path: "home", CardVersion: "v1",
		Template: json.RawMessage(`{"_tcardVersion":"v1","title":"Home","widgets":["news","weather"]}`),
	}
	profileCard = card{
		Path: "profile", CardVersion: "v2",
		Template: json.RawMessage(`{"_tcardVersion":"v2","title":"Profile"}`),
	}
)
```

Also in `TestCardCache_SamePathDifferentVersionsCoexist` and `TestCardCache_AddConcurrentWithLoad`, replace the inline `"cardVersion"` keys in template JSON with `"_tcardVersion"`:

```go
	homeV2 := card{Path: "home", CardVersion: "v2", Template: json.RawMessage(`{"_tcardVersion":"v2","title":"Home v2"}`)}
```

```go
	extra := card{Path: "extra", CardVersion: "1.0.0", Template: json.RawMessage(`{"_tcardVersion":"1.0.0"}`)}
```

- [ ] **Step 5: Run unit tests, verify pass**

Run: `make test SERVICE=tcard-service`
Expected: PASS (`TestDocToCard` green; all cache/handler tests still green — they assert on fixture content, not field names).

- [ ] **Step 6: Update `integration_test.go` for the new field name**

In `TestMongoCardStore_ListCards`: every inserted doc's `"cardVersion"` key becomes `"_tcardVersion"` (docs `c-home`, `c-profile`, `c-no-path`, `c-bad-path`; `c-no-version` keeps having no version field). Both `assert.JSONEq` template expectations change `"cardVersion"` to `"_tcardVersion"`. The `require.Len` message becomes "docs missing a string path or _tcardVersion are skipped".

In `TestMongoCardStore_EnsureIndexes_UniquePathVersion`: all three inserted docs use `"_tcardVersion"` instead of `"cardVersion"` (messages updated accordingly).

In `TestRefreshEndToEnd`: both inserted docs use `"_tcardVersion"`; both `assert.JSONEq` bodies become `{"_tcardVersion":"v1","title":"Home"}` / `{"_tcardVersion":"v1","title":"Profile"}`.

(`TestRegisterEndToEnd` posts through the API — it changes in Task 3, not here.)

- [ ] **Step 7: Run integration tests** (requires Docker)

Also update `TestRegisterEndToEnd`'s template expectation: the `assert.JSONEq` body changes `"cardVersion":"1.0.0"` to `"_tcardVersion":"1.0.0"`. Do NOT change the `mk()` request body in this task — `cardDoc` still binds the JSON field `cardVersion` until Task 3, and `InsertCard` now stores it under `_tcardVersion`, so the round trip works.

Run: `make test-integration SERVICE=tcard-service`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add tcard-service/
git commit -m "refactor(tcard-service): store card version as _tcardVersion in Mongo"
```

---

### Task 2: `EnsureIndexes` drops the legacy `(path, cardVersion)` index

**Files:**
- Modify: `tcard-service/store_mongo.go` (`EnsureIndexes` + new helper `isIndexAbsent`)
- Modify: `tcard-service/integration_test.go` (new test)

**Interfaces:**
- Produces: `EnsureIndexes` is idempotent, drops index named `path_1_cardVersion_1` when present, tolerates Mongo error codes 26 (NamespaceNotFound) and 27 (IndexNotFound).

- [ ] **Step 1: Write the failing integration test** — add to `tcard-service/integration_test.go` (add `"go.mongodb.org/mongo-driver/v2/mongo"` and `"go.mongodb.org/mongo-driver/v2/mongo/options"` to imports):

```go
// A deployment that ran the old code has a unique index on (path, cardVersion).
// With the field renamed, that index sees every doc as (path, null) and rejects
// the second insert for any path — EnsureIndexes must drop it.
func TestMongoCardStore_EnsureIndexes_DropsLegacyIndex(t *testing.T) {
	db := testutil.MongoDB(t, "tcard")
	store := newMongoCardStore(db)
	ctx := context.Background()

	_, err := db.Collection("cards").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "path", Value: 1}, {Key: "cardVersion", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	require.NoError(t, store.EnsureIndexes(ctx))
	require.NoError(t, store.EnsureIndexes(ctx), "EnsureIndexes must stay idempotent")

	_, err = db.Collection("cards").InsertOne(ctx, bson.M{"path": "a/b/c", "_tcardVersion": "1.0.0"})
	require.NoError(t, err)
	_, err = db.Collection("cards").InsertOne(ctx, bson.M{"path": "a/b/c", "_tcardVersion": "1.0.1"})
	require.NoError(t, err, "second version for a path must not collide as (path, null)")

	cur, err := db.Collection("cards").Indexes().List(ctx)
	require.NoError(t, err)
	var names []string
	for cur.Next(ctx) {
		names = append(names, cur.Current.Lookup("name").StringValue())
	}
	require.NoError(t, cur.Err())
	assert.NotContains(t, names, "path_1_cardVersion_1")
	assert.Contains(t, names, "path_1__tcardVersion_1")
}
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `make test-integration SERVICE=tcard-service`
Expected: FAIL — the second `InsertOne` errors (duplicate `(path, null)` under the legacy index) and `names` still contains `path_1_cardVersion_1`.

- [ ] **Step 3: Implement the drop** — in `store_mongo.go`, `EnsureIndexes` becomes:

```go
// EnsureIndexes enforces (path, _tcardVersion) uniqueness so two docs can't
// claim one version, and drops the legacy (path, cardVersion) index — with the
// field renamed it would treat every doc as (path, null) and block inserts.
// The data-type `version` field is unrelated and is not indexed.
func (s *mongoCardStore) EnsureIndexes(ctx context.Context) error {
	if err := s.cards.Indexes().DropOne(ctx, "path_1_cardVersion_1"); err != nil && !isIndexAbsent(err) {
		return fmt.Errorf("drop legacy cards (path, cardVersion) index: %w", err)
	}
	if _, err := s.cards.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "path", Value: 1}, {Key: "_tcardVersion", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("ensure cards (path, _tcardVersion) unique index: %w", err)
	}
	return nil
}

// isIndexAbsent matches IndexNotFound (27) and NamespaceNotFound (26): there is
// nothing to drop on a fresh database or collection.
func isIndexAbsent(err error) bool {
	var cmdErr mongo.CommandError
	return errors.As(err, &cmdErr) && (cmdErr.Code == 26 || cmdErr.Code == 27)
}
```

- [ ] **Step 4: Run integration tests, verify pass**

Run: `make test-integration SERVICE=tcard-service`
Expected: PASS (including the other EnsureIndexes test on a fresh collection — codes 26/27 tolerated).

- [ ] **Step 5: Commit**

```bash
git add tcard-service/
git commit -m "fix(tcard-service): drop legacy (path, cardVersion) index in EnsureIndexes"
```

---

### Task 3: Register API accepts `_tcardVersion`

**Files:**
- Modify: `tcard-service/store.go` (`cardDoc` JSON tag)
- Modify: `tcard-service/handler.go` (validation messages)
- Modify: `tcard-service/handler_test.go` (`validCardJSON`, new validation case)
- Modify: `tcard-service/integration_test.go` (`TestRegisterEndToEnd.mk`)

**Interfaces:**
- Produces: `POST /api/v1/cards/register` binds the version from JSON field `"_tcardVersion"`; a body using the legacy `"cardVersion"` key gets 400 "_tcardVersion is required".

- [ ] **Step 1: Write the failing tests** — in `handler_test.go`, change `validCardJSON`:

```go
const validCardJSON = `{"path":"welcome","_tcardVersion":"1.0.0","type":"AdaptiveCard",` +
	`"schema":"http://adaptivecards.io/schemas/adaptive-card.json","version":"1.5",` +
	`"body":[{"type":"TextBlock","text":"Hi"}]}`
```

In `TestHandleRegister_ValidationErrors`, update the two version-mangling cases to the new field name and add a legacy-field case:

```go
		{name: "non-semver _tcardVersion", body: strings.Replace(validCardJSON, `"_tcardVersion":"1.0.0"`, `"_tcardVersion":"1.0"`, 1)},
		{name: "leading-zero _tcardVersion", body: strings.Replace(validCardJSON, `"_tcardVersion":"1.0.0"`, `"_tcardVersion":"1.0.01"`, 1)},
		{name: "legacy cardVersion field is not accepted", body: strings.Replace(validCardJSON, `"_tcardVersion"`, `"cardVersion"`, 1)},
```

- [ ] **Step 2: Run tests to verify failure**

Run: `make test SERVICE=tcard-service`
Expected: FAIL — register happy-path tests 400 (binder no longer sees a version under the old tag... it does: the tag is still `cardVersion`, so `validCardJSON`'s `_tcardVersion` key is ignored → validation "cardVersion is required" → 400 in `TestHandleRegister_HappyPath`), and "legacy cardVersion field is not accepted" FAILS because the legacy key currently binds fine (201-path).

- [ ] **Step 3: Implement** — in `store.go`:

```go
type cardDoc struct {
	Path        string          `json:"path"`
	CardVersion string          `json:"_tcardVersion"`
	CardUsage   json.RawMessage `json:"cardUsage"`
	Type        string          `json:"type"`
	Schema      string          `json:"schema"`
	Version     string          `json:"version"`
	Body        json.RawMessage `json:"body"`
}
```

In `handler.go`, update the user-facing strings (identifiers unchanged):

```go
	case doc.CardVersion == "":
		return errcode.BadRequest("_tcardVersion is required")
```

```go
	if _, ok := parseSemver(doc.CardVersion); !ok {
		return errcode.BadRequest("_tcardVersion must be a semantic version a.b.c")
	}
```

```go
	if !isHighest(doc.CardVersion, versions) {
		errhttp.Write(ctx, c, errcode.Conflict("_tcardVersion must be the highest for this path"))
		return
	}
```

Also update the doc comments in `handler.go` that say `{path}@{cardVersion}` — the URL grammar keeps the same shape; reword to `{path}@{version}` where touched (`templateSuffix` comment, `HandleGetTemplate` comment, bad-request message strings may keep "cardVersion" replaced with "version"):

```go
// templateSuffix is the required template filename suffix:
// GET /api/v1/cards/{path}@{version}.template.json.
```

```go
		errhttp.Write(ctx, c, errcode.BadRequest("card template file must be named {path}@{version}"+templateSuffix))
```

```go
		errhttp.Write(ctx, c, errcode.BadRequest("card template request must include a version: {path}@{version}"+templateSuffix))
```

```go
		errhttp.Write(ctx, c, errcode.BadRequest("card template path and version must both be non-empty"))
```

- [ ] **Step 4: Run unit tests, verify pass**

Run: `make test SERVICE=tcard-service`
Expected: PASS.

- [ ] **Step 5: Update the integration register body** — in `integration_test.go` `TestRegisterEndToEnd`, the `mk` helper's `"cardVersion"` key becomes `"_tcardVersion"`:

```go
		return `{"path":"` + path + `","_tcardVersion":"` + version + `","type":"AdaptiveCard",` +
			`"schema":"http://adaptivecards.io/schemas/adaptive-card.json","version":"1.5",` +
			`"body":[{"type":"TextBlock","text":"Hi","maxLines":9007199254740993}],"cardUsage":"greeting"}`
```

Run: `make test-integration SERVICE=tcard-service`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add tcard-service/
git commit -m "feat(tcard-service): register API takes _tcardVersion field"
```

---

### Task 4: Wildcard route + slash-path template flow

**Files:**
- Modify: `tcard-service/routes.go`
- Modify: `tcard-service/handler.go` (trim wildcard param)
- Modify: `tcard-service/handler_test.go` (new fixture + tests)

**Interfaces:**
- Produces: route `GET /api/v1/cards/*file`; `HandleGetTemplate` normalizes `c.Param("file")` with `strings.Trim(file, "/")`. Template lookups for paths containing `/` work end to end. Listing does not exist yet — a suffix-less URL still returns the existing 400.

- [ ] **Step 1: Write the failing tests** — add to `handler_test.go`:

```go
// deepCard exercises slash-containing card paths (a/b/c@version).
var deepCard = card{
	Path: "greetings/en/welcome", CardVersion: "0.0.1",
	Template: json.RawMessage(`{"_tcardVersion":"0.0.1","title":"Welcome"}`),
}

func TestHandleGetTemplate_SlashPath(t *testing.T) {
	r := setupRouter(t, NewCardHandler(cacheWith(deepCard), nil))

	w := doRequest(t, r, http.MethodGet, "/api/v1/cards/greetings/en/welcome@0.0.1.template.json")
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, string(deepCard.Template), w.Body.String())

	miss := doRequest(t, r, http.MethodGet, "/api/v1/cards/greetings/en/welcome@9.9.9.template.json")
	require.Equal(t, http.StatusNotFound, miss.Code)
	errtest.AssertCode(t, miss.Body.Bytes(), errcode.CodeNotFound)
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `make test SERVICE=tcard-service`
Expected: FAIL — `TestHandleGetTemplate_SlashPath` gets 404 from Gin (no route matches the multi-segment URL), and the body is not the errcode envelope (`errtest.AssertCode` fails on the miss case too).

- [ ] **Step 3: Implement** — `routes.go`:

```go
func registerRoutes(r *gin.Engine, h *CardHandler) {
	// Refresh/register are POST-only (separate method tree, no conflict with the
	// GET wildcard). The wildcard matches slash-containing card paths.
	r.POST("/api/v1/cards/register", h.HandleRegister)
	r.POST("/api/v1/cards/refresh", h.HandleRefresh)
	r.GET("/api/v1/cards/*file", h.HandleGetTemplate)
	r.GET("/healthz", h.HandleHealth)
	r.GET("/readyz", h.HandleReady)
}
```

In `HandleGetTemplate`, normalize the wildcard param (it arrives with a leading `/`; a trailing `/` is tolerated):

```go
	file := strings.Trim(c.Param("file"), "/")
```

(The rest of the function is unchanged in this task.)

- [ ] **Step 4: Run tests, verify pass**

Run: `make test SERVICE=tcard-service`
Expected: PASS — including the pre-existing single-segment tests (`home@v1.template.json`) and `TestHandleRefresh_GetIsNotRouted` (still 400: suffix-less URLs keep the old behavior until Task 6).

- [ ] **Step 5: Commit**

```bash
git add tcard-service/
git commit -m "feat(tcard-service): wildcard route serves slash-containing card paths"
```

---

### Task 5: `cardCache.List` — directory scan over the snapshot

**Files:**
- Modify: `tcard-service/cache.go` (new type + method)
- Modify: `tcard-service/cache_test.go` (new tests)

**Interfaces:**
- Consumes: `cardCache.entries` snapshot, `parseSemver`/`semver.greater` from `semver.go`.
- Produces (Task 6 depends on these exact names):

```go
type listResult struct {
	cards     []string // full "path@version" entries, sorted
	folders   []string // full folder paths, sorted, deduped
	exactPath bool     // prefix equals a cached card path (version missing)
	found     bool     // at least one card or folder entry matched
}

func (c *cardCache) List(prefix string) listResult
```

`prefix` is already trimmed of leading/trailing `/` by the caller; `""` means root. `cards`/`folders` are always non-nil (serialize as `[]`). A nil (never-loaded) snapshot returns the zero result with empty slices.

- [ ] **Step 1: Write the failing tests** — add to `cache_test.go`:

```go
// listSeed is a depth-3 hierarchy with multiple versions of one card.
func listSeed() *cardCache {
	return cacheWith(
		card{Path: "a/b/c", CardVersion: "0.0.1", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/c", CardVersion: "0.0.2", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/c", CardVersion: "0.0.10", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/d", CardVersion: "1.0.0", Template: json.RawMessage(`{}`)},
		card{Path: "a/x/y", CardVersion: "2.0.0", Template: json.RawMessage(`{}`)},
		card{Path: "z/w/v", CardVersion: "1.2.3", Template: json.RawMessage(`{}`)},
	)
}

func TestCardCache_List(t *testing.T) {
	tests := []struct {
		name          string
		prefix        string
		wantCards     []string
		wantFolders   []string
		wantExact     bool
		wantFound     bool
	}{
		{
			name: "root lists first segments as folders", prefix: "",
			wantCards: []string{}, wantFolders: []string{"a", "z"}, wantFound: true,
		},
		{
			name: "one segment lists two-segment folders", prefix: "a",
			wantCards: []string{}, wantFolders: []string{"a/b", "a/x"}, wantFound: true,
		},
		{
			name: "two segments list cards with every version in semver order", prefix: "a/b",
			wantCards:   []string{"a/b/c@0.0.1", "a/b/c@0.0.2", "a/b/c@0.0.10", "a/b/d@1.0.0"},
			wantFolders: []string{}, wantFound: true,
		},
		{
			name: "full card path without version is an exact hit", prefix: "a/b/c",
			wantCards: []string{}, wantFolders: []string{}, wantExact: true,
		},
		{
			name: "unknown prefix finds nothing", prefix: "nope",
			wantCards: []string{}, wantFolders: []string{},
		},
		{
			name: "prefix deeper than any card finds nothing", prefix: "a/b/c/x",
			wantCards: []string{}, wantFolders: []string{},
		},
		{
			name: "partial segment is not a prefix match", prefix: "a/b/cc",
			wantCards: []string{}, wantFolders: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := listSeed().List(tt.prefix)
			assert.Equal(t, tt.wantCards, res.cards)
			assert.Equal(t, tt.wantFolders, res.folders)
			assert.Equal(t, tt.wantExact, res.exactPath)
			assert.Equal(t, tt.wantFound, res.found)
		})
	}
}

func TestCardCache_List_EmptyAndUnloaded(t *testing.T) {
	t.Run("never-loaded cache lists nothing", func(t *testing.T) {
		res := newCardCache().List("")
		assert.Equal(t, []string{}, res.cards)
		assert.Equal(t, []string{}, res.folders)
		assert.False(t, res.found)
		assert.False(t, res.exactPath)
	})

	t.Run("loaded-but-empty cache lists nothing at root", func(t *testing.T) {
		res := cacheWith().List("")
		assert.Equal(t, []string{}, res.cards)
		assert.Equal(t, []string{}, res.folders)
		assert.False(t, res.found)
	})
}

// Non-semver versions fall back to lexicographic order within a path.
func TestCardCache_List_NonSemverVersionOrder(t *testing.T) {
	c := cacheWith(
		card{Path: "a/b/c", CardVersion: "v2", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/c", CardVersion: "v1", Template: json.RawMessage(`{}`)},
	)
	res := c.List("a/b")
	assert.Equal(t, []string{"a/b/c@v1", "a/b/c@v2"}, res.cards)
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `make test SERVICE=tcard-service`
Expected: FAIL to compile — `List`/`listResult` undefined.

- [ ] **Step 3: Implement** — add to `cache.go` (add `"sort"` and `"strings"` to imports):

```go
// listResult is the outcome of scanning the snapshot for one prefix's direct
// children (design spec §4: generic over depth, entries are full paths).
type listResult struct {
	cards     []string // "path@version", one per cached version, sorted
	folders   []string // full folder paths, deduped, sorted
	exactPath bool     // prefix names a cached card path exactly (no version)
	found     bool     // at least one card or folder matched under prefix
}

// List returns the direct children of prefix ("" = root) in the current
// snapshot. Lock-free: one atomic load, then a pure scan. The caller passes
// prefix already trimmed of leading/trailing slashes.
func (c *cardCache) List(prefix string) listResult {
	res := listResult{cards: []string{}, folders: []string{}}
	m := c.entries.Load()
	if m == nil {
		return res
	}

	type cardEntry struct{ path, version string }
	var cards []cardEntry
	folderSet := make(map[string]struct{})
	for k := range *m {
		if k.path == prefix {
			res.exactPath = true
			continue
		}
		rest := k.path
		if prefix != "" {
			if !strings.HasPrefix(k.path, prefix+"/") {
				continue
			}
			rest = k.path[len(prefix)+1:]
		}
		if i := strings.Index(rest, "/"); i >= 0 {
			folder := rest[:i]
			if prefix != "" {
				folder = prefix + "/" + folder
			}
			folderSet[folder] = struct{}{}
		} else {
			cards = append(cards, cardEntry{path: k.path, version: k.cardVersion})
		}
	}

	// Cards sort by path, then semver order; non-semver versions fall back to
	// lexicographic so the output stays deterministic.
	sort.Slice(cards, func(i, j int) bool {
		if cards[i].path != cards[j].path {
			return cards[i].path < cards[j].path
		}
		vi, oki := parseSemver(cards[i].version)
		vj, okj := parseSemver(cards[j].version)
		if oki && okj {
			return vj.greater(vi)
		}
		return cards[i].version < cards[j].version
	})
	for _, e := range cards {
		res.cards = append(res.cards, e.path+"@"+e.version)
	}
	for f := range folderSet {
		res.folders = append(res.folders, f)
	}
	sort.Strings(res.folders)
	res.found = len(res.cards) > 0 || len(res.folders) > 0
	return res
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `make test SERVICE=tcard-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tcard-service/
git commit -m "feat(tcard-service): cache List scans snapshot for cards/folders under a prefix"
```

---

### Task 6: Listing handler flow (no `.template.json` suffix)

**Files:**
- Modify: `tcard-service/handler.go` (suffix branch + `handleList` + `listResponse`)
- Modify: `tcard-service/handler_test.go` (new tests; two existing expectations change)

**Interfaces:**
- Consumes: `cardCache.List(prefix) listResult` from Task 5.
- Produces: suffix-less GETs return `200 {"statusCode":200,"cards":[...],"folders":[...]}` or the two 404s: `no version specified for card "<prefix>"` (exact path) and `given path "<prefix>" for card list not found` (no match, non-root). Root is always 200.

- [ ] **Step 1: Write the failing tests** — add to `handler_test.go`:

```go
func listRouter(t *testing.T) *gin.Engine {
	t.Helper()
	return setupRouter(t, NewCardHandler(cacheWith(
		card{Path: "a/b/c", CardVersion: "0.0.1", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/c", CardVersion: "0.0.2", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/d", CardVersion: "1.0.0", Template: json.RawMessage(`{}`)},
		card{Path: "a/x/y", CardVersion: "2.0.0", Template: json.RawMessage(`{}`)},
		card{Path: "z/w/v", CardVersion: "1.2.3", Template: json.RawMessage(`{}`)},
	), nil))
}

func TestHandleList(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		wantBody string
	}{
		{
			name:   "root lists top-level folders",
			target: "/api/v1/cards/",
			wantBody: `{"statusCode":200,"cards":[],"folders":["a","z"]}`,
		},
		{
			name:   "one segment lists subfolders",
			target: "/api/v1/cards/a",
			wantBody: `{"statusCode":200,"cards":[],"folders":["a/b","a/x"]}`,
		},
		{
			name:   "trailing slash is normalized",
			target: "/api/v1/cards/a/",
			wantBody: `{"statusCode":200,"cards":[],"folders":["a/b","a/x"]}`,
		},
		{
			name:   "two segments list cards with versions",
			target: "/api/v1/cards/a/b",
			wantBody: `{"statusCode":200,"cards":["a/b/c@0.0.1","a/b/c@0.0.2","a/b/d@1.0.0"],"folders":[]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := doRequest(t, listRouter(t), http.MethodGet, tt.target)
			require.Equal(t, http.StatusOK, w.Code)
			assert.JSONEq(t, tt.wantBody, w.Body.String())
		})
	}
}

func TestHandleList_Errors(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		wantMsg string
	}{
		{
			name:    "full card path without version",
			target:  "/api/v1/cards/a/b/c",
			wantMsg: `no version specified for card "a/b/c"`,
		},
		{
			name:    "unknown prefix",
			target:  "/api/v1/cards/does/not/exist",
			wantMsg: `given path "does/not/exist" for card list not found`,
		},
		{
			name:    "version without .template.json suffix is not a listing match",
			target:  "/api/v1/cards/a/b/c@0.0.1",
			wantMsg: `given path "a/b/c@0.0.1" for card list not found`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := doRequest(t, listRouter(t), http.MethodGet, tt.target)
			require.Equal(t, http.StatusNotFound, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeNotFound)
			assert.Contains(t, w.Body.String(), tt.wantMsg)
		})
	}
}

func TestHandleList_RootAlwaysOK(t *testing.T) {
	t.Run("loaded but empty cache", func(t *testing.T) {
		r := setupRouter(t, NewCardHandler(cacheWith(), nil))
		w := doRequest(t, r, http.MethodGet, "/api/v1/cards/")
		require.Equal(t, http.StatusOK, w.Code)
		assert.JSONEq(t, `{"statusCode":200,"cards":[],"folders":[]}`, w.Body.String())
	})

	t.Run("never-loaded cache behaves as empty", func(t *testing.T) {
		r := setupRouter(t, NewCardHandler(newCardCache(), nil))
		w := doRequest(t, r, http.MethodGet, "/api/v1/cards/")
		require.Equal(t, http.StatusOK, w.Code)
		assert.JSONEq(t, `{"statusCode":200,"cards":[],"folders":[]}`, w.Body.String())
	})
}
```

Update two existing tests whose behavior changes (suffix-less URLs are now listings):

`TestHandleRefresh_GetIsNotRouted` — a GET to `/refresh` is now a listing miss:

```go
// Refresh is POST-only: a GET falls through to the wildcard route and is a
// listing lookup for the unknown prefix "refresh".
func TestHandleRefresh_GetIsNotRouted(t *testing.T) {
	r := setupRouter(t, NewCardHandler(cacheWith(), nil))
	w := doRequest(t, r, http.MethodGet, "/api/v1/cards/refresh")
	require.Equal(t, http.StatusNotFound, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeNotFound)
}
```

In `TestHandleGetTemplate_Errors`, the row "filename without the .template.json suffix is a bad request" (`/api/v1/cards/home@v1.json`) becomes a listing 404 — change that row to:

```go
		{
			name:     "filename without the .template.json suffix is a listing miss",
			target:   "/api/v1/cards/home@v1.json",
			wantCode: http.StatusNotFound,
			wantErr:  errcode.CodeNotFound,
		},
```

- [ ] **Step 2: Run tests to verify failure**

Run: `make test SERVICE=tcard-service`
Expected: FAIL — new listing tests get 400 (old suffix-less branch), updated rows fail on 400 vs 404.

- [ ] **Step 3: Implement** — in `handler.go` (add `"fmt"` already imported), add the response type next to `refreshResponse`:

```go
// listResponse is the directory-listing payload for suffix-less GETs: the
// direct children of the requested prefix as full paths from root.
type listResponse struct {
	StatusCode int      `json:"statusCode"`
	Cards      []string `json:"cards"`
	Folders    []string `json:"folders"`
}
```

Rework the top of `HandleGetTemplate` to branch on the suffix:

```go
// HandleGetTemplate serves the cached card for {path}@{version}.template.json
// (a lock-free lookup, no Mongo read); without the suffix it serves a
// directory listing of the prefix instead.
func (h *CardHandler) HandleGetTemplate(c *gin.Context) {
	ctx := reqCtx(c)

	file := strings.Trim(c.Param("file"), "/")
	if !strings.HasSuffix(file, templateSuffix) {
		h.handleList(ctx, c, file)
		return
	}
	spec := strings.TrimSuffix(file, templateSuffix)
	if spec == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("card template file name is empty"))
		return
	}
	...
```

(The `spec == file` check and its 400 disappear — the suffix test above replaces it. Everything from `at := strings.LastIndex(spec, "@")` down is unchanged.)

Add the listing handler:

```go
// handleList serves the directory listing for prefix from the cache snapshot.
// Root ("") is always 200; an exact card path without a version and an unknown
// prefix are the two 404 cases (design spec §4).
func (h *CardHandler) handleList(ctx context.Context, c *gin.Context, prefix string) {
	res := h.cache.List(prefix)
	if res.exactPath {
		errhttp.Write(ctx, c, errcode.NotFound(fmt.Sprintf("no version specified for card %q", prefix)))
		return
	}
	if !res.found && prefix != "" {
		errhttp.Write(ctx, c, errcode.NotFound(fmt.Sprintf("given path %q for card list not found", prefix)))
		return
	}
	c.JSON(http.StatusOK, listResponse{StatusCode: http.StatusOK, Cards: res.cards, Folders: res.folders})
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `make test SERVICE=tcard-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tcard-service/
git commit -m "feat(tcard-service): directory listing for card prefixes without .template.json"
```

---

### Task 7: Register path validation — exactly 3 segments, slashes allowed, no `@`

**Files:**
- Modify: `tcard-service/handler.go` (`validateRegister`)
- Modify: `tcard-service/handler_test.go` (`validCardJSON` path, mock expectations, validation table)
- Modify: `tcard-service/integration_test.go` (`TestRegisterEndToEnd` paths; remove the `a@b` sub-case)

**Interfaces:**
- Produces: `validateRegister` accepts only paths of exactly 3 non-empty `/`-separated segments with no `@`; error messages: `path must have exactly 3 segments a/b/c`, `path segments must be non-empty`, `path must not contain '@'`.

- [ ] **Step 1: Write the failing tests** — in `handler_test.go`, change `validCardJSON`'s path to a 3-segment one:

```go
const validCardJSON = `{"path":"greetings/en/welcome","_tcardVersion":"1.0.0","type":"AdaptiveCard",` +
	`"schema":"http://adaptivecards.io/schemas/adaptive-card.json","version":"1.5",` +
	`"body":[{"type":"TextBlock","text":"Hi"}]}`
```

Update every mock expectation that referenced the old path — in `TestHandleRegister_HappyPath`, `TestHandleRegister_NotHighest`, `TestHandleRegister_Duplicate`, `TestHandleRegister_CacheAddFailureStill201`:

```go
	store.EXPECT().ListVersions(gomock.Any(), "greetings/en/welcome").Return(nil, nil)
```

and in `TestHandleRegister_HappyPath`:

```go
	store.EXPECT().GetCard(gomock.Any(), "greetings/en/welcome", "1.0.0").
		Return(card{Path: "greetings/en/welcome", CardVersion: "1.0.0", Template: json.RawMessage(`{}`)}, true, nil)
```

Also update the missing-path case and replace the "path with slash" case with the new shape rules in `TestHandleRegister_ValidationErrors`:

```go
		{name: "missing path", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":""`, 1)},
		{name: "single-segment path", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"welcome"`, 1)},
		{name: "two-segment path", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"greetings/welcome"`, 1)},
		{name: "four-segment path", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"a/b/c/d"`, 1)},
		{name: "empty path segment", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"a//c"`, 1)},
		{name: "leading slash", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"/a/b"`, 1)},
		{name: "trailing slash", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"a/b/"`, 1)},
		{name: "path with @", body: strings.Replace(validCardJSON, `"path":"greetings/en/welcome"`, `"path":"a/b/c@d"`, 1)},
```

- [ ] **Step 2: Run tests to verify failure**

Run: `make test SERVICE=tcard-service`
Expected: FAIL — register happy-path tests 400 ("path must not contain '/'"), and the new validation rows for 1/2-segment paths FAIL (they currently pass validation).

- [ ] **Step 3: Implement** — in `validateRegister`, replace the slash rejection:

```go
	if strings.Contains(doc.Path, "@") {
		return errcode.BadRequest("path must not contain '@'")
	}
	segments := strings.Split(doc.Path, "/")
	if len(segments) != 3 {
		return errcode.BadRequest("path must have exactly 3 segments a/b/c")
	}
	for _, s := range segments {
		if s == "" {
			return errcode.BadRequest("path segments must be non-empty")
		}
	}
```

Update the `validateRegister` doc comment to mention "3-segment path" instead of "path safety".

- [ ] **Step 4: Run unit tests, verify pass**

Run: `make test SERVICE=tcard-service`
Expected: PASS.

- [ ] **Step 5: Update `TestRegisterEndToEnd`** in `integration_test.go` — 3-segment paths and slash-path GETs; the `a@b` sub-case is deleted (register now rejects `@` in paths — the last-`@` split behavior is already covered by unit tests):

Replace `mk("welcome", ...)` calls and GET URLs:

```go
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
```

(Delete the final "A path containing '@' round-trips" block entirely.)

- [ ] **Step 6: Run integration tests, verify pass**

Run: `make test-integration SERVICE=tcard-service`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add tcard-service/
git commit -m "feat(tcard-service): register paths are exactly 3 slash-separated segments"
```

---

### Task 8: Full verification and push

**Files:** none new — verification only.

- [ ] **Step 1: Format and lint**

Run: `make fmt && make lint`
Expected: no diffs, no lint errors. If `make fmt` changes files, re-run tests and amend the last commit.

- [ ] **Step 2: Unit tests with race + coverage**

Run: `make test SERVICE=tcard-service`
Expected: PASS.

Then verify coverage ≥ 80% (the Makefile's test target uses -race; for the coverage number):

Run: `cd /home/user/newchat && go test -race -coverprofile=/tmp/claude-0/-home-user-newchat/018cf170-e0ca-59d7-bd7a-6047678459ea/scratchpad/tcard-cover.out ./tcard-service/ && go tool cover -func=/tmp/claude-0/-home-user-newchat/018cf170-e0ca-59d7-bd7a-6047678459ea/scratchpad/tcard-cover.out | tail -1`

(Direct `go test` is sanctioned here only because the Makefile has no coverage target; if it does — `make help` to check — use it instead.)
Expected: total ≥ 80%.

- [ ] **Step 3: Integration tests**

Run: `make test-integration SERVICE=tcard-service`
Expected: PASS (requires Docker; if unavailable, note it and proceed — CI runs them).

- [ ] **Step 4: SAST**

Run: `make sast`
Expected: no medium+ findings.

- [ ] **Step 5: Push**

```bash
git push -u origin claude/tcard-service-path-versioning-7xmv9w
```

Retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s) on network failure only.
