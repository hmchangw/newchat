# Teams Room Creation Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Flag every Teams chat whose room-creation event was published, then audit each site to confirm the room and its subscriptions actually exist there.

**Architecture:** `teams-room-creation` sets `needVerify=true` in the same write that clears `needCreateRoom`. A new global CronJob `teams-room-verify` reads those flagged chats from the global Mongo, groups them by `siteId`, and asks each site's new `teams-room-inspector` (a read-only Gin service reading that site's own Mongo) how many rooms and subscriptions exist for a batch of Teams chat ids. The verifier compares against the chat's member count and clears the flag only for chats that converged.

**Tech Stack:** Go 1.25, MongoDB (`go.mongodb.org/mongo-driver/v2` via `pkg/mongoutil`), Gin, Resty (`pkg/restyutil`), `caarlos0/env`, `go.uber.org/mock`, `stretchr/testify`, `testcontainers-go` via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-08-01-teams-room-verify-design.md`

## Global Constraints

- Never run raw `go` commands. Use `make` targets: `make test SERVICE=<name>`, `make test-integration SERVICE=<name>`, `make generate SERVICE=<name>`, `make lint`, `make fmt`, `make build SERVICE=<name>`.
- TDD is mandatory: write the test, run it, confirm it FAILS, then implement, then confirm it PASSES, then commit.
- Minimum 80% coverage per package.
- New services are flat `package main` directories at the repo root — no `cmd/`, no `internal/`.
- Per-service file layout: `main.go`, `handler.go`, `routes.go` (Gin only), `store.go`, `store_mongo.go`, `handler_test.go`, `integration_test.go`, `mock_store_test.go` (generated, never hand-edited).
- Every error is wrapped with context: `fmt.Errorf("short description: %w", err)`. Never a bare `err`, never `fmt.Errorf("error: %w", err)`.
- Client-facing errors use `pkg/errcode` constructors, written with `errcode/errhttp.Write`. Infra failures are returned as raw wrapped errors — they collapse to `internal` at the boundary. Never `slog.Error` AND return the same error (that double-logs).
- Logging is `log/slog` JSON with structured key-value fields. Never `fmt.Println`, never interpolated strings, never log tokens or full message bodies.
- All model structs carry both `json` and `bson` tags in `camelCase` (except `_id`).
- Every Mongo find/aggregation specifies an explicit projection. `$lookup` is forbidden.
- Config comes from environment variables parsed by `caarlos0/env` into a typed `Config` — never `os.Getenv` in service code. Secrets and connection strings get no `envDefault`; everything else does.
- Integration tests are tagged `//go:build integration`, live in `package main`, use `pkg/testutil` containers, and each package needs `func TestMain(m *testing.M) { testutil.RunTests(m) }`.
- Commits are made after each task's tests pass. Lint and tests run in a pre-commit hook — fix failures rather than bypassing the hook.
- Branch: `claude/teams-room-verify-services-sxugxb`. Push with `git push -u origin claude/teams-room-verify-services-sxugxb`.
- Do NOT create a pull request unless explicitly asked.

## File Structure

**Modified:**
- `pkg/model/teams.go` — add `TeamsChat.NeedVerify`; add the three verify wire-contract structs.
- `pkg/model/model_test.go` — round-trip tests for the new field and structs.
- `teams-room-creation/store_mongo.go` — `MarkRoomsCreated` also sets `needVerify: true`.
- `teams-room-creation/store_mongo_test.go` — assert the new field transition.

**Created — `teams-room-inspector/` (per-site Gin service):**
- `main.go` — config struct, wiring, graceful shutdown.
- `routes.go` — route registration.
- `handler.go` — request binding, chat id → room id derivation, response assembly.
- `store.go` — `RoomStore` interface + `RoomState` + mockgen directive.
- `store_mongo.go` — two projected queries (rooms find, subscriptions group).
- `handler_test.go`, `main_test.go`, `integration_test.go`, `mock_store_test.go` (generated).
- `deploy/Dockerfile`, `deploy/docker-compose.yml`, `deploy/azure-pipelines.yml`.

**Created — `teams-room-verify/` (global CronJob):**
- `main.go` — wiring, one pass, exit.
- `config.go` — `Config`, `parseSiteURLs`, `validateConfig`.
- `client.go` — `verifyFunc` type + Resty implementation.
- `runner.go` — batching, fan-out, comparison, flag clearing, summary logging.
- `store.go` — `TeamsChatStore` interface + `VerifiedRef` + mockgen directive.
- `store_mongo.go` — list + CAS clear.
- `config_test.go`, `client_test.go`, `runner_test.go`, `main_test.go`, `store_mongo_test.go` (integration), `mock_store_test.go` (generated).
- `deploy/Dockerfile`, `deploy/docker-compose.yml`, `deploy/azure-pipelines.yml`.

---

### Task 1: The `needVerify` flag

**Files:**
- Modify: `pkg/model/teams.go` (end of the `TeamsChat` struct)
- Modify: `teams-room-creation/store_mongo.go:45-64`
- Test: `pkg/model/model_test.go`, `teams-room-creation/store_mongo_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `model.TeamsChat.NeedVerify bool` (bson key `needVerify`), set to `true` by `teams-room-creation`'s `MarkRoomsCreated`. Task 7 reads it.

- [ ] **Step 1: Write the failing model test**

Append to `pkg/model/model_test.go`:

```go
func TestTeamsChatJSON_NeedVerify(t *testing.T) {
	c := model.TeamsChat{ID: "19:abc@thread.v2", SiteID: "site-a", NeedVerify: true}
	roundTrip(t, &c, &model.TeamsChat{})

	data, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"needVerify":true`) {
		t.Errorf("needVerify missing from JSON: %s", data)
	}
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `c.NeedVerify undefined (type model.TeamsChat has no field or method NeedVerify)`

- [ ] **Step 3: Add the field**

In `pkg/model/teams.go`, immediately after the `NeedCreateRoom` field of `TeamsChat`:

```go
	// NeedVerify is set true by teams-room-creation in the same write that clears
	// NeedCreateRoom, so it means "a creation event for this chat was durably
	// published". teams-room-verify audits these chats against their site and
	// clears the flag only for chats whose room and subscriptions converged.
	NeedVerify bool `json:"needVerify" bson:"needVerify"`
```

- [ ] **Step 4: Run the model test to confirm it passes**

Run: `make test SERVICE=pkg/model`
Expected: PASS

- [ ] **Step 5: Write the failing store test**

Replace the body of `TestMongoStore_ListAndMark` in `teams-room-creation/store_mongo_test.go` — after the existing `MarkRoomsCreated` assertions, append:

```go
	// The clear is one write: needCreateRoom goes false AND needVerify goes true,
	// so the verification lane picks the chat up on its next run.
	var doc model.TeamsChat
	require.NoError(t, col.FindOne(ctx, bson.M{"_id": "c1"}).Decode(&doc))
	assert.False(t, doc.NeedCreateRoom, "needCreateRoom must be cleared")
	assert.True(t, doc.NeedVerify, "needVerify must be set for the verification lane")
```

Add `"github.com/hmchangw/chat/pkg/model"` to that file's imports.

- [ ] **Step 6: Run it to confirm it fails**

Run: `make test-integration SERVICE=teams-room-creation`
Expected: FAIL on `needVerify must be set for the verification lane` (Docker required).

- [ ] **Step 7: Set the flag in the same write**

In `teams-room-creation/store_mongo.go`, inside `MarkRoomsCreated`, change the update document:

```go
			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": r.ID, "updatedAt": r.UpdatedAt}).
				SetUpdate(bson.M{"$set": bson.M{"needCreateRoom": false, "needVerify": true}}))
```

Update the doc comment above the method to:

```go
// MarkRoomsCreated clears needCreateRoom and raises needVerify for the given
// refs, in one write. Written by the primary client. A nil/empty ref slice is a
// no-op. Because this runs only after JetStream acknowledged the batch,
// needVerify=true means the creation event was durably published — which is
// exactly the precondition teams-room-verify audits.
```

- [ ] **Step 8: Run both test suites to confirm they pass**

Run: `make test SERVICE=teams-room-creation && make test-integration SERVICE=teams-room-creation`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add pkg/model/teams.go pkg/model/model_test.go teams-room-creation/store_mongo.go teams-room-creation/store_mongo_test.go
git commit -m "feat(teams-room-creation): raise needVerify when clearing needCreateRoom"
```

---

### Task 2: Verify wire contracts in `pkg/model`

**Files:**
- Modify: `pkg/model/teams.go` (append at end of file)
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `model.TeamsRoomVerifyRequest{ChatIDs []string}`, `model.TeamsRoomVerifyResult{ChatID, RoomID string; RoomExists bool; SubscriptionCount, RoomUserCount int}`, `model.TeamsRoomVerifyResponse{SiteID string; RequestedCount, FoundCount int; Chats []TeamsRoomVerifyResult}`. Tasks 4, 8 and 9 all use these.

- [ ] **Step 1: Write the failing test**

Append to `pkg/model/model_test.go`:

```go
func TestTeamsRoomVerifyRequestJSON(t *testing.T) {
	r := model.TeamsRoomVerifyRequest{ChatIDs: []string{"19:abc@thread.v2", "19:def@unq.gbl.spaces"}}
	roundTrip(t, &r, &model.TeamsRoomVerifyRequest{})
}

func TestTeamsRoomVerifyResponseJSON(t *testing.T) {
	r := model.TeamsRoomVerifyResponse{
		SiteID:         "site-a",
		RequestedCount: 2,
		FoundCount:     1,
		Chats: []model.TeamsRoomVerifyResult{
			{ChatID: "19:abc@thread.v2", RoomID: "7bQ1kR2mN8xY4pL0v", RoomExists: true, SubscriptionCount: 5, RoomUserCount: 5},
			{ChatID: "19:def@unq.gbl.spaces", RoomID: "9xV4jP7sD2fG6bT1m"},
		},
	}
	roundTrip(t, &r, &model.TeamsRoomVerifyResponse{})

	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"siteId"`, `"requestedCount"`, `"foundCount"`, `"chatId"`, `"roomExists"`, `"subscriptionCount"`, `"roomUserCount"`} {
		if !strings.Contains(string(data), key) {
			t.Errorf("key %s missing from JSON: %s", key, data)
		}
	}
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `undefined: model.TeamsRoomVerifyRequest`

- [ ] **Step 3: Add the structs**

Append to `pkg/model/teams.go`:

```go
// Wire contracts for the room-creation verification lane: teams-room-verify
// (global CronJob) asks each site's teams-room-inspector what its Mongo holds
// for a batch of Teams chat ids. Service-to-service HTTP — not a client-facing
// RPC — so these are absent from docs/client-api.md by design.

// TeamsRoomVerifyRequest is the body of POST /internal/teams/rooms/verify.
// Chat ids are Graph ids (…@thread.v2 / …@unq.gbl.spaces); the inspector maps
// each to its room id itself, so caller and callee speak one vocabulary.
type TeamsRoomVerifyRequest struct {
	ChatIDs []string `json:"chatIds" bson:"chatIds"`
}

// TeamsRoomVerifyResult is one chat's state at the answering site.
// SubscriptionCount is the live count of subscription documents for the room;
// RoomUserCount is the room's denormalized counter, reported so drift between
// the two is visible. Both are zero when RoomExists is false.
type TeamsRoomVerifyResult struct {
	ChatID            string `json:"chatId" bson:"chatId"`
	RoomID            string `json:"roomId" bson:"roomId"`
	RoomExists        bool   `json:"roomExists" bson:"roomExists"`
	SubscriptionCount int    `json:"subscriptionCount" bson:"subscriptionCount"`
	RoomUserCount     int    `json:"roomUserCount" bson:"roomUserCount"`
}

// TeamsRoomVerifyResponse is the inspector's reply. Chats carries exactly one
// result per requested chat id, in request order; FoundCount is how many of
// them have a room.
type TeamsRoomVerifyResponse struct {
	SiteID         string                  `json:"siteId" bson:"siteId"`
	RequestedCount int                     `json:"requestedCount" bson:"requestedCount"`
	FoundCount     int                     `json:"foundCount" bson:"foundCount"`
	Chats          []TeamsRoomVerifyResult `json:"chats" bson:"chats"`
}
```

- [ ] **Step 4: Run the test to confirm it passes**

Run: `make test SERVICE=pkg/model`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/model/teams.go pkg/model/model_test.go
git commit -m "feat(model): add teams room verification wire contracts"
```

---

### Task 3: `teams-room-inspector` store

**Files:**
- Create: `teams-room-inspector/store.go`, `teams-room-inspector/store_mongo.go`
- Test: `teams-room-inspector/integration_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `RoomState{RoomID string; Exists bool; UserCount int; SubscriptionCount int}` and `RoomStore` with `RoomStates(ctx context.Context, roomIDs []string) (map[string]RoomState, error)` keyed by room id. `newMongoStore(db *mongo.Database) *mongoStore` implements it. Task 4 consumes `RoomStore`; Task 5 calls `newMongoStore`.

- [ ] **Step 1: Write the failing integration test**

Create `teams-room-inspector/integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestMongoStore_RoomStates(t *testing.T) {
	db := testutil.MongoDB(t, "teamsinspector")
	ctx := context.Background()

	_, err := db.Collection("rooms").InsertMany(ctx, []any{
		bson.M{"_id": "roomA", "name": "A", "userCount": 3},
		bson.M{"_id": "roomB", "name": "B", "userCount": 0},
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "s1", "roomId": "roomA"},
		bson.M{"_id": "s2", "roomId": "roomA"},
		bson.M{"_id": "s3", "roomId": "roomC"}, // subscriptions without a room doc
	})
	require.NoError(t, err)

	store := newMongoStore(db)
	got, err := store.RoomStates(ctx, []string{"roomA", "roomB", "roomMissing", "roomC"})
	require.NoError(t, err)

	assert.Equal(t, RoomState{RoomID: "roomA", Exists: true, UserCount: 3, SubscriptionCount: 2}, got["roomA"])
	assert.Equal(t, RoomState{RoomID: "roomB", Exists: true, UserCount: 0, SubscriptionCount: 0}, got["roomB"])
	assert.NotContains(t, got, "roomMissing", "a room with neither doc nor subs has no entry")
	assert.Equal(t, RoomState{RoomID: "roomC", Exists: false, SubscriptionCount: 1}, got["roomC"],
		"orphan subscriptions are reported with Exists=false")
}

func TestMongoStore_RoomStates_EmptyInput(t *testing.T) {
	db := testutil.MongoDB(t, "teamsinspector")
	store := newMongoStore(db)
	got, err := store.RoomStates(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test-integration SERVICE=teams-room-inspector`
Expected: FAIL to build — `undefined: newMongoStore`, `undefined: RoomState`

- [ ] **Step 3: Write the store interface**

Create `teams-room-inspector/store.go`:

```go
package main

import "context"

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// RoomState is one room's materialisation state at this site. Exists reports
// whether the room document itself is present; SubscriptionCount is the live
// count of subscription documents pointing at it, and UserCount the room's
// denormalized counter (reported so drift between the two is visible).
type RoomState struct {
	RoomID            string
	Exists            bool
	UserCount         int
	SubscriptionCount int
}

// RoomStore reads room and subscription state from this site's Mongo. Read-only:
// the inspector never writes.
type RoomStore interface {
	// RoomStates returns one entry per room id that has a room document, a
	// subscription, or both. Ids with neither are absent from the map — the
	// caller reports those as a missing room.
	RoomStates(ctx context.Context, roomIDs []string) (map[string]RoomState, error)
}
```

- [ ] **Step 4: Write the Mongo implementation**

Create `teams-room-inspector/store_mongo.go`:

```go
package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

// roomDoc is the projected room shape: identity plus the denormalized counter.
type roomDoc struct {
	ID        string `bson:"_id"`
	UserCount int    `bson:"userCount"`
}

// subCountDoc is one $group output row: a room id and its subscription count.
type subCountDoc struct {
	RoomID string `bson:"_id"`
	Count  int    `bson:"count"`
}

// mongoStore reads this site's rooms and subscriptions.
type mongoStore struct {
	rooms *mongoutil.Collection[roomDoc]
	subs  *mongoutil.Collection[subCountDoc]
}

func newMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		rooms: mongoutil.NewCollection[roomDoc](db.Collection("rooms")),
		subs:  mongoutil.NewCollection[subCountDoc](db.Collection("subscriptions")),
	}
}

// RoomStates answers with two queries and no $lookup: a projected find over
// rooms, and a $group over subscriptions that counts server-side instead of
// shipping documents.
func (s *mongoStore) RoomStates(ctx context.Context, roomIDs []string) (map[string]RoomState, error) {
	out := make(map[string]RoomState, len(roomIDs))
	if len(roomIDs) == 0 {
		return out, nil
	}

	rooms, err := s.rooms.FindMany(ctx, bson.M{"_id": bson.M{"$in": roomIDs}},
		mongoutil.WithProjection(bson.M{"_id": 1, "userCount": 1}))
	if err != nil {
		return nil, fmt.Errorf("find rooms: %w", err)
	}
	for _, r := range rooms {
		out[r.ID] = RoomState{RoomID: r.ID, Exists: true, UserCount: r.UserCount}
	}

	counts, err := s.subs.Aggregate(ctx, bson.A{
		bson.M{"$match": bson.M{"roomId": bson.M{"$in": roomIDs}}},
		bson.M{"$group": bson.M{"_id": "$roomId", "count": bson.M{"$sum": 1}}},
	})
	if err != nil {
		return nil, fmt.Errorf("count subscriptions by room: %w", err)
	}
	for _, c := range counts {
		st := out[c.RoomID]
		st.RoomID = c.RoomID
		st.SubscriptionCount = c.Count
		out[c.RoomID] = st
	}
	return out, nil
}
```

- [ ] **Step 5: Run the integration test to confirm it passes**

Run: `make test-integration SERVICE=teams-room-inspector`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add teams-room-inspector/store.go teams-room-inspector/store_mongo.go teams-room-inspector/integration_test.go
git commit -m "feat(teams-room-inspector): add room and subscription state store"
```

---

### Task 4: `teams-room-inspector` handler

**Files:**
- Create: `teams-room-inspector/handler.go`, `teams-room-inspector/routes.go`
- Test: `teams-room-inspector/handler_test.go`
- Generated: `teams-room-inspector/mock_store_test.go`

**Interfaces:**
- Consumes: `RoomStore`, `RoomState` (Task 3); `model.TeamsRoomVerify*` (Task 2).
- Produces: `NewHandler(store RoomStore, siteID string) *Handler`, method `(*Handler).HandleVerify(c *gin.Context)`, `(*Handler).HandleHealth(c *gin.Context)`, and `registerRoutes(r *gin.Engine, h *Handler)`. Task 5 calls all three.

- [ ] **Step 1: Generate the store mock**

Run: `make generate SERVICE=teams-room-inspector`
Expected: creates `teams-room-inspector/mock_store_test.go` with `NewMockRoomStore(ctrl)`.

If `mockgen` is missing, run `make tools` first. Never hand-edit the generated file.

- [ ] **Step 2: Write the failing handler test**

Create `teams-room-inspector/handler_test.go`:

```go
package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// newTestServer wires a Gin engine around a mocked store, matching production
// route registration so path and binding behaviour are exercised too.
func newTestServer(t *testing.T, store RoomStore) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, NewHandler(store, "site-a"))
	return r
}

func postVerify(t *testing.T, r *gin.Engine, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/internal/teams/rooms/verify", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestHandler_HandleVerify_ReportsPerChatState(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	const chatFull = "19:full@thread.v2"
	const chatShort = "19:short@thread.v2"
	const chatMissing = "19:missing@unq.gbl.spaces"
	roomFull := idgen.DeterministicID([]byte(chatFull))
	roomShort := idgen.DeterministicID([]byte(chatShort))
	roomMissing := idgen.DeterministicID([]byte(chatMissing))

	store.EXPECT().RoomStates(gomock.Any(), []string{roomFull, roomShort, roomMissing}).
		Return(map[string]RoomState{
			roomFull:  {RoomID: roomFull, Exists: true, UserCount: 4, SubscriptionCount: 4},
			roomShort: {RoomID: roomShort, Exists: true, UserCount: 3, SubscriptionCount: 2},
		}, nil)

	w := postVerify(t, newTestServer(t, store),
		`{"chatIds":["`+chatFull+`","`+chatShort+`","`+chatMissing+`"]}`)
	require.Equal(t, http.StatusOK, w.Code)

	var got model.TeamsRoomVerifyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "site-a", got.SiteID)
	assert.Equal(t, 3, got.RequestedCount)
	assert.Equal(t, 2, got.FoundCount)
	require.Len(t, got.Chats, 3)

	assert.Equal(t, model.TeamsRoomVerifyResult{
		ChatID: chatFull, RoomID: roomFull, RoomExists: true, SubscriptionCount: 4, RoomUserCount: 4,
	}, got.Chats[0], "results must come back in request order")
	assert.Equal(t, model.TeamsRoomVerifyResult{
		ChatID: chatShort, RoomID: roomShort, RoomExists: true, SubscriptionCount: 2, RoomUserCount: 3,
	}, got.Chats[1])
	assert.Equal(t, model.TeamsRoomVerifyResult{
		ChatID: chatMissing, RoomID: roomMissing,
	}, got.Chats[2], "an id the store never saw reports as a missing room")
}

func TestHandler_HandleVerify_RoomWithNoSubscriptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	const chatID = "19:empty@thread.v2"
	roomID := idgen.DeterministicID([]byte(chatID))

	store.EXPECT().RoomStates(gomock.Any(), []string{roomID}).
		Return(map[string]RoomState{roomID: {RoomID: roomID, Exists: true}}, nil)

	w := postVerify(t, newTestServer(t, store), `{"chatIds":["`+chatID+`"]}`)
	require.Equal(t, http.StatusOK, w.Code)

	var got model.TeamsRoomVerifyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, 1, got.FoundCount)
	assert.True(t, got.Chats[0].RoomExists)
	assert.Zero(t, got.Chats[0].SubscriptionCount)
}

func TestHandler_HandleVerify_InvalidInput(t *testing.T) {
	tooMany := make([]string, maxChatIDsPerRequest+1)
	for i := range tooMany {
		tooMany[i] = "19:chat@thread.v2"
	}
	tooManyJSON, err := json.Marshal(model.TeamsRoomVerifyRequest{ChatIDs: tooMany})
	require.NoError(t, err)

	tests := []struct {
		name string
		body string
	}{
		{"malformed json", `{"chatIds":`},
		{"wrong type", `{"chatIds":"not-an-array"}`},
		{"empty array", `{"chatIds":[]}`},
		{"missing field", `{}`},
		{"over the limit", string(tooManyJSON)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl) // no EXPECT: the store must not be touched
			w := postVerify(t, newTestServer(t, store), tt.body)
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestHandler_HandleVerify_StoreErrorIsInternal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().RoomStates(gomock.Any(), gomock.Any()).Return(nil, errors.New("mongo down"))

	w := postVerify(t, newTestServer(t, store), `{"chatIds":["19:abc@thread.v2"]}`)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.NotContains(t, w.Body.String(), "mongo down", "internal cause must never reach the client")
}

func TestHandler_HandleHealth(t *testing.T) {
	ctrl := gomock.NewController(t)
	r := newTestServer(t, NewMockRoomStore(ctrl))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}
```

- [ ] **Step 3: Run it to confirm it fails**

Run: `make test SERVICE=teams-room-inspector`
Expected: FAIL to build — `undefined: registerRoutes`, `undefined: NewHandler`, `undefined: maxChatIDsPerRequest`

- [ ] **Step 4: Write the handler**

Create `teams-room-inspector/handler.go`:

```go
package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// maxChatIDsPerRequest bounds one verify call. The caller batches at 200 by
// default; the cap leaves headroom while keeping the $in lists sane.
const maxChatIDsPerRequest = 500

// Handler serves the read-only verification endpoint for this site.
type Handler struct {
	store  RoomStore
	siteID string
}

func NewHandler(store RoomStore, siteID string) *Handler {
	return &Handler{store: store, siteID: siteID}
}

// HandleHealth is the liveness probe.
func (h *Handler) HandleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// HandleVerify reports, per requested Teams chat id, whether this site holds
// the room and how many subscriptions point at it. Room ids are derived with
// the same idgen.DeterministicID room-worker used to create them, so the caller
// never has to know the mapping.
func (h *Handler) HandleVerify(c *gin.Context) {
	ctx := c.Request.Context()

	var req model.TeamsRoomVerifyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("decode verify request"))
		return
	}
	if len(req.ChatIDs) == 0 {
		errhttp.Write(ctx, c, errcode.BadRequest("chatIds must not be empty"))
		return
	}
	if len(req.ChatIDs) > maxChatIDsPerRequest {
		errhttp.Write(ctx, c, errcode.BadRequest(
			fmt.Sprintf("chatIds exceeds the per-request limit of %d", maxChatIDsPerRequest)))
		return
	}

	roomIDs := make([]string, 0, len(req.ChatIDs))
	for _, chatID := range req.ChatIDs {
		roomIDs = append(roomIDs, idgen.DeterministicID([]byte(chatID)))
	}

	states, err := h.store.RoomStates(ctx, roomIDs)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("read room states: %w", err))
		return
	}

	resp := model.TeamsRoomVerifyResponse{
		SiteID:         h.siteID,
		RequestedCount: len(req.ChatIDs),
		Chats:          make([]model.TeamsRoomVerifyResult, 0, len(req.ChatIDs)),
	}
	for i, chatID := range req.ChatIDs {
		roomID := roomIDs[i]
		st := states[roomID] // zero value when absent: no room, no subscriptions
		if st.Exists {
			resp.FoundCount++
		}
		resp.Chats = append(resp.Chats, model.TeamsRoomVerifyResult{
			ChatID:            chatID,
			RoomID:            roomID,
			RoomExists:        st.Exists,
			SubscriptionCount: st.SubscriptionCount,
			RoomUserCount:     st.UserCount,
		})
	}
	c.JSON(http.StatusOK, resp)
}
```

- [ ] **Step 5: Write the routes**

Create `teams-room-inspector/routes.go`:

```go
package main

import "github.com/gin-gonic/gin"

// registerRoutes wires the inspector's two endpoints. The verify endpoint is
// cluster-internal (no auth) — network policy is the boundary.
func registerRoutes(r *gin.Engine, h *Handler) {
	r.GET("/healthz", h.HandleHealth)
	r.POST("/internal/teams/rooms/verify", h.HandleVerify)
}
```

- [ ] **Step 6: Run the tests to confirm they pass**

Run: `make test SERVICE=teams-room-inspector`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add teams-room-inspector/handler.go teams-room-inspector/routes.go teams-room-inspector/handler_test.go teams-room-inspector/mock_store_test.go
git commit -m "feat(teams-room-inspector): add verify endpoint"
```

---

### Task 5: `teams-room-inspector` wiring and deploy

**Files:**
- Create: `teams-room-inspector/main.go`, `teams-room-inspector/main_test.go`
- Create: `teams-room-inspector/deploy/Dockerfile`, `deploy/docker-compose.yml`, `deploy/azure-pipelines.yml`

**Interfaces:**
- Consumes: `newMongoStore` (Task 3), `NewHandler` / `registerRoutes` (Task 4).
- Produces: a runnable service listening on `PORT` (default `8080`).

- [ ] **Step 1: Write the failing test**

Create `teams-room-inspector/main_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// run() must fail fast when required config is absent (no MONGO_URI/SITE_ID).
func TestRun_MissingConfigFailsFast(t *testing.T) {
	t.Setenv("MONGO_URI", "")
	t.Setenv("SITE_ID", "")
	require.Error(t, run())
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=teams-room-inspector`
Expected: FAIL to build — `undefined: run`

- [ ] **Step 3: Write main.go**

Create `teams-room-inspector/main.go`:

```go
// Command teams-room-inspector is a per-site read-only HTTP service that
// reports what this site's Mongo holds for a batch of Teams chat ids: whether
// each chat's room exists and how many subscriptions point at it. It is called
// by the global teams-room-verify CronJob, which compares the counts against
// the chat's member list. One deployment per site.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	o11ygin "github.com/flywindy/o11y/gin"
	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
)

// Config is the service's environment configuration. Mongo is this site's own
// operational database — the inspector never reaches across sites.
type Config struct {
	MongoURI      string `env:"MONGO_URI,required,notEmpty"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	// SiteID is echoed in every response so a misrouted call is obvious in the
	// caller's logs rather than silently counted against the wrong site.
	SiteID string `env:"SITE_ID,required,notEmpty"`
	Port   string `env:"PORT" envDefault:"8080"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("teams-room-inspector failed", "error", err)
		os.Exit(1)
	}
}

// run wires dependencies and serves until shutdown. It returns an error rather
// than calling os.Exit so deferred cleanup always runs.
func run() error {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	// Reads only — but the site's primary is the authoritative view of what was
	// just created, and a secondary lag would report false missing rooms.
	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}

	handler := NewHandler(newMongoStore(mongoClient.Database(cfg.MongoDB)), cfg.SiteID)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(o11ygin.Middleware("teams-room-inspector", sdk.TracerProvider(), sdk.MeterProvider(), sdk.Propagator, o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(ginutil.AccessLog())
	registerRoutes(r, handler)

	addr := fmt.Sprintf(":%s", cfg.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("teams-room-inspector starting", "addr", addr, "site_id", cfg.SiteID)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second,
			func(ctx context.Context) error {
				slog.Info("shutting down teams-room-inspector")
				err := srv.Shutdown(ctx)
				mongoutil.Disconnect(ctx, mongoClient)
				return err
			},
			func(ctx context.Context) error { return obsShutdown(ctx) },
		)
	}()

	if err := <-srvErr; err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen teams-room-inspector: %w", err)
	}
	<-shutdownDone
	return nil
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `make test SERVICE=teams-room-inspector && make build SERVICE=teams-room-inspector`
Expected: PASS, and a binary at `bin/teams-room-inspector`.

- [ ] **Step 5: Write the Dockerfile**

Create `teams-room-inspector/deploy/Dockerfile`:

```dockerfile
FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-room-inspector/ teams-room-inspector/
RUN CGO_ENABLED=0 go build -o /teams-room-inspector ./teams-room-inspector/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-room-inspector /teams-room-inspector
USER app
EXPOSE 8080
ENTRYPOINT ["/teams-room-inspector"]
```

- [ ] **Step 6: Write the compose file**

Create `teams-room-inspector/deploy/docker-compose.yml`:

```yaml
name: teams-room-inspector

services:
  teams-room-inspector:
    build:
      context: ../..
      dockerfile: teams-room-inspector/deploy/Dockerfile
    environment:
      - MONGO_URI=${MONGO_URI:-mongodb://mongo:27017}
      - MONGO_DB=${MONGO_DB:-chat}
      - SITE_ID=${SITE_ID:-site-local}
      - PORT=${PORT:-8080}
    ports:
      - "8080:8080"
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 7: Write the pipeline**

Create `teams-room-inspector/deploy/azure-pipelines.yml` by copying `teams-room-creation/deploy/azure-pipelines.yml` verbatim and replacing every occurrence of `teams-room-creation` with `teams-room-inspector`:

```bash
sed 's/teams-room-creation/teams-room-inspector/g' \
  teams-room-creation/deploy/azure-pipelines.yml > teams-room-inspector/deploy/azure-pipelines.yml
```

Then open the result and confirm `SERVICE_DIR`, `IMAGE_NAME`, and both `paths.include` lists read `teams-room-inspector`.

- [ ] **Step 8: Lint and commit**

```bash
make fmt && make lint
git add teams-room-inspector/
git commit -m "feat(teams-room-inspector): add service wiring and deploy artifacts"
```

---

### Task 6: `teams-room-verify` config

**Files:**
- Create: `teams-room-verify/config.go`
- Test: `teams-room-verify/config_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `Config` (fields `MongoURI`, `MongoDB`, `MongoUsername`, `MongoPassword`, `SiteURLs string`, `BatchSize int`, `MaxWorkers int`), `parseSiteURLs(raw string) (map[string]string, error)`, `validateConfig(cfg Config) error`. Tasks 9 and 10 consume all three.

- [ ] **Step 1: Write the failing test**

Create `teams-room-verify/config_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseConfig() Config {
	return Config{BatchSize: 200, MaxWorkers: 8}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(*Config) {}, false},
		{"zero batch size", func(c *Config) { c.BatchSize = 0 }, true},
		{"negative batch size", func(c *Config) { c.BatchSize = -1 }, true},
		{"zero workers", func(c *Config) { c.MaxWorkers = 0 }, true},
		{"negative workers", func(c *Config) { c.MaxWorkers = -3 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.mutate(&cfg)
			err := validateConfig(cfg)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestParseSiteURLs(t *testing.T) {
	t.Run("valid registry", func(t *testing.T) {
		sites, err := parseSiteURLs(`{"site-a":"http://teams-room-inspector.site-a:8080","site-b":"http://teams-room-inspector.site-b:8080"}`)
		require.NoError(t, err)
		assert.Len(t, sites, 2)
		assert.Equal(t, "http://teams-room-inspector.site-a:8080", sites["site-a"])
	})

	tests := []struct {
		name string
		raw  string
	}{
		{"not json", `site-a=http://x`},
		{"wrong shape", `{"site-a":{"baseUrl":"http://x"}}`},
		{"empty object", `{}`},
		{"empty url", `{"site-a":""}`},
		{"blank string", ``},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseSiteURLs(tt.raw)
			require.Error(t, err)
		})
	}
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=teams-room-verify`
Expected: FAIL to build — `undefined: Config`, `undefined: parseSiteURLs`

- [ ] **Step 3: Write config.go**

Create `teams-room-verify/config.go`:

```go
package main

import (
	"encoding/json"
	"fmt"
)

// Config is the job's environment configuration. Mongo is the global database
// holding teams_chat: the flagged-chat scan reads through a secondary-preferred
// client and the needVerify clear writes through a primary client, so they
// share one URI, DB and credential pair — only the read preference differs.
type Config struct {
	MongoURI      string `env:"MONGO_URI,required,notEmpty"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	// SiteURLs is the per-site inspector registry: a JSON object mapping siteId
	// to that site's teams-room-inspector base URL. A single template can't
	// express sites on different domains, so each site is listed explicitly —
	// same reasoning as portal-service's PORTAL_SITE_URLS.
	SiteURLs string `env:"TEAMS_VERIFY_SITE_URLS,required,notEmpty"`

	// BatchSize is the maximum number of chat ids sent in one inspector call.
	BatchSize int `env:"VERIFY_BATCH_SIZE" envDefault:"200"`
	// MaxWorkers bounds concurrent inspector calls across all site groups.
	MaxWorkers int `env:"MAX_WORKERS" envDefault:"8"`
}

// parseSiteURLs decodes TEAMS_VERIFY_SITE_URLS. An empty registry is rejected:
// with no destinations the job would clear nothing and report nothing, which
// looks identical to a healthy run.
func parseSiteURLs(raw string) (map[string]string, error) {
	var sites map[string]string
	if err := json.Unmarshal([]byte(raw), &sites); err != nil {
		return nil, fmt.Errorf("parse TEAMS_VERIFY_SITE_URLS: %w", err)
	}
	if len(sites) == 0 {
		return nil, fmt.Errorf("invalid config: TEAMS_VERIFY_SITE_URLS has no sites")
	}
	for site, url := range sites {
		if url == "" {
			return nil, fmt.Errorf("invalid config: TEAMS_VERIFY_SITE_URLS[%q] has an empty URL", site)
		}
	}
	return sites, nil
}

// validateConfig checks the parsed Config's numeric knobs. It isolates run()'s
// pure precondition checks so they are unit testable without wiring any real
// dependency; the site registry is validated separately by parseSiteURLs,
// whose parsed map run() needs.
//
//nolint:gocritic // hugeParam: cfg is passed by value once at startup; not a hot path
func validateConfig(cfg Config) error {
	if cfg.BatchSize <= 0 {
		return fmt.Errorf("invalid config: VERIFY_BATCH_SIZE must be positive")
	}
	if cfg.MaxWorkers <= 0 {
		return fmt.Errorf("invalid config: MAX_WORKERS must be positive")
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `make test SERVICE=teams-room-verify`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add teams-room-verify/config.go teams-room-verify/config_test.go
git commit -m "feat(teams-room-verify): add config and site registry parsing"
```

---

### Task 7: `teams-room-verify` store

**Files:**
- Create: `teams-room-verify/store.go`, `teams-room-verify/store_mongo.go`
- Test: `teams-room-verify/store_mongo_test.go`

**Interfaces:**
- Consumes: `model.TeamsChat.NeedVerify` (Task 1).
- Produces: `VerifiedRef{ID string; UpdatedAt time.Time}`, `TeamsChatStore` with `ListChatsNeedingVerify(ctx) ([]model.TeamsChat, error)` and `MarkVerified(ctx, refs []VerifiedRef) error`, and `newMongoStore(readDB, writeDB *mongo.Database) *mongoStore`. Tasks 9 and 10 consume them.

- [ ] **Step 1: Write the failing integration test**

Create `teams-room-verify/store_mongo_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestMongoStore_ListChatsNeedingVerify(t *testing.T) {
	db := testutil.MongoDB(t, "teamsverify")
	col := db.Collection("teams_chat")
	ctx := context.Background()
	ua := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)

	_, err := col.InsertMany(ctx, []any{
		bson.M{"_id": "c1", "siteId": "site-a", "needVerify": true, "updatedAt": ua,
			"members": []bson.M{{"account": "alice"}, {"account": "bob"}}},
		bson.M{"_id": "c2", "siteId": "site-b", "needVerify": true, "updatedAt": ua, "members": []bson.M{}},
		bson.M{"_id": "c3", "siteId": "site-a", "needVerify": false, "updatedAt": ua, "members": []bson.M{}},
	})
	require.NoError(t, err)

	store := newMongoStore(db, db)

	got, err := store.ListChatsNeedingVerify(ctx)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "c1", got[0].ID, "results are sorted by _id")
	assert.Equal(t, "site-a", got[0].SiteID)
	assert.Len(t, got[0].Members, 2, "members are projected — the expected count comes from them")
	assert.Equal(t, ua, got[0].UpdatedAt.UTC())
	assert.Equal(t, "c2", got[1].ID)
}

func TestMongoStore_MarkVerified_ClearsOnlyMatchingRefs(t *testing.T) {
	db := testutil.MongoDB(t, "teamsverify")
	col := db.Collection("teams_chat")
	ctx := context.Background()
	current := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	stale := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)

	_, err := col.InsertOne(ctx, bson.M{"_id": "c1", "siteId": "site-a",
		"needVerify": true, "updatedAt": current, "members": []bson.M{}})
	require.NoError(t, err)

	store := newMongoStore(db, db)

	// Stale updatedAt: the chat was re-written since it was listed, so the CAS
	// misses and the flag stays for the next run.
	require.NoError(t, store.MarkVerified(ctx, []VerifiedRef{{ID: "c1", UpdatedAt: stale}}))
	got, err := store.ListChatsNeedingVerify(ctx)
	require.NoError(t, err)
	assert.Len(t, got, 1, "stale ref must not clear the flag")

	require.NoError(t, store.MarkVerified(ctx, []VerifiedRef{{ID: "c1", UpdatedAt: current}}))
	after, err := store.ListChatsNeedingVerify(ctx)
	require.NoError(t, err)
	assert.Empty(t, after, "matching ref clears the flag")
}

func TestMongoStore_MarkVerified_EmptyIsNoop(t *testing.T) {
	db := testutil.MongoDB(t, "teamsverify")
	store := newMongoStore(db, db)
	ctx := context.Background()
	require.NoError(t, store.MarkVerified(ctx, nil))
	require.NoError(t, store.MarkVerified(ctx, []VerifiedRef{}))
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test-integration SERVICE=teams-room-verify`
Expected: FAIL to build — `undefined: newMongoStore`, `undefined: VerifiedRef`

- [ ] **Step 3: Write the store interface**

Create `teams-room-verify/store.go`:

```go
package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// VerifiedRef identifies a chat to clear plus the updatedAt it was read at.
// MarkVerified uses it as a compare-and-set token: a chat whose updatedAt
// changed since it was listed (teams-chat-member-sync re-resolved its roster)
// is left flagged and re-verified next run against its new member list.
type VerifiedRef struct {
	ID        string
	UpdatedAt time.Time
}

// TeamsChatStore reads chats awaiting verification and clears the flag once
// their site confirms the room and subscriptions exist. Satisfied by
// *mongoStore, whose reads and writes go to separate clients (secondary-read,
// primary-write).
type TeamsChatStore interface {
	// ListChatsNeedingVerify returns every teams_chat with needVerify=true,
	// projected to the fields verification needs (_id, members, siteId, updatedAt).
	ListChatsNeedingVerify(ctx context.Context) ([]model.TeamsChat, error)
	// MarkVerified clears needVerify for each ref whose updatedAt still matches.
	MarkVerified(ctx context.Context, refs []VerifiedRef) error
}
```

- [ ] **Step 4: Write the Mongo implementation**

Create `teams-room-verify/store_mongo.go`:

```go
package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// mongoStore implements TeamsChatStore over two databases: readChats (the
// flagged-chat scan, typically a secondary-preferred read client) and
// writeChats (the needVerify flag clear, a primary client).
type mongoStore struct {
	readChats  *mongoutil.Collection[model.TeamsChat]
	writeChats *mongoutil.Collection[model.TeamsChat]
}

func newMongoStore(readDB, writeDB *mongo.Database) *mongoStore {
	return &mongoStore{
		readChats:  mongoutil.NewCollection[model.TeamsChat](readDB.Collection("teams_chat")),
		writeChats: mongoutil.NewCollection[model.TeamsChat](writeDB.Collection("teams_chat")),
	}
}

// ListChatsNeedingVerify returns every teams_chat with needVerify=true,
// projected to exactly the fields verification needs. Served by the read client.
func (s *mongoStore) ListChatsNeedingVerify(ctx context.Context) ([]model.TeamsChat, error) {
	// Stable _id sort so batch composition is deterministic across runs.
	// updatedAt is carried as the compare-and-set token for MarkVerified.
	chats, err := s.readChats.FindMany(ctx, bson.M{"needVerify": true},
		mongoutil.WithProjection(bson.M{"_id": 1, "members": 1, "siteId": 1, "updatedAt": 1}),
		mongoutil.WithSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list chats needing verify: %w", err)
	}
	return chats, nil
}

// MarkVerified clears needVerify for the given refs. Written by the primary
// client. A nil/empty ref slice is a no-op.
func (s *mongoStore) MarkVerified(ctx context.Context, refs []VerifiedRef) error {
	if len(refs) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(refs))
	for _, r := range refs {
		// Compare-and-set on updatedAt: only clear the flag if member-sync has not
		// re-written the chat since we read it. A stale ref matches nothing, so the
		// chat is re-verified next run against its new roster.
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": r.ID, "updatedAt": r.UpdatedAt}).
			SetUpdate(bson.M{"$set": bson.M{"needVerify": false}}))
	}
	if _, err := s.writeChats.BulkWrite(ctx, models); err != nil {
		return fmt.Errorf("mark verified: %w", err)
	}
	return nil
}
```

- [ ] **Step 5: Run the integration test to confirm it passes**

Run: `make test-integration SERVICE=teams-room-verify`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add teams-room-verify/store.go teams-room-verify/store_mongo.go teams-room-verify/store_mongo_test.go
git commit -m "feat(teams-room-verify): add flagged-chat store"
```

---

### Task 8: `teams-room-verify` inspector client

**Files:**
- Create: `teams-room-verify/client.go`
- Test: `teams-room-verify/client_test.go`

**Interfaces:**
- Consumes: `model.TeamsRoomVerify*` (Task 2).
- Produces: `type verifyFunc func(ctx context.Context, baseURL string, chatIDs []string) (*model.TeamsRoomVerifyResponse, error)`, `newHTTPVerifier(timeout time.Duration) verifyFunc`, and `const verifyPath = "/internal/teams/rooms/verify"`. Task 9 injects a `verifyFunc`; Task 10 calls `newHTTPVerifier`.

- [ ] **Step 1: Write the failing test**

Create `teams-room-verify/client_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestHTTPVerifier_PostsChatIDsAndDecodesReply(t *testing.T) {
	var gotPath string
	var gotBody model.TeamsRoomVerifyRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(model.TeamsRoomVerifyResponse{
			SiteID: "site-a", RequestedCount: 1, FoundCount: 1,
			Chats: []model.TeamsRoomVerifyResult{
				{ChatID: "19:abc@thread.v2", RoomID: "room1", RoomExists: true, SubscriptionCount: 2, RoomUserCount: 2},
			},
		}))
	}))
	defer srv.Close()

	verify := newHTTPVerifier(5 * time.Second)
	got, err := verify(context.Background(), srv.URL, []string{"19:abc@thread.v2"})
	require.NoError(t, err)

	assert.Equal(t, verifyPath, gotPath)
	assert.Equal(t, []string{"19:abc@thread.v2"}, gotBody.ChatIDs)
	assert.Equal(t, "site-a", got.SiteID)
	require.Len(t, got.Chats, 1)
	assert.Equal(t, 2, got.Chats[0].SubscriptionCount)
}

func TestHTTPVerifier_Errors(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"server error", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}},
		{"bad request", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		}},
		{"malformed body", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"chats":`))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()
			verify := newHTTPVerifier(5 * time.Second)
			_, err := verify(context.Background(), srv.URL, []string{"19:abc@thread.v2"})
			require.Error(t, err)
		})
	}
}

func TestHTTPVerifier_UnreachableHost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing is listening now

	verify := newHTTPVerifier(1 * time.Second)
	_, err := verify(context.Background(), url, []string{"19:abc@thread.v2"})
	require.Error(t, err)
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=teams-room-verify`
Expected: FAIL to build — `undefined: newHTTPVerifier`, `undefined: verifyPath`

- [ ] **Step 3: Write client.go**

Create `teams-room-verify/client.go`:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/restyutil"
)

// verifyPath is the inspector's verification endpoint.
const verifyPath = "/internal/teams/rooms/verify"

// verifyFunc asks one site's inspector about a batch of chat ids. Injected into
// the runner so unit tests exercise the comparison logic without HTTP.
type verifyFunc func(ctx context.Context, baseURL string, chatIDs []string) (*model.TeamsRoomVerifyResponse, error)

// newHTTPVerifier returns a verifyFunc backed by one shared Resty client. The
// base URL varies per site, so each call posts to an absolute URL rather than
// the client's base.
func newHTTPVerifier(timeout time.Duration) verifyFunc {
	client := restyutil.New("", restyutil.WithTimeout(timeout))
	return func(ctx context.Context, baseURL string, chatIDs []string) (*model.TeamsRoomVerifyResponse, error) {
		var out model.TeamsRoomVerifyResponse
		resp, err := client.R().
			SetContext(ctx).
			SetBody(model.TeamsRoomVerifyRequest{ChatIDs: chatIDs}).
			SetResult(&out).
			Post(baseURL + verifyPath)
		if err != nil {
			return nil, fmt.Errorf("call inspector at %q: %w", baseURL, err)
		}
		if resp.IsError() {
			return nil, fmt.Errorf("inspector at %q returned status %d", baseURL, resp.StatusCode())
		}
		// A 200 whose body failed to decode leaves the result zero-valued; treat a
		// reply with no chats as a failed call rather than "everything is missing".
		if len(out.Chats) == 0 {
			return nil, fmt.Errorf("inspector at %q returned no results for %d chat ids", baseURL, len(chatIDs))
		}
		return &out, nil
	}
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `make test SERVICE=teams-room-verify`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add teams-room-verify/client.go teams-room-verify/client_test.go
git commit -m "feat(teams-room-verify): add inspector http client"
```

---

### Task 9: `teams-room-verify` runner

**Files:**
- Create: `teams-room-verify/runner.go`
- Test: `teams-room-verify/runner_test.go`
- Generated: `teams-room-verify/mock_store_test.go`

**Interfaces:**
- Consumes: `TeamsChatStore`, `VerifiedRef` (Task 7); `verifyFunc` (Task 8); `model.TeamsChat`, `model.TeamsRoomVerify*` (Tasks 1–2).
- Produces: `runConfig{BatchSize int; MaxWorkers int; SiteURLs map[string]string}`, `newRunner(store TeamsChatStore, verify verifyFunc, cfg runConfig) *runner`, method `(*runner).run(ctx context.Context) error`. Task 10 calls both.

- [ ] **Step 1: Generate the store mock**

Run: `make generate SERVICE=teams-room-verify`
Expected: creates `teams-room-verify/mock_store_test.go` with `NewMockTeamsChatStore(ctrl)`.

- [ ] **Step 2: Write the failing test**

Create `teams-room-verify/runner_test.go`:

```go
package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// chat builds a flagged chat with n members, all with accounts.
func chat(id, site string, accounts ...string) model.TeamsChat {
	members := make([]model.TeamsChatMember, 0, len(accounts))
	for _, a := range accounts {
		members = append(members, model.TeamsChatMember{ID: "g-" + a, Account: a})
	}
	return model.TeamsChat{
		ID:        id,
		SiteID:    site,
		Members:   members,
		UpdatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
	}
}

// recordingVerifier returns a verifyFunc that answers from results (keyed by
// chat id) and records the base URLs it was called with.
type recordingVerifier struct {
	mu      sync.Mutex
	calls   []string
	results map[string]model.TeamsRoomVerifyResult
	err     error
}

func (rv *recordingVerifier) fn(_ context.Context, baseURL string, chatIDs []string) (*model.TeamsRoomVerifyResponse, error) {
	rv.mu.Lock()
	rv.calls = append(rv.calls, baseURL)
	rv.mu.Unlock()
	if rv.err != nil {
		return nil, rv.err
	}
	resp := &model.TeamsRoomVerifyResponse{RequestedCount: len(chatIDs)}
	for _, id := range chatIDs {
		res, ok := rv.results[id]
		if !ok {
			continue // simulates an inspector that omitted a requested id
		}
		if res.RoomExists {
			resp.FoundCount++
		}
		resp.Chats = append(resp.Chats, res)
	}
	return resp, nil
}

func result(chatID string, exists bool, subs int) model.TeamsRoomVerifyResult {
	return model.TeamsRoomVerifyResult{
		ChatID: chatID, RoomID: idgen.DeterministicID([]byte(chatID)),
		RoomExists: exists, SubscriptionCount: subs, RoomUserCount: subs,
	}
}

func testRunConfig() runConfig {
	return runConfig{BatchSize: 10, MaxWorkers: 4, SiteURLs: map[string]string{
		"site-a": "http://inspector-a", "site-b": "http://inspector-b",
	}}
}

func TestRunner_ClearsOnlyConvergedChats(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)

	chats := []model.TeamsChat{
		chat("c-ok", "site-a", "alice", "bob"),
		chat("c-short", "site-a", "alice", "bob", "carol"),
		chat("c-extra", "site-a", "alice"),
		chat("c-missing", "site-a", "alice", "bob"),
	}
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return(chats, nil)

	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{
		"c-ok":      result("c-ok", true, 2),
		"c-short":   result("c-short", true, 2),
		"c-extra":   result("c-extra", true, 3),
		"c-missing": result("c-missing", false, 0),
	}}

	var marked []VerifiedRef
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, refs []VerifiedRef) error {
			marked = append(marked, refs...)
			return nil
		})

	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))

	require.Len(t, marked, 1, "only the converged chat is cleared")
	assert.Equal(t, "c-ok", marked[0].ID)
	assert.Equal(t, chats[0].UpdatedAt, marked[0].UpdatedAt, "the CAS token is the listed updatedAt")
}

func TestRunner_MembersWithoutAccountsStillCountAsExpected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)

	// A guest with no account: room-worker skips it, so the site holds 1
	// subscription while the raw member count is 2. Per spec, expected is the
	// raw member count, so this reports as a mismatch and keeps its flag.
	guestChat := chat("c-guest", "site-a", "alice")
	guestChat.Members = append(guestChat.Members, model.TeamsChatMember{ID: "g-x", Account: ""})
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{guestChat}, nil)

	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{
		"c-guest": result("c-guest", true, 1),
	}}
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Len(0)).Return(nil).AnyTimes()

	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))
}

func TestRunner_RoutesEachSiteToItsOwnInspector(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{
		chat("c-a", "site-a", "alice"),
		chat("c-b", "site-b", "bob"),
	}, nil)

	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{
		"c-a": result("c-a", true, 1),
		"c-b": result("c-b", true, 1),
	}}
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Any()).Return(nil).Times(2)

	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))

	assert.ElementsMatch(t, []string{"http://inspector-a", "http://inspector-b"}, rv.calls)
}

func TestRunner_UnknownSiteIsSkipped(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{
		chat("c-x", "site-unknown", "alice"),
	}, nil)
	// No MarkVerified and no inspector call: the chat keeps its flag.

	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{}}
	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))
	assert.Empty(t, rv.calls)
}

func TestRunner_InspectorFailureLeavesFlags(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{
		chat("c-a", "site-a", "alice"),
	}, nil)

	rv := &recordingVerifier{err: errors.New("connection refused")}
	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()), "a site failure must not fail the whole run")
}

func TestRunner_MissingResultLeavesFlag(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{
		chat("c-present", "site-a", "alice"),
		chat("c-omitted", "site-a", "alice"),
	}, nil)

	// The inspector answers about one chat only; the omitted one is not treated
	// as a missing room, it is simply left unverified.
	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{
		"c-present": result("c-present", true, 1),
	}}
	var marked []VerifiedRef
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, refs []VerifiedRef) error {
			marked = append(marked, refs...)
			return nil
		})

	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))
	require.Len(t, marked, 1)
	assert.Equal(t, "c-present", marked[0].ID)
}

func TestRunner_BatchesLargeSites(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)

	chats := make([]model.TeamsChat, 0, 25)
	results := map[string]model.TeamsRoomVerifyResult{}
	for i := 0; i < 25; i++ {
		id := "c" + string(rune('A'+i))
		chats = append(chats, chat(id, "site-a", "alice"))
		results[id] = result(id, true, 1)
	}
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return(chats, nil)
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Any()).Return(nil).Times(3)

	rv := &recordingVerifier{results: results}
	r := newRunner(store, rv.fn, testRunConfig()) // BatchSize 10 → 10/10/5
	require.NoError(t, r.run(context.Background()))
	assert.Len(t, rv.calls, 3, "25 chats at batch size 10 is three calls")
}

func TestRunner_EmptyListIsNoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return(nil, nil)

	rv := &recordingVerifier{}
	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))
	assert.Empty(t, rv.calls)
}

func TestRunner_ListErrorFailsRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return(nil, errors.New("mongo down"))

	rv := &recordingVerifier{}
	r := newRunner(store, rv.fn, testRunConfig())
	require.Error(t, r.run(context.Background()))
}

func TestRunner_MarkVerifiedErrorDoesNotFailRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{
		chat("c-a", "site-a", "alice"),
	}, nil)
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Any()).Return(errors.New("write failed"))

	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{
		"c-a": result("c-a", true, 1),
	}}
	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()), "verification is read-only; a repeat next run is harmless")
}

func TestPlanBatches(t *testing.T) {
	chats := []model.TeamsChat{
		chat("c1", "site-a", "alice"),
		chat("c2", "site-b", "bob"),
		chat("c3", "site-a", "carol"),
	}
	got := planBatches(chats, 1)
	require.Len(t, got, 3)
	assert.Equal(t, "site-a", got[0].siteID)
	assert.Equal(t, "c1", got[0].chats[0].ID)
	assert.Equal(t, "site-a", got[1].siteID, "a site's chats stay contiguous in input order")
	assert.Equal(t, "c3", got[1].chats[0].ID)
	assert.Equal(t, "site-b", got[2].siteID)
}
```

- [ ] **Step 3: Run it to confirm it fails**

Run: `make test SERVICE=teams-room-verify`
Expected: FAIL to build — `undefined: newRunner`, `undefined: runConfig`, `undefined: planBatches`

- [ ] **Step 4: Write runner.go**

Create `teams-room-verify/runner.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/hmchangw/chat/pkg/model"
)

// runConfig holds the runner's pure knobs, so the pass is testable without any
// real dependency.
type runConfig struct {
	BatchSize  int
	MaxWorkers int
	// SiteURLs maps siteId to that site's inspector base URL.
	SiteURLs map[string]string
}

// runner performs one verification pass: list flagged chats, group by site,
// chunk into batches, ask each site's inspector what it holds, and clear the
// flag for the chats that converged.
type runner struct {
	store  TeamsChatStore
	verify verifyFunc
	cfg    runConfig

	mu    sync.Mutex
	stats map[string]*siteStats
}

// siteStats accumulates one site's outcome across its batches, so the summary
// is per site rather than per batch.
type siteStats struct {
	checked      int
	roomsMissing int
	subsMismatch int
	ok           int
	unanswered   int
}

func newRunner(store TeamsChatStore, verify verifyFunc, cfg runConfig) *runner {
	return &runner{store: store, verify: verify, cfg: cfg, stats: make(map[string]*siteStats)}
}

// batch is one site's worth of up to BatchSize chats.
type batch struct {
	siteID string
	chats  []model.TeamsChat
}

// run executes one pass. It returns an error only when the initial list fails;
// per-site and per-batch failures are logged and leave those chats flagged for
// the next CronJob run.
func (r *runner) run(ctx context.Context) error {
	chats, err := r.store.ListChatsNeedingVerify(ctx)
	if err != nil {
		return fmt.Errorf("list chats needing verify: %w", err)
	}
	if len(chats) == 0 {
		slog.InfoContext(ctx, "no chats need verification")
		return nil
	}
	batches := planBatches(chats, r.cfg.BatchSize)
	slog.InfoContext(ctx, "verifying room creation", "chats", len(chats), "batches", len(batches))

	sem := make(chan struct{}, r.cfg.MaxWorkers)
	var wg sync.WaitGroup
	for _, b := range batches {
		wg.Add(1)
		sem <- struct{}{}
		go func(b batch) {
			defer wg.Done()
			defer func() { <-sem }()
			r.verifyBatch(ctx, b)
		}(b)
	}
	wg.Wait()
	r.logSummary(ctx)
	return nil
}

// verifyBatch asks one site about one batch and clears the flag for the chats
// that converged. Every failure path leaves the batch's chats flagged.
func (r *runner) verifyBatch(ctx context.Context, b batch) {
	baseURL, ok := r.cfg.SiteURLs[b.siteID]
	if !ok {
		slog.WarnContext(ctx, "no inspector URL for site; skipping batch",
			"site_id", b.siteID, "chats", len(b.chats))
		return
	}

	ids := make([]string, 0, len(b.chats))
	for i := range b.chats {
		ids = append(ids, b.chats[i].ID)
	}
	resp, err := r.verify(ctx, baseURL, ids)
	if err != nil {
		slog.WarnContext(ctx, "inspector call failed; chats stay flagged for the next run",
			"site_id", b.siteID, "chats", len(ids), "error", err)
		return
	}

	byChat := make(map[string]model.TeamsRoomVerifyResult, len(resp.Chats))
	for _, res := range resp.Chats {
		byChat[res.ChatID] = res
	}

	st := &siteStats{}
	refs := make([]VerifiedRef, 0, len(b.chats))
	for i := range b.chats {
		c := &b.chats[i]
		res, answered := byChat[c.ID]
		if !answered {
			st.unanswered++
			slog.WarnContext(ctx, "inspector omitted a requested chat; leaving it flagged",
				"chat_id", c.ID, "site_id", b.siteID)
			continue
		}
		st.checked++
		expected := len(c.Members)
		switch {
		case !res.RoomExists:
			st.roomsMissing++
			r.logMismatch(ctx, c, &res, b.siteID, expected, "missing_room")
		case res.SubscriptionCount != expected:
			st.subsMismatch++
			r.logMismatch(ctx, c, &res, b.siteID, expected, "subscription_mismatch")
		default:
			st.ok++
			refs = append(refs, VerifiedRef{ID: c.ID, UpdatedAt: c.UpdatedAt})
		}
	}
	r.addStats(b.siteID, st)

	if err := r.store.MarkVerified(ctx, refs); err != nil {
		slog.WarnContext(ctx, "mark verified failed; chats re-verify next run",
			"site_id", b.siteID, "chats", len(refs), "error", err)
	}
}

// logMismatch reports one chat that did not converge. accounts_present is the
// diagnostic that separates a genuine gap from a member room-worker legitimately
// skipped (a guest with no account).
func (r *runner) logMismatch(ctx context.Context, c *model.TeamsChat, res *model.TeamsRoomVerifyResult, siteID string, expected int, reason string) {
	slog.WarnContext(ctx, "teams room verification mismatch",
		"chat_id", c.ID,
		"site_id", siteID,
		"room_id", res.RoomID,
		"expected_members", expected,
		"accounts_present", accountsPresent(c.Members),
		"actual_subscriptions", res.SubscriptionCount,
		"room_user_count", res.RoomUserCount,
		"reason", reason)
}

// addStats folds one batch's counters into its site's totals.
func (r *runner) addStats(siteID string, st *siteStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	agg, ok := r.stats[siteID]
	if !ok {
		agg = &siteStats{}
		r.stats[siteID] = agg
	}
	agg.checked += st.checked
	agg.roomsMissing += st.roomsMissing
	agg.subsMismatch += st.subsMismatch
	agg.ok += st.ok
	agg.unanswered += st.unanswered
}

// logSummary emits one line per site that answered.
func (r *runner) logSummary(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for siteID, st := range r.stats {
		slog.InfoContext(ctx, "teams room verification summary",
			"site_id", siteID,
			"chats_checked", st.checked,
			"rooms_missing", st.roomsMissing,
			"subs_mismatched", st.subsMismatch,
			"chats_ok", st.ok,
			"chats_unanswered", st.unanswered)
	}
}

// accountsPresent counts members room-worker would actually subscribe: distinct
// non-empty accounts.
func accountsPresent(members []model.TeamsChatMember) int {
	seen := make(map[string]struct{}, len(members))
	for _, m := range members {
		if m.Account == "" {
			continue
		}
		seen[m.Account] = struct{}{}
	}
	return len(seen)
}

// planBatches groups chats by siteID (deterministic: sites and chats keep input
// order) and chunks each group into batches of at most size.
func planBatches(chats []model.TeamsChat, size int) []batch {
	order := make([]string, 0)
	bySite := make(map[string][]model.TeamsChat)
	//nolint:gocritic // rangeValCopy: c is heavy but using index-range would be less idiomatic
	for _, c := range chats {
		if _, ok := bySite[c.SiteID]; !ok {
			order = append(order, c.SiteID)
		}
		bySite[c.SiteID] = append(bySite[c.SiteID], c)
	}
	var out []batch
	for _, site := range order {
		cs := bySite[site]
		for i := 0; i < len(cs); i += size {
			end := i + size
			if end > len(cs) {
				end = len(cs)
			}
			out = append(out, batch{siteID: site, chats: cs[i:end]})
		}
	}
	return out
}
```

- [ ] **Step 5: Run the tests to confirm they pass**

Run: `make test SERVICE=teams-room-verify`
Expected: PASS (with `-race`, which the Makefile sets — the `stats` map is mutex-guarded).

- [ ] **Step 6: Commit**

```bash
git add teams-room-verify/runner.go teams-room-verify/runner_test.go teams-room-verify/mock_store_test.go
git commit -m "feat(teams-room-verify): add verification runner"
```

---

### Task 10: `teams-room-verify` wiring and deploy

**Files:**
- Create: `teams-room-verify/main.go`, `teams-room-verify/main_test.go`
- Create: `teams-room-verify/deploy/Dockerfile`, `deploy/docker-compose.yml`, `deploy/azure-pipelines.yml`

**Interfaces:**
- Consumes: `Config`, `parseSiteURLs`, `validateConfig` (Task 6); `newMongoStore` (Task 7); `newHTTPVerifier` (Task 8); `newRunner`, `runConfig` (Task 9).
- Produces: a runnable CronJob binary.

- [ ] **Step 1: Write the failing test**

Create `teams-room-verify/main_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// run() must fail fast when required config is absent (no MONGO_URI, no registry).
func TestRun_MissingConfigFailsFast(t *testing.T) {
	t.Setenv("MONGO_URI", "")
	t.Setenv("TEAMS_VERIFY_SITE_URLS", "")
	require.Error(t, run())
}

// A syntactically valid Mongo URI but an unparseable registry must still fail
// before any work begins.
func TestRun_BadSiteRegistryFailsFast(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("TEAMS_VERIFY_SITE_URLS", "not-json")
	require.Error(t, run())
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `make test SERVICE=teams-room-verify`
Expected: FAIL to build — `undefined: run`

- [ ] **Step 3: Write main.go**

Create `teams-room-verify/main.go`:

```go
// Command teams-room-verify is a run-to-completion job (k8s CronJob) that
// audits Teams room creation. It lists every teams_chat with needVerify=true,
// groups them by siteId, and asks each site's teams-room-inspector how many
// rooms and subscriptions it actually holds for those chats. Chats whose room
// exists with one subscription per member have their flag cleared; everything
// else is reported and stays flagged for the next run. One global instance
// serves the whole federation.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

// inspectorTimeout bounds one inspector call. A batch is two projected Mongo
// queries at the far end, so this is generous; a hung site must not eat the
// CronJob's whole deadline.
const inspectorTimeout = 30 * time.Second

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("teams-room-verify failed", "error", err)
		os.Exit(1)
	}
}

// run wires dependencies and performs one pass. It returns an error rather than
// calling os.Exit so deferred cleanup always runs.
func run() error {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if err := validateConfig(cfg); err != nil {
		return err
	}
	sites, err := parseSiteURLs(cfg.SiteURLs)
	if err != nil {
		return err
	}

	// SIGTERM/SIGINT (pod deletion, Job activeDeadlineSeconds) cancels the run so
	// it aborts between operations instead of being killed mid-batch. The run
	// deadline is owned by the Kubernetes CronJob, not an app-level timeout.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	readClient, err := mongoutil.ConnectRead(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("mongo read connect: %w", err)
	}
	defer mongoutil.Disconnect(context.Background(), readClient)

	writeClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("mongo write connect: %w", err)
	}
	defer mongoutil.Disconnect(context.Background(), writeClient)

	store := newMongoStore(readClient.Database(cfg.MongoDB), writeClient.Database(cfg.MongoDB))
	r := newRunner(store, newHTTPVerifier(inspectorTimeout), runConfig{
		BatchSize:  cfg.BatchSize,
		MaxWorkers: cfg.MaxWorkers,
		SiteURLs:   sites,
	})
	if err := r.run(ctx); err != nil {
		return fmt.Errorf("run: %w", err)
	}
	slog.Info("teams-room-verify done")
	return nil
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `make test SERVICE=teams-room-verify && make build SERVICE=teams-room-verify`
Expected: PASS, and a binary at `bin/teams-room-verify`.

- [ ] **Step 5: Write the Dockerfile**

Create `teams-room-verify/deploy/Dockerfile`:

```dockerfile
FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-room-verify/ teams-room-verify/
RUN CGO_ENABLED=0 go build -o /teams-room-verify ./teams-room-verify/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-room-verify /teams-room-verify
USER app
ENTRYPOINT ["/teams-room-verify"]
```

- [ ] **Step 6: Write the compose file**

Create `teams-room-verify/deploy/docker-compose.yml`:

```yaml
name: teams-room-verify

services:
  teams-room-verify:
    build:
      context: ../..
      dockerfile: teams-room-verify/deploy/Dockerfile
    environment:
      - MONGO_URI=${MONGO_URI:-mongodb://mongo:27017}
      - MONGO_DB=${MONGO_DB:-chat}
      # Quoted and NOT written as ${VAR:-default}: the JSON's closing brace would
      # terminate a compose substitution early. Override by exporting the var.
      - 'TEAMS_VERIFY_SITE_URLS={"site-local":"http://teams-room-inspector:8080"}'
      - VERIFY_BATCH_SIZE=${VERIFY_BATCH_SIZE:-200}
      - MAX_WORKERS=${MAX_WORKERS:-8}
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 7: Write the pipeline**

```bash
sed 's/teams-room-creation/teams-room-verify/g' \
  teams-room-creation/deploy/azure-pipelines.yml > teams-room-verify/deploy/azure-pipelines.yml
```

Then open the result and confirm `SERVICE_DIR`, `IMAGE_NAME`, and both `paths.include` lists read `teams-room-verify`.

- [ ] **Step 8: Verify the whole change**

```bash
make fmt
make lint
make test SERVICE=teams-room-verify
make test SERVICE=teams-room-inspector
make test SERVICE=teams-room-creation
make test SERVICE=pkg/model
make test-integration SERVICE=teams-room-verify
make test-integration SERVICE=teams-room-inspector
make test-integration SERVICE=teams-room-creation
make sast
```

All must pass. Then check coverage for the two new packages:

```bash
go test -coverprofile=/tmp/verify.out ./teams-room-verify/ && go tool cover -func=/tmp/verify.out | tail -1
go test -coverprofile=/tmp/inspector.out ./teams-room-inspector/ && go tool cover -func=/tmp/inspector.out | tail -1
```

Expected: both at or above 80%. If either is below, add table-driven cases for the uncovered branches before committing.

- [ ] **Step 9: Commit and push**

```bash
git add teams-room-verify/
git commit -m "feat(teams-room-verify): add service wiring and deploy artifacts"
git push -u origin claude/teams-room-verify-services-sxugxb
```

---

## Notes for the implementer

- **Why the inspector derives room ids.** `room-worker` creates a migrated room with `_id = idgen.DeterministicID([]byte(chatID))` (see `room-worker/teamsroomcreate.go`). The inspector calls the same function, so the two stay in lockstep. Do not reimplement the derivation.
- **Why expected is the raw member count.** This is a deliberate product decision recorded in the spec: a chat containing a member with no `account` (a guest that `room-worker` skips) will report as mismatched every run. `accounts_present` in the log line exists precisely so an operator can tell that case apart from a genuine gap. Do not "fix" it by switching the comparison to `accountsPresent`.
- **No `docs/client-api.md` update.** These are service-to-service HTTP contracts, not `chat.user.*` NATS handlers and not `auth-service` routes.
- **Nothing re-triggers creation.** Verification is read-only apart from clearing its own flag. Do not add a `needCreateRoom` re-flag — it is explicitly out of scope in the spec.
