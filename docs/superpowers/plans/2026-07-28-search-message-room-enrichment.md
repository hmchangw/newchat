# Search-Message Room & Sender Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enrich each `search.messages` hit with a `room` object (`id`, `name`, `type`, `appInfo` for botDM, `hrInfo` for DM), a `sender` object (`account`, `displayName`), and the `tshow` flag — resolved server-side in `search-service`.

**Architecture:** After the Elasticsearch query, `search-service` batch-resolves enrichment from the local per-site MongoDB (`subscriptions` for room type + join key, `users` for HR/sender names, `apps` for bot/app names) and issues a server↔server `RoomsInfoBatch` NATS request (grouped by the hit's `siteId`) only for channel/discussion room names. All enrichment is best-effort: any miss degrades the field to omitted and never fails the search.

**Tech Stack:** Go 1.25, NATS request/reply (`o11ynats.Conn`), MongoDB (`mongo-driver/v2`), `go.uber.org/mock` + hand-written fakes, `testify`.

**Spec:** `docs/superpowers/specs/2026-07-28-search-message-room-enrichment-design.md`

## Global Constraints

- Go 1.25; single root `go.mod`. Use `make` targets only — never raw `go`.
- TDD Red→Green→Refactor per task; commit after each task passes.
- Coverage: ≥80% per package, target ≥90% for the enrichment logic.
- Errors: wrap with `fmt.Errorf("desc: %w", err)`; never bare. Client-facing errors via `errcode`. Enrichment failures are logged with `slog.Warn` and degrade — they are NOT returned to the client.
- Logging: `log/slog` structured key-values only; never log tokens or full message bodies.
- Concurrency: no `time.Sleep` for sync; goroutines bounded and joined via `sync.WaitGroup`.
- Mongo: precise projections only (select just the fields needed); no `$lookup`.
- `search-service` uses `encoding/json` (not sonic) — keep it.
- New model type names must not collide with existing ones (`model.SearchRoom` already exists — the enrichment types are `MessageRoom` / `MessageSender`).
- Client-facing handler change → update `docs/client-api.md` and `docs/client-api/request-reply.md` in the same PR (Task 8).
- Run `make generate SERVICE=search-service` whenever `store.go` interfaces change, before tests.

---

## File Structure

- `pkg/model/search.go` — add `MessageRoom`, `MessageSender`; add `Room`, `Sender`, `TShow` fields to `SearchMessage`. (Task 1)
- `pkg/model/model_test.go` — roundtrip cases. (Task 1)
- `search-service/response.go` — decode `tshow` in `messageSearchHit`; surface in `toSearchMessage`. (Task 2)
- `search-service/store.go` — extend `MongoStore`; add `RoomInfoClient`; add `SubscriptionMeta`, `HRUser`. (Task 3)
- `search-service/store_mongo.go` — implement the three new `MongoStore` methods + indexes. (Task 4)
- `search-service/integration_enrich_test.go` — Mongo integration tests for the new store methods. (Task 4)
- `search-service/room_client.go` — concrete NATS `RoomInfoClient`. (Task 5)
- `search-service/enrich.go` — enrichment orchestration + pure helpers. (Task 6)
- `search-service/enrich_test.go` — unit tests with fakes. (Task 6)
- `search-service/handler.go` — call `enrichMessages`; add `room` field to `handler` + constructor. (Task 7)
- `search-service/handler_test.go` — fakes for new deps + end-to-end enrich test. (Task 7)
- `search-service/main.go` — wire the room client. (Task 7)
- `docs/client-api.md`, `docs/client-api/request-reply.md` — response schema. (Task 8)

---

## Task 1: Model types & SearchMessage fields

**Files:**
- Modify: `pkg/model/search.go` (`SearchMessage` struct ~lines 31-47; add new types after it)
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Produces: `model.MessageRoom{ID string, Name string, Type RoomType, AppInfo *AppSubscription, HRInfo *SubscriptionHRInfo}`; `model.MessageSender{Account string, DisplayName string}`; `SearchMessage.Room *MessageRoom`, `SearchMessage.Sender *MessageSender`, `SearchMessage.TShow bool`.

- [ ] **Step 1: Write the failing test**

Add to `pkg/model/model_test.go` (new test function):

```go
func TestSearchMessageEnrichmentJSON(t *testing.T) {
	m := model.SearchMessage{
		MessageID:   "m1",
		RoomID:      "r1",
		SiteID:      "site-a",
		UserAccount: "alice",
		Content:     "hi",
		CreatedAt:   time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
		TShow:       true,
		Sender:      &model.MessageSender{Account: "alice", DisplayName: "Alice Wong"},
		Room: &model.MessageRoom{
			ID:     "r1",
			Name:   "Bob Chan",
			Type:   model.RoomTypeDM,
			HRInfo: &model.SubscriptionHRInfo{Account: "bob", Name: "陳", EngName: "Bob Chan"},
		},
	}
	roundTrip(t, &m, &model.SearchMessage{})

	// omitempty: a zero-value SearchMessage must not emit room/sender/tshow keys.
	b, err := json.Marshal(model.SearchMessage{MessageID: "x"})
	require.NoError(t, err)
	assert.NotContains(t, string(b), "\"room\"")
	assert.NotContains(t, string(b), "\"sender\"")
	assert.NotContains(t, string(b), "\"tshow\"")
}
```

(Ensure `encoding/json`, `github.com/stretchr/testify/assert`, `github.com/stretchr/testify/require`, and `time` are imported in the test file — they already are.)

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=model` (or `make test` if model has no SERVICE target — the package is `pkg/model`; use `make test` and confirm the new test fails to compile: `MessageSender`/`MessageRoom` undefined).
Expected: FAIL — `undefined: model.MessageSender` / `model.MessageRoom` / unknown field `TShow`.

- [ ] **Step 3: Write minimal implementation**

In `pkg/model/search.go`, add the three fields to `SearchMessage` (after `Card`, before the closing brace) and update its doc comment:

```go
	// Enrichment resolved server-side by search-service (best-effort; a field is
	// omitted when it could not be resolved).
	TShow  bool           `json:"tshow,omitempty"`
	Room   *MessageRoom   `json:"room,omitempty"`
	Sender *MessageSender `json:"sender,omitempty"`
```

Then add the new types below `SearchMessage`:

```go
// MessageRoom is the enriched room object attached to a SearchMessage.
// Type is the room type from the caller's subscription. AppInfo is set only
// for botDM rooms; HRInfo only for dm rooms. Name is the app name (botDM),
// the counterpart's display name (dm), or the canonical room name (channel/
// discussion, from the RoomsInfoBatch RPC).
type MessageRoom struct {
	ID      string              `json:"id"`
	Name    string              `json:"name,omitempty"`
	Type    RoomType            `json:"type,omitempty"`
	AppInfo *AppSubscription    `json:"appInfo,omitempty"`
	HRInfo  *SubscriptionHRInfo `json:"hrInfo,omitempty"`
}

// MessageSender is the enriched author object attached to a SearchMessage.
// DisplayName is the human display name (engName+chineseName, fallback account)
// or, for a bot sender, the app's display name.
type MessageSender struct {
	Account     string `json:"account"`
	DisplayName string `json:"displayName,omitempty"`
}
```

Also update the `SearchMessage` doc comment (lines 26-30) to drop the "no Mongo enrichment / client's responsibility" wording, replacing with: `// SearchMessage is the per-hit projection returned by search.messages, with room/sender enrichment resolved server-side (see MessageRoom / MessageSender).`

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=model` (or `make test`).
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/search.go pkg/model/model_test.go
git commit -m "feat(model): add room/sender/tshow enrichment fields to SearchMessage"
```

---

## Task 2: Decode & surface `tshow`

**Files:**
- Modify: `search-service/response.go` (`messageSearchHit` ~lines 29-43; `toSearchMessage` ~lines 81-96)
- Test: `search-service/response_test.go`

**Interfaces:**
- Consumes: `model.SearchMessage.TShow` (Task 1).
- Produces: `messageSearchHit.TShow bool`; `toSearchMessage` sets `TShow` on its output. `toSearchMessage` remains the base builder that enrichment layers on top of.

- [ ] **Step 1: Write the failing test**

Add to `search-service/response_test.go`:

```go
func TestParseMessagesResponse_DecodesTShow(t *testing.T) {
	raw := json.RawMessage(`{"hits":{"total":{"value":1},"hits":[
		{"_source":{"messageId":"m1","roomId":"r1","siteId":"s","userAccount":"a",
		 "content":"c","createdAt":"2026-04-01T12:00:00Z","threadParentMessageId":"p0","tshow":true}}
	]}}`)
	hits, total, err := parseMessagesResponse(raw)
	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	require.Len(t, hits, 1)
	assert.True(t, hits[0].TShow)

	msg := toSearchMessage(&hits[0])
	assert.True(t, msg.TShow)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=search-service`
Expected: FAIL — `hits[0].TShow` undefined (field not on `messageSearchHit`).

- [ ] **Step 3: Write minimal implementation**

In `search-service/response.go`, add to `messageSearchHit` (after `ThreadParentCreatedAt`, before `Attachments`):

```go
	TShow                 bool               `json:"tshow,omitempty"`
```

In `toSearchMessage`, add `TShow: hit.TShow,` to the returned `model.SearchMessage` literal. Update the stale doc comment on `toSearchMessage` (drop "Display enrichment … is the client's responsibility"; state that enrichment is layered on by `enrichMessages`).

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=search-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add search-service/response.go search-service/response_test.go
git commit -m "feat(search-service): decode and surface tshow on message hits"
```

---

## Task 3: Store interfaces & supporting types

**Files:**
- Modify: `search-service/store.go` (`MongoStore` ~lines 36-43; add types + `RoomInfoClient`)
- Regenerate: `search-service/mock_store_test.go` (via `make generate`)

**Interfaces:**
- Consumes: `model.App`, `model.RoomType`, `model.RoomInfo`.
- Produces:
  - `SubscriptionMeta{RoomType model.RoomType, Name string}`
  - `HRUser{Account, EngName, ChineseName string}`
  - `MongoStore.SubscriptionsByRoomIDs(ctx, account string, roomIDs []string) (map[string]SubscriptionMeta, error)` — keyed by roomID.
  - `MongoStore.UsersByAccounts(ctx, accounts []string) (map[string]HRUser, error)` — keyed by account.
  - `MongoStore.AppsByAssistantNames(ctx, botAccounts []string) (map[string]model.App, error)` — keyed by `assistant.name`.
  - `RoomInfoClient.GetRoomsInfo(ctx, siteID string, roomIDs []string) ([]model.RoomInfo, error)`

- [ ] **Step 1: Write the failing test**

Add a compile-time interface-satisfaction assertion to `search-service/handler_test.go` (proves the new methods exist and shapes are stable):

```go
func TestMongoStoreInterfaceShape(t *testing.T) {
	var _ MongoStore = (*mongoStore)(nil) // must satisfy the extended interface
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=search-service`
Expected: FAIL — `*mongoStore` does not implement `MongoStore` (missing `SubscriptionsByRoomIDs`, `UsersByAccounts`, `AppsByAssistantNames`). (Compile error is the expected red.)

- [ ] **Step 3: Write minimal implementation**

In `search-service/store.go`, extend the `MongoStore` interface and add the types + client interface:

```go
// SubscriptionMeta is the caller's subscription projection used for enrichment:
// the room type plus the join-key Name (DM counterpart account / botDM bot account).
type SubscriptionMeta struct {
	RoomType model.RoomType
	Name     string
}

// HRUser is the users-collection projection used to render display / HR names.
type HRUser struct {
	Account     string
	EngName     string
	ChineseName string
}

type MongoStore interface {
	SearchAppsByName(
		ctx context.Context,
		query, account string,
		assistantEnabled *bool,
		offset, limit int,
	) ([]model.App, error)

	// SubscriptionsByRoomIDs returns the caller's subscription meta for the given
	// rooms, keyed by roomID. Rooms with no subscription are omitted.
	SubscriptionsByRoomIDs(ctx context.Context, account string, roomIDs []string) (map[string]SubscriptionMeta, error)

	// UsersByAccounts returns HR/display projections keyed by account. Missing accounts are omitted.
	UsersByAccounts(ctx context.Context, accounts []string) (map[string]HRUser, error)

	// AppsByAssistantNames returns apps keyed by their assistant.name (bot account). Missing are omitted.
	AppsByAssistantNames(ctx context.Context, botAccounts []string) (map[string]model.App, error)
}

// RoomInfoClient fetches canonical room metadata from room-service on a given
// site (the RoomsInfoBatch server↔server RPC), used only for channel/discussion
// room names (cross-site safe: caller groups by the hit's siteID).
type RoomInfoClient interface {
	GetRoomsInfo(ctx context.Context, siteID string, roomIDs []string) ([]model.RoomInfo, error)
}
```

Then run: `make generate SERVICE=search-service` to regenerate `mock_store_test.go` (adds mocks for the new methods + `MockRoomInfoClient`).

- [ ] **Step 4: Run test to verify it fails differently, then implement store methods in Task 4**

Run: `make test SERVICE=search-service`
Expected: still FAIL — now because `*mongoStore` has no method bodies yet (implemented in Task 4). This task's deliverable is the interface + generated mocks compiling into the test binary. Proceed to Task 4 before committing (they form one reviewable unit); commit at the end of Task 4.

---

## Task 4: Mongo store implementations

**Files:**
- Modify: `search-service/store_mongo.go` (add collections + 3 methods + indexes)
- Test: `search-service/integration_enrich_test.go` (new, `//go:build integration`)

**Interfaces:**
- Consumes: `SubscriptionMeta`, `HRUser` (Task 3).
- Produces: concrete `*mongoStore` implementations of the Task 3 methods.

Collections & schema (from `pkg/model`): `subscriptions` docs have `u.account` (string), `roomId`, `roomType`, `name`. `users` docs have `account`, `engName`, `chineseName`. `apps` docs have `_id`, `name`, `assistant.name`, etc.

- [ ] **Step 1: Write the failing test**

Create `search-service/integration_enrich_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMongoStore_EnrichLookups(t *testing.T) {
	db := testutil.MongoDB(t, "search_service_test") // per-test isolated DB
	ctx := context.Background()
	store := newMongoStore(db)

	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		bson.M{"_id": "s1", "u": bson.M{"account": "alice"}, "roomId": "rDM", "roomType": "dm", "name": "bob"},
		bson.M{"_id": "s2", "u": bson.M{"account": "alice"}, "roomId": "rBot", "roomType": "botDM", "name": "helper.bot"},
		bson.M{"_id": "s3", "u": bson.M{"account": "alice"}, "roomId": "rCh", "roomType": "channel", "name": "General"},
		bson.M{"_id": "s4", "u": bson.M{"account": "carol"}, "roomId": "rDM", "roomType": "dm", "name": "alice"},
	})
	require.NoError(t, err)
	_, err = db.Collection("users").InsertMany(ctx, []any{
		bson.M{"_id": "u-bob", "account": "bob", "engName": "Bob Chan", "chineseName": "陳大文"},
	})
	require.NoError(t, err)
	_, err = db.Collection("apps").InsertMany(ctx, []any{
		bson.M{"_id": "app-1", "name": "Helper", "assistant": bson.M{"name": "helper.bot", "enabled": true}},
	})
	require.NoError(t, err)

	subs, err := store.SubscriptionsByRoomIDs(ctx, "alice", []string{"rDM", "rBot", "rCh", "rMissing"})
	require.NoError(t, err)
	assert.Equal(t, model.RoomTypeDM, subs["rDM"].RoomType)
	assert.Equal(t, "bob", subs["rDM"].Name)
	assert.Equal(t, model.RoomTypeBotDM, subs["rBot"].RoomType)
	assert.Equal(t, "helper.bot", subs["rBot"].Name)
	assert.Equal(t, model.RoomTypeChannel, subs["rCh"].RoomType)
	_, missing := subs["rMissing"]
	assert.False(t, missing)
	// carol's row for rDM must NOT leak into alice's result
	assert.Equal(t, "bob", subs["rDM"].Name)

	users, err := store.UsersByAccounts(ctx, []string{"bob", "nobody"})
	require.NoError(t, err)
	assert.Equal(t, "Bob Chan", users["bob"].EngName)
	assert.Equal(t, "陳大文", users["bob"].ChineseName)
	_, ok := users["nobody"]
	assert.False(t, ok)

	apps, err := store.AppsByAssistantNames(ctx, []string{"helper.bot", "ghost.bot"})
	require.NoError(t, err)
	assert.Equal(t, "Helper", apps["helper.bot"].Name)
	_, ok = apps["ghost.bot"]
	assert.False(t, ok)

	// empty input → empty map, no query error
	empty, err := store.SubscriptionsByRoomIDs(ctx, "alice", nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}
```

Note: confirm the shared Mongo helper name in `search-service/setup_shared_test.go` (it wraps `testutil.MongoDB`). If the helper is named differently (e.g. `mongoDBForTest`), use that name instead of `testMongoDB`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=search-service` (requires Docker).
Expected: FAIL — methods unimplemented (compile error) or, once stubbed, empty results.

If Docker is unavailable in this environment, note it and rely on the Task 6/7 unit tests (fakes) as the functional gate; still implement the methods and keep this integration test in the tree.

- [ ] **Step 3: Write minimal implementation**

In `search-service/store_mongo.go`, add collections to the struct and constructor:

```go
type mongoStore struct {
	apps          *mongo.Collection
	subscriptions *mongo.Collection
	users         *mongo.Collection
}

func newMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		apps:          db.Collection("apps"),
		subscriptions: db.Collection("subscriptions"),
		users:         db.Collection("users"),
	}
}
```

Add the three methods (precise projections; missing keys omitted; empty input short-circuits):

```go
func (s *mongoStore) SubscriptionsByRoomIDs(ctx context.Context, account string, roomIDs []string) (map[string]SubscriptionMeta, error) {
	out := map[string]SubscriptionMeta{}
	if len(roomIDs) == 0 {
		return out, nil
	}
	filter := bson.M{"u.account": account, "roomId": bson.M{"$in": roomIDs}}
	proj := bson.M{"roomId": 1, "roomType": 1, "name": 1}
	cur, err := s.subscriptions.Find(ctx, filter, options.Find().SetProjection(proj))
	if err != nil {
		return nil, fmt.Errorf("find subscriptions: %w", err)
	}
	defer cur.Close(ctx)
	var rows []struct {
		RoomID   string         `bson:"roomId"`
		RoomType model.RoomType `bson:"roomType"`
		Name     string         `bson:"name"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode subscriptions: %w", err)
	}
	for _, r := range rows {
		out[r.RoomID] = SubscriptionMeta{RoomType: r.RoomType, Name: r.Name}
	}
	return out, nil
}

func (s *mongoStore) UsersByAccounts(ctx context.Context, accounts []string) (map[string]HRUser, error) {
	out := map[string]HRUser{}
	if len(accounts) == 0 {
		return out, nil
	}
	filter := bson.M{"account": bson.M{"$in": accounts}}
	proj := bson.M{"account": 1, "engName": 1, "chineseName": 1}
	cur, err := s.users.Find(ctx, filter, options.Find().SetProjection(proj))
	if err != nil {
		return nil, fmt.Errorf("find users: %w", err)
	}
	defer cur.Close(ctx)
	var rows []struct {
		Account     string `bson:"account"`
		EngName     string `bson:"engName"`
		ChineseName string `bson:"chineseName"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode users: %w", err)
	}
	for _, r := range rows {
		out[r.Account] = HRUser{Account: r.Account, EngName: r.EngName, ChineseName: r.ChineseName}
	}
	return out, nil
}

func (s *mongoStore) AppsByAssistantNames(ctx context.Context, botAccounts []string) (map[string]model.App, error) {
	out := map[string]model.App{}
	if len(botAccounts) == 0 {
		return out, nil
	}
	filter := bson.M{"assistant.name": bson.M{"$in": botAccounts}}
	cur, err := s.apps.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("find apps by assistant: %w", err)
	}
	defer cur.Close(ctx)
	var apps []model.App
	if err := cur.All(ctx, &apps); err != nil {
		return nil, fmt.Errorf("decode apps: %w", err)
	}
	for _, a := range apps {
		if a.Assistant != nil && a.Assistant.Name != "" {
			out[a.Assistant.Name] = a
		}
	}
	return out, nil
}
```

Add the import for `options` (`go.mongodb.org/mongo-driver/v2/mongo/options`) to `store_mongo.go`.

Add supporting indexes in `ensureIndexes` (idempotent `CreateOne`), so the enrichment lookups are index-backed:

```go
	if _, err := s.subscriptions.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "u.account", Value: 1}, {Key: "roomId", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure subscriptions (u.account, roomId) index: %w", err)
	}
	if _, err := s.users.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "account", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure users (account) index: %w", err)
	}
	if _, err := s.apps.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "assistant.name", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure apps (assistant.name) index: %w", err)
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=search-service` (unit binary compiles; `TestMongoStoreInterfaceShape` now passes).
Run (if Docker available): `make test-integration SERVICE=search-service` → PASS.

- [ ] **Step 5: Commit**

```bash
git add search-service/store.go search-service/store_mongo.go search-service/mock_store_test.go search-service/integration_enrich_test.go search-service/handler_test.go
git commit -m "feat(search-service): add subscriptions/users/apps enrichment store lookups"
```

---

## Task 5: NATS room-info client

**Files:**
- Create: `search-service/room_client.go`
- Test: covered via the handler/enrich fakes (Tasks 6-7); no separate unit test (thin RPC wrapper, integration-exercised).

**Interfaces:**
- Consumes: `o11ynats.Conn`, `subject.RoomsInfoBatch`, `model.RoomsInfoBatchRequest/Response`, `errcode.Parse`.
- Produces: `newRoomClient(nc *o11ynats.Conn) *roomClient` implementing `RoomInfoClient`.

- [ ] **Step 1: Write the implementation** (thin adapter mirroring `user-service/roomclient`)

Create `search-service/room_client.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// roomRPCTimeout bounds each RoomsInfoBatch round trip.
const roomRPCTimeout = 5 * time.Second

// roomClient is the NATS-backed RoomInfoClient. It issues the RoomsInfoBatch
// server↔server RPC to room-service on the target site.
type roomClient struct {
	nc *o11ynats.Conn
}

func newRoomClient(nc *o11ynats.Conn) *roomClient { return &roomClient{nc: nc} }

func (c *roomClient) GetRoomsInfo(ctx context.Context, siteID string, roomIDs []string) ([]model.RoomInfo, error) {
	req, err := json.Marshal(model.RoomsInfoBatchRequest{RoomIDs: roomIDs})
	if err != nil {
		return nil, fmt.Errorf("marshal rooms-info request: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.RoomsInfoBatch(siteID), req, roomRPCTimeout)
	if err != nil {
		return nil, fmt.Errorf("rooms-info rpc: %w", err)
	}
	if e, ok := errcode.Parse(msg.Data); ok {
		return nil, e
	}
	var out model.RoomsInfoBatchResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return nil, fmt.Errorf("decode rooms-info response: %w", err)
	}
	return out.Rooms, nil
}
```

- [ ] **Step 2: Verify it compiles**

Run: `make build SERVICE=search-service`
Expected: builds (client not yet wired — that's Task 7). If `make build` fails only because `newRoomClient` is unused, ignore for now; it is consumed in Task 7. (If the linter's `unused` check blocks the build, proceed directly into Task 7 before running lint, and commit them together.)

- [ ] **Step 3: Commit**

```bash
git add search-service/room_client.go
git commit -m "feat(search-service): add RoomsInfoBatch NATS client"
```

---

## Task 6: Enrichment orchestration

**Files:**
- Create: `search-service/enrich.go`
- Test: `search-service/enrich_test.go`

**Interfaces:**
- Consumes: `messageSearchHit` (response.go), `MongoStore`, `RoomInfoClient`, `SubscriptionMeta`, `HRUser`, `model.*`, `displayfmt.CombineWithFallback`, `model.IsBot`.
- Produces: `(h *handler) enrichMessages(ctx, account string, hits []messageSearchHit) []model.SearchMessage`; pure helpers `resolveSenderName`, `keysOf`.
- Note: `enrichMessages` reads `h.mongo` and `h.room`; the `h.room RoomInfoClient` field is added to the `handler` struct in Task 7. To keep this task compiling on its own, add the field in Task 7 **before** running these tests — or add the field stub as the first step here. This plan adds the field in Task 7; run Task 6's tests after Task 7's Step 3. If you prefer strict per-task green, add the `room RoomInfoClient` field + constructor param (Task 7 Step 3) as the first action of this task.

- [ ] **Step 1: Write the failing tests**

Create `search-service/enrich_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// fakeMongo implements MongoStore for enrichment tests.
type fakeMongo struct {
	subs     map[string]SubscriptionMeta
	subsErr  error
	users    map[string]HRUser
	usersErr error
	apps     map[string]model.App
	appsErr  error

	subsAccount string
	subsRoomIDs []string
	userAccts   []string
	appBots     []string
}

func (f *fakeMongo) SearchAppsByName(_ context.Context, _, _ string, _ *bool, _, _ int) ([]model.App, error) {
	return nil, nil
}
func (f *fakeMongo) SubscriptionsByRoomIDs(_ context.Context, account string, roomIDs []string) (map[string]SubscriptionMeta, error) {
	f.subsAccount, f.subsRoomIDs = account, roomIDs
	if f.subsErr != nil {
		return nil, f.subsErr
	}
	return f.subs, nil
}
func (f *fakeMongo) UsersByAccounts(_ context.Context, accounts []string) (map[string]HRUser, error) {
	f.userAccts = accounts
	if f.usersErr != nil {
		return nil, f.usersErr
	}
	return f.users, nil
}
func (f *fakeMongo) AppsByAssistantNames(_ context.Context, bots []string) (map[string]model.App, error) {
	f.appBots = bots
	if f.appsErr != nil {
		return nil, f.appsErr
	}
	return f.apps, nil
}

// fakeRoom implements RoomInfoClient.
type fakeRoom struct {
	bySite map[string][]model.RoomInfo
	err    error
	calls  int
}

func (f *fakeRoom) GetRoomsInfo(_ context.Context, siteID string, _ []string) ([]model.RoomInfo, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.bySite[siteID], nil
}

func enrichHandler(m MongoStore, r RoomInfoClient) *handler {
	h := newTestHandler(&fakeStore{}, m, nil, newFakeCache())
	h.room = r
	return h
}

func hit(id, room, site, sender string, ttype string) messageSearchHit {
	return messageSearchHit{MessageID: id, RoomID: room, SiteID: site, UserAccount: sender, CreatedAt: time.Now().UTC()}
}

func TestEnrichMessages_DM(t *testing.T) {
	m := &fakeMongo{
		subs:  map[string]SubscriptionMeta{"rDM": {RoomType: model.RoomTypeDM, Name: "bob"}},
		users: map[string]HRUser{"bob": {Account: "bob", EngName: "Bob Chan", ChineseName: "陳"}, "alice": {Account: "alice", EngName: "Alice", ChineseName: ""}},
	}
	h := enrichHandler(m, &fakeRoom{})
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rDM", "site-a", "alice", "")})
	require.Len(t, out, 1)
	require.NotNil(t, out[0].Room)
	assert.Equal(t, model.RoomTypeDM, out[0].Room.Type)
	assert.Equal(t, "Bob Chan 陳", out[0].Room.Name)
	require.NotNil(t, out[0].Room.HRInfo)
	assert.Equal(t, "陳", out[0].Room.HRInfo.Name)
	assert.Equal(t, "Bob Chan", out[0].Room.HRInfo.EngName)
	assert.Nil(t, out[0].Room.AppInfo)
	require.NotNil(t, out[0].Sender)
	assert.Equal(t, "alice", out[0].Sender.Account)
	assert.Equal(t, "Alice", out[0].Sender.DisplayName)
}

func TestEnrichMessages_BotDM(t *testing.T) {
	m := &fakeMongo{
		subs: map[string]SubscriptionMeta{"rBot": {RoomType: model.RoomTypeBotDM, Name: "helper.bot"}},
		apps: map[string]model.App{"helper.bot": {ID: "app1", Name: "Helper", Assistant: &model.AppAssistant{Name: "helper.bot"}}},
	}
	h := enrichHandler(m, &fakeRoom{})
	// sender is the bot itself
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rBot", "site-a", "helper.bot", "")})
	require.NotNil(t, out[0].Room)
	assert.Equal(t, model.RoomTypeBotDM, out[0].Room.Type)
	assert.Equal(t, "Helper", out[0].Room.Name)
	require.NotNil(t, out[0].Room.AppInfo)
	assert.Equal(t, "app1", out[0].Room.AppInfo.AppID)
	assert.Nil(t, out[0].Room.HRInfo)
	// bot sender display name = app name
	assert.Equal(t, "Helper", out[0].Sender.DisplayName)
}

func TestEnrichMessages_ChannelUsesRoomBatch(t *testing.T) {
	m := &fakeMongo{
		subs:  map[string]SubscriptionMeta{"rCh": {RoomType: model.RoomTypeChannel, Name: "ignored"}},
		users: map[string]HRUser{"alice": {Account: "alice", EngName: "Alice"}},
	}
	r := &fakeRoom{bySite: map[string][]model.RoomInfo{
		"site-b": {{RoomID: "rCh", Found: true, Name: "General"}},
	}}
	h := enrichHandler(m, r)
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rCh", "site-b", "alice", "")})
	assert.Equal(t, model.RoomTypeChannel, out[0].Room.Type)
	assert.Equal(t, "General", out[0].Room.Name)
	assert.Nil(t, out[0].Room.AppInfo)
	assert.Nil(t, out[0].Room.HRInfo)
	assert.Equal(t, 1, r.calls)
}

func TestEnrichMessages_MissingSubscriptionFallsBackToRoomBatch(t *testing.T) {
	m := &fakeMongo{subs: map[string]SubscriptionMeta{}} // no sub for the room
	r := &fakeRoom{bySite: map[string][]model.RoomInfo{
		"site-a": {{RoomID: "rX", Found: true, Name: "Mystery"}},
	}}
	h := enrichHandler(m, r)
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rX", "site-a", "alice", "")})
	assert.Equal(t, "Mystery", out[0].Room.Name)
	assert.Equal(t, model.RoomType(""), out[0].Room.Type) // type unknown without a sub
}

func TestEnrichMessages_DegradesOnAllErrors(t *testing.T) {
	m := &fakeMongo{subsErr: errors.New("db down"), usersErr: errors.New("db down"), appsErr: errors.New("db down")}
	r := &fakeRoom{err: errors.New("rpc down")}
	h := enrichHandler(m, r)
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rCh", "site-a", "alice", "")})
	require.Len(t, out, 1)
	// still returns the base message; room present with id, sender falls back to account
	require.NotNil(t, out[0].Room)
	assert.Equal(t, "rCh", out[0].Room.ID)
	assert.Equal(t, "alice", out[0].Sender.DisplayName) // fallback to account
}

func TestEnrichMessages_Empty(t *testing.T) {
	h := enrichHandler(&fakeMongo{}, &fakeRoom{})
	out := h.enrichMessages(context.Background(), "alice", nil)
	assert.Empty(t, out)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=search-service`
Expected: FAIL — `enrichMessages` undefined and `h.room` field undefined. (Add the field/constructor per Task 7 Step 3 first if you want an isolated red→green here.)

- [ ] **Step 3: Write the implementation**

Create `search-service/enrich.go`:

```go
package main

import (
	"context"
	"log/slog"
	"sync"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/model"
)

// maxSiteFanout bounds concurrent RoomsInfoBatch RPCs across distinct sites.
const maxSiteFanout = 8

// enrichMessages layers room/sender enrichment onto the base hit projections.
// Every lookup is best-effort: a failure is logged and the affected field is
// omitted — the search response is never failed by enrichment.
func (h *handler) enrichMessages(ctx context.Context, account string, hits []messageSearchHit) []model.SearchMessage {
	out := make([]model.SearchMessage, 0, len(hits))
	for i := range hits {
		out = append(out, toSearchMessage(&hits[i]))
	}
	if len(hits) == 0 {
		return out
	}

	roomIDSet := map[string]struct{}{}
	senderSet := map[string]struct{}{}
	roomSite := map[string]string{}
	for i := range hits {
		roomIDSet[hits[i].RoomID] = struct{}{}
		roomSite[hits[i].RoomID] = hits[i].SiteID
		if hits[i].UserAccount != "" {
			senderSet[hits[i].UserAccount] = struct{}{}
		}
	}

	subs, err := h.mongo.SubscriptionsByRoomIDs(ctx, account, keysOf(roomIDSet))
	if err != nil {
		slog.Warn("search enrich: subscriptions lookup failed", "account", account, "error", err)
		subs = map[string]SubscriptionMeta{}
	}

	// Partition rooms and collect the join keys for the batch lookups.
	dmCounterparts := map[string]struct{}{}
	botAccounts := map[string]struct{}{}
	channelRoomsBySite := map[string][]string{}
	for rid := range roomIDSet {
		meta, ok := subs[rid]
		if !ok {
			channelRoomsBySite[roomSite[rid]] = append(channelRoomsBySite[roomSite[rid]], rid)
			continue
		}
		switch meta.RoomType {
		case model.RoomTypeDM:
			if meta.Name != "" {
				dmCounterparts[meta.Name] = struct{}{}
			}
		case model.RoomTypeBotDM:
			if meta.Name != "" {
				botAccounts[meta.Name] = struct{}{}
			}
		default:
			channelRoomsBySite[roomSite[rid]] = append(channelRoomsBySite[roomSite[rid]], rid)
		}
	}

	// Sender names: bots resolve via apps, humans via users.
	userAccts := map[string]struct{}{}
	for a := range dmCounterparts {
		userAccts[a] = struct{}{}
	}
	for a := range senderSet {
		if model.IsBot(a) {
			botAccounts[a] = struct{}{}
		} else {
			userAccts[a] = struct{}{}
		}
	}

	users := map[string]HRUser{}
	if len(userAccts) > 0 {
		u, err := h.mongo.UsersByAccounts(ctx, keysOf(userAccts))
		if err != nil {
			slog.Warn("search enrich: users lookup failed", "error", err)
		} else {
			users = u
		}
	}
	apps := map[string]model.App{}
	if len(botAccounts) > 0 {
		a, err := h.mongo.AppsByAssistantNames(ctx, keysOf(botAccounts))
		if err != nil {
			slog.Warn("search enrich: apps lookup failed", "error", err)
		} else {
			apps = a
		}
	}

	roomNames := h.fetchRoomNames(ctx, channelRoomsBySite)

	for i := range hits {
		rid := hits[i].RoomID
		out[i].Sender = &model.MessageSender{
			Account:     hits[i].UserAccount,
			DisplayName: resolveSenderName(hits[i].UserAccount, users, apps),
		}
		room := &model.MessageRoom{ID: rid}
		if meta, ok := subs[rid]; ok {
			room.Type = meta.RoomType
			switch meta.RoomType {
			case model.RoomTypeDM:
				if hr, ok := users[meta.Name]; ok {
					room.HRInfo = &model.SubscriptionHRInfo{Account: hr.Account, Name: hr.ChineseName, EngName: hr.EngName}
					room.Name = displayfmt.CombineWithFallback(hr.EngName, hr.ChineseName, meta.Name)
				} else {
					room.Name = meta.Name
				}
			case model.RoomTypeBotDM:
				if app, ok := apps[meta.Name]; ok {
					room.AppInfo = model.AppSubscriptionFromApp(&app)
					room.Name = app.Name
				} else {
					room.Name = meta.Name
				}
			default:
				room.Name = roomNames[rid]
			}
		} else {
			room.Name = roomNames[rid]
		}
		out[i].Room = room
	}
	return out
}

// fetchRoomNames issues one RoomsInfoBatch RPC per distinct site (bounded
// fan-out), returning roomID→name for rooms the RPC resolved. A per-site
// failure is logged and skipped.
func (h *handler) fetchRoomNames(ctx context.Context, bySite map[string][]string) map[string]string {
	names := map[string]string{}
	if len(bySite) == 0 || h.room == nil {
		return names
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxSiteFanout)
	for site, ids := range bySite {
		site, ids := site, ids
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			infos, err := h.room.GetRoomsInfo(ctx, site, ids)
			if err != nil {
				slog.Warn("search enrich: room-info rpc failed", "site", site, "error", err)
				return
			}
			mu.Lock()
			for _, info := range infos {
				if info.Found && info.Name != "" {
					names[info.RoomID] = info.Name
				}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return names
}

// resolveSenderName returns the sender's display name: the app name for a bot
// account, else engName+chineseName (fallback account) from the users map.
func resolveSenderName(account string, users map[string]HRUser, apps map[string]model.App) string {
	if account == "" {
		return ""
	}
	if model.IsBot(account) {
		if app, ok := apps[account]; ok && app.Name != "" {
			return app.Name
		}
		return account
	}
	if hr, ok := users[account]; ok {
		return displayfmt.CombineWithFallback(hr.EngName, hr.ChineseName, account)
	}
	return account
}

func keysOf(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass** (after Task 7 Step 3 adds `h.room`)

Run: `make test SERVICE=search-service`
Expected: PASS (all `TestEnrichMessages_*`).

- [ ] **Step 5: Commit** (jointly with Task 7 if the field was added there)

```bash
git add search-service/enrich.go search-service/enrich_test.go
git commit -m "feat(search-service): batch room/sender enrichment for message hits"
```

---

## Task 7: Wire enrichment into the handler & main

**Files:**
- Modify: `search-service/handler.go` (`handler` struct ~lines 36-42; `searchMessages` ~lines 123-127)
- Modify: `search-service/main.go` (set `handler.room` after construction)
- Test: `search-service/handler_test.go` (end-to-end enrich assertion)

**Interfaces:**
- Consumes: `enrichMessages` (Task 6), `newRoomClient` (Task 5).
- Produces: `handler.room RoomInfoClient` field (settable post-construction).

**Design note:** `newHandler` has 9 call sites (main + 8 test files). Do NOT change its signature. Add the `room` field to the struct, leave `newHandler` unchanged (a handler built by it has `room == nil`, which `fetchRoomNames` treats as a no-op), and set `h.room` explicitly in `main.go` and in the enrichment tests.

- [ ] **Step 1: Write the failing test**

Add to `search-service/handler_test.go` an end-to-end test that drives `searchMessages` with a fake ES body and asserts enrichment. Reuse the existing fake-store harness; set `searchBody` to one message hit and inject a `fakeMongo`/`fakeRoom`:

```go
func TestSearchMessages_EnrichesResponse(t *testing.T) {
	store := &fakeStore{searchBody: json.RawMessage(`{"hits":{"total":{"value":1},"hits":[
		{"_source":{"messageId":"m1","roomId":"rDM","siteId":"site-a","userAccount":"alice",
		 "content":"hi","createdAt":"2026-04-01T12:00:00Z"}}
	]}}`)}
	m := &fakeMongo{
		subs:  map[string]SubscriptionMeta{"rDM": {RoomType: model.RoomTypeDM, Name: "bob"}},
		users: map[string]HRUser{"bob": {Account: "bob", EngName: "Bob Chan"}, "alice": {Account: "alice", EngName: "Alice"}},
	}
	h := newTestHandler(store, m, nil, newFakeCache())
	h.room = &fakeRoom{}

	resp, err := invokeSearchMessages(t, h, "alice", model.SearchMessagesRequest{Query: "hi"})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	require.NotNil(t, resp.Messages[0].Room)
	assert.Equal(t, model.RoomTypeDM, resp.Messages[0].Room.Type)
	assert.Equal(t, "Bob Chan", resp.Messages[0].Room.Name)
	assert.Equal(t, "Alice", resp.Messages[0].Sender.DisplayName)
}
```

If a helper to invoke the `natsrouter`-registered handler with a fake subject/account does not already exist in `handler_test.go`, reuse whatever pattern the existing message-search tests use (they already call `searchMessages` through the router or directly). Match the existing invocation helper — do not invent a new one if the file already drives `searchMessages` in other tests. (Grep `handler_test.go` for the existing `searchMessages` call site and mirror it; name the helper `invokeSearchMessages` only if none exists.)

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=search-service`
Expected: FAIL — `h.room` undefined and response not enriched.

- [ ] **Step 3: Write the implementation**

In `search-service/handler.go`, add the field to the struct only (leave `newHandler` as-is):

```go
type handler struct {
	store SearchStore
	mongo MongoStore
	users SearchUsersClient
	cache RestrictedRoomCache
	room  RoomInfoClient
	cfg   handlerConfig
}
```

Replace the projection loop in `searchMessages` (lines 123-127) with:

```go
	messages := h.enrichMessages(ctx, account, hits)
	return &model.SearchMessagesResponse{Messages: messages, Total: total}, nil
```

Leave `newHandler` and `newTestHandler` signatures unchanged — a handler they build has `room == nil`, and `fetchRoomNames` no-ops on nil (channel names come back empty; the degradation tests cover this). No other call site changes.

In `search-service/main.go`, set the room client on the constructed handler (the `nc` from `natsutil.Connect` is already `*o11ynats.Conn`):

```go
	handler := newHandler(store, mongoStore, usersClient, cache, &handlerConfig{ /* ...existing... */ })
	handler.room = newRoomClient(nc)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=search-service`
Expected: PASS (new end-to-end test + all Task 6 enrich tests + existing suite).

- [ ] **Step 5: Commit**

```bash
git add search-service/handler.go search-service/handler_test.go search-service/main.go search-service/enrich.go search-service/enrich_test.go
git commit -m "feat(search-service): enrich search.messages with room and sender"
```

---

## Task 8: Client-API docs

**Files:**
- Modify: `docs/client-api.md` (`search.messages` section — example ~lines 3877-3909; `SearchMessage` fields table ~lines 3916-3935)
- Modify: `docs/client-api/request-reply.md` (`search.messages` ~lines 1237-1271)

**Interfaces:** none (docs only).

- [ ] **Step 1: Update `docs/client-api.md`**

Extend the JSON example (inside the single message object, after `card`) with:

```json
      "tshow": true,
      "sender": { "account": "alice", "displayName": "Alice Wong" },
      "room": {
        "id": "r1",
        "name": "Bob Chan",
        "type": "dm",
        "hrInfo": { "account": "bob", "name": "陳大文", "engName": "Bob Chan" }
      }
```

Change the `SearchMessage` heading line (3916) from "(all sourced directly from the ES message index — no Mongo round-trip)" to "(base fields from the ES message index; `room`/`sender` resolved server-side)". Append these rows to the `SearchMessage` fields table:

```
| `tshow` | boolean | omitted when false / not a thread reply shown in channel |
| `sender` | [MessageSender](#messagesender) | present on every hit (account always set; displayName best-effort) |
| `room` | [MessageRoom](#messageroom) | present on every hit (id always set; name/type/appInfo/hrInfo best-effort) |
```

Replace the trailing "Display fields (user name, room name) are intentionally NOT carried…" paragraph (3935) with a note that `room`/`sender` are now server-resolved best-effort and may be partial for federated rooms whose origin site is unreachable.

Add two shared-type definitions. Reference existing anchors for `AppSubscription` and `SubscriptionHRInfo` if they already exist in §3.0 (grep for `#### AppSubscription` / `hrInfo`); otherwise add compact tables. New tables:

```
##### MessageSender
| Field | Type | Notes |
|---|---|---|
| `account` | string | sender's account |
| `displayName` | string | engName+chineseName (fallback account); app name for a bot sender |

##### MessageRoom
| Field | Type | Notes |
|---|---|---|
| `id` | string | roomId |
| `name` | string | app name (botDM) / counterpart name (dm) / canonical room name (channel, discussion) |
| `type` | string | `channel` \| `dm` \| `botDM` \| `discussion`; omitted when the caller has no subscription |
| `appInfo` | [AppSubscription](#appsubscription) | botDM only |
| `hrInfo` | [SubscriptionHRInfo](#subscriptionhrinfo) | dm only |
```

- [ ] **Step 2: Update `docs/client-api/request-reply.md`**

In the `search.messages` section, update the `SearchMessage` field enumeration (line ~1268-1271) to add `tshow`, `sender {account, displayName}`, and `room {id, name, type, appInfo?, hrInfo?}`, mirroring the canonical doc.

- [ ] **Step 3: Verify links & consistency**

Run: `grep -n "messagesender\|messageroom\|appInfo\|hrInfo" docs/client-api.md` — confirm anchors resolve (headings match the `#messagesender` / `#messageroom` slugs). Fix any dangling links.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs(client-api): document room/sender/tshow on search.messages"
```

---

## Task 9: Full verification & push

**Files:** none (verification).

- [ ] **Step 1: Regenerate mocks (idempotent check)**

Run: `make generate SERVICE=search-service`
Expected: no unexpected diff beyond Task 3's regen. If a diff appears, commit it.

- [ ] **Step 2: Lint**

Run: `make lint`
Expected: clean. Fix any `goimports`/`staticcheck`/`errcheck` issues (e.g. unused params, unchecked errors).

- [ ] **Step 3: Unit tests with race**

Run: `make test SERVICE=search-service` then `make test SERVICE=model`
Expected: PASS. Confirm coverage: `go tool cover` via the Makefile's coverage path if available; the enrichment logic (`enrich.go`) should be ≥90%.

- [ ] **Step 4: Integration tests (if Docker available)**

Run: `make test-integration SERVICE=search-service`
Expected: PASS (includes `integration_enrich_test.go`). If Docker is unavailable, record that these were not run.

- [ ] **Step 5: SAST**

Run: `make sast`
Expected: no medium+ findings. (No `InsecureSkipVerify`, unsafe conversions, or command exec introduced.)

- [ ] **Step 6: Push**

```bash
git push -u origin claude/roomid-search-response-2mgtya
```

(Retry with exponential backoff on network errors: 2s, 4s, 8s, 16s. Do NOT open a PR unless the user asks.)

---

## Self-Review (completed at authoring time)

- **Spec coverage:** room object {id,name,type,appInfo,hrInfo} → Tasks 1,6; sender {account,displayName} → Tasks 1,6; tshow → Tasks 1,2; attachments/card already present → no task (confirmed); local Mongo for user/app names → Task 4,6; room.batch for room names grouped by site → Tasks 5,6; bot sender = app name → Task 6 (`resolveSenderName`); graceful degradation → Task 6 tests; docs → Task 8.
- **Placeholder scan:** none — every code step carries real code.
- **Type consistency:** `MessageRoom`/`MessageSender` used identically across Tasks 1/6/7/8; `SubscriptionMeta`/`HRUser` defined in Task 3, consumed in Tasks 4/6; `RoomInfoClient.GetRoomsInfo` signature identical in Tasks 3/5/6/7; `h.room` field added in Task 7 (dependency noted in Task 6).
- **Name-collision check:** enrichment types avoid the existing `model.SearchRoom`; verify no existing `MessageRoom`/`MessageSender` before Task 1 (`grep -rn "MessageRoom\|MessageSender" pkg/model`).
