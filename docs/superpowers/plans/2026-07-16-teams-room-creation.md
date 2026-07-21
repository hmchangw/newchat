# teams-room-creation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `teams-room-creation`, a k8s-CronJob-triggered job that lists `teams_chat` docs flagged `needCreateRoom=true`, groups them by `siteId`, publishes each group in batches of N to the room-canonical NATS subject, and clears the flag per batch on JetStream ack.

**Architecture:** A run-to-completion `package main` service at the repo root, structurally a twin of `teams-chat-sync` (env config, `run() error`, `os.Exit(1)` only on total failure), but it publishes to NATS/JetStream instead of calling Graph. It reads via a secondary-preferred Mongo client and writes flag updates via a primary client. Grouping/batching/publishing is pure and unit-tested with an injected publish function; the real publish path is JetStream `PublishMsg` with a deterministic dedup id.

**Tech Stack:** Go 1.25, `caarlos0/env/v11`, `pkg/mongoutil`, `pkg/natsutil` + `nats.go/jetstream`, `pkg/obs`, `pkg/subject`, `pkg/model`, `stretchr/testify`, `go.uber.org/mock`, `testcontainers` via `pkg/testutil`.

## Global Constraints

- Language: Go 1.25; single root `go.mod`, module `github.com/hmchangw/chat`.
- No new third-party dependencies (`go.mod` unchanged).
- All commands via `make` targets — never raw `go`.
- Config only via env vars parsed with `caarlos0/env` into a typed `Config`; `SCREAMING_SNAKE_CASE`; `envDefault` for non-secrets, `required,notEmpty` for secrets/connection strings; never `os.Getenv`.
- All NATS payloads are JSON via `encoding/json` with typed `pkg/model` structs — never `map[string]interface{}`.
- Every NATS event struct in `pkg/model` carries `Timestamp int64 \`json:"timestamp" bson:"timestamp"\``, stamped at publish with `time.Now().UTC().UnixMilli()`.
- Subjects come from `pkg/subject` builders, never raw `fmt.Sprintf` in service code.
- Logging: `log/slog` JSON only; structured key-value fields; never log tokens/bodies.
- Errors: wrap with context `fmt.Errorf("short desc: %w", err)`; never bare `err`; never compare errors by string.
- Service layout: `main.go`, `config.go`, `store.go`, `store_mongo.go`, plus `publisher.go`, `runner.go`; tests in `package main`; generated mock in `mock_store_test.go` (never hand-edited).
- TDD Red-Green-Refactor for every task; commit after each task; ≥80% coverage floor, target 90%+ on `runner.go` and store.
- Integration tests tagged `//go:build integration`, use `pkg/testutil` containers, `TestMain` calls `testutil.RunTests(m)`.
- Committer identity: `git config user.email noreply@anthropic.com && git config user.name Claude` before committing. Do NOT put the model id in any commit message or pushed artifact.
- Subject op token is `teams.create` (plural). Member wire field is `visibleHistoryStartDateTime`.
- This subject is internal (`chat.room.canonical.…`), NOT a `chat.user.` RPC — no `docs/client-api.md` change.

---

### Task 1: Event model (`pkg/model/teamsroom.go`)

**Files:**
- Create: `pkg/model/teamsroom.go`
- Test: `pkg/model/model_test.go` (append round-trip test)

**Interfaces:**
- Produces: `model.TeamsRoomCreateEvent{ SiteID string; Chats []TeamsRoomCreateChat; Timestamp int64 }`, `model.TeamsRoomCreateChat{ ID, Name string; Members []TeamsRoomCreateMember; CreatedDateTime time.Time }`, `model.TeamsRoomCreateMember{ Account string; VisibleHistoryStartDateTime time.Time }`.

- [ ] **Step 1: Write the failing test** — append to `pkg/model/model_test.go`:

```go
func TestTeamsRoomCreateEventJSON(t *testing.T) {
	e := model.TeamsRoomCreateEvent{
		SiteID: "site-a",
		Chats: []model.TeamsRoomCreateChat{{
			ID:              "chat-1",
			Name:            "Project X",
			CreatedDateTime: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
			Members: []model.TeamsRoomCreateMember{{
				Account:                     "alice",
				VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
			}},
		}},
		Timestamp: 1_700_000_000_000,
	}
	roundTrip(t, &e, &model.TeamsRoomCreateEvent{})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model` (or `go test ./pkg/model/ -run TestTeamsRoomCreateEventJSON`)
Expected: FAIL — `undefined: model.TeamsRoomCreateEvent`.

- [ ] **Step 3: Write minimal implementation** — create `pkg/model/teamsroom.go`:

```go
package model

import "time"

// TeamsRoomCreateEvent is the batch envelope published by teams-room-creation
// to the room-canonical subject chat.room.canonical.{siteID}.teams.create.
// One event carries up to N chats that all share SiteID. Consumed by the room
// materialization worker (out of scope here).
type TeamsRoomCreateEvent struct {
	SiteID    string                `json:"siteId" bson:"siteId"`
	Chats     []TeamsRoomCreateChat `json:"chats" bson:"chats"`
	Timestamp int64                 `json:"timestamp" bson:"timestamp"` // publish time, UnixMilli UTC
}

// TeamsRoomCreateChat is one chat's room-creation input.
type TeamsRoomCreateChat struct {
	ID              string                  `json:"id" bson:"id"`
	Name            string                  `json:"name" bson:"name"`
	Members         []TeamsRoomCreateMember `json:"members" bson:"members"`
	CreatedDateTime time.Time               `json:"createdDateTime" bson:"createdDateTime"`
}

// TeamsRoomCreateMember is one member reference in a room-creation event: only
// the account and history-visibility cutoff are carried (the Graph member id is
// intentionally dropped).
type TeamsRoomCreateMember struct {
	Account                     string    `json:"account" bson:"account"`
	VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime" bson:"visibleHistoryStartDateTime"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/model`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/teamsroom.go pkg/model/model_test.go
git commit -m "feat(model): add TeamsRoomCreateEvent room-creation batch envelope"
```

---

### Task 2: Subject builder (`pkg/subject`)

**Files:**
- Modify: `pkg/subject/subject.go` (add builder near `RoomCanonical`, ~line 137)
- Test: `pkg/subject/subject_test.go` (add case to the builders table)

**Interfaces:**
- Produces: `subject.RoomCanonicalTeamsCreate(siteID string) string` → `chat.room.canonical.{siteID}.teams.create`.

- [ ] **Step 1: Write the failing test** — add a row to the existing string-builder table test in `pkg/subject/subject_test.go` (the table that holds the `RoomCanonical` case near line 33):

```go
{"RoomCanonicalTeamsCreate", subject.RoomCanonicalTeamsCreate("site-a"),
	"chat.room.canonical.site-a.teams.create"},
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/subject/ -run TestSubjects` (use the actual test func name containing that table; grep if unsure)
Expected: FAIL — `undefined: subject.RoomCanonicalTeamsCreate`.

- [ ] **Step 3: Write minimal implementation** — add to `pkg/subject/subject.go` immediately after `RoomCanonical` (line ~138):

```go
// RoomCanonicalTeamsCreate returns the room-canonical subject for a batch of
// Teams-derived room-creation events for one site. Lands in ROOMS_{siteID}.
func RoomCanonicalTeamsCreate(siteID string) string {
	return fmt.Sprintf("chat.room.canonical.%s.teams.create", siteID)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/subject/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add RoomCanonicalTeamsCreate builder"
```

---

### Task 3: Config (`teams-room-creation/config.go`)

**Files:**
- Create: `teams-room-creation/config.go`
- Test: `teams-room-creation/config_test.go`

**Interfaces:**
- Produces: `Config` struct; `validateConfig(cfg Config) error`.

- [ ] **Step 1: Write the failing test** — create `teams-room-creation/config_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseConfig() Config {
	return Config{BatchSize: 100, MaxWorkers: 8}
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=teams-room-creation`
Expected: FAIL — `undefined: Config` / `validateConfig`.

- [ ] **Step 3: Write minimal implementation** — create `teams-room-creation/config.go`:

```go
package main

import (
	"fmt"
	"time"
)

// Config is the job's environment configuration. One replica-set serves both
// lanes: the teams_chat scan reads through a secondary-preferred client and the
// needCreateRoom flag update writes through a primary client, so they share one
// URI, DB and credential pair — only the read preference differs.
type Config struct {
	MongoURI      string `env:"MONGO_URI,required,notEmpty"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	NatsURL       string `env:"NATS_URL,required,notEmpty"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	// BatchSize is the maximum number of chats packed into one room-canonical
	// event. Each site's flagged chats are chunked into batches of this size.
	BatchSize int `env:"ROOM_CREATE_BATCH_SIZE" envDefault:"100"`
	// MaxWorkers bounds concurrent batch publishes across all site groups.
	MaxWorkers int `env:"MAX_WORKERS" envDefault:"8"`
	// RunTimeout is the whole-run deadline.
	RunTimeout time.Duration `env:"RUN_TIMEOUT" envDefault:"30m"`
}

// validateConfig checks the parsed Config for internal consistency. It isolates
// run()'s pure precondition checks so they are unit testable without wiring any
// real dependency.
//
//nolint:gocritic // hugeParam: cfg is passed by value once at startup; not a hot path
func validateConfig(cfg Config) error {
	if cfg.BatchSize <= 0 {
		return fmt.Errorf("invalid config: ROOM_CREATE_BATCH_SIZE must be positive")
	}
	if cfg.MaxWorkers <= 0 {
		return fmt.Errorf("invalid config: MAX_WORKERS must be positive")
	}
	if cfg.RunTimeout <= 0 {
		return fmt.Errorf("invalid config: RUN_TIMEOUT must be positive")
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=teams-room-creation`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add teams-room-creation/config.go teams-room-creation/config_test.go
git commit -m "feat(teams-room-creation): config and validation"
```

---

### Task 4: Store interface + Mongo implementation

**Files:**
- Create: `teams-room-creation/store.go`
- Create: `teams-room-creation/store_mongo.go`
- Create: `teams-room-creation/store_mongo_test.go` (integration)
- Generate: `teams-room-creation/mock_store_test.go`

**Interfaces:**
- Produces: `TeamsChatStore` interface { `ListChatsNeedingRoom(ctx) ([]model.TeamsChat, error)`; `MarkRoomsCreated(ctx, ids []string) error` }; `newMongoStore(readDB, writeDB *mongo.Database) *mongoStore`.

- [ ] **Step 1: Write the store interface** — create `teams-room-creation/store.go`:

```go
package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// TeamsChatStore reads chats flagged for room creation and clears the flag
// once their room-creation event is durably published. Satisfied by
// *mongoStore, whose reads and writes go to separate clients (secondary-read,
// primary-write).
type TeamsChatStore interface {
	// ListChatsNeedingRoom returns every teams_chat with needCreateRoom=true,
	// projected to the fields the event needs (_id, name, members,
	// createdDateTime, siteId).
	ListChatsNeedingRoom(ctx context.Context) ([]model.TeamsChat, error)
	// MarkRoomsCreated clears needCreateRoom for the given chat ids.
	MarkRoomsCreated(ctx context.Context, ids []string) error
}
```

- [ ] **Step 2: Generate the mock**

Run: `make generate SERVICE=teams-room-creation`
Expected: creates `teams-room-creation/mock_store_test.go` with `MockTeamsChatStore`.

- [ ] **Step 3: Write the failing store integration test** — create `teams-room-creation/store_mongo_test.go`:

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

func TestMain(m *testing.M) { testutil.RunTests(m) }

func seedChat(t *testing.T, db interface{ /* placeholder */ }) {}

func TestMongoStore_ListAndMark(t *testing.T) {
	db := testutil.MongoDB(t, "teamsroom")
	col := db.Collection("teams_chat")
	ctx := context.Background()

	_, err := col.InsertMany(ctx, []any{
		bson.M{"_id": "c1", "name": "A", "siteId": "site-a", "needCreateRoom": true,
			"members": []bson.M{{"account": "alice", "visibleHistoryStartDateTime": nil}}},
		bson.M{"_id": "c2", "name": "B", "siteId": "site-b", "needCreateRoom": true, "members": []bson.M{}},
		bson.M{"_id": "c3", "name": "C", "siteId": "site-a", "needCreateRoom": false, "members": []bson.M{}},
	})
	require.NoError(t, err)

	store := newMongoStore(db, db)

	got, err := store.ListChatsNeedingRoom(ctx)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, c := range got {
		ids[c.ID] = true
	}
	assert.Len(t, got, 2)
	assert.True(t, ids["c1"] && ids["c2"])
	assert.False(t, ids["c3"], "needCreateRoom=false must be excluded")

	require.NoError(t, store.MarkRoomsCreated(ctx, []string{"c1"}))
	after, err := store.ListChatsNeedingRoom(ctx)
	require.NoError(t, err)
	assert.Len(t, after, 1)
	assert.Equal(t, "c2", after[0].ID)
}
```

Delete the `seedChat` placeholder line before running — it is a scratch artifact; the real seeding is inline via `InsertMany`.

- [ ] **Step 4: Run test to verify it fails**

Run: `make test-integration SERVICE=teams-room-creation`
Expected: FAIL — `undefined: newMongoStore`.

- [ ] **Step 5: Write minimal implementation** — create `teams-room-creation/store_mongo.go`:

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
// writeChats (the needCreateRoom flag clear, a primary client).
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

// ListChatsNeedingRoom returns every teams_chat with needCreateRoom=true,
// projected to exactly the fields the event needs. Served by the read client.
func (s *mongoStore) ListChatsNeedingRoom(ctx context.Context) ([]model.TeamsChat, error) {
	chats, err := s.readChats.FindMany(ctx, bson.M{"needCreateRoom": true}, mongoutil.WithProjection(bson.M{
		"_id": 1, "name": 1, "members": 1, "createdDateTime": 1, "siteId": 1,
	}))
	if err != nil {
		return nil, fmt.Errorf("list chats needing room: %w", err)
	}
	return chats, nil
}

// MarkRoomsCreated clears needCreateRoom for the given ids. Written by the
// primary client. A nil/empty id slice is a no-op.
func (s *mongoStore) MarkRoomsCreated(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.writeChats.Raw().UpdateMany(ctx,
		bson.M{"_id": bson.M{"$in": ids}},
		bson.M{"$set": bson.M{"needCreateRoom": false}})
	if err != nil {
		return fmt.Errorf("mark rooms created: %w", err)
	}
	return nil
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `make test-integration SERVICE=teams-room-creation`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add teams-room-creation/store.go teams-room-creation/store_mongo.go \
        teams-room-creation/store_mongo_test.go teams-room-creation/mock_store_test.go
git commit -m "feat(teams-room-creation): store interface and Mongo read/write impl"
```

---

### Task 5: Publisher (`teams-room-creation/publisher.go`)

**Files:**
- Create: `teams-room-creation/publisher.go`
- Test: `teams-room-creation/publisher_test.go`

**Interfaces:**
- Produces: `type publishFunc func(ctx context.Context, subj string, data []byte, dedupID string) error`; `dedupID(siteID string, chatIDs []string) string`; `newJetStreamPublisher(js jetstream.JetStream) publishFunc`.
- Consumes: `subject.RoomCanonicalTeamsCreate` (Task 2) is used by the runner, not here.

- [ ] **Step 1: Write the failing test** — create `teams-room-creation/publisher_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDedupID_DeterministicAndOrderInsensitive(t *testing.T) {
	a := dedupID("site-a", []string{"c1", "c2", "c3"})
	b := dedupID("site-a", []string{"c3", "c1", "c2"}) // different order, same set
	assert.Equal(t, a, b, "dedup id must not depend on chat-id order")
	assert.Contains(t, a, "teamroom:site-a:")
}

func TestDedupID_DistinctSets(t *testing.T) {
	assert.NotEqual(t, dedupID("site-a", []string{"c1"}), dedupID("site-a", []string{"c2"}))
	assert.NotEqual(t, dedupID("site-a", []string{"c1"}), dedupID("site-b", []string{"c1"}))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=teams-room-creation`
Expected: FAIL — `undefined: dedupID`.

- [ ] **Step 3: Write minimal implementation** — create `teams-room-creation/publisher.go`:

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// publishFunc publishes one room-creation batch to subj with a JetStream
// dedup id. Injected into the runner so unit tests capture batches without a
// real NATS connection.
type publishFunc func(ctx context.Context, subj string, data []byte, dedupID string) error

// dedupID is a deterministic Nats-Msg-Id for a batch: identical (site, chat-id
// set) always yields the same id regardless of chat order, so a re-published
// un-flipped batch is deduplicated server-side.
func dedupID(siteID string, chatIDs []string) string {
	sorted := append([]string(nil), chatIDs...)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, ",")))
	return fmt.Sprintf("teamroom:%s:%s", siteID, hex.EncodeToString(sum[:]))
}

// newJetStreamPublisher returns a publishFunc that publishes via JetStream and
// blocks on the PubAck, honoring dedupID as Nats-Msg-Id.
func newJetStreamPublisher(js jetstream.JetStream) publishFunc {
	return func(ctx context.Context, subj string, data []byte, dedup string) error {
		msg := natsutil.NewMsg(ctx, subj, data)
		if _, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(dedup)); err != nil {
			return fmt.Errorf("publish to %q: %w", subj, err)
		}
		return nil
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=teams-room-creation`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add teams-room-creation/publisher.go teams-room-creation/publisher_test.go
git commit -m "feat(teams-room-creation): JetStream publisher with deterministic dedup id"
```

---

### Task 6: Runner (`teams-room-creation/runner.go`)

**Files:**
- Create: `teams-room-creation/runner.go`
- Test: `teams-room-creation/runner_test.go`

**Interfaces:**
- Consumes: `TeamsChatStore` (Task 4), `publishFunc` + `dedupID` (Task 5), `subject.RoomCanonicalTeamsCreate` (Task 2), `model.TeamsChat` / `model.TeamsRoomCreateEvent` (Task 1).
- Produces: `type runConfig struct { BatchSize, MaxWorkers int; Now func() time.Time }`; `newRunner(store TeamsChatStore, publish publishFunc, cfg runConfig) *runner`; `(*runner).run(ctx) error`; helpers `groupBySite`, `chunk`, `buildEvent`.

- [ ] **Step 1: Write the failing tests** — create `teams-room-creation/runner_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

// captured records one publish call.
type captured struct {
	subj  string
	dedup string
	evt   model.TeamsRoomCreateEvent
}

// recorder is a thread-safe publishFunc that decodes and stores each batch.
func recorder(mu *sync.Mutex, out *[]captured, fail map[string]bool) publishFunc {
	return func(_ context.Context, subj string, data []byte, dedup string) error {
		var e model.TeamsRoomCreateEvent
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		if fail[e.SiteID] {
			return errors.New("boom")
		}
		mu.Lock()
		defer mu.Unlock()
		*out = append(*out, captured{subj: subj, dedup: dedup, evt: e})
		return nil
	}
}

func chat(id, site string) model.TeamsChat {
	return model.TeamsChat{
		ID: id, Name: "n-" + id, SiteID: site,
		CreatedDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		Members: []model.TeamsChatMember{{
			ID: "m-" + id, Account: "acct-" + id,
			VisibleHistoryStartDateTime: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		}},
	}
}

func TestRunner_GroupsBatchesAndFlipsOnAck(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	chats := []model.TeamsChat{
		chat("a1", "site-a"), chat("a2", "site-a"), chat("a3", "site-a"),
		chat("b1", "site-b"),
	}
	store.EXPECT().ListChatsNeedingRoom(gomock.Any()).Return(chats, nil)

	var markMu sync.Mutex
	marked := map[string]bool{}
	store.EXPECT().MarkRoomsCreated(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) error {
			markMu.Lock()
			defer markMu.Unlock()
			for _, id := range ids {
				marked[id] = true
			}
			return nil
		}).AnyTimes()

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, nil), runConfig{
		BatchSize: 2, MaxWorkers: 4, Now: func() time.Time { return time.UnixMilli(1700) },
	})
	require.NoError(t, r.run(context.Background()))

	// site-a (3 chats, batch 2) -> 2 batches; site-b -> 1 batch. Total 3.
	assert.Len(t, got, 3)
	for _, c := range got {
		assert.Equal(t, int64(1700), c.evt.Timestamp)
		assert.Equal(t, "chat.room.canonical."+c.evt.SiteID+".teams.create", c.subj)
		assert.LessOrEqual(t, len(c.evt.Chats), 2)
		for _, ch := range c.evt.Chats {
			assert.Equal(t, "acct-"+ch.ID, ch.Members[0].Account)
			assert.Equal(t, "n-"+ch.ID, ch.Name)
		}
	}
	assert.True(t, marked["a1"] && marked["a2"] && marked["a3"] && marked["b1"])
}

func TestRunner_FailedBatchNotFlipped(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingRoom(gomock.Any()).Return(
		[]model.TeamsChat{chat("a1", "site-a"), chat("b1", "site-b")}, nil)
	// Only site-a's chats may be flipped; site-b publish fails.
	store.EXPECT().MarkRoomsCreated(gomock.Any(), []string{"a1"}).Return(nil)

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, map[string]bool{"site-b": true}), runConfig{
		BatchSize: 10, MaxWorkers: 2, Now: time.Now,
	})
	require.NoError(t, r.run(context.Background()))
	assert.Len(t, got, 1)
	assert.Equal(t, "site-a", got[0].evt.SiteID)
}

func TestRunner_EmptyListNoPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingRoom(gomock.Any()).Return(nil, nil)

	var mu sync.Mutex
	var got []captured
	r := newRunner(store, recorder(&mu, &got, nil), runConfig{BatchSize: 5, MaxWorkers: 2, Now: time.Now})
	require.NoError(t, r.run(context.Background()))
	assert.Empty(t, got)
}

func TestRunner_ListErrorReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingRoom(gomock.Any()).Return(nil, errors.New("db down"))

	r := newRunner(store, recorder(new(sync.Mutex), &[]captured{}, nil), runConfig{BatchSize: 5, MaxWorkers: 2, Now: time.Now})
	require.Error(t, r.run(context.Background()))
}

func TestBuildEvent_MapsMembersDropsID(t *testing.T) {
	e := buildEvent("site-a", []model.TeamsChat{chat("a1", "site-a")}, time.UnixMilli(42))
	require.Len(t, e.Chats, 1)
	require.Len(t, e.Chats[0].Members, 1)
	assert.Equal(t, "acct-a1", e.Chats[0].Members[0].Account)
	assert.Equal(t, int64(42), e.Timestamp)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=teams-room-creation`
Expected: FAIL — `undefined: newRunner` / `buildEvent`.

- [ ] **Step 3: Write minimal implementation** — create `teams-room-creation/runner.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// runConfig holds the runner's pure knobs. Now is injected so tests control the
// event Timestamp.
type runConfig struct {
	BatchSize  int
	MaxWorkers int
	Now        func() time.Time
}

// runner performs one room-creation pass: list flagged chats, group by site,
// chunk into batches, publish each batch, and clear the flag for batches that
// were acknowledged.
type runner struct {
	store   TeamsChatStore
	publish publishFunc
	cfg     runConfig
}

func newRunner(store TeamsChatStore, publish publishFunc, cfg runConfig) *runner {
	return &runner{store: store, publish: publish, cfg: cfg}
}

// batch is one site's worth of up to BatchSize chats.
type batch struct {
	siteID string
	chats  []model.TeamsChat
}

// run executes one pass. It returns an error only when the initial list fails;
// per-batch publish failures are logged and leave those chats flagged for the
// next CronJob run.
func (r *runner) run(ctx context.Context) error {
	chats, err := r.store.ListChatsNeedingRoom(ctx)
	if err != nil {
		return fmt.Errorf("list chats needing room: %w", err)
	}
	if len(chats) == 0 {
		slog.InfoContext(ctx, "no chats need room creation")
		return nil
	}
	batches := planBatches(chats, r.cfg.BatchSize)
	slog.InfoContext(ctx, "publishing room-creation batches",
		"chats", len(chats), "batches", len(batches))

	sem := make(chan struct{}, r.cfg.MaxWorkers)
	var wg sync.WaitGroup
	for _, b := range batches {
		wg.Add(1)
		sem <- struct{}{}
		go func(b batch) {
			defer wg.Done()
			defer func() { <-sem }()
			r.publishBatch(ctx, b)
		}(b)
	}
	wg.Wait()
	return nil
}

// publishBatch marshals and publishes one batch, then clears the flag for its
// chats iff the publish was acknowledged.
func (r *runner) publishBatch(ctx context.Context, b batch) {
	evt := buildEvent(b.siteID, b.chats, r.cfg.Now())
	data, err := json.Marshal(evt)
	if err != nil {
		slog.ErrorContext(ctx, "marshal room-creation event", "site_id", b.siteID, "error", err)
		return
	}
	ids := chatIDs(b.chats)
	subj := subject.RoomCanonicalTeamsCreate(b.siteID)
	if err := r.publish(ctx, subj, data, dedupID(b.siteID, ids)); err != nil {
		slog.WarnContext(ctx, "publish room-creation batch failed; will retry next run",
			"site_id", b.siteID, "chats", len(ids), "error", err)
		return
	}
	if err := r.store.MarkRoomsCreated(ctx, ids); err != nil {
		slog.WarnContext(ctx, "mark rooms created failed; batch republishes next run (dedup absorbs it)",
			"site_id", b.siteID, "chats", len(ids), "error", err)
	}
}

// planBatches groups chats by siteID (deterministic: sites and chats keep
// input order) and chunks each group into batches of at most size.
func planBatches(chats []model.TeamsChat, size int) []batch {
	order := make([]string, 0)
	bySite := make(map[string][]model.TeamsChat)
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

// buildEvent maps a batch of teams_chat docs into the wire event, dropping each
// member's Graph id and stamping the publish timestamp.
func buildEvent(siteID string, chats []model.TeamsChat, now time.Time) model.TeamsRoomCreateEvent {
	out := make([]model.TeamsRoomCreateChat, 0, len(chats))
	for _, c := range chats {
		members := make([]model.TeamsRoomCreateMember, 0, len(c.Members))
		for _, m := range c.Members {
			members = append(members, model.TeamsRoomCreateMember{
				Account:                     m.Account,
				VisibleHistoryStartDateTime: m.VisibleHistoryStartDateTime,
			})
		}
		out = append(out, model.TeamsRoomCreateChat{
			ID:              c.ID,
			Name:            c.Name,
			Members:         members,
			CreatedDateTime: c.CreatedDateTime,
		})
	}
	return model.TeamsRoomCreateEvent{
		SiteID:    siteID,
		Chats:     out,
		Timestamp: now.UTC().UnixMilli(),
	}
}

// chatIDs extracts the chat ids of a batch, preserving order.
func chatIDs(chats []model.TeamsChat) []string {
	ids := make([]string, 0, len(chats))
	for _, c := range chats {
		ids = append(ids, c.ID)
	}
	return ids
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=teams-room-creation`
Expected: PASS (all runner tests).

- [ ] **Step 5: Commit**

```bash
git add teams-room-creation/runner.go teams-room-creation/runner_test.go
git commit -m "feat(teams-room-creation): batch grouping, publish, and per-batch flag flip"
```

---

### Task 7: main wiring (`teams-room-creation/main.go`)

**Files:**
- Create: `teams-room-creation/main.go`
- Test: `teams-room-creation/main_test.go`

**Interfaces:**
- Consumes: `Config`/`validateConfig` (Task 3), `newMongoStore` (Task 4), `newJetStreamPublisher` (Task 5), `newRunner`/`runConfig` (Task 6).
- Produces: `main()`, `run() error`.

- [ ] **Step 1: Write the failing test** — create `teams-room-creation/main_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// run() must fail fast when required config is absent (no MONGO_URI/NATS_URL).
func TestRun_MissingConfigFailsFast(t *testing.T) {
	t.Setenv("MONGO_URI", "")
	t.Setenv("NATS_URL", "")
	require.Error(t, run())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=teams-room-creation`
Expected: FAIL — `undefined: run`.

- [ ] **Step 3: Write minimal implementation** — create `teams-room-creation/main.go`:

```go
// Command teams-room-creation is a run-to-completion job (k8s CronJob) that
// turns Teams chats flagged needCreateRoom=true into room-canonical NATS
// events. It lists every such teams_chat, groups them by siteId, publishes each
// group in batches to chat.room.canonical.{siteId}.teams.create, and clears the
// flag for each batch that JetStream acknowledges. One global instance serves
// the whole federation.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
)

func main() {
	if err := run(); err != nil {
		slog.Error("teams-room-creation failed", "error", err)
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

	ctx, cancel := context.WithTimeout(context.Background(), cfg.RunTimeout)
	defer cancel()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}
	defer func() {
		if err := obsShutdown(context.Background()); err != nil {
			slog.Error("observability shutdown", "error", err)
		}
	}()

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

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator)
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("jetstream init: %w", err)
	}

	store := newMongoStore(readClient.Database(cfg.MongoDB), writeClient.Database(cfg.MongoDB))
	r := newRunner(store, newJetStreamPublisher(js), runConfig{
		BatchSize:  cfg.BatchSize,
		MaxWorkers: cfg.MaxWorkers,
		Now:        time.Now,
	})
	if err := r.run(ctx); err != nil {
		return fmt.Errorf("run: %w", err)
	}
	slog.Info("teams-room-creation done")
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=teams-room-creation`
Expected: PASS. (Config parse rejects the empty required `MONGO_URI`.)

Note: confirm `nc.JetStream()` matches the return type used elsewhere (`outbox-worker/main.go` uses `nc.JetStream()` on the `*o11ynats.Conn`). If the wrapper exposes it differently, mirror `outbox-worker/main.go` exactly.

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: clean (fix any `errcheck` on `nc.Drain()` by assigning `_ = nc.Drain()` if the linter requires it — mirror how `outbox-worker`/other services handle Drain).

- [ ] **Step 6: Commit**

```bash
git add teams-room-creation/main.go teams-room-creation/main_test.go
git commit -m "feat(teams-room-creation): main wiring (mongo, nats, runner)"
```

---

### Task 8: End-to-end integration test + deploy files

**Files:**
- Create: `teams-room-creation/integration_test.go`
- Create: `teams-room-creation/deploy/Dockerfile`
- Create: `teams-room-creation/deploy/docker-compose.yml`
- Create: `teams-room-creation/deploy/azure-pipelines.yml`

**Interfaces:**
- Consumes: everything above; `pkg/testutil.MongoDB`, `pkg/testutil.NATS`, `pkg/stream.Rooms`, `pkg/subject.RoomCanonicalTeamsCreate`.

- [ ] **Step 1: Write the failing e2e test** — create `teams-room-creation/integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestEndToEnd_PublishesAndClearsFlag(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "teamsroom-e2e")
	_, err := db.Collection("teams_chat").InsertMany(ctx, []any{
		bson.M{"_id": "c1", "name": "A", "siteId": "site-a", "needCreateRoom": true,
			"members": []bson.M{{"account": "alice"}}},
		bson.M{"_id": "c2", "name": "B", "siteId": "site-a", "needCreateRoom": true, "members": []bson.M{}},
	})
	require.NoError(t, err)

	natsURL := testutil.NATS(t)
	nc, err := jetstreamTestConn(t, natsURL)
	require.NoError(t, err)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	// Create the ROOMS stream so the publish lands (dev-only; ops owns it in prod).
	rc := stream.Rooms("site-a")
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: rc.Name, Subjects: rc.Subjects})
	require.NoError(t, err)

	store := newMongoStore(db, db)
	r := newRunner(store, newJetStreamPublisher(js), runConfig{BatchSize: 10, MaxWorkers: 2, Now: time.Now})
	require.NoError(t, r.run(ctx))

	// One event with both chats landed on the subject.
	cons, err := js.CreateOrUpdateConsumer(ctx, rc.Name, jetstream.ConsumerConfig{
		FilterSubject: subject.RoomCanonicalTeamsCreate("site-a"),
	})
	require.NoError(t, err)
	msg, err := cons.Next(jetstream.FetchMaxWait(3 * time.Second))
	require.NoError(t, err)
	var evt model.TeamsRoomCreateEvent
	require.NoError(t, json.Unmarshal(msg.Data(), &evt))
	assert.Equal(t, "site-a", evt.SiteID)
	assert.Len(t, evt.Chats, 2)

	// Flags cleared.
	remaining, err := store.ListChatsNeedingRoom(ctx)
	require.NoError(t, err)
	assert.Empty(t, remaining)
}
```

Add a small helper in the same file for the raw NATS connection (mirror the pattern other integration tests use — a plain `nats.Connect` with a `t.Cleanup(nc.Close)`), named `jetstreamTestConn(t, url) (*nats.Conn, error)`. If an existing `testutil` helper already returns a `*nats.Conn`, use that instead and delete this helper. The `TestMain` is already defined in `store_mongo_test.go` (Task 4) — do not redefine it.

- [ ] **Step 2: Run test to verify it fails, then passes**

Run: `make test-integration SERVICE=teams-room-creation`
Expected: initially FAIL if helper undefined; after adding the connection helper, PASS.

- [ ] **Step 3: Create `teams-room-creation/deploy/Dockerfile`:**

```dockerfile
FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY teams-room-creation/ teams-room-creation/
RUN CGO_ENABLED=0 go build -o /teams-room-creation ./teams-room-creation/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /teams-room-creation /teams-room-creation
USER app
ENTRYPOINT ["/teams-room-creation"]
```

- [ ] **Step 4: Create `teams-room-creation/deploy/docker-compose.yml`:**

```yaml
name: teams-room-creation

services:
  teams-room-creation:
    build:
      context: ../..
      dockerfile: teams-room-creation/deploy/Dockerfile
    environment:
      - MONGO_URI=${MONGO_URI:-mongodb://mongo:27017}
      - MONGO_DB=${MONGO_DB:-chat}
      - NATS_URL=${NATS_URL:-nats://nats:4222}
      - NATS_CREDS_FILE=${NATS_CREDS_FILE:-}
      - ROOM_CREATE_BATCH_SIZE=${ROOM_CREATE_BATCH_SIZE:-100}
      - MAX_WORKERS=${MAX_WORKERS:-8}
      - RUN_TIMEOUT=${RUN_TIMEOUT:-30m}
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 5: Create `teams-room-creation/deploy/azure-pipelines.yml`** — copy `teams-chat-sync/deploy/azure-pipelines.yml` verbatim, then replace every `teams-chat-sync` occurrence with `teams-room-creation` (the `paths`, `SERVICE_DIR`, `IMAGE_NAME`, `REGISTRY` block, and any build/test script paths). Verify with:

```bash
grep -n "teams-chat-sync" teams-room-creation/deploy/azure-pipelines.yml   # expect: no matches
```

- [ ] **Step 6: Full verification**

Run:
```bash
make lint
make test SERVICE=teams-room-creation
make test-integration SERVICE=teams-room-creation
make sast
```
Expected: all green; SAST no medium+ findings.

- [ ] **Step 7: Commit**

```bash
git add teams-room-creation/integration_test.go teams-room-creation/deploy/
git commit -m "feat(teams-room-creation): end-to-end integration test and deploy files"
```

---

## Final verification (after all tasks)

- [ ] `make lint` clean
- [ ] `make test SERVICE=teams-room-creation` green
- [ ] `make test SERVICE=pkg/model` and `go test ./pkg/subject/` green
- [ ] `make test-integration SERVICE=teams-room-creation` green
- [ ] `make sast` no medium+ findings
- [ ] Coverage: `go test -coverprofile=coverage.out ./teams-room-creation/... && go tool cover -func=coverage.out` ≥ 80% (target 90%+ on `runner.go`)
- [ ] Delete this plan and the spec from `docs/superpowers/` only if repo policy requires a clean branch before PR (the `docs/reviews/` deletion rule is separate and does not apply here). Otherwise leave them.
- [ ] Push to `claude/teams-chat-sync-service-9ispb0` with `git push -u origin claude/teams-chat-sync-service-9ispb0`.

## Self-review notes

- **Spec coverage:** §1 purpose → Tasks 6/7; §2 structure → all tasks; §3 event model → Task 1; §4 subject/stream/dedup → Tasks 2 & 5; §5 store → Task 4; §6 config → Task 3; §7 flow/error handling → Task 6 (`run`/`publishBatch`); §8 testing → Tasks 1,4,6,8; §9 out-of-scope respected (no room-worker changes, no client-api.md); §10 naming → Tasks 2 & 1.
- **Type consistency:** `TeamsChatStore`, `publishFunc`, `dedupID`, `newRunner`, `runConfig`, `buildEvent`, `planBatches`, `newMongoStore`, `newJetStreamPublisher` used identically across tasks.
- **No new deps:** all imports are already in `go.mod` (`caarlos0/env/v11`, `nats.go/jetstream`, mongo driver, testify, mock, testutil).
