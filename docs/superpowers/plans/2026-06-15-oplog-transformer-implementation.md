# oplog-transformer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Follow the Red-Green-Refactor TDD cycle in CLAUDE.md §4 for every task — write the failing test first, confirm it fails, then implement.

**Goal:** Build `data-migration/oplog-transformer` — a JetStream consumer that reads `MIGRATION_OPLOG_{site}`, formats migrated RocketChat **messages**, and re-injects them into the new-stack pipeline (insert → canonical `.created`; update/soft-delete → history-service migration handlers), all tagged `X-Migration: live` so broadcast/notification suppress live delivery.

**Architecture:** One durable, **sequential** consumer (single active replica). Route by collection (`rocketchat_message` only this stage). Insert: map the full doc → `model.MessageEvent` → **confirmed** publish to `chat.msg.canonical.{site}.created`. Update/replace: resolve the full doc (lookup for `update` via `primaryPreferred`, event-doc for `replace`), classify edit vs soft-delete, **sync request/reply** to `chat.migration.internal.{site}.msg.{edit,delete}`; history-service upserts Cassandra + best-effort canonical republish. Ack the oplog message only after success.

**Tech Stack:** Go 1.25, `nats.go` + `nats.go/jetstream` via `Marz32onE/.../oteljetstream`, `mongo-driver/v2` (source lookups, `bson.UnmarshalExtJSON` for relaxed extJSON), `caarlos0/env`, `log/slog`, `stretchr/testify`, `go.uber.org/mock`, `testcontainers-go` via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-06-15-oplog-transformer-design.md` — read it fully first.

---

## File map

| File | Responsibility |
|---|---|
| `pkg/natsutil/migration.go` | `X-Migration` header constant + set/get helpers (shared by all 4 services) |
| `pkg/subject/subject.go` (+ `oplog_test.go`) | `MigrationInternalMsgEdit/Delete` builders |
| `pkg/model/migration.go` (+ model_test) | `MigrationEditRequest`/`MigrationDeleteRequest`/`MigrationAck` wire types |
| `data-migration/oplog-transformer/config.go` | typed env config |
| `…/messagemap.go` | RocketChat doc (relaxed extJSON) → `model.Message` + edit/soft-delete classification |
| `…/sourcelookup.go` | `sourceLookup` interface + Mongo impl (`primaryPreferred`) |
| `…/canonical.go` | build `MessageEvent` + confirmed canonical `.created` publish (header set) |
| `…/historyclient.go` | `historyClient` interface + NATS request/reply impl |
| `…/handler.go` | route-by-collection + op dispatch + ack/nak/term |
| `…/main.go` | wiring, durable consumer, consume loop, graceful shutdown |
| `…/metrics.go` | Prometheus instruments + `/metrics` + `/healthz` |
| `…/{config,messagemap,canonical,handler,metrics}_test.go`, `integration_test.go`, `mock_*_test.go` | tests |
| `…/deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}` | deploy |
| `history-service/internal/publisher/publisher.go` | add header-aware publish |
| `history-service/internal/service/migration.go` (+ test) | `MigrationEditMessage`/`MigrationDeleteMessage` + route registration |
| `broadcast-worker/main.go` | skip `X-Migration: live` in the consume wrapper |
| `notification-worker/main.go` | skip `X-Migration: live` in the consume wrapper |

---

## Phase 0 — Pre-flight verification (do FIRST, blocks the delete path)

### Task 0: Confirm source deletion mode + message field set

- [ ] **Step 1: Inspect the source for deletion semantics.** Against a source Mongo (or a sample dump), check how a deleted message appears:

```bash
# In a mongosh shell against the source (read-only):
# 1) Does deleting a message leave a tombstoned doc (soft delete) or remove it (hard)?
db.rocketchat_message.findOne({ t: "rm" })          # RocketChat "message removed" system marker
db.rocketchat_message.findOne({ _hidden: true })    # alt soft-delete flag
# 2) Field census for the mapper:
db.rocketchat_message.findOne({}, { _id:1, rid:1, msg:1, ts:1, editedAt:1, editedBy:1, u:1, tmid:1, t:1 })
```

- [ ] **Step 2: Record the result in the spec.** Edit `docs/superpowers/specs/2026-06-15-oplog-transformer-design.md` §4.2: replace the "pin during impl" note with the confirmed deletion marker (e.g. `t == "rm"`, or `_hidden == true`). **If the source HARD-deletes**, STOP and escalate — the delete path needs a locator index (out of this plan's scope).

- [ ] **Step 3: Commit.**

```bash
git add docs/superpowers/specs/2026-06-15-oplog-transformer-design.md
git commit -m "docs(oplog-transformer): confirm source deletion mode + message field set"
```

> The mapper code below assumes the common RocketChat shape (`_id`, `rid`, `msg`, `ts`, `editedAt`, `u.{_id,username,name}`, `tmid`, `t`) and soft-delete via `t == "rm"`. Adjust `messagemap.go` constants to the confirmed marker if different.

---

## Phase 1 — Shared primitives

### Task 1: `X-Migration` header helper (`pkg/natsutil`)

**Files:** Create `pkg/natsutil/migration.go`; Test `pkg/natsutil/migration_test.go`.

- [ ] **Step 1: Write the failing test.**

```go
package natsutil

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestMigrationHeader(t *testing.T) {
	msg := &nats.Msg{Subject: "x", Header: nats.Header{}}
	assert.False(t, IsMigrationLive(msg))
	SetMigrationLive(msg)
	assert.Equal(t, "live", msg.Header.Get(HeaderMigration))
	assert.True(t, IsMigrationLive(msg))
}

func TestIsMigrationLive_NilHeader(t *testing.T) {
	assert.False(t, IsMigrationLive(&nats.Msg{Subject: "x"}))
}
```

- [ ] **Step 2: Run — expect FAIL** (`SetMigrationLive` undefined): `go test ./pkg/natsutil/ -run TestMigration -v`

- [ ] **Step 3: Implement.**

```go
package natsutil

import "github.com/nats-io/nats.go"

// HeaderMigration marks an event as produced by the data migration. Live-delivery
// consumers (broadcast, notification) skip it; persistence/index consumers ignore it.
const HeaderMigration = "X-Migration"

// MigrationLive is the only value: "persist & index, but do not re-deliver — the
// source system already delivered this message to users."
const MigrationLive = "live"

// SetMigrationLive stamps msg as a migrated event. msg.Header must be non-nil.
func SetMigrationLive(msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	msg.Header.Set(HeaderMigration, MigrationLive)
}

// IsMigrationLive reports whether msg carries X-Migration: live.
func IsMigrationLive(msg *nats.Msg) bool {
	return msg != nil && msg.Header.Get(HeaderMigration) == MigrationLive
}
```

- [ ] **Step 4: Run — expect PASS.** `go test ./pkg/natsutil/ -run TestMigration -v`

- [ ] **Step 5: Commit.** `git add pkg/natsutil/migration*.go && git commit -m "feat(natsutil): X-Migration header helper"`

### Task 2: Internal migration subject builders (`pkg/subject`)

**Files:** Modify `pkg/subject/subject.go` (after `MsgCanonicalDeleted`, ~line 190); Test `pkg/subject/oplog_test.go`.

- [ ] **Step 1: Add the failing test** to `pkg/subject/oplog_test.go`:

```go
func TestMigrationInternalSubjects(t *testing.T) {
	assert.Equal(t, "chat.migration.internal.site1.msg.edit", MigrationInternalMsgEdit("site1"))
	assert.Equal(t, "chat.migration.internal.site1.msg.delete", MigrationInternalMsgDelete("site1"))
}
```

- [ ] **Step 2: Run — expect FAIL.** `go test ./pkg/subject/ -run TestMigrationInternal -v`

- [ ] **Step 3: Implement** in `pkg/subject/subject.go`:

```go
// MigrationInternalMsgEdit is the server-only request subject for applying a migrated
// message edit. MUST be locked to server identities in NATS permissions (no client access).
func MigrationInternalMsgEdit(siteID string) string {
	return fmt.Sprintf("chat.migration.internal.%s.msg.edit", siteID)
}

// MigrationInternalMsgDelete is the server-only request subject for a migrated soft-delete.
func MigrationInternalMsgDelete(siteID string) string {
	return fmt.Sprintf("chat.migration.internal.%s.msg.delete", siteID)
}
```

- [ ] **Step 4: Run — expect PASS.** **Step 5: Commit.** `git add pkg/subject && git commit -m "feat(subject): migration internal msg edit/delete subjects"`

### Task 3: Migration wire types (`pkg/model`)

**Files:** Create `pkg/model/migration.go`; add a round-trip case to `pkg/model/model_test.go`.

- [ ] **Step 1: Add the failing test** to `pkg/model/model_test.go`:

```go
func TestMigrationRequests_RoundTrip(t *testing.T) {
	ts := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)
	roundTrip(t, &model.MigrationEditRequest{MessageID: "m1", RoomID: "r1", CreatedAt: ts, Content: "edited", EditedAt: ts}, &model.MigrationEditRequest{})
	roundTrip(t, &model.MigrationDeleteRequest{MessageID: "m1", RoomID: "r1", CreatedAt: ts, DeletedAt: ts}, &model.MigrationDeleteRequest{})
	roundTrip(t, &model.MigrationAck{OK: true}, &model.MigrationAck{})
}
```

- [ ] **Step 2: Run — expect FAIL.** `go test ./pkg/model/ -run TestMigrationRequests -v`

- [ ] **Step 3: Implement** `pkg/model/migration.go`:

```go
package model

import "time"

// MigrationEditRequest is the payload the oplog-transformer sends to history-service's
// internal migration-edit handler. It carries the Cassandra locator (RoomID + CreatedAt +
// MessageID) plus the new content, since the oplog update event lacks roomId/createdAt.
type MigrationEditRequest struct {
	MessageID string    `json:"messageId" bson:"messageId"`
	RoomID    string    `json:"roomId"    bson:"roomId"`
	CreatedAt time.Time `json:"createdAt" bson:"createdAt"`
	Content   string    `json:"content"   bson:"content"`
	EditedAt  time.Time `json:"editedAt"  bson:"editedAt"`
}

// MigrationDeleteRequest is the payload for the internal migration-delete (soft-delete) handler.
type MigrationDeleteRequest struct {
	MessageID string    `json:"messageId" bson:"messageId"`
	RoomID    string    `json:"roomId"    bson:"roomId"`
	CreatedAt time.Time `json:"createdAt" bson:"createdAt"`
	DeletedAt time.Time `json:"deletedAt" bson:"deletedAt"`
}

// MigrationAck is the reply from the internal migration handlers.
type MigrationAck struct {
	OK bool `json:"ok" bson:"ok"`
}
```

- [ ] **Step 4: Run — expect PASS.** **Step 5: Commit.** `git add pkg/model && git commit -m "feat(model): migration edit/delete request + ack types"`

---

## Phase 2 — oplog-transformer service

### Task 4: Service scaffold + config

**Files:** Create `data-migration/oplog-transformer/config.go`; Test `config_test.go`.

- [ ] **Step 1: Failing test** (`config_test.go`):

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SITE_ID", "site1")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SOURCE_MONGO_URI", "mongodb://localhost:27017")
}

func TestParseConfig_Defaults(t *testing.T) {
	setEnv(t)
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "site1", cfg.SiteID)
	assert.Equal(t, "rocketchat", cfg.SourceDB)
	assert.Equal(t, "rocketchat_message", cfg.SourceMessageCollection)
	assert.Equal(t, "primaryPreferred", cfg.SourceReadPreference)
	assert.Equal(t, "oplog-transformer", cfg.ConsumerDurable)
}

func TestParseConfig_MissingRequired(t *testing.T) {
	t.Setenv("SITE_ID", "site1")
	_, err := parseConfig()
	require.Error(t, err)
}
```

- [ ] **Step 2: Run — expect FAIL.** `make test SERVICE=data-migration/oplog-transformer`

- [ ] **Step 3: Implement** `config.go`:

```go
package main

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type config struct {
	SiteID                  string        `env:"SITE_ID,required"`
	NatsURL                 string        `env:"NATS_URL,required"`
	NatsCredsFile           string        `env:"NATS_CREDS_FILE" envDefault:""`
	SourceMongoURI          string        `env:"SOURCE_MONGO_URI,required"`
	SourceUsername          string        `env:"SOURCE_MONGO_USERNAME" envDefault:""`
	SourcePassword          string        `env:"SOURCE_MONGO_PASSWORD" envDefault:""`
	SourceDB                string        `env:"SOURCE_DB" envDefault:"rocketchat"`
	SourceMessageCollection string        `env:"SOURCE_MESSAGE_COLLECTION" envDefault:"rocketchat_message"`
	SourceReadPreference    string        `env:"SOURCE_READ_PREFERENCE" envDefault:"primaryPreferred"`
	ConsumerDurable         string        `env:"CONSUMER_DURABLE" envDefault:"oplog-transformer"`
	HistoryRequestTimeout   time.Duration `env:"HISTORY_REQUEST_TIMEOUT" envDefault:"10s"`
	MaxDeliver              int           `env:"MAX_DELIVER" envDefault:"5"`
	MetricsAddr             string        `env:"METRICS_ADDR" envDefault:":9090"`
	LogLevel                string        `env:"LOG_LEVEL" envDefault:"info"`
}

func parseConfig() (config, error) {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return config{}, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}
```

- [ ] **Step 4: Run — expect PASS.** **Step 5: Commit.** `git add data-migration/oplog-transformer/config*.go && git commit -m "feat(oplog-transformer): config scaffold"`

### Task 5: `messagemap` — decode + map + classify

**Files:** Create `messagemap.go`, `messagemap_test.go`, `testdata/insert.json`, `testdata/edit.json`, `testdata/softdelete.json`.

- [ ] **Step 1: Add fixtures** (relaxed extJSON, exactly what the connector emits via `bson.MarshalExtJSON(_, false, false)`):

`testdata/insert.json`:
```json
{"_id":"abc123def456ghi78","rid":"room1","msg":"hello world","ts":{"$date":"2023-01-02T03:04:05Z"},"u":{"_id":"u1","username":"alice","name":"Alice A"}}
```
`testdata/edit.json`:
```json
{"_id":"abc123def456ghi78","rid":"room1","msg":"edited text","ts":{"$date":"2023-01-02T03:04:05Z"},"editedAt":{"$date":"2023-01-02T04:00:00Z"},"u":{"_id":"u1","username":"alice","name":"Alice A"}}
```
`testdata/softdelete.json`:
```json
{"_id":"abc123def456ghi78","rid":"room1","msg":"","t":"rm","ts":{"$date":"2023-01-02T03:04:05Z"},"editedAt":{"$date":"2023-01-02T05:00:00Z"},"u":{"_id":"u1","username":"alice","name":"Alice A"}}
```

- [ ] **Step 2: Failing test** (`messagemap_test.go`):

```go
package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadDoc(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return b
}

func TestDecodeAndMap_Insert(t *testing.T) {
	rc, err := decodeRocketchatMessage(loadDoc(t, "insert.json"))
	require.NoError(t, err)
	msg := mapToMessage(rc)
	assert.Equal(t, "abc123def456ghi78", msg.ID)
	assert.Equal(t, "room1", msg.RoomID)
	assert.Equal(t, "u1", msg.UserID)
	assert.Equal(t, "alice", msg.UserAccount)
	assert.Equal(t, "Alice A", msg.UserDisplayName)
	assert.Equal(t, "hello world", msg.Content)
	assert.Equal(t, time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC), msg.CreatedAt.UTC())
	assert.False(t, isSoftDeleted(rc))
}

func TestClassify_Edit(t *testing.T) {
	rc, err := decodeRocketchatMessage(loadDoc(t, "edit.json"))
	require.NoError(t, err)
	assert.False(t, isSoftDeleted(rc))
	require.NotNil(t, rc.EditedAt)
}

func TestClassify_SoftDelete(t *testing.T) {
	rc, err := decodeRocketchatMessage(loadDoc(t, "softdelete.json"))
	require.NoError(t, err)
	assert.True(t, isSoftDeleted(rc))
}

func TestDecode_Invalid(t *testing.T) {
	_, err := decodeRocketchatMessage([]byte(`{not json`))
	require.Error(t, err)
}
```

- [ ] **Step 3: Run — expect FAIL.**

- [ ] **Step 4: Implement** `messagemap.go`:

```go
package main

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

// softDeleteType is the RocketChat system-message type for a removed message.
// CONFIRM against the source in Task 0; change here if the source uses a different marker.
const softDeleteType = "rm"

// rocketchatMessage is the subset of a RocketChat message doc we consume. Decoded from the
// connector's relaxed extended JSON via bson.UnmarshalExtJSON (handles $date/$oid).
type rocketchatMessage struct {
	ID       string     `bson:"_id"`
	RID      string     `bson:"rid"`
	Msg      string     `bson:"msg"`
	TS       time.Time  `bson:"ts"`
	EditedAt *time.Time `bson:"editedAt"`
	T        string     `bson:"t"`
	TMID     string     `bson:"tmid"`
	U        struct {
		ID       string `bson:"_id"`
		Username string `bson:"username"`
		Name     string `bson:"name"`
	} `bson:"u"`
}

// decodeRocketchatMessage parses the connector's opaque relaxed-extJSON document.
func decodeRocketchatMessage(raw []byte) (*rocketchatMessage, error) {
	var doc rocketchatMessage
	if err := bson.UnmarshalExtJSON(raw, false, &doc); err != nil {
		return nil, fmt.Errorf("decode rocketchat message: %w", err)
	}
	return &doc, nil
}

// isSoftDeleted reports whether the source doc represents a soft-deleted message.
func isSoftDeleted(rc *rocketchatMessage) bool {
	return rc.T == softDeleteType
}

// mapToMessage translates a RocketChat doc into the new-stack model.Message. The _id is kept
// verbatim (17-char RocketChat id, accepted by idgen.IsValidMessageID).
func mapToMessage(rc *rocketchatMessage) model.Message {
	return model.Message{
		ID:                    rc.ID,
		RoomID:                rc.RID,
		UserID:                rc.U.ID,
		UserAccount:           rc.U.Username,
		UserDisplayName:       rc.U.Name,
		Content:               rc.Msg,
		CreatedAt:             rc.TS,
		EditedAt:              rc.EditedAt,
		ThreadParentMessageID: rc.TMID,
	}
}
```

- [ ] **Step 5: Run — expect PASS.** **Step 6: Commit.** `git add data-migration/oplog-transformer/messagemap*.go data-migration/oplog-transformer/testdata && git commit -m "feat(oplog-transformer): rocketchat message decode/map/classify"`

### Task 6: Canonical insert publish

**Files:** Create `canonical.go`, `canonical_test.go`.

- [ ] **Step 1: Failing test** (inject the publish fn so no real NATS):

```go
package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestPublishInsert(t *testing.T) {
	var got *nats.Msg
	pub := func(_ context.Context, m *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		got = m
		return &jetstream.PubAck{Sequence: 1}, nil
	}
	p := &canonicalPublisher{siteID: "site1", publish: pub, now: func() int64 { return 123 }}
	msg := model.Message{ID: "m1", RoomID: "r1", Content: "hi", CreatedAt: time.Unix(0, 0)}

	require.NoError(t, p.publishInsert(context.Background(), msg))
	require.NotNil(t, got)
	assert.Equal(t, subject.MsgCanonicalCreated("site1"), got.Subject)
	assert.True(t, natsutil.IsMigrationLive(got))

	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(got.Data, &evt))
	assert.Equal(t, model.EventCreated, evt.Event)
	assert.Equal(t, "m1", evt.Message.ID)
	assert.Equal(t, int64(123), evt.Timestamp)
}
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** `canonical.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// publishFunc is the minimal JetStream publish surface (oteljetstream.JetStream.PublishMsg).
type publishFunc func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)

// canonicalPublisher emits migrated inserts onto the canonical .created subject.
type canonicalPublisher struct {
	siteID  string
	publish publishFunc
	now     func() int64
}

// publishInsert maps a migrated message to a MessageEvent{created} and publishes it,
// blocking on the pub-ack (the only durability handoff for inserts). The X-Migration: live
// header suppresses live delivery; dedup id = message ID so replays collapse.
func (p *canonicalPublisher) publishInsert(ctx context.Context, msg model.Message) error {
	evt := model.MessageEvent{Event: model.EventCreated, Message: msg, SiteID: p.siteID, Timestamp: p.now()}
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal message event: %w", err)
	}
	m := natsutil.NewMsg(ctx, subject.MsgCanonicalCreated(p.siteID), data)
	natsutil.SetMigrationLive(m)
	if _, err := p.publish(ctx, m, jetstream.WithMsgID(msg.ID)); err != nil {
		return fmt.Errorf("publish canonical created: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run — expect PASS.** **Step 5: Commit.** `git commit -am "feat(oplog-transformer): confirmed canonical insert publish"`

### Task 7: Source lookup

**Files:** Create `sourcelookup.go`; add `//go:generate mockgen` + regenerate after Task 9 wiring. Unit-test via the interface (no real Mongo).

- [ ] **Step 1: Define the interface + Mongo impl** (`sourcelookup.go`):

```go
package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

//go:generate mockgen -destination=mock_lookup_test.go -package=main . sourceLookup

// sourceLookup fetches the current full message doc from the source by _id.
type sourceLookup interface {
	// FindByID returns the raw BSON-extended-JSON document, or (nil, nil) if absent.
	FindByID(ctx context.Context, id string) ([]byte, error)
}

type mongoSourceLookup struct {
	coll *mongo.Collection
}

func newMongoSourceLookup(coll *mongo.Collection) *mongoSourceLookup {
	return &mongoSourceLookup{coll: coll}
}

// FindByID reads the doc and re-encodes it as relaxed extended JSON, matching the shape
// messagemap expects (same as the connector emits).
func (m *mongoSourceLookup) FindByID(ctx context.Context, id string) ([]byte, error) {
	var raw bson.Raw
	err := m.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&raw)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("source find %q: %w", id, err)
	}
	out, err := bson.MarshalExtJSON(raw, false, false)
	if err != nil {
		return nil, fmt.Errorf("encode source doc %q: %w", id, err)
	}
	return out, nil
}
```

- [ ] **Step 2:** Generate the mock: `make generate SERVICE=data-migration/oplog-transformer` (creates `mock_lookup_test.go`). If mockgen errors on Go version, hand-write a `fakeLookup` in the test file instead.

- [ ] **Step 3: Commit.** `git add data-migration/oplog-transformer/sourcelookup.go data-migration/oplog-transformer/mock_lookup_test.go && git commit -m "feat(oplog-transformer): source mongo lookup"`

### Task 8: History client (request/reply)

**Files:** Create `historyclient.go`.

- [ ] **Step 1: Implement** the interface + NATS impl:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

//go:generate mockgen -destination=mock_history_test.go -package=main . historyClient

// historyClient applies a migrated edit/soft-delete via history-service's internal handlers.
type historyClient interface {
	Edit(ctx context.Context, req model.MigrationEditRequest) error
	Delete(ctx context.Context, req model.MigrationDeleteRequest) error
}

type natsHistoryClient struct {
	nc      *nats.Conn
	siteID  string
	timeout time.Duration
}

func (c *natsHistoryClient) Edit(ctx context.Context, req model.MigrationEditRequest) error {
	return c.request(ctx, subject.MigrationInternalMsgEdit(c.siteID), req)
}

func (c *natsHistoryClient) Delete(ctx context.Context, req model.MigrationDeleteRequest) error {
	return c.request(ctx, subject.MigrationInternalMsgDelete(c.siteID), req)
}

func (c *natsHistoryClient) request(ctx context.Context, subj string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal migration request: %w", err)
	}
	rctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	reply, err := c.nc.RequestMsgWithContext(rctx, natsutil.NewMsg(ctx, subj, data))
	if err != nil {
		return fmt.Errorf("history request %q: %w", subj, err)
	}
	var ack model.MigrationAck
	if err := json.Unmarshal(reply.Data, &ack); err != nil {
		return fmt.Errorf("decode migration ack: %w", err)
	}
	if !ack.OK {
		return fmt.Errorf("history rejected migration op on %q", subj)
	}
	return nil
}
```

> `c.nc` is the raw `*nats.Conn` underlying the `*otelnats.Conn` (use `nc.Conn` or the otelnats accessor). Confirm the accessor when wiring main.go.

- [ ] **Step 2:** `make generate SERVICE=data-migration/oplog-transformer` to add `mock_history_test.go`.

- [ ] **Step 3: Commit.** `git commit -am "feat(oplog-transformer): history internal request/reply client"`

### Task 9: Handler — route + op dispatch + ack semantics

**Files:** Create `handler.go`, `handler_test.go`.

- [ ] **Step 1: Failing test** — table-driven over ops, using the fake lookup + mock publisher + mock history client. Asserts: insert→publish; update(edit)→history.Edit; update(softdelete)→history.Delete; replace→no lookup; unknown collection/op delete→skip; lookup miss→skip.

```go
package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

type recordPublisher struct{ inserts []model.Message }

func (r *recordPublisher) publishInsert(_ context.Context, m model.Message) error {
	r.inserts = append(r.inserts, m)
	return nil
}

type recordHistory struct {
	edits   []model.MigrationEditRequest
	deletes []model.MigrationDeleteRequest
}

func (r *recordHistory) Edit(_ context.Context, req model.MigrationEditRequest) error {
	r.edits = append(r.edits, req)
	return nil
}
func (r *recordHistory) Delete(_ context.Context, req model.MigrationDeleteRequest) error {
	r.deletes = append(r.deletes, req)
	return nil
}

type fakeLookup map[string][]byte

func (f fakeLookup) FindByID(_ context.Context, id string) ([]byte, error) { return f[id], nil }

func newTestHandler(pub inserter, hist historyClient, look sourceLookup) *handler {
	return &handler{collection: "rocketchat_message", publisher: pub, history: hist, lookup: look}
}

func TestHandle_Insert(t *testing.T) {
	pub := &recordPublisher{}
	h := newTestHandler(pub, &recordHistory{}, fakeLookup{})
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "insert", FullDocument: loadDoc(t, "insert.json")}))
	require.Len(t, pub.inserts, 1)
	assert.Equal(t, "abc123def456ghi78", pub.inserts[0].ID)
}

func TestHandle_UpdateEdit(t *testing.T) {
	hist := &recordHistory{}
	look := fakeLookup{"abc123def456ghi78": loadDoc(t, "edit.json")}
	h := newTestHandler(&recordPublisher{}, hist, look)
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "update", DocumentKey: []byte(`{"_id":"abc123def456ghi78"}`)}))
	require.Len(t, hist.edits, 1)
	assert.Equal(t, "edited text", hist.edits[0].Content)
}

func TestHandle_UpdateSoftDelete(t *testing.T) {
	hist := &recordHistory{}
	look := fakeLookup{"abc123def456ghi78": loadDoc(t, "softdelete.json")}
	h := newTestHandler(&recordPublisher{}, hist, look)
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "update", DocumentKey: []byte(`{"_id":"abc123def456ghi78"}`)}))
	require.Len(t, hist.deletes, 1)
}

func TestHandle_ReplaceUsesEventDoc(t *testing.T) {
	hist := &recordHistory{}
	h := newTestHandler(&recordPublisher{}, hist, fakeLookup{}) // empty lookup → must use event doc
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "replace", FullDocument: loadDoc(t, "edit.json")}))
	require.Len(t, hist.edits, 1)
}

func TestHandle_DeleteOpSkipped(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "delete", DocumentKey: []byte(`{"_id":"x"}`)}))
}

func TestHandle_UnknownCollectionSkipped(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "users", Op: "insert", FullDocument: []byte(`{}`)}))
}

func TestHandle_LookupMissSkipped(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "update", DocumentKey: []byte(`{"_id":"gone"}`)}))
}
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** `handler.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// oplogEvent mirrors model.OplogEvent's wire shape (decoded from the consumed message).
type oplogEvent struct {
	EventID           string          `json:"eventId"`
	Op                string          `json:"op"`
	Collection        string          `json:"coll"`
	DocumentKey       json.RawMessage `json:"documentKey"`
	FullDocument      json.RawMessage `json:"fullDocument"`
	UpdateDescription json.RawMessage `json:"updateDescription"`
}

// inserter is the canonicalPublisher surface the handler needs (lets tests fake it).
type inserter interface {
	publishInsert(ctx context.Context, msg model.Message) error
}

type handler struct {
	collection string // the watched message collection name (cfg.SourceMessageCollection)
	publisher  inserter
	history    historyClient
	lookup     sourceLookup
	now        func() int64
}

// handle processes one decoded oplog event. Returning nil = ack; returning an error = the
// caller Naks (transient) or, for a poison/permanent error, Terms. Skips return nil (ack).
func (h *handler) handle(ctx context.Context, ev oplogEvent) error {
	if ev.Collection != h.collection {
		slog.Debug("skip non-message collection", "collection", ev.Collection)
		return nil
	}
	switch ev.Op {
	case "insert":
		return h.handleInsert(ctx, ev.FullDocument)
	case "update":
		return h.handleUpdate(ctx, ev)
	case "replace":
		return h.handleReplace(ctx, ev.FullDocument)
	case "delete":
		slog.Warn("hard-delete op skipped (source soft-deletes; out of scope)", "eventId", ev.EventID)
		return nil
	default:
		slog.Warn("unknown op skipped", "op", ev.Op, "eventId", ev.EventID)
		return nil
	}
}

func (h *handler) handleInsert(ctx context.Context, doc []byte) error {
	rc, err := decodeRocketchatMessage(doc)
	if err != nil {
		return fmt.Errorf("%w: %w", errPoison, err) // unmappable doc → Term
	}
	return h.publisher.publishInsert(ctx, mapToMessage(rc))
}

func (h *handler) handleUpdate(ctx context.Context, ev oplogEvent) error {
	var key struct {
		ID string `json:"_id"`
	}
	if err := json.Unmarshal(ev.DocumentKey, &key); err != nil || key.ID == "" {
		return fmt.Errorf("%w: bad documentKey", errPoison)
	}
	doc, err := h.lookup.FindByID(ctx, key.ID)
	if err != nil {
		return fmt.Errorf("lookup %q: %w", key.ID, err) // transient → Nak
	}
	if doc == nil {
		slog.Warn("update lookup miss — skipping", "id", key.ID)
		return nil
	}
	return h.applyUpdate(ctx, doc)
}

func (h *handler) handleReplace(ctx context.Context, doc []byte) error {
	if len(doc) == 0 {
		return fmt.Errorf("%w: replace without fullDocument", errPoison)
	}
	return h.applyUpdate(ctx, doc)
}

// applyUpdate classifies the resolved doc and routes to the right history handler.
func (h *handler) applyUpdate(ctx context.Context, doc []byte) error {
	rc, err := decodeRocketchatMessage(doc)
	if err != nil {
		return fmt.Errorf("%w: %w", errPoison, err)
	}
	if isSoftDeleted(rc) {
		when := rc.TS
		if rc.EditedAt != nil {
			when = *rc.EditedAt
		}
		return h.history.Delete(ctx, model.MigrationDeleteRequest{
			MessageID: rc.ID, RoomID: rc.RID, CreatedAt: rc.TS, DeletedAt: when,
		})
	}
	edited := rc.TS
	if rc.EditedAt != nil {
		edited = *rc.EditedAt
	}
	return h.history.Edit(ctx, model.MigrationEditRequest{
		MessageID: rc.ID, RoomID: rc.RID, CreatedAt: rc.TS, Content: rc.Msg, EditedAt: edited,
	})
}

var _ = time.Now
```

Also add `errors.go` with the poison sentinel:

```go
package main

import "errors"

// errPoison marks an event that can never succeed (unmappable doc). The consume loop Terms
// these (ack-poison) instead of redelivering, so one bad event never wedges the stream.
var errPoison = errors.New("poison event")
```

- [ ] **Step 4: Run — expect PASS.** **Step 5: Commit.** `git add data-migration/oplog-transformer && git commit -m "feat(oplog-transformer): route + op dispatch + classify"`

### Task 10: main.go — wiring, consumer, shutdown

**Files:** Create `main.go`.

- [ ] **Step 1: Implement** (mirrors the connector's `main`/shutdown shape; sequential `Consume`):

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

func main() {
	cfg, err := parseConfig()
	if err != nil {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)})))

	ctx := context.Background()
	if _, err := otelutil.InitMeter("oplog-transformer"); err != nil {
		slog.Error("init meter", "error", err)
		os.Exit(1)
	}

	client, err := mongoutil.Connect(ctx, cfg.SourceMongoURI, cfg.SourceUsername, cfg.SourcePassword)
	if err != nil {
		slog.Error("source mongo connect", "error", err)
		os.Exit(1)
	}
	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		mongoutil.Disconnect(ctx, client)
		slog.Error("nats connect", "error", err)
		os.Exit(1)
	}
	js, err := oteljetstream.New(nc)
	if err != nil {
		slog.Error("jetstream init", "error", err)
		os.Exit(1)
	}

	rp, _ := readpref.FromString(cfg.SourceReadPreference) // primaryPreferred etc.
	sourceColl := client.Database(cfg.SourceDB).Collection(cfg.SourceMessageCollection,
		options.Collection().SetReadPreference(rp))

	h := &handler{
		collection: cfg.SourceMessageCollection,
		publisher:  &canonicalPublisher{siteID: cfg.SiteID, publish: js.PublishMsg, now: nowMs},
		history:    &natsHistoryClient{nc: nc.Conn, siteID: cfg.SiteID, timeout: cfg.HistoryRequestTimeout},
		lookup:     newMongoSourceLookup(sourceColl),
		now:        nowMs,
	}

	streamCfg := stream.MigrationOplog(cfg.SiteID)
	cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, jetstream.ConsumerConfig{
		Durable:       cfg.ConsumerDurable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    cfg.MaxDeliver,
		FilterSubject: subjectWildcard(cfg.SiteID),
	})
	if err != nil {
		slog.Error("create consumer", "error", err)
		os.Exit(1)
	}

	cc, err := cons.Consume(func(m jetstream.Msg) { processOne(ctx, h, m) })
	if err != nil {
		slog.Error("consume", "error", err)
		os.Exit(1)
	}

	slog.Info("oplog-transformer started", "site", cfg.SiteID)
	shutdown.Wait(ctx, 25*time.Second,
		func(context.Context) error { cc.Stop(); return nil },
		func(context.Context) error { return nc.Drain() },
		func(c context.Context) error { mongoutil.Disconnect(c, client); return nil },
	)
}

func processOne(ctx context.Context, h *handler, m jetstream.Msg) {
	var ev oplogEvent
	if err := json.Unmarshal(m.Data(), &ev); err != nil {
		slog.Error("decode oplog event — term", "error", err)
		_ = m.Term()
		return
	}
	switch err := h.handle(ctx, ev); {
	case err == nil:
		_ = m.Ack()
	case errors.Is(err, errPoison):
		slog.Error("poison event — term (skipping)", "eventId", ev.EventID, "error", err)
		_ = m.Term()
	default:
		slog.Error("transient failure — nak", "eventId", ev.EventID, "error", err)
		_ = m.NakWithDelay(2 * time.Second)
	}
}

func nowMs() int64 { return time.Now().UTC().UnixMilli() }

func subjectWildcard(siteID string) string { return "chat.oplog." + siteID + ".rocketchat_message.>" }

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

var _ = model.MessageEvent{}
```

> When wiring, confirm the otelnats raw-conn accessor (`nc.Conn`) and `readpref.FromString` (driver v2 helper) — adjust if the API differs. The `FilterSubject` scopes the consumer to the message collection only.

- [ ] **Step 2: Build.** `make build SERVICE=data-migration/oplog-transformer` → expect success. **Step 3: Commit.** `git commit -am "feat(oplog-transformer): main wiring + sequential consumer"`

### Task 11: Metrics + /healthz

**Files:** Create `metrics.go`; wire into `main.go` (a `/metrics`+`/healthz` listener like the connector's `newMetricsServer`).

- [ ] **Step 1:** Copy the connector's `newMetricsServer()` (`data-migration/oplog-connector/main.go`) and add instruments: `oplog_transformer_events_total{op}`, `..._skipped_total{reason}`, `..._lookup_ms`, `..._history_rtt_ms`, `..._naks_total`, `..._terms_total`. Record them in `handler`/`processOne`. Bind synchronously in `main.go` before `Consume`. Mirror the connector's nil-safe metrics pattern.
- [ ] **Step 2: Commit.** `git commit -am "feat(oplog-transformer): prometheus metrics + healthz"`

### Task 12: Deploy manifests

**Files:** Create `deploy/Dockerfile`, `deploy/docker-compose.yml`, `deploy/azure-pipelines.yml`.

- [ ] **Step 1:** Copy `data-migration/oplog-connector/deploy/*` and adjust `IMAGE_NAME`/`SERVICE_PATH`/env to `oplog-transformer` (depends_on source-mongo + nats; `SITE_ID`, `SOURCE_MONGO_URI`, `NATS_URL`). Keep the connector's azure-pipelines coverage gate (`-tags=integration`, 80% floor).
- [ ] **Step 2: Commit.** `git commit -am "feat(oplog-transformer): deploy manifests"`

### Task 13: Integration test

**Files:** Create `integration_test.go` (`//go:build integration`), `TestMain` → `testutil.RunTests`.

- [ ] **Step 1:** Using `testutil.NATS(t)` + `testutil.MongoDB(t,...)`: (a) insert event published to `MIGRATION_OPLOG` → assert a `.created` lands on `MESSAGES_CANONICAL` with `X-Migration: live` header + correct `_id`; (b) update event with a soft-delete doc in source → subscribe a **fake responder** on `chat.migration.internal.{site}.msg.delete` that replies `{"ok":true}`, assert it receives the mapped `MigrationDeleteRequest`. Inline-justify the source RS container per the CLAUDE.md exception.
- [ ] **Step 2: Run** `make test-integration SERVICE=data-migration/oplog-transformer` (Docker). **Step 3: Commit.** `git commit -am "test(oplog-transformer): insert + update e2e"`

---

## Phase 3 — history-service migration handlers

### Task 14: Header-aware publish

**Files:** Modify `history-service/internal/publisher/publisher.go`; Test `publisher_test.go`.

- [ ] **Step 1: Failing test** — assert a new `PublishMigration` sets the `X-Migration` header (capture via an injected `js`/fake or a real `testutil.NATS` sub).
- [ ] **Step 2: Implement** — add a method to `Publisher` (and extend `EventPublisher` interface in `service.go`):

```go
// PublishMigration publishes like Publish but stamps X-Migration: live so live-delivery
// consumers suppress the event.
func (p *Publisher) PublishMigration(ctx context.Context, subj string, data []byte, msgID string) error {
	msg := natsutil.NewMsg(ctx, subj, data)
	natsutil.SetMigrationLive(msg)
	if _, err := p.js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID)); err != nil {
		return fmt.Errorf("publishing migration to %q: %w", subj, err)
	}
	return nil
}
```

Add to `EventPublisher`:
```go
PublishMigration(ctx context.Context, subject string, data []byte, msgID string) error
```
Regenerate publisher mocks if any (`make generate SERVICE=history-service`).

- [ ] **Step 3: Run — expect PASS. Step 4: Commit.** `git commit -am "feat(history-service): header-aware migration publish"`

### Task 15: Migration edit/delete handlers + routes

**Files:** Create `history-service/internal/service/migration.go`, `migration_test.go`; Modify `service.go` `RegisterHandlers`.

- [ ] **Step 1: Failing test** — with a mocked `MessageWriter` + mocked `EventPublisher`, assert: `MigrationEditMessage` calls `UpdateMessageContent` with a `*models.Message{ID,RoomID,CreatedAt}` + content/editedAt, then `PublishMigration` on `subject.MsgCanonicalUpdated(site)`; `MigrationDeleteMessage` calls `SoftDeleteMessage` then `PublishMigration` on `MsgCanonicalDeleted(site)`; returns `&model.MigrationAck{OK:true}`.

- [ ] **Step 2: Implement** `migration.go`:

```go
package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

// MigrationEditMessage applies a migrated content edit (upsert) and republishes the canonical
// .updated event with X-Migration: live. Idempotent: the writer upserts by (RoomID, CreatedAt, ID).
func (s *HistoryService) MigrationEditMessage(c *natsrouter.Context, siteID string, req model.MigrationEditRequest) (*model.MigrationAck, error) {
	m := &models.Message{ID: req.MessageID, RoomID: req.RoomID, CreatedAt: req.CreatedAt}
	if err := s.msgWriter.UpdateMessageContent(c.Context(), m, req.Content, req.EditedAt); err != nil {
		return nil, fmt.Errorf("migration edit %q: %w", req.MessageID, err)
	}
	evt := &model.MessageEvent{
		Event:   model.EventUpdated,
		Message: model.Message{ID: req.MessageID, RoomID: req.RoomID, CreatedAt: req.CreatedAt, Content: req.Content, EditedAt: &req.EditedAt},
		SiteID:  siteID,
	}
	s.publishMigrationBestEffort(c, subject.MsgCanonicalUpdated(siteID), evt)
	return &model.MigrationAck{OK: true}, nil
}

// MigrationDeleteMessage applies a migrated soft-delete (upsert) and republishes .deleted.
func (s *HistoryService) MigrationDeleteMessage(c *natsrouter.Context, siteID string, req model.MigrationDeleteRequest) (*model.MigrationAck, error) {
	m := &models.Message{ID: req.MessageID, RoomID: req.RoomID, CreatedAt: req.CreatedAt}
	if _, _, _, err := s.msgWriter.SoftDeleteMessage(c.Context(), m, req.DeletedAt); err != nil {
		return nil, fmt.Errorf("migration delete %q: %w", req.MessageID, err)
	}
	deletedAt := req.DeletedAt
	evt := &model.MessageEvent{
		Event:   model.EventDeleted,
		Message: model.Message{ID: req.MessageID, RoomID: req.RoomID, CreatedAt: req.CreatedAt, UpdatedAt: &deletedAt},
		SiteID:  siteID,
	}
	s.publishMigrationBestEffort(c, subject.MsgCanonicalDeleted(siteID), evt)
	return &model.MigrationAck{OK: true}, nil
}

func (s *HistoryService) publishMigrationBestEffort(c *natsrouter.Context, subj string, evt *model.MessageEvent) {
	payload, err := json.Marshal(evt)
	if err != nil {
		return
	}
	_ = s.publisher.PublishMigration(c.Context(), subj, payload, natsutil.CanonicalDedupID(evt))
}
```

> Confirm `models.Message` field names (`ID`/`RoomID`/`CreatedAt`) and `c.Context()` against history-service internals; adjust the locator construction if the writer needs more keys (e.g. a bucket). This is the one spot to verify against the real `MessageWriter` impl.

- [ ] **Step 3: Register routes** in `service.go` `RegisterHandlers`:

```go
natsrouter.Register(r, subject.MigrationInternalMsgEdit(siteID), func(c *natsrouter.Context, req model.MigrationEditRequest) (*model.MigrationAck, error) {
	return s.MigrationEditMessage(c, siteID, req)
})
natsrouter.Register(r, subject.MigrationInternalMsgDelete(siteID), func(c *natsrouter.Context, req model.MigrationDeleteRequest) (*model.MigrationAck, error) {
	return s.MigrationDeleteMessage(c, siteID, req)
})
```

- [ ] **Step 4:** `make generate SERVICE=history-service` (if writer mocks changed). Run `make test SERVICE=history-service` — expect PASS. **Step 5: Commit.** `git commit -am "feat(history-service): migration edit/delete internal handlers"`

> **Update `docs/client-api.md`?** No — these are `chat.migration.internal.*` server subjects, not `chat.user.*`. Skip per CLAUDE.md.

---

## Phase 4 — live-delivery suppression

### Task 16: broadcast-worker skips `X-Migration: live`

**Files:** Modify `broadcast-worker/main.go` (the consume wrapper that has the `jetstream.Msg`); Test in `main_test.go` or the wrapper's test.

- [ ] **Step 1: Failing test** — a unit test on the wrapper: a `jetstream.Msg` carrying header `X-Migration: live` is acked and **not** passed to `handler.HandleMessage`. Use a fake `jetstream.Msg` (or refactor the predicate into a pure `func shouldSkipMigration(h nats.Header) bool` and test that).
- [ ] **Step 2: Implement** — extract the header check and early-return+Ack in the processor:

```go
// in the consume callback / broadcastProcessor, before HandleMessage:
if msg.Headers().Get(natsutil.HeaderMigration) == natsutil.MigrationLive {
	_ = msg.Ack()
	return
}
```

- [ ] **Step 3: Run — expect PASS. Step 4: Commit.** `git commit -am "feat(broadcast-worker): skip X-Migration: live events"`

### Task 17: notification-worker skips `X-Migration: live`

**Files:** Modify `notification-worker/main.go` consume wrapper; test as in Task 16.

- [ ] **Steps 1–4:** Same pattern as Task 16, in notification-worker's consume callback. Commit `feat(notification-worker): skip X-Migration: live events`.

---

## Phase 5 — docs & ops

### Task 18: Suite README + ops notes

**Files:** Modify `data-migration/README.md`.

- [ ] **Step 1:** Add an `oplog-transformer` section to the components table (status: implemented) + the §0 flow, and an **ops note**: the `chat.migration.internal.{site}.msg.*` subjects MUST be restricted to server identities in NATS account permissions (no client publish). Note `SOURCE_READ_PREFERENCE=primaryPreferred`. Note the accepted limitation (thread/unread aggregates not maintained).
- [ ] **Step 2: Commit.** `git commit -am "docs(data-migration): document oplog-transformer + internal-subject auth"`

---

## Final verification (run after all tasks)

- [ ] `make fmt` — no changes
- [ ] `make lint` — 0 issues
- [ ] `make test` (or per-service) — all pass with `-race`
- [ ] `make sast` — no medium+ (gosec/govulncheck/semgrep)
- [ ] `make test-integration SERVICE=data-migration/oplog-transformer` — pass (Docker)
- [ ] Manually re-read spec §1–§10; confirm every requirement maps to a task above.
