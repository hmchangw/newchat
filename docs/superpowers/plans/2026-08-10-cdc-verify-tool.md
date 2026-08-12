# CDC Migration Verification Tool (`tools/cdc-verify`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A read-only browser dashboard that tails `MIGRATION-OPLOG-{siteID}`, shows stream telemetry, and verifies every CDC event by comparing current source-Mongo state against mapped destination records (target Mongo / Cassandra) through a JSON fan-out field mapping, with freeze-on-match semantics.

**Architecture:** Flat `package main` under `tools/cdc-verify` (nats-debug pattern: net/http + SSE + one static page, env-configured connections, global state, browser = pure viewer). Pipeline: watcher (ordered ephemeral JetStream consumer) → verifier engine (per-event check fanning out into per-target sub-checks that poll and freeze independently) → in-memory capped results store → SSE broadcaster. A stats poller reads `StreamInfo`/`ConsumerInfo` on an interval.

**Tech Stack:** Go 1.25, `nats.go/jetstream`, `mongo-driver/v2` via `pkg/mongoutil`, `gocql` via `pkg/cassutil`, `pkg/msgbucket`, `pkg/shutdown`, `pkg/subject`, `pkg/stream`, `pkg/model.OplogEvent`. Tests: testify, mockgen (`go.uber.org/mock`), `pkg/testutil` containers. **No new third-party dependencies.**

**Spec:** `docs/superpowers/specs/2026-08-10-cdc-verify-tool-design.md` — read it first; §5 (check lifecycle) and §6 (mapping schema) are the contract this plan implements.

## Global Constraints

- All commands through `make` targets, never raw `go` (`make test SERVICE=tools/cdc-verify`, `make lint`, `make fmt`, `make generate SERVICE=tools/cdc-verify`, `make test-integration SERVICE=tools/cdc-verify`).
- TDD Red-Green-Refactor for every task; run the failing test before implementing.
- `log/slog` JSON only; never log tokens/creds/full message bodies.
- Errors: `fmt.Errorf("doing X: %w", err)`; `errors.Is/As` only; no `errcode` needed (no client-facing NATS/Gin surface).
- Tool is **strictly read-only** on NATS state and all DBs.
- Env config via `caarlos0/env` typed struct; required vars fail fast; never default secrets.
- Unit tests in `package main`, mocks in `mock_store_test.go` via `make generate`; integration tests tagged `//go:build integration` with `testutil.RunTests` TestMain.
- Coverage floor 80%, target 90%+ on mapping/compare/verifier.
- No `docs/client-api.md` update needed (no `chat.user.*` handlers). Commit after every task.

## File Structure

```
tools/cdc-verify/
  main.go              # config struct, wiring, graceful shutdown
  event.go             # CDCEvent decode from model.OplogEvent + subject parse
  mapping.go           # mapping JSON schema structs + custom unmarshals + Load
  mapping_validate.go  # startup validation (fail fast)
  transform.go         # named transform registry (unixMilli, toString, msgBucket)
  compare.go           # dot-path get, normalization, equality, field/verbatim diff
  store.go             # lookup interfaces + go:generate mockgen
  lookup_mongo.go      # source-by-id, dest find, resolver find (Mongo)
  lookup_cassandra.go  # dest find (CQL point select)
  results.go           # ring buffer (recent) + capped failures + counters
  verifier.go          # check lifecycle engine (fan-out, poll, freeze, supersede)
  stats.go             # StreamInfo/ConsumerInfo poller + rate math
  watcher.go           # ordered ephemeral consumer -> verifier.Submit
  hub.go               # SSE broadcaster
  handler.go           # HTTP handlers (state snapshot, SSE, failures.json, healthz)
  routes.go            # mux registration
  static/index.html    # single-page viewer (vanilla JS)
  static.go            # go:embed
  mapping.example.json # shipped default mapping
  README.md
  deploy/Dockerfile
  deploy/docker-compose.yml
  *_test.go            # per-file unit tests; integration_test.go
```

---

### Task 1: Config parsing and validation

**Files:**
- Create: `tools/cdc-verify/main.go`
- Test: `tools/cdc-verify/main_test.go`

**Interfaces:**
- Produces: `type config struct` (fields below) and `func (c config) validate() error` — later tasks read `cfg.SiteID`, `cfg.VerifyPoll`, `cfg.VerifyTimeout`, `cfg.MaxChecks`, `cfg.SamplePercent`, `cfg.RecentCap`, `cfg.FailedCap`, `cfg.StatsInterval`, `cfg.MessageBucketHours`, `cfg.TrackConsumers`, `cfg.StartAtTime`.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parseFrom(t *testing.T, kv map[string]string) (config, error) {
	t.Helper()
	return env.ParseAsWithOptions[config](env.Options{Environment: kv})
}

func fullEnv() map[string]string {
	return map[string]string{
		"SITE_ID": "site1", "NATS_URL": "nats://x:4222",
		"SOURCE_MONGO_URI": "mongodb://s:27017", "TARGET_MONGO_URI": "mongodb://t:27017",
		"MAPPING_FILE": "/tmp/m.json",
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg, err := parseFrom(t, fullEnv())
	require.NoError(t, err)
	assert.Equal(t, 8091, cfg.Port)
	assert.Equal(t, "rocketchat", cfg.SourceDB)
	assert.Equal(t, "chat", cfg.TargetDB)
	assert.Equal(t, 2*time.Second, cfg.VerifyPoll)
	assert.Equal(t, 60*time.Second, cfg.VerifyTimeout)
	assert.Equal(t, 32, cfg.MaxChecks)
	assert.Equal(t, 100, cfg.SamplePercent)
	assert.Equal(t, 200, cfg.RecentCap)
	assert.Equal(t, 1000, cfg.FailedCap)
	assert.Equal(t, 5*time.Second, cfg.StatsInterval)
	assert.Equal(t, 72, cfg.MessageBucketHours)
	assert.NoError(t, cfg.validate())
}

func TestConfig_MissingRequired(t *testing.T) {
	kv := fullEnv()
	delete(kv, "SITE_ID")
	_, err := parseFrom(t, kv)
	assert.Error(t, err)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*config)
		wantErr string
	}{
		{"sample percent over 100", func(c *config) { c.SamplePercent = 101 }, "SAMPLE_PERCENT"},
		{"sample percent negative", func(c *config) { c.SamplePercent = -1 }, "SAMPLE_PERCENT"},
		{"zero poll", func(c *config) { c.VerifyPoll = 0 }, "VERIFY_POLL"},
		{"timeout below poll", func(c *config) { c.VerifyTimeout = time.Second }, "VERIFY_TIMEOUT"},
		{"zero bucket hours", func(c *config) { c.MessageBucketHours = 0 }, "MESSAGE_BUCKET_HOURS"},
		{"bad start time", func(c *config) { c.StartAtTime = "not-a-time" }, "START_AT_TIME"},
		{"ok start time", func(c *config) { c.StartAtTime = "2026-08-10T00:00:00Z" }, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseFrom(t, fullEnv())
			require.NoError(t, err)
			tt.mutate(&cfg)
			err = cfg.validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — `undefined: config`

- [ ] **Step 3: Write minimal implementation**

`main.go` (main() stays a stub until Task 13; only config + validate now):

```go
package main

import (
	"fmt"
	"time"
)

type config struct {
	SiteID    string `env:"SITE_ID,required"`
	NATSURL   string `env:"NATS_URL,required"`
	CredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	SourceMongoURI      string `env:"SOURCE_MONGO_URI,required"`
	SourceMongoUsername string `env:"SOURCE_MONGO_USERNAME" envDefault:""`
	SourceMongoPassword string `env:"SOURCE_MONGO_PASSWORD" envDefault:""`
	SourceDB            string `env:"SOURCE_DB" envDefault:"rocketchat"`

	TargetMongoURI      string `env:"TARGET_MONGO_URI,required"`
	TargetMongoUsername string `env:"TARGET_MONGO_USERNAME" envDefault:""`
	TargetMongoPassword string `env:"TARGET_MONGO_PASSWORD" envDefault:""`
	TargetDB            string `env:"TARGET_DB" envDefault:"chat"`

	// Cassandra is required only when the mapping references a cassandra
	// target — enforced in main after the mapping is loaded, not here.
	CassandraHosts    string `env:"CASSANDRA_HOSTS" envDefault:""`
	CassandraKeyspace string `env:"CASSANDRA_KEYSPACE" envDefault:""`
	CassandraUsername string `env:"CASSANDRA_USERNAME" envDefault:""`
	CassandraPassword string `env:"CASSANDRA_PASSWORD" envDefault:""`

	MappingFile        string        `env:"MAPPING_FILE,required"`
	MessageBucketHours int           `env:"MESSAGE_BUCKET_HOURS" envDefault:"72"`
	TrackConsumers     []string      `env:"TRACK_CONSUMERS" envDefault:""`
	StartAtTime        string        `env:"START_AT_TIME" envDefault:""`
	VerifyPoll         time.Duration `env:"VERIFY_POLL" envDefault:"2s"`
	VerifyTimeout      time.Duration `env:"VERIFY_TIMEOUT" envDefault:"60s"`
	MaxChecks          int           `env:"MAX_CHECKS" envDefault:"32"`
	SamplePercent      int           `env:"SAMPLE_PERCENT" envDefault:"100"`
	RecentCap          int           `env:"RECENT_CAP" envDefault:"200"`
	FailedCap          int           `env:"FAILED_CAP" envDefault:"1000"`
	StatsInterval      time.Duration `env:"STATS_INTERVAL" envDefault:"5s"`
	Port               int           `env:"PORT" envDefault:"8091"`
}

func (c config) validate() error {
	if c.SamplePercent < 0 || c.SamplePercent > 100 {
		return fmt.Errorf("SAMPLE_PERCENT must be 0..100, got %d", c.SamplePercent)
	}
	if c.VerifyPoll <= 0 {
		return fmt.Errorf("VERIFY_POLL must be positive, got %s", c.VerifyPoll)
	}
	if c.VerifyTimeout < c.VerifyPoll {
		return fmt.Errorf("VERIFY_TIMEOUT (%s) must be >= VERIFY_POLL (%s)", c.VerifyTimeout, c.VerifyPoll)
	}
	if c.MaxChecks <= 0 {
		return fmt.Errorf("MAX_CHECKS must be positive, got %d", c.MaxChecks)
	}
	if c.RecentCap <= 0 || c.FailedCap <= 0 {
		return fmt.Errorf("RECENT_CAP and FAILED_CAP must be positive")
	}
	if c.MessageBucketHours <= 0 {
		return fmt.Errorf("MESSAGE_BUCKET_HOURS must be positive, got %d", c.MessageBucketHours)
	}
	if c.StartAtTime != "" {
		if _, err := time.Parse(time.RFC3339, c.StartAtTime); err != nil {
			return fmt.Errorf("START_AT_TIME must be RFC3339: %w", err)
		}
	}
	return nil
}

func main() {}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/cdc-verify`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/main.go tools/cdc-verify/main_test.go
git commit -m "feat(cdc-verify): config parsing and validation"
```

---

### Task 2: CDC event decode + subject parse

**Files:**
- Create: `tools/cdc-verify/event.go`
- Test: `tools/cdc-verify/event_test.go`

**Interfaces:**
- Produces:
  - `type CDCEvent struct { Collection, Op, DocID string; ClusterTime int64 }`
  - `func decodeCDCEvent(data []byte) (CDCEvent, error)` — unmarshals `model.OplogEvent` (encoding/json — this is not a hot-path worker), extracts `documentKey._id` as a string (RocketChat ids are strings; a non-string `_id` — e.g. ext-JSON `$oid` object — is an error).
- Consumes: `pkg/model.OplogEvent`.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeCDCEvent(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    CDCEvent
		wantErr string
	}{
		{
			name:    "insert with string id",
			payload: `{"eventId":"e1","op":"insert","db":"rocketchat","coll":"rocketchat_message","documentKey":{"_id":"msg123"},"clusterTime":1700000000000}`,
			want:    CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "msg123", ClusterTime: 1700000000000},
		},
		{
			name:    "delete carries only documentKey",
			payload: `{"eventId":"e2","op":"delete","coll":"rocketchat_room","documentKey":{"_id":"r1"}}`,
			want:    CDCEvent{Collection: "rocketchat_room", Op: "delete", DocID: "r1"},
		},
		{
			name:    "non-string id rejected",
			payload: `{"op":"insert","coll":"c","documentKey":{"_id":{"$oid":"64f0"}}}`,
			wantErr: "documentKey._id is not a string",
		},
		{
			name:    "missing documentKey rejected",
			payload: `{"op":"insert","coll":"c"}`,
			wantErr: "documentKey",
		},
		{
			name:    "invalid json",
			payload: `{`,
			wantErr: "unmarshal oplog event",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeCDCEvent([]byte(tt.payload))
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — `undefined: decodeCDCEvent`

- [ ] **Step 3: Write minimal implementation**

`event.go`:

```go
package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
)

// CDCEvent is the verifier's trigger: which document in which source
// collection changed. Payload content beyond the key is ignored — the check
// re-reads current state from the source (spec §6).
type CDCEvent struct {
	Collection  string
	Op          string
	DocID       string
	ClusterTime int64
}

func decodeCDCEvent(data []byte) (CDCEvent, error) {
	var ev model.OplogEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return CDCEvent{}, fmt.Errorf("unmarshal oplog event: %w", err)
	}
	if len(ev.DocumentKey) == 0 {
		return CDCEvent{}, fmt.Errorf("oplog event has no documentKey")
	}
	var key struct {
		ID any `json:"_id"`
	}
	if err := json.Unmarshal(ev.DocumentKey, &key); err != nil {
		return CDCEvent{}, fmt.Errorf("unmarshal documentKey: %w", err)
	}
	id, ok := key.ID.(string)
	if !ok {
		return CDCEvent{}, fmt.Errorf("documentKey._id is not a string: %T", key.ID)
	}
	return CDCEvent{Collection: ev.Collection, Op: ev.Op, DocID: id, ClusterTime: ev.ClusterTime}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/cdc-verify`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/event.go tools/cdc-verify/event_test.go
git commit -m "feat(cdc-verify): decode CDC oplog events"
```

---

### Task 3: Mapping schema structs + JSON load

**Files:**
- Create: `tools/cdc-verify/mapping.go`
- Test: `tools/cdc-verify/mapping_test.go`

**Interfaces:**
- Produces (later tasks depend on these exact names):
  - `type Mapping struct { Sources []SourceMapping }`
  - `type SourceMapping struct { Collection string; Ops map[string]OpAction; Resolvers map[string]Resolver; Targets map[string]Target; Fields map[string][]DestRef; Derived []Derived }`
  - `type OpAction string` — consts `OpVerify OpAction = "verify"`, `OpVerifyAbsent OpAction = "verify-absent"`, `OpSkip OpAction = "skip"`
  - `type Resolver struct { DB, Collection string; Key map[string]KeyFrom; Fields []string }` (`DB` is `"source"` or `"target"`)
  - `type Target struct { Kind, Collection, Table string; Key map[string]KeyFrom; Mode string; Ignore []string }` (`Kind` `"mongo"`/`"cassandra"`, `Mode` `""` or `"verbatim"`)
  - `type KeyFrom struct { From []string; Transform string }` — custom UnmarshalJSON: a bare string `"path"` == `{From:["path"]}`; object form `{"from": "p" | ["p1","p2"], "transform": "name"}`
  - `type DestRef struct { Dest, Transform string; Required bool }` — custom UnmarshalJSON: bare string `"alias.field"` == `{Dest:"alias.field"}`
  - `type Derived struct { From []string; Transform string; Dest []string }`
  - `func loadMapping(path string) (*Mapping, error)` — read + unmarshal + `validateMapping` (Task 4)
  - `func (d DestRef) Split() (alias, field string)` — alias is the first dot-segment
  - `func (s *SourceMapping) Action(op string) OpAction` — missing op key → `OpSkip`
- JSON tags: lowercase (`sources`, `collection`, `ops`, `resolvers`, `targets`, `fields`, `derived`, `db`, `key`, `kind`, `table`, `mode`, `ignore`, `from`, `transform`, `dest`, `required`).

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyFrom_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want KeyFrom
	}{
		{"bare string", `"u._id"`, KeyFrom{From: []string{"u._id"}}},
		{"object single from", `{"from":"ts","transform":"unixMilli"}`, KeyFrom{From: []string{"ts"}, Transform: "unixMilli"}},
		{"object multi from", `{"from":["a","b"],"transform":"dmRoomID"}`, KeyFrom{From: []string{"a", "b"}, Transform: "dmRoomID"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got KeyFrom
			require.NoError(t, json.Unmarshal([]byte(tt.in), &got))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDestRef_UnmarshalJSON(t *testing.T) {
	var short DestRef
	require.NoError(t, json.Unmarshal([]byte(`"msgById.body"`), &short))
	assert.Equal(t, DestRef{Dest: "msgById.body"}, short)

	var full DestRef
	require.NoError(t, json.Unmarshal([]byte(`{"dest":"msgById.created_at","transform":"unixMilli","required":true}`), &full))
	assert.Equal(t, DestRef{Dest: "msgById.created_at", Transform: "unixMilli", Required: true}, full)
}

func TestDestRef_Split(t *testing.T) {
	alias, field := DestRef{Dest: "msgById.meta.count"}.Split()
	assert.Equal(t, "msgById", alias)
	assert.Equal(t, "meta.count", field)
}

func TestSourceMapping_Action(t *testing.T) {
	s := SourceMapping{Ops: map[string]OpAction{"insert": OpVerify, "delete": OpVerifyAbsent}}
	assert.Equal(t, OpVerify, s.Action("insert"))
	assert.Equal(t, OpVerifyAbsent, s.Action("delete"))
	assert.Equal(t, OpSkip, s.Action("update"))
}

func writeMapping(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "m.json")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

const validMappingJSON = `{
  "sources": [{
    "collection": "rocketchat_message",
    "ops": {"insert": "verify", "delete": "verify-absent"},
    "resolvers": {
      "user": {"db": "source", "collection": "users", "key": {"_id": "u._id"}, "fields": ["username"]}
    },
    "targets": {
      "msgById": {"kind": "cassandra", "table": "messages_by_id", "key": {"message_id": "_id"}}
    },
    "fields": {
      "msg": ["msgById.body"],
      "ts": [{"dest": "msgById.created_at", "transform": "unixMilli"}]
    },
    "derived": [{"from": ["u._id"], "transform": "toString", "dest": ["msgById.sender_account"]}]
  }]
}`

func TestLoadMapping_Valid(t *testing.T) {
	m, err := loadMapping(writeMapping(t, validMappingJSON))
	require.NoError(t, err)
	require.Len(t, m.Sources, 1)
	src := m.Sources[0]
	assert.Equal(t, "rocketchat_message", src.Collection)
	assert.Equal(t, []string{"u._id"}, src.Resolvers["user"].Key["_id"].From)
	assert.Equal(t, "cassandra", src.Targets["msgById"].Kind)
	assert.Equal(t, []DestRef{{Dest: "msgById.body"}}, src.Fields["msg"])
	assert.Equal(t, "unixMilli", src.Fields["ts"][0].Transform)
}

func TestLoadMapping_FileMissing(t *testing.T) {
	_, err := loadMapping("/nonexistent/m.json")
	assert.Error(t, err)
}

func TestLoadMapping_BadJSON(t *testing.T) {
	_, err := loadMapping(writeMapping(t, `{`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse mapping")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — `undefined: KeyFrom` etc.

- [ ] **Step 3: Write minimal implementation**

`mapping.go`:

```go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type OpAction string

const (
	OpVerify       OpAction = "verify"
	OpVerifyAbsent OpAction = "verify-absent"
	OpSkip         OpAction = "skip"
)

type Mapping struct {
	Sources []SourceMapping `json:"sources"`
}

type SourceMapping struct {
	Collection string               `json:"collection"`
	Ops        map[string]OpAction  `json:"ops"`
	Resolvers  map[string]Resolver  `json:"resolvers,omitempty"`
	Targets    map[string]Target    `json:"targets"`
	Fields     map[string][]DestRef `json:"fields,omitempty"`
	Derived    []Derived            `json:"derived,omitempty"`
}

// Action returns the configured action for op; unlisted ops are skipped.
func (s *SourceMapping) Action(op string) OpAction {
	if a, ok := s.Ops[op]; ok {
		return a
	}
	return OpSkip
}

type Resolver struct {
	DB         string             `json:"db"`
	Collection string             `json:"collection"`
	Key        map[string]KeyFrom `json:"key"`
	Fields     []string           `json:"fields"`
}

type Target struct {
	Kind       string             `json:"kind"`
	Collection string             `json:"collection,omitempty"`
	Table      string             `json:"table,omitempty"`
	Key        map[string]KeyFrom `json:"key"`
	Mode       string             `json:"mode,omitempty"`
	Ignore     []string           `json:"ignore,omitempty"`
}

// KeyFrom is one dest-key/resolver-key entry: source path(s), optionally
// through a named transform. JSON forms: "path" | {"from": "p"|["p"...], "transform": "t"}.
type KeyFrom struct {
	From      []string
	Transform string
}

func (k *KeyFrom) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		k.From = []string{s}
		return nil
	}
	var obj struct {
		From      json.RawMessage `json:"from"`
		Transform string          `json:"transform"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return fmt.Errorf("key entry must be a string or object: %w", err)
	}
	k.Transform = obj.Transform
	var one string
	if err := json.Unmarshal(obj.From, &one); err == nil {
		k.From = []string{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(obj.From, &many); err != nil {
		return fmt.Errorf(`key "from" must be a string or string array: %w`, err)
	}
	k.From = many
	return nil
}

// DestRef is one fan-out destination for a source field.
// JSON forms: "alias.field" | {"dest": "...", "transform": "...", "required": true}.
type DestRef struct {
	Dest      string
	Transform string
	Required  bool
}

func (d *DestRef) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		d.Dest = s
		return nil
	}
	var obj struct {
		Dest      string `json:"dest"`
		Transform string `json:"transform"`
		Required  bool   `json:"required"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return fmt.Errorf("dest ref must be a string or object: %w", err)
	}
	d.Dest, d.Transform, d.Required = obj.Dest, obj.Transform, obj.Required
	return nil
}

// Split separates the target alias (first dot-segment) from the dest field path.
func (d DestRef) Split() (alias, field string) {
	alias, field, _ = strings.Cut(d.Dest, ".")
	return alias, field
}

type Derived struct {
	From      []string `json:"from"`
	Transform string   `json:"transform"`
	Dest      []string `json:"dest"`
}

func loadMapping(path string) (*Mapping, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read mapping file: %w", err)
	}
	var m Mapping
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse mapping file %s: %w", path, err)
	}
	if err := validateMapping(&m); err != nil {
		return nil, fmt.Errorf("validate mapping file %s: %w", path, err)
	}
	return &m, nil
}
```

Until Task 4 exists, add a temporary `func validateMapping(*Mapping) error { return nil }` **in mapping.go** (Task 4 moves it to `mapping_validate.go` and implements it).

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/cdc-verify`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/mapping.go tools/cdc-verify/mapping_test.go
git commit -m "feat(cdc-verify): mapping schema structs and JSON load"
```

---

### Task 4: Mapping validation

**Files:**
- Create: `tools/cdc-verify/mapping_validate.go` (move the stub out of `mapping.go`)
- Test: `tools/cdc-verify/mapping_validate_test.go`

**Interfaces:**
- Produces: `func validateMapping(m *Mapping) error`; `func (m *Mapping) Source(collection string) (*SourceMapping, bool)`; `func (m *Mapping) NeedsCassandra() bool`.
- Consumes: `knownTransform(name string) bool` from Task 5 — **stub it here** as `func knownTransform(string) bool { return true }` in `mapping_validate.go`; Task 5 replaces it with the registry lookup.

Validation rules (spec §6), each with its own error message: duplicate source collection; unknown op key (not insert/update/replace/delete) or unknown action value; target with bad `kind`, mongo target without `collection`, cassandra target without `table`, empty `key`; verbatim target with a non-empty `fields`/`derived` reference to it; destRef alias not in `targets`; destRef into a verbatim target; `@alias` reference (in any KeyFrom.From, Fields source path, or Derived.From) not in `resolvers`; resolver with bad `db` (not source/target), empty `key`/`fields`/`collection`; resolver key using `@` (no chaining); unknown transform name anywhere; derived with empty `from`/`dest` or missing transform; source entry with no targets.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseSource() SourceMapping {
	return SourceMapping{
		Collection: "rocketchat_message",
		Ops:        map[string]OpAction{"insert": OpVerify},
		Targets: map[string]Target{
			"msgById": {Kind: "cassandra", Table: "messages_by_id", Key: map[string]KeyFrom{"message_id": {From: []string{"_id"}}}},
		},
		Fields: map[string][]DestRef{"msg": {{Dest: "msgById.body"}}},
	}
}

func TestValidateMapping(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*SourceMapping)
		wantErr string
	}{
		{"valid", func(s *SourceMapping) {}, ""},
		{"no targets", func(s *SourceMapping) { s.Targets = nil }, "no targets"},
		{"unknown op", func(s *SourceMapping) { s.Ops["upsert"] = OpVerify }, "unknown op"},
		{"unknown action", func(s *SourceMapping) { s.Ops["insert"] = "observe" }, "unknown action"},
		{"bad kind", func(s *SourceMapping) {
			s.Targets["bad"] = Target{Kind: "redis", Key: map[string]KeyFrom{"k": {From: []string{"_id"}}}}
		}, "kind"},
		{"cassandra without table", func(s *SourceMapping) {
			s.Targets["bad"] = Target{Kind: "cassandra", Key: map[string]KeyFrom{"k": {From: []string{"_id"}}}}
		}, "table"},
		{"mongo without collection", func(s *SourceMapping) {
			s.Targets["bad"] = Target{Kind: "mongo", Key: map[string]KeyFrom{"k": {From: []string{"_id"}}}}
		}, "collection"},
		{"empty target key", func(s *SourceMapping) {
			s.Targets["bad"] = Target{Kind: "mongo", Collection: "c", Key: nil}
		}, "empty key"},
		{"destref unknown alias", func(s *SourceMapping) {
			s.Fields["msg"] = append(s.Fields["msg"], DestRef{Dest: "ghost.body"})
		}, "unknown target"},
		{"destref into verbatim", func(s *SourceMapping) {
			tgt := s.Targets["msgById"]
			tgt.Mode = "verbatim"
			s.Targets["msgById"] = tgt
		}, "verbatim"},
		{"resolver ref missing", func(s *SourceMapping) {
			s.Targets["msgById"] = Target{Kind: "cassandra", Table: "t",
				Key: map[string]KeyFrom{"user_id": {From: []string{"@user.username"}}}}
		}, "resolver"},
		{"resolver chaining", func(s *SourceMapping) {
			s.Resolvers = map[string]Resolver{
				"a": {DB: "source", Collection: "users", Fields: []string{"x"},
					Key: map[string]KeyFrom{"_id": {From: []string{"@b.x"}}}},
				"b": {DB: "source", Collection: "users", Fields: []string{"x"},
					Key: map[string]KeyFrom{"_id": {From: []string{"u._id"}}}},
			}
		}, "chain"},
		{"resolver bad db", func(s *SourceMapping) {
			s.Resolvers = map[string]Resolver{"u": {DB: "cassandra", Collection: "users",
				Fields: []string{"x"}, Key: map[string]KeyFrom{"_id": {From: []string{"u._id"}}}}}
		}, "db"},
		{"derived missing transform", func(s *SourceMapping) {
			s.Derived = []Derived{{From: []string{"a"}, Dest: []string{"msgById.x"}}}
		}, "transform"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := baseSource()
			tt.mutate(&src)
			err := validateMapping(&Mapping{Sources: []SourceMapping{src}})
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.wantErr), "error %q should contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateMapping_DuplicateCollection(t *testing.T) {
	err := validateMapping(&Mapping{Sources: []SourceMapping{baseSource(), baseSource()}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestMapping_SourceAndNeedsCassandra(t *testing.T) {
	m := &Mapping{Sources: []SourceMapping{baseSource()}}
	src, ok := m.Source("rocketchat_message")
	require.True(t, ok)
	assert.Equal(t, "rocketchat_message", src.Collection)
	_, ok = m.Source("nope")
	assert.False(t, ok)
	assert.True(t, m.NeedsCassandra())

	mongoOnly := baseSource()
	mongoOnly.Targets = map[string]Target{"t": {Kind: "mongo", Collection: "c",
		Key: map[string]KeyFrom{"_id": {From: []string{"_id"}}}}}
	mongoOnly.Fields = map[string][]DestRef{"msg": {{Dest: "t.body"}}}
	assert.False(t, (&Mapping{Sources: []SourceMapping{mongoOnly}}).NeedsCassandra())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — validation stub returns nil, `Source`/`NeedsCassandra` undefined

- [ ] **Step 3: Write minimal implementation**

`mapping_validate.go` (delete the stub from `mapping.go`):

```go
package main

import (
	"fmt"
	"strings"
)

// knownTransform is replaced by the transform registry in transform.go
// (Task 5). Until then every name is accepted.
func knownTransform(string) bool { return true }

var validOps = map[string]bool{"insert": true, "update": true, "replace": true, "delete": true}

func validateMapping(m *Mapping) error {
	seen := map[string]bool{}
	for i := range m.Sources {
		src := &m.Sources[i]
		if seen[src.Collection] {
			return fmt.Errorf("duplicate source collection %q", src.Collection)
		}
		seen[src.Collection] = true
		if err := validateSource(src); err != nil {
			return fmt.Errorf("source %q: %w", src.Collection, err)
		}
	}
	return nil
}

func validateSource(src *SourceMapping) error {
	if len(src.Targets) == 0 {
		return fmt.Errorf("no targets declared")
	}
	for op, action := range src.Ops {
		if !validOps[op] {
			return fmt.Errorf("unknown op %q", op)
		}
		if action != OpVerify && action != OpVerifyAbsent && action != OpSkip {
			return fmt.Errorf("op %q: unknown action %q", op, action)
		}
	}
	for alias, r := range src.Resolvers {
		if r.DB != "source" && r.DB != "target" {
			return fmt.Errorf("resolver %q: db must be source or target, got %q", alias, r.DB)
		}
		if r.Collection == "" || len(r.Key) == 0 || len(r.Fields) == 0 {
			return fmt.Errorf("resolver %q: collection, key, and fields are all required", alias)
		}
		for kf, k := range r.Key {
			if err := checkKeyFrom(k, nil); err != nil { // nil resolvers: no chaining allowed
				return fmt.Errorf("resolver %q key %q: resolvers must not chain: %w", alias, kf, err)
			}
		}
	}
	for alias, t := range src.Targets {
		if err := validateTarget(alias, t, src.Resolvers); err != nil {
			return err
		}
	}
	for path, refs := range src.Fields {
		if err := checkSourcePath(path, src.Resolvers); err != nil {
			return fmt.Errorf("fields %q: %w", path, err)
		}
		for _, ref := range refs {
			if err := checkDestRef(ref, src.Targets); err != nil {
				return fmt.Errorf("fields %q: %w", path, err)
			}
		}
	}
	for i, d := range src.Derived {
		if len(d.From) == 0 || len(d.Dest) == 0 || d.Transform == "" {
			return fmt.Errorf("derived[%d]: from, dest, and transform are all required", i)
		}
		if !knownTransform(d.Transform) {
			return fmt.Errorf("derived[%d]: unknown transform %q", i, d.Transform)
		}
		for _, f := range d.From {
			if err := checkSourcePath(f, src.Resolvers); err != nil {
				return fmt.Errorf("derived[%d]: %w", i, err)
			}
		}
		for _, dest := range d.Dest {
			if err := checkDestRef(DestRef{Dest: dest}, src.Targets); err != nil {
				return fmt.Errorf("derived[%d]: %w", i, err)
			}
		}
	}
	return nil
}

func validateTarget(alias string, t Target, resolvers map[string]Resolver) error {
	switch t.Kind {
	case "mongo":
		if t.Collection == "" {
			return fmt.Errorf("target %q: mongo target requires collection", alias)
		}
	case "cassandra":
		if t.Table == "" {
			return fmt.Errorf("target %q: cassandra target requires table", alias)
		}
	default:
		return fmt.Errorf("target %q: kind must be mongo or cassandra, got %q", alias, t.Kind)
	}
	if len(t.Key) == 0 {
		return fmt.Errorf("target %q: empty key", alias)
	}
	if t.Mode != "" && t.Mode != "verbatim" {
		return fmt.Errorf("target %q: mode must be empty or verbatim, got %q", alias, t.Mode)
	}
	for kf, k := range t.Key {
		if err := checkKeyFrom(k, resolvers); err != nil {
			return fmt.Errorf("target %q key %q: %w", alias, kf, err)
		}
	}
	return nil
}

func checkKeyFrom(k KeyFrom, resolvers map[string]Resolver) error {
	if len(k.From) == 0 {
		return fmt.Errorf("empty from")
	}
	if k.Transform != "" && !knownTransform(k.Transform) {
		return fmt.Errorf("unknown transform %q", k.Transform)
	}
	for _, f := range k.From {
		if err := checkSourcePath(f, resolvers); err != nil {
			return err
		}
	}
	return nil
}

// checkSourcePath validates a source path; "@alias.field" must reference a
// declared resolver. Passing nil resolvers rejects any @-reference (used to
// forbid resolver chaining).
func checkSourcePath(path string, resolvers map[string]Resolver) error {
	if !strings.HasPrefix(path, "@") {
		return nil
	}
	alias, _, ok := strings.Cut(strings.TrimPrefix(path, "@"), ".")
	if !ok || alias == "" {
		return fmt.Errorf("malformed resolver reference %q", path)
	}
	if _, declared := resolvers[alias]; !declared {
		return fmt.Errorf("path %q references undeclared resolver %q", path, alias)
	}
	return nil
}

func checkDestRef(ref DestRef, targets map[string]Target) error {
	alias, field := ref.Split()
	if field == "" {
		return fmt.Errorf("dest ref %q must be alias.field", ref.Dest)
	}
	t, ok := targets[alias]
	if !ok {
		return fmt.Errorf("dest ref %q: unknown target %q", ref.Dest, alias)
	}
	if t.Mode == "verbatim" {
		return fmt.Errorf("dest ref %q: target %q is verbatim and takes no field refs", ref.Dest, alias)
	}
	if ref.Transform != "" && !knownTransform(ref.Transform) {
		return fmt.Errorf("dest ref %q: unknown transform %q", ref.Dest, ref.Transform)
	}
	return nil
}

// Source returns the mapping entry for a raw source collection.
func (m *Mapping) Source(collection string) (*SourceMapping, bool) {
	for i := range m.Sources {
		if m.Sources[i].Collection == collection {
			return &m.Sources[i], true
		}
	}
	return nil, false
}

// NeedsCassandra reports whether any target is a cassandra table — gates
// whether main must connect to Cassandra at all.
func (m *Mapping) NeedsCassandra() bool {
	for _, s := range m.Sources {
		for _, t := range s.Targets {
			if t.Kind == "cassandra" {
				return true
			}
		}
	}
	return false
}
```

Note the "resolver chaining" test expects the error to contain "chain" — the wrapping added at the resolver-key call site provides it.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/cdc-verify`
Expected: PASS (including Tasks 1-3 tests)

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/mapping.go tools/cdc-verify/mapping_validate.go tools/cdc-verify/mapping_validate_test.go
git commit -m "feat(cdc-verify): mapping validation"
```

---

### Task 5: Transform registry

**Files:**
- Create: `tools/cdc-verify/transform.go`
- Modify: `tools/cdc-verify/mapping_validate.go` (delete the `knownTransform` stub)
- Test: `tools/cdc-verify/transform_test.go`

**Interfaces:**
- Produces:
  - `type transformFn func(args []any) (any, error)` — args are the resolved `From` values in declared order.
  - `type transformRegistry map[string]transformFn`
  - `func newTransformRegistry(bucket msgbucket.Sizer) transformRegistry` — registers `unixMilli`, `toString`, `msgBucket`.
  - `func (r transformRegistry) apply(name string, args []any) (any, error)` — empty name = identity passthrough of `args[0]`; unknown name = error.
  - `func knownTransform(name string) bool` — package-level, backed by a package-level `var transformNames = map[string]bool{...}` so validation needs no Sizer.
- Consumes: `pkg/msgbucket.Sizer`.

Transform semantics:
- `unixMilli`: `time.Time` → `.UTC().UnixMilli()`; `primitive.DateTime`-decoded values arrive as `time.Time` from the driver already; a float64/int64/json.Number that is already unix-ms passes through as int64; anything else errors.
- `toString`: `fmt.Sprintf("%v", arg)` for scalar types; errors on map/slice.
- `msgBucket`: same input coercion as `unixMilli`, then `sizer.Of(time.UnixMilli(ms))`.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

func testRegistry() transformRegistry {
	return newTransformRegistry(msgbucket.New(72 * time.Hour))
}

func TestTransform_UnixMilli(t *testing.T) {
	r := testRegistry()
	ts := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	got, err := r.apply("unixMilli", []any{ts})
	require.NoError(t, err)
	assert.Equal(t, ts.UnixMilli(), got)

	got, err = r.apply("unixMilli", []any{float64(1700000000000)})
	require.NoError(t, err)
	assert.Equal(t, int64(1700000000000), got)

	_, err = r.apply("unixMilli", []any{"not-a-time"})
	assert.Error(t, err)
}

func TestTransform_ToString(t *testing.T) {
	r := testRegistry()
	got, err := r.apply("toString", []any{42})
	require.NoError(t, err)
	assert.Equal(t, "42", got)

	_, err = r.apply("toString", []any{map[string]any{}})
	assert.Error(t, err)
}

func TestTransform_MsgBucket(t *testing.T) {
	r := testRegistry()
	sizer := msgbucket.New(72 * time.Hour)
	ts := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	got, err := r.apply("msgBucket", []any{ts})
	require.NoError(t, err)
	assert.Equal(t, sizer.Of(ts), got)
}

func TestTransform_IdentityAndUnknown(t *testing.T) {
	r := testRegistry()
	got, err := r.apply("", []any{"passthrough"})
	require.NoError(t, err)
	assert.Equal(t, "passthrough", got)

	_, err = r.apply("nope", []any{1})
	assert.Error(t, err)

	_, err = r.apply("unixMilli", nil)
	assert.Error(t, err)
}

func TestKnownTransform(t *testing.T) {
	assert.True(t, knownTransform("unixMilli"))
	assert.True(t, knownTransform("toString"))
	assert.True(t, knownTransform("msgBucket"))
	assert.False(t, knownTransform("nope"))
}
```

Also extend `mapping_validate_test.go` behavior: with the stub gone, add one case to `TestValidateMapping` verifying an unknown transform is now rejected:

```go
		{"unknown transform", func(s *SourceMapping) {
			s.Fields["msg"] = []DestRef{{Dest: "msgById.body", Transform: "nope"}}
		}, "unknown transform"},
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — `undefined: transformRegistry`

- [ ] **Step 3: Write minimal implementation**

`transform.go`:

```go
package main

import (
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

type transformFn func(args []any) (any, error)

type transformRegistry map[string]transformFn

// transformNames mirrors the registry keys so mapping validation can check
// names without constructing a registry (which needs a bucket Sizer).
// Keep in sync with newTransformRegistry.
var transformNames = map[string]bool{
	"unixMilli": true,
	"toString":  true,
	"msgBucket": true,
}

func knownTransform(name string) bool { return transformNames[name] }

func newTransformRegistry(sizer msgbucket.Sizer) transformRegistry {
	return transformRegistry{
		"unixMilli": func(args []any) (any, error) {
			return coerceUnixMilli(args)
		},
		"toString": func(args []any) (any, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("toString takes 1 arg, got %d", len(args))
			}
			switch args[0].(type) {
			case map[string]any, []any:
				return nil, fmt.Errorf("toString: composite value %T not supported", args[0])
			}
			return fmt.Sprintf("%v", args[0]), nil
		},
		"msgBucket": func(args []any) (any, error) {
			ms, err := coerceUnixMilli(args)
			if err != nil {
				return nil, err
			}
			return sizer.Of(time.UnixMilli(ms)), nil
		},
	}
}

func coerceUnixMilli(args []any) (int64, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("expected 1 arg, got %d", len(args))
	}
	switch v := args[0].(type) {
	case time.Time:
		return v.UTC().UnixMilli(), nil
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("cannot coerce %T to unix millis", args[0])
	}
}

// apply runs the named transform; the empty name is the identity on args[0].
func (r transformRegistry) apply(name string, args []any) (any, error) {
	if name == "" {
		if len(args) != 1 {
			return nil, fmt.Errorf("identity transform takes 1 arg, got %d", len(args))
		}
		return args[0], nil
	}
	fn, ok := r[name]
	if !ok {
		return nil, fmt.Errorf("unknown transform %q", name)
	}
	return fn(args)
}
```

In `mapping_validate.go`, delete the stub `func knownTransform(string) bool { return true }` and its comment.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/cdc-verify`
Expected: PASS, including the new unknown-transform validation case

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/transform.go tools/cdc-verify/transform_test.go tools/cdc-verify/mapping_validate.go tools/cdc-verify/mapping_validate_test.go
git commit -m "feat(cdc-verify): transform registry"
```

---

### Task 6: Path extraction, normalization, comparison

**Files:**
- Create: `tools/cdc-verify/compare.go`
- Test: `tools/cdc-verify/compare_test.go`

**Interfaces:**
- Produces:
  - `func getPath(doc map[string]any, path string) (any, bool)` — dot-path descent through nested `map[string]any`; missing segment → `(nil, false)`. No array indexing in v1.
  - `func normalize(v any) any` — canonical comparison form: all signed/unsigned ints and float64 → float64 (unix-ms magnitudes are < 2^53, safe); `time.Time` → float64 unix-ms; `[]byte` → string; `fmt.Stringer` left alone; nil stays nil.
  - `func valuesEqual(a, b any) bool` — `reflect.DeepEqual(normalize(a), normalize(b))`.
  - `type FieldDiff struct { SourcePath, DestField string; Want, Got any; Cause string }` (json tags `sourcePath`, `destField`, `want`, `got`, `cause`)
  - `func diffFields(src, dst map[string]any, pairs []fieldPair, reg transformRegistry) []FieldDiff`
  - `type fieldPair struct { SourcePaths []string; DestField string; Transform string; Required bool }` — the per-target compiled form (verifier compiles `Fields`+`Derived` into these in Task 9).
  - `func diffVerbatim(src, dst map[string]any, ignore []string) []FieldDiff` — symmetric deep compare of all top-level keys except ignored; nested maps compared wholesale via `valuesEqual`.
- Comparison rules (spec §5.3): a source value that is nil/absent matches an absent/nil dest value unless `Required`; transform errors surface as a diff with `Cause: "transform-error: <msg>"`.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

func TestGetPath(t *testing.T) {
	doc := map[string]any{"a": map[string]any{"b": "x"}, "top": 1}
	v, ok := getPath(doc, "a.b")
	assert.True(t, ok)
	assert.Equal(t, "x", v)

	v, ok = getPath(doc, "top")
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	_, ok = getPath(doc, "a.missing")
	assert.False(t, ok)
	_, ok = getPath(doc, "missing.b")
	assert.False(t, ok)
}

func TestValuesEqual(t *testing.T) {
	ts := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		a, b any
		want bool
	}{
		{"int32 vs int64", int32(5), int64(5), true},
		{"int vs float64", 5, float64(5), true},
		{"time vs unix ms", ts, ts.UnixMilli(), true},
		{"bytes vs string", []byte("x"), "x", true},
		{"nil vs nil", nil, nil, true},
		{"string mismatch", "a", "b", false},
		{"nil vs value", nil, "a", false},
		{"nested map equal", map[string]any{"k": int64(1)}, map[string]any{"k": float64(1)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, valuesEqual(tt.a, tt.b))
		})
	}
}

func TestDiffFields(t *testing.T) {
	reg := newTransformRegistry(msgbucket.New(72 * time.Hour))
	ts := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	src := map[string]any{"msg": "hello", "ts": ts, "u": map[string]any{"_id": "u1"}}
	dst := map[string]any{"body": "hello", "created_at": ts.UnixMilli(), "sender_account": "u1"}

	pairs := []fieldPair{
		{SourcePaths: []string{"msg"}, DestField: "body"},
		{SourcePaths: []string{"ts"}, DestField: "created_at", Transform: "unixMilli"},
		{SourcePaths: []string{"u._id"}, DestField: "sender_account"},
	}
	assert.Empty(t, diffFields(src, dst, pairs, reg))

	dst["body"] = "tampered"
	diffs := diffFields(src, dst, pairs, reg)
	assert.Len(t, diffs, 1)
	assert.Equal(t, "msg", diffs[0].SourcePath)
	assert.Equal(t, "hello", diffs[0].Want)
	assert.Equal(t, "tampered", diffs[0].Got)
}

func TestDiffFields_AbsentSemantics(t *testing.T) {
	reg := newTransformRegistry(msgbucket.New(72 * time.Hour))
	src := map[string]any{}
	dst := map[string]any{}

	optional := []fieldPair{{SourcePaths: []string{"gone"}, DestField: "gone"}}
	assert.Empty(t, diffFields(src, dst, optional, reg))

	required := []fieldPair{{SourcePaths: []string{"gone"}, DestField: "gone", Required: true}}
	diffs := diffFields(src, dst, required, reg)
	assert.Len(t, diffs, 1)
	assert.Contains(t, diffs[0].Cause, "required")
}

func TestDiffFields_TransformError(t *testing.T) {
	reg := newTransformRegistry(msgbucket.New(72 * time.Hour))
	src := map[string]any{"ts": "not-a-time"}
	dst := map[string]any{"created_at": int64(1)}
	diffs := diffFields(src, dst, []fieldPair{{SourcePaths: []string{"ts"}, DestField: "created_at", Transform: "unixMilli"}}, reg)
	assert.Len(t, diffs, 1)
	assert.Contains(t, diffs[0].Cause, "transform-error")
}

func TestDiffVerbatim(t *testing.T) {
	src := map[string]any{"_id": "a", "n": int64(1), "_updatedAt": "x"}
	dst := map[string]any{"_id": "a", "n": float64(1), "_updatedAt": "y"}
	assert.Empty(t, diffVerbatim(src, dst, []string{"_updatedAt"}))

	dst["n"] = float64(2)
	diffs := diffVerbatim(src, dst, []string{"_updatedAt"})
	assert.Len(t, diffs, 1)
	assert.Equal(t, "n", diffs[0].SourcePath)

	dst["extra"] = true
	diffs = diffVerbatim(src, dst, []string{"_updatedAt", "n"})
	assert.Len(t, diffs, 1)
	assert.Equal(t, "extra", diffs[0].DestField)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — `undefined: getPath`

- [ ] **Step 3: Write minimal implementation**

`compare.go`:

```go
package main

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// getPath walks a dot-path through nested map[string]any. No array indexing.
func getPath(doc map[string]any, path string) (any, bool) {
	cur := any(doc)
	for _, seg := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[seg]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// normalize maps driver-specific scalar types onto canonical comparison forms.
// Numeric magnitudes in this domain (unix ms, counts) are far below 2^53, so
// float64 is a safe common numeric type.
func normalize(v any) any {
	switch t := v.(type) {
	case int:
		return float64(t)
	case int8:
		return float64(t)
	case int16:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case uint:
		return float64(t)
	case uint8:
		return float64(t)
	case uint16:
		return float64(t)
	case uint32:
		return float64(t)
	case uint64:
		return float64(t)
	case float32:
		return float64(t)
	case time.Time:
		return float64(t.UTC().UnixMilli())
	case []byte:
		return string(t)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = normalize(vv)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			out[i] = normalize(vv)
		}
		return out
	default:
		return v
	}
}

func valuesEqual(a, b any) bool {
	return reflect.DeepEqual(normalize(a), normalize(b))
}

// FieldDiff is one mismatched field in a failed sub-check.
type FieldDiff struct {
	SourcePath string `json:"sourcePath"`
	DestField  string `json:"destField"`
	Want       any    `json:"want"`
	Got        any    `json:"got"`
	Cause      string `json:"cause,omitempty"`
}

// fieldPair is the compiled per-target form of one mapping entry: source
// path(s) -> one dest field, optionally through a transform.
type fieldPair struct {
	SourcePaths []string
	DestField   string
	Transform   string
	Required    bool
}

func diffFields(src, dst map[string]any, pairs []fieldPair, reg transformRegistry) []FieldDiff {
	var diffs []FieldDiff
	for _, p := range pairs {
		args := make([]any, 0, len(p.SourcePaths))
		anyPresent := false
		for _, sp := range p.SourcePaths {
			v, ok := getPath(src, sp)
			if ok {
				anyPresent = true
			}
			args = append(args, v)
		}
		got, gotOK := getPath(dst, p.DestField)

		if !anyPresent {
			if p.Required && (!gotOK || got == nil) {
				diffs = append(diffs, FieldDiff{SourcePath: strings.Join(p.SourcePaths, ","),
					DestField: p.DestField, Cause: "required field absent on both sides"})
			}
			// optional + absent in source: matches absent or any dest value? No —
			// matches only absent/nil dest (spec §5.3).
			if !p.Required && gotOK && got != nil {
				diffs = append(diffs, FieldDiff{SourcePath: strings.Join(p.SourcePaths, ","),
					DestField: p.DestField, Want: nil, Got: got, Cause: "absent in source, present in dest"})
			}
			continue
		}

		want, err := reg.apply(p.Transform, args)
		if err != nil {
			diffs = append(diffs, FieldDiff{SourcePath: strings.Join(p.SourcePaths, ","),
				DestField: p.DestField, Cause: fmt.Sprintf("transform-error: %v", err)})
			continue
		}
		if !valuesEqual(want, got) {
			diffs = append(diffs, FieldDiff{SourcePath: strings.Join(p.SourcePaths, ","),
				DestField: p.DestField, Want: want, Got: got})
		}
	}
	return diffs
}

// diffVerbatim deep-compares whole documents both ways, skipping ignored
// top-level keys. Dest-only keys are reported with an empty SourcePath want.
func diffVerbatim(src, dst map[string]any, ignore []string) []FieldDiff {
	skip := make(map[string]bool, len(ignore))
	for _, k := range ignore {
		skip[k] = true
	}
	var diffs []FieldDiff
	for k, want := range src {
		if skip[k] {
			continue
		}
		got, ok := getPath(dst, k)
		if !ok || !valuesEqual(want, got) {
			diffs = append(diffs, FieldDiff{SourcePath: k, DestField: k, Want: want, Got: got})
		}
	}
	for k, got := range dst {
		if skip[k] {
			continue
		}
		if _, ok := src[k]; !ok {
			diffs = append(diffs, FieldDiff{DestField: k, Got: got, Cause: "present only in dest"})
		}
	}
	return diffs
}
```

Note: `TestDiffFields_AbsentSemantics`'s optional case has both sides absent → no diff; the required case produces the "required" cause. The absent-in-source/present-in-dest rule is exercised implicitly by `diffVerbatim`'s dest-only branch and stays strict for `fields` mode.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/cdc-verify`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/compare.go tools/cdc-verify/compare_test.go
git commit -m "feat(cdc-verify): path extraction, normalization, field and verbatim diff"
```

---

### Task 7: Lookup interfaces, Mongo + Cassandra implementations

**Files:**
- Create: `tools/cdc-verify/store.go`, `tools/cdc-verify/lookup_mongo.go`, `tools/cdc-verify/lookup_cassandra.go`
- Generate: `tools/cdc-verify/mock_store_test.go` (`make generate SERVICE=tools/cdc-verify`)
- Test: `tools/cdc-verify/lookup_mongo_test.go` (query-shape unit tests; live DB behavior is Task 14)

**Interfaces:**
- Produces (`store.go` — consumer-defined, mocked for verifier tests):

```go
package main

import "context"

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// errNotFound / errAmbiguous are sentinel lookup outcomes the verifier
// branches on with errors.Is.
// Declared in store.go: var errNotFound = errors.New("record not found")
// and var errAmbiguous = errors.New("key matched more than one record").

// SourceStore reads current documents from the legacy source Mongo.
type SourceStore interface {
	// FindByID returns the full current doc (no projection: the source field
	// set is mapping-driven and small docs dominate; revisit if profiling
	// disagrees). errNotFound when missing.
	FindByID(ctx context.Context, collection, id string) (map[string]any, error)
	// FindOne locates a doc by field equality — resolver lookups. Projected
	// to fields. errNotFound / errAmbiguous.
	FindOne(ctx context.Context, collection string, key map[string]any, fields []string) (map[string]any, error)
}

// TargetStore reads destination records (target Mongo).
type TargetStore interface {
	// FindOne as above, against the target DB.
	FindOne(ctx context.Context, collection string, key map[string]any, fields []string) (map[string]any, error)
}

// CassStore reads destination rows (Cassandra).
type CassStore interface {
	// SelectOne runs SELECT cols FROM table WHERE key; errNotFound /
	// errAmbiguous. Column values come back keyed by column name.
	SelectOne(ctx context.Context, table string, key map[string]any, cols []string) (map[string]any, error)
}
```

- `lookup_mongo.go`: `type mongoStore struct { db *mongo.Database }`, `func newMongoStore(db *mongo.Database) *mongoStore` — implements both `SourceStore` and `TargetStore` (source and target get separate instances). `FindOne` uses `bson.M` filter built from key, `options.FindOne().SetProjection(...)` from fields, then a `Find` with `SetLimit(2)` to detect ambiguity (2 docs → `errAmbiguous`). Decode into `bson.M` and convert to `map[string]any` via a `bsonToMap` helper (`bson.M` values recursively: `bson.M`→`map[string]any`, `bson.A`→`[]any`, `bson.DateTime`→`time.Time`).
- `lookup_cassandra.go`: `type cassStore struct { session *gocql.Session }`, `func newCassStore(s *gocql.Session) *cassStore`. `SelectOne` builds `SELECT <cols> FROM <table> WHERE k1=? AND k2=?` with sorted key columns (deterministic for tests), `.WithContext(ctx).Iter()`, `MapScan` rows; 0 rows → `errNotFound`, 2+ → `errAmbiguous`. Table/column names come only from the validated mapping file (operator-owned config, not user input); still reject any identifier not matching `^[a-zA-Z0-9_]+$` with an error (defense in depth, keeps gosec quiet on the string-built query with a `// #nosec G201 -- identifiers validated against ^[a-zA-Z0-9_]+$, values are bound parameters` comment).
- Unit tests cover: `buildFilter`/`buildProjection` output shape, `bsonToMap` conversions, cassandra query-string building + identifier rejection (`func buildSelect(table string, key map[string]any, cols []string) (string, []any, error)` extracted pure).

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestBsonToMap(t *testing.T) {
	now := bson.NewDateTimeFromTime(time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC))
	in := bson.M{
		"s":   "x",
		"n":   int32(5),
		"t":   now,
		"sub": bson.M{"k": "v"},
		"arr": bson.A{"a", bson.M{"b": int64(1)}},
	}
	out := bsonToMap(in)
	assert.Equal(t, "x", out["s"])
	assert.Equal(t, int32(5), out["n"])
	assert.Equal(t, now.Time().UTC(), out["t"])
	assert.Equal(t, map[string]any{"k": "v"}, out["sub"])
	assert.Equal(t, []any{"a", map[string]any{"b": int64(1)}}, out["arr"])
}

func TestBuildSelect(t *testing.T) {
	q, args, err := buildSelect("messages_by_id", map[string]any{"message_id": "m1"}, []string{"body", "created_at"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT body, created_at FROM messages_by_id WHERE message_id = ?", q)
	assert.Equal(t, []any{"m1"}, args)

	q, args, err = buildSelect("t", map[string]any{"b": int64(2), "a": "x"}, []string{"c"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT c FROM t WHERE a = ? AND b = ?", q) // sorted key columns
	assert.Equal(t, []any{"x", int64(2)}, args)

	_, _, err = buildSelect("bad;table", map[string]any{"a": 1}, []string{"c"})
	assert.Error(t, err)
	_, _, err = buildSelect("t", map[string]any{"a; DROP": 1}, []string{"c"})
	assert.Error(t, err)
	_, _, err = buildSelect("t", map[string]any{"a": 1}, []string{"c*"})
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — `undefined: bsonToMap`, `undefined: buildSelect`

- [ ] **Step 3: Write the implementation**

`store.go` exactly as in the Interfaces block above, plus:

```go
var (
	errNotFound  = errors.New("record not found")
	errAmbiguous = errors.New("key matched more than one record")
)
```

`lookup_mongo.go`:

```go
package main

import (
	"context"
	"fmt"
	"sort"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoStore struct {
	db *mongo.Database
}

func newMongoStore(db *mongo.Database) *mongoStore { return &mongoStore{db: db} }

func (s *mongoStore) FindByID(ctx context.Context, collection, id string) (map[string]any, error) {
	var doc bson.M
	err := s.db.Collection(collection).FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, errNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find %s by id: %w", collection, err)
	}
	return bsonToMap(doc), nil
}

func (s *mongoStore) FindOne(ctx context.Context, collection string, key map[string]any, fields []string) (map[string]any, error) {
	filter := bson.M{}
	for k, v := range key {
		filter[k] = v
	}
	proj := bson.M{}
	for _, f := range fields {
		proj[f] = 1
	}
	// Limit 2 so a non-unique key is detected as ambiguity, not silently
	// verified against an arbitrary doc.
	cur, err := s.db.Collection(collection).Find(ctx, filter,
		options.Find().SetProjection(proj).SetLimit(2))
	if err != nil {
		return nil, fmt.Errorf("find %s by key: %w", collection, err)
	}
	var docs []bson.M
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("decode %s find result: %w", collection, err)
	}
	switch len(docs) {
	case 0:
		return nil, errNotFound
	case 1:
		return bsonToMap(docs[0]), nil
	default:
		return nil, errAmbiguous
	}
}

// bsonToMap converts driver types into the plain map/slice/scalar forms the
// compare layer understands.
func bsonToMap(m bson.M) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = bsonValue(v)
	}
	return out
}

func bsonValue(v any) any {
	switch t := v.(type) {
	case bson.M:
		return bsonToMap(t)
	case bson.D:
		sub := bson.M{}
		for _, e := range t {
			sub[e.Key] = e.Value
		}
		return bsonToMap(sub)
	case bson.A:
		arr := make([]any, len(t))
		for i, e := range t {
			arr[i] = bsonValue(e)
		}
		return arr
	case bson.DateTime:
		return t.Time().UTC()
	default:
		return v
	}
}

// sortedKeys gives deterministic iteration for query building and logs.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
```

`lookup_cassandra.go`:

```go
package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/gocql/gocql"
)

type cassStore struct {
	session *gocql.Session
}

func newCassStore(session *gocql.Session) *cassStore { return &cassStore{session: session} }

var cqlIdent = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// buildSelect assembles a point-select. Identifiers come from the validated
// mapping file, but are re-checked here; values are always bound parameters.
func buildSelect(table string, key map[string]any, cols []string) (string, []any, error) {
	if !cqlIdent.MatchString(table) {
		return "", nil, fmt.Errorf("invalid table identifier %q", table)
	}
	for _, c := range cols {
		if !cqlIdent.MatchString(c) {
			return "", nil, fmt.Errorf("invalid column identifier %q", c)
		}
	}
	var conds []string
	var args []any
	for _, k := range sortedKeys(key) {
		if !cqlIdent.MatchString(k) {
			return "", nil, fmt.Errorf("invalid key column identifier %q", k)
		}
		conds = append(conds, k+" = ?")
		args = append(args, key[k])
	}
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s", //nolint:gocritic
		strings.Join(cols, ", "), table, strings.Join(conds, " AND "))
	return q, args, nil
}

func (s *cassStore) SelectOne(ctx context.Context, table string, key map[string]any, cols []string) (map[string]any, error) {
	q, args, err := buildSelect(table, key, cols)
	if err != nil {
		return nil, fmt.Errorf("build select: %w", err)
	}
	// #nosec G201 -- identifiers validated against ^[a-zA-Z0-9_]+$ in buildSelect; values are bound parameters
	iter := s.session.Query(q, args...).WithContext(ctx).Iter()
	var rows []map[string]any
	for {
		row := map[string]any{}
		if !iter.MapScan(row) {
			break
		}
		rows = append(rows, row)
		if len(rows) > 1 {
			break
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select from %s: %w", table, err)
	}
	switch len(rows) {
	case 0:
		return nil, errNotFound
	case 1:
		return rows[0], nil
	default:
		return nil, errAmbiguous
	}
}
```

- [ ] **Step 4: Generate mocks, run tests**

Run: `make generate SERVICE=tools/cdc-verify && make test SERVICE=tools/cdc-verify`
Expected: PASS; `mock_store_test.go` now exists with `MockSourceStore`, `MockTargetStore`, `MockCassStore`.

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/store.go tools/cdc-verify/lookup_mongo.go tools/cdc-verify/lookup_cassandra.go tools/cdc-verify/lookup_mongo_test.go tools/cdc-verify/mock_store_test.go
git commit -m "feat(cdc-verify): lookup interfaces with mongo and cassandra implementations"
```

---

### Task 8: Results store (ring buffer + failures + counters)

**Files:**
- Create: `tools/cdc-verify/results.go`
- Test: `tools/cdc-verify/results_test.go`

**Interfaces:**
- Produces:

```go
type CheckState string

const (
	StatePending    CheckState = "pending"
	StateMatched    CheckState = "matched"
	StateFailed     CheckState = "failed"
	StateSkipped    CheckState = "skipped"
	StateSuperseded CheckState = "superseded"
)

// TargetResult is one sub-check's live state.
type TargetResult struct {
	Alias     string      `json:"alias"`
	Matched   bool        `json:"matched"`
	LastCause string      `json:"lastCause,omitempty"` // "", "mismatch", "dest-missing", "resolver-miss: u", "ambiguous-key", "lookup-error: ...", "source-missing"
	Diffs     []FieldDiff `json:"diffs,omitempty"`     // populated on final failure only
}

// CheckResult is one table row. A copy is what leaves the store — callers
// never see the live pointer.
type CheckResult struct {
	ID          string         `json:"id"` // idgen.GenerateID()
	Collection  string         `json:"collection"`
	Op          string         `json:"op"`
	DocID       string         `json:"docId"`
	State       CheckState     `json:"state"`
	SkipReason  string         `json:"skipReason,omitempty"`
	Targets     []TargetResult `json:"targets,omitempty"`
	Attempts    int            `json:"attempts"`
	StartedAtMs int64          `json:"startedAtMs"`
	EndedAtMs   int64          `json:"endedAtMs,omitempty"`
}

type Counters struct {
	Checked    uint64 `json:"checked"`
	Matched    uint64 `json:"matched"`
	Failed     uint64 `json:"failed"`
	Skipped    uint64 `json:"skipped"`
	Superseded uint64 `json:"superseded"`
	Evicted    uint64 `json:"evicted"` // failures dropped by FAILED_CAP
}

func newResultsStore(recentCap, failedCap int, onUpdate func(CheckResult)) *resultsStore
func (s *resultsStore) Upsert(r CheckResult)         // insert or update by ID; terminal states bump counters once
func (s *resultsStore) Recent() []CheckResult        // newest first, <= recentCap
func (s *resultsStore) Failures() []CheckResult      // newest first, <= failedCap
func (s *resultsStore) Snapshot() (recent, failures []CheckResult, c Counters)
```

- `onUpdate` is the SSE bridge (Task 12 passes `hub.broadcastResult`); called outside the lock with a copy.
- Ring semantics: `Recent()` holds every state; when over cap, oldest evicted. A row reaching `StateFailed` is **also** appended to failures (independent list, own cap + `Evicted` counter). Counter bump happens exactly once per check ID (track terminal IDs in a small set that is pruned when the row leaves both lists).

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mkResult(id string, st CheckState) CheckResult {
	return CheckResult{ID: id, Collection: "c", Op: "insert", DocID: "d" + id, State: st}
}

func TestResultsStore_UpsertAndRecentOrder(t *testing.T) {
	s := newResultsStore(3, 10, nil)
	s.Upsert(mkResult("a", StatePending))
	s.Upsert(mkResult("b", StatePending))
	got := s.Recent()
	require.Len(t, got, 2)
	assert.Equal(t, "b", got[0].ID) // newest first

	// update in place keeps position, changes state
	s.Upsert(mkResult("a", StateMatched))
	got = s.Recent()
	require.Len(t, got, 2)
	assert.Equal(t, StateMatched, got[1].State)
}

func TestResultsStore_RecentCapEvicts(t *testing.T) {
	s := newResultsStore(2, 10, nil)
	for i := 0; i < 4; i++ {
		s.Upsert(mkResult(fmt.Sprint(i), StateMatched))
	}
	got := s.Recent()
	require.Len(t, got, 2)
	assert.Equal(t, "3", got[0].ID)
	assert.Equal(t, "2", got[1].ID)
}

func TestResultsStore_FailuresAndEviction(t *testing.T) {
	s := newResultsStore(10, 2, nil)
	s.Upsert(mkResult("x", StateFailed))
	s.Upsert(mkResult("y", StateFailed))
	s.Upsert(mkResult("z", StateFailed))
	f := s.Failures()
	require.Len(t, f, 2)
	assert.Equal(t, "z", f[0].ID)
	_, _, c := s.Snapshot()
	assert.Equal(t, uint64(3), c.Failed)
	assert.Equal(t, uint64(1), c.Evicted)
}

func TestResultsStore_CountersOncePerCheck(t *testing.T) {
	s := newResultsStore(10, 10, nil)
	r := mkResult("a", StatePending)
	s.Upsert(r)
	r.State = StateMatched
	s.Upsert(r)
	s.Upsert(r) // duplicate terminal upsert must not double-count
	_, _, c := s.Snapshot()
	assert.Equal(t, uint64(1), c.Checked)
	assert.Equal(t, uint64(1), c.Matched)
}

func TestResultsStore_OnUpdateFires(t *testing.T) {
	var events []CheckResult
	s := newResultsStore(10, 10, func(r CheckResult) { events = append(events, r) })
	s.Upsert(mkResult("a", StatePending))
	s.Upsert(mkResult("a", StateMatched))
	require.Len(t, events, 2)
	assert.Equal(t, StateMatched, events[1].State)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — `undefined: newResultsStore`

- [ ] **Step 3: Write the implementation**

`results.go` — key implementation notes (write it in full):

```go
package main

import "sync"

// ... type declarations exactly as the Interfaces block above ...

type resultsStore struct {
	mu        sync.Mutex
	recent    []CheckResult // newest at index 0
	failures  []CheckResult // newest at index 0
	counters  Counters
	counted   map[string]bool // check IDs whose terminal state was tallied
	recentCap int
	failedCap int
	onUpdate  func(CheckResult)
}

func newResultsStore(recentCap, failedCap int, onUpdate func(CheckResult)) *resultsStore {
	return &resultsStore{
		counted:   map[string]bool{},
		recentCap: recentCap,
		failedCap: failedCap,
		onUpdate:  onUpdate,
	}
}

func (s *resultsStore) Upsert(r CheckResult) {
	s.mu.Lock()
	idx := -1
	for i := range s.recent {
		if s.recent[i].ID == r.ID {
			idx = i
			break
		}
	}
	if idx >= 0 {
		s.recent[idx] = r
	} else {
		s.recent = append([]CheckResult{r}, s.recent...)
		if len(s.recent) > s.recentCap {
			evicted := s.recent[len(s.recent)-1]
			s.recent = s.recent[:s.recentCap]
			delete(s.counted, evictedIDIfUncounted(s, evicted))
		}
	}
	if isTerminal(r.State) && !s.counted[r.ID] {
		s.counted[r.ID] = true
		s.counters.Checked++
		switch r.State {
		case StateMatched:
			s.counters.Matched++
		case StateFailed:
			s.counters.Failed++
			s.failures = append([]CheckResult{r}, s.failures...)
			if len(s.failures) > s.failedCap {
				s.failures = s.failures[:s.failedCap]
				s.counters.Evicted++
			}
		case StateSkipped:
			s.counters.Skipped++
		case StateSuperseded:
			s.counters.Superseded++
		}
	}
	cb := s.onUpdate
	s.mu.Unlock()
	if cb != nil {
		cb(r)
	}
}

func isTerminal(st CheckState) bool {
	return st == StateMatched || st == StateFailed || st == StateSkipped || st == StateSuperseded
}

// evictedIDIfUncounted lets the counted set shrink with the window: an ID
// evicted from recent that is also gone from failures can never be upserted
// again (checks are single-writer), so its dedup entry is dropped.
func evictedIDIfUncounted(s *resultsStore, r CheckResult) string {
	for i := range s.failures {
		if s.failures[i].ID == r.ID {
			return "" // still referenced; deleting "" is a no-op
		}
	}
	return r.ID
}

func (s *resultsStore) Recent() []CheckResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]CheckResult, len(s.recent))
	copy(out, s.recent)
	return out
}

func (s *resultsStore) Failures() []CheckResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]CheckResult, len(s.failures))
	copy(out, s.failures)
	return out
}

func (s *resultsStore) Snapshot() ([]CheckResult, []CheckResult, Counters) {
	s.mu.Lock()
	defer s.mu.Unlock()
	recent := make([]CheckResult, len(s.recent))
	copy(recent, s.recent)
	failures := make([]CheckResult, len(s.failures))
	copy(failures, s.failures)
	return recent, failures, s.counters
}
```

(Also paste the full type declarations from the Interfaces block at the top of the file.)

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/cdc-verify`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/results.go tools/cdc-verify/results_test.go
git commit -m "feat(cdc-verify): capped results store with failure list and counters"
```

---

### Task 9: Verifier engine (fan-out, poll, freeze, supersede, sampling)

**Files:**
- Create: `tools/cdc-verify/verifier.go`
- Test: `tools/cdc-verify/verifier_test.go` (uses `MockSourceStore`/`MockTargetStore`/`MockCassStore` from Task 7)

**Interfaces:**
- Produces:

```go
type verifierConfig struct {
	Poll          time.Duration
	Timeout       time.Duration
	MaxChecks     int
	SamplePercent int
}

func newVerifier(m *Mapping, src SourceStore, tgt TargetStore, cass CassStore,
	reg transformRegistry, results *resultsStore, cfg verifierConfig) *verifier

func (v *verifier) Submit(ev CDCEvent)      // non-blocking classify + spawn
func (v *verifier) Shutdown(ctx context.Context) // cancel pollers, wait for goroutines (bounded by ctx)
```

- Internals (all in `verifier.go`):
  - `now func() time.Time`, `sleep func(ctx context.Context, d time.Duration) bool` and `sampleFn func() int` fields — production defaults (`time.Now`, timer-based sleep, `rand.IntN(100)`); tests override directly (fields are unexported, tests live in `package main`).
  - `compile(src *SourceMapping) map[string][]fieldPair` — done once per source at `newVerifier`: fold `Fields` (each DestRef → `fieldPair{SourcePaths: [sourcePath], DestField, Transform, Required}`) and `Derived` (one fieldPair per Dest with `SourcePaths: From`) grouped by target alias. Also precompute per-target dest column/field list = key columns + compared fields (the Cassandra `cols` / Mongo projection input). Verbatim targets get `pairs=nil`.
  - `pendingByKey map[string]string` (`collection+"\x00"+docID` → check ID) + mutex: supersede on collision — the old check's context is cancelled; its goroutine marks its row `StateSuperseded`.
  - Semaphore `chan struct{}` of `MaxChecks`; `sync.WaitGroup` for Shutdown.
- **Check algorithm** (one goroutine per accepted event):

```
action := mapping.Action(op); if unmapped collection or action==skip -> row StateSkipped (reason "unmapped" / "op-skip"), return
if sampleFn() >= SamplePercent -> row StateSkipped (reason "sampled-out"), return
register pendingByKey (cancel+supersede any previous check on the key)
row StatePending, targets initialised (Matched=false)
deadline := now()+Timeout
loop:
  attempt++
  srcDoc, err := src.FindByID(collection, docID)        // skip when action==verify-absent (no source doc exists)
  if errNotFound && action==verify:                      // doc deleted between event and check
      allTargets cause="source-missing"; goto sleep      // keeps polling; a later delete event will supersede
  resolve resolvers (point lookups, cache per attempt); a miss -> dependent targets cause="resolver-miss: <alias>"
  for each unfrozen target:
      key, err := buildKey(target, srcDoc, resolved)     // KeyFrom eval via getPath + registry.apply
      rec, err := lookupDest(target, key)                // TargetStore.FindOne / CassStore.SelectOne
      verify-absent: errNotFound -> target.Matched=true; found -> cause="still-present"
      verify: errNotFound -> cause="dest-missing"; errAmbiguous -> FAIL NOW whole check (cause "ambiguous-key", no more polling); other err -> cause="lookup-error: <err>"
      found:  diffs := diffFields(srcDoc, rec, pairs, reg) or diffVerbatim(srcDoc, rec, ignore)
              empty -> target.Matched=true (frozen); else cause="mismatch" (diffs kept only in memory for final report)
  if all targets Matched -> row StateMatched (EndedAtMs), unregister, return
  Upsert(row) // live partial progress for the UI
  if now() >= deadline -> row StateFailed with per-target Diffs+LastCause, unregister, return
  if !sleep(ctx, Poll) -> ctx cancelled: superseded (row StateSuperseded) or shutdown (leave last state), unregister, return
```

- [ ] **Step 1: Write the failing tests** — cover the lifecycle table from spec §5.2. Use a mapping fixture with two targets (one mongo `fields` target, one cassandra target), instant `sleep` override that counts polls, and `now` driven by a fake clock advancing per sleep.

```go
package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

func verifierMapping() *Mapping {
	return &Mapping{Sources: []SourceMapping{{
		Collection: "rocketchat_message",
		Ops:        map[string]OpAction{"insert": OpVerify, "delete": OpVerifyAbsent},
		Targets: map[string]Target{
			"byId": {Kind: "cassandra", Table: "messages_by_id",
				Key: map[string]KeyFrom{"message_id": {From: []string{"_id"}}}},
			"room": {Kind: "mongo", Collection: "rooms",
				Key: map[string]KeyFrom{"_id": {From: []string{"rid"}}}},
		},
		Fields: map[string][]DestRef{
			"msg": {{Dest: "byId.body"}},
			"rid": {{Dest: "room._id"}},
		},
	}}}
}

// testVerifier wires mocks and a deterministic clock. Each sleep advances the
// clock by poll; timeout after N polls is then exact.
func testVerifier(t *testing.T, cfg verifierConfig) (*verifier, *MockSourceStore, *MockTargetStore, *MockCassStore, *resultsStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	src := NewMockSourceStore(ctrl)
	tgt := NewMockTargetStore(ctrl)
	cass := NewMockCassStore(ctrl)
	results := newResultsStore(100, 100, nil)
	v := newVerifier(verifierMapping(), src, tgt, cass,
		newTransformRegistry(msgbucket.New(72*time.Hour)), results, cfg)
	var mu sync.Mutex
	fake := time.Unix(0, 0)
	v.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return fake }
	v.sleep = func(ctx context.Context, d time.Duration) bool {
		mu.Lock()
		fake = fake.Add(d)
		mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	v.sampleFn = func() int { return 0 } // always sampled in
	return v, src, tgt, cass, results
}

func waitState(t *testing.T, results *resultsStore, docID string, want CheckState) CheckResult {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range results.Recent() {
			if r.DocID == docID && r.State == want {
				return r
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no result for %s in state %s; have %+v", docID, want, results.Recent())
	return CheckResult{}
}

func srcDoc() map[string]any {
	return map[string]any{"_id": "m1", "msg": "hi", "rid": "r1"}
}

func TestVerifier_MatchFirstAttempt(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 10 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), "rocketchat_message", "m1").Return(srcDoc(), nil).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), "messages_by_id", map[string]any{"message_id": "m1"}, gomock.Any()).
		Return(map[string]any{"body": "hi"}, nil)
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", map[string]any{"_id": "r1"}, gomock.Any()).
		Return(map[string]any{"_id": "r1"}, nil)

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateMatched)
	assert.Equal(t, 1, r.Attempts)
	for _, tr := range r.Targets {
		assert.True(t, tr.Matched)
	}
}

func TestVerifier_ConvergesAfterDelay(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 10 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", gomock.Any(), gomock.Any()).
		Return(map[string]any{"_id": "r1"}, nil).AnyTimes()
	first := cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errNotFound)
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]any{"body": "hi"}, nil).After(first)

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateMatched)
	assert.Equal(t, 2, r.Attempts)
}

func TestVerifier_FrozenTargetNotRechecked(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 10 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	// room target matches on attempt 1 and MUST NOT be looked up again
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", gomock.Any(), gomock.Any()).
		Return(map[string]any{"_id": "r1"}, nil).Times(1)
	first := cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errNotFound)
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]any{"body": "hi"}, nil).After(first)

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	waitState(t, results, "m1", StateMatched)
}

func TestVerifier_TimeoutFailsWithDiffs(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 3 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", gomock.Any(), gomock.Any()).
		Return(map[string]any{"_id": "r1"}, nil).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]any{"body": "WRONG"}, nil).AnyTimes()

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateFailed)
	require.Len(t, r.Targets, 2)
	for _, tr := range r.Targets {
		if tr.Alias == "byId" {
			assert.Equal(t, "mismatch", tr.LastCause)
			require.Len(t, tr.Diffs, 1)
			assert.Equal(t, "hi", tr.Diffs[0].Want)
			assert.Equal(t, "WRONG", tr.Diffs[0].Got)
		}
	}
	assert.Len(t, results.Failures(), 1)
}

func TestVerifier_AmbiguousKeyFailsImmediately(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 60 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", gomock.Any(), gomock.Any()).Return(nil, errAmbiguous).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]any{"body": "hi"}, nil).AnyTimes()

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateFailed)
	assert.Equal(t, 1, r.Attempts) // no polling loop on ambiguity
	for _, tr := range r.Targets {
		if tr.Alias == "room" {
			assert.Equal(t, "ambiguous-key", tr.LastCause)
		}
	}
}

func TestVerifier_VerifyAbsentDelete(t *testing.T) {
	v, _, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 10 * time.Second, MaxChecks: 4, SamplePercent: 100})
	// delete: no source lookup; dest keys derive from documentKey only (_id)
	cass.EXPECT().SelectOne(gomock.Any(), "messages_by_id", map[string]any{"message_id": "m1"}, gomock.Any()).
		Return(nil, errNotFound)
	_ = tgt // room target key needs rid from source doc -> unresolvable on delete -> counted matched-by-absence? No:
	// see implementation note below — targets whose key cannot be built from
	// documentKey alone on verify-absent are reported cause="key-unresolvable"
	// and the check fails at deadline unless all resolvable targets are absent.
	// For this fixture the room key (rid) is unresolvable; the test asserts the
	// byId target matched and room carries key-unresolvable, final state failed.
	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "delete", DocID: "m1"})
	r := waitState(t, results, "m1", StateFailed)
	for _, tr := range r.Targets {
		switch tr.Alias {
		case "byId":
			assert.True(t, tr.Matched)
		case "room":
			assert.Equal(t, "key-unresolvable", tr.LastCause)
		}
	}
}

func TestVerifier_SkipClassification(t *testing.T) {
	v, _, _, _, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: time.Second, MaxChecks: 4, SamplePercent: 100})
	v.Submit(CDCEvent{Collection: "unmapped_coll", Op: "insert", DocID: "x"})
	r := waitState(t, results, "x", StateSkipped)
	assert.Equal(t, "unmapped", r.SkipReason)

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "update", DocID: "y"}) // op not in Ops map
	r = waitState(t, results, "y", StateSkipped)
	assert.Equal(t, "op-skip", r.SkipReason)
}

func TestVerifier_SampledOut(t *testing.T) {
	v, _, _, _, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: time.Second, MaxChecks: 4, SamplePercent: 50})
	v.sampleFn = func() int { return 99 } // above 50 -> sampled out
	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateSkipped)
	assert.Equal(t, "sampled-out", r.SkipReason)
}

func TestVerifier_Supersede(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 60 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errNotFound).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errNotFound).AnyTimes()

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	waitState(t, results, "m1", StatePending)
	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "update", DocID: "m1"})
	// old check flips to superseded; new check is pending on the same key
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var superseded, pending bool
		for _, r := range results.Recent() {
			if r.DocID == "m1" && r.State == StateSuperseded {
				superseded = true
			}
			if r.DocID == "m1" && r.State == StatePending && r.Op == "update" {
				pending = true
			}
		}
		if superseded && pending {
			v.Shutdown(context.Background())
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("supersede did not happen")
}

func TestVerifier_ResolverMissKeepsPolling(t *testing.T) {
	m := verifierMapping()
	m.Sources[0].Resolvers = map[string]Resolver{
		"user": {DB: "source", Collection: "users",
			Key: map[string]KeyFrom{"_id": {From: []string{"u._id"}}}, Fields: []string{"username"}},
	}
	m.Sources[0].Targets["room"] = Target{Kind: "mongo", Collection: "rooms",
		Key: map[string]KeyFrom{"_id": {From: []string{"@user.username"}}}}

	ctrl := gomock.NewController(t)
	src := NewMockSourceStore(ctrl)
	tgt := NewMockTargetStore(ctrl)
	cass := NewMockCassStore(ctrl)
	results := newResultsStore(100, 100, nil)
	v := newVerifier(m, src, tgt, cass, newTransformRegistry(msgbucket.New(72*time.Hour)), results,
		verifierConfig{Poll: time.Second, Timeout: 2 * time.Second, MaxChecks: 4, SamplePercent: 100})
	var mu sync.Mutex
	fake := time.Unix(0, 0)
	v.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return fake }
	v.sleep = func(ctx context.Context, d time.Duration) bool { mu.Lock(); fake = fake.Add(d); mu.Unlock(); return true }
	v.sampleFn = func() int { return 0 }

	doc := srcDoc()
	doc["u"] = map[string]any{"_id": "u1"}
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(doc, nil).AnyTimes()
	src.EXPECT().FindOne(gomock.Any(), "users", map[string]any{"_id": "u1"}, []string{"username"}).
		Return(nil, errNotFound).AnyTimes() // resolver never resolves
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]any{"body": "hi"}, nil).AnyTimes()

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateFailed)
	assert.Greater(t, r.Attempts, 1) // it kept polling, then failed at deadline
	for _, tr := range r.Targets {
		if tr.Alias == "room" {
			assert.Equal(t, "resolver-miss: user", tr.LastCause)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — `undefined: newVerifier`

- [ ] **Step 3: Write the implementation**

`verifier.go` implementing the algorithm block above. Structure:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
)

type verifierConfig struct {
	Poll          time.Duration
	Timeout       time.Duration
	MaxChecks     int
	SamplePercent int
}

// compiledSource is the mapping pre-folded for the hot path.
type compiledSource struct {
	src       *SourceMapping
	pairs     map[string][]fieldPair // target alias -> compared fields
	destCols  map[string][]string    // target alias -> dest columns to fetch (key cols + compared fields; nil => full doc for verbatim)
}

type verifier struct {
	compiled map[string]compiledSource // by source collection
	source   SourceStore
	target   TargetStore
	cass     CassStore
	reg      transformRegistry
	results  *resultsStore
	cfg      verifierConfig

	sem      chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex
	pending  map[string]context.CancelFunc // collection+"\x00"+docID -> cancel of the active check
	baseCtx  context.Context
	baseStop context.CancelFunc

	now      func() time.Time
	sleep    func(ctx context.Context, d time.Duration) bool // false => ctx done
	sampleFn func() int
}
```

Key implementation points (write the full file):
- `newVerifier` compiles every source (fold Fields+Derived into `pairs`; `destCols[alias]` = sorted union of key column names and pair DestFields' first segments for cassandra / full field paths for mongo projection; verbatim → nil meaning "fetch whole doc"), builds `sem`, `pending`, `baseCtx` from `context.Background()`, and production `now`/`sleep`/`sampleFn` (`rand.IntN(100)`).
- `Submit` classifies synchronously (skip rows don't consume a semaphore slot; their row is `Upsert`ed directly with `StartedAtMs=EndedAtMs=now()`); verify rows acquire the semaphore inside the spawned goroutine (`wg.Add` first).
- Supersede: `registerPending` cancels and replaces any existing entry; the cancelled goroutine observes `ctx.Err()` and, if its own entry was replaced (compare check IDs), writes `StateSuperseded`.
- `runCheck` follows the algorithm block verbatim, with helpers:
  - `resolveAll(ctx, cs, srcDoc) (map[string]map[string]any, map[string]string)` — resolver alias → resolved doc, alias → miss/error cause.
  - `buildKey(t Target, srcDoc map[string]any, resolved map[string]map[string]any, reg transformRegistry) (map[string]any, error)` — evaluates each `KeyFrom`: paths starting `@alias.` read from resolved docs, otherwise `getPath(srcDoc, path)`; missing value → error `key-unresolvable`; then `reg.apply(k.Transform, args)`.
  - `lookupDest(ctx, t Target, key map[string]any, cols []string)` — routes mongo/cassandra stores.
- `Shutdown` cancels `baseCtx` and waits `wg` (select with ctx deadline).
- On `verify-absent`, `srcDoc` is `map[string]any{"_id": docID}` — keys derivable from `_id` alone work (byId), others report `key-unresolvable`.
- Row updates always go through `results.Upsert` with a fully copied `CheckResult` (targets slice copied) — the store is the only shared state.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/cdc-verify` (race detector is on via the Makefile)
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/verifier.go tools/cdc-verify/verifier_test.go
git commit -m "feat(cdc-verify): verifier engine with fan-out sub-checks, freeze, supersede"
```

---

### Task 10: Stats poller

**Files:**
- Create: `tools/cdc-verify/stats.go`
- Test: `tools/cdc-verify/stats_test.go`

**Interfaces:**
- Produces:

```go
// StreamStats is one stats tick, broadcast to the UI.
type StreamStats struct {
	Stream      string            `json:"stream"`
	Msgs        uint64            `json:"msgs"`
	Bytes       uint64            `json:"bytes"`
	FirstSeq    uint64            `json:"firstSeq"`
	LastSeq     uint64            `json:"lastSeq"`
	PerSubject  map[string]uint64 `json:"perSubject"`  // full subject -> count
	RatePerSec  float64           `json:"ratePerSec"`  // delta(LastSeq)/delta(t), sliding window
	Consumers   []ConsumerLag     `json:"consumers"`
	WatcherLive bool              `json:"watcherLive"`
	TakenAtMs   int64             `json:"takenAtMs"`
	Error       string            `json:"error,omitempty"` // poll failure, shown in UI
}

type ConsumerLag struct {
	Name       string `json:"name"`
	NumPending uint64 `json:"numPending"`
	AckPending int    `json:"ackPending"`
	Error      string `json:"error,omitempty"`
}

// streamInfoFn abstracts the JetStream calls for unit tests.
type streamInfoFn func(ctx context.Context) (*jetstream.StreamInfo, error)
type consumerInfoFn func(ctx context.Context, name string) (*jetstream.ConsumerInfo, error)

func newStatsPoller(stream string, si streamInfoFn, ci consumerInfoFn,
	trackConsumers []string, interval time.Duration,
	watcherLive func() bool, onTick func(StreamStats)) *statsPoller
func (p *statsPoller) Run(ctx context.Context)   // blocking loop; returns on ctx cancel
func (p *statsPoller) Last() StreamStats          // latest tick (for /api/state)
```

- Rate math: keep a ring of the last 13 `(takenAt, lastSeq)` samples (~1m at 5s interval); `RatePerSec = (newest.lastSeq - oldest.lastSeq) / (newest.t - oldest.t).Seconds()`; fewer than 2 samples → 0. Sequence going *down* (stream purge/recreate) resets the window.
- Per-subject counts come from `StreamInfo` requested with `jetstream.WithSubjectFilter("chat.migration.oplog." + siteID + ".>")` — the poller only gets the pre-built `streamInfoFn` closure, wiring happens in Task 13:
  `func(ctx context.Context) (*jetstream.StreamInfo, error) { return s.Info(ctx, jetstream.WithSubjectFilter(filter)) }`.
- A failed stream poll produces a tick with `Error` set (UI shows staleness) — never kills the loop.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fakeStreamInfo(msgs, lastSeq uint64, subjects map[string]uint64) *jetstream.StreamInfo {
	return &jetstream.StreamInfo{State: jetstream.StreamState{
		Msgs: msgs, Bytes: msgs * 100, FirstSeq: 1, LastSeq: lastSeq, Subjects: subjects,
	}}
}

func TestStatsPoller_TickAndRate(t *testing.T) {
	seq := uint64(100)
	si := func(ctx context.Context) (*jetstream.StreamInfo, error) {
		return fakeStreamInfo(seq, seq, map[string]uint64{"chat.migration.oplog.s1.c.insert": seq}), nil
	}
	ci := func(ctx context.Context, name string) (*jetstream.ConsumerInfo, error) {
		return &jetstream.ConsumerInfo{NumPending: 7, NumAckPending: 2}, nil
	}
	var ticks []StreamStats
	p := newStatsPoller("MIGRATION-OPLOG-s1", si, ci, []string{"transformer"}, time.Millisecond,
		func() bool { return true }, func(s StreamStats) { ticks = append(ticks, s) })

	// drive three polls manually via the exported-for-test pollOnce
	p.pollOnce(context.Background(), time.Unix(0, 0))
	seq = 200
	p.pollOnce(context.Background(), time.Unix(10, 0))
	seq = 300
	p.pollOnce(context.Background(), time.Unix(20, 0))

	require.Len(t, ticks, 3)
	assert.Equal(t, float64(0), ticks[0].RatePerSec)
	assert.InDelta(t, 10.0, ticks[2].RatePerSec, 0.001) // (300-100)/20s
	assert.Equal(t, uint64(300), ticks[2].Msgs)
	require.Len(t, ticks[2].Consumers, 1)
	assert.Equal(t, uint64(7), ticks[2].Consumers[0].NumPending)
	assert.True(t, ticks[2].WatcherLive)
	assert.Equal(t, ticks[2], p.Last())
}

func TestStatsPoller_PollErrorKeepsGoing(t *testing.T) {
	calls := 0
	si := func(ctx context.Context) (*jetstream.StreamInfo, error) {
		calls++
		if calls == 1 {
			return nil, fmt.Errorf("nats down")
		}
		return fakeStreamInfo(1, 1, nil), nil
	}
	var ticks []StreamStats
	p := newStatsPoller("S", si, nil, nil, time.Millisecond, func() bool { return false },
		func(s StreamStats) { ticks = append(ticks, s) })
	p.pollOnce(context.Background(), time.Unix(0, 0))
	p.pollOnce(context.Background(), time.Unix(5, 0))
	require.Len(t, ticks, 2)
	assert.Contains(t, ticks[0].Error, "nats down")
	assert.Empty(t, ticks[1].Error)
}

func TestStatsPoller_SequenceResetClearsWindow(t *testing.T) {
	seq := uint64(1000)
	si := func(ctx context.Context) (*jetstream.StreamInfo, error) {
		return fakeStreamInfo(seq, seq, nil), nil
	}
	var last StreamStats
	p := newStatsPoller("S", si, nil, nil, time.Millisecond, func() bool { return true },
		func(s StreamStats) { last = s })
	p.pollOnce(context.Background(), time.Unix(0, 0))
	seq = 5 // purge
	p.pollOnce(context.Background(), time.Unix(10, 0))
	assert.Equal(t, float64(0), last.RatePerSec)
}

func TestStatsPoller_ConsumerError(t *testing.T) {
	si := func(ctx context.Context) (*jetstream.StreamInfo, error) { return fakeStreamInfo(1, 1, nil), nil }
	ci := func(ctx context.Context, name string) (*jetstream.ConsumerInfo, error) {
		return nil, fmt.Errorf("no such consumer")
	}
	var last StreamStats
	p := newStatsPoller("S", si, ci, []string{"ghost"}, time.Millisecond, func() bool { return true },
		func(s StreamStats) { last = s })
	p.pollOnce(context.Background(), time.Unix(0, 0))
	require.Len(t, last.Consumers, 1)
	assert.Contains(t, last.Consumers[0].Error, "no such consumer")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — `undefined: newStatsPoller`

- [ ] **Step 3: Write the implementation**

`stats.go`:

```go
package main

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// (StreamStats / ConsumerLag / streamInfoFn / consumerInfoFn as in the
// Interfaces block above.)

type seqSample struct {
	t   time.Time
	seq uint64
}

type statsPoller struct {
	stream         string
	si             streamInfoFn
	ci             consumerInfoFn
	trackConsumers []string
	interval       time.Duration
	watcherLive    func() bool
	onTick         func(StreamStats)

	mu      sync.Mutex
	window  []seqSample // ring, oldest first, max rateWindowSamples
	last    StreamStats
}

const rateWindowSamples = 13

func newStatsPoller(stream string, si streamInfoFn, ci consumerInfoFn,
	trackConsumers []string, interval time.Duration,
	watcherLive func() bool, onTick func(StreamStats)) *statsPoller {
	return &statsPoller{stream: stream, si: si, ci: ci, trackConsumers: trackConsumers,
		interval: interval, watcherLive: watcherLive, onTick: onTick}
}

func (p *statsPoller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	p.pollOnce(ctx, time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			p.pollOnce(ctx, now)
		}
	}
}

func (p *statsPoller) pollOnce(ctx context.Context, now time.Time) {
	stats := StreamStats{Stream: p.stream, TakenAtMs: now.UnixMilli(), WatcherLive: p.watcherLive()}

	info, err := p.si(ctx)
	if err != nil {
		stats.Error = err.Error()
	} else {
		st := info.State
		stats.Msgs, stats.Bytes = st.Msgs, st.Bytes
		stats.FirstSeq, stats.LastSeq = st.FirstSeq, st.LastSeq
		stats.PerSubject = st.Subjects
		stats.RatePerSec = p.pushAndRate(now, st.LastSeq)
	}

	for _, name := range p.trackConsumers {
		lag := ConsumerLag{Name: name}
		if p.ci == nil {
			continue
		}
		if cinfo, cerr := p.ci(ctx, name); cerr != nil {
			lag.Error = cerr.Error()
		} else {
			lag.NumPending = cinfo.NumPending
			lag.AckPending = cinfo.NumAckPending
		}
		stats.Consumers = append(stats.Consumers, lag)
	}

	p.mu.Lock()
	p.last = stats
	p.mu.Unlock()
	if p.onTick != nil {
		p.onTick(stats)
	}
}

// pushAndRate appends a sample and returns the sliding-window rate. A
// sequence that went backwards (stream purge) resets the window.
func (p *statsPoller) pushAndRate(now time.Time, lastSeq uint64) float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.window); n > 0 && lastSeq < p.window[n-1].seq {
		p.window = nil
	}
	p.window = append(p.window, seqSample{t: now, seq: lastSeq})
	if len(p.window) > rateWindowSamples {
		p.window = p.window[1:]
	}
	if len(p.window) < 2 {
		return 0
	}
	oldest, newest := p.window[0], p.window[len(p.window)-1]
	secs := newest.t.Sub(oldest.t).Seconds()
	if secs <= 0 {
		return 0
	}
	return float64(newest.seq-oldest.seq) / secs
}

func (p *statsPoller) Last() StreamStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/cdc-verify`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/stats.go tools/cdc-verify/stats_test.go
git commit -m "feat(cdc-verify): stream stats poller with sliding-window rate"
```

---

### Task 11: Watcher (ordered ephemeral consumer)

**Files:**
- Create: `tools/cdc-verify/watcher.go`
- Test: `tools/cdc-verify/watcher_test.go`

**Interfaces:**
- Produces:

```go
// submitter is what the watcher feeds — satisfied by *verifier.
type submitter interface {
	Submit(ev CDCEvent)
}

func newWatcher(js jetstream.JetStream, streamName string, startAt time.Time, sub submitter) *watcher
func (w *watcher) Run(ctx context.Context) error // create ordered consumer + Consume; blocks until ctx cancel
func (w *watcher) Live() bool                    // feeds StreamStats.WatcherLive
```

- Uses `js.OrderedConsumer(ctx, streamName, jetstream.OrderedConsumerConfig{DeliverPolicy: ..., OptStartTime: ...})`:
  zero `startAt` → `jetstream.DeliverNewPolicy`; non-zero → `jetstream.DeliverByStartTimePolicy` with `OptStartTime: &startAt`.
  Ordered consumers are ephemeral, auto-recreated on failure by the client library, and never ack — exactly the passive-observer semantics the spec requires (no acks that matter, no durable state).
- Each message: `decodeCDCEvent(msg.Data())`; decode failure → `slog.Warn("skip undecodable oplog event", "subject", msg.Subject(), "error", err)` (subject only — never the payload) + a `watcherDecodeErrors` counter exposed via `Live` struct? No — keep v1 minimal: warn log only.
- `Live()` = consumer created and `Consume` callback saw a message or heartbeat within `3 * statsInterval`? Simplest honest signal in v1: `Live()` is true once the ordered consumer is created and false after `Run` returns; message-recency is visible via the stream stats themselves.
- Unit test strategy: `watcher.Run` needs a real JetStream — that's Task 14. Unit-testable pieces: `deliverPolicy(startAt)` helper returning the config, and the message handler closure `handleMsg(data []byte, subject string, sub submitter)` extracted as a method taking raw bytes.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

type captureSubmitter struct{ events []CDCEvent }

func (c *captureSubmitter) Submit(ev CDCEvent) { c.events = append(c.events, ev) }

func TestDeliverPolicy(t *testing.T) {
	cfg := orderedConfig(time.Time{})
	assert.Equal(t, jetstream.DeliverNewPolicy, cfg.DeliverPolicy)
	assert.Nil(t, cfg.OptStartTime)

	at := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	cfg = orderedConfig(at)
	assert.Equal(t, jetstream.DeliverByStartTimePolicy, cfg.DeliverPolicy)
	assert.Equal(t, at, *cfg.OptStartTime)
}

func TestHandleMsg(t *testing.T) {
	sub := &captureSubmitter{}
	w := &watcher{sub: sub}

	w.handleMsg([]byte(`{"op":"insert","coll":"rocketchat_message","documentKey":{"_id":"m1"}}`), "chat.migration.oplog.s1.rocketchat_message.insert")
	assert.Len(t, sub.events, 1)
	assert.Equal(t, "m1", sub.events[0].DocID)

	w.handleMsg([]byte(`not json`), "chat.migration.oplog.s1.x.insert") // must not panic, not submit
	assert.Len(t, sub.events, 1)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — `undefined: orderedConfig`

- [ ] **Step 3: Write the implementation**

`watcher.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type submitter interface {
	Submit(ev CDCEvent)
}

type watcher struct {
	js      jetstream.JetStream
	stream  string
	startAt time.Time
	sub     submitter
	live    atomic.Bool
}

func newWatcher(js jetstream.JetStream, streamName string, startAt time.Time, sub submitter) *watcher {
	return &watcher{js: js, stream: streamName, startAt: startAt, sub: sub}
}

// orderedConfig maps the optional replay start onto the ordered-consumer
// deliver policy: zero time = live tail (new messages only).
func orderedConfig(startAt time.Time) jetstream.OrderedConsumerConfig {
	if startAt.IsZero() {
		return jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverNewPolicy}
	}
	return jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
		OptStartTime:  &startAt,
	}
}

func (w *watcher) Run(ctx context.Context) error {
	cons, err := w.js.OrderedConsumer(ctx, w.stream, orderedConfig(w.startAt))
	if err != nil {
		return fmt.Errorf("create ordered consumer on %s: %w", w.stream, err)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		w.handleMsg(msg.Data(), msg.Subject())
	})
	if err != nil {
		return fmt.Errorf("consume from %s: %w", w.stream, err)
	}
	w.live.Store(true)
	defer func() {
		w.live.Store(false)
		cc.Stop()
	}()
	<-ctx.Done()
	return nil
}

func (w *watcher) handleMsg(data []byte, subject string) {
	ev, err := decodeCDCEvent(data)
	if err != nil {
		// Subject only — the payload may hold user content and must not be logged.
		slog.Warn("skip undecodable oplog event", "subject", subject, "error", err)
		return
	}
	w.sub.Submit(ev)
}

func (w *watcher) Live() bool { return w.live.Load() }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/cdc-verify`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/watcher.go tools/cdc-verify/watcher_test.go
git commit -m "feat(cdc-verify): ordered ephemeral stream watcher"
```

---

### Task 12: SSE hub, HTTP handlers, static UI

**Files:**
- Create: `tools/cdc-verify/hub.go`, `tools/cdc-verify/handler.go`, `tools/cdc-verify/routes.go`, `tools/cdc-verify/static.go`, `tools/cdc-verify/static/index.html`
- Test: `tools/cdc-verify/handler_test.go`

**Interfaces:**
- Produces:

```go
// hub.go — one global broadcaster (no per-session state; every viewer sees
// the same pipeline).
type sseEvent struct {
	Kind string `json:"kind"` // "stats" | "result"
	Data any    `json:"data"` // StreamStats or CheckResult
}
func newHub() *hub
func (h *hub) register() (id string, ch <-chan sseEvent)
func (h *hub) unregister(id string)
func (h *hub) broadcastStats(s StreamStats)     // non-blocking; drops on full client buffer (cap 64)
func (h *hub) broadcastResult(r CheckResult)

// handler.go
type stateProvider interface {
	Snapshot() ([]CheckResult, []CheckResult, Counters)
}
type statsProvider interface {
	Last() StreamStats
}
func newHandler(hub *hub, results stateProvider, stats statsProvider) *handler
// GET /healthz            -> 200 {"status":"ok"}
// GET /api/state          -> {"stats":..., "recent":[...], "failures":[...], "counters":{...}} (initial page fill)
// GET /api/events         -> SSE stream of sseEvent (stats + result updates)
// GET /failures.json      -> download of the failures list
// GET /                   -> static/index.html (embedded)

// routes.go
func (h *handler) registerRoutes(mux *http.ServeMux)
```

- `hub` mirrors nats-debug's client-map + non-blocking send pattern (`select { case ch <- ev: default: }`), `idgen.GenerateID()` for client ids.
- SSE handler: `Content-Type: text/event-stream`, flush after each event, 15s keep-alive comment ticker (`: ping\n\n`), unregister on request-context done. (`http.NewServeMux` + `net/http` exactly like nats-debug — the tools/ precedent for operator UIs; not Gin.)
- `/failures.json` sets `Content-Disposition: attachment; filename=cdc-verify-failures.json`.
- `static.go`:

```go
package main

import "embed"

//go:embed static
var staticFS embed.FS
```

- `static/index.html` — single self-contained page, vanilla JS (no CDN — must work air-gapped), copy the layout idioms of `tools/nats-debug/static/`: dark theme, monospace tables. Three sections:
  1. header strip: stream name, msgs, rate/s, watcher-live dot, counters (checked/matched/failed/skipped/superseded/evicted), per-subject chips (`collection.op count`), consumer-lag badges (green `NumPending==0`, amber `<1000`, red otherwise), stats `Error` banner when set.
  2. **Recent verifications** table: Time | Collection | Op | Doc | State | Targets | Attempts | Duration. State badge colored (pending amber pulse, matched green, failed red, skipped grey, superseded grey-strike). Targets cell: one chip per TargetResult (`alias ✓` green / `alias …` amber with `lastCause` tooltip). Filter input matches collection/docId substring. Rows come from `/api/state` then live-update in place by `id` from SSE `result` events; table trimmed client-side to the server's recent length.
  3. **Failures** table: same columns plus expandable `<details>` per row rendering `targets[].diffs[]` as a `sourcePath → want / got / cause` sub-table; "Download JSON" button linking `/failures.json`.
  JS: `EventSource('/api/events')`; `kind==="stats"` re-renders the header; `kind==="result"` upserts the row map and re-renders whichever table(s) the state belongs to. Keep everything in one `<script>` block; no framework.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeState struct{}

func (fakeState) Snapshot() ([]CheckResult, []CheckResult, Counters) {
	return []CheckResult{{ID: "1", State: StateMatched}},
		[]CheckResult{{ID: "2", State: StateFailed}},
		Counters{Checked: 2, Matched: 1, Failed: 1}
}

type fakeStats struct{}

func (fakeStats) Last() StreamStats { return StreamStats{Stream: "S", Msgs: 42} }

func testServer(t *testing.T) (*httptest.Server, *hub) {
	t.Helper()
	h := newHub()
	handler := newHandler(h, fakeState{}, fakeStats{})
	mux := http.NewServeMux()
	handler.registerRoutes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, h
}

func TestHealthz(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAPIState(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/api/state")
	require.NoError(t, err)
	defer resp.Body.Close()
	var body struct {
		Stats    StreamStats   `json:"stats"`
		Recent   []CheckResult `json:"recent"`
		Failures []CheckResult `json:"failures"`
		Counters Counters      `json:"counters"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, uint64(42), body.Stats.Msgs)
	require.Len(t, body.Recent, 1)
	require.Len(t, body.Failures, 1)
	assert.Equal(t, uint64(2), body.Counters.Checked)
}

func TestFailuresDownload(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/failures.json")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Contains(t, resp.Header.Get("Content-Disposition"), "cdc-verify-failures.json")
	var failures []CheckResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&failures))
	require.Len(t, failures, 1)
	assert.Equal(t, "2", failures[0].ID)
}

func TestIndexServed(t *testing.T) {
	srv, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/html")
}

func TestSSEDeliversEvents(t *testing.T) {
	srv, h := testServer(t)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/events", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	// give the handler a beat to register, then broadcast
	deadline := time.Now().Add(time.Second)
	for h.clientCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.NotZero(t, h.clientCount())
	h.broadcastResult(CheckResult{ID: "r1", State: StateMatched})

	buf := make([]byte, 4096)
	n, err := resp.Body.Read(buf)
	require.NoError(t, err)
	chunk := string(buf[:n])
	assert.True(t, strings.Contains(chunk, `"kind":"result"`), "got: %s", chunk)
	assert.Contains(t, chunk, `"r1"`)
}

func TestHub_RegisterUnregisterAndDrop(t *testing.T) {
	h := newHub()
	id, ch := h.register()
	assert.Equal(t, 1, h.clientCount())
	h.broadcastStats(StreamStats{Msgs: 1})
	ev := <-ch
	assert.Equal(t, "stats", ev.Kind)

	// fill the buffer; further broadcasts must not block
	for i := 0; i < 100; i++ {
		h.broadcastStats(StreamStats{Msgs: uint64(i)})
	}
	h.unregister(id)
	assert.Equal(t, 0, h.clientCount())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/cdc-verify`
Expected: FAIL — `undefined: newHub`

- [ ] **Step 3: Write the implementation**

`hub.go`:

```go
package main

import (
	"sync"

	"github.com/hmchangw/chat/pkg/idgen"
)

type sseEvent struct {
	Kind string `json:"kind"`
	Data any    `json:"data"`
}

const clientBuffer = 64

type hub struct {
	mu      sync.RWMutex
	clients map[string]chan sseEvent
}

func newHub() *hub {
	return &hub{clients: make(map[string]chan sseEvent)}
}

func (h *hub) register() (string, <-chan sseEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := idgen.GenerateID()
	ch := make(chan sseEvent, clientBuffer)
	h.clients[id] = ch
	return id, ch
}

func (h *hub) unregister(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, id)
}

func (h *hub) clientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *hub) broadcastStats(s StreamStats)   { h.broadcast(sseEvent{Kind: "stats", Data: s}) }
func (h *hub) broadcastResult(r CheckResult)  { h.broadcast(sseEvent{Kind: "result", Data: r}) }

// broadcast never blocks: a slow viewer loses intermediate frames, and the
// next /api/state reload or SSE frame catches it up.
func (h *hub) broadcast(ev sseEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.clients {
		select {
		case ch <- ev:
		default:
		}
	}
}
```

`handler.go`:

```go
package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type stateProvider interface {
	Snapshot() ([]CheckResult, []CheckResult, Counters)
}

type statsProvider interface {
	Last() StreamStats
}

type handler struct {
	hub     *hub
	results stateProvider
	stats   statsProvider
}

func newHandler(hub *hub, results stateProvider, stats statsProvider) *handler {
	return &handler{hub: hub, results: results, stats: stats}
}

func (h *handler) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *handler) state(w http.ResponseWriter, _ *http.Request) {
	recent, failures, counters := h.results.Snapshot()
	writeJSON(w, map[string]any{
		"stats":    h.stats.Last(),
		"recent":   recent,
		"failures": failures,
		"counters": counters,
	})
}

func (h *handler) failuresJSON(w http.ResponseWriter, _ *http.Request) {
	_, failures, _ := h.results.Snapshot()
	w.Header().Set("Content-Disposition", `attachment; filename=cdc-verify-failures.json`)
	writeJSON(w, failures)
}

func (h *handler) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	id, ch := h.hub.register()
	defer h.hub.unregister(id)
	flusher.Flush()

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case ev := <-ch:
			b, err := json.Marshal(ev)
			if err != nil {
				slog.Error("marshal sse event", "error", err)
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write json response", "error", err)
	}
}
```

`routes.go`:

```go
package main

import (
	"io/fs"
	"net/http"
)

func (h *handler) registerRoutes(mux *http.ServeMux) {
	staticRoot, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err) // embedded FS layout is fixed at build time
	}
	mux.Handle("GET /", http.FileServer(http.FS(staticRoot)))
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /api/state", h.state)
	mux.HandleFunc("GET /api/events", h.events)
	mux.HandleFunc("GET /failures.json", h.failuresJSON)
}
```

`static/index.html`: build it per the section-3 description in this task's Interfaces block. Structural skeleton to implement (fill in the CSS matching nats-debug's dark/monospace look and the JS behaviors listed):

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>CDC Verify</title>
  <style>/* dark theme, monospace, badge + chip classes:
    .badge.pending .badge.matched .badge.failed .badge.skipped .badge.superseded
    .chip.ok .chip.wait  .lag.green .lag.amber .lag.red  */</style>
</head>
<body>
  <header id="stats"><!-- stream name, msgs, rate, watcher dot, counters, subject chips, lag badges, error banner --></header>
  <section>
    <h2>Recent verifications <input id="filter" placeholder="filter collection/doc"></h2>
    <table id="recent"><thead><tr>
      <th>Time</th><th>Collection</th><th>Op</th><th>Doc</th><th>State</th><th>Targets</th><th>Attempts</th><th>Duration</th>
    </tr></thead><tbody></tbody></table>
  </section>
  <section>
    <h2>Failures <a href="/failures.json" download>Download JSON</a></h2>
    <table id="failures"><thead><tr>
      <th>Time</th><th>Collection</th><th>Op</th><th>Doc</th><th>Targets</th><th>Diff</th>
    </tr></thead><tbody></tbody></table>
  </section>
  <script>
    // 1. fetch('/api/state') -> render header + both tables (rows keyed by id in a Map)
    // 2. new EventSource('/api/events'); kind==="stats" -> renderHeader(data);
    //    kind==="result" -> upsert row Map, re-render recent tbody (and failures tbody when state==="failed")
    // 3. filter input hides rows whose collection+docId don't include the substring
    // 4. duration = endedAtMs ? endedAtMs-startedAtMs+"ms" : "…"
    // 5. failures rows: <details> with a nested table of targets[].diffs[]
  </script>
</body>
</html>
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/cdc-verify`
Expected: PASS. Also eyeball the page: `go` isn't run directly, so use `make build SERVICE=tools/cdc-verify` and run the binary with a bogus env — page rendering itself is checked in Task 14's compose smoke.

- [ ] **Step 5: Commit**

```bash
git add tools/cdc-verify/hub.go tools/cdc-verify/handler.go tools/cdc-verify/routes.go tools/cdc-verify/static.go tools/cdc-verify/static/index.html tools/cdc-verify/handler_test.go
git commit -m "feat(cdc-verify): SSE hub, HTTP handlers, static viewer UI"
```

---

### Task 13: main wiring, example mapping, deploy files, README

**Files:**
- Modify: `tools/cdc-verify/main.go` (replace the stub `main()`)
- Create: `tools/cdc-verify/mapping.example.json`, `tools/cdc-verify/README.md`, `tools/cdc-verify/deploy/Dockerfile`, `tools/cdc-verify/deploy/docker-compose.yml`

**Interfaces:**
- Consumes everything produced so far. No new exported surface.

- [ ] **Step 1: Write `main()`** (wiring is integration-verified in Task 14; there is no unit test for `main` itself — keep it thin):

```go
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	if err := cfg.validate(); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
	if cfg.CredsFile != "" {
		if _, err := os.Stat(cfg.CredsFile); err != nil {
			slog.Error("nats creds file not accessible", "path", cfg.CredsFile, "error", err)
			os.Exit(1)
		}
	}

	mapping, err := loadMapping(cfg.MappingFile)
	if err != nil {
		slog.Error("load mapping", "error", err)
		os.Exit(1)
	}
	if mapping.NeedsCassandra() && (cfg.CassandraHosts == "" || cfg.CassandraKeyspace == "") {
		slog.Error("mapping references cassandra targets but CASSANDRA_HOSTS/CASSANDRA_KEYSPACE are unset")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- connections (fail fast, read-only use) ---
	natsOpts := []nats.Option{nats.Name("cdc-verify")}
	if cfg.CredsFile != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(cfg.CredsFile))
	}
	nc, err := nats.Connect(cfg.NATSURL, natsOpts...)
	if err != nil {
		slog.Error("connect nats", "error", err)
		os.Exit(1)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("create jetstream context", "error", err)
		os.Exit(1)
	}

	srcClient, err := mongoutil.Connect(ctx, cfg.SourceMongoURI, cfg.SourceMongoUsername, cfg.SourceMongoPassword,
		mongoutil.WithReadPreference(readpref.PrimaryPreferred()))
	if err != nil {
		slog.Error("connect source mongo", "error", err)
		os.Exit(1)
	}
	tgtClient, err := mongoutil.Connect(ctx, cfg.TargetMongoURI, cfg.TargetMongoUsername, cfg.TargetMongoPassword)
	if err != nil {
		slog.Error("connect target mongo", "error", err)
		os.Exit(1)
	}

	var cass CassStore
	var cassSession *gocql.Session
	if mapping.NeedsCassandra() {
		cassSession, err = cassutil.Connect(cassutil.Config{
			Hosts: cfg.CassandraHosts, Keyspace: cfg.CassandraKeyspace,
			Username: cfg.CassandraUsername, Password: cfg.CassandraPassword,
		})
		if err != nil {
			slog.Error("connect cassandra", "error", err)
			os.Exit(1)
		}
		cass = newCassStore(cassSession)
	}

	// --- pipeline ---
	sizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)
	reg := newTransformRegistry(sizer)
	sseHub := newHub()
	results := newResultsStore(cfg.RecentCap, cfg.FailedCap, sseHub.broadcastResult)
	v := newVerifier(mapping,
		newMongoStore(srcClient.Database(cfg.SourceDB)),
		newMongoStore(tgtClient.Database(cfg.TargetDB)),
		cass, reg, results, verifierConfig{
			Poll: cfg.VerifyPoll, Timeout: cfg.VerifyTimeout,
			MaxChecks: cfg.MaxChecks, SamplePercent: cfg.SamplePercent,
		})

	streamName := stream.MigrationOplog(cfg.SiteID).Name
	var startAt time.Time
	if cfg.StartAtTime != "" {
		startAt, _ = time.Parse(time.RFC3339, cfg.StartAtTime) // validated already
	}
	w := newWatcher(js, streamName, startAt, v)
	go func() {
		if err := w.Run(ctx); err != nil {
			slog.Error("watcher stopped", "error", err)
			os.Exit(1) // a dead feed makes the dashboard lie; die loudly
		}
	}()

	s, err := js.Stream(ctx, streamName)
	if err != nil {
		slog.Error("open stream", "stream", streamName, "error", err)
		os.Exit(1)
	}
	filter := subject.MigrationOplogWildcard(cfg.SiteID)
	poller := newStatsPoller(streamName,
		func(ctx context.Context) (*jetstream.StreamInfo, error) {
			return s.Info(ctx, jetstream.WithSubjectFilter(filter))
		},
		func(ctx context.Context, name string) (*jetstream.ConsumerInfo, error) {
			c, err := s.Consumer(ctx, name)
			if err != nil {
				return nil, fmt.Errorf("open consumer %s: %w", name, err)
			}
			return c.Info(ctx)
		},
		cfg.TrackConsumers, cfg.StatsInterval, w.Live, sseHub.broadcastStats)
	go poller.Run(ctx)

	// --- HTTP ---
	h := newHandler(sseHub, results, poller)
	mux := http.NewServeMux()
	h.registerRoutes(mux)
	srv := &http.Server{
		Addr:        fmt.Sprintf(":%d", cfg.Port),
		Handler:     mux,
		ReadTimeout: 30 * time.Second,
		// WriteTimeout deliberately omitted — SSE connections are long-lived.
		IdleTimeout: 60 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "error", err)
			os.Exit(1)
		}
	}()

	slog.Info("cdc-verify started", "port", cfg.Port, "stream", streamName,
		"site", cfg.SiteID, "sample_percent", cfg.SamplePercent)

	shutdown.Wait(context.Background(), 25*time.Second,
		func(sctx context.Context) error { return srv.Shutdown(sctx) },
		func(sctx context.Context) error { cancel(); v.Shutdown(sctx); return nil },
		func(_ context.Context) error { return nc.Drain() },
		func(sctx context.Context) error {
			mongoutil.Disconnect(sctx, srcClient)
			mongoutil.Disconnect(sctx, tgtClient)
			if cassSession != nil {
				cassutil.Close(cassSession)
			}
			return nil
		},
	)
}
```

(Imports to add: `context`, `net/http`, `github.com/nats-io/nats.go`, `github.com/nats-io/nats.go/jetstream`, `github.com/gocql/gocql`, `go.mongodb.org/mongo-driver/v2/mongo/readpref`, `github.com/hmchangw/chat/pkg/cassutil`, `pkg/mongoutil`, `pkg/msgbucket`, `pkg/shutdown`, `pkg/stream`, `pkg/subject`, `github.com/caarlos0/env/v11`, `log/slog`, `os`, `time`.)
Check `pkg/shutdown.Wait`'s exact signature before writing (`grep -n "func Wait" pkg/shutdown/*.go`) and match nats-debug's usage.

- [ ] **Step 2: Verify compilation**

Run: `make build SERVICE=tools/cdc-verify && make lint`
Expected: builds clean; lint passes.

- [ ] **Step 3: Write `mapping.example.json`** — cover one of each shape so operators can copy-paste; exact per-collection field lists for production runs are derived from `data-migration/SOURCE_DATA.md` + the transformers and tuned per environment (this file is a template, and CI never runs it against data):

```json
{
  "sources": [
    {
      "collection": "rocketchat_message",
      "ops": { "insert": "verify", "update": "verify", "replace": "verify", "delete": "verify-absent" },
      "targets": {
        "msgById": { "kind": "cassandra", "table": "messages_by_id",
                     "key": { "message_id": "_id" } },
        "msgByRoom": { "kind": "cassandra", "table": "messages_by_room",
                       "key": { "room_id": "rid",
                                "bucket": { "from": "ts", "transform": "msgBucket" },
                                "created_at": { "from": "ts", "transform": "unixMilli" },
                                "message_id": "_id" } }
      },
      "fields": {
        "msg": [ "msgById.body", "msgByRoom.body" ],
        "ts":  [ { "dest": "msgById.created_at", "transform": "unixMilli" } ]
      },
      "derived": [
        { "from": ["u._id"], "transform": "toString", "dest": ["msgById.sender_account", "msgByRoom.sender_account"] }
      ]
    },
    {
      "collection": "rocketchat_subscription",
      "ops": { "insert": "verify", "update": "verify", "replace": "verify", "delete": "skip" },
      "resolvers": {
        "user": { "db": "source", "collection": "users",
                  "key": { "_id": "u._id" }, "fields": [ "username" ] }
      },
      "targets": {
        "subs": { "kind": "mongo", "collection": "subscriptions",
                  "key": { "roomId": "rid", "account": "@user.username" } }
      },
      "fields": {
        "open": [ "subs.joined" ],
        "f":    [ "subs.favorite" ]
      }
    },
    {
      "collection": "rocketchat_avatar",
      "ops": { "insert": "verify", "update": "verify", "replace": "verify", "delete": "verify-absent" },
      "targets": {
        "avatar": { "kind": "mongo", "collection": "rocketchat_avatar",
                    "key": { "_id": "_id" }, "mode": "verbatim", "ignore": [ "_updatedAt" ] }
      }
    }
  ]
}
```

Add a unit test in `mapping_test.go` that loads this shipped file and asserts it validates:

```go
func TestMappingExampleFileIsValid(t *testing.T) {
	_, err := loadMapping("mapping.example.json")
	assert.NoError(t, err)
}
```

- [ ] **Step 4: Write deploy files**

`deploy/Dockerfile` (repo convention — builder + alpine runtime, context = repo root):

```dockerfile
FROM golang:1.25.12-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY tools/cdc-verify/ tools/cdc-verify/
RUN CGO_ENABLED=0 go build -o /out/cdc-verify ./tools/cdc-verify

FROM alpine:3.21
RUN adduser -D -u 10001 app
USER app
COPY --from=builder /out/cdc-verify /usr/local/bin/cdc-verify
EXPOSE 8091
ENTRYPOINT ["cdc-verify"]
```

`deploy/docker-compose.yml` (local demo: NATS+JetStream, two Mongos, Cassandra, the tool; mapping mounted read-only):

```yaml
services:
  nats:
    image: nats:2.10-alpine
    command: ["--jetstream", "--http_port", "8222"]
    ports: ["4222:4222", "8222:8222"]
  source-mongo:
    image: mongo:7
    ports: ["27017:27017"]
  target-mongo:
    image: mongo:7
    ports: ["27018:27017"]
  cassandra:
    image: cassandra:4.1
    ports: ["9042:9042"]
    environment:
      - HEAP_NEWSIZE=128M
      - MAX_HEAP_SIZE=512M
  cdc-verify:
    build:
      context: ../../..
      dockerfile: tools/cdc-verify/deploy/Dockerfile
    depends_on: [nats, source-mongo, target-mongo, cassandra]
    ports: ["8091:8091"]
    volumes:
      - ./mapping.local.json:/etc/cdc-verify/mapping.json:ro
    environment:
      SITE_ID: site1
      NATS_URL: nats://nats:4222
      SOURCE_MONGO_URI: mongodb://source-mongo:27017
      TARGET_MONGO_URI: mongodb://target-mongo:27017
      CASSANDRA_HOSTS: cassandra:9042
      CASSANDRA_KEYSPACE: chat
      MAPPING_FILE: /etc/cdc-verify/mapping.json
```

(`mapping.local.json` is operator-provided next to the compose file — document in README; a copy of `mapping.example.json` works out of the box.)

- [ ] **Step 5: Write `README.md`** — follow `tools/nats-debug/README.md` structure: what it is (one paragraph + the spec's ASCII architecture diagram), quick start (compose + binary), the three UI panels, verification semantics (convergence model, freeze-on-match, states incl. superseded/skipped and failure causes `mismatch` / `dest-missing` / `source-missing` / `resolver-miss` / `ambiguous-key` / `key-unresolvable` / `still-present` / `lookup-error`), the full mapping JSON reference (targets/key/fields/derived/resolvers/verbatim, transform list), the env table from the spec §8, and the read-only/ephemeral-consumer guarantees. State that the tool (like `data-migration/`) is deletable at source retirement.

- [ ] **Step 6: Run everything and commit**

Run: `make fmt && make lint && make test SERVICE=tools/cdc-verify && make build SERVICE=tools/cdc-verify`
Expected: all green.

```bash
git add tools/cdc-verify/main.go tools/cdc-verify/mapping.example.json tools/cdc-verify/README.md tools/cdc-verify/deploy/ tools/cdc-verify/mapping_test.go
git commit -m "feat(cdc-verify): main wiring, example mapping, deploy files, README"
```

---

### Task 14: Integration tests

**Files:**
- Create: `tools/cdc-verify/integration_test.go`
- Test: run with `make test-integration SERVICE=tools/cdc-verify` (Docker required)

**Interfaces:**
- Consumes: `testutil.NATS(t)`, `testutil.MongoDB(t, prefix)`, `testutil.CassandraKeyspace(t, prefix)`; the full pipeline (`newVerifier`, `newWatcher`, real stores).

Scenarios (each an independent subtest set; per-test isolation via testutil prefixes and per-test streams named after hashed `t.Name()`):

1. **End-to-end match** — start NATS; create stream `MIGRATION-OPLOG-it1` (`js.CreateOrUpdateStream` with `stream.MigrationOplog("it1")` — the test owns its stream, mirroring the integration pattern in `data-migration/oplog-connector/integration_test.go`); seed source Mongo doc + matching target Mongo doc; wire watcher+verifier with a mongo-only mapping; publish an `OplogEvent` (insert) on `chat.migration.oplog.it1.rocketchat_room.insert`; poll `results.Recent()` until the row is `StateMatched` (use `require.Eventually`, 15s).
2. **Delayed convergence** — same, but insert the target doc ~2 poll intervals after publishing; assert final `StateMatched` with `Attempts > 1`.
3. **Mismatch → failure with diff** — target doc has a wrong field value; `VerifyTimeout` set short (5s); assert `StateFailed`, failure list has per-target diff with the expected want/got.
4. **Cassandra target** — `testutil.CassandraKeyspace` + `CREATE TABLE messages_by_id (message_id text PRIMARY KEY, body text, created_at bigint)`; seed row; mapping with a cassandra target; assert match. (Bucketed-table shape is covered by the same code path; one cassandra table suffices.)
5. **Verify-absent delete** — target row absent; publish delete event; assert `StateMatched`.
6. **Resolver hop** — source `users` doc; mapping with a `@user.username` key; assert match, and a missing user doc path asserting the `resolver-miss: user` cause on failure.

Structure requirements: `//go:build integration` tag; `TestMain(m) { testutil.RunTests(m) }`; a `startPipeline(t, mapping, cfgOverrides)` helper that builds the pipeline against the test containers, starts `watcher.Run` in a goroutine with `t.Cleanup(cancel)`, and returns `(*resultsStore, publish func(subject string, ev model.OplogEvent))` — publish marshals with `encoding/json` and uses `js.Publish`. Each test creates its own stream + ordered consumer via a unique site id (`"it-" + short hash of t.Name()`), so tests are independent and parallel-safe.

- [ ] **Step 1: Write the tests** (scenarios above; they fail — no harm, the pipeline exists, but expect wiring gaps to surface)
- [ ] **Step 2: Run** `make test-integration SERVICE=tools/cdc-verify` — iterate until green. Any bug found here gets a targeted unit test in the owning file's `_test.go` before the fix (systematic-debugging + TDD).
- [ ] **Step 3: Commit**

```bash
git add tools/cdc-verify/integration_test.go
git commit -m "test(cdc-verify): end-to-end integration coverage"
```

---

### Task 15: Final quality gate

**Files:** none new — verification pass.

- [ ] **Step 1: Coverage** — `go test -coverprofile` via the repo's coverage tooling (`make test SERVICE=tools/cdc-verify` then `go tool cover -func` — check `tools/coveragecheck` for the repo's canonical invocation and use that). Confirm ≥80% overall, 90%+ on `mapping*.go`, `compare.go`, `verifier.go`. Add table cases for any uncovered error path (do not chase vanity lines in `main()` — wiring is integration-covered).
- [ ] **Step 2: Lint + SAST** — `make fmt && make lint && make sast`. The `// #nosec G201` in `lookup_cassandra.go` must carry its justification comment; no other suppressions expected.
- [ ] **Step 3: Full test sweep** — `make test && make test-integration SERVICE=tools/cdc-verify` (unit across the repo to catch accidental cross-package breakage — there should be none; this tool only consumes `pkg/`).
- [ ] **Step 4: Docs cross-check** — README env table matches `config` struct exactly; mapping reference matches `mapping.go` types; spec §12 decisions unchanged (if implementation diverged anywhere, update the spec in the same commit and say so in the message).
- [ ] **Step 5: Commit any fixes**

```bash
git add -A tools/cdc-verify docs/superpowers/specs/2026-08-10-cdc-verify-tool-design.md
git commit -m "chore(cdc-verify): coverage, lint, sast, docs alignment"
```

---

## Plan self-review notes (kept for the executor)

- **Spec coverage:** §4 components → Tasks 7-12; §5 lifecycle (incl. fan-out freeze, supersede, sampling, verify-absent, ambiguous-key, resolver-miss) → Task 9; §5.3 comparison rules → Task 6; §6 mapping schema incl. resolvers/derived/verbatim → Tasks 3-5; §7 UI → Task 12; §8 env → Tasks 1, 13; §9 error handling → Tasks 9-11 (lookup errors poll-retry inside the check loop; watcher liveness via `Live()`; shutdown in Task 13); §10 testing → every task + Task 14; §11 placement/deploy → Task 13.
- **Known intentional deviations from spec text:** (a) the watcher uses an *ordered consumer* (client-managed ephemeral) rather than a hand-rolled ephemeral pull consumer — same guarantees, less code; (b) `SourceStore.FindByID` fetches the whole doc rather than a projection — the mapping needs most fields and resolver/derived paths make static projections fragile; noted in store.go. Both are within the spec's intent; do not "fix" them back.
- **Type-consistency anchors:** `CheckResult`/`TargetResult`/`Counters` (Task 8) are the wire format for `/api/state`, SSE, and `/failures.json` — do not rename fields after Task 12 builds the UI against them. `fieldPair` (Task 6) is the only currency between mapping compilation (Task 9) and comparison (Task 6).
