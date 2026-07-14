# Loadgen `thread` max-rps Workload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an isolated `thread` workload to `tools/loadgen` that measures the maximum sustainable RPS for sending **thread replies** through the single-site messaging pipeline, directly comparable to the existing `messages` workload.

**Architecture:** Reuse the `messages` presets (rooms/subscriptions) and the existing `rpsWorkload` ramp/SLO/report harness. Pre-seed a fixed number of real parent messages per room into Cassandra (via the existing history-seed write path) so the gatekeeper's synchronous parent-fetch resolves. Extend the open-loop `Generator` to publish frontdoor sends with `ThreadParentMessageID` set, and add a `threadWorkload` adapter plus `seed`/`teardown`/`max-rps` CLI wiring.

**Tech Stack:** Go 1.25, NATS JetStream, Cassandra (`gocql`), MongoDB (`mongo-driver/v2`), Prometheus client, `stretchr/testify`. All commands via the root `Makefile`.

---

## Background the engineer needs

`tools/loadgen` is a single `package main` binary. Key existing pieces you will reuse (do not reimplement):

- `tools/loadgen/preset.go`:
  - `type Preset struct { Name string; Users int; Rooms int; ... ContentBytes Range; ... }`
  - `BuiltinPreset(name string) (Preset, bool)` — messages presets: `small`, `medium`, `large`, `realistic`.
  - `type Fixtures struct { Users []model.User; Rooms []model.Room; Subscriptions []model.Subscription; RoomKeys map[string]roomkeystore.RoomKeyPair }`
  - `BuildFixtures(p *Preset, seed int64, siteID string) Fixtures`
- `tools/loadgen/seed.go`:
  - `Seed(ctx, db *mongo.Database, f *Fixtures) error`
  - `Teardown(ctx, db *mongo.Database) error`
  - `SeedRoomKeys(ctx, keys roomKeyStore, roomKeys map[string]roomkeystore.RoomKeyPair) error`
- `tools/loadgen/generator.go`: `GeneratorConfig`, `Generator`, `publishOne`. Frontdoor path builds `model.SendMessageRequest{ID, Content, RequestID}` and publishes on `subject.MsgSend(account, roomID, siteID)`; calls `Collector.RecordPublish(reqID, msgID, publishTime)`.
- `tools/loadgen/maxrps_messages.go`: `messagesWorkload`, `newMessagesWorkload`, `msgCounters`, `diffCounters`, `buildMessagesInputs`, `msgErrorReasons`. RunStep builds a `Generator` per step.
- `tools/loadgen/history_seed.go`: `writePlannedMessage(ctx, session, sizer, msg *plannedMessage, siteID, parentCreatedAtByID map[string]time.Time) error` — writes `messages_by_room` + `messages_by_id` for a top-level message (`msg.ThreadParentID == ""`).
- `tools/loadgen/history.go`: `type plannedMessage struct { RoomID, MessageID, SenderID, SenderAccount, SenderEngName, Content string; CreatedAt time.Time; ThreadRoomID, ThreadParentID string; TCount int }`.
- `tools/loadgen/history_main.go`: `connectCassandra(cfg *config) (*gocql.Session, error)`; pattern `sizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)`.
- `pkg/model`: `SendMessageRequest{ ID, Content, RequestID string; ThreadParentMessageID string \`json:"threadParentMessageId,omitempty"\` ... }`. `Subscription` has `User model.Participant` (fields `ID`, `Account`, `EngName`) and `RoomID string`.
- `pkg/idgen`: `GenerateMessageID()` (20-char base62), `GenerateRequestID()`.

**Why parents must be pre-seeded:** `message-gatekeeper` resolves a thread reply's parent by issuing a synchronous `GetMessageByID` RPC to history-service (reads Cassandra `messages_by_id`). A reply to a non-existent parent makes the gatekeeper Nak (never acks). So every room used for replies must have real parent messages in `messages_by_id`.

**Determinism:** all minting uses an RNG seeded from the run `seed` so `seed` and `max-rps`/`teardown` agree on the same IDs.

---

## File Structure

- **Create** `tools/loadgen/thread_fixtures.go` — `ThreadFixtures`, `BuildThreadFixtures`, `SeedThreadParents`, `TeardownThreadParents`.
- **Create** `tools/loadgen/thread_fixtures_test.go` — unit tests for the above.
- **Create** `tools/loadgen/maxrps_thread.go` — `threadWorkload`, `newThreadWorkload`.
- **Create** `tools/loadgen/maxrps_thread_test.go` — `Label()` + generator thread-mode tests.
- **Create** `tools/loadgen/thread_main.go` — `runSeedThread`, `runTeardownThread`.
- **Modify** `tools/loadgen/generator.go` — add `ParentsByRoom` to `GeneratorConfig`; add a thread branch in `publishOne`.
- **Modify** `tools/loadgen/generator_test.go` — thread-mode publish test (or place it in `maxrps_thread_test.go`; this plan uses `maxrps_thread_test.go`).
- **Modify** `tools/loadgen/maxrps.go` — add `case "thread"` to `runMaxRPS`; update `--workload` usage string.
- **Modify** `tools/loadgen/main.go` — add `case "thread"` to seed and teardown dispatch; add `--parents-per-room` flag; update usage strings.
- **Modify** `tools/loadgen/README.md` — "Thread-reply workload" section.

Default constant: **8 parents per room** (`defaultParentsPerRoom`).

---

## Task 1: `ThreadFixtures` + `BuildThreadFixtures`

**Files:**
- Create: `tools/loadgen/thread_fixtures.go`
- Test: `tools/loadgen/thread_fixtures_test.go`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/thread_fixtures_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func subscribersByRoom(f Fixtures) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, s := range f.Subscriptions {
		if out[s.RoomID] == nil {
			out[s.RoomID] = map[string]bool{}
		}
		out[s.RoomID][s.User.ID] = true
	}
	return out
}

func TestBuildThreadFixtures_Deterministic(t *testing.T) {
	p, ok := BuiltinPreset("medium")
	require.True(t, ok)

	a := BuildThreadFixtures(&p, 42, 3, "site-a")
	b := BuildThreadFixtures(&p, 42, 3, "site-a")
	assert.Equal(t, a.ParentsByRoom, b.ParentsByRoom)
}

func TestBuildThreadFixtures_ParentsPerRoomAndOwnership(t *testing.T) {
	p, ok := BuiltinPreset("medium")
	require.True(t, ok)

	tf := BuildThreadFixtures(&p, 42, 4, "site-a")
	require.NotEmpty(t, tf.Subscriptions)

	subs := subscribersByRoom(tf.Fixtures)
	for _, room := range tf.Rooms {
		parents := tf.ParentsByRoom[room.ID]
		require.Len(t, parents, 4, "room %s parent count", room.ID)
		for _, pm := range parents {
			require.Len(t, pm.MessageID, 20, "message id length")
			assert.True(t, subs[room.ID][pm.SenderID],
				"parent sender %s must subscribe to room %s", pm.SenderID, room.ID)
		}
	}
}

func TestBuildThreadFixtures_EverySeededRoomHasParents(t *testing.T) {
	p, ok := BuiltinPreset("small")
	require.True(t, ok)

	tf := BuildThreadFixtures(&p, 7, 2, "site-a")
	subs := subscribersByRoom(tf.Fixtures)
	for roomID := range subs {
		assert.GreaterOrEqual(t, len(tf.ParentsByRoom[roomID]), 1,
			"room %s has subscribers but no parents", roomID)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=loadgen 2>&1 | tail -20`
Expected: compile failure — `undefined: BuildThreadFixtures`, `undefined: ThreadFixtures` (its `.ParentsByRoom`, `.Fixtures` embedding), `threadParent` (`.MessageID`, `.SenderID`).

- [ ] **Step 3: Write the minimal implementation**

Create `tools/loadgen/thread_fixtures.go`:

```go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// defaultParentsPerRoom is how many thread-parent messages BuildThreadFixtures
// mints per room when the caller does not override it. Several parents per room
// spread thread fan-out across distinct threads rather than one hot thread,
// matching realistic steady state.
const defaultParentsPerRoom = 8

// threadParentSeedConcurrency caps in-flight parent INSERTs during the seed.
// Each INSERT targets a distinct partition, so this only bounds coordinator
// queuing; matches history_seed's historySeedConcurrency.
const threadParentSeedConcurrency = 50

// threadParent is one seeded parent message: the ID a thread reply references
// plus the subscriber that authored it (so the Cassandra row's sender is a real
// room member, mirroring production).
type threadParent struct {
	MessageID     string
	SenderID      string
	SenderAccount string
	SenderEngName string
}

// ThreadFixtures is the messages Fixtures plus the per-room thread parents the
// thread workload replies to. ParentsByRoom is keyed by room ID; every room
// that has subscriptions gets ParentsPerRoom entries.
type ThreadFixtures struct {
	Fixtures
	ParentsByRoom  map[string][]threadParent
	ParentsPerRoom int
}

// BuildThreadFixtures builds the base messages fixtures for the preset, then
// deterministically mints parentsPerRoom thread-parent messages per room, each
// authored by a random subscriber of that room. A room with no subscribers gets
// no parents. parentsPerRoom <= 0 falls back to defaultParentsPerRoom.
func BuildThreadFixtures(p *Preset, seed int64, parentsPerRoom int, siteID string) ThreadFixtures {
	if parentsPerRoom <= 0 {
		parentsPerRoom = defaultParentsPerRoom
	}
	base := BuildFixtures(p, seed, siteID)

	// Group subscriptions by room for O(1) author selection.
	subsByRoom := make(map[string][]int, len(base.Rooms))
	for i := range base.Subscriptions {
		rid := base.Subscriptions[i].RoomID
		subsByRoom[rid] = append(subsByRoom[rid], i)
	}

	// A dedicated RNG offset from the run seed keeps parent minting independent
	// of BuildFixtures' own RNG stream while staying reproducible.
	rng := rand.New(rand.NewSource(seed ^ 0x7e57_0001))
	parents := make(map[string][]threadParent, len(base.Rooms))
	for _, room := range base.Rooms {
		members := subsByRoom[room.ID]
		if len(members) == 0 {
			continue
		}
		list := make([]threadParent, 0, parentsPerRoom)
		for n := 0; n < parentsPerRoom; n++ {
			sub := base.Subscriptions[members[rng.Intn(len(members))]]
			list = append(list, threadParent{
				MessageID:     idgen.GenerateMessageID(),
				SenderID:      sub.User.ID,
				SenderAccount: sub.User.Account,
				SenderEngName: sub.User.EngName,
			})
		}
		parents[room.ID] = list
	}

	return ThreadFixtures{Fixtures: base, ParentsByRoom: parents, ParentsPerRoom: parentsPerRoom}
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test SERVICE=loadgen 2>&1 | tail -20`
Expected: PASS for `TestBuildThreadFixtures_*` (other loadgen tests unaffected).

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/thread_fixtures.go tools/loadgen/thread_fixtures_test.go
git commit -m "feat(loadgen): thread-reply fixtures with per-room parents"
```

---

## Task 2: Cassandra seed/teardown for thread parents

**Files:**
- Modify: `tools/loadgen/thread_fixtures.go`
- Test: `tools/loadgen/thread_fixtures_test.go`

`SeedThreadParents` reuses `writePlannedMessage` (history_seed.go) — it writes a top-level message (`ThreadParentID == ""`) into `messages_by_room` + `messages_by_id`, which is exactly what history-service's `GetMessageByID` reads. No new CQL.

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/thread_fixtures_test.go`:

```go
func TestThreadParentToPlanned_TopLevel(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	pm := threadParent{MessageID: "msg20charbase62000001", SenderID: "u1", SenderAccount: "u1.acct", SenderEngName: "User One"}
	planned := threadParentToPlanned(pm, "room-1", now)

	assert.Equal(t, "room-1", planned.RoomID)
	assert.Equal(t, pm.MessageID, planned.MessageID)
	assert.Equal(t, pm.SenderID, planned.SenderID)
	assert.Equal(t, pm.SenderAccount, planned.SenderAccount)
	assert.Equal(t, now, planned.CreatedAt)
	assert.Empty(t, planned.ThreadParentID, "parent is a top-level message")
	assert.Empty(t, planned.ThreadRoomID)
	assert.NotEmpty(t, planned.Content)
}
```

Add `"time"` to the test file's imports if not already present.

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=loadgen 2>&1 | tail -20`
Expected: compile failure — `undefined: threadParentToPlanned`.

- [ ] **Step 3: Write the minimal implementation**

Append to `tools/loadgen/thread_fixtures.go`:

```go
// threadParentContent is the fixed body stamped on every seeded parent. The
// gatekeeper fetch only needs the parent's CreatedAt; the body is irrelevant to
// the benchmark, so a constant keeps the seed deterministic.
const threadParentContent = "loadgen thread parent"

// threadParentToPlanned projects a seeded parent into the plannedMessage shape
// writePlannedMessage consumes. createdAt is the parent's timestamp (also the
// value the gatekeeper resolves). ThreadParentID is empty: a parent is a
// top-level message.
func threadParentToPlanned(pm threadParent, roomID string, createdAt time.Time) plannedMessage {
	return plannedMessage{
		RoomID:        roomID,
		MessageID:     pm.MessageID,
		SenderID:      pm.SenderID,
		SenderAccount: pm.SenderAccount,
		SenderEngName: pm.SenderEngName,
		Content:       threadParentContent,
		CreatedAt:     createdAt,
	}
}

// SeedThreadParents writes every parent in fixtures.ParentsByRoom into Cassandra
// (messages_by_room + messages_by_id) via the shared writePlannedMessage path,
// so message-gatekeeper's GetMessageByID resolves them. Parents are stamped at
// `now` and bucketed with the supplied sizer (MESSAGE_BUCKET_HOURS). Returns the
// number of parents written. Bounded fan-out mirrors writeRoomCassandra.
func SeedThreadParents(
	ctx context.Context,
	session *gocql.Session,
	sizer msgbucket.Sizer,
	fixtures *ThreadFixtures,
	siteID string,
	now time.Time,
) (int, error) {
	// Parents are top-level messages, so the parent-CreatedAt lookup
	// writePlannedMessage takes is unused for them; an empty map is safe.
	noParentLookup := map[string]time.Time{}

	sem := make(chan struct{}, threadParentSeedConcurrency)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	total := 0
	cancelled := false
	for roomID, list := range fixtures.ParentsByRoom {
		for i := range list {
			planned := threadParentToPlanned(list[i], roomID, now)
			select {
			case <-ctx.Done():
				cancelled = true
			case sem <- struct{}{}:
			}
			if cancelled {
				break
			}
			total++
			wg.Add(1)
			go func(m plannedMessage) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := writePlannedMessage(ctx, session, sizer, &m, siteID, noParentLookup); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}(planned)
		}
		if cancelled {
			break
		}
	}
	wg.Wait()
	close(errCh)
	if cancelled {
		return total, ctx.Err()
	}
	if err, ok := <-errCh; ok {
		return total, fmt.Errorf("seed thread parents: %w", err)
	}
	return total, nil
}

// TeardownThreadParents removes seeded parents from the message tables. It
// reuses TeardownHistoryCassandra, which TRUNCATEs messages_by_room,
// messages_by_id, and thread_messages_by_thread — the same tables the thread
// reply path writes, so this also clears replies produced during a run.
func TeardownThreadParents(ctx context.Context, session *gocql.Session) error {
	return TeardownHistoryCassandra(ctx, session)
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test SERVICE=loadgen 2>&1 | tail -20`
Expected: PASS for `TestThreadParentToPlanned_TopLevel`.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/thread_fixtures.go tools/loadgen/thread_fixtures_test.go
git commit -m "feat(loadgen): seed thread parents into Cassandra"
```

---

## Task 3: Generator thread-reply publish path

**Files:**
- Modify: `tools/loadgen/generator.go` (`GeneratorConfig`, `publishOne`)
- Test: `tools/loadgen/maxrps_thread_test.go`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/maxrps_thread_test.go`. This uses the existing test Publisher/Collector/Metrics helpers already used by `generator_test.go` — confirm their names first.

Run: `grep -n "func.*Publish(ctx\|capturePublisher\|recordingPublisher\|NewMetrics\|NewCollector" tools/loadgen/generator_test.go tools/loadgen/maxrps_messages_test.go`
Use whatever in-package fake Publisher exists (a struct with `Publish(ctx, subj, data) error` capturing calls). The test below assumes a minimal local fake; if a shared one exists, use it instead of redeclaring.

```go
package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// threadCapturePublisher records published subjects + payloads for assertions.
type threadCapturePublisher struct {
	mu    sync.Mutex
	subj  []string
	datas [][]byte
}

func (p *threadCapturePublisher) Publish(_ context.Context, subj string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subj = append(p.subj, subj)
	cp := make([]byte, len(data))
	copy(cp, data)
	p.datas = append(p.datas, cp)
	return nil
}

func TestGenerator_ThreadMode_SetsParentFromRoom(t *testing.T) {
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	tf := BuildThreadFixtures(&p, 42, 3, "site-a")
	require.NotEmpty(t, tf.Subscriptions)

	// valid parent IDs per room, for membership assertions
	validByRoom := map[string]map[string]bool{}
	for rid, list := range tf.ParentsByRoom {
		validByRoom[rid] = map[string]bool{}
		for _, pm := range list {
			validByRoom[rid][pm.MessageID] = true
		}
	}

	pub := &threadCapturePublisher{}
	metrics := NewMetrics()
	collector := NewCollector(metrics, p.Name)
	gen := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: tf.Fixtures, SiteID: "site-a",
		Rate: 1, Inject: InjectFrontdoor, Publisher: pub,
		Metrics: metrics, Collector: collector,
		ParentsByRoom: tf.ParentsByRoom,
		WarmupDeadline: time.Now().Add(-time.Hour), // count as measured
	}, 42)

	for i := 0; i < 50; i++ {
		gen.publishOne(context.Background())
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	require.NotEmpty(t, pub.datas)
	for i, data := range pub.datas {
		var req model.SendMessageRequest
		require.NoError(t, json.Unmarshal(data, &req))
		assert.NotEmpty(t, req.ThreadParentMessageID, "reply %d must set a thread parent", i)
		// subj is chat.user.{account}.room.{roomID}.{siteID}.msg.send — the
		// parent must belong to the room actually posted to. Cross-check by
		// confirming the parent ID is valid in at least one room (rooms are
		// disjoint in fixtures, so a hit proves room consistency).
		hit := false
		for _, valid := range validByRoom {
			if valid[req.ThreadParentMessageID] {
				hit = true
				break
			}
		}
		assert.True(t, hit, "parent %s must be a seeded parent", req.ThreadParentMessageID)
	}
}

func TestGenerator_PlainMode_NoParent(t *testing.T) {
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	f := BuildFixtures(&p, 42, "site-a")

	pub := &threadCapturePublisher{}
	metrics := NewMetrics()
	collector := NewCollector(metrics, p.Name)
	gen := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: f, SiteID: "site-a",
		Rate: 1, Inject: InjectFrontdoor, Publisher: pub,
		Metrics: metrics, Collector: collector,
		// ParentsByRoom nil → plain send
		WarmupDeadline: time.Now().Add(-time.Hour),
	}, 42)

	for i := 0; i < 10; i++ {
		gen.publishOne(context.Background())
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	require.NotEmpty(t, pub.datas)
	for _, data := range pub.datas {
		var req model.SendMessageRequest
		require.NoError(t, json.Unmarshal(data, &req))
		assert.Empty(t, req.ThreadParentMessageID, "plain send must not set a thread parent")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=loadgen 2>&1 | tail -25`
Expected: compile failure — `unknown field 'ParentsByRoom' in struct literal of type GeneratorConfig`.

- [ ] **Step 3: Add the config field**

In `tools/loadgen/generator.go`, add to `GeneratorConfig` (after `MaxInFlight int`):

```go
	// ParentsByRoom, when non-nil, switches the frontdoor path into thread-reply
	// mode: each send sets ThreadParentMessageID to a random seeded parent of the
	// target room. nil = plain sends. Keyed by room ID.
	ParentsByRoom map[string][]threadParent
}
```

(Replace the existing closing brace of the struct accordingly — the field goes inside the struct.)

- [ ] **Step 4: Add the thread branch in `publishOne`**

In `tools/loadgen/generator.go`, replace the frontdoor `default:` block of the inject switch:

```go
	default:
		reqID = idgen.GenerateRequestID()
		req := model.SendMessageRequest{ID: msgID, Content: content, RequestID: reqID}
		data, err = json.Marshal(req)
		subj = subject.MsgSend(sub.User.Account, sub.RoomID, g.cfg.SiteID)
		g.cfg.Collector.RecordPublish(reqID, msgID, publishTime)
	}
```

with:

```go
	default:
		reqID = idgen.GenerateRequestID()
		req := model.SendMessageRequest{ID: msgID, Content: content, RequestID: reqID}
		if g.cfg.ParentsByRoom != nil {
			parents := g.cfg.ParentsByRoom[sub.RoomID]
			if len(parents) == 0 {
				// Room has no seeded parents — cannot form a valid thread reply.
				return
			}
			req.ThreadParentMessageID = parents[g.intn(len(parents))].MessageID
		}
		data, err = json.Marshal(req)
		subj = subject.MsgSend(sub.User.Account, sub.RoomID, g.cfg.SiteID)
		g.cfg.Collector.RecordPublish(reqID, msgID, publishTime)
	}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=loadgen 2>&1 | tail -25`
Expected: PASS for `TestGenerator_ThreadMode_SetsParentFromRoom` and `TestGenerator_PlainMode_NoParent`.

- [ ] **Step 6: Commit**

```bash
git add tools/loadgen/generator.go tools/loadgen/maxrps_thread_test.go
git commit -m "feat(loadgen): generator thread-reply publish path"
```

---

## Task 4: `threadWorkload` adapter

**Files:**
- Create: `tools/loadgen/maxrps_thread.go`
- Test: `tools/loadgen/maxrps_thread_test.go` (append)

This mirrors `messagesWorkload` (maxrps_messages.go) exactly, swapping fixtures for `ThreadFixtures` and forcing `InjectFrontdoor` + `ParentsByRoom` in `RunStep`. It reuses `msgCounters`, `diffCounters`, `buildMessagesInputs`, `msgErrorReasons`, and `newE2Handler` unchanged.

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/maxrps_thread_test.go`:

```go
func TestThreadWorkload_Label(t *testing.T) {
	w := &threadWorkload{}
	assert.Equal(t, "thread", w.Label())
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=loadgen 2>&1 | tail -20`
Expected: compile failure — `undefined: threadWorkload`.

- [ ] **Step 3: Write the implementation**

Create `tools/loadgen/maxrps_thread.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// threadWorkload drives the thread-reply send path at a given RPS. It is the
// messagesWorkload shape with ThreadFixtures and a forced frontdoor inject with
// ParentsByRoom wired into the per-step Generator. E1/E2 correlation, counters,
// and pending model are reused unchanged.
type threadWorkload struct {
	cfg       *config
	preset    *Preset
	fixtures  ThreadFixtures
	seed      int64
	js        jetstream.JetStream
	metrics   *Metrics
	collector *Collector
	publisher Publisher
	canonical string
	durables  []string
}

func (w *threadWorkload) Label() string { return "thread" }

// newThreadWorkload wires NATS, the metrics server, the E1/E2 subscriptions, and
// the publisher. The returned cleanup unsubscribes, shuts the metrics server,
// and drains NATS. fixtures must already be seeded (rooms/subs/keys in Mongo,
// parents in Cassandra).
func newThreadWorkload(ctx context.Context, cfg *config, preset *Preset, fixtures ThreadFixtures, seed int64) (*threadWorkload, func(), error) {
	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		return nil, nil, fmt.Errorf("nats connect: %w", err)
	}
	js, err := jetstream.New(nc.NatsConn())
	if err != nil {
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("jetstream init: %w", err)
	}
	metrics := NewMetrics()
	srv := &http.Server{Addr: cfg.MetricsAddr, Handler: metrics.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("metrics server stopped", "error", err)
		}
	}()
	shutdownSrv := func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}

	collector := NewCollector(metrics, preset.Name)

	e1Sub, err := nc.NatsConn().Subscribe(subject.UserResponseWildcard(), func(msg *nats.Msg) {
		reqID := lastToken(msg.Subject)
		var payload struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			metrics.PublishErrors.WithLabelValues(preset.Name, "bad_reply").Inc()
			return
		}
		if payload.Error != "" {
			metrics.PublishErrors.WithLabelValues(preset.Name, "gatekeeper").Inc()
		}
		collector.RecordReply(reqID, time.Now())
	})
	if err != nil {
		shutdownSrv()
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("subscribe e1: %w", err)
	}
	e2Handler := newE2Handler(collector)
	e2Sub, err := nc.NatsConn().Subscribe(subject.RoomEventWildcard(), e2Handler)
	if err != nil {
		shutdownSrv()
		_ = e1Sub.Unsubscribe()
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("subscribe e2: %w", err)
	}
	e2DMSub, err := nc.NatsConn().Subscribe(subject.UserRoomEventWildcard(), e2Handler)
	if err != nil {
		shutdownSrv()
		_ = e1Sub.Unsubscribe()
		_ = e2Sub.Unsubscribe()
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("subscribe e2 dm: %w", err)
	}

	w := &threadWorkload{
		cfg: cfg, preset: preset, fixtures: fixtures, seed: seed,
		js: js, metrics: metrics, collector: collector,
		publisher: newNatsCorePublisher(nc.NatsConn(), InjectFrontdoor, js),
		canonical: stream.MessagesCanonical(cfg.SiteID).Name,
		durables:  []string{"message-worker", "broadcast-worker"},
	}
	cleanup := func() {
		_ = e1Sub.Unsubscribe()
		_ = e2Sub.Unsubscribe()
		_ = e2DMSub.Unsubscribe()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = srv.Shutdown(shutCtx)
		cancel()
		_ = nc.Drain()
	}
	return w, cleanup, nil
}

func (w *threadWorkload) snapshotCounters() msgCounters {
	mfs, err := w.metrics.Registry.Gather()
	if err != nil {
		slog.Warn("metrics gather", "error", err)
	}
	c := msgCounters{
		published: gatheredCounterValue(mfs, "loadgen_published_total", "", ""),
		err:       map[string]float64{},
	}
	for _, reason := range msgErrorReasons {
		c.err[reason] = gatheredCounterValue(mfs, "loadgen_publish_errors_total", "reason", reason)
	}
	return c
}

func (w *threadWorkload) snapshotPending(ctx context.Context) (map[string]uint64, error) {
	out := map[string]uint64{}
	for _, d := range w.durables {
		cons, err := w.js.Consumer(ctx, w.canonical, d)
		if err != nil {
			return nil, fmt.Errorf("consumer %s: %w", d, err)
		}
		info, err := cons.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("consumer info %s: %w", d, err)
		}
		out[d] = info.NumPending
	}
	return out, nil
}

// RunStep runs a fresh thread-reply generator at targetRPS for warmup+hold,
// resetting the collector at the hold boundary so only the hold window is
// measured. Identical to messagesWorkload.RunStep except the Generator is wired
// with InjectFrontdoor + ParentsByRoom.
func (w *threadWorkload) RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error) {
	gen := NewGenerator(&GeneratorConfig{
		Preset: w.preset, Fixtures: w.fixtures.Fixtures, SiteID: w.cfg.SiteID,
		Rate: targetRPS, Inject: InjectFrontdoor, Publisher: w.publisher,
		Metrics: w.metrics, Collector: w.collector,
		ParentsByRoom:  w.fixtures.ParentsByRoom,
		WarmupDeadline: time.Now().Add(warmup), MaxInFlight: w.cfg.MaxInFlight,
	}, w.seed)

	genCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = gen.Run(genCtx)
	}()

	if err := waitOrCancel(ctx, warmup); err != nil {
		cancel()
		wg.Wait()
		return rpsStepInputs{}, err
	}

	holdStart := time.Now()
	w.collector.Reset()
	startCounts := w.snapshotCounters()
	startPending, perr1 := w.snapshotPending(ctx)

	holdErr := waitOrCancel(ctx, hold)

	endCounts := w.snapshotCounters()
	endPending, perr2 := w.snapshotPending(ctx)
	cancel()
	wg.Wait()
	time.Sleep(2 * time.Second) // drain trailing replies/broadcasts
	w.collector.DiscardBefore(holdStart)

	if holdErr != nil {
		return rpsStepInputs{}, holdErr
	}

	delta := diffCounters(startCounts, endCounts)
	pendingOK := perr1 == nil && perr2 == nil
	if !pendingOK {
		slog.Warn("pending snapshot failed", "start_err", perr1, "end_err", perr2)
	}
	return buildMessagesInputs(targetRPS, hold, delta,
		w.collector.E1Samples(), w.collector.E2Samples(),
		startPending, endPending, w.durables, pendingOK), nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=loadgen 2>&1 | tail -20`
Expected: PASS for `TestThreadWorkload_Label`. Run `make lint 2>&1 | tail -20` — expected: no new findings (the `time.Sleep` mirrors the sanctioned drain in messagesWorkload; if the linter flags it, add the same nolint/comment the messages file uses — check `grep -n "time.Sleep" tools/loadgen/maxrps_messages.go` for the existing treatment).

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/maxrps_thread.go tools/loadgen/maxrps_thread_test.go
git commit -m "feat(loadgen): thread-reply max-rps workload adapter"
```

---

## Task 5: Seed + teardown runners

**Files:**
- Create: `tools/loadgen/thread_main.go`
- Test: covered by integration (Task 7) + manual; unit-level dispatch in Task 6.

- [ ] **Step 1: Write the implementation**

Create `tools/loadgen/thread_main.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// runSeedThread seeds the messages fixtures (rooms/subs/room-keys in Mongo) then
// the per-room thread parents in Cassandra, so the thread max-rps workload's
// replies reference resolvable parents. parentsPerRoom <= 0 uses the default.
func runSeedThread(ctx context.Context, cfg *config, preset string, seed int64, usersOverride, parentsPerRoom int) int {
	if cfg.CassandraHosts == "" {
		fmt.Fprintln(os.Stderr, "thread workload requires CASSANDRA_HOSTS")
		return 2
	}
	p, ok := BuiltinPreset(preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown preset: %s\n", preset)
		return 2
	}
	if usersOverride > 0 {
		p.Users = usersOverride
	}

	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()

	session, err := connectCassandra(cfg)
	if err != nil {
		slog.Error("cassandra connect", "error", err)
		return 1
	}
	defer cassutil.Close(session)

	fixtures := BuildThreadFixtures(&p, seed, parentsPerRoom, cfg.SiteID)
	if err := Seed(ctx, db, &fixtures.Fixtures); err != nil {
		slog.Error("seed mongo fixtures", "error", err)
		return 1
	}
	if err := SeedRoomKeys(ctx, keyStore, fixtures.RoomKeys); err != nil {
		slog.Error("seed room keys", "error", err)
		return 1
	}
	sizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)
	parentCount, err := SeedThreadParents(ctx, session, sizer, &fixtures, cfg.SiteID, time.Now().UTC())
	if err != nil {
		slog.Error("seed thread parents", "error", err)
		return 1
	}

	slog.Info("seed complete (thread)",
		"preset", p.Name,
		"users", len(fixtures.Users),
		"rooms", len(fixtures.Rooms),
		"subs", len(fixtures.Subscriptions),
		"parentsPerRoom", fixtures.ParentsPerRoom,
		"threadParents", parentCount,
		"bucketHours", cfg.MessageBucketHours)
	return 0
}

// runTeardownThread clears the Mongo fixtures and TRUNCATEs the message tables
// (parents + any replies produced during the run).
func runTeardownThread(ctx context.Context, cfg *config, preset string, seed int64) int {
	if cfg.CassandraHosts == "" {
		fmt.Fprintln(os.Stderr, "thread workload requires CASSANDRA_HOSTS")
		return 2
	}
	if _, ok := BuiltinPreset(preset); !ok {
		fmt.Fprintf(os.Stderr, "unknown preset: %s\n", preset)
		return 2
	}

	db, _, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()

	session, err := connectCassandra(cfg)
	if err != nil {
		slog.Error("cassandra connect", "error", err)
		return 1
	}
	defer cassutil.Close(session)

	if err := Teardown(ctx, db); err != nil {
		slog.Error("teardown mongo", "error", err)
		return 1
	}
	if err := TeardownThreadParents(ctx, session); err != nil {
		slog.Error("teardown thread parents", "error", err)
		return 1
	}
	slog.Info("teardown complete (thread)", "preset", preset)
	return 0
}
```

Note: `seed` is accepted for signature symmetry with the other teardown runners; `Teardown`/`TRUNCATE` are wholesale, so `runTeardownThread` does not need it to recompute IDs. If `golangci-lint` flags the unused `seed` param, rename it to `_`.

- [ ] **Step 2: Verify compilation**

Run: `make build SERVICE=loadgen 2>&1 | tail -20`
Expected: build succeeds. (Runners are wired into dispatch in Task 6; until then they're unreferenced — Go allows unused package-level funcs, so the build passes.)

- [ ] **Step 3: Commit**

```bash
git add tools/loadgen/thread_main.go
git commit -m "feat(loadgen): thread seed and teardown runners"
```

---

## Task 6: CLI wiring (`seed`, `teardown`, `max-rps`)

**Files:**
- Modify: `tools/loadgen/main.go` (seed dispatch + teardown dispatch + `--parents-per-room` flag + usage strings)
- Modify: `tools/loadgen/maxrps.go` (`runMaxRPS` switch + `--workload` usage)
- Test: `tools/loadgen/maxrps_thread_test.go` (append a dispatch-validation test)

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/maxrps_thread_test.go`:

```go
func TestRunMaxRPS_ThreadRequiresPreset(t *testing.T) {
	cfg := &config{}
	// No --preset → exit code 2 before any NATS/Cassandra connection.
	code := runMaxRPS(context.Background(), cfg, []string{"--workload=thread"})
	assert.Equal(t, 2, code)
}

func TestRunMaxRPS_ThreadUnknownPreset(t *testing.T) {
	cfg := &config{}
	code := runMaxRPS(context.Background(), cfg, []string{"--workload=thread", "--preset=nope"})
	assert.Equal(t, 2, code)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=loadgen 2>&1 | tail -20`
Expected: `TestRunMaxRPS_ThreadUnknownPreset` fails — `thread` hits the `default:` "unknown workload" branch and returns 2 for the wrong reason, OR (after preset gate) the unknown-preset path doesn't exist yet. (The "requires preset" test may already pass via the global `--preset` gate; the unknown-preset test is the meaningful red.)

- [ ] **Step 3: Add the `max-rps` thread case**

In `tools/loadgen/maxrps.go`, add this case to the `switch *workload` in `runMaxRPS`, immediately after the `case "messages":` block (before `case "history":`):

```go
	case "thread":
		p, ok := BuiltinPreset(*preset)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown preset: %s\n", *preset)
			return 2
		}
		if cfg.CassandraHosts == "" {
			fmt.Fprintln(os.Stderr, "thread workload requires CASSANDRA_HOSTS")
			return 2
		}
		fixtures := BuildThreadFixtures(&p, *seed, 0, cfg.SiteID)
		tw, clean, err := newThreadWorkload(ctx, cfg, &p, fixtures, *seed)
		if err != nil {
			slog.Error("init thread workload", "error", err)
			return 1
		}
		w, cleanup, presetID = tw, clean, p.Name
```

Note: the thread workload rebuilds fixtures (same `seed`, default parents-per-room) to recover the `ParentsByRoom` map the generator needs. This must match the `seed` invocation's `--seed` and `--parents-per-room` (default 8). Document this in the README (Task 8).

- [ ] **Step 4: Update the `max-rps` `--workload` usage string**

In `tools/loadgen/maxrps.go`, change:

```go
	workload := fs.String("workload", "messages", "messages|history|read-receipt|room-read")
```

to:

```go
	workload := fs.String("workload", "messages", "messages|thread|history|read-receipt|room-read")
```

- [ ] **Step 5: Add the `seed` thread case + `--parents-per-room` flag**

In `tools/loadgen/main.go`, in the seed subcommand (around line 127), add the flag near the existing `users` flag:

```go
	parentsPerRoom := fs.Int("parents-per-room", 0, "thread workload: parent messages seeded per room (0 = default 8; must match `loadgen max-rps` runtime default)")
```

Update the seed `--workload` usage string (line 127):

```go
	workload := fs.String("workload", "messages", "messages|thread|members|history|read-receipt|room-read|botroom")
```

Add to the seed `switch *workload`:

```go
	case "thread":
		return runSeedThread(ctx, cfg, *preset, *seed, *users, *parentsPerRoom)
```

- [ ] **Step 6: Add the `teardown` thread case**

In `tools/loadgen/main.go`, update the teardown `--workload` usage string (line 280):

```go
	workload := fs.String("workload", "messages", "messages|thread|members|history|room-read|botroom")
```

Add to the teardown `switch *workload`:

```go
	case "thread":
		return runTeardownThread(ctx, cfg, *preset, *seed)
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `make test SERVICE=loadgen 2>&1 | tail -20`
Expected: PASS for both `TestRunMaxRPS_Thread*`. Run `make build SERVICE=loadgen` — expected: success.

- [ ] **Step 8: Commit**

```bash
git add tools/loadgen/main.go tools/loadgen/maxrps.go tools/loadgen/maxrps_thread_test.go
git commit -m "feat(loadgen): wire thread workload into seed/teardown/max-rps"
```

---

## Task 7: Integration test — parents resolve in Cassandra

**Files:**
- Create: `tools/loadgen/thread_seed_integration_test.go`

First confirm the existing integration harness shape:

Run: `grep -rln "go:build integration" tools/loadgen/*.go` and read one (e.g. the history seed integration test) for the `TestMain`, `testutil.CassandraKeyspace`, and `msgbucket` setup pattern. Mirror it — do not invent container setup.

- [ ] **Step 1: Write the integration test**

Create `tools/loadgen/thread_seed_integration_test.go` (adapt the keyspace/DDL bootstrap to match the existing history integration test in this package — the snippet below assumes `testutil.CassandraKeyspace` returns a session against a keyspace with the message tables already created by the package's integration `TestMain`/setup; if the existing test applies DDL inline, copy that):

```go
//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestSeedThreadParents_Integration(t *testing.T) {
	_, session, _ := testutil.CassandraKeyspace(t, "loadgen_thread")
	// Ensure the message tables exist (reuse the package's DDL helper used by the
	// history integration test; replace ensureMessageTables with that helper).
	ensureMessageTables(t, session)

	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	tf := BuildThreadFixtures(&p, 42, 2, "site-a")

	sizer := msgbucket.New(72 * time.Hour)
	ctx := context.Background()
	now := time.Now().UTC()
	count, err := SeedThreadParents(ctx, session, sizer, &tf, "site-a", now)
	require.NoError(t, err)
	assert.Greater(t, count, 0)

	// Every seeded parent must be readable by ID (what message-gatekeeper does).
	for _, list := range tf.ParentsByRoom {
		for _, pm := range list {
			var got cassandra.Message
			err := session.Query(
				`SELECT message_id, room_id, created_at FROM messages_by_id WHERE message_id = ?`,
				pm.MessageID,
			).WithContext(ctx).Scan(&got.MessageID, &got.RoomID, &got.CreatedAt)
			require.NoErrorf(t, err, "parent %s must be readable", pm.MessageID)
			assert.Equal(t, pm.MessageID, got.MessageID)
		}
	}
}
```

If the package has no `ensureMessageTables` helper, replace that line with the exact DDL-apply call the existing history integration test uses (find it: `grep -rn "messages_by_id\|CREATE TABLE\|init.*cql" tools/loadgen/*integration*`). Do not duplicate DDL if a helper exists.

- [ ] **Step 2: Run the integration test**

Run: `make test-integration SERVICE=loadgen 2>&1 | tail -30`
Expected: PASS (requires Docker). If `cassandra.Message` field names differ, adjust the `Scan` targets to match `pkg/model/cassandra`.

- [ ] **Step 3: Commit**

```bash
git add tools/loadgen/thread_seed_integration_test.go
git commit -m "test(loadgen): integration coverage for thread parent seeding"
```

---

## Task 8: README documentation

**Files:**
- Modify: `tools/loadgen/README.md`

- [ ] **Step 1: Read the README to find the messages workload section**

Run: `grep -n "## \|max-rps\|workload=messages\|### " tools/loadgen/README.md | head -40`
Identify where the `messages` and `history` workloads are documented; insert the thread section adjacent to `messages`.

- [ ] **Step 2: Add the thread-reply workload section**

Insert (adjust heading level to match surrounding sections):

```markdown
### Thread-reply workload (`thread`)

Measures the maximum sustainable RPS for **sending thread replies**, directly
comparable to the `messages` workload on the same box. A thread reply costs more
than a plain send because `message-gatekeeper` issues a synchronous
`GetMessageByID` RPC to history-service to resolve the parent (extra E1 latency),
and `message-worker` writes `thread_messages_by_thread` plus thread-metadata
fan-out (extra E2 latency).

**Frontdoor only.** The unique thread cost lives on the gatekeeper path, so the
`thread` workload always uses frontdoor injection and ignores `--inject`.

**Parents must be pre-seeded.** The gatekeeper fetches the parent, so each reply
must reference a real message. `seed --workload=thread` writes
`--parents-per-room` (default 8) parent messages per room into Cassandra
(`messages_by_room` + `messages_by_id`). Requires `CASSANDRA_HOSTS` and the same
`MESSAGE_BUCKET_HOURS` as the running services.

Quick start (single site):

​```bash
# 1. Seed rooms/subs/keys (Mongo) + parents (Cassandra). Pick the same --seed
#    and --parents-per-room you will run with (defaults: seed 42, 8 parents).
loadgen seed --workload=thread --preset=medium --seed=42

# 2. Ramp the thread-reply send path.
loadgen max-rps --workload=thread --preset=medium --seed=42

# 3. (optional) Compare against plain sends on the same box.
loadgen max-rps --workload=messages --preset=medium --seed=42 --inject=frontdoor

# 4. Clean up (TRUNCATEs message tables + clears Mongo fixtures).
loadgen teardown --workload=thread --preset=medium
​```

`--seed` and `--parents-per-room` MUST match between `seed` and `max-rps`: the
ramp rebuilds the parent IDs from the seed to reference them, so a mismatch makes
every reply target a non-existent parent and the gatekeeper rejects the run.
```

(Replace the zero-width `​` placeholders around code fences with real triple-backticks; they are shown here only to nest inside this plan.)

- [ ] **Step 3: Verify no client-api doc change is needed**

The thread workload adds no client-facing handler (it publishes existing `SendMessageRequest` on the existing `MsgSend` subject). No `docs/client-api.md` change. Confirm by re-reading the CLAUDE.md client-facing-handler rule — this task touches only `tools/loadgen`.

- [ ] **Step 4: Commit**

```bash
git add tools/loadgen/README.md
git commit -m "docs(loadgen): document thread-reply max-rps workload"
```

---

## Task 9: Full verification + push

- [ ] **Step 1: Lint, unit tests, build**

Run: `make lint 2>&1 | tail -20`
Expected: no findings.

Run: `make test SERVICE=loadgen 2>&1 | tail -20`
Expected: all loadgen tests PASS, coverage for new files ≥ 80% (unit tests cover fixtures, generator branch, workload label, dispatch).

Run: `make build SERVICE=loadgen 2>&1 | tail -5`
Expected: success.

- [ ] **Step 2: Integration tests**

Run: `make test-integration SERVICE=loadgen 2>&1 | tail -30`
Expected: PASS (Docker required).

- [ ] **Step 3: SAST**

Run: `make sast 2>&1 | tail -20`
Expected: no medium+ findings in the new files.

- [ ] **Step 4: Push the branch**

```bash
git push -u origin claude/thread-message-perf-loadgen-h8nqsl
```

(Retry on network error with backoff 2s/4s/8s/16s. Do NOT open a PR unless the user asks.)

---

## Self-Review Notes (verified against the spec)

- **Frontdoor-only:** `newThreadWorkload` hard-codes `InjectFrontdoor`; `max-rps` thread case never reads `--inject`. ✔ (spec decision 2)
- **Parents pre-seeded in Cassandra via history-seed machinery:** Task 2 reuses `writePlannedMessage`. ✔ (decision 3)
- **8 parents/room default, configurable:** `defaultParentsPerRoom`, `--parents-per-room`. ✔ (decision 4)
- **Reuse messages presets:** `BuiltinPreset`, `BuildFixtures`. ✔ (decision 6)
- **rpsWorkload interface reuse:** `threadWorkload` implements `RunStep`/`Label`; reuses `buildMessagesInputs`/`msgCounters`/`diffCounters`. ✔ (decision 5)
- **No thread rooms pre-seeded in Mongo:** seed writes only parents to Cassandra; warmup absorbs first-reply thread-room creation. ✔ (spec §Architecture)
- **No client-api change:** Task 8 step 3. ✔
- **Type consistency:** `ThreadFixtures.ParentsByRoom map[string][]threadParent` is used identically in `GeneratorConfig`, `threadWorkload`, and the seed; `threadParent` fields (`MessageID`/`SenderID`/`SenderAccount`/`SenderEngName`) are consistent across Tasks 1–5. ✔
- **Seed/run agreement:** README + the `max-rps` thread case both rebuild fixtures from `--seed`; the parents-per-room mismatch risk is documented. (Known sharp edge — both default to 8.) ✔

**Open verification item for the executor:** confirm the exact in-package fake `Publisher` / `NewMetrics` / `NewCollector` / `lastToken` / `newE2Handler` / `gatheredCounterValue` / `newNatsCorePublisher` / `Publisher` interface names by grepping before writing Task 3/4 tests — they are referenced from existing `maxrps_messages*.go` and must be reused verbatim, not redeclared.
