# MongoDB Degraded Start Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the eight services whose primary datastore is not MongoDB start while MongoDB is unreachable, instead of crashlooping until it returns.

**Architecture:** A new `mongoutil.WithDegradedStart()` Connect option downgrades an unreachable MongoDB at startup from an error to a warning; the eight in-scope services pass it and nothing else about them changes. Because degraded start makes it routine for `message-worker` to run without asserting the unique indexes its own write path depends on, `room-service` (fail-fast, already the owner of the `thread_subscriptions` unique key) also owns `thread_rooms.parentMessageId`, and `message-worker` keeps creating both with the identical spec **and gates each document-creating write on the index being confirmed** (`indexGate`), because a degraded pod resumes consuming the moment MongoDB returns, before the crashlooping owner has restarted. (An earlier revision of this plan had `message-worker` verify-and-warn only; review showed that leaves the recovery window open. The steps below were updated to the shipped design.)

**Tech Stack:** Go 1.25, `go.mongodb.org/mongo-driver/v2` v2.7.0, `stretchr/testify`, `testcontainers-go` via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-09-10-mongodb-degraded-start-design.md`

## Global Constraints

- Branch: `claude/services-resilience-mongodb-down-wdhebq`. Never push elsewhere.
- TDD is mandatory: write the failing test, run it, watch it fail, then implement. Never write implementation before its test.
- Use `make` targets only — never raw `go` commands. `make lint`, `make test`, `make test-integration SERVICE=<name>`, `make sast`.
- All tests run with `-race` (the Makefile handles it).
- Integration tests use the `//go:build integration` tag and live in the same package.
- **Minimize test-code churn.** This branch must stay cheap to rebase. Where an existing test helper can absorb a change, change the helper — never edit N call sites. Task 3 depends on this.
- Errors wrap with context: `fmt.Errorf("short description: %w", err)`. Never bare `err`.
- Logging is `log/slog` with key-value fields. Never log credentials — connection strings go through `sanitizeURI`.
- `pkg/mongoutil` is a shared package: 90% coverage target.
- Do not add third-party dependencies.
- Do not create a pull request unless explicitly asked.

---

### Task 1: `mongoutil.WithDegradedStart`

**Files:**
- Modify: `pkg/mongoutil/observability.go:23-34` (add `degradedStart` to `connectConfig`)
- Modify: `pkg/mongoutil/mongo.go:36-66` (the `connect` function)
- Test: `pkg/mongoutil/mongo_test.go` (unit, no I/O)
- Test: `pkg/mongoutil/mongo_integration_test.go` (behaviour, needs a socket)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `func WithDegradedStart() Option` in package `mongoutil`. Tasks 2 and 3 call it as `mongoutil.WithDegradedStart()`. It takes no arguments and returns `Option` (`func(*connectConfig)`).

- [ ] **Step 1: Write the failing unit test**

Append to `pkg/mongoutil/mongo_test.go`:

```go
func TestWithDegradedStart(t *testing.T) {
	t.Run("unset by default", func(t *testing.T) {
		cfg := newConnectConfig()
		assert.False(t, cfg.degradedStart)
	})

	t.Run("sets the flag", func(t *testing.T) {
		cfg := newConnectConfig(WithDegradedStart())
		assert.True(t, cfg.degradedStart)
	})

	t.Run("composes with other options", func(t *testing.T) {
		cfg := newConnectConfig(
			WithDegradedStart(),
			WithReadPreference(readpref.Primary()),
			WithMaxPoolSize(7),
		)
		assert.True(t, cfg.degradedStart)
		require.NotNil(t, cfg.readPref)
		assert.Equal(t, readpref.PrimaryMode, cfg.readPref.Mode())
		require.NotNil(t, cfg.maxPoolSize)
		assert.EqualValues(t, 7, *cfg.maxPoolSize)
	})
}
```

- [ ] **Step 2: Run the unit test to verify it fails**

Run: `make test SERVICE=pkg/mongoutil`

Expected: FAIL — compile error, `undefined: WithDegradedStart` and `cfg.degradedStart undefined`.

- [ ] **Step 3: Write the failing integration test**

Append to `pkg/mongoutil/mongo_integration_test.go`. `127.0.0.1:1` is a port nothing listens on, so server selection times out; the short `ServerSelectionTimeout` keeps the test fast.

```go
// unreachableMongo is an address nothing listens on, with a server-selection
// bound short enough to keep these tests quick.
const unreachableMongo = "mongodb://127.0.0.1:1/?connectTimeoutMS=200"

func fastFailPool() PoolConfig {
	return PoolConfig{MaxPoolSize: 1, ServerSelectionTimeout: 300 * time.Millisecond}
}

func TestConnect_UnreachableMongo_FailsWithoutDegradedStart(t *testing.T) {
	client, err := Connect(context.Background(), unreachableMongo, "", "",
		WithPool(fastFailPool()))
	require.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "mongo ping")
}

func TestConnect_UnreachableMongo_SucceedsWithDegradedStart(t *testing.T) {
	client, err := Connect(context.Background(), unreachableMongo, "", "",
		WithPool(fastFailPool()), WithDegradedStart())
	require.NoError(t, err)
	require.NotNil(t, client)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	// The client is usable in the sense that calls fail cleanly rather than panic.
	err = client.Database("degraded").Collection("docs").
		FindOne(context.Background(), bson.M{"_id": "x"}).Err()
	require.Error(t, err)
	assert.NotErrorIs(t, err, mongo.ErrNoDocuments)
}

func TestConnect_ReachableMongo_DegradedStartUnchangedHappyPath(t *testing.T) {
	ctx := context.Background()
	client, err := Connect(ctx, testutil.MongoURI(t), "", "", WithDegradedStart())
	require.NoError(t, err)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	db := client.Database("mongoutil_degraded_start_test")
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	_, err = db.Collection("docs").InsertOne(ctx, bson.M{"_id": "x"})
	require.NoError(t, err)
	n, err := db.Collection("docs").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}
```

Add `"time"` and `"go.mongodb.org/mongo-driver/v2/mongo"` to that file's imports.

- [ ] **Step 4: Run the integration test to verify it fails**

Run: `make test-integration SERVICE=pkg/mongoutil`

Expected: FAIL — compile error, `undefined: WithDegradedStart`.

- [ ] **Step 5: Add the config field**

In `pkg/mongoutil/observability.go`, add the field to `connectConfig` (after `writeConcern`):

```go
	// degradedStart keeps an unreachable MongoDB from failing Connect. Set by
	// WithDegradedStart, for services whose primary datastore is not MongoDB.
	degradedStart bool
```

- [ ] **Step 6: Add the option**

In `pkg/mongoutil/mongo.go`, below `ConnectRead`:

```go
// WithDegradedStart downgrades an unreachable MongoDB at startup from an error
// to a warning: Connect returns a usable client and the service starts. For a
// service whose primary datastore is not MongoDB — the driver reconnects on its
// own (SDAM) once MongoDB returns, and the call sites already handle its errors
// through breakers, L2 caches and fail-open paths.
//
// Only reachability stops being a startup gate. An unparseable URI, a TLS
// failure or an instrumentation failure still returns an error, so genuine
// misconfiguration still stops the service.
func WithDegradedStart() Option {
	return func(c *connectConfig) { c.degradedStart = true }
}
```

- [ ] **Step 7: Rewrite the ping branch in `connect`**

Replace the block in `pkg/mongoutil/mongo.go` that currently reads:

```go
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		runCleanup(cleanup)
		return nil, fmt.Errorf("mongo ping: %w", err)
	}
	if cleanup != nil {
		cleanups.Store(client, cleanup)
	}
```

with:

```go
	// Stored before the ping: the degraded path below keeps the client, so the
	// client must already own its instrumentation teardown or Disconnect at
	// shutdown would not flush the SDK's pool metrics.
	if cleanup != nil {
		cleanups.Store(client, cleanup)
	}
	if err := client.Ping(ctx, nil); err != nil {
		if !cfg.degradedStart {
			Disconnect(context.Background(), client)
			return nil, fmt.Errorf("mongo ping: %w", err)
		}
		slog.Warn("mongo unreachable at startup; continuing in degraded mode",
			"uri", sanitizeURI(uri), "error", err)
		return client, nil
	}
```

`runCleanup` may now be unused — if `make lint` reports it, delete it and its call sites.

- [ ] **Step 8: Run both test suites**

Run: `make test SERVICE=pkg/mongoutil && make test-integration SERVICE=pkg/mongoutil`

Expected: PASS. In the integration output you should see the `mongo unreachable at startup; continuing in degraded mode` warn line, with a `uri` field of `mongodb://127.0.0.1:1` and **no** credentials.

- [ ] **Step 9: Lint**

Run: `make lint`

Expected: clean. If `runCleanup` is now unused, remove it and re-run.

- [ ] **Step 10: Commit**

```bash
git add pkg/mongoutil/
git commit -m "feat(mongoutil): add WithDegradedStart for non-MongoDB-primary services

An unreachable MongoDB at startup becomes a warning rather than an error, so
a service whose primary datastore is elsewhere can start and serve from its
caches. Misconfiguration still fails Connect.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018LS6QbdvKHnDn6uifN4jDg"
```

---

### Task 2: Wire the eight degradable services

**Files:**
- Modify: `broadcast-worker/main.go:179-180`
- Modify: `message-gatekeeper/main.go:128-129`
- Modify: `message-worker/main.go:148-149`
- Modify: `notification-worker/main.go:118-119`
- Modify: `history-service/cmd/main.go:144-148`
- Modify: `search-service/main.go:236-237`
- Modify: `tcard-service/main.go:81-82`
- Modify: `user-presence-service/main.go:133-134`
- Modify: `docs/health-probes.md`

**Interfaces:**
- Consumes: `mongoutil.WithDegradedStart()` from Task 1.
- Produces: nothing consumed by later tasks.

There is no new test here. These are `main.go` wiring lines with no unit-testable seam — the behaviour they select is already covered by Task 1's tests. Do not invent a test that asserts a literal option is present in a `main` function; it would assert the diff, not the behaviour.

- [ ] **Step 1: Wire the seven single-line call sites**

In each of `broadcast-worker/main.go`, `message-gatekeeper/main.go`, `message-worker/main.go`, `notification-worker/main.go`, `search-service/main.go`, `tcard-service/main.go`, `user-presence-service/main.go`, the second line of the `mongoutil.Connect(...)` call currently reads:

```go
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref))
```

Change it to:

```go
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref),
		// This service's primary datastore is not MongoDB: start and serve from
		// the L2 caches rather than crashloop through an outage.
		mongoutil.WithDegradedStart())
```

Leave the `if err != nil` block below each one exactly as it is — it still fires for misconfiguration.

- [ ] **Step 2: Wire history-service (multi-line form)**

In `history-service/cmd/main.go:144-148`, change:

```go
	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password,
		mongoutil.WithPool(cfg.Pool),
		mongoutil.WithObservability(sdk),
		mongoutil.WithReadPreference(readPref),
	)
```

to:

```go
	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password,
		mongoutil.WithPool(cfg.Pool),
		mongoutil.WithObservability(sdk),
		mongoutil.WithReadPreference(readPref),
		// Cassandra is the primary datastore here: serve history through an
		// outage rather than crashloop.
		mongoutil.WithDegradedStart(),
	)
```

- [ ] **Step 3: Verify every service still builds**

Run:

```bash
for s in broadcast-worker message-gatekeeper message-worker notification-worker history-service search-service tcard-service user-presence-service; do make build SERVICE=$s || echo "BUILD FAILED: $s"; done
```

Expected: eight successful builds, no `BUILD FAILED` lines.

- [ ] **Step 4: Confirm no out-of-scope service was touched**

Run: `git diff --name-only`

Expected: exactly the eight `main.go` files above. If `room-service/`, `user-service/`, `inbox-worker/`, `roomlist-worker/`, `room-worker/`, `admin-service/`, `portal-service/`, `botplatform-service/`, `upload-service/`, `media-service/`, any `bot-*`, any `teams-*`, `hr-sync-worker` or `search-sync-worker` appears, revert it — those must keep failing fast.

- [ ] **Step 5: Document it**

Add this section to `docs/health-probes.md`, immediately after the "What readiness checks — and why only NATS" section:

```markdown
## MongoDB degraded start

Eight services pass `mongoutil.WithDegradedStart()`, so an unreachable MongoDB
at startup is a warning rather than a fatal error and the pod starts:
`broadcast-worker`, `history-service`, `message-gatekeeper`, `message-worker`,
`notification-worker`, `search-service`, `tcard-service`,
`user-presence-service`.

Each has a primary datastore that is not MongoDB (Cassandra, Elasticsearch,
Valkey, or an in-process cache) and reaches MongoDB through Valkey-backed L2
tiers, fail-open enrichment, or NAK-and-retry. The L2 tiers are external and
shared, so a pod that starts cold during an outage still gets warm cache hits —
it serves real traffic rather than starting only to fail everything. The driver
reconnects on its own (SDAM) once MongoDB returns.

Every other service still exits when MongoDB is unreachable: MongoDB is their
job, and a pod that cannot reach it can do no useful work. Membership is decided
in code, not by an env var.

**The probes do not change.** `/healthz` stays process-up only and `/readyz`
stays NATS-only, for the reason given above: MongoDB is shared by every replica,
so probing it in readiness would flip every pod `NotReady` at once on a blip. A
pod running degraded is genuinely serving traffic from its caches, so reporting
it `NotReady` would be wrong.

A degraded start is visible in the logs as
`mongo unreachable at startup; continuing in degraded mode`.
```

- [ ] **Step 6: Lint**

Run: `make lint`

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add broadcast-worker/main.go message-gatekeeper/main.go message-worker/main.go \
  notification-worker/main.go history-service/cmd/main.go search-service/main.go \
  tcard-service/main.go user-presence-service/main.go docs/health-probes.md
git commit -m "feat: start the eight non-MongoDB-primary services when MongoDB is down

Each serves its primary job from Cassandra, Elasticsearch, Valkey or an
in-process cache, and reaches MongoDB through L2 tiers, fail-open enrichment
or NAK-and-retry. MongoDB-primary services still fail fast.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018LS6QbdvKHnDn6uifN4jDg"
```

---

### Task 3: Move `thread_rooms.parentMessageId` ownership to room-service

**Why this is in the same change:** `message-worker` is the sole creator of that unique index, and `CreateThreadRoom` (`message-worker/store_mongo.go:55-66`) blind-inserts a fresh UUIDv7 with no read-before-insert, relying entirely on the duplicate-key error to tell a first reply from a subsequent one. Its `EnsureIndexes` call site already warns and continues (`message-worker/main.go:203`). Before Task 2, skipping the index needed a narrow race; after Task 2, "MongoDB down at startup" is a routine path to skipping it, and the damage lands when MongoDB recovers — one thread room per reply, scattered across `thread_messages_by_thread` partitions, permanently.

**Files:**
- Modify: `room-service/store_mongo.go` (one line inside `EnsureIndexes`)
- Modify: `message-worker/store_mongo.go:38-52` (`EnsureIndexes`)
- Modify: `message-worker/integration_test.go:194` (`setupMongo` helper — **one edit, no call-site edits**)
- Test: `room-service/integration_test.go`
- Modify: `CLAUDE.md` (§6 MongoDB)

**Interfaces:**
- Consumes: nothing from Tasks 1-2.
- Produces: nothing consumed by later tasks. `(*threadStoreMongo).EnsureIndexes(ctx context.Context) error` keeps its exact signature — that is what lets the nine existing call sites stay untouched.

- [ ] **Step 1: Write the failing room-service test**

Append to `room-service/integration_test.go`. This mirrors the existing
`TestEnsureIndexes_ThreadSubsDropsLegacyAndCreatesUnique_Integration`
(`:4673`) — same `setupMongo`/`NewMongoStore` setup and the same
`cur.All` into `[]bson.M` index-inspection idiom. There is no shared
index-assertion helper in this file; do not add one for two call sites.

```go
// room-service owns the thread_rooms unique key: message-worker's
// CreateThreadRoom blind-inserts and reads the duplicate-key error as "this
// thread already exists", and message-worker now starts with MongoDB down, so
// it can no longer be relied on to assert the constraint it depends on.
func TestEnsureIndexes_ThreadRoomsParentMessageIDUnique_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	require.NoError(t, store.EnsureIndexes(ctx))

	cur, err := db.Collection("thread_rooms").Indexes().List(ctx)
	require.NoError(t, err)
	var idxs []bson.M
	require.NoError(t, cur.All(ctx, &idxs))
	unique := make(map[string]bool, len(idxs))
	for _, ix := range idxs {
		if n, ok := ix["name"].(string); ok {
			u, _ := ix["unique"].(bool)
			unique[n] = u
		}
	}
	assert.True(t, unique["parentMessageId_1"],
		"thread_rooms parentMessageId_1 must exist and be unique, got %v", idxs)
}

// A pre-existing non-unique index on the same keys must be repaired, not left
// in place — EnsureIndexWithRepair drops and recreates it to the unique spec.
func TestEnsureIndexes_ThreadRoomsRepairsNonUniqueIndex_Integration(t *testing.T) {
	db := setupMongo(t)
	store := NewMongoStore(db)
	ctx := context.Background()

	// Same keys, no constraint: the name is parentMessageId_1 either way, which
	// is what makes this a spec conflict rather than a second index.
	_, err := db.Collection("thread_rooms").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "parentMessageId", Value: 1}},
	})
	require.NoError(t, err)

	require.NoError(t, store.EnsureIndexes(ctx))

	cur, err := db.Collection("thread_rooms").Indexes().List(ctx)
	require.NoError(t, err)
	var idxs []bson.M
	require.NoError(t, cur.All(ctx, &idxs))
	unique := make(map[string]bool, len(idxs))
	for _, ix := range idxs {
		if n, ok := ix["name"].(string); ok {
			u, _ := ix["unique"].(bool)
			unique[n] = u
		}
	}
	assert.True(t, unique["parentMessageId_1"],
		"a non-unique parentMessageId_1 must be repaired to unique, got %v", idxs)
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test-integration SERVICE=room-service`

Expected: FAIL on both tests — `thread_rooms parentMessageId_1 must exist and be unique`.

- [ ] **Step 3: Add the index to room-service**

In `room-service/store_mongo.go`, inside `EnsureIndexes`, directly above the `// room-service owns the thread_subscriptions unique key.` comment block, add:

```go
	// room-service owns the thread_rooms unique key too. message-worker's
	// CreateThreadRoom blind-inserts and reads the duplicate-key error as "this
	// thread already exists", so without this constraint every reply creates its
	// own thread room. It cannot live in message-worker: that service now starts
	// with MongoDB down, so it can no longer be relied on to assert it.
	unique(s.threadRooms, "thread_rooms (parentMessageId)",
		bson.D{{Key: "parentMessageId", Value: 1}})
```

The existing `unique` helper already routes through `mongoutil.EnsureIndexWithRepair`, which is what makes the repair test pass.

- [ ] **Step 4: Run the room-service test to verify it passes**

Run: `make test-integration SERVICE=room-service`

Expected: PASS, both new tests.

- [ ] **Step 5: Keep message-worker's tests green with ONE helper edit**

All nine `EnsureIndexes` call sites in `message-worker/integration_test.go` (`:525`, `:657`, `:746`, `:833`, `:880`, `:892`, `:947`, `:1099`, `:2100`) obtain their database from the same three-line helper. Change **only** the helper at `message-worker/integration_test.go:194`:

```go
func setupMongo(t *testing.T) *mongo.Database {
	db := testutil.MongoDB(t, "message_worker_test")
	// thread_rooms.parentMessageId is room-service's index in production. These
	// tests stand in for room-service, not for message-worker, whose EnsureIndexes
	// only verifies it.
	_, err := db.Collection("thread_rooms").Indexes().CreateOne(context.Background(),
		mongo.IndexModel{
			Keys:    bson.D{{Key: "parentMessageId", Value: 1}},
			Options: options.Index().SetUnique(true),
		})
	require.NoError(t, err)
	return db
}
```

Do **not** touch any of the nine call sites. Each `require.NoError(t, ...EnsureIndexes(ctx))` line stays verbatim and still asserts something real — that the startup gates return nil once the indexes are confirmed. This one-file, one-function edit, plus the new fresh-database gate tests, is the whole test-churn budget for this task.

- [ ] **Step 6: Gate message-worker's writes on the confirmed indexes**

In `message-worker/store_mongo.go`, add an `indexGate` (`ensure` func, a
`singleflight.Group`, an `atomic.Bool`) whose `Ready(ctx)` runs the ensure
once under its own 30s deadline, detached from the caller's cancellation,
shares an in-flight attempt with concurrent callers, and returns the error so
the handler NAKs. Give `threadStoreMongo` two gates built in
`newThreadStoreMongo` with the same specs `room-service` uses, through the
non-destructive `mongoutil.EnsureIndex` (unique; never repairs — a conflicting
index closes the gate until `room-service` repairs): `parentIndex` for
`thread_rooms.parentMessageId` and `subIndex` for
`thread_subscriptions.(threadRoomId,userAccount)`. `EnsureIndexes` asks both
gates (best-effort at startup); `CreateThreadRoom` asks `parentIndex` before
its insert; `InsertThreadSubscription`, `UpsertThreadSubscription` and
`MarkThreadSubscriptionMention` ask `subIndex` before theirs. Unit-test the
gate (once, retry after failure, concurrent callers ensure once, waiters share
a failed attempt, bounded and detached context) in `indexgate_test.go`.

- [ ] **Step 7: Run message-worker's tests**

Run: `make test SERVICE=message-worker && make test-integration SERVICE=message-worker`

Expected: PASS, with **no changes to any of the nine call sites**; the only
additions to `message-worker/integration_test.go` are the `setupMongo` helper
edit and the new fresh-database tests for the two gates.

- [ ] **Step 8: Record the rule in CLAUDE.md**

In `CLAUDE.md`, at the end of the `### MongoDB` subsection of Section 6, add:

```markdown
- **Degraded start.** Eight services pass `mongoutil.WithDegradedStart()` so an
  unreachable MongoDB at startup warns instead of exiting: `broadcast-worker`,
  `history-service`, `message-gatekeeper`, `message-worker`,
  `notification-worker`, `search-service`, `tcard-service`,
  `user-presence-service`. Every other service MUST keep failing fast — MongoDB
  is their job. See `docs/health-probes.md`.
- **A degradable service must not be the sole creator of an index whose absence
  is a correctness hazard for its own write path.** It starts with MongoDB down,
  so its `EnsureIndexes` is skipped exactly when an outage makes that most
  likely, and the damage lands on recovery. Such an index must also be owned
  by a fail-fast service, and the degradable one keeps creating it with the
  identical spec and gates the write that relies on it on the index being
  confirmed (`indexGate`) — as `message-worker` does for
  `thread_rooms.parentMessageId` and `thread_subscriptions`, both owned by
  `room-service`. A performance-only index may stay: its absence costs collection
  scans until the next healthy restart, not integrity.
```

- [ ] **Step 9: Lint and commit**

Run: `make lint`

Expected: clean.

```bash
git add room-service/store_mongo.go room-service/integration_test.go \
  message-worker/store_mongo.go message-worker/integration_test.go CLAUDE.md
git commit -m "fix: move the thread_rooms unique index to room-service

message-worker is the sole creator of thread_rooms.parentMessageId, and
CreateThreadRoom blind-inserts and reads the duplicate-key error as 'this
thread already exists'. Now that message-worker starts with MongoDB down, its
warn-and-continue EnsureIndexes would skip that index exactly when an outage
made it most likely, and every reply would create its own thread room once
MongoDB recovered.

room-service fails fast and already owns the sibling thread_subscriptions
unique key, so it takes this one too; message-worker verifies and warns.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018LS6QbdvKHnDn6uifN4jDg"
```

---

### Task 4: Full verification and push

**Files:** none modified.

**Interfaces:**
- Consumes: Tasks 1-3 complete.
- Produces: a pushed branch.

- [ ] **Step 1: Full unit suite**

Run: `make test`

Expected: PASS. Any failure here is a regression from Tasks 1-3 — fix it before continuing, do not push.

- [ ] **Step 2: Integration suites for every touched service**

Run:

```bash
for s in pkg/mongoutil room-service message-worker broadcast-worker message-gatekeeper \
         notification-worker history-service search-service tcard-service user-presence-service; do
  make test-integration SERVICE=$s || echo "INTEGRATION FAILED: $s"
done
```

Expected: no `INTEGRATION FAILED` lines.

- [ ] **Step 3: Lint and SAST**

Run: `make lint && make sast`

Expected: both clean. SAST is a blocking CI gate — do not push past it.

- [ ] **Step 4: Review the whole diff adversarially**

Run: `git diff master...HEAD`

Check specifically:
- No out-of-scope service gained `WithDegradedStart`.
- No credentials in any new log line — the warn uses `sanitizeURI(uri)`.
- `message-worker/integration_test.go` shows only the `setupMongo` helper changed.
- No `docs/reviews/` files are present on the branch.

- [ ] **Step 5: Push**

```bash
git push -u origin claude/services-resilience-mongodb-down-wdhebq
```

Retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s) on network failure only.

- [ ] **Step 6: Report**

Report to the user what passed, quoting the actual command output — not "tests pass". If anything was skipped (e.g. an integration suite that could not start its container), say so explicitly.
