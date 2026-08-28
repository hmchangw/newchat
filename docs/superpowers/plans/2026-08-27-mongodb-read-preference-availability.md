# MongoDB Read-Preference Availability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move every MongoDB read that can safely be served by a secondary off the `primary` read preference, so reads survive a primary-down incident.

**Architecture:** Each service gains a `READ_PREFERENCE` env var parsed by the existing `mongoutil.ParseReadPreference`, passed to `mongoutil.WithReadPreference` at connect time (client-level) or to an existing per-collection override. No new abstraction is introduced — seven services already follow this exact pattern and consistency is worth more than saving five lines per service.

**Tech Stack:** Go 1.25, `go.mongodb.org/mongo-driver/v2`, `caarlos0/env`, `stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-08-27-mongodb-read-preference-availability-design.md`

## Global Constraints

- **TDD is mandatory** (CLAUDE.md §4): write the failing test, run it, confirm it fails, then implement. Never write implementation before its test.
- **Never run raw `go` commands** (CLAUDE.md §2). Use `make test SERVICE=<name>`, `make lint`, `make fmt`.
- Config comes from env vars parsed by `caarlos0/env` into a typed struct; never `os.Getenv` in service code.
- Every non-critical config field gets an `envDefault`.
- `mongoutil.ParseReadPreference("")` returns `readpref.Primary()` (`pkg/mongoutil/readpref.go:19-21`), so an unset var preserves today's behaviour. The `envDefault` is what changes each service.
- Error wrapping: `fmt.Errorf("short description: %w", err)`. Never bare `err`.
- Structured logging via `log/slog` only, key-value pairs, never interpolated strings.
- **No `docs/client-api.md` update is required by this plan.** No handler registration, request schema, response schema, or `pkg/model` struct changes. If a task appears to need one, stop — it has drifted from the spec.
- Every service touched must also get `READ_PREFERENCE` added to its `deploy/docker-compose.yml` so local dev matches production wiring.
- Commit after each task passes. Do not batch commits across tasks.

## Canonical patterns

Copy these verbatim; every task below instantiates one of them.

**Pattern A — client-level preference.** In the service's `config` struct:

```go
// ReadPreference routes reads to secondaries when the primary is unavailable;
// primaryPreferred is a no-op in steady state.
ReadPreference string `env:"READ_PREFERENCE" envDefault:"primaryPreferred"`
```

In `main.go`, immediately before the `mongoutil.Connect` call:

```go
readPref, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
if err != nil {
    slog.Error("invalid mongo read preference", "value", cfg.ReadPreference, "error", err)
    os.Exit(1)
}
```

Add `mongoutil.WithReadPreference(readPref)` to the `Connect` options, and after the connect succeeds:

```go
slog.Info("mongo read preference configured", "readPreference", readPref.Mode().String())
```

**Pattern B — config default test** (`package main` services):

```go
func TestConfig_ReadPreferenceDefault(t *testing.T) {
    // <required env vars for this service>
    require.NoError(t, os.Unsetenv("READ_PREFERENCE")) // the default only applies when unset

    cfg, err := env.ParseAs[config]()
    require.NoError(t, err)
    require.Equal(t, "primaryPreferred", cfg.ReadPreference)

    rp, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
    require.NoError(t, err)
    require.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}

func TestConfig_ReadPreferenceRejectsGarbage(t *testing.T) {
    _, err := mongoutil.ParseReadPreference("quorum")
    require.Error(t, err)
}
```

---

### Task 1: message-gatekeeper + message-worker — keep the send-and-store path alive

These two ship together: gatekeeper admits the message, message-worker persists it. Either alone leaves the path broken.

**Files:**
- Modify: `message-gatekeeper/main.go:28-53` (config struct), `message-gatekeeper/main.go:107` (connect call)
- Modify: `message-worker/main.go:47-49` (config struct), `message-worker/main.go:127` (connect call)
- Modify: `message-gatekeeper/deploy/docker-compose.yml`, `message-worker/deploy/docker-compose.yml`
- Create: `message-gatekeeper/config_test.go`, `message-worker/config_test.go` — **not** `main_test.go`, which is `//go:build integration` and would keep these unit tests out of `make test`

**Interfaces:**
- Consumes: `mongoutil.ParseReadPreference(string) (*readpref.ReadPref, error)`, `mongoutil.WithReadPreference(*readpref.ReadPref) Option` — both already exist.
- Produces: `config.ReadPreference string` on both services. Later tasks use the same field name.

- [ ] **Step 1: Write the failing test for message-gatekeeper**

Create `message-gatekeeper/config_test.go` (no build tag):

```go
func TestConfig_ReadPreferenceDefault(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	require.NoError(t, os.Unsetenv("READ_PREFERENCE")) // the default only applies when unset

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "primaryPreferred", cfg.ReadPreference,
		"gatekeeper authorises sends from Mongo; a primary-only read takes messaging down with the primary")

	rp, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	require.NoError(t, err)
	require.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}
```

Ensure the file imports `os`, `testing`, `github.com/caarlos0/env/v11`, `github.com/stretchr/testify/require`, `github.com/hmchangw/chat/pkg/mongoutil`, `go.mongodb.org/mongo-driver/v2/mongo/readpref`.

- [ ] **Step 2: Run it and confirm it fails**

Run: `make test SERVICE=message-gatekeeper`
Expected: FAIL — `cfg.ReadPreference` undefined (the field does not exist yet).

- [ ] **Step 3: Add the config field**

In `message-gatekeeper/main.go`, inside `type config struct`, after `MongoPassword`:

```go
	// ReadPreference routes reads to secondaries when the primary is unavailable.
	// primaryPreferred, not secondaryPreferred: the sub cache means Mongo is hit
	// only on a cold miss, which is exactly the just-joined-a-room case.
	ReadPreference     string `env:"READ_PREFERENCE" envDefault:"primaryPreferred"`
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `make test SERVICE=message-gatekeeper`
Expected: PASS.

- [ ] **Step 5: Wire the preference into the client**

In `message-gatekeeper/main.go`, replace line 107 and insert the parse above it:

```go
	readPref, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	if err != nil {
		slog.Error("invalid mongo read preference", "value", cfg.ReadPreference, "error", err)
		os.Exit(1)
	}
	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	slog.Info("mongo read preference configured", "readPreference", readPref.Mode().String())
```

- [ ] **Step 6: Repeat Steps 1-5 for message-worker**

Create `message-worker/config_test.go` (no build tag) — identical body, with this assertion message:

```go
	require.Equal(t, "primaryPreferred", cfg.ReadPreference,
		"the non-thread branch writes only Cassandra; a primary-only Mongo read blocks plain-message persistence")
```

Required env for the parse: `NATS_URL`, `SITE_ID`, `MONGO_URI` (same three).

Config field comment:

```go
	// ReadPreference routes reads to secondaries when the primary is unavailable.
	// Mongo writes precede the Cassandra write (handler.go:159-201), so an outage
	// aborts before persisting rather than persisting against a stale read.
	ReadPreference     string `env:"READ_PREFERENCE" envDefault:"primaryPreferred"`
```

Connect call is at `message-worker/main.go:127` and takes the same option list.

- [ ] **Step 7: Add the env var to both compose files**

In `message-gatekeeper/deploy/docker-compose.yml` and `message-worker/deploy/docker-compose.yml`, in the service `environment:` block next to the other `MONGO_*` entries:

```yaml
      - READ_PREFERENCE=${READ_PREFERENCE:-primaryPreferred}
```

- [ ] **Step 8: Lint, format, full test**

Run: `make fmt && make lint && make test SERVICE=message-gatekeeper && make test SERVICE=message-worker`
Expected: all clean, all pass.

- [ ] **Step 9: Commit**

```bash
git add message-gatekeeper message-worker
git commit -m "feat(mongo): primaryPreferred reads on the message send-and-store path

message-gatekeeper authorises sends from Mongo and message-worker's non-thread
branch writes only Cassandra, so both are read-only against Mongo on the plain
message path. Under the primary-only default the whole path dies with the
primary; primaryPreferred is a no-op in steady state and keeps it alive."
```

---

### Task 2: admin-service — client flip plus the transaction guard

Highest-risk task in the plan: flipping the client without the guard breaks password reset in **normal** operation, not just during an incident. Ships alone.

**Files:**
- Modify: `admin-service/config.go:25-27` (config struct)
- Modify: `admin-service/main.go:68` (connect call)
- Modify: `admin-service/store_mongo.go:249-259` (`withTransaction`)
- Modify: `admin-service/deploy/docker-compose.yml`
- Test: `admin-service/config_test.go`, `admin-service/integration_test.go`

**Interfaces:**
- Consumes: `Config.ReadPreference string` (added in this task), `mongoutil.WithReadPreference`.
- Produces: nothing later tasks depend on.

- [ ] **Step 1: Write the failing transaction test**

The driver defaults a transaction's read preference to the session's, which defaults to the **client's**, and MongoDB rejects a non-primary read preference inside a transaction. This test proves the guard holds. Add to `admin-service/integration_test.go` (build tag `//go:build integration`):

```go
func TestWithTransaction_SurvivesNonPrimaryClientReadPreference(t *testing.T) {
	// A primaryPreferred CLIENT must not leak into the transaction: MongoDB
	// rejects any non-primary read preference inside one.
	client, dbName := replicaSetClient(t, readpref.PrimaryPreferred())
	s := newStoreMongo(client.Database(dbName))

	ctx := context.Background()
	require.NoError(t, s.withTransaction(ctx, func(ctx context.Context) error {
		_, err := s.users.InsertOne(ctx, bson.M{"_id": "txn-guard-probe", "account": "probe"})
		return err
	}))
}
```

`replicaSetClient(t, rp)` is a helper this task adds to `admin-service/integration_test.go`; it connects to the shared replica-set container from `pkg/testutil/mongo_replicaset.go` with the given client read preference and returns an isolated database name. If the existing integration file already has an equivalent helper, use it rather than adding a second.

- [ ] **Step 2: Run it and confirm it fails**

Run: `make test-integration SERVICE=admin-service`
Expected: FAIL — the driver returns an error naming the read preference in the transaction.

- [ ] **Step 3: Add the transaction guard**

In `admin-service/store_mongo.go`, change `withTransaction` to pin the session's read preference:

```go
func (s *storeMongo) withTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	// The session — and therefore the transaction — inherits the client read
	// preference, and MongoDB rejects a non-primary one inside a transaction.
	// SessionOptionsBuilder has no SetDefaultReadPreference in driver v2 — the
	// transaction options are the seam. Pin primary here so the client stays free
	// to prefer a secondary for plain reads.
	sess, err := s.users.Database().Client().StartSession(
		options.Session().SetDefaultTransactionOptions(
			options.Transaction().SetReadPreference(readpref.Primary())))
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer sess.EndSession(ctx)
	_, err = sess.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		return nil, fn(ctx)
	})
	return err
}
```

Add imports `go.mongodb.org/mongo-driver/v2/mongo/options` and `go.mongodb.org/mongo-driver/v2/mongo/readpref` if absent.

- [ ] **Step 4: Run the integration test and confirm it passes**

Run: `make test-integration SERVICE=admin-service`
Expected: PASS.

- [ ] **Step 5: Write the failing config test**

Add to `admin-service/config_test.go`:

```go
func TestLoadConfig_ReadPreferenceDefault(t *testing.T) {
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	require.NoError(t, os.Unsetenv("READ_PREFERENCE")) // the default only applies when unset

	cfg, err := loadConfig()
	require.NoError(t, err)
	require.Equal(t, "primaryPreferred", cfg.ReadPreference)

	rp, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	require.NoError(t, err)
	require.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}
```

- [ ] **Step 6: Run it and confirm it fails**

Run: `make test SERVICE=admin-service`
Expected: FAIL — `cfg.ReadPreference` undefined.

- [ ] **Step 7: Add the config field and wire the client**

In `admin-service/config.go`, after `MongoUsername`:

```go
	// ReadPreference routes reads to secondaries when the primary is unavailable.
	// Transactions are pinned to primary independently — see storeMongo.withTransaction.
	ReadPreference        string `env:"READ_PREFERENCE" envDefault:"primaryPreferred"`
```

In `admin-service/main.go`, before line 68:

```go
	readPref, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	if err != nil {
		slog.Error("invalid mongo read preference", "value", cfg.ReadPreference, "error", err)
		os.Exit(1)
	}
	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk), mongoutil.WithReadPreference(readPref))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	slog.Info("mongo read preference configured", "readPreference", readPref.Mode().String())
```

- [ ] **Step 8: Add the compose env var**

In `admin-service/deploy/docker-compose.yml`:

```yaml
      - READ_PREFERENCE=${READ_PREFERENCE:-primaryPreferred}
```

- [ ] **Step 9: Verify and commit**

Run: `make fmt && make lint && make test SERVICE=admin-service && make test-integration SERVICE=admin-service`
Expected: all pass.

```bash
git add admin-service
git commit -m "feat(admin): primaryPreferred reads with an explicit transaction guard

A session inherits the client read preference and MongoDB rejects a non-primary
one inside a transaction, so withTransaction now pins primary explicitly.
Without that guard the client flip would break UpdateUserPasswordAndRevoke in
normal operation, not only during an incident."
```

---

### Task 3: room-service + user-service — client-level preference

These two already carry per-collection `*Secondary` overrides, but their plain handles are created with no collection options and therefore inherit the **client** preference, which is `primary` today. Only 12 of room-service's methods and 16 of user-service's 39 repo methods use a `*Secondary` handle; every other read — List Members among them — fails during an incident. The `*Secondary` overrides are unchanged by this task.

**Files:**
- Modify: `room-service/main.go:44-46` (config — the field already exists as `MongoReadPreference`), `room-service/main.go:193-194` (connect call)
- Modify: `user-service/config/config.go:18-20` (field already exists), `user-service/main.go:127-128` (connect call)
- Test: `room-service/config_test.go`, `user-service/config/config_test.go`

**Interfaces:**
- Consumes: `cfg.MongoReadPreference` (room-service) and `cfg.Mongo.ReadPreference` (user-service) — both already exist and already default to `secondaryPreferred`.
- Produces: a second config field on each service, `MongoClientReadPreference` / `Mongo.ClientReadPreference`, defaulting to `primaryPreferred`.

The existing field keeps driving the `*Secondary` collection clones. The new field drives the client. Two fields because they are genuinely two decisions: `secondaryPreferred` for the vetted staleness-tolerant handles, `primaryPreferred` for everything else.

- [ ] **Step 1: Write the failing test for room-service**

Add to `room-service/config_test.go`:

```go
func TestConfig_ClientReadPreferenceDefault(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	require.NoError(t, os.Unsetenv("MONGO_CLIENT_READ_PREFERENCE"))
	require.NoError(t, os.Unsetenv("MONGO_READ_PREFERENCE"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	// The per-collection override stays secondaryPreferred...
	require.Equal(t, "secondaryPreferred", cfg.MongoReadPreference)
	// ...while every handle without an override now falls back instead of failing.
	require.Equal(t, "primaryPreferred", cfg.MongoClientReadPreference)

	rp, err := mongoutil.ParseReadPreference(cfg.MongoClientReadPreference)
	require.NoError(t, err)
	require.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `make test SERVICE=room-service`
Expected: FAIL — `cfg.MongoClientReadPreference` undefined.

- [ ] **Step 3: Add the field and wire the client**

In `room-service/main.go`, next to `MongoReadPreference`:

```go
	// MongoClientReadPreference applies to every collection handle WITHOUT an
	// explicit override — plain handles inherit the client. primaryPreferred is a
	// no-op in steady state and keeps those reads alive when the primary is gone.
	MongoClientReadPreference string `env:"MONGO_CLIENT_READ_PREFERENCE" envDefault:"primaryPreferred"`
```

Replace the connect call at `room-service/main.go:193-194`:

```go
	clientReadPref, err := mongoutil.ParseReadPreference(cfg.MongoClientReadPreference)
	if err != nil {
		slog.Error("invalid mongo client read preference", "value", cfg.MongoClientReadPreference, "error", err)
		os.Exit(1)
	}
	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithPool(cfg.Pool), mongoutil.WithObservability(sdk),
		mongoutil.WithReadPreference(clientReadPref))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	slog.Info("mongo client read preference configured", "readPreference", clientReadPref.Mode().String())
```

Leave the existing `readPref` parse at `room-service/main.go:212-219` and the `NewMongoStore(db, WithReadPreference(readPref))` call untouched.

- [ ] **Step 4: Run the test and confirm it passes**

Run: `make test SERVICE=room-service`
Expected: PASS.

- [ ] **Step 5: Repeat for user-service**

In `user-service/config/config.go`, next to `ReadPreference`:

```go
	// ClientReadPreference applies to every collection handle WITHOUT an explicit
	// override — plain handles inherit the client.
	ClientReadPreference string `env:"MONGO_CLIENT_READ_PREFERENCE" envDefault:"primaryPreferred"`
```

Add it to the existing validation alongside `ReadPreference` at `user-service/config/config.go:268`:

```go
	if _, err := mongoutil.ParseReadPreference(cfg.Mongo.ClientReadPreference); err != nil {
		return fmt.Errorf("MONGO_CLIENT_READ_PREFERENCE: %w", err)
	}
```

Test in `user-service/config/config_test.go`:

```go
func TestLoad_DefaultsClientReadPreferenceToPrimaryPreferred(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	unsetEnv(t, "MONGO_CLIENT_READ_PREFERENCE")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "secondaryPreferred", cfg.Mongo.ReadPreference)
	assert.Equal(t, "primaryPreferred", cfg.Mongo.ClientReadPreference)
}
```

Reuse the existing `unsetEnv` helper in that file. If `Load()` needs more required vars, the parse error names them — add them as `t.Setenv` lines.

Wire both `user-service/main.go:127` and the second client at `user-service/main.go:138` with `mongoutil.WithReadPreference(clientReadPref)`.

- [ ] **Step 6: Add the compose env vars**

In `room-service/deploy/docker-compose.yml` and `user-service/deploy/docker-compose.yml`:

```yaml
      - MONGO_CLIENT_READ_PREFERENCE=${MONGO_CLIENT_READ_PREFERENCE:-primaryPreferred}
```

- [ ] **Step 7: Verify and commit**

Run: `make fmt && make lint && make test SERVICE=room-service && make test SERVICE=user-service`

```bash
git add room-service user-service
git commit -m "feat(mongo): client-level primaryPreferred for room-service and user-service

Their plain collection handles are created without options and inherit the
client preference, which was primary. The per-collection *Secondary overrides
are unchanged; this covers every read that had none — List Members included."
```

---

### Task 4: the remaining request/reply readers

Five services, one shape: Pattern A with `primaryPreferred`. Grouped because a reviewer accepting one accepts all five — the argument is identical.

**Files:**
- Modify: `botplatform-service/config.go:16-18`, `botplatform-service/main.go:58`
- Modify: `upload-service/main.go:43-45`, `upload-service/main.go:122`
- Modify: `media-service/config.go:59-61`, `media-service/main.go:63`
- Modify: `bot-message-handler/main.go:24-26`, `bot-message-handler/main.go:80`
- Modify: `bot-room-service/main.go:30-32`, `bot-room-service/main.go:88`
- Modify: each service's `deploy/docker-compose.yml`
- Create/modify: `botplatform-service/config_test.go`, `upload-service/config_test.go`, `media-service/config_test.go`, `bot-message-handler/config_test.go`, `bot-room-service/config_test.go`

**Interfaces:**
- Consumes: `mongoutil.ParseReadPreference`, `mongoutil.WithReadPreference`.
- Produces: `ReadPreference string` on each config struct.

- [ ] **Step 0: Create the test files that do not exist yet**

`botplatform-service/config_test.go`, `upload-service/config_test.go`,
`bot-message-handler/config_test.go` and `bot-room-service/config_test.go` do not
exist. Use `config_test.go`, never `main_test.go` — several services reserve
`main_test.go` for an integration-tagged `TestMain`. Create each with this header (adjust the package line only if the service
uses a sub-package):

```go
package main

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/mongoutil"
)
```

`botplatform-service` uses `package main` with a `loadConfig()` in `config.go`, so
its test calls `loadConfig()` rather than `env.ParseAs[config]()` and does not need
the `env` import.

- [ ] **Step 1: Write the failing tests**

One per service, using Pattern B. Required env vars per service:

| Service | `t.Setenv` lines needed |
|---|---|
| botplatform-service | `SITE_ID`, `MONGO_URI`, `NATS_URL` |
| upload-service | `SITE_ID`, `MONGO_URI`, `BOTPLATFORM_URL`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` |
| media-service | `SITE_ID`, `CLUSTER_DOMAINS`, `EMPLOYEE_PHOTO_BASE_URL`, `MONGO_URI`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `NATS_URL`, `BOTPLATFORM_URL` |
| bot-message-handler | `NATS_URL`, `SITE_ID`, `MONGO_URI` |
| bot-room-service | `NATS_URL`, `SITE_ID`, `MONGO_URI` |

Any placeholder value parses (`"x"` for URLs is fine — nothing connects). Example, for bot-room-service:

```go
func TestConfig_ReadPreferenceDefault(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	require.NoError(t, os.Unsetenv("READ_PREFERENCE"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "primaryPreferred", cfg.ReadPreference)

	rp, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	require.NoError(t, err)
	require.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}
```

`botplatform-service`, `media-service` and `admin-service` parse via a `loadConfig()`/`Load()` function rather than `env.ParseAs[config]()` — call whichever their existing config test already calls.

- [ ] **Step 2: Run them and confirm they fail**

Run: `make test SERVICE=botplatform-service && make test SERVICE=upload-service && make test SERVICE=media-service && make test SERVICE=bot-message-handler && make test SERVICE=bot-room-service`
Expected: five failures, each `cfg.ReadPreference` undefined.

- [ ] **Step 3: Add the config field to each**

Pattern A's field, with a service-specific comment:

- `botplatform-service`: `// primaryPreferred, not secondaryPreferred: InsertSession then FindSessionByHash on the next request is a read-after-write.`
- `upload-service`: `// primaryPreferred: GetUpload reads a doc written outside this repo, so the write-to-read window cannot be bounded.`
- `media-service`: `// primaryPreferred: EmojiDoc/Avatar are read right after UpsertEmoji/SetBotAvatar.`
- `bot-message-handler`: `// primaryPreferred: same authz shape as message-gatekeeper, with no cache in front.`
- `bot-room-service`: `// primaryPreferred: creates rooms and subscriptions, then reads them back.`

- [ ] **Step 4: Wire each client**

Apply Pattern A's parse + `mongoutil.WithReadPreference(readPref)` + log at each connect site listed under **Files**.

- [ ] **Step 5: Run the tests and confirm they pass**

Run the same five `make test SERVICE=...` commands.
Expected: all PASS.

- [ ] **Step 6: Add the compose env vars**

`READ_PREFERENCE=${READ_PREFERENCE:-primaryPreferred}` in all five `deploy/docker-compose.yml` files.

- [ ] **Step 7: Verify and commit**

Run: `make fmt && make lint` then the five test commands.

```bash
git add botplatform-service upload-service media-service bot-message-handler bot-room-service
git commit -m "feat(mongo): primaryPreferred reads across the remaining request/reply readers

Each has a read-after-write that rules out secondaryPreferred but none that
rules out primaryPreferred, which is a no-op in steady state."
```

---

### Task 5: notification-worker — relax the users pin

`notification-worker/main.go:227` pins `users` to `readpref.Primary()` because settings gate push delivery. A stale read means notifying a just-muted user: self-correcting and not durable. Under the pin, push delivery fails entirely during an incident.

**Files:**
- Modify: `notification-worker/main.go:46-48` (config struct), `notification-worker/main.go:222-227`
- Modify: `notification-worker/deploy/user/docker-compose.yml`, `notification-worker/deploy/bot/docker-compose.yml`
- Test: `notification-worker/config_test.go`

**Interfaces:**
- Consumes: `mongoutil.CollectionWithReadPreference(*mongo.Collection, *readpref.ReadPref) *mongo.Collection`.
- Produces: `config.MongoUserReadPreference string`.

- [ ] **Step 1: Write the failing test**

```go
func TestConfig_UserReadPreferenceDefault(t *testing.T) {
	t.Setenv("MODE", "user")
	require.NoError(t, os.Unsetenv("MONGO_USER_READ_PREFERENCE"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "primaryPreferred", cfg.MongoUserReadPreference,
		"a stale mute read notifies a just-muted user; the pin took push delivery down with the primary")

	rp, err := mongoutil.ParseReadPreference(cfg.MongoUserReadPreference)
	require.NoError(t, err)
	require.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}
```

Add any further `t.Setenv` lines the parse error names.

- [ ] **Step 2: Run it and confirm it fails**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `cfg.MongoUserReadPreference` undefined.

- [ ] **Step 3: Add the field**

Next to `MongoReadPreference` in `notification-worker/main.go`:

```go
	// MongoUserReadPreference covers the settings read that gates push delivery.
	// Kept off the client-wide preference (the other collections tolerate more lag),
	// but primaryPreferred rather than primary: a stale mute is recoverable, a dead
	// push pipeline is not.
	MongoUserReadPreference string `env:"MONGO_USER_READ_PREFERENCE" envDefault:"primaryPreferred"`
```

- [ ] **Step 4: Replace the pin**

At `notification-worker/main.go:222-227`, replace the `readpref.Primary()` argument:

```go
	userReadPref, err := mongoutil.ParseReadPreference(cfg.MongoUserReadPreference)
	if err != nil {
		slog.Error("invalid mongo user read preference", "value", cfg.MongoUserReadPreference, "error", err)
		os.Exit(1)
	}
	// Settings gate push delivery, so this collection keeps its own preference
	// rather than the client-wide one; primaryPreferred still falls back rather
	// than failing when no primary exists.
	usersCol := mongoutil.CollectionWithReadPreference(db.Collection("users"), userReadPref)
	slog.Info("mongo user read preference configured", "readPreference", userReadPref.Mode().String())
```

Remove the now-unused `readpref` import if nothing else in the file uses it.

- [ ] **Step 5: Run the test and confirm it passes**

Run: `make test SERVICE=notification-worker`
Expected: PASS.

- [ ] **Step 6: Add the compose env var to both deploy variants**

```yaml
      - MONGO_USER_READ_PREFERENCE=${MONGO_USER_READ_PREFERENCE:-primaryPreferred}
```

- [ ] **Step 7: Verify and commit**

Run: `make fmt && make lint && make test SERVICE=notification-worker`

```bash
git add notification-worker
git commit -m "feat(notification-worker): primaryPreferred for the mute-settings read

The primary pin took push delivery down with the primary. A stale mute read
notifies a just-muted user — recoverable; a dead push pipeline is not."
```

---

### Task 6: tcard-service + search-sync-worker — secondaryPreferred

The only two services where permanent replica lag is provably harmless, so they take the extra primary-offload win.

**Files:**
- Modify: `tcard-service/main.go:34-36`, `tcard-service/main.go:74`
- Modify: `search-sync-worker/main.go:43-45`, `search-sync-worker/main.go:172`
- Modify: both `deploy/docker-compose.yml`
- Create/modify: `tcard-service/config_test.go`, `search-sync-worker/main_test.go` (the latter exists and is untagged)

**Interfaces:**
- Consumes: `mongoutil.ParseReadPreference`, `mongoutil.WithReadPreference`.
- Produces: `config.ReadPreference string` defaulting to `secondaryPreferred` on both.

- [ ] **Step 0: Create `tcard-service/config_test.go`**

It does not exist. Use the same header shown in Task 4 Step 0.

- [ ] **Step 1: Write the failing tests**

tcard-service (`MONGO_URI` is its only required var):

```go
func TestConfig_ReadPreferenceDefault(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	require.NoError(t, os.Unsetenv("READ_PREFERENCE"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "secondaryPreferred", cfg.ReadPreference,
		"a full-collection scan behind a once-daily cache is the strongest offload candidate in the repo")

	rp, err := mongoutil.ParseReadPreference(cfg.ReadPreference)
	require.NoError(t, err)
	require.Equal(t, readpref.SecondaryPreferredMode, rp.Mode())
}
```

search-sync-worker — same body, required env `NATS_URL`, `SITE_ID`, `MONGO_URI`, `SEARCH_URL`, `MSG_INDEX_PREFIX`, `SPOTLIGHT_INDEX`, `SPOTLIGHT_ORG_INDEX`, `HR_CENTRAL_SITE_ID`, `USER_ROOM_INDEX`, and this message:

```go
		"unmatched ids are already omitted by design (teams_user_store.go:31); lag widens an accepted race")
```

- [ ] **Step 2: Run them and confirm they fail**

Run: `make test SERVICE=tcard-service && make test SERVICE=search-sync-worker`
Expected: both FAIL — `cfg.ReadPreference` undefined.

- [ ] **Step 3: Add the fields**

tcard-service:

```go
	// ReadPreference: cards are read by a full scan behind a once-daily cache and
	// written by nothing in this repo, so replica lag is noise against a 24h TTL.
	ReadPreference string `env:"READ_PREFERENCE" envDefault:"secondaryPreferred"`
```

search-sync-worker:

```go
	// ReadPreference: the resolver already treats a missing user as a normal
	// outcome, so secondary lag widens an accepted race rather than opening one.
	ReadPreference      string `env:"READ_PREFERENCE" envDefault:"secondaryPreferred"`
```

- [ ] **Step 4: Wire both clients**

Apply Pattern A's parse + `mongoutil.WithReadPreference(readPref)` + log at `tcard-service/main.go:74` and `search-sync-worker/main.go:172`.

- [ ] **Step 5: Run the tests and confirm they pass**

Run: `make test SERVICE=tcard-service && make test SERVICE=search-sync-worker`
Expected: PASS.

- [ ] **Step 6: Add the compose env vars**

```yaml
      - READ_PREFERENCE=${READ_PREFERENCE:-secondaryPreferred}
```

- [ ] **Step 7: Verify and commit**

Run: `make fmt && make lint` then both test commands.

```bash
git add tcard-service search-sync-worker
git commit -m "feat(mongo): secondaryPreferred for the two provably lag-tolerant readers

tcard-service scans cards behind a once-daily cache; search-sync-worker's
resolver already treats a missing user as a normal outcome. Both take the
primary-offload win the other services cannot."
```

---

### Task 7: the DEK and room-key pins

Four sites pinned to `readpref.Primary()` for key material. Relaxing them is what restores **encrypted-room send and encrypted-message history reads** during an incident — both fail outright today. Safe because each path already guards a stale read: `$setOnInsert` plus a re-read comparison for DEKs (`pkg/atrest/dek_store.go:53`, `pkg/atrest/cipher.go:206-217`), and error-on-missing plus the retired-key archive for room keys (`broadcast-worker/handler.go:996`).

**Before starting:** re-check the CLAUDE.md retention rule — `ROOM_KEY_RETIRED_TTL` must outlast `ROOM_KEY_CACHE_TTL` plus a client `key.get` and retry. A secondary read widens the staleness window that budget absorbs. If the current margin is thin, raise it in this task.

**Files:**
- Modify: `broadcast-worker/main.go:54-56` (config), `:194` (preview DEK), `:233` (`roomsPrimary`)
- Modify: `history-service/internal/config/config.go:32-34`, `history-service/cmd/main.go:149`, `:155`
- Modify: `bot-message-worker/main.go:116`
- Modify: the three services' `deploy/docker-compose.yml` files
- Create/modify: `broadcast-worker/config_test.go`, `history-service/internal/config/config_test.go`, `bot-message-worker/config_test.go`

**Interfaces:**
- Consumes: `mongoutil.ParseReadPreference`, `options.Collection().SetReadPreference`.
- Produces: `MongoKeyReadPreference` / `Mongo.KeyReadPreference` / `ReadPreference` config fields.

- [ ] **Step 1: Write the failing test for broadcast-worker**

```go
func TestConfig_KeyReadPreferenceDefault(t *testing.T) {
	t.Setenv("MODE", "user")
	require.NoError(t, os.Unsetenv("MONGO_KEY_READ_PREFERENCE"))

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	require.Equal(t, "primaryPreferred", cfg.MongoKeyReadPreference,
		"the primary pin makes encrypted-room delivery fail outright when the primary is gone")

	rp, err := mongoutil.ParseReadPreference(cfg.MongoKeyReadPreference)
	require.NoError(t, err)
	require.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `cfg.MongoKeyReadPreference` undefined.

- [ ] **Step 3: Add the field and replace both broadcast-worker pins**

Config field:

```go
	// MongoKeyReadPreference covers room keys and preview DEKs. Kept separate from
	// the client-wide preference because key freshness matters more than room meta,
	// but primaryPreferred rather than primary: a stale DEK read cannot diverge
	// ($setOnInsert plus a re-read comparison) and a missing room key is a retryable
	// error, so failing outright buys nothing an incident does not already cost.
	MongoKeyReadPreference string `env:"MONGO_KEY_READ_PREFERENCE" envDefault:"primaryPreferred"`
```

Parse once, before the two call sites:

```go
	keyReadPref, err := mongoutil.ParseReadPreference(cfg.MongoKeyReadPreference)
	if err != nil {
		slog.Error("invalid mongo key read preference", "value", cfg.MongoKeyReadPreference, "error", err)
		os.Exit(1)
	}
	slog.Info("mongo key read preference configured", "readPreference", keyReadPref.Mode().String())
```

Replace `main.go:194`:

```go
			dekColl := db.Collection(preview.DEKCollection, options.Collection().SetReadPreference(keyReadPref))
```

Replace `main.go:233`:

```go
		roomsForKeys := db.Collection("rooms", options.Collection().SetReadPreference(keyReadPref))
		keyStore = roomkeystore.NewMongoStore(roomsForKeys, cfg.RoomKeyGracePeriod)
```

Rename the variable from `roomsPrimary` to `roomsForKeys` — the old name now asserts something untrue.

- [ ] **Step 4: Run the test and confirm it passes**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS.

- [ ] **Step 5: Repeat for history-service**

Add to `history-service/internal/config/config.go` next to `ReadPreference`:

```go
	// KeyReadPreference covers the at-rest and preview DEK collections.
	KeyReadPreference string `env:"MONGO_KEY_READ_PREFERENCE" envDefault:"primaryPreferred"`
```

Add to `validate` beside the existing `ReadPreference` check at `config.go:146`:

```go
	if _, err := mongoutil.ParseReadPreference(cfg.Mongo.KeyReadPreference); err != nil {
		return fmt.Errorf("MONGO_KEY_READ_PREFERENCE: %w", err)
	}
```

Test, following the existing file's style:

```go
func TestLoad_DefaultsKeyReadPreferenceToPrimaryPreferred(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("CASSANDRA_HOSTS", "localhost")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	unsetEnv(t, "MONGO_KEY_READ_PREFERENCE")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "primaryPreferred", cfg.Mongo.KeyReadPreference)
}
```

Then replace `readpref.Primary()` at `history-service/cmd/main.go:149` and `:155` with a `keyReadPref` parsed the same way.

- [ ] **Step 6: Repeat for bot-message-worker**

Its only Mongo collection is the at-rest DEK collection at `main.go:116`, currently on the client default. `bot-message-worker/config_test.go` does not exist — create it with the header shown in Task 4 Step 0. Add Pattern A's `ReadPreference` field with `envDefault:"primaryPreferred"` and this comment:

```go
	// ReadPreference: the DEK collection is this service's only Mongo read.
	ReadPreference string `env:"READ_PREFERENCE" envDefault:"primaryPreferred"`
```

Wire it at the connect call, and confirm with a Pattern B test.

- [ ] **Step 7: Add the compose env vars**

`MONGO_KEY_READ_PREFERENCE` for broadcast-worker (both deploy variants) and history-service; `READ_PREFERENCE` for bot-message-worker.

- [ ] **Step 8: Verify and commit**

Run: `make fmt && make lint && make test SERVICE=broadcast-worker && make test SERVICE=history-service && make test SERVICE=bot-message-worker`

```bash
git add broadcast-worker history-service bot-message-worker
git commit -m "feat(mongo): primaryPreferred for room-key and DEK reads

Encrypted rooms can neither send nor read history during a primary-down
incident today, because both key paths are pinned to the primary. Relaxing to
primaryPreferred is safe: a stale DEK read cannot diverge (\$setOnInsert plus a
re-read comparison) and a missing room key is already a retryable error."
```

---

### Task 8: end-to-end outage assertion

Proves the whole change does what it claims: reads survive when the primary is gone.

**Files:**
- Modify: `tools/loadgen/mongo_outage_recovery_integration_test.go`
- Test: same file

**Interfaces:**
- Consumes: `startMongoOutageContainer(t, ctx) (*mongocontainer.MongoDBContainer, string)` — already exists in that file.
- Produces: nothing.

- [ ] **Step 1: Write the failing test**

```go
// TestReadPreference_ReadsSurvivePrimaryLoss is the end-to-end claim of the
// read-preference work: a primaryPreferred client keeps reading when no primary
// is selectable, where a primary client fails.
func TestReadPreference_ReadsSurvivePrimaryLoss(t *testing.T) {
	ctx := context.Background()
	_, uri := startMongoOutageContainer(t, ctx)

	seed, err := mongo.Connect(options.Client().ApplyURI(uri))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, seed.Disconnect(context.Background())) })
	_, err = seed.Database("chat").Collection("probe").
		InsertOne(ctx, bson.M{"_id": "p1", "v": 1})
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		rp         *readpref.ReadPref
		wantErrors bool
	}{
		{"primary fails without a primary", readpref.Primary(), true},
		{"primaryPreferred falls back", readpref.PrimaryPreferred(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := mongo.Connect(options.Client().ApplyURI(uri).
				SetReadPreference(tc.rp).
				SetServerSelectionTimeout(3 * time.Second))
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, c.Disconnect(context.Background())) })

			readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			err = c.Database("chat").Collection("probe").
				FindOne(readCtx, bson.M{"_id": "p1"}).Err()
			if tc.wantErrors {
				require.Error(t, err, "a primary-only read must fail with no primary")
				return
			}
			require.NoError(t, err, "primaryPreferred must fall back to a secondary")
		})
	}
}
```

The test needs the container stepped down to a secondary-only state between seeding and reading. `startMongoOutageContainer` gives a container whose port binding can be manipulated; if the existing file already has a step-down or partition helper, call it here. If it does not, add one that issues `rs.stepDown()` via `runCommand` on the `admin` database and waits for `hello.isWritablePrimary` to report false.

- [ ] **Step 2: Run it and confirm it fails**

Run: `make test-integration SERVICE=tools/loadgen`
Expected: FAIL — the `primaryPreferred` subtest errors, because without a step-down there is still a primary and the `primary` subtest's expectation of failure is not met.

- [ ] **Step 3: Add the step-down helper and make it pass**

Implement the helper described in Step 1, then re-run.

- [ ] **Step 4: Run and confirm it passes**

Run: `make test-integration SERVICE=tools/loadgen`
Expected: PASS, both subtests.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen
git commit -m "test(loadgen): assert reads survive MongoDB primary loss

Pins the end-to-end claim of the read-preference work: primary fails with no
primary selectable, primaryPreferred falls back to a secondary."
```

---

## Final verification

- [ ] `make fmt && make lint` — clean
- [ ] `make test` — all unit tests pass with `-race`
- [ ] `make sast` — no medium+ findings
- [ ] `make test-integration SERVICE=admin-service` — the transaction guard holds
- [ ] `make test-integration SERVICE=tools/loadgen` — reads survive primary loss
- [ ] Confirm no file under `docs/reviews/` exists before opening a PR (CLAUDE.md §5)
- [ ] Confirm `docs/client-api.md` is untouched — this plan changes no client-facing schema
