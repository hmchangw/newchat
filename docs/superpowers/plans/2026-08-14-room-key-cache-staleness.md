# Room Key Cache Staleness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every room-key version that `broadcast-worker` can stamp into a message resolvable by a client, and make the version fanned out to survivors match the version the store assigned.

**Architecture:** Retired key versions are copied into a new `retired_room_keys` MongoDB collection, one immutable document per `(roomID, version)`, expired by a TTL index. `GetByVersion` falls back to it when neither the current nor the previous slot matches. Separately, `room-worker` and `bot-room-service` stop predicting `current + 1` and fan out the version `Rotate` actually returned. The room document's `encKey`, the `Rotate` pipeline semantics, the 24h grace period, and `broadcast-worker`'s L1 cache are unchanged.

**Tech Stack:** Go 1.25, MongoDB (`go.mongodb.org/mongo-driver/v2`), OpenTelemetry metrics, `go.uber.org/mock`, `testify`, `testcontainers-go` via `pkg/testutil`.

**Design spec:** `docs/superpowers/specs/2026-08-14-room-key-cache-staleness-design.md`

## Global Constraints

- Never run raw `go` commands — always `make` targets. `SERVICE=` accepts a path prefix, so `make test SERVICE=pkg/roomkeystore` works.
- TDD is mandatory: write the failing test, run it, confirm it fails, then implement.
- Every error is wrapped with context describing what the current function was doing: `fmt.Errorf("short description: %w", err)`. Never a bare `err`.
- Logging is `log/slog` with structured key-value fields. Never log key bytes.
- Integration tests are tagged `//go:build integration` and live in the same package. `pkg/roomkeystore` already has `main_test.go` with `TestMain` → `testutil.RunTests(m)`. Do not add another.
- Containers come from `pkg/testutil` — never `testcontainers.GenericContainer` directly.
- Run `make generate SERVICE=<name>` after changing any store interface, before testing. Exception: `bot-room-service` has **no** mockgen infrastructure — it uses hand-written fakes in `roomkey_test.go`, which are edited by hand.
- Lint and tests are enforced by a pre-commit hook. Fix failures before retrying the commit.
- New collection name: `retired_room_keys`. New env var: `ROOM_KEY_RETIRED_TTL`, default `20m`.
- `docs/client-api.md` needs **no** change in this plan — no client-facing request/response struct changes.

---

### Task 1: Retired-key collection scaffolding

Adds the document shape, the deterministic `_id`, the opt-in store option, and the TTL index. Nothing writes or reads the collection yet.

**Files:**
- Modify: `pkg/roomkeystore/roomkeystore_mongo.go`
- Modify: `pkg/roomkeystore/roomkeystore.go` (interface)
- Test: `pkg/roomkeystore/roomkeystore_test.go`
- Test: `pkg/roomkeystore/integration_test.go`

**Interfaces:**
- Produces: `retiredKeyID(roomID string, version int) string`; `retiredKeyDoc` struct; `WithRetiredKeys(col *mongo.Collection, ttl time.Duration) Option`; `NewMongoStore(col *mongo.Collection, gracePeriod time.Duration, opts ...Option) RoomKeyStore`; `(*mongoStore).EnsureIndexes(ctx context.Context) error`; `mongoStore.retiredCol` and `mongoStore.retiredTTL` fields.

- [ ] **Step 1: Write the failing unit tests**

Append to `pkg/roomkeystore/roomkeystore_test.go`:

```go
func TestRetiredKeyID(t *testing.T) {
	assert.Equal(t, "room123:7", retiredKeyID("room123", 7))
	assert.Equal(t, "room123:0", retiredKeyID("room123", 0))
}

func TestNewMongoStore_RetiredKeysOptional(t *testing.T) {
	t.Run("omitted leaves the archive disabled", func(t *testing.T) {
		s := newMongoStore(nil, time.Hour)
		assert.Nil(t, s.retiredCol)
		assert.Zero(t, s.retiredTTL)
	})

	t.Run("option records the retention", func(t *testing.T) {
		s := newMongoStore(nil, time.Hour, WithRetiredKeys(nil, 20*time.Minute))
		assert.Equal(t, 20*time.Minute, s.retiredTTL)
	})
}

func TestMongoStore_EnsureIndexes_NoArchiveConfigured(t *testing.T) {
	s := newMongoStore(nil, time.Hour)
	require.NoError(t, s.EnsureIndexes(context.Background()),
		"EnsureIndexes must no-op when the archive is not configured")
}
```

Add `"context"` to that file's imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=pkg/roomkeystore`
Expected: FAIL — `undefined: retiredKeyID`, `undefined: WithRetiredKeys`, `s.EnsureIndexes undefined`.

- [ ] **Step 3: Implement the scaffolding**

In `pkg/roomkeystore/roomkeystore_mongo.go`, extend the struct and constructor:

```go
type mongoStore struct {
	col         *mongo.Collection
	gracePeriod time.Duration
	now         func() time.Time

	// retiredCol holds one immutable document per retired key version, expired
	// by a TTL index. nil disables the archive: consumers that only read the
	// current key (broadcast-worker, tools) omit the option.
	retiredCol *mongo.Collection
	retiredTTL time.Duration
}

// Option configures a mongoStore.
type Option func(*mongoStore)

// WithRetiredKeys enables the retired-key archive. Rotate copies each demoted
// version into col, and GetByVersion falls back to it when neither the current
// nor the previous slot matches. ttl must exceed broadcast-worker's room-key
// cache TTL — see ROOM_KEY_RETIRED_TTL.
func WithRetiredKeys(col *mongo.Collection, ttl time.Duration) Option {
	return func(s *mongoStore) {
		s.retiredCol = col
		s.retiredTTL = ttl
	}
}

func NewMongoStore(col *mongo.Collection, gracePeriod time.Duration, opts ...Option) RoomKeyStore {
	return newMongoStore(col, gracePeriod, opts...)
}

func newMongoStore(col *mongo.Collection, gracePeriod time.Duration, opts ...Option) *mongoStore {
	s := &mongoStore{col: col, gracePeriod: gracePeriod, now: time.Now}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
```

Add the document type and `_id` helper near `keyDoc`:

```go
// retiredKeyDoc is one retired key version in the retired_room_keys collection.
// Documents are immutable: a given (roomID, version) maps to fixed bytes.
type retiredKeyDoc struct {
	ID        string    `bson:"_id"`
	Priv      []byte    `bson:"priv"`
	ExpiresAt time.Time `bson:"expiresAt"`
}

// retiredKeyID is the deterministic _id for a retired version. The natural key
// is what callers look up by, so no secondary index is needed and the archive
// write is idempotent by construction.
func retiredKeyID(roomID string, version int) string {
	return fmt.Sprintf("%s:%d", roomID, version)
}
```

Add `EnsureIndexes`:

```go
// EnsureIndexes creates the indexes this store owns. Idempotent; safe to call
// from every service at startup. No-op when the archive is not configured.
func (s *mongoStore) EnsureIndexes(ctx context.Context) error {
	if s.retiredCol == nil {
		return nil
	}
	// expireAfterSeconds 0 with a per-document expiresAt keeps retention at
	// write time and tunable by config without rebuilding the index.
	if _, err := s.retiredCol.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expiresAt", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(0),
	}); err != nil {
		return fmt.Errorf("ensure retired_room_keys expiresAt TTL index: %w", err)
	}
	return nil
}
```

In `pkg/roomkeystore/roomkeystore.go`, add to the `RoomKeyStore` interface (above `Close`):

```go
	// EnsureIndexes creates the indexes this store owns. Idempotent.
	EnsureIndexes(ctx context.Context) error
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `make test SERVICE=pkg/roomkeystore`
Expected: PASS.

- [ ] **Step 5: Write the failing integration test for the TTL index**

Append to `pkg/roomkeystore/integration_test.go`:

```go
func TestMongoStore_EnsureIndexes_CreatesTTLIndex(t *testing.T) {
	db := testutil.MongoDB(t, "roomkey_ttl")
	store := newMongoStore(db.Collection("rooms"), time.Hour,
		WithRetiredKeys(db.Collection("retired_room_keys"), 20*time.Minute))

	require.NoError(t, store.EnsureIndexes(context.Background()))

	cur, err := db.Collection("retired_room_keys").Indexes().List(context.Background())
	require.NoError(t, err)
	var specs []bson.M
	require.NoError(t, cur.All(context.Background(), &specs))

	var found bool
	for _, spec := range specs {
		if spec["name"] == "expiresAt_1" {
			found = true
			require.Contains(t, spec, "expireAfterSeconds",
				"the expiresAt index must be a TTL index")
			assert.EqualValues(t, 0, spec["expireAfterSeconds"],
				"retention is per-document via expiresAt, so expireAfterSeconds is 0")
		}
	}
	assert.True(t, found, "expiresAt_1 index must exist")
}
```

- [ ] **Step 6: Run the integration test to verify it passes**

Run: `make test-integration SERVICE=pkg/roomkeystore`
Expected: PASS (requires Docker).

- [ ] **Step 7: Verify the whole repo still compiles**

Run: `make lint`
Expected: clean. The new interface method is satisfied only by `mongoStore`, which now implements it.

- [ ] **Step 8: Commit**

```bash
git add pkg/roomkeystore/
git commit -m "feat(roomkeystore): add retired-key collection scaffolding and TTL index"
```

---

### Task 2: `Rotate` archives the demoted version

**Files:**
- Modify: `pkg/roomkeystore/roomkeystore_mongo.go`
- Test: `pkg/roomkeystore/roomkeystore_test.go`
- Test: `pkg/roomkeystore/integration_test.go`

**Interfaces:**
- Consumes: `retiredKeyID`, `retiredKeyDoc`, `mongoStore.retiredCol`, `mongoStore.retiredTTL` (Task 1).
- Produces: `(*mongoStore).retiredDoc(priv []byte) bson.M`; `(*mongoStore).archiveRetired(ctx context.Context, roomID string, version int, priv []byte)`. `Rotate`'s signature is unchanged: `Rotate(ctx, roomID, newPair) (int, error)`.

- [ ] **Step 1: Write the failing unit tests**

Append to `pkg/roomkeystore/roomkeystore_test.go`:

```go
func TestMongoStore_retiredDoc(t *testing.T) {
	priv := bytes.Repeat([]byte{0x33}, 32)
	now := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)

	s := newMongoStore(nil, time.Hour, WithRetiredKeys(nil, 20*time.Minute))
	s.now = func() time.Time { return now }

	doc := s.retiredDoc(priv)
	assert.Equal(t, priv, doc["priv"])
	assert.Equal(t, now.Add(20*time.Minute).UTC(), doc["expiresAt"],
		"expiresAt is stamped from the injected clock plus the retention")
}

func TestMongoStore_archiveRetired_NoArchiveConfigured(t *testing.T) {
	s := newMongoStore(nil, time.Hour)
	// retiredCol is nil — must return without touching Mongo rather than panic.
	s.archiveRetired(context.Background(), "room1", 4, bytes.Repeat([]byte{0x01}, 32))
}

func TestMongoStore_archiveRetired_EmptySecret(t *testing.T) {
	s := newMongoStore(nil, time.Hour, WithRetiredKeys(nil, time.Minute))
	// A malformed document with no demoted secret must not attempt a write.
	s.archiveRetired(context.Background(), "room1", 4, nil)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=pkg/roomkeystore`
Expected: FAIL — `s.retiredDoc undefined`, `s.archiveRetired undefined`.

- [ ] **Step 3: Implement the archive write**

Add to `pkg/roomkeystore/roomkeystore_mongo.go`:

```go
// retiredDoc builds the immutable body of a retired-key document.
func (s *mongoStore) retiredDoc(priv []byte) bson.M {
	return bson.M{"priv": priv, "expiresAt": s.now().UTC().Add(s.retiredTTL)}
}

// archiveRetired copies a just-demoted key into the retired-key collection.
//
// Best-effort by design: the rotation is already committed and must not be
// re-run, so a failure here is logged and counted rather than returned. The
// demoted key also stays readable from the previous slot for the grace period,
// so the archive is a copy, never the only one — it only becomes load-bearing
// once a further rotation evicts that slot.
//
// $setOnInsert makes the write idempotent: a redelivered rotation re-archiving
// the same version is a no-op, and the bytes for a version never change.
func (s *mongoStore) archiveRetired(ctx context.Context, roomID string, version int, priv []byte) {
	if s.retiredCol == nil || len(priv) == 0 {
		return
	}
	_, err := s.retiredCol.UpdateByID(ctx, retiredKeyID(roomID, version),
		bson.M{"$setOnInsert": s.retiredDoc(priv)},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		roomkeymetrics.StoreErrors.Add(ctx, 1,
			metric.WithAttributes(attribute.String("op", "ArchiveRetired")))
		slog.ErrorContext(ctx, "archive retired room key failed",
			"room_id", roomID, "version", version, "error", err)
	}
}
```

Add these imports to the file:

```go
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hmchangw/chat/pkg/roomkeymetrics"
```

In `Rotate`, widen the projection and archive after the atomic update. Replace:

```go
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetProjection(bson.M{"encKey.ver": 1})
```

with:

```go
	// The After projection also returns the previous slot, which now holds
	// exactly what THIS call demoted — precise even when rotations race, which
	// a separate pre-read would not be.
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetProjection(bson.M{"encKey.ver": 1, "encKey.prevPriv": 1, "encKey.prevVer": 1})
```

and replace the final `return updated.EncKey.Ver, nil` with:

```go
	s.archiveRetired(ctx, roomID, updated.EncKey.PrevVer, updated.EncKey.PrevPriv)
	return updated.EncKey.Ver, nil
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `make test SERVICE=pkg/roomkeystore`
Expected: PASS.

- [ ] **Step 5: Write the failing integration test**

Append to `pkg/roomkeystore/integration_test.go`:

```go
func TestMongoStore_Rotate_ArchivesDemotedVersion(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "roomkey_archive")
	rooms := db.Collection("rooms")
	retired := db.Collection("retired_room_keys")
	store := newMongoStore(rooms, time.Hour, WithRetiredKeys(retired, 20*time.Minute))

	v0 := bytes.Repeat([]byte{0xA0}, 32)
	_, err := rooms.InsertOne(ctx, bson.M{"_id": "room1", "encKey": InitialKeyDoc(RoomKeyPair{PrivateKey: v0})})
	require.NoError(t, err)

	v1 := bytes.Repeat([]byte{0xA1}, 32)
	newVer, err := store.Rotate(ctx, "room1", RoomKeyPair{PrivateKey: v1})
	require.NoError(t, err)
	require.Equal(t, 1, newVer)

	var doc retiredKeyDoc
	require.NoError(t, retired.FindOne(ctx, bson.M{"_id": "room1:0"}).Decode(&doc),
		"the demoted version 0 must be archived")
	assert.Equal(t, v0, doc.Priv)
	assert.False(t, doc.ExpiresAt.IsZero(), "expiresAt drives the TTL reaper")
}

func TestMongoStore_Rotate_ConcurrentRotationsArchiveEveryVersion(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "roomkey_archive_concurrent")
	rooms := db.Collection("rooms")
	retired := db.Collection("retired_room_keys")
	store := newMongoStore(rooms, time.Hour, WithRetiredKeys(retired, 20*time.Minute))

	_, err := rooms.InsertOne(ctx, bson.M{
		"_id":    "room2",
		"encKey": InitialKeyDoc(RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0xB0}, 32)}),
	})
	require.NoError(t, err)

	const rotations = 5
	var wg sync.WaitGroup
	for i := range rotations {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, rotErr := store.Rotate(ctx, "room2", RoomKeyPair{PrivateKey: bytes.Repeat([]byte{byte(0xC0 + i)}, 32)})
			assert.NoError(t, rotErr)
		}()
	}
	wg.Wait()

	// Versions 0..rotations-1 were each demoted exactly once, so each must have
	// an archive document regardless of the order the rotations interleaved.
	for v := range rotations {
		id := retiredKeyID("room2", v)
		err := retired.FindOne(ctx, bson.M{"_id": id}).Err()
		assert.NoError(t, err, "version %d must be archived", v)
	}
}
```

Add `"sync"` to the integration file's imports if not already present.

- [ ] **Step 6: Run the integration tests to verify they pass**

Run: `make test-integration SERVICE=pkg/roomkeystore`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/roomkeystore/
git commit -m "feat(roomkeystore): archive the demoted key version on Rotate"
```

---

### Task 3: `GetByVersion` falls back to the archive

**Files:**
- Modify: `pkg/roomkeystore/roomkeystore_mongo.go`
- Test: `pkg/roomkeystore/roomkeystore_test.go`
- Test: `pkg/roomkeystore/integration_test.go`

**Interfaces:**
- Consumes: `retiredKeyID`, `retiredKeyDoc`, `validateSecret`, `mongoStore.retiredCol` (Tasks 1–2).
- Produces: `(*mongoStore).retiredByVersion(ctx context.Context, roomID string, version int) (*RoomKeyPair, error)`. `GetByVersion`'s signature is unchanged.

- [ ] **Step 1: Write the failing unit test**

Append to `pkg/roomkeystore/roomkeystore_test.go`:

```go
func TestMongoStore_retiredByVersion_NoArchiveConfigured(t *testing.T) {
	s := newMongoStore(nil, time.Hour)
	pair, err := s.retiredByVersion(context.Background(), "room1", 3)
	require.NoError(t, err, "an unconfigured archive is a clean miss, not an error")
	assert.Nil(t, pair)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=pkg/roomkeystore`
Expected: FAIL — `s.retiredByVersion undefined`.

- [ ] **Step 3: Implement the fallback**

Add to `pkg/roomkeystore/roomkeystore_mongo.go`:

```go
// retiredByVersion resolves a version from the retired-key archive. Returns
// (nil, nil) when the archive is not configured or holds no such version.
//
// expiresAt is deliberately not re-checked. Unlike the previous slot's
// prevExpiresAt — a read gate — it exists only to drive MongoDB's TTL reaper,
// which deletes lazily. Serving a document the reaper has not yet collected is
// harmless: a client asking for that version legitimately needs it.
func (s *mongoStore) retiredByVersion(ctx context.Context, roomID string, version int) (*RoomKeyPair, error) {
	if s.retiredCol == nil {
		return nil, nil
	}
	var doc retiredKeyDoc
	err := s.retiredCol.FindOne(ctx,
		bson.M{"_id": retiredKeyID(roomID, version)},
		options.FindOne().SetProjection(bson.M{"priv": 1}),
	).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get retired room key: %w", err)
	}
	if err := validateSecret(doc.Priv); err != nil {
		return nil, fmt.Errorf("decode retired key: %w", err)
	}
	return &RoomKeyPair{PrivateKey: doc.Priv}, nil
}
```

Rewrite `GetByVersion` so every miss falls through to the archive:

```go
func (s *mongoStore) GetByVersion(ctx context.Context, roomID string, version int) (*RoomKeyPair, error) {
	var doc roomKeyDoc
	err := s.col.FindOne(ctx, bson.M{"_id": roomID}, options.FindOne().SetProjection(encKeyProjection)).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return s.retiredByVersion(ctx, roomID, version)
	}
	if err != nil {
		return nil, fmt.Errorf("get room key by version: %w", err)
	}
	if doc.EncKey == nil {
		return s.retiredByVersion(ctx, roomID, version)
	}
	pair, err := doc.EncKey.pairForVersion(version, s.now().UTC())
	if err != nil {
		return nil, fmt.Errorf("get room key by version: %w", err)
	}
	if pair == nil {
		// Neither slot matched — the version may have been evicted from the
		// previous slot by a later rotation while a cached copy was still
		// stamping messages with it.
		return s.retiredByVersion(ctx, roomID, version)
	}
	return pair, nil
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `make test SERVICE=pkg/roomkeystore`
Expected: PASS.

- [ ] **Step 5: Write the failing integration test**

Append to `pkg/roomkeystore/integration_test.go`:

```go
func TestMongoStore_GetByVersion_FallsBackToArchive(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "roomkey_fallback")
	rooms := db.Collection("rooms")
	retired := db.Collection("retired_room_keys")
	store := newMongoStore(rooms, time.Hour, WithRetiredKeys(retired, 20*time.Minute))

	v0 := bytes.Repeat([]byte{0xD0}, 32)
	_, err := rooms.InsertOne(ctx, bson.M{"_id": "room3", "encKey": InitialKeyDoc(RoomKeyPair{PrivateKey: v0})})
	require.NoError(t, err)

	// Three rotations: v0 and v1 are pushed out of the single previous slot.
	for i, priv := range [][]byte{
		bytes.Repeat([]byte{0xD1}, 32),
		bytes.Repeat([]byte{0xD2}, 32),
		bytes.Repeat([]byte{0xD3}, 32),
	} {
		ver, rotErr := store.Rotate(ctx, "room3", RoomKeyPair{PrivateKey: priv})
		require.NoError(t, rotErr)
		require.Equal(t, i+1, ver)
	}

	// v0 is two rotations behind the previous slot — resolvable only via the archive.
	got, err := store.GetByVersion(ctx, "room3", 0)
	require.NoError(t, err)
	require.NotNil(t, got, "a version evicted from the previous slot must resolve from the archive")
	assert.Equal(t, v0, got.PrivateKey)

	// The current version still resolves from the room document.
	cur, err := store.GetByVersion(ctx, "room3", 3)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.Equal(t, bytes.Repeat([]byte{0xD3}, 32), cur.PrivateKey)

	// A version that never existed is still a clean miss.
	missing, err := store.GetByVersion(ctx, "room3", 99)
	require.NoError(t, err)
	assert.Nil(t, missing)
}

func TestMongoStore_GetByVersion_NoArchiveBehavesAsBefore(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "roomkey_no_archive")
	rooms := db.Collection("rooms")
	store := newMongoStore(rooms, time.Hour) // archive not configured

	v0 := bytes.Repeat([]byte{0xE0}, 32)
	_, err := rooms.InsertOne(ctx, bson.M{"_id": "room4", "encKey": InitialKeyDoc(RoomKeyPair{PrivateKey: v0})})
	require.NoError(t, err)

	for _, priv := range [][]byte{bytes.Repeat([]byte{0xE1}, 32), bytes.Repeat([]byte{0xE2}, 32)} {
		_, rotErr := store.Rotate(ctx, "room4", RoomKeyPair{PrivateKey: priv})
		require.NoError(t, rotErr)
	}

	got, err := store.GetByVersion(ctx, "room4", 0)
	require.NoError(t, err)
	assert.Nil(t, got, "without the archive, an evicted version stays unresolvable")
}
```

- [ ] **Step 6: Run the integration tests to verify they pass**

Run: `make test-integration SERVICE=pkg/roomkeystore`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/roomkeystore/
git commit -m "feat(roomkeystore): resolve evicted versions from the retired-key archive"
```

---

### Task 4: Wire the archive into room-worker, bot-room-service, and room-service

**Files:**
- Modify: `room-worker/main.go`
- Modify: `bot-room-service/main.go`
- Modify: `room-service/main.go`

**Interfaces:**
- Consumes: `roomkeystore.WithRetiredKeys`, `RoomKeyStore.EnsureIndexes` (Task 1).
- Produces: `Config.RoomKeyRetiredTTL time.Duration` in all three services, env `ROOM_KEY_RETIRED_TTL` default `20m`.

- [ ] **Step 1: Add the config field to all three services**

In each service's config struct, directly below the existing `RoomKeyGracePeriod` field, add:

```go
	// RoomKeyRetiredTTL is how long a rotated-out key stays in retired_room_keys.
	// Must be at least twice broadcast-worker's ROOM_KEY_CACHE_TTL — a version can
	// be stamped at the very end of a cache entry's life, so retention has to
	// outlast that entry plus the client's fetch and retry.
	RoomKeyRetiredTTL time.Duration `env:"ROOM_KEY_RETIRED_TTL" envDefault:"20m"`
```

Files and anchors:
- `room-worker/main.go` — after `RoomKeyGracePeriod` (line ~58)
- `bot-room-service/main.go` — after `RoomKeyGracePeriod` (line ~36)
- `room-service/main.go` — after `RoomKeyGracePeriod` (line ~46)

- [ ] **Step 2: Validate the new config and wire the collection**

In `room-worker/main.go`, extend the existing grace-period guard block:

```go
	if cfg.RoomKeyGracePeriod <= 0 {
		slog.Error("ROOM_KEY_GRACE_PERIOD must be a positive duration",
			"room_key_grace_period", cfg.RoomKeyGracePeriod)
		os.Exit(1)
	}
	if cfg.RoomKeyRetiredTTL <= 0 {
		slog.Error("ROOM_KEY_RETIRED_TTL must be a positive duration",
			"room_key_retired_ttl", cfg.RoomKeyRetiredTTL)
		os.Exit(1)
	}
```

Then replace the `keyStore := roomkeystore.NewMongoStore(...)` line (line ~147) with:

```go
	roomKeyDB := mongoClient.Database(cfg.MongoDB)
	keyStore := roomkeystore.NewMongoStore(
		roomKeyDB.Collection("rooms"),
		cfg.RoomKeyGracePeriod,
		roomkeystore.WithRetiredKeys(roomKeyDB.Collection("retired_room_keys"), cfg.RoomKeyRetiredTTL),
	)
	keyIdxCtx, keyIdxCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := keyStore.EnsureIndexes(keyIdxCtx); err != nil {
		keyIdxCancel()
		slog.Error("ensure room key indexes failed", "error", err)
		os.Exit(1)
	}
	keyIdxCancel()
```

Apply the same three edits to `room-service/main.go` (its collection handle is `db`, so use `db.Collection(...)`) and `bot-room-service/main.go` (handle is `mc.Database(cfg.MongoDB)`; that file returns errors instead of calling `os.Exit`, so use `return fmt.Errorf("ensure room key indexes: %w", err)` and the existing `ensureCtx` pattern rather than `os.Exit`).

- [ ] **Step 3: Verify everything compiles and existing tests pass**

Run: `make lint && make test`
Expected: clean lint, all tests pass.

- [ ] **Step 4: Add the env var to the local compose files**

In each of `room-worker/deploy/docker-compose.yml`, `bot-room-service/deploy/docker-compose.yml`, and `room-service/deploy/docker-compose.yml`, add to the service's `environment:` block, next to the existing `ROOM_KEY_GRACE_PERIOD` entry:

```yaml
      ROOM_KEY_RETIRED_TTL: 20m
```

If a file has no `ROOM_KEY_GRACE_PERIOD` entry, add `ROOM_KEY_RETIRED_TTL` at the end of the `environment:` block instead.

- [ ] **Step 5: Commit**

```bash
git add room-worker/ bot-room-service/ room-service/
git commit -m "feat: wire the retired-key archive into the room key services"
```

---

### Task 5: `broadcast-worker` startup guard

`broadcast-worker` is the only service that knows both the cache TTL and the retention, so it enforces the cross-service constraint.

**Files:**
- Modify: `broadcast-worker/keycache.go`
- Modify: `broadcast-worker/main.go`
- Test: `broadcast-worker/keycache_test.go`

**Interfaces:**
- Produces: `retiredTTLSafe(retiredTTL, cacheTTL time.Duration) bool`; `Config.RoomKeyRetiredTTL time.Duration`.

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/keycache_test.go`:

```go
func TestRetiredTTLSafe(t *testing.T) {
	tests := []struct {
		name       string
		retiredTTL time.Duration
		cacheTTL   time.Duration
		want       bool
	}{
		{"exactly twice is safe", 20 * time.Minute, 10 * time.Minute, true},
		{"more than twice is safe", time.Hour, 10 * time.Minute, true},
		{"less than twice is unsafe", 15 * time.Minute, 10 * time.Minute, false},
		{"equal is unsafe", 10 * time.Minute, 10 * time.Minute, false},
		{"zero retention is unsafe", 0, 10 * time.Minute, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, retiredTTLSafe(tt.retiredTTL, tt.cacheTTL))
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `undefined: retiredTTLSafe`.

- [ ] **Step 3: Implement the guard**

Add to `broadcast-worker/keycache.go`, directly below `keyCacheTTLSafe`:

```go
// retiredTTLSafe reports whether the retired-key retention outlasts a cached key
// by enough for a client to fetch the version it was stamped with. A version can
// be stamped at the very end of a cache entry's life, so retention must cover
// that entry plus the client's fetch and retry. Below this, a burst of rotations
// can produce messages no client can decrypt.
func retiredTTLSafe(retiredTTL, cacheTTL time.Duration) bool {
	return retiredTTL >= 2*cacheTTL
}
```

Add the config field to `broadcast-worker/main.go`, below `RoomKeyCacheSize` (line ~58):

```go
	// RoomKeyRetiredTTL mirrors the room key services' ROOM_KEY_RETIRED_TTL.
	// Read here only to fail fast when it is too short for this cache's TTL.
	RoomKeyRetiredTTL time.Duration `env:"ROOM_KEY_RETIRED_TTL" envDefault:"20m"`
```

In the key-cache `switch` in `main.go` (line ~200), add a case immediately **before** `default:`:

```go
	case !retiredTTLSafe(cfg.RoomKeyRetiredTTL, cfg.RoomKeyCacheTTL):
		// A cached key can be stamped into a message the client must later fetch
		// by version; too short a retention makes that fetch fail. Refuse to start
		// rather than serve undecryptable messages.
		slog.Error("ROOM_KEY_RETIRED_TTL must be at least twice ROOM_KEY_CACHE_TTL",
			"retired_ttl", cfg.RoomKeyRetiredTTL, "cache_ttl", cfg.RoomKeyCacheTTL)
		os.Exit(1)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS.

- [ ] **Step 5: Add the env var to broadcast-worker's compose file**

In `broadcast-worker/deploy/docker-compose.yml`, next to the existing `ROOM_KEY_CACHE_TTL` entry:

```yaml
      ROOM_KEY_RETIRED_TTL: 20m
```

- [ ] **Step 6: Commit**

```bash
git add broadcast-worker/
git commit -m "feat(broadcast-worker): fail fast when retired-key retention is too short"
```

---

### Task 6: Rotate before fan-out in `room-worker`

**Files:**
- Modify: `room-worker/handler.go:324-361` (`rotateAndFanOut`)
- Modify: `room-worker/store.go:165-176` (remove `SetWithVersion`)
- Modify: `room-worker/mock_publisher_test.go:70-72` (drop `SetWithVersion` from `stubRoomKeyStore`)
- Test: `room-worker/handler_test.go` — **replaces** the existing `TestHandler_RotateAndFanOut_ErrNoCurrentKey_UsesPredictedVersion` at line ~6156
- Regenerate: `room-worker/mock_store_test.go`

**Interfaces:**
- Produces: `(*Handler).commitRoomKey(ctx context.Context, roomID string, currentPair *roomkeystore.VersionedKeyPair, newPair *roomkeystore.RoomKeyPair) (int, error)`. `rotateAndFanOut`'s signature is unchanged.
- Existing test fixtures: `testKeySender` (`roomkeysender.NewSender(&mockPublisher{})`) and `testKeyStore` (`stubRoomKeyStore{}`) in `mock_publisher_test.go`; `MockRoomKeyStore` in `mock_store_test.go`.

- [ ] **Step 1: Delete the test that pins the old contract**

`room-worker/handler_test.go` line ~6150 holds
`TestHandler_RotateAndFanOut_ErrNoCurrentKey_UsesPredictedVersion`, whose doc
comment explicitly pins the behaviour this task removes ("the fallback calls
SetWithVersion at predictedVersion … rather than Set"). Delete the whole test
function and its doc comment. Step 2 replaces it.

- [ ] **Step 2: Write the failing tests**

`h.keySender` is a concrete `*roomkeysender.Sender`, not an interface, so the
fan-out is captured by giving the sender a recording `Publisher`. Fan-out runs on
a worker pool, so the recorder needs a mutex. Append to
`room-worker/handler_test.go`:

```go
// keyEventRecorder captures RoomKeyEvent payloads published through a real
// roomkeysender.Sender. fanOutKey publishes concurrently, hence the mutex.
type keyEventRecorder struct {
	mu     sync.Mutex
	events []model.RoomKeyEvent
}

func (r *keyEventRecorder) Publish(_ string, data []byte) error {
	var evt model.RoomKeyEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
	return nil
}

func (r *keyEventRecorder) captured() []model.RoomKeyEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]model.RoomKeyEvent(nil), r.events...)
}

// newRotateTestHandler builds a Handler wired to mockKeys and a recording key
// sender, mirroring the direct-construction style already used in this file.
func newRotateTestHandler(t *testing.T, ctrl *gomock.Controller, mockKeys *MockRoomKeyStore) (*Handler, *keyEventRecorder) {
	t.Helper()
	rec := &keyEventRecorder{}
	h := &Handler{
		store:     NewMockSubscriptionStore(ctrl),
		siteID:    "site-a",
		keyStore:  mockKeys,
		keySender: roomkeysender.NewSender(rec),
		publish: func(_ context.Context, _ string, _ []byte, _ string) error {
			return nil
		},
	}
	return h, rec
}

func TestHandler_RotateAndFanOut_FansOutStoreAssignedVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKeys := NewMockRoomKeyStore(ctrl)

	// The store assigns 7, not the predicted 6 — a concurrent removal got there
	// first. Fanning out 6 would label these bytes with a version the store gave
	// to a different key, which clients then cache permanently.
	mockKeys.EXPECT().
		Rotate(gomock.Any(), "test-room", gomock.Any()).
		Return(7, nil)

	h, rec := newRotateTestHandler(t, ctrl, mockKeys)
	currentPair := &roomkeystore.VersionedKeyPair{
		Version: 5,
		KeyPair: roomkeystore.RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0xAA}, 32)},
	}

	require.NoError(t, h.rotateAndFanOut(context.Background(), "test-room", currentPair, []string{"alice"}))

	events := rec.captured()
	require.Len(t, events, 1)
	assert.Equal(t, 7, events[0].Version,
		"fan-out must carry the version the store assigned, never current+1")
}

func TestHandler_RotateAndFanOut_ErrNoCurrentKey_AdoptsSetVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKeys := NewMockRoomKeyStore(ctrl)

	// The key vanished between the caller's Get and Rotate. Nothing has been
	// fanned out yet, so plain Set at v0 is correct — there is no already-
	// distributed version to match.
	gomock.InOrder(
		mockKeys.EXPECT().
			Rotate(gomock.Any(), "test-room", gomock.Any()).
			Return(0, roomkeystore.ErrNoCurrentKey),
		mockKeys.EXPECT().
			Set(gomock.Any(), "test-room", gomock.Any()).
			Return(0, nil),
	)

	h, rec := newRotateTestHandler(t, ctrl, mockKeys)
	currentPair := &roomkeystore.VersionedKeyPair{
		Version: 4,
		KeyPair: roomkeystore.RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0xAA}, 32)},
	}

	require.NoError(t, h.rotateAndFanOut(context.Background(), "test-room", currentPair, []string{"alice"}))

	events := rec.captured()
	require.Len(t, events, 1)
	assert.Equal(t, 0, events[0].Version, "the Set fallback adopts version 0")
}

func TestHandler_RotateAndFanOut_StoreFailureFansOutNothing(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKeys := NewMockRoomKeyStore(ctrl)

	mockKeys.EXPECT().
		Rotate(gomock.Any(), "test-room", gomock.Any()).
		Return(0, errors.New("mongo down"))

	h, rec := newRotateTestHandler(t, ctrl, mockKeys)
	currentPair := &roomkeystore.VersionedKeyPair{
		Version: 5,
		KeyPair: roomkeystore.RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0xAA}, 32)},
	}

	require.Error(t, h.rotateAndFanOut(context.Background(), "test-room", currentPair, []string{"alice"}))
	assert.Empty(t, rec.captured(), "a failed rotation must not hand survivors a phantom key")
}
```

Ensure the file imports `encoding/json`, `sync`, `bytes`, `errors`, and
`github.com/hmchangw/chat/pkg/roomkeysender`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `make test SERVICE=room-worker`
Expected: FAIL — the current implementation fans out `current + 1` (6) so the
first test fails its version assertion; the second gets an unexpected call to
`Set` (the code calls `SetWithVersion`); the third captures one event because
fan-out happens before the store call.

- [ ] **Step 4: Rewrite `rotateAndFanOut`**

Replace `room-worker/handler.go:324-361` with:

```go
// rotateAndFanOut commits the new key, then fans out the version the store
// assigned. Committing first is what makes that version correct under
// concurrent removals: predicting current+1 labels two different keys with the
// same version when two removals race, and clients cache the wrong bytes under
// it permanently. The brief window where broadcast-worker encrypts at the new
// version before a survivor holds it is self-healing — the client fetches the
// unknown version via key.get.
// survivorAccounts is a pre-computed post-deletion snapshot of the room's member accounts.
func (h *Handler) rotateAndFanOut(ctx context.Context, roomID string, currentPair *roomkeystore.VersionedKeyPair, survivorAccounts []string) error {
	newPair, err := roomkeystore.GenerateKeyPair()
	if err != nil {
		return fmt.Errorf("generate room key: %w", err)
	}

	version, err := h.commitRoomKey(ctx, roomID, currentPair, newPair)
	if err != nil {
		return err
	}

	h.fanOutRoomKeyToSurvivors(ctx, roomID,
		&roomkeystore.VersionedKeyPair{Version: version, KeyPair: *newPair}, survivorAccounts)
	return nil
}

// commitRoomKey persists newPair and returns the version the store assigned. A
// room with no current key adopts version 0 via Set; ErrNoCurrentKey means the
// key vanished between the caller's Get and here, which Set handles identically.
func (h *Handler) commitRoomKey(ctx context.Context, roomID string, currentPair *roomkeystore.VersionedKeyPair, newPair *roomkeystore.RoomKeyPair) (int, error) {
	if currentPair != nil {
		version, err := h.keyStore.Rotate(ctx, roomID, *newPair)
		if err == nil {
			return version, nil
		}
		if !errors.Is(err, roomkeystore.ErrNoCurrentKey) {
			roomkeymetrics.StoreErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Rotate")))
			return 0, fmt.Errorf("rotate room key: %w", err)
		}
	}
	version, err := h.keyStore.Set(ctx, roomID, *newPair)
	if err != nil {
		roomkeymetrics.StoreErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Set")))
		return 0, fmt.Errorf("store room key: %w", err)
	}
	return version, nil
}
```

- [ ] **Step 5: Remove the now-unused `SetWithVersion` from the store interface**

Delete this block from `room-worker/store.go:170-173`:

```go
	// SetWithVersion writes pair at an explicit version. Used by the rotate
	// fallback when Rotate finds no current key but fan-out already committed
	// to predictedVersion = currentPair.Version + 1.
	SetWithVersion(ctx context.Context, roomID string, pair roomkeystore.RoomKeyPair, version int) error
```

Also delete the corresponding stub method from
`room-worker/mock_publisher_test.go:70-72`:

```go
func (stubRoomKeyStore) SetWithVersion(_ context.Context, _ string, _ roomkeystore.RoomKeyPair, _ int) error {
	return nil
}
```

- [ ] **Step 6: Regenerate mocks**

Run: `make generate SERVICE=room-worker`
Expected: `room-worker/mock_store_test.go` no longer contains `SetWithVersion`.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `make test SERVICE=room-worker`
Expected: PASS. If any remaining test sets a `SetWithVersion` expectation, delete it — the path no longer exists.

- [ ] **Step 8: Commit**

```bash
git add room-worker/
git commit -m "fix(room-worker): fan out the rotated key version the store assigned"
```

---

### Task 7: Rotate before fan-out in `bot-room-service`

Same defect, same fix, different service. The code differs — `bot-room-service` loads survivors itself and calls `fanOutKey` with a value rather than a pointer.

**Files:**
- Modify: `bot-room-service/handler.go:448-488` (`rotateAndFanOut`)
- Modify: `bot-room-service/store.go:51-52` (remove `SetWithVersion`)
- Modify: `bot-room-service/roomkey_test.go` (drop `SetWithVersionFn` from `fakeKeyStore`)
- Test: `bot-room-service/handler_remove_key_test.go` — **rewrites** `TestHandleRemove_RotateNoCurrentKey_FallsBackToSetWithVersion` at line ~164

**Interfaces:**
- Produces: `(*handler).commitKey(ctx context.Context, roomID string, currentPair *roomkeystore.VersionedKeyPair, newPair *roomkeystore.RoomKeyPair) (int, error)`. `rotateAndFanOut`'s signature is unchanged.
- Existing test fixtures: **this package has no mockgen infrastructure.** It uses hand-written fakes in `roomkey_test.go` — `fakeKeyStore` (fields `SetFn`, `GetFn`, `RotateFn`, `SetWithVersionFn`) and `fakePublisher` (captures `subjects []string` and `payloads [][]byte`). Do **not** run `make generate` for this service.

- [ ] **Step 1: Flip the ordering assertions in the existing test**

`TestHandleRemove_DiffNonEmpty_RotatesAndFansOutToSurvivorsInOrder` (line ~36)
asserts the exact ordering this task inverts. Replace its doc comment and its
final assertion block. Everything above `require.Len(t, order, 3, ...)` — the
`fakeStore`, `fakeKeyStore`, `orderedPublisher`, handler construction, and the
per-payload version assertions — stays as it is.

Replace the doc comment with:

```go
// TestHandleRemove_DiffNonEmpty_RotatesThenFansOutToSurvivors: when at least one
// account is actually removed, Rotate commits FIRST and survivors then receive
// the version the store assigned. Predicting current+1 and fanning out first
// labels two different keys with the same version when removals race, which
// clients cache permanently.
func TestHandleRemove_DiffNonEmpty_RotatesThenFansOutToSurvivors(t *testing.T) {
```

Replace the trailing assertions with:

```go
	require.Len(t, order, 3, "1 rotate + 2 fan-out sends")
	assert.Equal(t, "rotate", order[0], "rotate must be the FIRST call, before any fan-out send")
	assert.NotEqual(t, "rotate", order[1], "fan-out follows rotate")
	assert.NotEqual(t, "rotate", order[2], "fan-out follows rotate")
```

The existing per-payload `assert.Equal(t, 4, evt.Version, ...)` assertions still
hold — `RotateFn` returns 4 — but update that assertion's message to
`"survivors get the version Rotate returned"`.

- [ ] **Step 2: Rewrite the SetWithVersion fallback test**

Replace `TestHandleRemove_RotateNoCurrentKey_FallsBackToSetWithVersion`
(line ~164) with:

```go
// TestHandleRemove_RotateNoCurrentKey_FallsBackToSet pins the post-rotate-first
// contract: nothing has been fanned out when Rotate reports ErrNoCurrentKey, so
// there is no already-distributed version to match and plain Set at v0 is
// correct. Before rotate-first this path needed SetWithVersion to match a
// version the fan-out had already committed to.
func TestHandleRemove_RotateNoCurrentKey_FallsBackToSet(t *testing.T) {
	var setCalled bool
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
		ListRoomMemberAccountsFn: func(_ context.Context, _ string) ([]string, error) {
			return []string{"carol"}, nil
		},
	}
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return &roomkeystore.VersionedKeyPair{
				Version: 4,
				KeyPair: roomkeystore.RoomKeyPair{PrivateKey: []byte("old-key-bytes-0123456789012345")},
			}, nil
		},
		RotateFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
			return 0, roomkeystore.ErrNoCurrentKey
		},
		SetFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
			setCalled = true
			return 0, nil
		},
	}
	var order []string
	pub := &orderedPublisher{log: &order}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(pub))
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.NoError(t, err)

	assert.True(t, setCalled, "the ErrNoCurrentKey fallback must use Set, not SetWithVersion")
	require.Len(t, pub.payloads, 1)
	var evt model.RoomKeyEvent
	require.NoError(t, json.Unmarshal(pub.payloads[0], &evt))
	assert.Equal(t, 0, evt.Version, "the Set fallback adopts version 0")
}
```

- [ ] **Step 3: Write the failing test for a failed rotation**

Append to `bot-room-service/handler_remove_key_test.go`:

```go
// TestHandleRemove_RotateFails_FansOutNothing: a rotation that never commits
// must not hand survivors a key the store does not have. Under the old
// fan-out-first ordering, survivors received a phantom version whose number a
// later rotation would reassign to different bytes.
func TestHandleRemove_RotateFails_FansOutNothing(t *testing.T) {
	store := &fakeStore{
		FindRoomFn: func(_ context.Context, _ string) (*Room, error) {
			return &Room{ID: "r1", Type: "c", CreatedByBot: "bot-1"}, nil
		},
		DeleteSubscriptionFn: func(_ context.Context, _, _ string) (bool, error) { return true, nil },
		FindUserFn: func(_ context.Context, id string) (*model.User, error) {
			return &model.User{ID: id, Account: "bob", SiteID: "site-a"}, nil
		},
		ListRoomMemberAccountsFn: func(_ context.Context, _ string) ([]string, error) {
			return []string{"carol"}, nil
		},
	}
	keyStore := &fakeKeyStore{
		GetFn: func(_ context.Context, _ string) (*roomkeystore.VersionedKeyPair, error) {
			return &roomkeystore.VersionedKeyPair{
				Version: 5,
				KeyPair: roomkeystore.RoomKeyPair{PrivateKey: []byte("old-key-bytes-0123456789012345")},
			}, nil
		},
		RotateFn: func(_ context.Context, _ string, _ roomkeystore.RoomKeyPair) (int, error) {
			return 0, errors.New("mongo down")
		},
	}
	var order []string
	pub := &orderedPublisher{log: &order}
	h := newHandler(store, "site-a", nil, (&captureOutbox{}).publish, keyStore, roomkeysender.NewSender(pub))
	c := withIdentity(t, "r1", ident())

	_, err := h.handleRemove(c, BotMembersBatchRequest{UserIDs: []string{"bob-id"}})
	require.Error(t, err)
	assert.Empty(t, pub.payloads, "a failed rotation must not hand survivors a phantom key")
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `make test SERVICE=bot-room-service`
Expected: FAIL — Step 1 fails because the current code sends before rotating;
Step 2 fails on an unexpected `Set` (the code calls `SetWithVersion`); Step 3
fails because fan-out already happened before `Rotate` returned its error.

- [ ] **Step 5: Rewrite `rotateAndFanOut`**

Replace `bot-room-service/handler.go:448-488` with:

```go
// rotateAndFanOut commits the new key, then fans out the version the store
// assigned. Predicting current+1 labels two different keys with the same
// version when removals race, and clients cache the wrong bytes permanently.
func (h *handler) rotateAndFanOut(ctx context.Context, roomID string) error {
	survivors, err := h.store.ListRoomMemberAccounts(ctx, roomID)
	if err != nil {
		return fmt.Errorf("list survivors: %w", err)
	}

	currentPair, err := h.keyStore.Get(ctx, roomID)
	if err != nil && !errors.Is(err, roomkeystore.ErrNoCurrentKey) {
		return fmt.Errorf("get current key: %w", err)
	}

	newPair, err := roomkeystore.GenerateKeyPair()
	if err != nil {
		return fmt.Errorf("generate new key: %w", err)
	}

	version, err := h.commitKey(ctx, roomID, currentPair, newPair)
	if err != nil {
		return err
	}

	h.fanOutKey(ctx, roomID, survivors,
		model.RoomKeyEvent{RoomID: roomID, Version: version, PrivateKey: newPair.PrivateKey},
		"fan out rotated key failed")
	return nil
}

// commitKey persists newPair and returns the version the store assigned.
func (h *handler) commitKey(ctx context.Context, roomID string, currentPair *roomkeystore.VersionedKeyPair, newPair *roomkeystore.RoomKeyPair) (int, error) {
	if currentPair != nil {
		version, err := h.keyStore.Rotate(ctx, roomID, *newPair)
		if err == nil {
			return version, nil
		}
		if !errors.Is(err, roomkeystore.ErrNoCurrentKey) {
			return 0, fmt.Errorf("rotate key: %w", err)
		}
	}
	slog.WarnContext(ctx, "no current key on remove-member; adopting a fresh key at v0", "roomID", roomID)
	version, err := h.keyStore.Set(ctx, roomID, *newPair)
	if err != nil {
		return 0, fmt.Errorf("store new key: %w", err)
	}
	return version, nil
}
```

Confirm `bot-room-service/store.go` declares `Set(ctx context.Context, roomID string, pair roomkeystore.RoomKeyPair) (int, error)` on its `RoomKeyStore` interface. If it does not, add it with the comment `// Set writes a fresh keypair at version 0 — the rotate fallback when no current key exists.`

- [ ] **Step 6: Remove the now-unused `SetWithVersion` from the interface and the fake**

Delete this block from `bot-room-service/store.go:51-52`:

```go
	// SetWithVersion is the Rotate-ErrNoCurrentKey fallback: stamps newPair at the caller-supplied version so it matches what was already fanned out to survivors.
	SetWithVersion(ctx context.Context, roomID string, newPair roomkeystore.RoomKeyPair, version int) error
```

Then delete the matching field and method from `fakeKeyStore` in
`bot-room-service/roomkey_test.go` — the `SetWithVersionFn` struct field and:

```go
func (f *fakeKeyStore) SetWithVersion(ctx context.Context, roomID string, newPair roomkeystore.RoomKeyPair, version int) error {
	if f.SetWithVersionFn != nil {
		return f.SetWithVersionFn(ctx, roomID, newPair, version)
	}
	return nil
}
```

There are no generated mocks in this service — do **not** run `make generate`.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `make test SERVICE=bot-room-service`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add bot-room-service/
git commit -m "fix(bot-room-service): fan out the rotated key version the store assigned"
```

---

### Task 8: Count unresolvable key versions in `room-service`

The only signal that retention was insufficient. Without it the failure is silent.

**Files:**
- Modify: `room-service/handler.go:482-488` (`getRoomKey`)
- Test: `room-service/handler_test.go`

**Interfaces:**
- Consumes: `roomkeymetrics.KeyAbsentErrors` (already defined in `pkg/roomkeymetrics/metrics.go`).

- [ ] **Step 1: Confirm the existing regression guard**

No new test is needed, and no honest Red phase exists here: adding a counter
changes no observable contract, and the OTel global meter is not assertable from
this package. The guard already exists — `room-service/handler_test.go` line
~5072 has a table case that stubs
`ks.EXPECT().GetByVersion(gomock.Any(), roomID, 1).Return(nil, nil)` and expects
the `"room key not available"` error. That case must keep passing after this
change; it is what proves the counter did not alter the reply path.

Run: `make test SERVICE=room-service`
Expected: PASS — establishes the baseline before the edit.

- [ ] **Step 2: Add the counter**

In `room-service/handler.go`, replace:

```go
	if pair == nil {
		return nil, errRoomKeyAbsent
	}
```

(the one at line ~486, inside the `req.Version != nil` branch — not the `existing == nil` check above it) with:

```go
	if pair == nil {
		// Neither the current slot, the previous slot, nor the retired-key
		// archive holds this version. This is the only signal that retention was
		// too short for a version broadcast-worker actually stamped.
		roomkeymetrics.KeyAbsentErrors.Add(ctx, 1)
		return nil, errRoomKeyAbsent
	}
```

Add the import:

```go
	"github.com/hmchangw/chat/pkg/roomkeymetrics"
```

- [ ] **Step 3: Run the tests to verify they pass**

Run: `make test SERVICE=room-service`
Expected: PASS — the existing absent-version case still returns the sentinel.

- [ ] **Step 4: Run the full verification sweep**

Run: `make lint && make test && make sast`
Expected: all clean. `make sast` is a blocking CI gate — fix findings rather than suppressing, unless a finding is a genuine false positive, in which case add `// #nosec <RULE> -- reason` directly above the statement.

- [ ] **Step 5: Commit**

```bash
git add room-service/
git commit -m "feat(room-service): count key.get requests for unresolvable versions"
```

---

## Final verification

- [ ] **Run the full integration suite**

Run: `make test-integration SERVICE=pkg/roomkeystore && make test-integration SERVICE=room-worker && make test-integration SERVICE=room-service`
Expected: all PASS (requires Docker).

- [ ] **Confirm the end-to-end behaviour by hand**

With the local stack up (`make up`), remove three members from a channel in quick succession, then confirm from the `retired_room_keys` collection that every demoted version has a document, and that `key.get` with the oldest of those versions returns the key rather than `errRoomKeyAbsent`.

- [ ] **Delete session-scoped review reports before opening a PR**

`docs/reviews/` holds working notes, not shippable artifacts. Remove every file under it from the branch if any exist.
