//go:build integration

package roomkeystore

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/testutil"
)

// setupMongo returns a Mongo-backed store over an isolated test database's
// rooms collection, plus the collection for direct fixture setup.
func setupMongo(t *testing.T, gracePeriod time.Duration) (*mongoStore, *mongo.Collection) {
	t.Helper()
	db := testutil.MongoDB(t, "roomkeystore")
	col := db.Collection("rooms")
	return newMongoStore(col, gracePeriod), col
}

// insertRoom creates a minimal room document so a key can be attached to it.
func insertRoom(t *testing.T, col *mongo.Collection, roomID string) {
	t.Helper()
	_, err := col.InsertOne(context.Background(), bson.M{"_id": roomID})
	require.NoError(t, err)
}

func TestMongoStore_Integration_RoundTrip(t *testing.T) {
	store, col := setupMongo(t, time.Hour)
	ctx := context.Background()
	insertRoom(t, col, "room-1")

	privKey := bytes.Repeat([]byte{0xCD}, 32)
	pair := RoomKeyPair{PrivateKey: privKey}

	ver, err := store.Set(ctx, "room-1", pair)
	require.NoError(t, err)
	assert.Equal(t, 0, ver)

	got, err := store.Get(ctx, "room-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 0, got.Version)
	assert.Equal(t, privKey, got.KeyPair.PrivateKey)

	err = store.Delete(ctx, "room-1")
	require.NoError(t, err)

	got, err = store.Get(ctx, "room-1")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestMongoStore_Integration_SetRoomNotFound(t *testing.T) {
	store, _ := setupMongo(t, time.Hour)
	ctx := context.Background()

	_, err := store.Set(ctx, "ghost-room", RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0x01}, 32)})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrRoomNotFound), "Set on a missing room must return ErrRoomNotFound")
}

func TestMongoStore_Integration_SetIfAbsentRoomNotFound(t *testing.T) {
	store, _ := setupMongo(t, time.Hour)

	got, err := store.SetIfAbsent(context.Background(), "ghost-room",
		RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0x07}, 32)})
	require.Error(t, err)
	assert.Nil(t, got)
	assert.True(t, errors.Is(err, ErrRoomNotFound), "SetIfAbsent on a missing room must return ErrRoomNotFound")
}

func TestMongoStore_Integration_SetIfAbsentKeepsExistingKey(t *testing.T) {
	store, col := setupMongo(t, time.Hour)
	ctx := context.Background()
	insertRoom(t, col, "room-occupied")

	installed := bytes.Repeat([]byte{0x41}, 32)
	_, err := store.Set(ctx, "room-occupied", RoomKeyPair{PrivateKey: installed})
	require.NoError(t, err)

	got, err := store.SetIfAbsent(ctx, "room-occupied", RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0x42}, 32)})
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 0, got.Version)
	assert.Equal(t, installed, got.KeyPair.PrivateKey, "an occupied slot must be reported back, not overwritten")

	held, err := store.Get(ctx, "room-occupied")
	require.NoError(t, err)
	require.NotNil(t, held)
	assert.Equal(t, installed, held.KeyPair.PrivateKey)
}

// The regression guard for the v0 split-brain: concurrent fallbacks on a keyless
// room must all report the single key the document ends up holding.
func TestMongoStore_Integration_SetIfAbsentConcurrentCallersConverge(t *testing.T) {
	store, col := setupMongo(t, time.Hour)
	ctx := context.Background()
	insertRoom(t, col, "room-race")

	const callers = 8
	results := make([]*VersionedKeyPair, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = store.SetIfAbsent(ctx, "room-race",
				RoomKeyPair{PrivateKey: bytes.Repeat([]byte{byte(0x50 + i)}, 32)})
		}()
	}
	close(start)
	wg.Wait()

	held, err := store.Get(ctx, "room-race")
	require.NoError(t, err)
	require.NotNil(t, held)

	for i := range callers {
		require.NoError(t, errs[i], "caller %d", i)
		require.NotNil(t, results[i], "caller %d must never get (nil, nil)", i)
		assert.Equal(t, 0, results[i].Version, "caller %d", i)
		assert.Equal(t, held.KeyPair.PrivateKey, results[i].KeyPair.PrivateKey,
			"caller %d must return the key the room holds, not its own candidate", i)
	}
}

func TestMongoStore_Integration_MissingKey(t *testing.T) {
	store, col := setupMongo(t, time.Hour)
	ctx := context.Background()

	// Absent room.
	got, err := store.Get(ctx, "nonexistent-room")
	require.NoError(t, err)
	assert.Nil(t, got, "Get on missing room must return nil, nil")

	// Room exists but has no key yet.
	insertRoom(t, col, "keyless-room")
	got, err = store.Get(ctx, "keyless-room")
	require.NoError(t, err)
	assert.Nil(t, got, "Get on a keyless room must return nil, nil")
}

func TestMongoStore_Integration_RotateRoundTrip(t *testing.T) {
	store, col := setupMongo(t, time.Hour)
	ctx := context.Background()
	insertRoom(t, col, "room-rot")

	oldPriv := bytes.Repeat([]byte{0xBB}, 32)
	newPriv := bytes.Repeat([]byte{0xDD}, 32)

	ver, err := store.Set(ctx, "room-rot", RoomKeyPair{PrivateKey: oldPriv})
	require.NoError(t, err)
	assert.Equal(t, 0, ver)

	ver, err = store.Rotate(ctx, "room-rot", RoomKeyPair{PrivateKey: newPriv})
	require.NoError(t, err)
	assert.Equal(t, 1, ver)

	got, err := store.Get(ctx, "room-rot")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 1, got.Version)
	assert.Equal(t, newPriv, got.KeyPair.PrivateKey)

	oldPair, err := store.GetByVersion(ctx, "room-rot", 0)
	require.NoError(t, err)
	require.NotNil(t, oldPair)
	assert.Equal(t, oldPriv, oldPair.PrivateKey)

	newPair, err := store.GetByVersion(ctx, "room-rot", 1)
	require.NoError(t, err)
	require.NotNil(t, newPair)
	assert.Equal(t, newPriv, newPair.PrivateKey)

	unknown, err := store.GetByVersion(ctx, "room-rot", 999)
	require.NoError(t, err)
	assert.Nil(t, unknown)
}

func TestMongoStore_Integration_GracePeriodExpiry(t *testing.T) {
	store, col := setupMongo(t, time.Hour)
	ctx := context.Background()
	insertRoom(t, col, "room-grace")

	oldPriv := bytes.Repeat([]byte{0x02}, 32)
	newPriv := bytes.Repeat([]byte{0x04}, 32)

	_, err := store.Set(ctx, "room-grace", RoomKeyPair{PrivateKey: oldPriv})
	require.NoError(t, err)

	_, err = store.Rotate(ctx, "room-grace", RoomKeyPair{PrivateKey: newPriv})
	require.NoError(t, err)

	oldPair, err := store.GetByVersion(ctx, "room-grace", 0)
	require.NoError(t, err)
	require.NotNil(t, oldPair, "old key should be retrievable during grace period")

	// Advance the store's clock past the grace period rather than sleeping.
	store.now = func() time.Time { return time.Now().Add(2 * time.Hour) }

	oldPair, err = store.GetByVersion(ctx, "room-grace", 0)
	require.NoError(t, err)
	assert.Nil(t, oldPair, "old key should be expired after grace period")

	got, err := store.Get(ctx, "room-grace")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 1, got.Version)
}

func TestMongoStore_Integration_RotateNoCurrentKey(t *testing.T) {
	store, col := setupMongo(t, time.Hour)
	ctx := context.Background()
	insertRoom(t, col, "room-empty")

	_, err := store.Rotate(ctx, "room-empty", RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0x02}, 32)})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoCurrentKey), "should return ErrNoCurrentKey")
}

func TestMongoStore_Integration_DeleteBothKeys(t *testing.T) {
	store, col := setupMongo(t, time.Hour)
	ctx := context.Background()
	insertRoom(t, col, "room-del")

	_, err := store.Set(ctx, "room-del", RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0xBB}, 32)})
	require.NoError(t, err)

	_, err = store.Rotate(ctx, "room-del", RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0xDD}, 32)})
	require.NoError(t, err)

	err = store.Delete(ctx, "room-del")
	require.NoError(t, err)

	got, err := store.Get(ctx, "room-del")
	require.NoError(t, err)
	assert.Nil(t, got)

	prev, err := store.GetByVersion(ctx, "room-del", 0)
	require.NoError(t, err)
	assert.Nil(t, prev)
}

func TestMongoStore_Integration_GetMany(t *testing.T) {
	store, col := setupMongo(t, time.Hour)
	ctx := context.Background()

	priv1 := bytes.Repeat([]byte{0x02}, 32)
	priv2 := bytes.Repeat([]byte{0x04}, 32)
	priv3 := bytes.Repeat([]byte{0x06}, 32)

	for _, id := range []string{"getmany-room-1", "getmany-room-2", "getmany-room-3", "getmany-keyless"} {
		insertRoom(t, col, id)
	}
	_, err := store.Set(ctx, "getmany-room-1", RoomKeyPair{PrivateKey: priv1})
	require.NoError(t, err)
	_, err = store.Set(ctx, "getmany-room-2", RoomKeyPair{PrivateKey: priv2})
	require.NoError(t, err)
	_, err = store.Set(ctx, "getmany-room-3", RoomKeyPair{PrivateKey: priv3})
	require.NoError(t, err)

	got, err := store.GetMany(ctx, []string{"getmany-room-1", "getmany-room-2", "getmany-room-3", "getmany-keyless", "getmany-room-missing"})
	require.NoError(t, err)
	require.Len(t, got, 3, "keyless and missing rooms must be omitted from result")

	require.Contains(t, got, "getmany-room-1")
	assert.Equal(t, 0, got["getmany-room-1"].Version)
	assert.Equal(t, priv1, got["getmany-room-1"].KeyPair.PrivateKey)

	require.Contains(t, got, "getmany-room-2")
	assert.Equal(t, priv2, got["getmany-room-2"].KeyPair.PrivateKey)

	require.Contains(t, got, "getmany-room-3")
	assert.Equal(t, priv3, got["getmany-room-3"].KeyPair.PrivateKey)

	_, missing := got["getmany-room-missing"]
	assert.False(t, missing)
	_, keyless := got["getmany-keyless"]
	assert.False(t, keyless)

	empty, err := store.GetMany(ctx, []string{})
	require.NoError(t, err)
	require.NotNil(t, empty)
	assert.Empty(t, empty)
}

func TestMongoStore_EnsureIndexes_CreatesTTLIndex(t *testing.T) {
	db := testutil.MongoDB(t, "roomkey_ttl")
	store := newMongoStore(db.Collection("rooms"), time.Hour,
		WithRetiredKeys(db.Collection(RetiredKeysCollection), 20*time.Minute))

	require.NoError(t, store.EnsureIndexes(context.Background()))

	cur, err := db.Collection(RetiredKeysCollection).Indexes().List(context.Background())
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

func TestMongoStore_Rotate_ArchivesDemotedVersion(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "roomkey_archive")
	rooms := db.Collection("rooms")
	retired := db.Collection(RetiredKeysCollection)
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
	retired := db.Collection(RetiredKeysCollection)
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

	// Each version was demoted exactly once, however the rotations interleaved.
	for v := range rotations {
		id := retiredKeyID("room2", v)
		err := retired.FindOne(ctx, bson.M{"_id": id}).Err()
		assert.NoError(t, err, "version %d must be archived", v)
	}
}

func TestMongoStore_GetByVersion_FallsBackToArchive(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "roomkey_fallback")
	rooms := db.Collection("rooms")
	retired := db.Collection(RetiredKeysCollection)
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

	cur, err := store.GetByVersion(ctx, "room3", 3)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.Equal(t, bytes.Repeat([]byte{0xD3}, 32), cur.PrivateKey)

	missing, err := store.GetByVersion(ctx, "room3", 99)
	require.NoError(t, err)
	assert.Nil(t, missing)
}

// deadRetiredCollection points at a closed port so every archive write fails fast.
func deadRetiredCollection(t *testing.T) *mongo.Collection {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI(
		"mongodb://127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=200&connectTimeoutMS=200"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	col := client.Database("roomkeystore_dead").Collection(RetiredKeysCollection)
	// Without this the tests below pass vacuously if the handle ever became usable;
	// exclude ErrNoDocuments, which a healthy-but-empty collection also returns.
	probeErr := col.FindOne(context.Background(), bson.M{"_id": "probe"}).Err()
	require.Error(t, probeErr, "the fixture collection must be genuinely unusable")
	require.NotErrorIs(t, probeErr, mongo.ErrNoDocuments,
		"a healthy-but-empty collection also errors here; the handle must be unreachable, not merely empty")
	return col
}

// A failed archive write must NOT fail Rotate: the rotation is already committed,
// and an error would redeliver the removal and rotate a second time.
func TestMongoStore_Rotate_ArchiveWriteFailureStillRotates(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "roomkey_archive_fail")
	rooms := db.Collection("rooms")
	store := newMongoStore(rooms, time.Hour, WithRetiredKeys(deadRetiredCollection(t), 20*time.Minute))

	v0 := bytes.Repeat([]byte{0xF0}, 32)
	_, err := rooms.InsertOne(ctx, bson.M{"_id": "room-archive-fail", "encKey": InitialKeyDoc(RoomKeyPair{PrivateKey: v0})})
	require.NoError(t, err)

	v1 := bytes.Repeat([]byte{0xF1}, 32)
	newVer, err := store.Rotate(ctx, "room-archive-fail", RoomKeyPair{PrivateKey: v1})
	require.NoError(t, err, "an archive-write failure is best-effort and must not surface as a Rotate error")
	assert.Equal(t, 1, newVer)

	// v1 is current and v0 sits in the previous slot, still resolvable there.
	cur, err := store.Get(ctx, "room-archive-fail")
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.Equal(t, 1, cur.Version)
	assert.Equal(t, v1, cur.KeyPair.PrivateKey)

	prev, err := store.GetByVersion(ctx, "room-archive-fail", 0)
	require.NoError(t, err)
	require.NotNil(t, prev, "the demoted key stays readable from the previous slot")
	assert.Equal(t, v0, prev.PrivateKey)
}

// A demoted slot with no bytes archives nothing; a real collection is needed to reach the guard.
func TestMongoStore_archiveRetired_EmptySecretWritesNothing(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "roomkey_archive_empty")
	retired := db.Collection(RetiredKeysCollection)
	store := newMongoStore(db.Collection("rooms"), time.Hour, WithRetiredKeys(retired, 20*time.Minute))

	store.archiveRetired(ctx, "room-empty-secret", 4, nil)
	store.archiveRetired(ctx, "room-empty-secret", 5, []byte{})

	count, err := retired.CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 0, count, "a demoted slot with no secret must not be archived")
}

// $set, not $setOnInsert: version numbers are reusable after Delete, so re-archiving
// must overwrite the stale incarnation's bytes rather than preserve them.
func TestMongoStore_archiveRetired_OverwritesReusedVersion(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "roomkey_archive_reuse")
	retired := db.Collection(RetiredKeysCollection)
	store := newMongoStore(db.Collection("rooms"), time.Hour, WithRetiredKeys(retired, 20*time.Minute))

	first := bytes.Repeat([]byte{0x11}, 32)
	second := bytes.Repeat([]byte{0x22}, 32)
	store.archiveRetired(ctx, "room-reuse", 0, first)
	store.archiveRetired(ctx, "room-reuse", 0, second)

	got, err := store.GetByVersion(ctx, "room-reuse", 0)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, second, got.PrivateKey,
		"the second incarnation's bytes must win; $setOnInsert would serve the first")
}

// The archive write is detached, so a caller deadline cannot lose the document.
func TestMongoStore_archiveRetired_SurvivesCancelledCallerContext(t *testing.T) {
	db := testutil.MongoDB(t, "roomkey_archive_cancel")
	retired := db.Collection(RetiredKeysCollection)
	store := newMongoStore(db.Collection("rooms"), time.Hour, WithRetiredKeys(retired, 20*time.Minute))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	v0 := bytes.Repeat([]byte{0x31}, 32)
	store.archiveRetired(ctx, "room-cancel", 0, v0)

	var doc retiredKeyDoc
	require.NoError(t, retired.FindOne(context.Background(), bson.M{"_id": "room-cancel:0"}).Decode(&doc),
		"the archive write must survive a cancelled caller context")
	assert.Equal(t, v0, doc.Priv)
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
