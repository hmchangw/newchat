package roomkeystore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hmchangw/chat/pkg/roomkeymetrics"
)

// mongoStore is the MongoDB-backed implementation of RoomKeyStore. The room
// encryption key lives as an "encKey" sub-document inside the room's document
// in the rooms collection, so a key shares the lifecycle of its room and is
// read in the same database as room metadata.
type mongoStore struct {
	col         *mongo.Collection
	gracePeriod time.Duration
	now         func() time.Time

	// retiredCol holds one document per retired key version; nil disables the archive.
	retiredCol *mongo.Collection
	retiredTTL time.Duration

	// indexReady is false until EnsureIndexes has succeeded. createIndexes is a
	// write, so a pod that starts during a primary-down incident cannot create
	// the TTL index; archiveRetired retries while this is false so the archive
	// self-heals at the first rotation after recovery, instead of writing into
	// a collection nothing expires for the life of the process.
	indexReady atomic.Bool
}

// RetiredKeysCollection holds retired key versions; every wiring site must name this same one.
const RetiredKeysCollection = "retired_room_keys"

// archiveWriteTimeout bounds the detached archive write in archiveRetired.
const archiveWriteTimeout = 5 * time.Second

// indexRepairTimeout bounds archiveRetired's retry of a startup index ensure.
// Deliberately shorter than archiveWriteTimeout: repairing the index must never
// cost the archive write itself.
const indexRepairTimeout = 2 * time.Second

// Option configures a mongoStore.
type Option func(*mongoStore)

// WithRetiredKeys enables the archive Rotate writes and GetByVersion falls back
// to. ttl must exceed broadcast-worker's ROOM_KEY_CACHE_TTL.
func WithRetiredKeys(col *mongo.Collection, ttl time.Duration) Option {
	return func(s *mongoStore) {
		s.retiredCol = col
		s.retiredTTL = ttl
	}
}

// NewMongoStore returns a RoomKeyStore that stores keys in the encKey field of
// documents in col (the rooms collection). gracePeriod is how long a rotated-out
// previous key remains valid for decrypt. The underlying mongo client is owned
// by the caller; Close is a no-op.
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

var encKeyProjection = bson.M{"encKey": 1}

// InitialKeyDoc returns the encKey sub-document for a freshly provisioned room
// key at version 0. Callers that persist the key inline with the room document
// at creation time (rather than via a follow-up Set) use this so the encKey BSON
// schema stays owned by this package. The previous-key slot is left unset until
// the first Rotate.
func InitialKeyDoc(pair RoomKeyPair) bson.M {
	return bson.M{"priv": pair.PrivateKey, "ver": 0}
}

// setCurrent overwrites the current key slot with priv stamped at version,
// without touching the previous-key slot. Returns ErrRoomNotFound (unwrapped)
// if no room document matched, leaving the caller to add op-specific context.
func (s *mongoStore) setCurrent(ctx context.Context, roomID string, priv []byte, version int) error {
	res, err := s.col.UpdateByID(ctx, roomID, bson.M{"$set": bson.M{
		"encKey.priv": priv,
		"encKey.ver":  version,
	}})
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return ErrRoomNotFound
	}
	return nil
}

// Set writes pair as the room's current key at version 0 without touching the
// previous-key slot. Returns ErrRoomNotFound if no room document exists.
func (s *mongoStore) Set(ctx context.Context, roomID string, pair RoomKeyPair) (int, error) {
	if err := s.setCurrent(ctx, roomID, pair.PrivateKey, 0); err != nil {
		return 0, fmt.Errorf("set room key: %w", err)
	}
	return 0, nil
}

// SetIfAbsent installs pair as the room's current key at version 0 only when the
// room has no current key, and returns whichever key the room holds afterwards —
// the caller's own when it won the race, the winner's when it lost, so competing
// callers converge on one v0 key. Returns ErrRoomNotFound if no room document
// exists; never returns (nil, nil).
func (s *mongoStore) SetIfAbsent(ctx context.Context, roomID string, pair RoomKeyPair) (*VersionedKeyPair, error) {
	var doc roomKeyDoc
	err := s.col.FindOneAndUpdate(ctx,
		bson.M{"_id": roomID, "encKey.priv": bson.M{"$exists": false}},
		bson.M{"$set": bson.M{"encKey.priv": pair.PrivateKey, "encKey.ver": 0}},
		options.FindOneAndUpdate().SetReturnDocument(options.After).SetProjection(encKeyProjection),
	).Decode(&doc)
	if err == nil {
		if doc.EncKey == nil {
			return nil, fmt.Errorf("set room key if absent: missing key after update")
		}
		vp, decErr := doc.EncKey.versioned()
		if decErr != nil {
			return nil, fmt.Errorf("set room key if absent: %w", decErr)
		}
		return vp, nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("set room key if absent: %w", err)
	}
	// No match: either a racer installed a key first, or the room does not exist.
	existing, getErr := s.Get(ctx, roomID)
	if getErr != nil {
		return nil, fmt.Errorf("set room key if absent: read current key: %w", getErr)
	}
	if existing == nil {
		return nil, fmt.Errorf("set room key if absent: %w", ErrRoomNotFound)
	}
	return existing, nil
}

// Get returns the room's current key, or (nil, nil) when the room or its key is absent.
func (s *mongoStore) Get(ctx context.Context, roomID string) (*VersionedKeyPair, error) {
	var doc roomKeyDoc
	err := s.col.FindOne(ctx, bson.M{"_id": roomID}, options.FindOne().SetProjection(encKeyProjection)).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get room key: %w", err)
	}
	if doc.EncKey == nil {
		return nil, nil
	}
	vp, err := doc.EncKey.versioned()
	if err != nil {
		return nil, fmt.Errorf("get room key: %w", err)
	}
	return vp, nil
}

// GetMany returns current key pairs for the rooms that have one; rooms without a
// document or without a key are omitted from the result map.
func (s *mongoStore) GetMany(ctx context.Context, roomIDs []string) (map[string]*VersionedKeyPair, error) {
	out := make(map[string]*VersionedKeyPair, len(roomIDs))
	if len(roomIDs) == 0 {
		return out, nil
	}
	cur, err := s.col.Find(ctx, bson.M{"_id": bson.M{"$in": roomIDs}}, options.Find().SetProjection(encKeyProjection))
	if err != nil {
		return nil, fmt.Errorf("get many room keys: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()
	for cur.Next(ctx) {
		var doc roomKeyDoc
		if err := cur.Decode(&doc); err != nil {
			return nil, fmt.Errorf("get many room keys: decode: %w", err)
		}
		if doc.EncKey == nil {
			continue
		}
		vp, err := doc.EncKey.versioned()
		if err != nil {
			return nil, fmt.Errorf("get many room keys: room %s: %w", doc.ID, err)
		}
		out[doc.ID] = vp
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("get many room keys: cursor: %w", err)
	}
	return out, nil
}

// GetByVersion returns the key for version from the current slot, the previous
// slot while its grace window is open, or the archive; (nil, nil) if none match.
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
		// A later rotation may have evicted this version while a cached copy still stamped it.
		return s.retiredByVersion(ctx, roomID, version)
	}
	return pair, nil
}

// retiredByVersion resolves a version from the archive; (nil, nil) if unconfigured or absent.
// expiresAt is no read gate, only a cue for the lazy reaper — serving an uncollected doc is fine.
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

// Rotate atomically demotes the current key into the previous slot (stamped with
// a grace-period expiry), increments the version, and installs newPair as the
// current key. Returns the new version, or ErrNoCurrentKey if the room has no
// current key. The whole transition runs as one aggregation-pipeline update so
// no concurrent reader observes a partially-rotated key.
func (s *mongoStore) Rotate(ctx context.Context, roomID string, newPair RoomKeyPair) (int, error) {
	expireAt := s.now().UTC().Add(s.gracePeriod)
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$set", Value: bson.M{
			"encKey.prevPriv":      "$encKey.priv",
			"encKey.prevVer":       "$encKey.ver",
			"encKey.prevExpiresAt": expireAt,
			"encKey.priv":          newPair.PrivateKey,
			"encKey.ver":           bson.M{"$add": bson.A{"$encKey.ver", 1}},
		}}},
	}
	// The After projection returns the slot THIS call demoted — precise when
	// rotations race, which a separate pre-read would not be.
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetProjection(bson.M{"encKey.ver": 1, "encKey.prevPriv": 1, "encKey.prevVer": 1})
	var updated roomKeyDoc
	err := s.col.FindOneAndUpdate(ctx,
		bson.M{"_id": roomID, "encKey.priv": bson.M{"$exists": true}},
		pipeline, opts,
	).Decode(&updated)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, ErrNoCurrentKey
	}
	if err != nil {
		return 0, fmt.Errorf("rotate room key: %w", err)
	}
	if updated.EncKey == nil {
		return 0, fmt.Errorf("rotate room key: missing key after update")
	}
	s.archiveRetired(ctx, roomID, updated.EncKey.PrevVer, updated.EncKey.PrevPriv)
	return updated.EncKey.Ver, nil
}

// Delete removes the entire encKey sub-document. No-op when the room or key is absent.
func (s *mongoStore) Delete(ctx context.Context, roomID string) error {
	if _, err := s.col.UpdateByID(ctx, roomID, bson.M{"$unset": bson.M{"encKey": ""}}); err != nil {
		return fmt.Errorf("delete room key: %w", err)
	}
	return nil
}

// Close is a no-op: the mongo client is owned and closed by the caller.
func (s *mongoStore) Close() error { return nil }

// EnsureIndexes creates the archive's TTL index. Idempotent; no-op when the archive is off.
func (s *mongoStore) EnsureIndexes(ctx context.Context) error {
	if s.retiredCol == nil {
		s.indexReady.Store(true)
		return nil
	}
	// Per-document expiresAt keeps retention tunable by config, no index rebuild.
	if _, err := s.retiredCol.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expiresAt", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(0),
	}); err != nil {
		return fmt.Errorf("ensure retired_room_keys expiresAt TTL index: %w", err)
	}
	s.indexReady.Store(true)
	return nil
}

// retiredKeyDoc is one retired key version in the retired_room_keys collection.
type retiredKeyDoc struct {
	ID        string    `bson:"_id"`
	Priv      []byte    `bson:"priv"`
	ExpiresAt time.Time `bson:"expiresAt"`
}

// retiredKeyID is the deterministic _id for a retired version — a point-read lookup.
func retiredKeyID(roomID string, version int) string {
	return fmt.Sprintf("%s:%d", roomID, version)
}

func (s *mongoStore) retiredDoc(priv []byte) bson.M {
	return bson.M{"priv": priv, "expiresAt": s.now().UTC().Add(s.retiredTTL)}
}

// archiveRetired copies a just-demoted key into the archive. Best-effort: the
// rotation is committed and must not be re-run, so failures are logged, not returned.
func (s *mongoStore) archiveRetired(ctx context.Context, roomID string, version int, priv []byte) {
	if s.retiredCol == nil || len(priv) == 0 {
		return
	}
	s.repairIndexes(ctx)
	// Detached: the rotation is committed, so a caller deadline expiring here must not drop the archive.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), archiveWriteTimeout)
	defer cancel()
	_, err := s.retiredCol.UpdateByID(writeCtx, retiredKeyID(roomID, version),
		// $set, not $setOnInsert: a version number is reused after Delete, and
		// $setOnInsert would keep the stale incarnation's bytes instead of overwriting.
		bson.M{"$set": s.retiredDoc(priv)},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		roomkeymetrics.StoreErrors.Add(ctx, 1,
			metric.WithAttributes(attribute.String("op", "ArchiveRetired")))
		slog.ErrorContext(ctx, "archive retired room key failed",
			"room_id", roomID, "version", version, "error", err)
	}
}

// repairIndexes re-attempts the TTL index when startup could not create it, so
// the archive is never written into an unexpiring collection. Idempotent, and
// it stops attempting once the ensure succeeds.
func (s *mongoStore) repairIndexes(ctx context.Context) {
	if s.indexReady.Load() {
		return
	}
	idxCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), indexRepairTimeout)
	defer cancel()
	if err := s.EnsureIndexes(idxCtx); err != nil {
		// Error, not Warn, and metered: a repair that keeps failing is an
		// operator-actionable condition (an index changed out from under us),
		// not a transient blip. Alert on this counter rather than on the absence
		// of expiry, which is invisible until the collection has already grown.
		roomkeymetrics.StoreErrors.Add(ctx, 1,
			metric.WithAttributes(attribute.String("op", "RepairIndexes")))
		slog.ErrorContext(ctx, "retired room key TTL index unavailable; archiving without expiry",
			"error", err)
	}
}

// keyDoc is the BSON shape of the encryption-key sub-document embedded in a
// room document under the "encKey" field. The current key (priv/ver) is always
// present once provisioned; the previous-key slot (prevPriv/prevVer/
// prevExpiresAt) is populated by Rotate and ignored once prevExpiresAt elapses.
type keyDoc struct {
	Priv          []byte     `bson:"priv"`
	Ver           int        `bson:"ver"`
	PrevPriv      []byte     `bson:"prevPriv,omitempty"`
	PrevVer       int        `bson:"prevVer,omitempty"`
	PrevExpiresAt *time.Time `bson:"prevExpiresAt,omitempty"`
}

// roomKeyDoc projects just the _id and encKey fields of a room document.
type roomKeyDoc struct {
	ID     string  `bson:"_id"`
	EncKey *keyDoc `bson:"encKey"`
}

// versioned converts the current key slot into a VersionedKeyPair, validating
// the secret length.
func (d *keyDoc) versioned() (*VersionedKeyPair, error) {
	if err := validateSecret(d.Priv); err != nil {
		return nil, fmt.Errorf("decode current key: %w", err)
	}
	return &VersionedKeyPair{Version: d.Ver, KeyPair: RoomKeyPair{PrivateKey: d.Priv}}, nil
}

// pairForVersion returns the key pair matching version from either the current
// slot or the previous slot (only while the previous slot's grace window is
// still open at now). Returns (nil, nil) when neither slot matches.
func (d *keyDoc) pairForVersion(version int, now time.Time) (*RoomKeyPair, error) {
	// Match the Valkey store: a slot whose version matches but whose secret is
	// corrupt surfaces an error rather than masquerading as a miss. The previous
	// slot only counts while its grace window is open; the grace check also
	// guards against a never-rotated room's zero-value PrevVer matching version 0.
	if d.Ver == version {
		if err := validateSecret(d.Priv); err != nil {
			return nil, fmt.Errorf("decode current key: %w", err)
		}
		return &RoomKeyPair{PrivateKey: d.Priv}, nil
	}
	if d.PrevExpiresAt != nil && now.Before(*d.PrevExpiresAt) && d.PrevVer == version {
		if err := validateSecret(d.PrevPriv); err != nil {
			return nil, fmt.Errorf("decode previous key: %w", err)
		}
		return &RoomKeyPair{PrivateKey: d.PrevPriv}, nil
	}
	return nil, nil
}

// validateSecret ensures a stored secret is the expected 32-byte length.
func validateSecret(priv []byte) error {
	if len(priv) != 32 {
		return fmt.Errorf("invalid key length %d", len(priv))
	}
	return nil
}
