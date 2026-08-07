package mongorepo

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/preview"
)

const roomsCollection = "rooms"

// PreviewDEKCollection holds the wrapped per-site preview DEKs. Deliberately
// separate from atrest.CollectionName, though both are wrapped by the same KEK:
// the split is operational, not cryptographic. Losing a preview DEK is a cache
// miss; losing a room DEK is permanent history loss, and co-locating them
// invites identical backup and retention treatment for records that deserve
// different handling.
const PreviewDEKCollection = "preview_deks"

// RoomRepo reads room metadata from MongoDB and owns the stored room preview:
// it is the only place the preview cipher is applied, so callers above it deal
// in plaintext model.PreviewMessage and never hold a key.
type RoomRepo struct {
	rooms  *mongoutil.Collection[model.Room]
	cipher atrest.Cipher // nil when ATREST_ENABLED=false — previews are then not stored
	key    preview.Key
}

// NewRoomRepo builds the repo. cipher may be nil (ATREST_ENABLED=false), in
// which case preview storage is disabled entirely rather than falling back to
// plaintext: the room doc must never hold message bodies in the clear.
func NewRoomRepo(db *mongo.Database, cipher atrest.Cipher, key preview.Key) *RoomRepo {
	return &RoomRepo{
		rooms:  mongoutil.NewCollection[model.Room](db.Collection(roomsCollection)),
		cipher: cipher,
		key:    key,
	}
}

// GetMinUserLastSeenAt returns (nil, nil) when the room is missing OR the
// field is unset — both mean "no read floor".
func (r *RoomRepo) GetMinUserLastSeenAt(ctx context.Context, roomID string) (*time.Time, error) {
	room, err := r.rooms.FindOne(ctx,
		bson.M{"_id": roomID},
		mongoutil.WithProjection(bson.M{"minUserLastSeenAt": 1, "_id": 0}),
	)
	if err != nil {
		return nil, fmt.Errorf("get room %s minUserLastSeenAt: %w", roomID, err)
	}
	if room == nil {
		return nil, nil
	}
	return room.MinUserLastSeenAt, nil
}

// GetRoomTimes returns lastMsgAt (zero time when unset) and createdAt for the given room.
// Returns mongo.ErrNoDocuments wrapped when the room does not exist.
func (r *RoomRepo) GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error) {
	room, err := r.rooms.FindByID(ctx, roomID, mongoutil.WithProjection(bson.M{"lastMsgAt": 1, "createdAt": 1, "_id": 0}))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("get room times for %s: %w", roomID, err)
	}
	if room == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("get room times for %s: %w", roomID, mongo.ErrNoDocuments)
	}
	if room.LastMsgAt != nil {
		lastMsgAt = *room.LastMsgAt
	}
	return lastMsgAt, room.CreatedAt, nil
}

// RoomTimes holds what GetRoomTimesByIDs projects for one room: the two
// bucket-walk timestamps plus, when one is stored and current, the memoized
// preview.
type RoomTimes struct {
	LastMsgAt time.Time
	CreatedAt time.Time
	// Preview is the stored preview, already opened — non-nil only when it was
	// present, current (previewForMsgId == lastMsgId), sealed under the
	// configured key epoch, and decrypted successfully. Anything else leaves it
	// nil so the caller walks Cassandra instead.
	Preview *model.PreviewMessage
}

// previewProjection is the stored-preview half of the GetRoomTimesByIDs
// projection. lastMsgId rides along because freshness is an identity check
// against it (see pkg/preview and the design's §3).
var previewProjection = bson.M{
	"lastMsgId":         1,
	"previewMeta":       1,
	"previewCiphertext": 1,
	"previewNonce":      1,
	"previewKeyEpoch":   1,
	"previewForMsgId":   1,
}

// GetRoomTimesByIDs batches GetRoomTimes across ids into a single $in query,
// returning a map keyed by room ID. Rooms absent from Mongo are simply absent
// from the map (not an error). Empty ids returns an empty, non-nil map with no query.
//
// The stored preview rides the same read, so serving a memoized preview costs
// no extra round-trip beyond the room-times read the caller already needs.
func (r *RoomRepo) GetRoomTimesByIDs(ctx context.Context, ids []string) (map[string]RoomTimes, error) {
	out := make(map[string]RoomTimes, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	projection := bson.M{"_id": 1, "lastMsgAt": 1, "createdAt": 1}
	for k, v := range previewProjection {
		projection[k] = v
	}
	rooms, err := r.rooms.FindMany(ctx,
		bson.M{"_id": bson.M{"$in": ids}},
		mongoutil.WithProjection(projection),
	)
	if err != nil {
		return nil, fmt.Errorf("get room times for %d rooms: %w", len(ids), err)
	}
	for i := range rooms {
		room := &rooms[i]
		var lastMsgAt time.Time
		if room.LastMsgAt != nil {
			lastMsgAt = *room.LastMsgAt
		}
		out[room.ID] = RoomTimes{
			LastMsgAt: lastMsgAt,
			CreatedAt: room.CreatedAt,
			Preview:   r.openStoredPreview(ctx, room),
		}
	}
	return out, nil
}

// openStoredPreview returns the room's memoized preview, or nil when the caller
// must walk instead. Every rejection is a cache miss, never an error: a preview
// is derived data, so failing to open one costs a walk and nothing else.
func (r *RoomRepo) openStoredPreview(ctx context.Context, room *model.Room) *model.PreviewMessage {
	if r.cipher == nil || room.PreviewMeta == nil || len(room.PreviewCiphertext) == 0 {
		return nil
	}
	// Identity, not timestamps: a stored preview is current iff the newest
	// message its walk observed is still the room's last message.
	if room.PreviewForMsgID == "" || room.PreviewForMsgID != room.LastMsgID {
		return nil
	}
	if room.PreviewKeyEpoch != r.key.Epoch {
		// Expected during rotation — both epochs are live across a rolling
		// deploy — so this is Info, distinct from a same-epoch failure below.
		slog.InfoContext(ctx, "stored preview on a retired key epoch, re-resolving",
			"room_id", room.ID, "stored_epoch", room.PreviewKeyEpoch, "configured_epoch", r.key.Epoch)
		return nil
	}

	pvw, err := preview.Open(ctx, r.cipher, r.key.SiteID, preview.Sealed{
		Meta:       *room.PreviewMeta,
		Ciphertext: room.PreviewCiphertext,
		Nonce:      room.PreviewNonce,
		KeyEpoch:   room.PreviewKeyEpoch,
		ForMsgID:   room.PreviewForMsgID,
	})
	if err != nil {
		// Same-epoch decrypt failure is NOT expected: it means tampering, a
		// truncated write, or a rotated-in-place DEK. Warn so it is visible.
		slog.WarnContext(ctx, "stored preview failed to decrypt on the configured epoch, re-resolving",
			"room_id", room.ID, "epoch", room.PreviewKeyEpoch, "error", err)
		return nil
	}
	return &pvw
}

// SetPreviewMessage seals pvw and stores it, watermark-guarded (see
// pkg/preview.GuardedSetFields).
//
// forMsgID is the freshness key and MUST be the newest message id the resolving
// walk observed in Cassandra — never the lastMsgId read out of Mongo.
// broadcast-worker and message-worker are unordered consumers of
// MESSAGES-CANONICAL, so the room doc can name a message Cassandra does not
// hold yet; stamping that id would claim freshness for a state never observed,
// and because the identity check would then hold, the error would not self-heal
// until the next message.
//
// asOf orders concurrent writers. A no-op when the cipher is disabled: the room
// doc must never hold a plaintext body.
//
//nolint:gocritic // hugeParam: pvw's by-value shape is the RoomRepository.SetPreviewMessage contract shared with the mock and the readcache passthrough; the copy cost is negligible on this best-effort write path.
func (r *RoomRepo) SetPreviewMessage(ctx context.Context, roomID string, pvw model.PreviewMessage, forMsgID string, asOf int64) error {
	if r.cipher == nil {
		return nil
	}
	sealed, err := preview.Seal(ctx, r.cipher, r.key, forMsgID, pvw)
	if err != nil {
		return fmt.Errorf("seal room preview %s: %w", roomID, err)
	}
	pipeline := mongo.Pipeline{{{Key: "$set", Value: preview.GuardedSetFields(sealed, asOf)}}}
	if _, err := r.rooms.Raw().UpdateOne(ctx, bson.M{"_id": roomID}, pipeline); err != nil {
		return fmt.Errorf("store room preview %s: %w", roomID, err)
	}
	return nil
}

// ClearPreview removes every stored preview field under the same watermark
// guard, advancing previewAsOf so a redelivered older write cannot resurrect
// what was cleared. Used when a mutation leaves the room with no eligible
// message at all.
func (r *RoomRepo) ClearPreview(ctx context.Context, roomID string, asOf int64) error {
	if r.cipher == nil {
		return nil
	}
	pipeline := mongo.Pipeline{{{Key: "$set", Value: preview.GuardedClearFields(asOf)}}}
	if _, err := r.rooms.Raw().UpdateOne(ctx, bson.M{"_id": roomID}, pipeline); err != nil {
		return fmt.Errorf("clear room preview %s: %w", roomID, err)
	}
	return nil
}

// GetRoomUserCount returns the room's userCount via a projected findOne.
// Returns mongo.ErrNoDocuments wrapped when the room does not exist —
// callers treat that as an infrastructure error (reaching this call already
// implies the caller is subscribed to the room).
func (r *RoomRepo) GetRoomUserCount(ctx context.Context, roomID string) (int, error) {
	room, err := r.rooms.FindByID(ctx, roomID, mongoutil.WithProjection(bson.M{"userCount": 1, "_id": 0}))
	if err != nil {
		return 0, fmt.Errorf("get room %s userCount: %w", roomID, err)
	}
	if room == nil {
		return 0, fmt.Errorf("get room %s userCount: %w", roomID, mongo.ErrNoDocuments)
	}
	return room.UserCount, nil
}
