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

// PreviewDEKCollection holds the wrapped per-site preview DEKs. Separate from
// atrest.CollectionName though both share the KEK — the split is operational:
// losing a preview DEK is a cache miss, losing a room DEK is permanent history
// loss, and co-locating them invites identical backup treatment.
const PreviewDEKCollection = "preview_deks"

// RoomRepo reads room metadata and owns the stored preview — the only place the
// preview cipher is applied, so callers above it never hold a key.
type RoomRepo struct {
	rooms  *mongoutil.Collection[model.Room]
	cipher atrest.Cipher // nil when ATREST_ENABLED=false — previews are then not stored
	key    preview.Key
}

// NewRoomRepo builds the repo. A nil cipher (ATREST_ENABLED=false) disables
// preview storage rather than falling back to plaintext.
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

// RoomTimes is what GetRoomTimesByIDs projects per room: the bucket-walk
// timestamps, plus the memoized preview when one is stored and current.
type RoomTimes struct {
	LastMsgAt time.Time
	CreatedAt time.Time
	// Preview is the stored preview, already opened. Non-nil only when present,
	// current (previewForMsgId == lastMsgId), on the configured epoch, and
	// decryptable; anything else is nil and the caller walks Cassandra.
	Preview *model.PreviewMessage
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
	// lastMsgId rides along because freshness is an identity check against it
	// (see pkg/preview and the design's §3).
	rooms, err := r.rooms.FindMany(ctx,
		bson.M{"_id": bson.M{"$in": ids}},
		mongoutil.WithProjection(bson.M{
			"_id": 1, "lastMsgAt": 1, "createdAt": 1,
			"lastMsgId":         1,
			"previewMeta":       1,
			"previewCiphertext": 1,
			"previewNonce":      1,
			"previewKeyEpoch":   1,
			"previewForMsgId":   1,
		}),
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

// openStoredPreview returns the room's memoized preview, or nil to make the
// caller walk. Every rejection is a cache miss, never an error — a preview is
// derived data, so failing to open one costs a walk and nothing else.
func (r *RoomRepo) openStoredPreview(ctx context.Context, room *model.Room) *model.PreviewMessage {
	if r.cipher == nil || room.PreviewMeta == nil || len(room.PreviewCiphertext) == 0 {
		return nil
	}
	// Identity, not timestamps: current iff the newest message the walk observed
	// is still the room's last message.
	if room.PreviewForMsgID == "" || room.PreviewForMsgID != room.LastMsgID {
		return nil
	}
	if room.PreviewKeyEpoch != r.key.Epoch {
		// Expected during rotation (both epochs live across a rolling deploy), so
		// Info — distinct from the same-epoch failure below.
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
		// NOT expected: tampering, a truncated write, or a rotated-in-place DEK.
		slog.WarnContext(ctx, "stored preview failed to decrypt on the configured epoch, re-resolving",
			"room_id", room.ID, "epoch", room.PreviewKeyEpoch, "error", err)
		return nil
	}
	return &pvw
}

// SetPreviewMessage seals pvw and stores it, watermark-guarded by asOf (see
// pkg/preview.GuardedSetFields). A no-op when the cipher is disabled — the room
// doc must never hold a plaintext body.
//
// forMsgID MUST be the newest message id the walk observed, never the lastMsgId
// read out of Mongo; see previewWalk.NewestObservedID for why.
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

// ClearPreview removes every stored preview field under the same guard,
// advancing previewAsOf so an older redelivery cannot resurrect it. For a
// mutation that leaves the room with no eligible message.
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
