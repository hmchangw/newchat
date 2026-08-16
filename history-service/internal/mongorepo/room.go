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

// RoomRepo reads room metadata plus the stored preview, and writes one on a mutation or
// warm-back. It never coordinates with broadcast-worker — the previewAsOf watermark does.
type RoomRepo struct {
	rooms  *mongoutil.Collection[model.Room]
	cipher atrest.Cipher // nil when ATREST_ENABLED=false — no preview can then be opened
	key    preview.Key
}

// NewRoomRepo builds the repo; a nil cipher disables preview reads.
func NewRoomRepo(db *mongo.Database, cipher atrest.Cipher, key preview.Key) *RoomRepo {
	return &RoomRepo{
		rooms:  mongoutil.NewCollection[model.Room](db.Collection(roomsCollection)),
		cipher: cipher,
		key:    key,
	}
}

// GetMinUserLastSeenAt returns (nil, nil) for a missing room or an unset field: no floor.
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

// GetRoomTimes returns lastMsgAt (zero when unset) and createdAt, wrapping ErrNoDocuments.
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

// RoomTimes is what GetRoomTimesByIDs projects per room.
type RoomTimes struct {
	LastMsgAt time.Time
	CreatedAt time.Time
	// Preview is opened; non-nil only when current, same-epoch, and decryptable.
	Preview *model.PreviewMessage
}

// GetRoomTimesByIDs batches GetRoomTimes into one $in query; rooms absent from Mongo are
// absent from the map, not an error. The stored preview rides along for free.
func (r *RoomRepo) GetRoomTimesByIDs(ctx context.Context, ids []string) (map[string]RoomTimes, error) {
	out := make(map[string]RoomTimes, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	// lastMsgId rides along because freshness is an identity check against it.
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

// openStoredPreview returns the preview or nil; every rejection is a miss, not an error.
func (r *RoomRepo) openStoredPreview(ctx context.Context, room *model.Room) *model.PreviewMessage {
	if r.cipher == nil || room.PreviewMeta == nil || len(room.PreviewCiphertext) == 0 {
		return nil
	}
	// Identity, not timestamps: catches a write that landed half-applied or out of order.
	if room.PreviewForMsgID == "" || room.PreviewForMsgID != room.LastMsgID {
		return nil
	}
	if room.PreviewKeyEpoch != r.key.Epoch {
		// Info, not Warn: expected during rotation, both epochs live across a rolling deploy.
		slog.InfoContext(ctx, "stored preview on a retired key epoch, dropping",
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
		slog.WarnContext(ctx, "stored preview failed to decrypt on the configured epoch, dropping",
			"room_id", room.ID, "epoch", room.PreviewKeyEpoch, "error", err)
		return nil
	}
	return &pvw
}

// applyPreviewFields is the chokepoint for every preview write; the nil-cipher check is
// load-bearing, since preview.Seal would call Encrypt on a nil interface.
func (r *RoomRepo) applyPreviewFields(ctx context.Context, roomID, what string, fields bson.M) error {
	if r.cipher == nil {
		return nil
	}
	pipeline := mongo.Pipeline{{{Key: "$set", Value: fields}}}
	if _, err := r.rooms.Raw().UpdateOne(ctx, bson.M{"_id": roomID}, pipeline); err != nil {
		return fmt.Errorf("%s %s: %w", what, roomID, err)
	}
	return nil
}

// seal encrypts pvw under the site preview DEK, or reports that previews are off.
//
//nolint:gocritic // hugeParam: pvw's by-value shape is the RoomRepository contract shared with the mock and the readcache passthrough.
func (r *RoomRepo) seal(ctx context.Context, roomID, forMsgID string, pvw model.PreviewMessage) (preview.Sealed, bool, error) {
	if r.cipher == nil {
		return preview.Sealed{}, false, nil
	}
	sealed, err := preview.Seal(ctx, r.cipher, r.key, forMsgID, pvw)
	if err != nil {
		return preview.Sealed{}, false, fmt.Errorf("seal room preview %s: %w", roomID, err)
	}
	return sealed, true, nil
}

// SetPreviewMessage seals and stores pvw, guarded by asOf. forMsgID MUST be the id the
// walk observed, never the lastMsgId read from Mongo — see previewWalk.NewestObservedID.
//
//nolint:gocritic // hugeParam: pvw's by-value shape is the RoomRepository.SetPreviewMessage contract shared with the mock and the readcache passthrough; the copy cost is negligible on this best-effort write path.
func (r *RoomRepo) SetPreviewMessage(ctx context.Context, roomID string, pvw model.PreviewMessage, forMsgID string, asOf int64) error {
	sealed, ok, err := r.seal(ctx, roomID, forMsgID, pvw)
	if err != nil || !ok {
		return err
	}
	return r.applyPreviewFields(ctx, roomID, "store room preview", preview.GuardedSetFields(sealed, asOf))
}

// UpdatePreviewBody reseals the body for a mutation, leaving previewForMsgId alone. It
// refuses to create: a doc minted here would carry no key and never be invalidatable.
//
//nolint:gocritic // hugeParam: pvw's by-value shape matches SetPreviewMessage and the RoomRepository contract.
func (r *RoomRepo) UpdatePreviewBody(ctx context.Context, roomID string, pvw model.PreviewMessage, asOf int64) error {
	sealed, ok, err := r.seal(ctx, roomID, "", pvw)
	if err != nil || !ok {
		return err
	}
	return r.applyPreviewFields(ctx, roomID, "update room preview body", preview.GuardedUpdateBodyFields(sealed, asOf))
}

// ClearPreview removes every preview field, advancing previewAsOf against older replays.
func (r *RoomRepo) ClearPreview(ctx context.Context, roomID string, asOf int64) error {
	return r.applyPreviewFields(ctx, roomID, "clear room preview", preview.GuardedClearFields(asOf))
}

// GetRoomUserCount returns userCount; a missing room is wrapped ErrNoDocuments, an infra error.
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
