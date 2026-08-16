package roomkeystore

import (
	"context"
	"errors"
)

// ErrNoCurrentKey is returned by Rotate when no current key exists for the room.
var ErrNoCurrentKey = errors.New("no current key")

// ErrRoomNotFound is returned by Set when no room document exists: a key is a
// field of its room, so the room must exist first.
var ErrRoomNotFound = errors.New("room not found")

// RoomKeyPair holds the 32-byte room secret used directly as the AES-256-GCM key by roomcrypto.
type RoomKeyPair struct {
	PrivateKey []byte // 32-byte secret; used directly as AES-256-GCM key material
}

// VersionedKeyPair pairs a key pair with its store-assigned version number.
type VersionedKeyPair struct {
	Version int
	KeyPair RoomKeyPair
}

// RoomKeyStore defines storage operations for room encryption secrets.
type RoomKeyStore interface {
	Set(ctx context.Context, roomID string, pair RoomKeyPair) (int, error)
	// SetWithVersion stamps pair at an explicit version for the rotate fallback.
	SetWithVersion(ctx context.Context, roomID string, pair RoomKeyPair, version int) error
	Get(ctx context.Context, roomID string) (*VersionedKeyPair, error)
	GetMany(ctx context.Context, roomIDs []string) (map[string]*VersionedKeyPair, error)
	GetByVersion(ctx context.Context, roomID string, version int) (*RoomKeyPair, error)
	Rotate(ctx context.Context, roomID string, newPair RoomKeyPair) (int, error)
	Delete(ctx context.Context, roomID string) error
	// EnsureIndexes creates the indexes this store owns. Idempotent.
	EnsureIndexes(ctx context.Context) error
	Close() error
}
