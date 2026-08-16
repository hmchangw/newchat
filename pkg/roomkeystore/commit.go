package roomkeystore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hmchangw/chat/pkg/roomkeymetrics"
)

// Committer is the subset of RoomKeyStore CommitRotation needs; both stores satisfy it structurally.
type Committer interface {
	Rotate(ctx context.Context, roomID string, newPair RoomKeyPair) (int, error)
	Set(ctx context.Context, roomID string, pair RoomKeyPair) (int, error)
	Get(ctx context.Context, roomID string) (*VersionedKeyPair, error)
}

// CommitRotation persists newPair and returns the pair the store actually holds,
// which is what callers must fan out. A keyless room adopts version 0 via Set.
func CommitRotation(ctx context.Context, store Committer, roomID string, currentPair *VersionedKeyPair, newPair *RoomKeyPair) (*VersionedKeyPair, error) {
	if currentPair != nil {
		version, err := store.Rotate(ctx, roomID, *newPair)
		if err == nil {
			return &VersionedKeyPair{Version: version, KeyPair: *newPair}, nil
		}
		if !errors.Is(err, ErrNoCurrentKey) {
			roomkeymetrics.StoreErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Rotate")))
			return nil, fmt.Errorf("rotate room key: %w", err)
		}
	}
	// Set is last-write-wins at v0, so only the read-back is authoritative.
	slog.WarnContext(ctx, "no current room key; adopting a fresh key at v0", "roomID", roomID)
	if _, err := store.Set(ctx, roomID, *newPair); err != nil {
		roomkeymetrics.StoreErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Set")))
		return nil, fmt.Errorf("store room key: %w", err)
	}
	committed, err := store.Get(ctx, roomID)
	if err != nil {
		roomkeymetrics.StoreErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Get")))
		return nil, fmt.Errorf("read back stored room key: %w", err)
	}
	// (nil, nil) is key-absent, not an I/O failure, so it is no store error.
	if committed == nil {
		return nil, fmt.Errorf("read back stored room key: %w", ErrNoCurrentKey)
	}
	return committed, nil
}
