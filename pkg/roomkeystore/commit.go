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
	SetIfAbsent(ctx context.Context, roomID string, pair RoomKeyPair) (*VersionedKeyPair, error)
}

// CommitRotation persists newPair and returns the pair the store actually holds,
// which is what callers must fan out. Both legs are atomic post-images, so racing
// commits on a keyless room converge on one v0 key instead of fanning out rival bytes.
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
	// SetIfAbsent's post-image is the winner's key even when this caller lost the
	// race, so every racer fans out the one key the room actually holds at v0.
	slog.WarnContext(ctx, "no current room key; adopting a fresh key at v0", "roomID", roomID)
	committed, err := store.SetIfAbsent(ctx, roomID, *newPair)
	if err != nil {
		roomkeymetrics.StoreErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "SetIfAbsent")))
		return nil, fmt.Errorf("store room key: %w", err)
	}
	return committed, nil
}
