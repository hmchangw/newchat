package main

import (
	"context"
	"errors"
)

// ErrRoomNotFound is returned by GetRoomSiteID when no room matches the given ID.
var ErrRoomNotFound = errors.New("room not found")

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// Store is the subset of persistence the upload handlers need.
type Store interface {
	// IsMember reports whether account has a subscription to roomID.
	IsMember(ctx context.Context, roomID, account string) (bool, error)
	// GetRoomSiteID returns the room's siteID, or ErrRoomNotFound (wrapped) when absent.
	GetRoomSiteID(ctx context.Context, roomID string) (string, error)
}

// errIsRoomNotFound reports whether err wraps ErrRoomNotFound.
func errIsRoomNotFound(err error) bool { return errors.Is(err, ErrRoomNotFound) }
