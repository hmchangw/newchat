package main

import (
	"context"
	"errors"
)

// ErrRoomNotFound is returned by GetRoomSiteID when no room matches the given ID.
var ErrRoomNotFound = errors.New("room not found")

// ErrUploadNotFound is returned by GetUpload when no upload matches the given ID.
var ErrUploadNotFound = errors.New("upload not found")

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// upload is the subset of an `uploads` document the download handler needs.
// Read DTO (bson tags only) — never serialized to clients.
type upload struct {
	ID       string `bson:"_id"`
	UserID   string `bson:"userId"`
	RID      string `bson:"rid"`
	Name     string `bson:"name"`
	Type     string `bson:"type"`
	Size     int64  `bson:"size"`
	AmazonS3 struct {
		Path string `bson:"path"`
	} `bson:"AmazonS3"`
}

// Store is the subset of persistence the upload handlers need.
type Store interface {
	// IsMember reports whether account has a subscription to roomID.
	IsMember(ctx context.Context, roomID, account string) (bool, error)
	// GetRoomSiteID returns the room's siteID, or ErrRoomNotFound (wrapped) when absent.
	GetRoomSiteID(ctx context.Context, roomID string) (string, error)
	// GetUpload returns the upload metadata for fileID, or ErrUploadNotFound (wrapped) when absent.
	GetUpload(ctx context.Context, fileID string) (*upload, error)
}

// errIsRoomNotFound reports whether err wraps ErrRoomNotFound.
func errIsRoomNotFound(err error) bool { return errors.Is(err, ErrRoomNotFound) }

// errIsUploadNotFound reports whether err wraps ErrUploadNotFound.
func errIsUploadNotFound(err error) bool { return errors.Is(err, ErrUploadNotFound) }
