package main

import (
	"context"
	"errors"
)

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
	// MemberSiteID resolves account's subscription to roomID, returning the
	// room's home siteID as stamped on the subscription (member=false, siteID
	// empty when no subscription exists). The subscription — not the rooms
	// collection — is the source, because a cross-site room has no local rooms
	// doc: federation mirrors only subscriptions to a member's site.
	MemberSiteID(ctx context.Context, roomID, account string) (siteID string, member bool, err error)
	// GetUpload returns the upload metadata for fileID, or ErrUploadNotFound (wrapped) when absent.
	GetUpload(ctx context.Context, fileID string) (*upload, error)
}

// errIsUploadNotFound reports whether err wraps ErrUploadNotFound.
func errIsUploadNotFound(err error) bool { return errors.Is(err, ErrUploadNotFound) }
