package main

import (
	"context"
	"errors"
	"io"
)

// ErrObjectNotFound is returned (wrapped) by Open when no object matches the key.
var ErrObjectNotFound = errors.New("object not found")

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// blobInfo is the object metadata the download path needs for response headers
// and the cache size decision.
type blobInfo struct {
	Size        int64
	ContentType string
}

// versionStore is the subset of object storage the update handlers need.
type versionStore interface {
	// Put streams r (of known size) to the object at key with the given content type.
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	// Open returns a streaming reader plus metadata (from the object's own Stat),
	// or ErrObjectNotFound (wrapped) when the object is absent.
	Open(ctx context.Context, key string) (io.ReadCloser, blobInfo, error)
}
