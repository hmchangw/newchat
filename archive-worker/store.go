package main

import (
	"context"
	"encoding/json"
	"io"

	"github.com/hmchangw/chat/pkg/searchengine"
)

// indexStore is the slice of the archive search engine the worker uses.
type indexStore interface {
	Bulk(ctx context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error)
	GetDoc(ctx context.Context, index, docID string) (json.RawMessage, bool, error)
	UpsertTemplate(ctx context.Context, name string, body json.RawMessage) error
	EnsureLifecyclePolicy(ctx context.Context, name string, body json.RawMessage) (bool, error)
}

// objectStore is the slice of the archive bucket the worker uses.
type objectStore interface {
	Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) error
	// Stat reports whether key exists; a missing key is (false, nil).
	Stat(ctx context.Context, key string) (bool, error)
	// Get opens key for reading; the caller closes it.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main
