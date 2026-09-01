package main

import (
	"context"
	"encoding/json"

	"github.com/hmchangw/chat/pkg/searchengine"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store

// Store defines the search engine operations needed by the handler.
type Store interface {
	Bulk(ctx context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error)
	// UpdateByQuery applies a by-query mutation for events that touch many docs
	// keyed by a field rather than by DocID (e.g. room rename → every member's
	// spotlight doc). Standalone ES call, outside the bulk buffer.
	UpdateByQuery(ctx context.Context, index string, body json.RawMessage) error
}
