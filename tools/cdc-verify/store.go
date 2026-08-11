package main

import (
	"context"
	"errors"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// Sentinel lookup outcomes the verifier branches on with errors.Is.
var (
	errNotFound  = errors.New("record not found")
	errAmbiguous = errors.New("key matched more than one record")
)

// SourceStore reads current documents from the legacy source Mongo.
type SourceStore interface {
	// FindByID returns the full current doc, unprojected — the field set is
	// mapping-driven and small docs dominate. errNotFound when missing.
	FindByID(ctx context.Context, collection, id string) (map[string]any, error)
	// FindOne locates a doc by field equality (resolver lookups), projected to fields.
	FindOne(ctx context.Context, collection string, key map[string]any, fields []string) (map[string]any, error)
}

// TargetStore reads destination records (target Mongo).
type TargetStore interface {
	// FindOne as above, against the target DB.
	FindOne(ctx context.Context, collection string, key map[string]any, fields []string) (map[string]any, error)
}

// CassStore reads destination rows (Cassandra).
type CassStore interface {
	// SelectOne runs SELECT cols FROM table WHERE key, values keyed by column name.
	SelectOne(ctx context.Context, table string, key map[string]any, cols []string) (map[string]any, error)
}
