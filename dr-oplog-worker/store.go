package main

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// targetStore is the verbatim per-collection write surface on the backup Mongo, keyed by native
// _id. Defined in the consumer (per CLAUDE.md) and hand-faked in tests, so no mockgen is needed.
type targetStore interface {
	UpsertByID(ctx context.Context, collection string, id any, doc bson.D) error
	DeleteByID(ctx context.Context, collection string, id any) error
}
