package migration

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

// SourceLookup fetches the current full source doc by _id, as relaxed extended JSON
// (the same shape the connector emits). Used on the update path, where the connector
// forwards only the change delta and the full current doc must be re-read.
type SourceLookup interface {
	// FindByID returns the raw relaxed-extJSON document, or (nil, nil) if absent.
	FindByID(ctx context.Context, id string) ([]byte, error)
}

// MongoSourceLookup is a SourceLookup backed by a source Mongo collection.
type MongoSourceLookup struct {
	coll *mongo.Collection
}

// NewMongoSourceLookup returns a MongoSourceLookup over the given source collection.
func NewMongoSourceLookup(coll *mongo.Collection) *MongoSourceLookup {
	return &MongoSourceLookup{coll: coll}
}

// FindByID reads the doc and re-encodes it as relaxed extended JSON, matching the shape
// the connector emits. A missing document returns (nil, nil), not an error.
func (m *MongoSourceLookup) FindByID(ctx context.Context, id string) (out []byte, err error) {
	ctx, span := otel.Tracer("migration").Start(ctx, "source.findByID")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()
	var raw bson.Raw
	if derr := m.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&raw); errors.Is(derr, mongo.ErrNoDocuments) {
		return nil, nil // a miss is not a span error — the named err stays nil
	} else if derr != nil {
		err = derr
	}
	if err != nil {
		return nil, fmt.Errorf("source find %q: %w", id, err)
	}
	out, err = bson.MarshalExtJSON(raw, false, false)
	if err != nil {
		return nil, fmt.Errorf("encode source doc %q: %w", id, err)
	}
	return out, nil
}
