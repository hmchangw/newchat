package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mongoTargetStore writes verbatim docs into arbitrary collections of the target DB, keyed by _id.
type mongoTargetStore struct {
	db *mongo.Database
}

var _ targetStore = (*mongoTargetStore)(nil)

// NewMongoTargetStore binds the target database; collections are resolved per-call by name.
func NewMongoTargetStore(db *mongo.Database) *mongoTargetStore {
	return &mongoTargetStore{db: db}
}

// UpsertByID replaces (or inserts) the doc keyed by _id. Idempotent under redelivery.
func (s *mongoTargetStore) UpsertByID(ctx context.Context, collection string, id any, doc bson.D) error {
	_, err := s.db.Collection(collection).ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}}, doc, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("upsert into %s: %w", collection, err)
	}
	return nil
}

// DeleteByID removes the doc keyed by _id. A missing row deletes nothing and is not an error.
func (s *mongoTargetStore) DeleteByID(ctx context.Context, collection string, id any) error {
	if _, err := s.db.Collection(collection).DeleteOne(ctx, bson.D{{Key: "_id", Value: id}}); err != nil {
		return fmt.Errorf("delete from %s: %w", collection, err)
	}
	return nil
}
