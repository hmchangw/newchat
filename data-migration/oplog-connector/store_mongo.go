package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// checkpointCollection holds one checkpoint doc per (site, collection), in CHECKPOINT_DB on the source RS.
const checkpointCollection = "oplog_checkpoints"

// mongoCheckpointStore persists checkpoints to MongoDB.
type mongoCheckpointStore struct {
	col    *mongo.Collection
	siteID string
}

// NewMongoCheckpointStore returns a CheckpointStore backed by col, with siteID scoping the checkpoint _id.
func NewMongoCheckpointStore(col *mongo.Collection, siteID string) *mongoCheckpointStore {
	return &mongoCheckpointStore{col: col, siteID: siteID}
}

func checkpointID(siteID, collection string) string {
	return siteID + ":" + collection
}

func (s *mongoCheckpointStore) Load(ctx context.Context, collection string) (*Checkpoint, error) {
	var cp Checkpoint
	err := s.col.FindOne(ctx, bson.M{"_id": checkpointID(s.siteID, collection)}).Decode(&cp)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load checkpoint %q: %w", collection, err)
	}
	return &cp, nil
}

func (s *mongoCheckpointStore) Save(ctx context.Context, cp *Checkpoint) error {
	// Copy so we don't mutate the caller's struct. Key by the store-scoped siteID so
	// Save and Load always target the same _id even if cp.SiteID is empty or differs.
	doc := *cp
	doc.SiteID = s.siteID
	doc.ID = checkpointID(s.siteID, doc.Collection)
	if _, err := s.col.ReplaceOne(ctx, bson.M{"_id": doc.ID}, doc, options.Replace().SetUpsert(true)); err != nil {
		return fmt.Errorf("save checkpoint %q: %w", doc.Collection, err)
	}
	return nil
}
