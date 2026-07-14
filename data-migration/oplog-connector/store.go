package main

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . CheckpointStore

// Checkpoint is the persisted resume position for one (site, collection). ResumeToken is the real checkpoint (opaque raw BSON); ClusterTime is a coarse fallback.
type Checkpoint struct {
	ID          string   `bson:"_id"` // "{siteID}:{collection}"
	SiteID      string   `bson:"siteId"`
	Collection  string   `bson:"collection"`
	ResumeToken bson.Raw `bson:"resumeToken"`
	ClusterTime int64    `bson:"clusterTime"` // op time of last acked event, unix ms
	EventID     string   `bson:"eventId"`     // _id._data of last acked event
	Source      string   `bson:"source"`      // "seed" | "runtime"
	UpdatedAt   int64    `bson:"updatedAt"`   // last persist time, unix ms
}

// CheckpointStore persists and loads per-collection resume positions.
type CheckpointStore interface {
	// Load returns the checkpoint for a collection, or (nil, nil) when absent.
	Load(ctx context.Context, collection string) (*Checkpoint, error)
	// Save upserts the checkpoint keyed by its _id.
	Save(ctx context.Context, cp *Checkpoint) error
}
