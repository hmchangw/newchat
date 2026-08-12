package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// mongoFailoverStore persists FailoverState in the failover_states collection,
// keyed by _id = siteID, with optimistic-concurrency CAS on the version field.
type mongoFailoverStore struct {
	coll *mongo.Collection
}

func newMongoFailoverStore(db *mongo.Database) *mongoFailoverStore {
	return &mongoFailoverStore{coll: db.Collection("failover_states")}
}

// Get returns the stored state for siteID, or a synthesized healthy version-0
// state when no document exists (an unfailed site has no row).
func (s *mongoFailoverStore) Get(ctx context.Context, siteID string) (FailoverState, error) {
	var st FailoverState
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: siteID}}).Decode(&st)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return FailoverState{SiteID: siteID, Status: StatusHealthy, Version: 0}, nil
	}
	if err != nil {
		return FailoverState{}, fmt.Errorf("get failover state %q: %w", siteID, err)
	}
	return st, nil
}

// List returns every stored failover state.
func (s *mongoFailoverStore) List(ctx context.Context) ([]FailoverState, error) {
	cur, err := s.coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list failover states: %w", err)
	}
	var out []FailoverState
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("decode failover states: %w", err)
	}
	return out, nil
}

// Transition persists next. When next.Version == 1 it inserts the first state
// (a concurrent insert -> duplicate key -> conflict). Otherwise it CAS-updates
// the document whose stored version == next.Version-1; a non-match means a
// racing writer moved first -> conflict.
func (s *mongoFailoverStore) Transition(ctx context.Context, next FailoverState) error {
	if next.Version < 1 {
		return fmt.Errorf("transition: next version must be >= 1, got %d", next.Version)
	}
	if next.Version == 1 {
		if _, err := s.coll.InsertOne(ctx, next); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return errFailoverVersionConflict
			}
			return fmt.Errorf("insert failover state: %w", err)
		}
		return nil
	}
	res, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: next.SiteID}, {Key: "version", Value: next.Version - 1}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "status", Value: next.Status},
			{Key: "reason", Value: next.Reason},
			{Key: "operator", Value: next.Operator},
			{Key: "since", Value: next.Since},
			{Key: "version", Value: next.Version},
			{Key: "timestamp", Value: next.Timestamp},
		}}},
	)
	if err != nil {
		return fmt.Errorf("update failover state: %w", err)
	}
	if res.MatchedCount == 0 {
		return errFailoverVersionConflict
	}
	return nil
}
