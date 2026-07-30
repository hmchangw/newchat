package main

import (
	"context"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoCardStore struct {
	cards *mongo.Collection
}

func newMongoCardStore(db *mongo.Database) *mongoCardStore {
	return &mongoCardStore{cards: db.Collection("cards")}
}

// EnsureIndexes enforces (path, _tcardVersion) uniqueness so two docs can't
// claim one version. The data-type `version` field is unrelated, not indexed.
func (s *mongoCardStore) EnsureIndexes(ctx context.Context) error {
	if _, err := s.cards.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "path", Value: 1}, {Key: "_tcardVersion", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("ensure cards (path, _tcardVersion) unique index: %w", err)
	}
	return nil
}

// ListCards returns every card keyed by (path, _tcardVersion) as relaxed
// ext-JSON minus the routing and storage-only keys; unkeyable docs are skipped.
func (s *mongoCardStore) ListCards(ctx context.Context) ([]card, error) {
	// Bandwidth only — docToCard is the correctness guarantee. Templates are
	// schemaless, so the wanted fields can't be enumerated.
	proj := options.Find().SetProjection(bson.D{
		{Key: "_id", Value: 0}, {Key: "migratedAt", Value: 0},
	})
	cursor, err := s.cards.Find(ctx, bson.D{}, proj)
	if err != nil {
		return nil, fmt.Errorf("find cards: %w", err)
	}
	defer cursor.Close(ctx)

	var cards []card
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode card document: %w", err)
		}
		c, ok, err := docToCard(doc)
		if err != nil {
			return nil, err
		}
		if !ok {
			slog.Warn("card document missing a string path or _tcardVersion, skipping")
			continue
		}
		cards = append(cards, c)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate cards: %w", err)
	}
	return cards, nil
}

// docToCard renders a cards doc to a cache card, dropping path and the
// storage-only top-level keys; ok is false when it can't be keyed.
func docToCard(doc bson.D) (card, bool, error) {
	var path, cardVersion string
	payload := make(bson.D, 0, len(doc))
	for _, e := range doc {
		switch e.Key {
		case "path":
			path, _ = e.Value.(string) // routing key, not template content — drop it
		case "_tcardVersion":
			cardVersion, _ = e.Value.(string)
			payload = append(payload, e)
		case "_id", "migratedAt":
			// Top-level only, so an element id inside body stays template content.
		default:
			payload = append(payload, e)
		}
	}
	if path == "" || cardVersion == "" {
		return card{}, false, nil
	}
	tmpl, err := bson.MarshalExtJSON(payload, false, false)
	if err != nil {
		return card{}, false, fmt.Errorf("render card %q@%q to JSON: %w", path, cardVersion, err)
	}
	return card{Path: path, CardVersion: cardVersion, Template: tmpl}, true, nil
}
