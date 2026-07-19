package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// EnsureIndexes enforces (path, cardVersion) uniqueness so two docs can't claim
// one version. The data-type `version` field is unrelated and is not indexed.
func (s *mongoCardStore) EnsureIndexes(ctx context.Context) error {
	if _, err := s.cards.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "path", Value: 1}, {Key: "cardVersion", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("ensure cards (path, cardVersion) unique index: %w", err)
	}
	return nil
}

// ListCards returns every card keyed by (path, cardVersion), each rendered to
// relaxed ext-JSON minus _id and path; docs missing either key are skipped.
func (s *mongoCardStore) ListCards(ctx context.Context) ([]card, error) {
	proj := options.Find().SetProjection(bson.D{{Key: "_id", Value: 0}})
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
			slog.Warn("card document missing a string path or cardVersion, skipping")
			continue
		}
		cards = append(cards, c)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate cards: %w", err)
	}
	return cards, nil
}

// GetCard fetches one card by (path, cardVersion); ok is false if none matches.
func (s *mongoCardStore) GetCard(ctx context.Context, path, cardVersion string) (card, bool, error) {
	proj := options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 0}})
	filter := bson.D{{Key: "path", Value: path}, {Key: "cardVersion", Value: cardVersion}}
	var doc bson.D
	if err := s.cards.FindOne(ctx, filter, proj).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return card{}, false, nil
		}
		return card{}, false, fmt.Errorf("find card %q@%q: %w", path, cardVersion, err)
	}
	return docToCard(doc)
}

// docToCard renders one _id-projected cards document to a cache card: the
// template is the doc minus path; ok is false when it can't be keyed.
func docToCard(doc bson.D) (card, bool, error) {
	var path, cardVersion string
	payload := make(bson.D, 0, len(doc))
	for _, e := range doc {
		switch e.Key {
		case "path":
			path, _ = e.Value.(string) // routing key, not template content — drop it
		case "cardVersion":
			cardVersion, _ = e.Value.(string)
			payload = append(payload, e)
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

// ListVersions returns the cardVersion of every document for path.
func (s *mongoCardStore) ListVersions(ctx context.Context, path string) ([]string, error) {
	proj := options.Find().SetProjection(bson.D{{Key: "cardVersion", Value: 1}, {Key: "_id", Value: 0}})
	cursor, err := s.cards.Find(ctx, bson.D{{Key: "path", Value: path}}, proj)
	if err != nil {
		return nil, fmt.Errorf("find card versions: %w", err)
	}
	defer cursor.Close(ctx)

	var versions []string
	for cursor.Next(ctx) {
		if v, ok := cursor.Current.Lookup("cardVersion").StringValueOK(); ok {
			versions = append(versions, v)
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate card versions: %w", err)
	}
	return versions, nil
}

// InsertCard writes one validated card, mapping a duplicate key to ErrDuplicateCard.
func (s *mongoCardStore) InsertCard(ctx context.Context, doc *cardDoc) error {
	body, err := jsonToBSON(doc.Body)
	if err != nil {
		return fmt.Errorf("decode body: %w", err)
	}
	d := bson.M{
		"path": doc.Path, "cardVersion": doc.CardVersion,
		"type": doc.Type, "schema": doc.Schema, "version": doc.Version, "body": body,
	}
	if len(doc.CardUsage) > 0 {
		usage, err := jsonToBSON(doc.CardUsage)
		if err != nil {
			return fmt.Errorf("decode cardUsage: %w", err)
		}
		d["cardUsage"] = usage
	}
	if _, err := s.cards.InsertOne(ctx, d); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrDuplicateCard
		}
		return fmt.Errorf("insert card: %w", err)
	}
	return nil
}

// jsonToBSON turns raw JSON into a value the mongo driver can marshal, decoding
// with UseNumber so large integers survive instead of rounding through float64.
func jsonToBSON(raw json.RawMessage) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("unmarshal json: %w", err)
	}
	return normalizeNumbers(v), nil
}

// normalizeNumbers converts json.Number to int64 (else float64) recursively so
// integers store as BSON int64/double rather than a rounded float64.
func normalizeNumbers(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			t[k] = normalizeNumbers(val)
		}
	case []any:
		for i, val := range t {
			t[i] = normalizeNumbers(val)
		}
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	}
	return v
}
