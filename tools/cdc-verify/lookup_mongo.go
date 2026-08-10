package main

import (
	"context"
	"fmt"
	"sort"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoStore struct {
	db *mongo.Database
}

//nolint:unused // wired into main.go's dependency graph by a later task
func newMongoStore(db *mongo.Database) *mongoStore { return &mongoStore{db: db} }

func (s *mongoStore) FindByID(ctx context.Context, collection, id string) (map[string]any, error) {
	var doc bson.M
	err := s.db.Collection(collection).FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, errNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find %s by id: %w", collection, err)
	}
	return bsonToMap(doc), nil
}

func (s *mongoStore) FindOne(ctx context.Context, collection string, key map[string]any, fields []string) (map[string]any, error) {
	filter := bson.M{}
	for k, v := range key {
		filter[k] = v
	}
	proj := bson.M{}
	for _, f := range fields {
		proj[f] = 1
	}
	// Limit 2 so a non-unique key is detected as ambiguity, not silently
	// verified against an arbitrary doc.
	cur, err := s.db.Collection(collection).Find(ctx, filter,
		options.Find().SetProjection(proj).SetLimit(2))
	if err != nil {
		return nil, fmt.Errorf("find %s by key: %w", collection, err)
	}
	var docs []bson.M
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("decode %s find result: %w", collection, err)
	}
	switch len(docs) {
	case 0:
		return nil, errNotFound
	case 1:
		return bsonToMap(docs[0]), nil
	default:
		return nil, errAmbiguous
	}
}

// bsonToMap converts driver types into the plain map/slice/scalar forms the
// compare layer understands.
func bsonToMap(m bson.M) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = bsonValue(v)
	}
	return out
}

func bsonValue(v any) any {
	switch t := v.(type) {
	case bson.M:
		return bsonToMap(t)
	case bson.D:
		sub := bson.M{}
		for _, e := range t {
			sub[e.Key] = e.Value
		}
		return bsonToMap(sub)
	case bson.A:
		arr := make([]any, len(t))
		for i, e := range t {
			arr[i] = bsonValue(e)
		}
		return arr
	case bson.DateTime:
		return t.Time().UTC()
	default:
		return v
	}
}

// sortedKeys gives deterministic iteration for query building and logs.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

var (
	_ SourceStore = (*mongoStore)(nil)
	_ TargetStore = (*mongoStore)(nil)
)
