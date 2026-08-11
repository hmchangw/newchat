package main

import (
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/migration"
)

// oplogEvent is the subset of the connector's OplogEvent wire shape this applier needs.
// fullDocument/documentKey are relaxed extended JSON (the connector's encoding). The DR feed
// opens its change stream with updateLookup, so fullDocument is present for insert/replace/update
// (absent only for delete) — this applier never re-reads the origin Mongo.
type oplogEvent struct {
	EventID      string          `json:"eventId"`
	Op           string          `json:"op"`
	Collection   string          `json:"coll"`
	DocumentKey  json.RawMessage `json:"documentKey"`
	FullDocument json.RawMessage `json:"fullDocument"`
}

// documentID extracts the _id value from a documentKey as its native BSON type (string, ObjectID,
// int, …) — NOT forced to string, since the operational collections key by different id types.
// Returns migration.ErrPoison when the payload is malformed or has no _id.
func documentID(raw json.RawMessage) (any, error) {
	var d bson.D
	if err := bson.UnmarshalExtJSON(raw, false, &d); err != nil {
		return nil, fmt.Errorf("%w: bad documentKey: %v", migration.ErrPoison, err) //nolint:errorlint // single-%w sentinel wrap; decode err is informational
	}
	for _, e := range d {
		if e.Key == "_id" {
			return e.Value, nil
		}
	}
	return nil, fmt.Errorf("%w: documentKey has no _id", migration.ErrPoison)
}

// decodeExtJSONDoc decodes a relaxed-extJSON document into an opaque, type-preserving bson.D.
func decodeExtJSONDoc(raw json.RawMessage) (bson.D, error) {
	var d bson.D
	if err := bson.UnmarshalExtJSON(raw, false, &d); err != nil {
		return nil, fmt.Errorf("%w: decode full document: %v", migration.ErrPoison, err) //nolint:errorlint // single-%w sentinel wrap; decode err is informational
	}
	return d, nil
}
