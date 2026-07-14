package main

import (
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/migration"
)

// oplogEvent is the subset of the connector's event wire shape this service needs.
// fullDocument/documentKey are relaxed extended JSON (the connector's encoding).
type oplogEvent struct {
	EventID      string          `json:"eventId"`
	Op           string          `json:"op"`
	Collection   string          `json:"coll"`
	DocumentKey  json.RawMessage `json:"documentKey"`
	FullDocument json.RawMessage `json:"fullDocument"`
	// Degraded is set by the connector when it couldn't encode fullDocument (left nil) but still
	// published; the handler recovers the doc via a source lookup instead of poisoning it.
	Degraded       bool   `json:"degraded"`
	DegradedReason string `json:"degradedReason"`
}

// documentID extracts the _id value from a documentKey/doc as its native BSON type (string,
// ObjectID, int, …) — NOT forced to string, since these collections may key by any type.
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
		return nil, fmt.Errorf("%w: decode source doc: %v", migration.ErrPoison, err) //nolint:errorlint // single-%w sentinel wrap; decode err is informational
	}
	return d, nil
}
