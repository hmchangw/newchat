package main

import (
	"context"
	"encoding/json"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// card is one cache row: the (path, cardVersion) key plus the template content
// rendered as JSON (schemaless templates stay raw JSON, not a typed struct).
type card struct {
	Path        string          `json:"path" bson:"path"`
	CardVersion string          `json:"_tcardVersion" bson:"_tcardVersion"`
	Template    json.RawMessage `json:"template" bson:"template"`
}

// cardDoc is the POST /validate payload: the submitted card document, decoded
// for the field checks only (json tags only, never persisted).
type cardDoc struct {
	Path        string          `json:"path"`
	CardVersion string          `json:"_tcardVersion"`
	Type        string          `json:"type"`
	Schema      string          `json:"schema"`
	Version     string          `json:"version"`
	Body        json.RawMessage `json:"body"`
}

// CardStore reads the cards collection the template cache is loaded from.
type CardStore interface {
	ListCards(ctx context.Context) ([]card, error)
}
