package main

import (
	"context"
	"encoding/json"
	"errors"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// card is one cache row: the (path, cardVersion) key plus the template content
// rendered as JSON (schemaless templates stay raw JSON, not a typed struct).
type card struct {
	Path        string          `json:"path" bson:"path"`
	CardVersion string          `json:"cardVersion" bson:"cardVersion"`
	Template    json.RawMessage `json:"template" bson:"template"`
}

// cardDoc is the POST /register payload (json tags only, never bson-marshaled;
// InsertCard builds the stored document explicitly).
type cardDoc struct {
	Path        string          `json:"path"`
	CardVersion string          `json:"cardVersion"`
	CardUsage   json.RawMessage `json:"cardUsage"`
	Type        string          `json:"type"`
	Schema      string          `json:"schema"`
	Version     string          `json:"version"`
	Body        json.RawMessage `json:"body"`
}

// ErrDuplicateCard is returned by InsertCard when (path, cardVersion) exists.
var ErrDuplicateCard = errors.New("card already exists")

// CardStore reads and writes the cards collection behind the template cache.
type CardStore interface {
	ListCards(ctx context.Context) ([]card, error)
	GetCard(ctx context.Context, path, cardVersion string) (card, bool, error)
	ListVersions(ctx context.Context, path string) ([]string, error)
	InsertCard(ctx context.Context, doc *cardDoc) error
}
