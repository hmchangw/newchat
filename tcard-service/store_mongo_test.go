package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoCardStore_DocToCard(t *testing.T) {
	tests := []struct {
		name        string
		doc         bson.D
		wantOK      bool
		wantPath    string
		wantVersion string
		wantJSON    string
	}{
		{
			name: "keys on _tcardVersion, strips path, keeps _tcardVersion in payload",
			doc: bson.D{
				{Key: "path", Value: "greetings/en/welcome"},
				{Key: "_tcardVersion", Value: "1.0.0"},
				{Key: "title", Value: "Hi"},
			},
			wantOK: true, wantPath: "greetings/en/welcome", wantVersion: "1.0.0",
			wantJSON: `{"_tcardVersion":"1.0.0","title":"Hi"}`,
		},
		{
			name: "strips the storage-only _id and migratedAt fields",
			doc: bson.D{
				{Key: "_id", Value: "c-welcome"},
				{Key: "migratedAt", Value: "2026-01-02T03:04:05Z"},
				{Key: "path", Value: "greetings/en/welcome"},
				{Key: "_tcardVersion", Value: "1.0.0"},
				{Key: "title", Value: "Hi"},
			},
			wantOK: true, wantPath: "greetings/en/welcome", wantVersion: "1.0.0",
			wantJSON: `{"_tcardVersion":"1.0.0","title":"Hi"}`,
		},
		{
			name: "only top-level keys are dropped: an element id inside body survives",
			doc: bson.D{
				{Key: "path", Value: "a/b/c"},
				{Key: "_tcardVersion", Value: "1.0.0"},
				{Key: "body", Value: bson.A{
					bson.D{{Key: "type", Value: "TextBlock"}, {Key: "id", Value: "greeting"}},
				}},
			},
			wantOK: true, wantPath: "a/b/c", wantVersion: "1.0.0",
			wantJSON: `{"_tcardVersion":"1.0.0","body":[{"type":"TextBlock","id":"greeting"}]}`,
		},
		{
			name: "legacy cardVersion key is no longer recognized",
			doc: bson.D{
				{Key: "path", Value: "greetings/en/welcome"},
				{Key: "cardVersion", Value: "1.0.0"},
			},
			wantOK: false,
		},
		{
			name:   "missing path is skipped",
			doc:    bson.D{{Key: "_tcardVersion", Value: "1.0.0"}, {Key: "title", Value: "x"}},
			wantOK: false,
		},
		{
			name:   "missing _tcardVersion is skipped",
			doc:    bson.D{{Key: "path", Value: "a/b/c"}, {Key: "title", Value: "x"}},
			wantOK: false,
		},
		{
			name:   "non-string _tcardVersion is skipped",
			doc:    bson.D{{Key: "path", Value: "a/b/c"}, {Key: "_tcardVersion", Value: 42}},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, ok, err := docToCard(tt.doc)
			require.NoError(t, err)
			require.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				return
			}
			assert.Equal(t, tt.wantPath, c.Path)
			assert.Equal(t, tt.wantVersion, c.CardVersion)
			assert.JSONEq(t, tt.wantJSON, string(c.Template))
		})
	}
}
