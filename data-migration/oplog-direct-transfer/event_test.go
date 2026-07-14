package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/migration"
)

func TestDocumentID_StringID(t *testing.T) {
	id, err := documentID(json.RawMessage(`{"_id":"abc123"}`))
	require.NoError(t, err)
	assert.Equal(t, "abc123", id)
}

func TestDocumentID_ObjectID(t *testing.T) {
	// Extended-JSON ObjectId decodes to a non-string BSON type — must not error.
	id, err := documentID(json.RawMessage(`{"_id":{"$oid":"5f9b1c2d3e4a5b6c7d8e9f01"}}`))
	require.NoError(t, err)
	assert.NotNil(t, id)
}

func TestDocumentID_MissingID_Poison(t *testing.T) {
	_, err := documentID(json.RawMessage(`{}`))
	assert.ErrorIs(t, err, migration.ErrPoison)
}

func TestDocumentID_Malformed_Poison(t *testing.T) {
	_, err := documentID(json.RawMessage(`{bad`))
	assert.ErrorIs(t, err, migration.ErrPoison)
}

func TestDecodeExtJSONDoc_PreservesFields(t *testing.T) {
	doc, err := decodeExtJSONDoc(json.RawMessage(`{"_id":"u1","name":"avatar","n":3}`))
	require.NoError(t, err)
	m := map[string]any{}
	for _, e := range doc {
		m[e.Key] = e.Value
	}
	assert.Equal(t, "u1", m["_id"])
	assert.Equal(t, "avatar", m["name"])
}
