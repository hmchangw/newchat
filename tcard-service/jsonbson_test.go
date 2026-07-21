package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestJSONToBSON_PreservesLargeInteger(t *testing.T) {
	// 2^53+1 can't be represented exactly as float64; it must survive as int64.
	v, err := jsonToBSON(json.RawMessage(`{"n":9007199254740993,"f":1.5,"s":"x","a":[7]}`))
	require.NoError(t, err)
	m := v.(map[string]any)
	assert.Equal(t, int64(9007199254740993), m["n"])
	assert.Equal(t, 1.5, m["f"])
	assert.Equal(t, "x", m["s"])
	assert.Equal(t, []any{int64(7)}, m["a"])

	// The stored int64 must render back as a plain JSON number, not $numberLong,
	// so the served template is unchanged.
	out, err := bson.MarshalExtJSON(bson.D{{Key: "n", Value: m["n"]}}, false, false)
	require.NoError(t, err)
	assert.JSONEq(t, `{"n":9007199254740993}`, string(out))
}
