package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestBsonToMap(t *testing.T) {
	now := bson.NewDateTimeFromTime(time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC))
	in := bson.M{
		"s":   "x",
		"n":   int32(5),
		"t":   now,
		"sub": bson.M{"k": "v"},
		"arr": bson.A{"a", bson.M{"b": int64(1)}},
	}
	out := bsonToMap(in)
	assert.Equal(t, "x", out["s"])
	assert.Equal(t, int32(5), out["n"])
	assert.Equal(t, now.Time().UTC(), out["t"])
	assert.Equal(t, map[string]any{"k": "v"}, out["sub"])
	assert.Equal(t, []any{"a", map[string]any{"b": int64(1)}}, out["arr"])
}

func TestBuildSelect(t *testing.T) {
	q, args, err := buildSelect("messages_by_id", map[string]any{"message_id": "m1"}, []string{"body", "created_at"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT body, created_at FROM messages_by_id WHERE message_id = ?", q)
	assert.Equal(t, []any{"m1"}, args)

	q, args, err = buildSelect("t", map[string]any{"b": int64(2), "a": "x"}, []string{"c"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT c FROM t WHERE a = ? AND b = ?", q) // sorted key columns
	assert.Equal(t, []any{"x", int64(2)}, args)

	_, _, err = buildSelect("bad;table", map[string]any{"a": 1}, []string{"c"})
	assert.Error(t, err)
	_, _, err = buildSelect("t", map[string]any{"a; DROP": 1}, []string{"c"})
	assert.Error(t, err)
	_, _, err = buildSelect("t", map[string]any{"a": 1}, []string{"c*"})
	assert.Error(t, err)
}
