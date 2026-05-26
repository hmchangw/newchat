package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestrictedCacheKey_Format(t *testing.T) {
	assert.Equal(t, "searchservice:restrictedrooms:alice", restrictedCacheKey("alice"))
}

func TestRestrictedCachePayload_IsValidJSONOfRoomsMap(t *testing.T) {
	entry := RestrictedCacheEntry{
		Account: "alice",
		Rooms:   map[string]int64{"r-eng": 1700000000000},
	}
	payload, err := restrictedCachePayload(entry)
	require.NoError(t, err)

	var got map[string]int64
	require.NoError(t, json.Unmarshal([]byte(payload), &got))
	assert.Equal(t, map[string]int64{"r-eng": 1700000000000}, got)
}
