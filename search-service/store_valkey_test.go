package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/valkeyfake"
)

// SetNX / IncrEx satisfy valkeyutil.Client but are unused here; panic on any call.

func TestValkeyCache_SetThenGet(t *testing.T) {
	ctx := context.Background()
	c := newValkeyCache(valkeyfake.New())

	require.NoError(t, c.SetRestricted(ctx, "alice", map[string]int64{"r1": 100}, time.Minute))
	got, hit, err := c.GetRestricted(ctx, "alice")
	require.NoError(t, err)
	assert.True(t, hit)
	assert.Equal(t, map[string]int64{"r1": 100}, got)
}

func TestValkeyCache_GetMiss(t *testing.T) {
	c := newValkeyCache(valkeyfake.New())
	got, hit, err := c.GetRestricted(context.Background(), "nobody")
	require.NoError(t, err)
	assert.False(t, hit)
	assert.Nil(t, got)
}

func TestValkeyCache_GetTransportError(t *testing.T) {
	stub := valkeyfake.New()
	stub.FailGet(errors.New("conn refused"))
	c := newValkeyCache(stub)

	_, hit, err := c.GetRestricted(context.Background(), "alice")
	assert.False(t, hit)
	assert.Error(t, err)
}

func TestValkeyCache_SetError(t *testing.T) {
	stub := valkeyfake.New()
	stub.FailSet(errors.New("disk full"))
	c := newValkeyCache(stub)

	err := c.SetRestricted(context.Background(), "alice", map[string]int64{}, time.Minute)
	assert.Error(t, err)
}

func TestValkeyCache_SetNilMapBecomesEmpty(t *testing.T) {
	stub := valkeyfake.New()
	c := newValkeyCache(stub)

	require.NoError(t, c.SetRestricted(context.Background(), "alice", nil, time.Minute))
	// Read back the stored value — should be `{}` (marshalled empty map),
	// not `null`, so a subsequent cache hit returns an empty map rather
	// than a nil map that the handler would fall through on.
	assert.Equal(t, "{}", stub.Value(restrictedKey("alice")))
}

func TestValkeyCache_GetJSONNullYieldsEmptyMap(t *testing.T) {
	stub := valkeyfake.New()
	stub.Seed(restrictedKey("alice"), "null", time.Minute)
	c := newValkeyCache(stub)

	got, hit, err := c.GetRestricted(context.Background(), "alice")
	require.NoError(t, err)
	assert.True(t, hit)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

func TestRestrictedKey_Format(t *testing.T) {
	assert.Equal(t, "searchservice:restrictedrooms:alice", restrictedKey("alice"))
}

// MGet loops the fake's own Get so it cannot drift from single-key behaviour.
