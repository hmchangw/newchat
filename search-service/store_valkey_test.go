package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// stubValkey is an in-memory stand-in for valkeyutil.Client — only the
// methods the valkeyCache actually uses are implemented.
type stubValkey struct {
	store    map[string]string
	getErr   error
	setErr   error
	lastTTL  time.Duration
	setCalls int
}

func newStubValkey() *stubValkey {
	return &stubValkey{store: map[string]string{}}
}

func (s *stubValkey) Get(_ context.Context, key string) (string, error) {
	if s.getErr != nil {
		return "", s.getErr
	}
	v, ok := s.store[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	return v, nil
}

func (s *stubValkey) Set(_ context.Context, key, value string, ttl time.Duration) error {
	s.setCalls++
	s.lastTTL = ttl
	if s.setErr != nil {
		return s.setErr
	}
	s.store[key] = value
	return nil
}

func (s *stubValkey) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(s.store, k)
	}
	return nil
}

func (s *stubValkey) Close() error { return nil }

// SetNX / IncrEx satisfy valkeyutil.Client but are unused here; panic on any call.
func (s *stubValkey) SetNX(_ context.Context, _, _ string, _ time.Duration) (bool, error) {
	panic("stubValkey.SetNX not implemented")
}

func (s *stubValkey) IncrEx(_ context.Context, _ string, _ time.Duration) (int64, error) {
	panic("stubValkey.IncrEx not implemented")
}

func TestValkeyCache_SetThenGet(t *testing.T) {
	ctx := context.Background()
	c := newValkeyCache(newStubValkey())

	require.NoError(t, c.SetRestricted(ctx, "alice", map[string]int64{"r1": 100}, time.Minute))
	got, hit, err := c.GetRestricted(ctx, "alice")
	require.NoError(t, err)
	assert.True(t, hit)
	assert.Equal(t, map[string]int64{"r1": 100}, got)
}

func TestValkeyCache_GetMiss(t *testing.T) {
	c := newValkeyCache(newStubValkey())
	got, hit, err := c.GetRestricted(context.Background(), "nobody")
	require.NoError(t, err)
	assert.False(t, hit)
	assert.Nil(t, got)
}

func TestValkeyCache_GetTransportError(t *testing.T) {
	stub := newStubValkey()
	stub.getErr = errors.New("conn refused")
	c := newValkeyCache(stub)

	_, hit, err := c.GetRestricted(context.Background(), "alice")
	assert.False(t, hit)
	assert.Error(t, err)
}

func TestValkeyCache_SetError(t *testing.T) {
	stub := newStubValkey()
	stub.setErr = errors.New("disk full")
	c := newValkeyCache(stub)

	err := c.SetRestricted(context.Background(), "alice", map[string]int64{}, time.Minute)
	assert.Error(t, err)
}

func TestValkeyCache_SetNilMapBecomesEmpty(t *testing.T) {
	stub := newStubValkey()
	c := newValkeyCache(stub)

	require.NoError(t, c.SetRestricted(context.Background(), "alice", nil, time.Minute))
	// Read back the stored value — should be `{}` (marshalled empty map),
	// not `null`, so a subsequent cache hit returns an empty map rather
	// than a nil map that the handler would fall through on.
	assert.Equal(t, "{}", stub.store[restrictedKey("alice")])
}

func TestValkeyCache_GetJSONNullYieldsEmptyMap(t *testing.T) {
	stub := newStubValkey()
	stub.store[restrictedKey("alice")] = "null"
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
