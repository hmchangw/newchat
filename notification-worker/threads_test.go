package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubThreadLookup struct {
	out []string
	err error
}

func (s *stubThreadLookup) Lookup(_ context.Context, _ string) (ThreadRoomInfo, error) {
	if s.err != nil {
		return ThreadRoomInfo{}, s.err
	}
	set := make(map[string]struct{}, len(s.out))
	for _, a := range s.out {
		set[a] = struct{}{}
	}
	return ThreadRoomInfo{Followers: set}, nil
}

func TestThreadFollowers_Resolve(t *testing.T) {
	s := &stubThreadLookup{out: []string{"alice", "bob"}}
	got, err := s.Lookup(context.Background(), "parent-1")
	require.NoError(t, err)
	assert.Contains(t, got.Followers, "alice")
	assert.Contains(t, got.Followers, "bob")
	assert.NotContains(t, got.Followers, "carol")
}

func TestThreadFollowers_PropagatesError(t *testing.T) {
	s := &stubThreadLookup{err: errors.New("mongo down")}
	_, err := s.Lookup(context.Background(), "parent-1")
	assert.Error(t, err)
}

// A zero threadParentCreatedAt must surface as nil, never as the epoch: isRestricted
// treats a nil parent time as "not visible" (fail closed), while time.Time{} would
// compare older than every historySharedSince and notify every mentionee.
func TestThreadRoomInfo_ZeroParentCreatedAtIsUnknown(t *testing.T) {
	info := threadRoomInfoFrom([]string{"alice", "bob"}, time.Time{})
	assert.Nil(t, info.ParentCreatedAt)
	assert.Len(t, info.Followers, 2)
}

func TestThreadRoomInfo_RealParentCreatedAtIsCarried(t *testing.T) {
	at := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	info := threadRoomInfoFrom([]string{"alice", ""}, at)
	require.NotNil(t, info.ParentCreatedAt)
	assert.Equal(t, at, *info.ParentCreatedAt)
	assert.Len(t, info.Followers, 1, "empty accounts are skipped")
}
