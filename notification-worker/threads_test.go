package main

import (
	"context"
	"errors"
	"testing"

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
