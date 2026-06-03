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

func (s *stubThreadLookup) Followers(_ context.Context, _ string) (map[string]struct{}, error) {
	if s.err != nil {
		return nil, s.err
	}
	set := make(map[string]struct{}, len(s.out))
	for _, a := range s.out {
		set[a] = struct{}{}
	}
	return set, nil
}

func TestThreadFollowers_Resolve(t *testing.T) {
	s := &stubThreadLookup{out: []string{"alice", "bob"}}
	got, err := s.Followers(context.Background(), "parent-1")
	require.NoError(t, err)
	assert.Contains(t, got, "alice")
	assert.Contains(t, got, "bob")
	assert.NotContains(t, got, "carol")
}

func TestThreadFollowers_PropagatesError(t *testing.T) {
	s := &stubThreadLookup{err: errors.New("mongo down")}
	_, err := s.Followers(context.Background(), "parent-1")
	assert.Error(t, err)
}
