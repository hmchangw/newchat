package main

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// stubUserFinder records the accounts it was asked for so tests can pin what
// reaches the users collection.
type stubUserFinder struct {
	mu       sync.Mutex
	out      []model.User
	err      error
	gotCalls [][]string
}

func (s *stubUserFinder) FindUsersByAccounts(_ context.Context, accounts []string) ([]model.User, error) {
	s.mu.Lock()
	got := make([]string, len(accounts))
	copy(got, accounts)
	s.gotCalls = append(s.gotCalls, got)
	s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	return s.out, nil
}

func (s *stubUserFinder) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.gotCalls)
}

func (s *stubUserFinder) lastAccounts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.gotCalls) == 0 {
		return nil
	}
	return s.gotCalls[len(s.gotCalls)-1]
}

func TestUserMentionNames_Resolve(t *testing.T) {
	tests := []struct {
		name     string
		accounts []string
		users    []model.User
		want     map[string]string
	}{
		{
			name:     "both names combine",
			accounts: []string{"alice"},
			users:    []model.User{{Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲"}},
			want:     map[string]string{"alice": "Alice Wang 愛麗絲"},
		},
		{
			name:     "english name only",
			accounts: []string{"bob"},
			users:    []model.User{{Account: "bob", EngName: "Bob Chen"}},
			want:     map[string]string{"bob": "Bob Chen"},
		},
		{
			name:     "chinese name only",
			accounts: []string{"carol"},
			users:    []model.User{{Account: "carol", ChineseName: "凱蘿"}},
			want:     map[string]string{"carol": "凱蘿"},
		},
		{
			name:     "identical names are not doubled",
			accounts: []string{"dave"},
			users:    []model.User{{Account: "dave", EngName: "Dave", ChineseName: "Dave"}},
			want:     map[string]string{"dave": "Dave"},
		},
		{
			name:     "user with no names is omitted so the raw token survives",
			accounts: []string{"erin"},
			users:    []model.User{{Account: "erin"}},
			want:     map[string]string{},
		},
		{
			name:     "account key is lowercased",
			accounts: []string{"frank"},
			users:    []model.User{{Account: "Frank", EngName: "Frank Lin"}},
			want:     map[string]string{"frank": "Frank Lin"},
		},
		{
			name:     "unmatched account is absent from the map",
			accounts: []string{"alice", "ghost"},
			users:    []model.User{{Account: "alice", EngName: "Alice Wang"}},
			want:     map[string]string{"alice": "Alice Wang"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			finder := &stubUserFinder{out: tt.users}
			r := newUserMentionNames(finder, 0)

			got, err := r.Resolve(context.Background(), tt.accounts)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUserMentionNames_Resolve_NoAccountsSkipsLookup(t *testing.T) {
	finder := &stubUserFinder{}
	r := newUserMentionNames(finder, 0)

	got, err := r.Resolve(context.Background(), nil)

	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Zero(t, finder.callCount(), "no accounts must not reach the users collection")
}

func TestUserMentionNames_Resolve_StoreErrorPropagates(t *testing.T) {
	finder := &stubUserFinder{err: errors.New("mongo down")}
	r := newUserMentionNames(finder, 0)

	got, err := r.Resolve(context.Background(), []string{"alice"})

	require.Error(t, err, "the handler decides to fail open; the resolver reports the truth")
	assert.Empty(t, got)
}

func TestUserMentionNames_Resolve_PassesAccountsThrough(t *testing.T) {
	finder := &stubUserFinder{}
	r := newUserMentionNames(finder, 0)

	_, err := r.Resolve(context.Background(), []string{"alice", "bob"})

	require.NoError(t, err)
	assert.Equal(t, []string{"alice", "bob"}, finder.lastAccounts())
}
