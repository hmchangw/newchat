package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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
	// Returns out AND err together: userstore.Cache does exactly that (cache hits
	// alongside a failed read for the misses), so a stub that nils out on error
	// would make the production partial-result path untestable.
	return s.out, s.err
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

// TestUserMentionNames_Resolve_PartialResultsOnError pins the contract the real
// dependency actually exercises: userstore.Cache returns cache hits alongside a
// non-nil error when the backing read for the misses fails. Resolve must keep
// those hits rather than discarding them.
func TestUserMentionNames_Resolve_PartialResultsOnError(t *testing.T) {
	finder := &stubUserFinder{
		out: []model.User{{Account: "bob", EngName: "Bob Chen"}},
		err: errors.New("mongo down"),
	}
	r := newUserMentionNames(finder, 0)

	got, err := r.Resolve(context.Background(), []string{"bob", "ghost"})

	require.Error(t, err)
	assert.Equal(t, map[string]string{"bob": "Bob Chen"}, got,
		"names collected before the error must survive so a partial read substitutes what it can")
}

func TestNewUserMentionNames_TimeoutDefault(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{name: "zero takes the default", in: 0, want: defaultMentionNamesTimeout},
		{name: "negative takes the default", in: -time.Second, want: defaultMentionNamesTimeout},
		{name: "positive is kept", in: 5 * time.Second, want: 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, newUserMentionNames(&stubUserFinder{}, tc.in).timeout)
		})
	}
}

// blockingUserFinder blocks until its context is cancelled, so the test can
// assert the resolver's own timeout bounds the call rather than the caller's.
type blockingUserFinder struct{}

func (blockingUserFinder) FindUsersByAccounts(ctx context.Context, _ []string) ([]model.User, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestUserMentionNames_Resolve_TimeoutBoundsTheCall(t *testing.T) {
	r := newUserMentionNames(blockingUserFinder{}, 20*time.Millisecond)

	got, err := r.Resolve(context.Background(), []string{"alice"})

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "the resolver's own timeout must bite")
	assert.Empty(t, got)
}
