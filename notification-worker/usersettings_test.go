package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestNotifSettings_ZeroValueIsPreEnforcementBehaviour(t *testing.T) {
	var ns notifSettings
	assert.False(t, ns.muteAll, "zero value must not mute")
	assert.False(t, ns.allowPriority, "zero value must not pierce")
	assert.False(t, ns.showInCall, "zero value must keep in-call suppressed")
	assert.False(t, ns.isPriority("alice"), "zero value has no priority contacts")
}

func TestNotifSettings_IsPriority(t *testing.T) {
	tests := []struct {
		name     string
		contacts map[string]struct{}
		account  string
		want     bool
	}{
		{"listed user", map[string]struct{}{"alice": {}}, "alice", true},
		{"listed bot", map[string]struct{}{"helper.bot": {}}, "helper.bot", true},
		{"not listed", map[string]struct{}{"alice": {}}, "bob", false},
		{"nil map", nil, "alice", false},
		{"empty map", map[string]struct{}{}, "alice", false},
		{"empty account never matches", map[string]struct{}{"": {}}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := notifSettings{priorityContacts: tt.contacts}
			assert.Equal(t, tt.want, ns.isPriority(tt.account))
		})
	}
}

func TestNoopUserSettings_EmptySnapshot(t *testing.T) {
	var s UserSettingsSnapshotter = noopUserSettings{}
	got, err := s.Snapshot(context.Background(), []string{"alice", "bob"})
	require.NoError(t, err)
	assert.Empty(t, got, "kill switch yields the zero notifSettings for every account")
}

func TestResolveNotifSettings(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	t.Run("nil settings yields zero value", func(t *testing.T) {
		assert.Equal(t, notifSettings{}, resolveNotifSettings(nil))
	})

	t.Run("empty settings yields zero value", func(t *testing.T) {
		assert.Equal(t, notifSettings{}, resolveNotifSettings(&model.UserSettings{}))
	})

	t.Run("all three set", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{
			MuteAllNotifications:             boolPtr(true),
			AlwaysAllowPriorityNotifications: boolPtr(true),
			ShowNotificationsInCall:          boolPtr(true),
		})
		assert.True(t, got.muteAll)
		assert.True(t, got.allowPriority)
		assert.True(t, got.showInCall)
	})

	t.Run("explicit false is false, not unset", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{
			MuteAllNotifications: boolPtr(false),
		})
		assert.False(t, got.muteAll)
	})

	t.Run("priority contacts become a set, empties dropped", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{
			PriorityContacts: []string{"alice", "helper.bot", ""},
		})
		assert.True(t, got.isPriority("alice"))
		assert.True(t, got.isPriority("helper.bot"))
		assert.Len(t, got.priorityContacts, 2, "empty account must not enter the set")
	})

	t.Run("no priority contacts leaves a nil set", func(t *testing.T) {
		got := resolveNotifSettings(&model.UserSettings{})
		assert.Nil(t, got.priorityContacts)
		assert.False(t, got.isPriority("alice"))
	})
}

func TestMongoUserSettings_EmptyAccountsSkipsQuery(t *testing.T) {
	// A nil collection proves no query is attempted on the empty-accounts path.
	s := newMongoUserSettings(nil, 512, time.Second, 0)
	got, err := s.Snapshot(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestNewMongoUserSettings_Defaults(t *testing.T) {
	s := newMongoUserSettings(nil, 0, 0, 0)
	assert.Equal(t, 512, s.batchSize)
	assert.Equal(t, 2*time.Second, s.timeout)
}

// stubSettingsFetch is a settingsChunkFetcher double: it records each chunk and
// can hold a chunk open long enough to prove chunks no longer share one budget.
type stubSettingsFetch struct {
	mu          sync.Mutex
	chunks      [][]string
	hold        time.Duration
	holdFirst   bool
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
	err         error
}

func (s *stubSettingsFetch) fetch(ctx context.Context, chunk []string) (map[string]notifSettings, error) {
	n := s.inFlight.Add(1)
	for {
		peak := s.maxInFlight.Load()
		if n <= peak || s.maxInFlight.CompareAndSwap(peak, n) {
			break
		}
	}
	defer s.inFlight.Add(-1)

	s.mu.Lock()
	first := len(s.chunks) == 0
	s.chunks = append(s.chunks, append([]string(nil), chunk...))
	s.mu.Unlock()

	if s.hold > 0 && (!s.holdFirst || first) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(s.hold):
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	out := make(map[string]notifSettings, len(chunk))
	for _, a := range chunk {
		out[a] = notifSettings{muteAll: true}
	}
	return out, nil
}

func (s *stubSettingsFetch) chunkCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.chunks)
}

// TestMongoUserSettings_ChunksRunConcurrently: chunks must overlap rather than
// run back-to-back, so a large recipient list is not serialised.
func TestMongoUserSettings_ChunksRunConcurrently(t *testing.T) {
	accounts := make([]string, 1000)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("u%04d", i)
	}
	stub := &stubSettingsFetch{hold: 15 * time.Millisecond}

	s := newMongoUserSettings(nil, 100, time.Second, 4)
	s.fetch = stub.fetch

	got, err := s.Snapshot(context.Background(), accounts)
	require.NoError(t, err)
	assert.Len(t, got, len(accounts), "every chunk must be merged into the snapshot")
	assert.Equal(t, 10, stub.chunkCount())
	assert.LessOrEqual(t, stub.maxInFlight.Load(), int64(4), "chunk fan-out must respect the concurrency limit")
	assert.Greater(t, stub.maxInFlight.Load(), int64(1), "chunks must actually overlap")
}

// TestMongoUserSettings_SlowChunkDoesNotStarveOthers is the regression pin for
// the shared-budget bug: one chunk exceeding the timeout must fail open alone,
// leaving every other chunk's settings intact. Under the old shared-deadline
// loop the remaining chunks were silently dropped and those users were pushed
// regardless of muteAllNotifications.
func TestMongoUserSettings_SlowChunkDoesNotStarveOthers(t *testing.T) {
	accounts := make([]string, 500)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("u%04d", i)
	}
	// Only the first chunk stalls past the per-chunk timeout.
	stub := &stubSettingsFetch{hold: 300 * time.Millisecond, holdFirst: true}

	s := newMongoUserSettings(nil, 100, 50*time.Millisecond, 1)
	s.fetch = stub.fetch

	got, err := s.Snapshot(context.Background(), accounts)
	require.NoError(t, err, "settings snapshot is fail-open by contract")
	assert.Equal(t, 5, stub.chunkCount(), "every chunk must still be attempted")
	assert.Len(t, got, 400, "only the stalled chunk's 100 accounts fail open")
}

// TestMongoUserSettings_ChunkErrorFailsOpenPerChunk: a chunk-level error must not
// abandon the chunks behind it.
func TestMongoUserSettings_ChunkErrorFailsOpenPerChunk(t *testing.T) {
	accounts := make([]string, 300)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("u%04d", i)
	}
	stub := &stubSettingsFetch{err: errors.New("mongo: connection reset")}

	s := newMongoUserSettings(nil, 100, time.Second, 2)
	s.fetch = stub.fetch

	got, err := s.Snapshot(context.Background(), accounts)
	require.NoError(t, err)
	assert.Equal(t, 3, stub.chunkCount(), "all chunks attempted despite the first failing")
	assert.Empty(t, got)
}

func TestNewMongoUserSettings_ConcurrencyDefault(t *testing.T) {
	s := newMongoUserSettings(nil, 0, 0, 0)
	assert.Equal(t, defaultUserSettingsConcurrency, s.concurrency)
}
