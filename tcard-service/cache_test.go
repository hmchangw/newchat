package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	homeCard = card{
		Path: "home", CardVersion: "v1",
		Template: json.RawMessage(`{"_tcardVersion":"v1","title":"Home","widgets":["news","weather"]}`),
	}
	profileCard = card{
		Path: "profile", CardVersion: "v2",
		Template: json.RawMessage(`{"_tcardVersion":"v2","title":"Profile"}`),
	}
)

func TestCardCache_EmptyUntilLoaded(t *testing.T) {
	cache := newCardCache()

	assert.False(t, cache.Ready())
	_, ok := cache.Get("home", "v1")
	assert.False(t, ok)
}

func TestCardCache_Load(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard, profileCard}, nil)

	cache := newCardCache()
	n, err := cache.Load(context.Background(), store)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	assert.True(t, cache.Ready())
	got, ok := cache.Get("home", "v1")
	require.True(t, ok)
	assert.JSONEq(t, string(homeCard.Template), string(got))
	got, ok = cache.Get("profile", "v2")
	require.True(t, ok)
	assert.JSONEq(t, string(profileCard.Template), string(got))
	_, ok = cache.Get("missing", "v1")
	assert.False(t, ok)
}

func TestCardCache_LoadError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListCards(gomock.Any()).Return(nil, errors.New("mongo down"))

	cache := newCardCache()
	_, err := cache.Load(context.Background(), store)
	require.Error(t, err)
	assert.False(t, cache.Ready())
}

func TestCardCache_LoadErrorKeepsPreviousEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	gomock.InOrder(
		store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard}, nil),
		store.EXPECT().ListCards(gomock.Any()).Return(nil, errors.New("mongo down")),
	)

	cache := newCardCache()
	_, err := cache.Load(context.Background(), store)
	require.NoError(t, err)
	_, err = cache.Load(context.Background(), store)
	require.Error(t, err)

	assert.True(t, cache.Ready(), "a failed refresh must keep serving the previous data")
	_, ok := cache.Get("home", "v1")
	assert.True(t, ok)
}

// An empty cards collection is a legitimate state: the cache is ready (it holds
// the authoritative snapshot) but resolves nothing.
func TestCardCache_EmptyLoadIsReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListCards(gomock.Any()).Return([]card{}, nil)

	cache := newCardCache()
	n, err := cache.Load(context.Background(), store)
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	assert.True(t, cache.Ready(), "an empty snapshot is still a loaded snapshot")
	_, ok := cache.Get("home", "v1")
	assert.False(t, ok)
}

// Refresh syncs the cache to Mongo — a card deleted upstream disappears from
// the cache on the next refresh rather than being served forever.
func TestCardCache_RefreshDropsRemovedCards(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	gomock.InOrder(
		store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard, profileCard}, nil),
		store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard}, nil),
	)

	cache := newCardCache()
	_, err := cache.Load(context.Background(), store)
	require.NoError(t, err)
	n, err := cache.Load(context.Background(), store)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	_, ok := cache.Get("profile", "v2")
	assert.False(t, ok, "a card removed from Mongo must drop out on refresh")
	_, ok = cache.Get("home", "v1")
	assert.True(t, ok)
}

func TestCardCache_DuplicateKeySkippedKeepsRest(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	dupHome := card{Path: "home", CardVersion: "v1", Template: json.RawMessage(`{"title":"Impostor"}`)}
	// Same (path, cardVersion): the first occurrence wins; the later duplicate
	// is skipped (with a warning) rather than rejecting the whole snapshot.
	store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard, profileCard, dupHome}, nil)

	cache := newCardCache()
	n, err := cache.Load(context.Background(), store)
	require.NoError(t, err, "a duplicate row must be skipped, not reject the whole snapshot")
	assert.Equal(t, 2, n)

	got, ok := cache.Get("home", "v1")
	require.True(t, ok)
	assert.JSONEq(t, string(homeCard.Template), string(got), "the first occurrence wins")
	_, ok = cache.Get("profile", "v2")
	assert.True(t, ok, "non-duplicate rows are still published")
}

func TestCardCache_SamePathDifferentVersionsCoexist(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	homeV2 := card{Path: "home", CardVersion: "v2", Template: json.RawMessage(`{"_tcardVersion":"v2","title":"Home v2"}`)}
	store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard, homeV2}, nil)

	cache := newCardCache()
	n, err := cache.Load(context.Background(), store)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "two versions of one path are distinct entries")

	v1, ok := cache.Get("home", "v1")
	require.True(t, ok)
	assert.JSONEq(t, string(homeCard.Template), string(v1))
	v2, ok := cache.Get("home", "v2")
	require.True(t, ok)
	assert.JSONEq(t, string(homeV2.Template), string(v2))
}

func TestCardCache_Add(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard}, nil)

	cache := newCardCache()

	// Before the first load, Add is a no-op — no partial (falsely-ready) snapshot.
	cache.Add(profileCard)
	assert.False(t, cache.Ready())
	_, ok := cache.Get("profile", "v2")
	assert.False(t, ok)

	// After a load, Add makes the card servable alongside the loaded set.
	_, err := cache.Load(context.Background(), store)
	require.NoError(t, err)
	cache.Add(profileCard)
	_, ok = cache.Get("home", "v1")
	assert.True(t, ok)
	got, ok := cache.Get("profile", "v2")
	require.True(t, ok)
	assert.JSONEq(t, string(profileCard.Template), string(got))
}

// A concurrent Add and Load must not corrupt the snapshot or drop the loaded
// set (Add is CAS-retried); the race detector guards the read/write path.
func TestCardCache_AddConcurrentWithLoad(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard, profileCard}, nil).AnyTimes()

	cache := newCardCache()
	_, err := cache.Load(context.Background(), store)
	require.NoError(t, err)

	extra := card{Path: "extra", CardVersion: "1.0.0", Template: json.RawMessage(`{"_tcardVersion":"1.0.0"}`)}
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = cache.Load(context.Background(), store) }()
		go func() { defer wg.Done(); cache.Add(extra) }()
	}
	wg.Wait()

	_, ok := cache.Get("home", "v1")
	assert.True(t, ok, "the loaded set survives a concurrent Add")
	_, ok = cache.Get("profile", "v2")
	assert.True(t, ok)
}

// TestCardCache_ConcurrentReadDuringLoad guards the lock-free read path: readers
// hit Get/Ready while the writer swaps the snapshot (fails under -race if unsafe).
func TestCardCache_ConcurrentReadDuringLoad(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	store.EXPECT().ListCards(gomock.Any()).Return([]card{homeCard, profileCard}, nil).AnyTimes()

	cache := newCardCache()
	_, err := cache.Load(context.Background(), store)
	require.NoError(t, err)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					cache.Get("home", "v1")
					cache.Ready()
				}
			}
		}()
	}
	for range 50 {
		_, err := cache.Load(context.Background(), store)
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()
}

func TestParseRefreshAt(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{name: "default clock time with UTC+8 offset", raw: "08:00+08:00"},
		{name: "UTC offset", raw: "23:30+00:00"},
		{name: "negative offset", raw: "06:15-05:00"},
		{name: "missing offset", raw: "08:00", wantErr: true},
		{name: "not a clock time", raw: "8am", wantErr: true},
		{name: "empty", raw: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at, err := parseRefreshAt(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			next := nextDailyRefresh(time.Now(), at)
			assert.True(t, next.After(time.Now()), "parsed schedule must yield a future occurrence")
		})
	}
}

func TestNextDailyRefresh(t *testing.T) {
	at8utc8, err := parseRefreshAt("08:00+08:00")
	require.NoError(t, err)
	utc8 := time.FixedZone("UTC+8", 8*60*60)

	tests := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{
			name: "before today's slot fires today",
			now:  time.Date(2026, 7, 14, 6, 30, 0, 0, utc8),
			want: time.Date(2026, 7, 14, 8, 0, 0, 0, utc8),
		},
		{
			name: "exactly at the slot fires tomorrow",
			now:  time.Date(2026, 7, 14, 8, 0, 0, 0, utc8),
			want: time.Date(2026, 7, 15, 8, 0, 0, 0, utc8),
		},
		{
			name: "after today's slot fires tomorrow",
			now:  time.Date(2026, 7, 14, 20, 0, 0, 0, utc8),
			want: time.Date(2026, 7, 15, 8, 0, 0, 0, utc8),
		},
		{
			name: "now in a different zone converts into the schedule zone",
			// 23:00 UTC on Jul 13 is 07:00 UTC+8 on Jul 14 — before the slot.
			now:  time.Date(2026, 7, 13, 23, 0, 0, 0, time.UTC),
			want: time.Date(2026, 7, 14, 8, 0, 0, 0, utc8),
		},
		{
			name: "month rollover",
			now:  time.Date(2026, 7, 31, 9, 0, 0, 0, utc8),
			want: time.Date(2026, 8, 1, 8, 0, 0, 0, utc8),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nextDailyRefresh(tt.now, at8utc8)
			assert.True(t, got.Equal(tt.want), "got %s, want %s", got, tt.want)
		})
	}
}

func TestCardCache_RefreshLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockCardStore(ctrl)
	loads := make(chan struct{}, 2)
	gomock.InOrder(
		// First attempt fails — the loop must retry on the short interval.
		store.EXPECT().ListCards(gomock.Any()).DoAndReturn(func(context.Context) ([]card, error) {
			loads <- struct{}{}
			return nil, errors.New("mongo down")
		}),
		store.EXPECT().ListCards(gomock.Any()).DoAndReturn(func(context.Context) ([]card, error) {
			loads <- struct{}{}
			return []card{homeCard}, nil
		}),
	)

	at, err := parseRefreshAt("08:00+08:00")
	require.NoError(t, err)

	cache := newCardCache()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		cache.RefreshLoop(ctx, store, at, time.Millisecond)
	}()

	<-loads
	<-loads
	require.Eventually(t, cache.Ready, time.Second, time.Millisecond)
	_, ok := cache.Get("home", "v1")
	assert.True(t, ok)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RefreshLoop did not stop on context cancel")
	}
}

// listSeed is a depth-3 hierarchy with multiple versions of one card.
func listSeed() *cardCache {
	return cacheWith(
		card{Path: "a/b/c", CardVersion: "0.0.1", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/c", CardVersion: "0.0.2", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/c", CardVersion: "0.0.10", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/d", CardVersion: "1.0.0", Template: json.RawMessage(`{}`)},
		card{Path: "a/x/y", CardVersion: "2.0.0", Template: json.RawMessage(`{}`)},
		card{Path: "z/w/v", CardVersion: "1.2.3", Template: json.RawMessage(`{}`)},
	)
}

func TestCardCache_List(t *testing.T) {
	tests := []struct {
		name        string
		prefix      string
		wantCards   []string
		wantFolders []string
		wantExact   bool
		wantFound   bool
	}{
		{
			name: "root lists first segments as folders", prefix: "",
			wantCards: []string{}, wantFolders: []string{"a", "z"}, wantFound: true,
		},
		{
			name: "one segment lists two-segment folders", prefix: "a",
			wantCards: []string{}, wantFolders: []string{"a/b", "a/x"}, wantFound: true,
		},
		{
			name: "two segments list cards with every version in semver order", prefix: "a/b",
			wantCards:   []string{"a/b/c@0.0.1", "a/b/c@0.0.2", "a/b/c@0.0.10", "a/b/d@1.0.0"},
			wantFolders: []string{}, wantFound: true,
		},
		{
			name: "full card path without version is an exact hit", prefix: "a/b/c",
			wantCards: []string{}, wantFolders: []string{}, wantExact: true,
		},
		{
			name: "unknown prefix finds nothing", prefix: "nope",
			wantCards: []string{}, wantFolders: []string{},
		},
		{
			name: "prefix deeper than any card finds nothing", prefix: "a/b/c/x",
			wantCards: []string{}, wantFolders: []string{},
		},
		{
			name: "partial segment is not a prefix match", prefix: "a/b/cc",
			wantCards: []string{}, wantFolders: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := listSeed().List(tt.prefix)
			assert.Equal(t, tt.wantCards, res.cards)
			assert.Equal(t, tt.wantFolders, res.folders)
			assert.Equal(t, tt.wantExact, res.exactPath)
			assert.Equal(t, tt.wantFound, res.found)
		})
	}
}

func TestCardCache_List_EmptyAndUnloaded(t *testing.T) {
	t.Run("never-loaded cache lists nothing", func(t *testing.T) {
		res := newCardCache().List("")
		assert.Equal(t, []string{}, res.cards)
		assert.Equal(t, []string{}, res.folders)
		assert.False(t, res.found)
		assert.False(t, res.exactPath)
	})

	t.Run("loaded-but-empty cache lists nothing at root", func(t *testing.T) {
		res := cacheWith().List("")
		assert.Equal(t, []string{}, res.cards)
		assert.Equal(t, []string{}, res.folders)
		assert.False(t, res.found)
	})
}

// Non-semver versions fall back to lexicographic order within a path.
func TestCardCache_List_NonSemverVersionOrder(t *testing.T) {
	c := cacheWith(
		card{Path: "a/b/c", CardVersion: "v2", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/c", CardVersion: "v1", Template: json.RawMessage(`{}`)},
	)
	res := c.List("a/b")
	assert.Equal(t, []string{"a/b/c@v1", "a/b/c@v2"}, res.cards)
}

// Pins the depth-generic scan (spec §4) on mixed-depth data — reachable only
// via out-of-band writes: cards+folders together, and exactPath with children.
func TestCardCache_List_MixedDepth(t *testing.T) {
	c := cacheWith(
		card{Path: "a/b", CardVersion: "1.0.0", Template: json.RawMessage(`{}`)},
		card{Path: "a/b/c", CardVersion: "0.0.1", Template: json.RawMessage(`{}`)},
		card{Path: "a/x/y", CardVersion: "2.0.0", Template: json.RawMessage(`{}`)},
	)

	t.Run("one prefix yields cards and folders together", func(t *testing.T) {
		res := c.List("a")
		assert.Equal(t, []string{"a/b@1.0.0"}, res.cards)
		assert.Equal(t, []string{"a/b", "a/x"}, res.folders)
		assert.False(t, res.exactPath)
		assert.True(t, res.found)
	})

	t.Run("exact card path with deeper children reports both", func(t *testing.T) {
		res := c.List("a/b")
		assert.True(t, res.exactPath, "a/b is itself a cached card path")
		assert.Equal(t, []string{"a/b/c@0.0.1"}, res.cards, "children are still scanned")
		assert.True(t, res.found)
	})
}
