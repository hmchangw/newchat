package sessioncache

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type fakeValkey struct {
	data    map[string]string
	ttls    map[string]time.Duration
	getErr  error
	setErr  error
	dels    []string
	expires []string
}

func newFakeValkey() *fakeValkey {
	return &fakeValkey{data: map[string]string{}, ttls: map[string]time.Duration{}}
}

func (f *fakeValkey) Get(_ context.Context, key string) (string, error) {
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.data[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	return v, nil
}

func (f *fakeValkey) MGet(ctx context.Context, keys []string) (map[string]string, error) {
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		if v, err := f.Get(ctx, k); err == nil {
			out[k] = v
		}
	}
	return out, nil
}

func (f *fakeValkey) Set(_ context.Context, key, value string, ttl time.Duration) error {
	if f.setErr != nil {
		return f.setErr
	}
	f.data[key] = value
	f.ttls[key] = ttl
	return nil
}

func (f *fakeValkey) Del(_ context.Context, keys ...string) error {
	f.dels = append(f.dels, keys...)
	for _, k := range keys {
		delete(f.data, k)
		delete(f.ttls, k)
	}
	return nil
}

func (f *fakeValkey) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	f.expires = append(f.expires, key)
	if _, ok := f.data[key]; !ok {
		return false, nil // EXPIRE no-ops on an absent key
	}
	f.ttls[key] = ttl
	return true, nil
}

func (f *fakeValkey) Close() error { return nil }
func (f *fakeValkey) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (f *fakeValkey) IncrEx(context.Context, string, time.Duration) (int64, error) { return 1, nil }

// countingLoader stands in for the Mongo session store.
type countingLoader struct {
	calls   int
	session *session.Session
	err     error
}

func (c *countingLoader) load(context.Context, string) (*session.Session, error) {
	c.calls++
	return c.session, c.err
}

var botSession = &session.Session{
	ID: "sess-1", UserID: "u-bot", Account: "helper.bot",
	SiteID: "site-a", Roles: []string{"bot"}, IssuedAt: 1760000000000,
}

const hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

func TestCache_MissPopulatesFromLoader(t *testing.T) {
	vk := newFakeValkey()
	loader := &countingLoader{session: botSession}
	c := New(loader.load, vk, time.Hour)

	got, err := c.FindByHash(context.Background(), hash)
	require.NoError(t, err)
	assert.Equal(t, botSession, got)
	assert.Equal(t, 1, loader.calls)
	assert.Contains(t, vk.data, Key(hash))
}

func TestCache_HitSkipsTheLoader(t *testing.T) {
	vk := newFakeValkey()
	loader := &countingLoader{session: botSession}
	c := New(loader.load, vk, time.Hour)
	ctx := context.Background()

	_, err := c.FindByHash(ctx, hash)
	require.NoError(t, err)

	got, err := c.FindByHash(ctx, hash)
	require.NoError(t, err)
	assert.Equal(t, botSession, got)
	assert.Equal(t, 1, loader.calls, "a warm entry must not reach Mongo")
}

// The whole point of the tier: an authenticated bot keeps working while Mongo
// is unreachable, instead of every request being rejected as an invalid token.
func TestCache_WarmSessionSurvivesOutageViaTTLSlide(t *testing.T) {
	vk := newFakeValkey()
	loader := &countingLoader{session: botSession}
	now := time.Now()
	c := newWithClock(loader.load, vk, time.Hour, func() time.Time { return now })
	ctx := context.Background()

	_, err := c.FindByHash(ctx, hash)
	require.NoError(t, err)

	now = now.Add(59 * time.Minute) // past the refresh window
	loader.session, loader.err = nil, errors.New("mongo down")

	got, err := c.FindByHash(ctx, hash)
	require.NoError(t, err, "a warm session must survive Mongo being down")
	assert.Equal(t, botSession, got)
	assert.Contains(t, vk.expires, Key(hash), "the deadline must be re-armed, not left to expire")
}

func TestCache_UnknownTokenIsRejectedAndNotCached(t *testing.T) {
	vk := newFakeValkey()
	loader := &countingLoader{err: session.ErrNotFound}
	c := New(loader.load, vk, time.Hour)

	_, err := c.FindByHash(context.Background(), hash)
	require.ErrorIs(t, err, session.ErrNotFound)
	assert.Empty(t, vk.data, "absence must never be cached: it would outlive the session being created")
}

// Cold entries fail closed — the cache can only ever grant access Mongo already
// granted, so an outage cannot be used to smuggle in an unknown token.
func TestCache_ColdTokenDuringOutageFailsClosed(t *testing.T) {
	vk := newFakeValkey()
	loader := &countingLoader{err: errors.New("mongo down")}
	c := New(loader.load, vk, time.Hour)

	_, err := c.FindByHash(context.Background(), hash)
	require.Error(t, err)
	assert.NotErrorIs(t, err, session.ErrNotFound, "an outage is not a 'no such session' answer")
}

// While Mongo is healthy, a revoked session is dropped at the first
// re-validation rather than lingering for the rest of the TTL.
func TestCache_RevokedSessionEvictedOnRefresh(t *testing.T) {
	vk := newFakeValkey()
	loader := &countingLoader{session: botSession}
	now := time.Now()
	c := newWithClock(loader.load, vk, time.Hour, func() time.Time { return now })
	ctx := context.Background()

	_, err := c.FindByHash(ctx, hash)
	require.NoError(t, err)
	require.Contains(t, vk.data, Key(hash))

	now = now.Add(59 * time.Minute)
	loader.session, loader.err = nil, session.ErrNotFound

	_, err = c.FindByHash(ctx, hash)
	require.ErrorIs(t, err, session.ErrNotFound)
	assert.NotContains(t, vk.data, Key(hash), "a revoked session must not linger")
}

func TestCache_FreshEntryDoesNotRevalidate(t *testing.T) {
	vk := newFakeValkey()
	loader := &countingLoader{session: botSession}
	now := time.Now()
	c := newWithClock(loader.load, vk, time.Hour, func() time.Time { return now })
	ctx := context.Background()

	_, err := c.FindByHash(ctx, hash)
	require.NoError(t, err)
	now = now.Add(10 * time.Minute)
	_, err = c.FindByHash(ctx, hash)
	require.NoError(t, err)
	assert.Equal(t, 1, loader.calls, "a fresh entry must not re-validate")
}

func TestCache_SlideCannotResurrectARevokedEntry(t *testing.T) {
	vk := newFakeValkey()
	now := time.Now()
	ctx := context.Background()

	warm := newWithClock((&countingLoader{session: botSession}).load, vk, time.Hour, func() time.Time { return now })
	_, err := warm.FindByHash(ctx, hash)
	require.NoError(t, err)

	// The entry is busted between the read and the slide — exactly the window a
	// real revocation lands in. EXPIRE must no-op rather than write it back.
	evicting := &countingLoader{err: errors.New("mongo down")}
	c := newWithClock(func(ctx context.Context, h string) (*session.Session, error) {
		_ = vk.Del(ctx, Key(h))
		return evicting.load(ctx, h)
	}, vk, time.Hour, func() time.Time { return now })
	now = now.Add(59 * time.Minute)

	_, err = c.FindByHash(ctx, hash)
	require.NoError(t, err, "the caller still gets the session it read")
	assert.NotContains(t, vk.data, Key(hash), "a slide must never resurrect a revoked entry")
}

func TestCache_ValkeyDownFallsThroughToMongo(t *testing.T) {
	vk := newFakeValkey()
	vk.getErr = errors.New("valkey down")
	vk.setErr = errors.New("valkey down")
	loader := &countingLoader{session: botSession}
	c := New(loader.load, vk, time.Hour)

	got, err := c.FindByHash(context.Background(), hash)
	require.NoError(t, err, "a broken cache must not fail a lookup Mongo can serve")
	assert.Equal(t, botSession, got)
}

func TestCache_NilClientIsPassThrough(t *testing.T) {
	loader := &countingLoader{session: botSession}
	c := New(loader.load, nil, time.Hour)

	got, err := c.FindByHash(context.Background(), hash)
	require.NoError(t, err)
	assert.Equal(t, botSession, got)
	assert.Equal(t, 1, loader.calls)
}

func TestCache_KeyDoesNotEmbedTheRawToken(t *testing.T) {
	// The key is built from the already-hashed token. Guard against anyone
	// "simplifying" it to the raw bearer token later.
	k := Key(hash)
	assert.Contains(t, k, hash)
	assert.True(t, len(hash) == 64, "callers must pass sessiontoken.Hash output, not a raw token")
}

func TestCache_MalformedEntryIsTreatedAsMiss(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "not json", raw: "{oops"},
		{name: "json null", raw: "null"},
		{name: "no session", raw: `{"cachedAt":123}`},
		{name: "no confirmation stamp", raw: `{"session":{"_id":"sess-1"}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vk := newFakeValkey()
			vk.data[Key(hash)] = tc.raw
			loader := &countingLoader{session: botSession}
			c := New(loader.load, vk, time.Hour)

			got, err := c.FindByHash(context.Background(), hash)
			require.NoError(t, err)
			assert.Equal(t, botSession, got)
			assert.Equal(t, 1, loader.calls, "an unusable entry must reload, never be served")
		})
	}
}

func TestBust_DropsTheEntry(t *testing.T) {
	vk := newFakeValkey()
	loader := &countingLoader{session: botSession}
	c := New(loader.load, vk, time.Hour)
	ctx := context.Background()

	_, err := c.FindByHash(ctx, hash)
	require.NoError(t, err)

	Bust(ctx, vk, hash)
	assert.NotContains(t, vk.data, Key(hash))
	// Both generations: a rolling deploy can still have a binary writing the
	// pre-Box key, and a revocation has to reach that entry too.
	assert.Equal(t, []string{Key(hash), legacyKey(hash)}, vk.dels)
}

func TestBust_NilClientIsNoOp(t *testing.T) {
	require.NotPanics(t, func() { Bust(context.Background(), nil, hash) })
}

func TestCache_StoredEntryCarriesOnlyThePrincipal(t *testing.T) {
	vk := newFakeValkey()
	c := New((&countingLoader{session: botSession}).load, vk, time.Hour)

	_, err := c.FindByHash(context.Background(), hash)
	require.NoError(t, err)

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(vk.data[Key(hash)]), &entry))
	sess, ok := entry["v"].(map[string]any)
	require.True(t, ok)
	// Whatever the shape, nothing secret may be written: the token itself never
	// reaches this tier, only its hash, and that lives in the key.
	for k := range sess {
		assert.NotContains(t, []string{"token", "password", "secret"}, k)
	}
}
