package valkeyutil

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// thing is a stand-in for a cached value. The tier wraps it in a Box; the tests
// assert on that wire form directly.
type thing struct {
	Payload string `json:"payload"`
}

func thingValid(t *thing) bool { return t.Payload != "" }

// tierFake is an in-memory Client recording enough to assert on which arm of
// the state machine ran: a SET is a rewrite, an EXPIRE is a slide, a DEL is an
// eviction.
type tierFake struct {
	Client
	store    map[string]string
	getErr   error
	setCalls int
	expCalls int
	delCalls int
}

func newTierFake() *tierFake { return &tierFake{store: map[string]string{}} }

func (f *tierFake) Get(_ context.Context, key string) (string, error) {
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.store[key]
	if !ok {
		return "", ErrCacheMiss
	}
	return v, nil
}

func (f *tierFake) Set(_ context.Context, key, value string, _ time.Duration) error {
	f.setCalls++
	f.store[key] = value
	return nil
}

func (f *tierFake) Expire(_ context.Context, key string, _ time.Duration) (bool, error) {
	f.expCalls++
	_, ok := f.store[key]
	return ok, nil
}

func (f *tierFake) Del(_ context.Context, keys ...string) error {
	f.delCalls++
	for _, k := range keys {
		delete(f.store, k)
	}
	return nil
}

// seed writes an envelope stamped at age before now, bypassing the tier so a
// test can place an entry precisely on either side of the refresh window.
func (f *tierFake) seed(t *testing.T, key, payload string, stampedAt time.Time) {
	t.Helper()
	raw, err := json.Marshal(Box[thing]{V: thing{Payload: payload}, CachedAt: stampedAt.UnixMilli()})
	require.NoError(t, err)
	f.store[key] = string(raw)
}

// storedBox decodes what the tier last wrote, so a test can assert on the stamp.
func (f *tierFake) storedBox(t *testing.T, key string) Box[thing] {
	t.Helper()
	var b Box[thing]
	require.NoError(t, json.Unmarshal([]byte(f.store[key]), &b))
	return b
}

const tierTTL = 90 * time.Minute

// fixedNow anchors every case: an entry stamped at now is fresh, one stamped
// beyond RefreshAfter(tierTTL) is due for re-validation but still serveable.
var fixedNow = time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

type loadResult struct {
	value thing
	found bool
	err   error
}

// newTestTier wires a Tier over fake with a loader that replays results in
// order, so a test can assert both what was returned and how many times the
// source of truth was consulted.
func newTestTier(fake Client, results ...loadResult) (Tier[string, thing], *int) {
	calls := 0
	tr := NewTierWithClock(TierConfig[string, thing]{
		Client: fake,
		TTL:    tierTTL,
		Label:  "thing",
		Key:    func(id string) string { return "thing:" + id },
		Load: func(_ context.Context, _ string) (thing, bool, error) {
			r := results[calls]
			calls++
			return r.value, r.found, r.err
		},
		Valid: thingValid,
	}, func() time.Time { return fixedNow })
	return tr, &calls
}

func TestTier_FreshHitIsServedWithoutTouchingTheSource(t *testing.T) {
	fake := newTierFake()
	fake.seed(t, "thing:a", "cached", fixedNow.Add(-time.Minute))
	tr, calls := newTestTier(fake)

	got, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "cached", got.Payload)
	assert.Zero(t, *calls, "a fresh entry must not consult the source of truth")
	assert.Zero(t, fake.setCalls, "a fresh serve is a pure read")
	assert.Zero(t, fake.expCalls, "a fresh serve does not slide by default")
}

// The refresh window is what makes outage survival possible: an entry past it
// is re-validated while it is still serveable, so a failure has something left
// to extend.
func TestTier_StaleHitRevalidatesAndRewrites(t *testing.T) {
	fake := newTierFake()
	fake.seed(t, "thing:a", "old", fixedNow.Add(-RefreshAfter(tierTTL)-time.Minute))
	tr, calls := newTestTier(fake, loadResult{value: thing{Payload: "new"}, found: true})

	got, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "new", got.Payload)
	assert.Equal(t, 1, *calls)
	assert.Equal(t, 1, fake.setCalls, "a confirmed refresh rewrites the entry")

	// The rewrite must restamp, or the entry stays permanently stale and every
	// later read pays a source round-trip.
	assert.Equal(t, fixedNow.UnixMilli(), fake.storedBox(t, "thing:a").CachedAt)
}

// The branch the whole tier exists for.
func TestTier_StaleHitSurvivesAnUnreachableSource(t *testing.T) {
	fake := newTierFake()
	fake.seed(t, "thing:a", "cached", fixedNow.Add(-RefreshAfter(tierTTL)-time.Minute))
	tr, _ := newTestTier(fake, loadResult{err: errors.New("mongo down")})

	got, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err, "an outage must not surface as a failed lookup")
	assert.True(t, found)
	assert.Equal(t, "cached", got.Payload)
	assert.Equal(t, 1, fake.expCalls, "the deadline is re-armed")
	assert.Zero(t, fake.setCalls, "EXPIRE, never SET — a busted entry must stay busted")
}

// A confirmed absence is a decision, not a failure: evict, so a missed
// invalidation stops mattering now rather than at the TTL.
func TestTier_StaleHitEvictsOnConfirmedAbsence(t *testing.T) {
	fake := newTierFake()
	fake.seed(t, "thing:a", "cached", fixedNow.Add(-RefreshAfter(tierTTL)-time.Minute))
	tr, _ := newTestTier(fake, loadResult{found: false})

	_, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, 1, fake.delCalls)
	assert.NotContains(t, fake.store, "thing:a")
}

// KeepOnAbsent inverts that for a tier whose rows are never deleted, where
// honoring an absence would do more damage than serving a stale value.
func TestTier_KeepOnAbsentRestampsInsteadOfEvicting(t *testing.T) {
	fake := newTierFake()
	fake.seed(t, "thing:a", "cached", fixedNow.Add(-RefreshAfter(tierTTL)-time.Minute))
	tr := NewTierWithClock(TierConfig[string, thing]{
		Client: fake, TTL: tierTTL, Label: "thing",
		Key:          func(id string) string { return "thing:" + id },
		Load:         func(context.Context, string) (thing, bool, error) { return thing{}, false, nil },
		Valid:        thingValid,
		KeepOnAbsent: true,
	}, func() time.Time { return fixedNow })

	got, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "cached", got.Payload)
	assert.Zero(t, fake.delCalls, "the entry must survive")
	assert.Equal(t, 1, fake.setCalls, "restamped, so the retry is once per window")

	stored := fake.storedBox(t, "thing:a")
	assert.Equal(t, fixedNow.UnixMilli(), stored.CachedAt)
	assert.Equal(t, "cached", stored.V.Payload, "the cached payload is kept, not replaced")
}

// SlideOnFresh is for a tier whose L1 TTL outlives the refresh window, where
// every L2 hit lands before the window and the entry would otherwise expire
// without ever being re-armed.
func TestTier_SlideOnFreshRearmsWithoutRestamping(t *testing.T) {
	fake := newTierFake()
	stampedAt := fixedNow.Add(-time.Minute)
	fake.seed(t, "thing:a", "cached", stampedAt)
	tr := NewTierWithClock(TierConfig[string, thing]{
		Client: fake, TTL: tierTTL, Label: "thing",
		Key: func(id string) string { return "thing:" + id },
		Load: func(context.Context, string) (thing, bool, error) {
			return thing{}, false, errors.New("must not be called")
		},
		Valid:        thingValid,
		SlideOnFresh: true,
	}, func() time.Time { return fixedNow })

	got, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "cached", got.Payload)
	assert.Equal(t, 1, fake.expCalls, "the eviction deadline moves")
	assert.Zero(t, fake.setCalls)

	// The staleness bound must NOT move: only a real re-validation may advance
	// CachedAt, or a slid entry would look freshly confirmed forever.
	assert.Equal(t, stampedAt.UnixMilli(), fake.storedBox(t, "thing:a").CachedAt)
}

func TestTier_ColdMissLoadsAndPopulates(t *testing.T) {
	fake := newTierFake()
	tr, calls := newTestTier(fake, loadResult{value: thing{Payload: "loaded"}, found: true})

	got, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "loaded", got.Payload)
	assert.Equal(t, 1, *calls)
	assert.Equal(t, 1, fake.setCalls)

	stored := fake.storedBox(t, "thing:a")
	assert.Equal(t, fixedNow.UnixMilli(), stored.CachedAt)
	assert.Equal(t, "loaded", stored.V.Payload)
}

// Positive-only: a confirmed absence is never written, so the tier can never
// grant what the source of truth did not.
func TestTier_ColdMissDoesNotCacheAnAbsence(t *testing.T) {
	fake := newTierFake()
	tr, _ := newTestTier(fake, loadResult{found: false})

	_, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err)
	assert.False(t, found)
	assert.Zero(t, fake.setCalls)
	assert.Empty(t, fake.store)
}

// A source that answers "found" with a hollow record must not be cached: the
// entry would be written once and read back as a miss forever after, costing a
// round trip per read and caching nothing.
func TestTier_UnusableLoadResultIsServedButNotCached(t *testing.T) {
	fake := newTierFake()
	tr, _ := newTestTier(fake, loadResult{value: thing{Payload: ""}, found: true})

	got, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err)
	assert.True(t, found, "the source said found; the tier does not overrule it")
	assert.Empty(t, got.Payload)
	assert.Zero(t, fake.setCalls)
	assert.Empty(t, fake.store)
}

// With nothing cached there is nothing to fail open onto, so the error is the
// answer — a cold read fails closed.
func TestTier_ColdMissSurfacesTheSourceError(t *testing.T) {
	fake := newTierFake()
	sentinel := errors.New("mongo down")
	tr, _ := newTestTier(fake, loadResult{err: sentinel})

	_, found, err := tr.Resolve(context.Background(), "a")

	require.ErrorIs(t, err, sentinel)
	assert.False(t, found)
	assert.Zero(t, fake.setCalls)
}

// An entry that decodes but fails Usable is a miss, not a hit: serving it would
// hand out a zero value for the rest of its TTL.
func TestTier_UnusableEntryReloads(t *testing.T) {
	fake := newTierFake()
	fake.store["thing:a"] = `{"v":{"payload":""},"cachedAt":` + itoa(fixedNow.UnixMilli()) + `}`
	tr, calls := newTestTier(fake, loadResult{value: thing{Payload: "loaded"}, found: true})

	got, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "loaded", got.Payload)
	assert.Equal(t, 1, *calls, "an unusable entry must not be served")
}

// An entry with no stamp is a miss for every tier, not just the ones that
// remembered to check. Before Box, each tier wrote its own predicate and two of
// four omitted this — so a pre-envelope entry was served as infinitely stale and
// reloaded from the source on every read, forever.
func TestTier_UnstampedEntryIsAMiss(t *testing.T) {
	fake := newTierFake()
	fake.store["thing:a"] = `{"v":{"payload":"cached"},"cachedAt":0}`
	tr, calls := newTestTier(fake, loadResult{value: thing{Payload: "loaded"}, found: true})

	got, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "loaded", got.Payload)
	assert.Equal(t, 1, *calls)
	assert.Equal(t, fixedNow.UnixMilli(), fake.storedBox(t, "thing:a").CachedAt, "and it is replaced, not left to reload forever")
}

// A broken cache must never fail a lookup the source of truth can serve.
func TestTier_ReadFailureFallsThroughToTheSource(t *testing.T) {
	fake := newTierFake()
	fake.getErr = errors.New("valkey down")
	tr, calls := newTestTier(fake, loadResult{value: thing{Payload: "loaded"}, found: true})

	got, found, err := tr.Resolve(context.Background(), "a")

	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "loaded", got.Payload)
	assert.Equal(t, 1, *calls)
}

// A nil client or a non-positive TTL disables the tier outright. Valkey treats
// a zero TTL as "store forever", so honoring the "0 disables" config convention
// any other way would cache with no expiry.
func TestTier_DisabledGoesStraightToTheSource(t *testing.T) {
	tests := []struct {
		name   string
		client Client
		ttl    time.Duration
	}{
		{"nil client", nil, tierTTL},
		{"zero ttl", newTierFake(), 0},
		{"negative ttl", newTierFake(), -time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			tr := NewTier(TierConfig[string, thing]{
				Client: tt.client, TTL: tt.ttl, Label: "thing",
				Key: func(id string) string { return "thing:" + id },
				Load: func(context.Context, string) (thing, bool, error) {
					calls++
					return thing{Payload: "loaded"}, true, nil
				},
				Valid: thingValid,
			})

			got, found, err := tr.Resolve(context.Background(), "a")

			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, "loaded", got.Payload)
			assert.Equal(t, 1, calls)
			if f, ok := tt.client.(*tierFake); ok {
				assert.Zero(t, f.setCalls, "a disabled tier must not write")
			}
		})
	}
}

// bust must work on a tier disabled by a zero TTL too: keys written while it was
// enabled are still out there and still have to be droppable.
func TestTier_BustDeletesAndTolerantOfDisabled(t *testing.T) {
	fake := newTierFake()
	fake.seed(t, "thing:a", "cached", fixedNow)
	tr, _ := newTestTier(fake)

	tr.bust(context.Background(), "a")
	assert.NotContains(t, fake.store, "thing:a")

	disabled := NewTier(TierConfig[string, thing]{
		Key:   func(id string) string { return "thing:" + id },
		Load:  func(context.Context, string) (thing, bool, error) { return thing{}, false, nil },
		Valid: thingValid,
	})
	require.NotPanics(t, func() { disabled.bust(context.Background(), "a") })
}

// itoa avoids pulling strconv into the test file for one call.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
