package msgpagecache

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// --- test doubles ---------------------------------------------------------

// fakeValkey is an in-memory valkeyutil.Client with per-op error hooks and a
// call log. IncrEx mirrors Redis INCR (parse-increment-store) so the gen key
// read by Get reflects prior Bumps.
type fakeValkey struct {
	mu      sync.Mutex
	data    map[string]string
	gets    []string
	sets    []string
	dels    []string
	getErr  error
	setErr  error
	incrErr error
}

func newFakeValkey() *fakeValkey { return &fakeValkey{data: map[string]string{}} }

func (f *fakeValkey) Get(_ context.Context, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets = append(f.gets, key)
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.data[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	return v, nil
}

func (f *fakeValkey) Set(_ context.Context, key, value string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets = append(f.sets, key)
	if f.setErr != nil {
		return f.setErr
	}
	f.data[key] = value
	return nil
}

func (f *fakeValkey) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.data[key]; ok {
		return false, nil
	}
	f.data[key] = value
	return true, nil
}

func (f *fakeValkey) IncrEx(_ context.Context, key string, _ time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.incrErr != nil {
		return 0, f.incrErr
	}
	n, _ := strconv.ParseInt(f.data[key], 10, 64)
	n++
	f.data[key] = strconv.FormatInt(n, 10)
	return n, nil
}

func (f *fakeValkey) Del(_ context.Context, keys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dels = append(f.dels, keys...)
	for _, k := range keys {
		delete(f.data, k)
	}
	return nil
}

func (f *fakeValkey) Close() error { return nil }

func (f *fakeValkey) getCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.gets) }
func (f *fakeValkey) setCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.sets) }

// stubReader implements service.MessageReader, recording per-method call counts
// and returning a fresh page carrying s.msg on every page read.
type stubReader struct {
	mu        sync.Mutex
	calls     map[string]int
	msg       string
	reactions models.Reactions // optional; exercises the gob codec on the struct-keyed map
	err       error
}

func newStubReader(msg string) *stubReader {
	return &stubReader{calls: map[string]int{}, msg: msg}
}

func (s *stubReader) rec(m string) { s.mu.Lock(); s.calls[m]++; s.mu.Unlock() }
func (s *stubReader) count(m string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[m]
}

func (s *stubReader) page() cassrepo.Page[models.Message] {
	return cassrepo.Page[models.Message]{
		Data: []models.Message{{
			RoomID:    "r1",
			MessageID: "m1",
			Msg:       s.msg,
			CreatedAt: time.Unix(0, 0).UTC(),
			Reactions: s.reactions,
		}},
		HasNext: false,
	}
}

func (s *stubReader) GetMessagesBefore(_ context.Context, _ string, _, _ time.Time, _ cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
	s.rec("before")
	if s.err != nil {
		return cassrepo.Page[models.Message]{}, s.err
	}
	return s.page(), nil
}

func (s *stubReader) GetMessagesBetweenDesc(_ context.Context, _ string, _, _ time.Time, _ cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
	s.rec("between")
	if s.err != nil {
		return cassrepo.Page[models.Message]{}, s.err
	}
	return s.page(), nil
}

func (s *stubReader) GetMessagesAfter(_ context.Context, _ string, _, _ time.Time, _ cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
	s.rec("after")
	return s.page(), nil
}

func (s *stubReader) GetAllMessagesAsc(_ context.Context, _ string, _, _ time.Time, _ cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
	s.rec("allasc")
	return s.page(), nil
}

func (s *stubReader) GetMessageByID(_ context.Context, _ string) (*models.Message, error) {
	s.rec("byid")
	m := s.page().Data[0]
	return &m, nil
}

func (s *stubReader) GetThreadMessages(_ context.Context, _ string, _, _ time.Time, _ cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
	s.rec("thread")
	return s.page(), nil
}

func (s *stubReader) GetMessagesByIDs(_ context.Context, _ []string) ([]models.Message, error) {
	s.rec("byids")
	return s.page().Data, nil
}

func (s *stubReader) GetPinnedMessages(_ context.Context, _ string, _ cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
	s.rec("pinned")
	return s.page(), nil
}

func (s *stubReader) GetAllPinnedMessages(_ context.Context, _ string) ([]models.Message, error) {
	s.rec("allpinned")
	return s.page().Data, nil
}

// --- fixtures -------------------------------------------------------------

const testWindow = 72 * time.Hour

var fixedNow = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

func testSizer() msgbucket.Sizer { return msgbucket.New(testWindow) }

// pinClock overrides the package now() for a test and restores it on cleanup.
func pinClock(t *testing.T) {
	t.Helper()
	prev := now
	now = func() time.Time { return fixedNow }
	t.Cleanup(func() { now = prev })
}

func newTestReader(t *testing.T, inner *stubReader, fv valkeyutil.Client) *Reader {
	t.Helper()
	r, err := NewReader(inner, fv, testSizer(), 1000, time.Minute, time.Second)
	require.NoError(t, err)
	return r
}

// sealedBefore is more than one window back, so its bucket is strictly older
// than the current bucket → cacheable.
func sealedBefore() time.Time { return fixedNow.Add(-100 * time.Hour) }

// deepFloor is far below any before anchor used in the tests.
func deepFloor() time.Time { return fixedNow.Add(-1000 * time.Hour) }

func pageReq() cassrepo.PageRequest { return cassrepo.PageRequest{PageSize: 20} }

// --- tests ----------------------------------------------------------------

func TestKeys_Format(t *testing.T) {
	assert.Equal(t, "hist:{r1}:gen", genKey("r1"))
	k := pageKey("r1", 7, "before", "b=100:f=200", 20)
	assert.True(t, strings.HasPrefix(k, "hist:{r1}:pg:7:before:"), "got %q", k)
	assert.Contains(t, k, "b=100:f=200")
	assert.True(t, strings.HasSuffix(k, ":20"), "got %q", k)
}

func TestNewReader_InvalidArgs(t *testing.T) {
	_, err := NewReader(newStubReader("x"), newFakeValkey(), testSizer(), 0, time.Minute, time.Second)
	require.Error(t, err)
	_, err = NewReader(newStubReader("x"), newFakeValkey(), testSizer(), 10, 0, time.Second)
	require.Error(t, err)
	_, err = NewReader(newStubReader("x"), newFakeValkey(), testSizer(), 10, time.Minute, 0)
	require.Error(t, err)
}

func TestReader_HotBucket_BypassesCache(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	fv := newFakeValkey()
	r := newTestReader(t, inner, fv)

	// before == now → current (hot) bucket → never cached.
	got, err := r.GetMessagesBefore(context.Background(), "r1", fixedNow, deepFloor(), pageReq())
	require.NoError(t, err)
	require.Len(t, got.Data, 1)
	assert.Equal(t, 1, inner.count("before"))
	assert.Equal(t, 0, fv.getCount(), "hot read must not touch Valkey")
	assert.Equal(t, 0, fv.setCount(), "hot read must not touch Valkey")
}

func TestReader_Sealed_MissThenHit_CopySafe(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	fv := newFakeValkey()
	r := newTestReader(t, inner, fv)
	ctx := context.Background()

	got1, err := r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Len(t, got1.Data, 1)
	assert.Equal(t, "orig", got1.Data[0].Msg)
	assert.Equal(t, 1, inner.count("before"), "first call is a miss → loads once")

	// Simulate the service layer mutating the returned page in place.
	got1.Data[0].Msg = "MUTATED"

	got2, err := r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Len(t, got2.Data, 1)
	assert.Equal(t, "orig", got2.Data[0].Msg, "cache hit must be pristine, not corrupted by prior caller")
	assert.Equal(t, 1, inner.count("before"), "second call is a hit → no reload")
}

func TestReader_L2Hit_WhenL1Cold(t *testing.T) {
	pinClock(t)
	fv := newFakeValkey() // shared L2 across two replicas
	ctx := context.Background()

	// Replica 1 populates L2.
	inner1 := newStubReader("orig")
	r1 := newTestReader(t, inner1, fv)
	_, err := r1.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Equal(t, 1, inner1.count("before"))

	// Replica 2 has a cold L1 but shares L2 → serves without hitting Cassandra.
	inner2 := newStubReader("different")
	r2 := newTestReader(t, inner2, fv)
	got, err := r2.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Len(t, got.Data, 1)
	assert.Equal(t, "orig", got.Data[0].Msg, "served from shared L2")
	assert.Equal(t, 0, inner2.count("before"), "L2 hit must not call the backing reader")
}

func TestReader_Bump_InvalidatesRoom(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	fv := newFakeValkey()
	r := newTestReader(t, inner, fv)
	ctx := context.Background()

	_, err := r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Equal(t, 1, inner.count("before"))

	r.Bump(ctx, "r1")

	_, err = r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	assert.Equal(t, 2, inner.count("before"), "after bump the room's cached pages are orphaned → reload")
}

func TestReader_Bump_OtherRoomUnaffected(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	fv := newFakeValkey()
	r := newTestReader(t, inner, fv)
	ctx := context.Background()

	_, err := r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Equal(t, 1, inner.count("before"))

	r.Bump(ctx, "other-room")

	_, err = r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	assert.Equal(t, 1, inner.count("before"), "bumping a different room must not invalidate r1")
}

func TestReader_FailOpen_GetError(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	fv := newFakeValkey()
	fv.getErr = errors.New("valkey down")
	r := newTestReader(t, inner, fv)

	got, err := r.GetMessagesBefore(context.Background(), "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err, "a Valkey GET error must degrade to the live read, not fail")
	require.Len(t, got.Data, 1)
	assert.Equal(t, "orig", got.Data[0].Msg)
	assert.Equal(t, 1, inner.count("before"))
}

func TestReader_FailOpen_SetError(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	fv := newFakeValkey()
	fv.setErr = errors.New("valkey down")
	r := newTestReader(t, inner, fv)
	ctx := context.Background()

	// A failed L2 write must be swallowed (fail-open): the read still succeeds.
	got, err := r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Len(t, got.Data, 1)
	assert.Equal(t, "orig", got.Data[0].Msg)
	require.Equal(t, 1, inner.count("before"))

	// L1 is an independent tier, so it still absorbs the repeat read even though
	// L2 is down — a second call is served from L1, not reloaded.
	_, err = r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	assert.Equal(t, 1, inner.count("before"))
}

func TestReader_LoadError_NotCached(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	inner.err = errors.New("cassandra boom")
	fv := newFakeValkey()
	r := newTestReader(t, inner, fv)

	_, err := r.GetMessagesBefore(context.Background(), "r1", sealedBefore(), deepFloor(), pageReq())
	require.Error(t, err)
	assert.Equal(t, 0, fv.setCount(), "a load error must not populate the cache")
}

func TestReader_BetweenDesc_AccessSinceInKey(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	fv := newFakeValkey()
	r := newTestReader(t, inner, fv)
	ctx := context.Background()
	since1 := fixedNow.Add(-200 * time.Hour)
	since2 := fixedNow.Add(-300 * time.Hour)
	before := sealedBefore()

	_, err := r.GetMessagesBetweenDesc(ctx, "r1", since1, before, pageReq())
	require.NoError(t, err)
	assert.Equal(t, 1, inner.count("between"))

	// Different accessSince → distinct key → miss.
	_, err = r.GetMessagesBetweenDesc(ctx, "r1", since2, before, pageReq())
	require.NoError(t, err)
	assert.Equal(t, 2, inner.count("between"))

	// Repeat of the first window → hit.
	_, err = r.GetMessagesBetweenDesc(ctx, "r1", since1, before, pageReq())
	require.NoError(t, err)
	assert.Equal(t, 2, inner.count("between"))
}

func TestReader_FloorQuantizedToBucket(t *testing.T) {
	pinClock(t)
	sizer := testSizer()
	floorA := deepFloor()
	floorSameBucket := floorA.Add(1 * time.Hour)
	floorOtherBucket := floorA.Add(-100 * time.Hour)
	// Preconditions for a meaningful test.
	require.Equal(t, sizer.Of(floorA), sizer.Of(floorSameBucket), "floorA and +1h must share a bucket")
	require.NotEqual(t, sizer.Of(floorA), sizer.Of(floorOtherBucket), "the third floor must be in another bucket")

	inner := newStubReader("orig")
	fv := newFakeValkey()
	r := newTestReader(t, inner, fv)
	ctx := context.Background()
	before := sealedBefore()

	_, err := r.GetMessagesBefore(ctx, "r1", before, floorA, pageReq())
	require.NoError(t, err)
	assert.Equal(t, 1, inner.count("before"))

	// Floor in the same bucket → same key → hit.
	_, err = r.GetMessagesBefore(ctx, "r1", before, floorSameBucket, pageReq())
	require.NoError(t, err)
	assert.Equal(t, 1, inner.count("before"), "floors in the same bucket must share a cache entry")

	// Floor in a different bucket → distinct key → miss.
	_, err = r.GetMessagesBefore(ctx, "r1", before, floorOtherBucket, pageReq())
	require.NoError(t, err)
	assert.Equal(t, 2, inner.count("before"))
}

func TestReader_PassThrough_NeverCaches(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	fv := newFakeValkey()
	r := newTestReader(t, inner, fv)
	ctx := context.Background()

	_, err := r.GetMessageByID(ctx, "m1")
	require.NoError(t, err)
	_, err = r.GetMessageByID(ctx, "m1")
	require.NoError(t, err)
	assert.Equal(t, 2, inner.count("byid"), "point reads are pass-through, never cached")
	assert.Equal(t, 0, fv.getCount())
	assert.Equal(t, 0, fv.setCount())

	// The cursor-bearing forward path is not cached in Phase 1.
	_, err = r.GetMessagesAfter(ctx, "r1", sealedBefore(), fixedNow, pageReq())
	require.NoError(t, err)
	_, err = r.GetMessagesAfter(ctx, "r1", sealedBefore(), fixedNow, pageReq())
	require.NoError(t, err)
	assert.Equal(t, 2, inner.count("after"), "GetMessagesAfter is pass-through in Phase 1")
}

func TestReader_BetweenDesc_HotBucket_Bypasses(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	fv := newFakeValkey()
	r := newTestReader(t, inner, fv)

	// before in the current bucket → never cached.
	_, err := r.GetMessagesBetweenDesc(context.Background(), "r1", fixedNow.Add(-200*time.Hour), fixedNow, pageReq())
	require.NoError(t, err)
	assert.Equal(t, 1, inner.count("between"))
	assert.Equal(t, 0, fv.getCount())
	assert.Equal(t, 0, fv.setCount())
}

func TestReader_Bump_FailOpen(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	fv := newFakeValkey()
	fv.incrErr = errors.New("valkey down")
	r := newTestReader(t, inner, fv)
	ctx := context.Background()

	_, err := r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Equal(t, 1, inner.count("before"))

	// A failed bump must be swallowed and must not invalidate (gen unchanged).
	assert.NotPanics(t, func() { r.Bump(ctx, "r1") })
	_, err = r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	assert.Equal(t, 1, inner.count("before"), "a failed bump leaves the cache intact (TTL reconciles)")
}

func TestReader_Gen_NonNumeric_TreatedAsZero(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	fv := newFakeValkey()
	fv.data[genKey("r1")] = "not-a-number"
	r := newTestReader(t, inner, fv)
	ctx := context.Background()

	// A corrupt gen must degrade to 0, not error, so the read still succeeds.
	got, err := r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Len(t, got.Data, 1)
	assert.Equal(t, 1, inner.count("before"))
}

func TestReader_NilValkey_Delegates(t *testing.T) {
	pinClock(t)
	inner := newStubReader("orig")
	r, err := NewReader(inner, nil, testSizer(), 1000, time.Minute, time.Second)
	require.NoError(t, err)
	ctx := context.Background()

	got, err := r.GetMessagesBefore(ctx, "r1", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Len(t, got.Data, 1)
	assert.Equal(t, 1, inner.count("before"))

	assert.NotPanics(t, func() { r.Bump(ctx, "r1") })
}
