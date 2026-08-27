package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/jsiter"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// stubBatch delivers msgs and then reports err after the drain — the only
// channel through which a Fetch loop learns its consumer was deleted.
type stubBatch struct {
	msgs []jetstream.Msg
	err  error
}

func (b stubBatch) Messages() <-chan o11ynats.FetchedMessage {
	ch := make(chan o11ynats.FetchedMessage, len(b.msgs))
	for _, m := range b.msgs {
		ch <- o11ynats.FetchedMessage{Ctx: context.Background(), Msg: m}
	}
	close(ch)
	return ch
}

func (b stubBatch) Error() error { return b.err }

// stubFetcher hands out scripted fetch outcomes and counts its calls.
type stubFetcher struct {
	mu       sync.Mutex
	batches  []msgBatch
	errs     []error
	calls    int
	fallback msgBatch
}

func (f *stubFetcher) Fetch(context.Context, int, ...jetstream.FetchOpt) (msgBatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.calls
	f.calls++
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	if i < len(f.batches) {
		return f.batches[i], nil
	}
	if f.fallback != nil {
		return f.fallback, nil
	}
	return stubBatch{}, nil
}

func (f *stubFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// builder hands out fetchers in order and records how often it was asked.
type builder struct {
	mu       sync.Mutex
	fetchers []msgFetcher
	errs     []error
	calls    int
}

func (b *builder) build(context.Context) (msgFetcher, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	i := b.calls
	b.calls++
	if i < len(b.errs) && b.errs[i] != nil {
		return nil, b.errs[i]
	}
	if i < len(b.fetchers) {
		return b.fetchers[i], nil
	}
	return &stubFetcher{}, nil
}

func (b *builder) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// newTestRecoveringFetcher skips the real backoff so recovery paths run at test
// speed, while still recording the schedule that was asked for.
func newTestRecoveringFetcher(t *testing.T, b *builder) (*recoveringFetcher, *[]time.Duration) {
	t.Helper()
	r, err := newRecoveringFetcher(context.Background(), "hr", b.build, nil)
	require.NoError(t, err)
	var mu sync.Mutex
	delays := make([]time.Duration, 0, 8)
	r.sleepFn = func(_ context.Context, d time.Duration) bool {
		mu.Lock()
		delays = append(delays, d)
		mu.Unlock()
		return true
	}
	return r, &delays
}

func TestNewRecoveringFetcher_BuildFailurePropagates(t *testing.T) {
	b := &builder{errs: []error{errors.New("consumer not found")}}

	r, err := newRecoveringFetcher(context.Background(), "hr", b.build, nil)

	require.Error(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "consumer not found")
}

func TestRecoveringFetcher_Fetch_DelegatesToCurrentFetcher(t *testing.T) {
	want := stubBatch{}
	f := &stubFetcher{batches: []msgBatch{want}}
	b := &builder{fetchers: []msgFetcher{f}}
	r, _ := newTestRecoveringFetcher(t, b)

	got, err := r.Fetch(context.Background(), 10)

	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, 1, f.callCount())
	assert.True(t, r.IsUp())
}

func TestRecoveringFetcher_Recover_RebuildsOnFatalError(t *testing.T) {
	b := &builder{fetchers: []msgFetcher{&stubFetcher{}, &stubFetcher{}}}
	r, delays := newTestRecoveringFetcher(t, b)

	ok := r.Recover(context.Background(), jetstream.ErrConsumerDeleted)

	assert.True(t, ok)
	assert.Equal(t, 2, b.callCount(), "a deleted consumer must be rebuilt")
	assert.True(t, r.IsUp())
	require.Len(t, *delays, 1)
	assert.Equal(t, jsiter.RebuildBackoff[0], (*delays)[0])
}

func TestRecoveringFetcher_Recover_RetriesTransientWithoutRebuilding(t *testing.T) {
	b := &builder{fetchers: []msgFetcher{&stubFetcher{}}}
	r, delays := newTestRecoveringFetcher(t, b)

	ok := r.Recover(context.Background(), jetstream.ErrNoHeartbeat)

	assert.True(t, ok)
	assert.Equal(t, 1, b.callCount(), "a transient error must not rebuild the consumer")
	assert.True(t, r.IsUp())
	require.Len(t, *delays, 1, "a transient retry still backs off so the loop cannot spin")
}

func TestRecoveringFetcher_Recover_EscalatesRepeatedTransientsToRebuild(t *testing.T) {
	b := &builder{fetchers: []msgFetcher{&stubFetcher{}, &stubFetcher{}}}
	r, _ := newTestRecoveringFetcher(t, b)

	for range jsiter.TransientEscalation {
		assert.True(t, r.Recover(context.Background(), jetstream.ErrNoHeartbeat))
	}

	assert.Equal(t, 2, b.callCount(), "a consumer that only ever misses heartbeats is rebuilt")
}

// runConsumer always calls Fetch before Recover, and Fetch returns (batch, nil)
// even when the server-side failure is waiting in batch.Error(). Resetting the
// run on a Fetch that merely returned would make escalation unreachable, so
// only a batch that completed cleanly counts as progress.
func TestRecoveringFetcher_EscalatesAcrossTheRealLoopOrder(t *testing.T) {
	b := &builder{fetchers: []msgFetcher{&stubFetcher{}, &stubFetcher{}}}
	r, _ := newTestRecoveringFetcher(t, b)

	for range jsiter.TransientEscalation {
		_, err := r.Fetch(context.Background(), 1)
		require.NoError(t, err)
		require.True(t, r.Recover(context.Background(), jetstream.ErrNoHeartbeat))
	}

	assert.Equal(t, 2, b.callCount(), "a fetch that keeps failing must still rebuild")
}

func TestRecoveringFetcher_CleanBatchResetsTransientRun(t *testing.T) {
	b := &builder{fetchers: []msgFetcher{&stubFetcher{}, &stubFetcher{}}}
	r, _ := newTestRecoveringFetcher(t, b)

	for range jsiter.TransientEscalation - 1 {
		require.True(t, r.Recover(context.Background(), jetstream.ErrNoHeartbeat))
	}
	require.True(t, r.Recover(context.Background(), nil), "a clean batch is progress")
	for range jsiter.TransientEscalation - 1 {
		require.True(t, r.Recover(context.Background(), jetstream.ErrNoHeartbeat))
	}

	assert.Equal(t, 1, b.callCount(), "a clean batch resets the transient run")
}

func TestRecoveringFetcher_Recover_NilNeverRebuilds(t *testing.T) {
	b := &builder{fetchers: []msgFetcher{&stubFetcher{}}}
	r, delays := newTestRecoveringFetcher(t, b)

	assert.True(t, r.Recover(context.Background(), nil))
	assert.Equal(t, 1, b.callCount())
	assert.Empty(t, *delays, "a clean batch must not back off")
	assert.True(t, r.IsUp())
}

func TestRecoveringFetcher_Recover_RebuildRetriesUntilItSucceeds(t *testing.T) {
	b := &builder{
		fetchers: []msgFetcher{&stubFetcher{}, nil, nil, &stubFetcher{}},
		errs:     []error{nil, errors.New("no responders"), errors.New("no responders"), nil},
	}
	r, delays := newTestRecoveringFetcher(t, b)

	ok := r.Recover(context.Background(), jetstream.ErrConsumerDeleted)

	assert.True(t, ok)
	assert.Equal(t, 4, b.callCount())
	require.Len(t, *delays, 3)
	assert.Equal(t, jsiter.RebuildBackoff[1], (*delays)[1])
}

func TestRecoveringFetcher_Recover_ConnectionClosedIsTerminal(t *testing.T) {
	b := &builder{fetchers: []msgFetcher{&stubFetcher{}}}
	r, _ := newTestRecoveringFetcher(t, b)

	ok := r.Recover(context.Background(), jetstream.ErrConnectionClosed)

	assert.False(t, ok, "a closed connection ends consumption")
	assert.Equal(t, 1, b.callCount())
	assert.False(t, r.IsUp())
}

func TestRecoveringFetcher_Recover_StopEndsRecovery(t *testing.T) {
	stopCh := make(chan struct{})
	close(stopCh)
	b := &builder{fetchers: []msgFetcher{&stubFetcher{}}, errs: []error{nil, errors.New("down")}}
	r, err := newRecoveringFetcher(context.Background(), "hr", b.build, stopCh)
	require.NoError(t, err)

	ok := r.Recover(context.Background(), jetstream.ErrConsumerDeleted)

	assert.False(t, ok, "shutdown must not be retried through")
	assert.False(t, r.IsUp())
}

func TestRecoveringFetcher_Recover_ContextCancellationEndsRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := &builder{fetchers: []msgFetcher{&stubFetcher{}}, errs: []error{nil, errors.New("down")}}
	r, err := newRecoveringFetcher(ctx, "hr", b.build, nil)
	require.NoError(t, err)

	assert.False(t, r.Recover(ctx, jetstream.ErrConsumerDeleted))
}

func TestRecoveringFetcher_HealthCheck(t *testing.T) {
	b := &builder{fetchers: []msgFetcher{&stubFetcher{}}}
	r, _ := newTestRecoveringFetcher(t, b)

	check := r.HealthCheck()
	assert.Equal(t, "jetstream-consumer:hr", check.Name)
	require.NoError(t, check.Probe(context.Background()))

	require.False(t, r.Recover(context.Background(), jetstream.ErrConnectionClosed))
	probeErr := check.Probe(context.Background())
	require.Error(t, probeErr)
	assert.Contains(t, probeErr.Error(), "hr")
}

// recordingSource adds recovery recording to stubFetcher's scripted fetches, so
// one fake drives runConsumer end to end.
type recordingSource struct {
	*stubFetcher
	mu        sync.Mutex
	recovered []error
	keepGoing bool
	onRecover func()
}

func (s *recordingSource) Recover(_ context.Context, err error) bool {
	s.mu.Lock()
	s.recovered = append(s.recovered, err)
	keep := s.keepGoing
	hook := s.onRecover
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return keep
}

func (s *recordingSource) recoveries() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.recovered...)
}

// runLoop runs runConsumer to completion, failing the test if it never returns.
func runLoop(t *testing.T, src fetchSource, stopCh chan struct{}) {
	t.Helper()
	doneCh := make(chan struct{})
	handler, _ := newFlushCountingHandler(t)
	go runConsumer(context.Background(), src, handler, 10, 500, time.Minute, stopCh, doneCh)
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("runConsumer did not return")
	}
}

// The regression that hides: a deleted consumer is reported only by the batch,
// never by the Fetch call, so a loop that ignores batch.Error indexes nothing
// for the rest of the pod's life.
func TestRunConsumer_RecoversWhenBatchReportsConsumerDeleted(t *testing.T) {
	stopCh := make(chan struct{})
	var once sync.Once
	src := &recordingSource{
		stubFetcher: &stubFetcher{batches: []msgBatch{stubBatch{err: jetstream.ErrConsumerDeleted}}},
		keepGoing:   true,
		onRecover:   func() { once.Do(func() { close(stopCh) }) },
	}

	runLoop(t, src, stopCh)

	assert.Contains(t, src.recoveries(), error(jetstream.ErrConsumerDeleted))
}

func TestRunConsumer_RecoversFromFetchError(t *testing.T) {
	stopCh := make(chan struct{})
	wantErr := errors.New("fetch failed")
	var once sync.Once
	src := &recordingSource{
		stubFetcher: &stubFetcher{errs: []error{wantErr}},
		keepGoing:   true,
		onRecover:   func() { once.Do(func() { close(stopCh) }) },
	}

	runLoop(t, src, stopCh)

	assert.Contains(t, src.recoveries(), wantErr)
}

func TestRunConsumer_ExitsWhenRecoveryGivesUp(t *testing.T) {
	src := &recordingSource{
		stubFetcher: &stubFetcher{errs: []error{jetstream.ErrConnectionClosed}},
	}

	runLoop(t, src, make(chan struct{}))

	assert.Contains(t, src.recoveries(), error(jetstream.ErrConnectionClosed))
}

func TestRunConsumer_HealthyBatchNeverRecovers(t *testing.T) {
	stopCh := make(chan struct{})
	var once sync.Once
	src := &recordingSource{
		stubFetcher: &stubFetcher{},
		keepGoing:   true,
		onRecover:   func() { once.Do(func() { close(stopCh) }) },
	}

	runLoop(t, src, stopCh)

	require.NotEmpty(t, src.recoveries(), "the loop must reach Recover at least once")
	for _, err := range src.recoveries() {
		assert.NoError(t, err, "an empty batch with no error is a quiet stream, not a failure")
	}
}

// errBatch reports a post-drain error, so the wrapper must surface it rather
// than the nil its own re-channelling goroutine would imply.
type errBatch struct {
	jetstream.MessageBatch
	err error
}

func (b errBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg)
	close(ch)
	return ch
}

func (b errBatch) Error() error { return b.err }

func TestRawBatch_Error_DelegatesToUnderlyingBatch(t *testing.T) {
	adapter := rawConsumerAdapter{c: fakeRawConsumer{batch: errBatch{err: jetstream.ErrConsumerDeleted}}}

	batch, err := adapter.Fetch(context.Background(), 10)
	require.NoError(t, err)
	for range batch.Messages() { //nolint:revive // drain before reading Error, per the batch contract
	}

	assert.ErrorIs(t, batch.Error(), jetstream.ErrConsumerDeleted)
}

// flushCountingSource reports how many ES actions were still buffered when the
// loop entered Recover, so a stranded buffer is visible.
type flushCountingSource struct {
	*recordingSource
	handler  *Handler
	buffered []int
	mu       sync.Mutex
	// onError ends the loop on the first failing recovery, so the clean batch
	// that precedes it cannot stop the test before the failure lands.
	onError func()
}

func (s *flushCountingSource) Recover(ctx context.Context, err error) bool {
	if err != nil {
		s.mu.Lock()
		s.buffered = append(s.buffered, s.handler.ActionCount())
		stop := s.onError
		s.mu.Unlock()
		if stop != nil {
			stop()
		}
	}
	return s.recordingSource.Recover(ctx, err)
}

// newFlushCountingHandler returns a handler whose store counts the actions that
// actually reached Elasticsearch.
func newFlushCountingHandler(t *testing.T) (*Handler, func() int) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)

	var mu sync.Mutex
	var actions int
	store.EXPECT().
		Bulk(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, a []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
			mu.Lock()
			actions += len(a)
			mu.Unlock()
			results := make([]searchengine.BulkResult, len(a))
			for i := range results {
				results[i] = searchengine.BulkResult{Status: 201}
			}
			return results, nil
		}).
		AnyTimes()

	h := NewHandler(store, newMessageCollection("msgs-v1", "site-a", time.Time{}, false), 500)
	return h, func() int {
		mu.Lock()
		defer mu.Unlock()
		return actions
	}
}

func (s *flushCountingSource) bufferedAtRecover() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.buffered...)
}

// indexableMsgs builds messages the message collection turns into bulk actions.
func indexableMsgs(t *testing.T, n int) []jetstream.Msg {
	t.Helper()
	msgs := make([]jetstream.Msg, 0, n)
	for i := range n {
		evt := model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: fmt.Sprintf("m%d", i), RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "hello", CreatedAt: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
			},
			SiteID: "site-a", Timestamp: 100,
		}
		msgs = append(msgs, makeStubMsg(t, &evt))
	}
	return msgs
}

// Recover can park for a whole outage while it rebuilds, so anything already
// built must reach Elasticsearch first rather than wait out the outage.
//
// bulkFlushInterval is an hour and bulkBatchSize is far above the message count,
// so neither the interval nor the size flush can fire: only a flush on the error
// path can empty the buffer, and dropping it turns this test red.
func TestRunConsumer_FlushesBufferedActionsBeforeRecovering(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  func(t *testing.T) *recordingSource
	}{
		{"fetch error", func(t *testing.T) *recordingSource {
			return &recordingSource{
				stubFetcher: &stubFetcher{
					errs:    []error{nil, errors.New("fetch failed")},
					batches: []msgBatch{stubBatch{msgs: indexableMsgs(t, 3)}},
				},
				keepGoing: true,
			}
		}},
		{"batch error", func(t *testing.T) *recordingSource {
			return &recordingSource{
				stubFetcher: &stubFetcher{batches: []msgBatch{
					stubBatch{msgs: indexableMsgs(t, 3), err: jetstream.ErrConsumerDeleted},
				}},
				keepGoing: true,
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stopCh := make(chan struct{})
			handler, flushed := newFlushCountingHandler(t)
			var once sync.Once
			src := &flushCountingSource{recordingSource: tc.src(t), handler: handler}
			src.onError = func() { once.Do(func() { close(stopCh) }) }

			doneCh := make(chan struct{})
			go runConsumer(context.Background(), src, handler, 10, 500, time.Hour, stopCh, doneCh)
			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				t.Fatal("runConsumer did not return")
			}

			require.NotEmpty(t, src.bufferedAtRecover(), "the loop must reach Recover")
			for _, buffered := range src.bufferedAtRecover() {
				assert.Zero(t, buffered,
					"actions must be flushed before a recovery that can outlast the flush interval")
			}
			assert.Positive(t, flushed(), "the messages must actually have produced actions")
		})
	}
}

// A fetcher that rebuilds cleanly and fails again at once must keep backing
// off. Restarting the schedule each time would aim ~13 CreateOrUpdateConsumer
// calls a second at an already-degraded control plane.
func TestRecoveringFetcher_RebuildBackoffAdvancesAcrossRebuilds(t *testing.T) {
	b := &builder{}
	r, delays := newTestRecoveringFetcher(t, b)

	for range 3 {
		require.True(t, r.Recover(context.Background(), jetstream.ErrConsumerDeleted))
	}

	require.Len(t, *delays, 3)
	assert.Equal(t, jsiter.RebuildBackoff[0], (*delays)[0])
	assert.Equal(t, jsiter.RebuildBackoff[1], (*delays)[1], "a second rebuild must back off further")
	assert.Equal(t, jsiter.RebuildBackoff[2], (*delays)[2])
}

// An empty poll is not proof that the replacement works: a flapping leader
// answers some pulls and fails others, and letting the clean one reset the
// schedule pins every later rebuild at the minimum delay forever.
func TestRecoveringFetcher_EmptyBatchDoesNotResetRebuildBackoff(t *testing.T) {
	b := &builder{}
	r, delays := newTestRecoveringFetcher(t, b)

	for range 3 {
		require.True(t, r.Recover(context.Background(), jetstream.ErrConsumerDeleted))
		require.True(t, r.Recover(context.Background(), nil), "a clean but empty poll")
	}

	require.Len(t, *delays, 3)
	assert.Equal(t, jsiter.RebuildBackoff[0], (*delays)[0])
	assert.Equal(t, jsiter.RebuildBackoff[1], (*delays)[1], "an interleaved empty poll must not reset the schedule")
	assert.Equal(t, jsiter.RebuildBackoff[2], (*delays)[2])
}

// Rebuilds far apart are unrelated failures, not a flap, so the schedule starts
// over — otherwise a long-lived pod would creep to the 30s cap and stay there.
func TestRecoveringFetcher_RebuildBackoffResetsAfterAQuietWindow(t *testing.T) {
	b := &builder{}
	r, delays := newTestRecoveringFetcher(t, b)
	now := time.Now()
	r.nowFn = func() time.Time { return now }

	require.True(t, r.Recover(context.Background(), jetstream.ErrConsumerDeleted))
	require.True(t, r.Recover(context.Background(), jetstream.ErrConsumerDeleted))
	now = now.Add(jsiter.EscalationWindow + time.Second)
	require.True(t, r.Recover(context.Background(), jetstream.ErrConsumerDeleted))

	require.Len(t, *delays, 3)
	assert.Equal(t, jsiter.RebuildBackoff[1], (*delays)[1], "a rebuild moments later keeps escalating")
	assert.Equal(t, jsiter.RebuildBackoff[0], (*delays)[2], "one long after the last starts over")
}
