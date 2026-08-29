package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// streamingFetcher hands out an endless supply of messages, one per Fetch, and counts
// the calls so a test can observe whether the consumer loop keeps pulling while a bulk
// request is in flight. failAfter > 0 makes every Fetch past that many calls fail, for
// the shape of a consumer that loses NATS with work still buffered.
type streamingFetcher struct {
	data      []byte
	failAfter int
	mu        sync.Mutex
	n         int
}

func (f *streamingFetcher) Fetch(ctx context.Context, _ int, _ ...jetstream.FetchOpt) (msgBatch, error) {
	f.mu.Lock()
	f.n++
	failed := f.failAfter > 0 && f.n > f.failAfter
	f.mu.Unlock()
	if failed {
		return nil, errors.New("fetch failed")
	}
	return fakeO11yBatch{msgs: []o11ynats.FetchedMessage{{Ctx: ctx, Msg: &stubMsg{data: f.data}}}}, nil
}

func (f *streamingFetcher) fetches() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

// blockingStore holds every Bulk call open until the test closes release, and
// records how many ran at once so a test can pin the pipeline's depth.
type blockingStore struct {
	entered chan struct{}
	release chan struct{}

	mu          sync.Mutex
	inFlight    int
	maxInFlight int
}

func newBlockingStore() *blockingStore {
	return &blockingStore{entered: make(chan struct{}, 64), release: make(chan struct{})}
}

func (s *blockingStore) Bulk(_ context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
	s.mu.Lock()
	s.inFlight++
	if s.inFlight > s.maxInFlight {
		s.maxInFlight = s.inFlight
	}
	s.mu.Unlock()

	s.entered <- struct{}{}
	<-s.release

	s.mu.Lock()
	s.inFlight--
	s.mu.Unlock()

	return okResults(len(actions)), nil
}

// okResults is an all-success bulk response for n actions.
func okResults(n int) []searchengine.BulkResult {
	results := make([]searchengine.BulkResult, n)
	for i := range results {
		results[i] = searchengine.BulkResult{Status: 200}
	}
	return results
}

// UpdateByQuery satisfies Store; the pipeline tests only exercise the bulk path.
func (s *blockingStore) UpdateByQuery(context.Context, string, json.RawMessage) error { return nil }

func (s *blockingStore) maxConcurrent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxInFlight
}

func pipelineMsgData(t *testing.T) []byte {
	t.Helper()
	data, err := json.Marshal(&model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content: "hello", CreatedAt: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
		},
		SiteID: "site-a", Timestamp: 100,
	})
	require.NoError(t, err)
	return data
}

// startPipelineConsumer runs a consumer with bulkBatchSize 1 so every message
// triggers a flush, reaching the first bulk request after a single fetch.
func startPipelineConsumer(t *testing.T, store Store, fetcher msgFetcher, depth int) (stopCh chan struct{}, doneCh chan struct{}) {
	t.Helper()
	handler := NewHandler(store, newMessageCollection("msgs-v1", "site-a", time.Time{}, false), 1)
	stopCh = make(chan struct{})
	doneCh = make(chan struct{})
	go runConsumer(context.Background(), fetcher, handler, consumerTuning{
		fetchBatchSize: 1, bulkFlushInterval: time.Hour, pipelineDepth: depth,
	}, stopCh, doneCh)
	return stopCh, doneCh
}

func requireClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

// Depth is what lets a collection overlap ES round-trips with the fetch and build behind
// them, and it is bounded: unbounded concurrency would queue batches against ES without
// limit and blow through the consumer's ack-pending budget.
func TestRunConsumer_RunsUpToPipelineDepthConcurrently(t *testing.T) {
	for _, depth := range []int{1, 2} {
		t.Run(fmt.Sprintf("depth %d", depth), func(t *testing.T) {
			store := newBlockingStore()
			fetcher := &streamingFetcher{data: pipelineMsgData(t)}
			stopCh, doneCh := startPipelineConsumer(t, store, fetcher, depth)

			for i := 0; i < depth; i++ {
				requireClosed(t, store.entered, "bulk request never started")
			}
			// Every slot is held, so a serial loop could not have fetched again.
			require.Eventually(t, func() bool { return fetcher.fetches() > depth }, 5*time.Second, 5*time.Millisecond,
				"consumer must keep fetching while the bulk requests are in flight")
			require.Eventually(t, func() bool { return store.maxConcurrent() == depth }, 5*time.Second, 5*time.Millisecond,
				"the pipeline must reach its configured depth")

			// Bounded negative assertion: a depth+1'th request would have started by now.
			select {
			case <-store.entered:
				t.Fatalf("a bulk request beyond depth %d started while all slots were held", depth)
			case <-time.After(200 * time.Millisecond):
			}
			assert.Equal(t, depth, store.maxConcurrent())

			close(store.release)
			close(stopCh)
			requireClosed(t, doneCh, "consumer did not shut down")
		})
	}
}

func TestRunConsumer_WaitsForInFlightBulksBeforeSignallingDone(t *testing.T) {
	for _, depth := range []int{1, 2} {
		t.Run(fmt.Sprintf("depth %d", depth), func(t *testing.T) {
			store := newBlockingStore()
			fetcher := &streamingFetcher{data: pipelineMsgData(t)}
			stopCh, doneCh := startPipelineConsumer(t, store, fetcher, depth)

			for i := 0; i < depth; i++ {
				requireClosed(t, store.entered, "bulk request never started")
			}
			close(stopCh)

			select {
			case <-doneCh:
				t.Fatal("consumer signalled done with bulk requests still in flight")
			case <-time.After(200 * time.Millisecond):
			}

			close(store.release)
			requireClosed(t, doneCh, "consumer did not shut down once every bulk request finished")
		})
	}
}

// recordingStore captures the batches handed to Bulk without blocking.
type recordingStore struct {
	mu    sync.Mutex
	sizes []int
}

func (s *recordingStore) Bulk(_ context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
	s.mu.Lock()
	s.sizes = append(s.sizes, len(actions))
	s.mu.Unlock()
	return okResults(len(actions)), nil
}

// UpdateByQuery satisfies Store; the pipeline tests only exercise the bulk path.
func (s *recordingStore) UpdateByQuery(context.Context, string, json.RawMessage) error { return nil }

func (s *recordingStore) batches() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.sizes...)
}

func TestRunConsumer_FlushesBufferedWorkOnShutdownAfterFetchErrors(t *testing.T) {
	store := &recordingStore{}
	fetcher := &streamingFetcher{data: pipelineMsgData(t), failAfter: 1}
	// Bulk size 10 and a one-hour interval keep the buffered message below both
	// flush triggers, so only the shutdown drain can get it to ES.
	handler := NewHandler(store, newMessageCollection("msgs-v1", "site-a", time.Time{}, false), 10)
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	go runConsumer(context.Background(), fetcher, handler, consumerTuning{
		fetchBatchSize: 10, bulkFlushInterval: time.Hour, pipelineDepth: 1,
	}, stopCh, doneCh)

	require.Eventually(t, func() bool { return fetcher.fetches() > 1 }, 5*time.Second, 5*time.Millisecond,
		"consumer must keep looping after a fetch error")

	close(stopCh)
	requireClosed(t, doneCh, "consumer did not shut down")

	assert.Equal(t, []int{1}, store.batches(), "the message buffered before the fetch errors must still reach ES")
}

// Depth is what lets a collection keep more than one bulk request in flight. It is
// bounded: unbounded concurrency would let batches queue up against ES without limit
// and blow past the consumer's ack-pending budget.
