package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// fakeObjects is shared with the lane tests, whose background flush writes
// puts concurrently with the test's polling, so access goes through mu.
type fakeObjects struct {
	mu        sync.Mutex
	failFirst int
	puts      []string
}

func (f *fakeObjects) Put(_ context.Context, key string, body io.Reader, _ int64, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFirst > 0 {
		f.failFirst--
		return errors.New("minio down")
	}
	_, _ = io.ReadAll(body)
	f.puts = append(f.puts, key)
	return nil
}

type fakeIndex struct {
	indexStore
	results []searchengine.BulkResult
	err     error
	calls   int
}

func (f *fakeIndex) Bulk(_ context.Context, a []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if f.results == nil {
		out := make([]searchengine.BulkResult, len(a))
		for i := range out {
			out[i] = searchengine.BulkResult{Status: 201}
		}
		return out, nil
	}
	return f.results, nil
}

func sealedWith(t *testing.T, seqs ...uint64) *sealed {
	t.Helper()
	items := make([]item, 0, len(seqs))
	for _, s := range seqs {
		it := mkItem(s, 8)
		it.msg = &fakeMsg{seq: s}
		items = append(items, it)
	}
	s, err := seal("site-a", "events", items, time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	return s
}

func cfgFast() flushConfig {
	return flushConfig{putTimeout: time.Second, bulkTimeout: time.Second, attempts: 2, retryWait: func(int) time.Duration { return 0 }}
}

func TestFlush(t *testing.T) {
	ctx := context.Background()
	t.Run("happy path acks everything", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &fakeIndex{}
		s := sealedWith(t, 1, 2)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.Equal(t, []string{s.key}, obj.puts)
		for _, it := range s.items {
			assert.True(t, it.msg.(*fakeMsg).acked)
		}
	})
	t.Run("409 is treated as archived", func(t *testing.T) {
		obj := &fakeObjects{}
		idx := &fakeIndex{results: []searchengine.BulkResult{{Status: 409, ErrorType: "version_conflict_engine_exception"}}}
		s := sealedWith(t, 3)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.True(t, s.items[0].msg.(*fakeMsg).acked)
	})
	t.Run("put retried once then succeeds", func(t *testing.T) {
		obj, idx := &fakeObjects{failFirst: 1}, &fakeIndex{}
		s := sealedWith(t, 4)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.Len(t, obj.puts, 1)
		assert.True(t, s.items[0].msg.(*fakeMsg).acked)
	})
	t.Run("put exhausted naks the batch and never indexes", func(t *testing.T) {
		obj, idx := &fakeObjects{failFirst: 5}, &fakeIndex{}
		s := sealedWith(t, 5, 6)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.Equal(t, 0, idx.calls)
		for _, it := range s.items {
			m := it.msg.(*fakeMsg)
			assert.True(t, m.naked)
			assert.False(t, m.acked)
			assert.Greater(t, m.nakDelay, time.Duration(0), "never a bare nak")
		}
	})
	t.Run("bulk transport failure naks after the segment is written", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &fakeIndex{err: errors.New("es down")}
		s := sealedWith(t, 7)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.Len(t, obj.puts, 1)
		assert.Equal(t, 2, idx.calls, "bulk is attempted cfg.attempts times")
		assert.True(t, s.items[0].msg.(*fakeMsg).naked)
		assert.False(t, s.items[0].msg.(*fakeMsg).acked)
	})
	t.Run("result count mismatch naks the batch", func(t *testing.T) {
		obj := &fakeObjects{}
		idx := &fakeIndex{results: []searchengine.BulkResult{{Status: 201}}}
		s := sealedWith(t, 10, 11)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		for _, it := range s.items {
			assert.True(t, it.msg.(*fakeMsg).naked)
			assert.False(t, it.msg.(*fakeMsg).acked)
		}
	})
	t.Run("429 naks with backpressure backoff", func(t *testing.T) {
		obj := &fakeObjects{}
		idx := &fakeIndex{results: []searchengine.BulkResult{{Status: 429, ErrorType: "es_rejected_execution_exception"}}}
		s := sealedWith(t, 8)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		m := s.items[0].msg.(*fakeMsg)
		assert.True(t, m.naked)
		assert.GreaterOrEqual(t, m.nakDelay, jsretry.BackpressureBackoff[0]/2, "jittered within [d/2, d]")
		assert.LessOrEqual(t, m.nakDelay, jsretry.BackpressureBackoff[0])
	})
	t.Run("500 naks with default backoff", func(t *testing.T) {
		obj := &fakeObjects{}
		idx := &fakeIndex{results: []searchengine.BulkResult{{Status: 500, ErrorType: "internal"}}}
		s := sealedWith(t, 12)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		m := s.items[0].msg.(*fakeMsg)
		assert.True(t, m.naked)
		assert.LessOrEqual(t, m.nakDelay, jsretry.DefaultBackoff[0])
	})
	t.Run("400 terminates the message", func(t *testing.T) {
		obj := &fakeObjects{}
		idx := &fakeIndex{results: []searchengine.BulkResult{{Status: 400, ErrorType: "mapper_parsing_exception"}}}
		s := sealedWith(t, 9)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		m := s.items[0].msg.(*fakeMsg)
		assert.True(t, m.acked || m.termed, "permanent failure must not be redelivered")
		assert.False(t, m.naked)
	})
	t.Run("results map back to items per message", func(t *testing.T) {
		obj := &fakeObjects{}
		idx := &fakeIndex{results: []searchengine.BulkResult{
			{Status: 201}, {Status: 429, ErrorType: "es_rejected_execution_exception"}, {Status: 409},
		}}
		s := sealedWith(t, 20, 21, 22)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.True(t, s.items[0].msg.(*fakeMsg).acked)
		assert.True(t, s.items[1].msg.(*fakeMsg).naked)
		assert.True(t, s.items[2].msg.(*fakeMsg).acked)
	})
	t.Run("an item with several documents settles on all of them", func(t *testing.T) {
		two := func(seq uint64) item {
			it := mkItem(seq, 8)
			it.msg = &fakeMsg{seq: seq}
			it.docs = append(it.docs, docSpec{Index: "audit-events-site-a-2026.10.05", ID: auditarchive.EventDocID("site-a", seq) + "-b", Doc: &auditarchive.EventDoc{Seq: seq}})
			return it
		}
		items := []item{two(30), two(31)}
		s, err := seal("site-a", "events", items, time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		idx := &fakeIndex{results: []searchengine.BulkResult{
			{Status: 201}, {Status: 201}, {Status: 201}, {Status: 400, ErrorType: "mapper_parsing_exception"},
		}}
		newFlusher(&fakeObjects{}, idx, cfgFast(), &metrics{}).flush(context.Background(), s)
		assert.True(t, items[0].msg.(*fakeMsg).acked, "both documents created")
		m := items[1].msg.(*fakeMsg)
		assert.False(t, m.naked, "one rejected document is permanent for the message")
		assert.True(t, m.acked || m.termed)
	})
	t.Run("a multi-doc item settles on its worst outcome whatever the doc order", func(t *testing.T) {
		busy := searchengine.BulkResult{Status: 429, ErrorType: "es_rejected_execution_exception"}
		bad := searchengine.BulkResult{Status: 400, ErrorType: "mapper_parsing_exception"}
		boom := searchengine.BulkResult{Status: 500, ErrorType: "internal"}
		ok := searchengine.BulkResult{Status: 201}
		tests := []struct {
			name         string
			results      []searchengine.BulkResult
			nak          bool
			backpressure bool
		}{
			{"permanent then backpressure", []searchengine.BulkResult{bad, busy}, true, true},
			{"backpressure then permanent", []searchengine.BulkResult{busy, bad}, true, true},
			{"permanent then transient", []searchengine.BulkResult{bad, boom}, true, false},
			{"transient then permanent", []searchengine.BulkResult{boom, bad}, true, false},
			{"transient then backpressure", []searchengine.BulkResult{boom, busy}, true, true},
			{"success then permanent", []searchengine.BulkResult{ok, bad}, false, false},
			{"permanent only", []searchengine.BulkResult{bad, bad}, false, false},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				it := mkItem(50, 8)
				it.msg = &fakeMsg{seq: 50}
				it.docs = append(it.docs, docSpec{Index: "audit-events-site-a-2026.10.05", ID: "site-a-50-b", Doc: &auditarchive.EventDoc{Seq: 50}})
				s, err := seal("site-a", "events", []item{it}, time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC))
				require.NoError(t, err)
				newFlusher(&fakeObjects{}, &fakeIndex{results: tc.results}, cfgFast(), &metrics{}).flush(ctx, s)
				m := it.msg.(*fakeMsg)
				assert.Equal(t, tc.nak, m.naked)
				assert.Equal(t, !tc.nak, m.acked || m.termed)
				if tc.nak {
					if tc.backpressure {
						assert.GreaterOrEqual(t, m.nakDelay, jsretry.BackpressureBackoff[0]/2)
					} else {
						assert.LessOrEqual(t, m.nakDelay, jsretry.DefaultBackoff[0])
					}
				}
			})
		}
	})
	t.Run("an Ack-dropped document is logged once with disposition=drop", func(t *testing.T) {
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
		t.Cleanup(func() { slog.SetDefault(prev) })
		idx := &fakeIndex{results: []searchengine.BulkResult{{Status: 400, ErrorType: "mapper_parsing_exception"}}}
		s := sealedWith(t, 60)
		newFlusher(&fakeObjects{}, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.Equal(t, 1, strings.Count(buf.String(), `"disposition":"drop"`))
		assert.Contains(t, buf.String(), `"seq":60`)
	})
	t.Run("cancelled context stops the retry wait", func(t *testing.T) {
		obj, idx := &fakeObjects{failFirst: 5}, &fakeIndex{}
		cfg := cfgFast()
		cfg.retryWait = func(int) time.Duration { return time.Hour }
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		s := sealedWith(t, 40)
		newFlusher(obj, idx, cfg, &metrics{}).flush(cctx, s)
		assert.True(t, s.items[0].msg.(*fakeMsg).naked)
		assert.Equal(t, 0, idx.calls)
	})
}

func TestNewFlusher_ClampsAttempts(t *testing.T) {
	obj, idx := &fakeObjects{}, &fakeIndex{}
	s := sealedWith(t, 70)
	newFlusher(obj, idx, flushConfig{putTimeout: time.Second, bulkTimeout: time.Second}, &metrics{}).flush(context.Background(), s)
	assert.Len(t, obj.puts, 1, "zero attempts still tries once")
	assert.True(t, s.items[0].msg.(*fakeMsg).acked)
}

func TestNewFlusher_DefaultsRetryWait(t *testing.T) {
	f := newFlusher(&fakeObjects{}, &fakeIndex{}, flushConfig{attempts: 1}, &metrics{})
	require.NotNil(t, f.cfg.retryWait)
	assert.Greater(t, f.cfg.retryWait(0), time.Duration(0))
}
