package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func (f *fakeObjects) Stat(context.Context, string) (bool, error) { return false, nil }

func (f *fakeObjects) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("fakeObjects does not serve reads")
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
	// stored maps a document id to the raw _source GetDoc serves for it;
	// an id that is absent is "not found".
	stored map[string]string
	getErr error
	gets   []string
	getsMu sync.Mutex
}

func (f *fakeIndex) GetDoc(_ context.Context, index, docID string) (json.RawMessage, bool, error) {
	f.getsMu.Lock()
	f.gets = append(f.gets, index+"/"+docID)
	f.getsMu.Unlock()
	if f.getErr != nil {
		return nil, false, f.getErr
	}
	src, ok := f.stored[docID]
	if !ok {
		return nil, false, nil
	}
	return json.RawMessage(`{"_index":"` + index + `","_id":"` + docID + `","found":true,"_source":` + src + `}`), true, nil
}

func (f *fakeIndex) getCount() int {
	f.getsMu.Lock()
	defer f.getsMu.Unlock()
	return len(f.gets)
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
		it.hash = fmt.Sprintf("hmac-sha256:h%d", s)
		items = append(items, it)
	}
	s, err := seal("site-a", "events", "kid-1", items, time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	return s
}

func cfgFast() flushConfig {
	return flushConfig{source: sourceEvents, putTimeout: time.Second, bulkTimeout: time.Second, attempts: 2, retryWait: func(int) time.Duration { return 0 }}
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
	t.Run("409 is verified against the stored record", func(t *testing.T) {
		conflict := []searchengine.BulkResult{{Status: 409, ErrorType: "version_conflict_engine_exception"}}
		tests := []struct {
			name      string
			stored    map[string]string
			getErr    error
			wantAck   bool
			wantTerm  bool
			wantNak   bool
			wantError string // logged at ERROR when set
		}{
			{name: "same record hash is archived", stored: map[string]string{"site-a-3": `{"contentHash":"hmac-sha256:h3"}`}, wantAck: true},
			{name: "a different record is terminated and logged", stored: map[string]string{"site-a-3": `{"contentHash":"hmac-sha256:other"}`}, wantTerm: true, wantError: "archive document conflict"},
			{name: "an undecodable stored document is terminated", stored: map[string]string{"site-a-3": `"not an object"`}, wantTerm: true, wantError: "archive document conflict"},
			{name: "a lookup failure is retried", getErr: errors.New("es down"), wantNak: true},
			{name: "a conflict with no document behind it is retried", stored: map[string]string{}, wantNak: true},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var buf bytes.Buffer
				prev := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
				t.Cleanup(func() { slog.SetDefault(prev) })
				m, reader := testMetrics(t)
				idx := &fakeIndex{results: conflict, stored: tc.stored, getErr: tc.getErr}
				s := sealedWith(t, 3)
				newFlusher(&fakeObjects{}, idx, cfgFast(), m).flush(ctx, s)
				msg := s.items[0].msg.(*fakeMsg)
				assert.Equal(t, tc.wantAck, msg.acked, "acked")
				assert.Equal(t, tc.wantTerm, msg.termed, "termed")
				assert.Equal(t, tc.wantNak, msg.naked, "naked")
				assert.Equal(t, []string{"audit-events-site-a-2026.10.05/site-a-3"}, idx.gets, "the conflicting doc is read back from its own index")
				if tc.wantError != "" {
					assert.Contains(t, buf.String(), `"level":"ERROR","msg":"`+tc.wantError)
					assert.Contains(t, buf.String(), `"seq":3`)
					assert.Contains(t, buf.String(), `"docID":"site-a-3"`)
					assert.Contains(t, buf.String(), `"index":"audit-events-site-a-2026.10.05"`)
					assert.Equal(t, int64(1), eventCount(t, reader, sourceEvents, outcomeConflict))
				} else {
					assert.Zero(t, eventCount(t, reader, sourceEvents, outcomeConflict))
				}
			})
		}
	})
	t.Run("a created document needs no read-back", func(t *testing.T) {
		idx := &fakeIndex{}
		s := sealedWith(t, 2)
		newFlusher(&fakeObjects{}, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.Zero(t, idx.getCount())
		assert.True(t, s.items[0].msg.(*fakeMsg).acked)
	})
	t.Run("an acked event records its lag and archived outcome under the lane's source", func(t *testing.T) {
		m, reader := testMetrics(t)
		s := sealedWith(t, 4)
		s.items[0].eventAt = time.Date(2026, 10, 5, 13, 59, 30, 0, time.UTC)
		cfg := cfgFast()
		cfg.source = sourceMembers
		cfg.now = func() time.Time { return time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC) }
		newFlusher(&fakeObjects{}, &fakeIndex{}, cfg, m).flush(ctx, s)
		assert.Equal(t, int64(1), eventCount(t, reader, sourceMembers, outcomeArchived))
		assert.InDelta(t, 30.0, lagSum(t, reader, "archive_lag_seconds", sourceMembers), 1e-9)
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
		}, stored: map[string]string{"site-a-22": `{"contentHash":"hmac-sha256:h22"}`}}
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
		s, err := seal("site-a", "events", "kid-1", items, time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC))
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
				s, err := seal("site-a", "events", "kid-1", []item{it}, time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC))
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

func TestDefaultRetryWait(t *testing.T) {
	assert.Equal(t, 500*time.Millisecond, defaultRetryWait(0))
	assert.Equal(t, time.Second, defaultRetryWait(1))
	assert.Equal(t, 1500*time.Millisecond, defaultRetryWait(2))
}
