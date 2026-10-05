package main

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/searchengine"
)

type flushConfig struct {
	putTimeout  time.Duration
	bulkTimeout time.Duration
	attempts    int
	retryWait   func(attempt int) time.Duration
}

// defaultRetryWait is the short pause between in-process attempts: 500ms, 1s, ...
func defaultRetryWait(attempt int) time.Duration {
	return time.Duration(attempt+1) * 500 * time.Millisecond
}

type flusher struct {
	objects objectStore
	index   indexStore
	cfg     flushConfig
	metrics *metrics
}

func newFlusher(objects objectStore, index indexStore, cfg flushConfig, m *metrics) *flusher {
	cfg.attempts = max(cfg.attempts, 1) // zero attempts would never run fn and NAK every batch forever
	if cfg.retryWait == nil {
		cfg.retryWait = defaultRetryWait
	}
	return &flusher{objects: objects, index: index, cfg: cfg, metrics: m}
}

func (f *flusher) withAttempts(ctx context.Context, op string, timeout time.Duration, fn func(context.Context) error) error {
	var last error
	for attempt := 0; attempt < f.cfg.attempts; attempt++ {
		actx, cancel := context.WithTimeout(ctx, timeout)
		last = fn(actx)
		cancel()
		if last == nil {
			return nil
		}
		slog.WarnContext(ctx, "archive write attempt failed", "op", op, "attempt", attempt+1, "of", f.cfg.attempts, "error", last)
		if attempt+1 < f.cfg.attempts {
			wait := time.NewTimer(f.cfg.retryWait(attempt))
			select {
			case <-wait.C:
			case <-ctx.Done():
				wait.Stop()
				return fmt.Errorf("%s: %w", op, ctx.Err())
			}
		}
	}
	return fmt.Errorf("%s after %d attempts: %w", op, f.cfg.attempts, last)
}

// flush writes the segment, then the documents, then settles every message:
// a PUT failure after all attempts NAKs the whole batch; a bulk transport
// failure NAKs the whole batch (the segment stays, a redelivery conflicts);
// per item, all actions 2xx or 409 means Ack, a 429 means NAK with
// BackpressureBackoff, a 400 means Term via errcode.Permanent, anything else
// NAKs with DefaultBackoff.
func (f *flusher) flush(ctx context.Context, s *sealed) {
	err := f.withAttempts(ctx, "put segment", f.cfg.putTimeout, func(c context.Context) error {
		return f.objects.Put(c, s.key, bytes.NewReader(s.body), int64(len(s.body)), "application/octet-stream")
	})
	if err != nil {
		f.metrics.writeFailure("bucket")
		f.nakAll(ctx, s.items, "segment put failed: "+err.Error())
		return
	}
	f.metrics.segments(1, len(s.body))

	var results []searchengine.BulkResult
	err = f.withAttempts(ctx, "bulk create", f.cfg.bulkTimeout, func(c context.Context) error {
		var berr error
		results, berr = f.index.Bulk(c, s.actions)
		if berr != nil {
			return berr
		}
		if len(results) != len(s.actions) {
			return fmt.Errorf("%d results for %d actions", len(results), len(s.actions))
		}
		return nil
	})
	if err != nil {
		f.metrics.writeFailure("index")
		f.nakAll(ctx, s.items, "bulk create failed: "+err.Error())
		return
	}

	pos := 0
	for _, it := range s.items {
		backoff, itemErr := f.settleOutcome(it, results[pos:pos+len(it.docs)])
		pos += len(it.docs)
		if itemErr == nil {
			f.metrics.events("archived", 1)
		} else {
			f.metrics.events("failed", 1)
		}
		if _, permanent := errcode.IsPermanent(itemErr); permanent {
			slog.WarnContext(it.ctx, "archive index rejected the document permanently", "seq", it.seq, "disposition", "drop", "error", itemErr)
		}
		jsretry.SettleQuiet(it.ctx, it.msg, backoff, itemErr)
	}
}

// settleOutcome folds every document result of one item into a single
// disposition, worst first: backpressure (retry slowly) over any other
// transient failure over a permanent rejection over success. A permanent
// rejection of one document must not drop its siblings that only need a retry,
// since the segment already holds the frame and a redelivery conflicts safely.
func (f *flusher) settleOutcome(it item, results []searchengine.BulkResult) ([]time.Duration, error) { //nolint:gocritic // hugeParam: item is moved, not mutated
	var perm, transient, pressure error
	for i, r := range results {
		if searchengine.IsBulkItemSuccess(searchengine.ActionCreate, r) {
			continue
		}
		slog.ErrorContext(it.ctx, "archive index item failed", "seq", it.seq, "docID", it.docs[i].ID, "status", r.Status, "errorType", r.ErrorType)
		switch {
		case searchengine.IsBulkItemBackpressure(r):
			pressure = cmp.Or(pressure, fmt.Errorf("archive index backpressure: status %d %s", r.Status, r.ErrorType))
		case searchengine.IsBulkItemPermanent(r):
			perm = cmp.Or[error](perm, errcode.Permanent(errcode.BadRequest(fmt.Sprintf("archive index rejected the document: status %d %s", r.Status, r.ErrorType))))
		default:
			transient = cmp.Or(transient, fmt.Errorf("archive index item failed: status %d %s", r.Status, r.ErrorType))
		}
	}
	switch {
	case pressure != nil:
		return jsretry.BackpressureBackoff, pressure
	case transient != nil:
		return jsretry.DefaultBackoff, transient
	default:
		return jsretry.DefaultBackoff, perm
	}
}

func (f *flusher) nakAll(ctx context.Context, items []item, reason string) {
	for _, it := range items {
		jsretry.Nak(it.ctx, it.msg, jsretry.DefaultBackoff, reason)
	}
	f.metrics.events("nak", len(items))
}
