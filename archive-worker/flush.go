package main

import (
	"bytes"
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
		var itemErr error
		backoff := jsretry.DefaultBackoff
		for range it.docs {
			r := results[pos]
			pos++
			if itemErr != nil || searchengine.IsBulkItemSuccess(searchengine.ActionCreate, r) {
				continue
			}
			switch {
			case searchengine.IsBulkItemPermanent(r):
				itemErr = errcode.Permanent(errcode.BadRequest(fmt.Sprintf("archive index rejected the document: status %d %s", r.Status, r.ErrorType)))
			case searchengine.IsBulkItemBackpressure(r):
				itemErr, backoff = fmt.Errorf("archive index backpressure: status %d %s", r.Status, r.ErrorType), jsretry.BackpressureBackoff
			default:
				itemErr = fmt.Errorf("archive index item failed: status %d %s", r.Status, r.ErrorType)
			}
			slog.ErrorContext(it.ctx, "archive index item failed", "seq", it.seq, "status", r.Status, "errorType", r.ErrorType)
		}
		if itemErr == nil {
			f.metrics.events("archived", 1)
		} else {
			f.metrics.events("failed", 1)
		}
		jsretry.SettleQuiet(it.ctx, it.msg, backoff, itemErr)
	}
}

func (f *flusher) nakAll(ctx context.Context, items []item, reason string) {
	for _, it := range items {
		jsretry.Nak(it.ctx, it.msg, jsretry.DefaultBackoff, reason)
	}
	f.metrics.events("nak", len(items))
}
