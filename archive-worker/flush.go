package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/searchengine"
)

type flushConfig struct {
	source      string // lane name, the metrics' source label
	putTimeout  time.Duration
	bulkTimeout time.Duration
	attempts    int
	retryWait   func(attempt int) time.Duration
	now         func() time.Time // for the lag metric
}

// defaultRetryWait is the short pause between in-process attempts: 500ms, 1s, ...
// config.validate sums it into the worst-case batch time.
func defaultRetryWait(attempt int) time.Duration {
	return time.Duration(attempt+1) * 500 * time.Millisecond
}

// errConflictMismatch means a create conflicted with a document that holds a
// different record: the id is taken by something this event is not.
var errConflictMismatch = errors.New("archive document conflict: existing document holds a different record")

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
	if cfg.now == nil {
		cfg.now = time.Now
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

// flush writes the segment, then the documents, then settles every message
// exactly once: a PUT failure after all attempts NAKs the whole batch; a bulk
// transport failure NAKs the whole batch (the segment stays, a redelivery
// conflicts); per item, every document created or verified means Ack, a 429
// means NAK with BackpressureBackoff, a 400 means Ack-drop via
// errcode.Permanent, a 409 over a different record means Term, anything else
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

	// Every conflict read-back in this flush shares one bulk timeout, so a
	// fully redelivered batch cannot stretch the flush past the ack wait;
	// lookups still pending when it expires fail and their messages NAK.
	vctx, cancel := context.WithTimeout(ctx, f.cfg.bulkTimeout)
	defer cancel()
	pos := 0
	for i := range s.items {
		it := &s.items[i]
		o := f.settleOutcome(vctx, it, results[pos:pos+len(it.docs)])
		pos += len(it.docs)
		f.settle(it, o)
	}
}

// outcome is one item's single disposition.
type outcome struct {
	backoff []time.Duration
	err     error
	term    bool // a conflicting document holds a different record
}

func (f *flusher) settle(it *item, o outcome) {
	switch {
	case o.term:
		f.metrics.events(f.cfg.source, outcomeConflict, 1)
		if err := it.msg.Term(); err != nil {
			slog.ErrorContext(it.ctx, "term failed", "lane", f.cfg.source, "seq", it.seq, "error", err)
		}
	case o.err == nil:
		f.metrics.events(f.cfg.source, outcomeArchived, 1)
		jsretry.SettleQuiet(it.ctx, it.msg, o.backoff, nil)
		if !it.eventAt.IsZero() {
			f.metrics.lag(f.cfg.source, f.cfg.now().Sub(it.eventAt))
		}
	default:
		f.metrics.events(f.cfg.source, outcomeFailed, 1)
		if _, permanent := errcode.IsPermanent(o.err); permanent {
			slog.WarnContext(it.ctx, "archive index rejected the document permanently", "seq", it.seq, "disposition", "drop", "error", o.err)
		}
		jsretry.SettleQuiet(it.ctx, it.msg, o.backoff, o.err)
	}
}

// settleOutcome folds every document result of one item into a single
// disposition, worst first: backpressure (retry slowly) over any other
// transient failure over a conflicting record (Term) over a permanent
// rejection over success. A permanent rejection of one document must not drop
// its siblings that only need a retry, since the segment already holds the
// frame and a redelivery conflicts safely. A 409 is success only once the
// existing document is read back and carries this event's record hash.
func (f *flusher) settleOutcome(ctx context.Context, it *item, results []searchengine.BulkResult) outcome {
	var perm, transient, pressure, conflict error
	for i, r := range results {
		if r.Status == http.StatusConflict {
			switch err := f.verifyConflict(ctx, it, it.docs[i]); {
			case err == nil:
			case errors.Is(err, errConflictMismatch):
				slog.ErrorContext(it.ctx, "archive document conflict", "seq", it.seq, "docID", it.docs[i].ID, "index", it.docs[i].Index, "error", err)
				conflict = cmp.Or(conflict, err)
			default:
				slog.WarnContext(it.ctx, "archive conflict not verified, will redeliver", "seq", it.seq, "docID", it.docs[i].ID, "error", err)
				transient = cmp.Or(transient, err)
			}
			continue
		}
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
		return outcome{backoff: jsretry.BackpressureBackoff, err: pressure}
	case transient != nil:
		return outcome{backoff: jsretry.DefaultBackoff, err: transient}
	case conflict != nil:
		return outcome{err: conflict, term: true}
	default:
		return outcome{backoff: jsretry.DefaultBackoff, err: perm}
	}
}

// verifyConflict reads back the document a create conflicted with. Its
// contentHash equal to this item's record hash means an earlier delivery
// archived this very event; a different (or unreadable) one means the id
// belongs to another record, which errConflictMismatch reports. A failed or
// empty lookup is returned as a plain error so the message is retried.
func (f *flusher) verifyConflict(ctx context.Context, it *item, d docSpec) error {
	raw, found, err := f.index.GetDoc(ctx, d.Index, d.ID)
	if err != nil {
		return fmt.Errorf("read back conflicting doc %s: %w", d.ID, err)
	}
	if !found {
		return fmt.Errorf("read back conflicting doc %s: not found", d.ID)
	}
	var hit struct {
		Source struct {
			ContentHash string `json:"contentHash"`
		} `json:"_source"`
	}
	if err := json.Unmarshal(raw, &hit); err != nil {
		return fmt.Errorf("%w: doc %s is undecodable: %v", errConflictMismatch, d.ID, err)
	}
	if hit.Source.ContentHash != it.hash {
		return fmt.Errorf("%w: doc %s", errConflictMismatch, d.ID)
	}
	return nil
}

func (f *flusher) nakAll(ctx context.Context, items []item, reason string) {
	for i := range items {
		jsretry.Nak(items[i].ctx, items[i].msg, jsretry.DefaultBackoff, reason)
	}
	f.metrics.events(f.cfg.source, outcomeNak, len(items))
}
