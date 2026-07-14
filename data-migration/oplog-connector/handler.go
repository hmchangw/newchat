package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	defaultInitialBackoff   = 200 * time.Millisecond
	defaultMaxBackoff       = 30 * time.Second
	defaultCheckpointMaxAge = 30 * time.Second
)

// publisher is the minimal JetStream publish surface the watcher needs.
type publisher interface {
	PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// changeSource yields decoded change events in oplog order; Next blocks until the next event, returning a wrapped context.Canceled when stopped (a graceful stop).
type changeSource interface {
	Next(ctx context.Context) (changeEvent, error)
	Close(ctx context.Context) error
}

// checkpointer coalesces checkpoint writes from the read loop (count-based) and the periodic flusher (time-based), de-duping by last-saved eventID so no frontier writes twice.
type checkpointer struct {
	store CheckpointStore

	mu        sync.Mutex
	pending   *Checkpoint
	lastSaved string
}

func (c *checkpointer) record(cp *Checkpoint) {
	c.mu.Lock()
	c.pending = cp
	c.mu.Unlock()
}

// flush persists the pending frontier if it has advanced since the last save.
func (c *checkpointer) flush(ctx context.Context) error {
	c.mu.Lock()
	cp := c.pending
	if cp == nil || cp.EventID == c.lastSaved {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if err := c.store.Save(ctx, cp); err != nil {
		return err
	}
	c.mu.Lock()
	c.lastSaved = cp.EventID
	c.mu.Unlock()
	return nil
}

// watcher runs the per-collection pipeline: read → publish (block on pub-ack) → checkpoint.
// One watcher/connection per collection, so order holds and the checkpoint never passes an un-acked event (lossless; dups dedup on Nats-Msg-Id).
type watcher struct {
	siteID     string
	collection string
	source     changeSource
	pub        publisher
	store      CheckpointStore

	checkpointEvery  int
	checkpointMaxAge time.Duration
	initialBackoff   time.Duration
	maxBackoff       time.Duration
	now              func() int64 // unix ms; injectable for tests
	metrics          *metrics     // nil-safe; set by start(), nil in unit tests
	log              *slog.Logger
}

func newWatcher(siteID, collection string, src changeSource, pub publisher, store CheckpointStore, checkpointEvery int, checkpointMaxAge time.Duration) *watcher {
	if checkpointMaxAge <= 0 {
		checkpointMaxAge = defaultCheckpointMaxAge
	}
	return &watcher{
		siteID:           siteID,
		collection:       collection,
		source:           src,
		pub:              pub,
		store:            store,
		checkpointEvery:  checkpointEvery,
		checkpointMaxAge: checkpointMaxAge,
		initialBackoff:   defaultInitialBackoff,
		maxBackoff:       defaultMaxBackoff,
		now:              func() int64 { return time.Now().UTC().UnixMilli() },
		log:              slog.With("collection", collection),
	}
}

// run drives the watcher until ctx is cancelled (graceful: nil after a final checkpoint) or
// a fatal error (caller exits). A lost resume token (Mongo 286) is fatal — reseed-from-now would drop events.
func (w *watcher) run(ctx context.Context) error {
	defer func() {
		// Best-effort close, detached from the (possibly cancelled) ctx.
		_ = w.source.Close(context.WithoutCancel(ctx))
	}()

	cps := &checkpointer{store: w.store}

	// Flush the latest frontier every checkpointMaxAge so progress survives a crash
	// even below checkpointEvery — bounds replay (RPO) by wall-clock for low-volume collections.
	flushCtx, stopFlush := context.WithCancel(ctx)
	var flushWG sync.WaitGroup
	flushWG.Go(func() {
		t := time.NewTicker(w.checkpointMaxAge)
		defer t.Stop()
		for {
			select {
			case <-flushCtx.Done():
				return
			case <-t.C:
				if err := cps.flush(flushCtx); err != nil {
					w.log.Error("periodic checkpoint save failed", "error", err)
				}
			}
		}
	})
	// Stop the flusher and persist the final frontier on any exit path.
	defer func() {
		stopFlush()
		flushWG.Wait()
		if err := cps.flush(context.WithoutCancel(ctx)); err != nil {
			w.log.Error("final checkpoint save failed", "error", err)
		}
	}()

	sinceSave := 0
	for {
		ev, err := w.source.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil // graceful — deferred flush persists the final frontier
			}
			if isHistoryLost(err) {
				return fmt.Errorf("resume token lost for %q — operator reseed required (history lost): %w", w.collection, err)
			}
			return fmt.Errorf("read change stream %q: %w", w.collection, err)
		}

		if err := w.publishWithRetry(ctx, &ev); err != nil {
			return nil // only ctx cancellation breaks the retry loop — graceful
		}

		cps.record(&Checkpoint{
			SiteID:      w.siteID,
			Collection:  w.collection,
			ResumeToken: ev.ResumeToken,
			ClusterTime: ev.ClusterTimeMs,
			EventID:     ev.EventID,
			Source:      "runtime",
			UpdatedAt:   w.now(),
		})
		sinceSave++
		if sinceSave >= w.checkpointEvery {
			if err := cps.flush(ctx); err != nil {
				// Non-fatal: a failed checkpoint only means more replay on crash
				// (deduped), never loss. Keep going and retry next interval.
				w.log.Error("checkpoint save failed", "eventId", ev.EventID, "error", err)
			} else {
				sinceSave = 0
			}
		}
	}
}

// publishWithRetry publishes one event synchronously, retrying with capped backoff until pub-ack or ctx cancel. EventID (the dedup id) is guaranteed non-empty upstream; a field that fails to encode is published degraded (not dropped) so the stream stays lossless.
func (w *watcher) publishWithRetry(ctx context.Context, ev *changeEvent) error {
	subj, msgID, evt := buildEnvelope(ev, w.siteID, w.now())
	data, err := json.Marshal(evt)
	if err != nil {
		// This event is DROPPED (never reaches the stream), so name the op — eventId alone
		// can't be looked up via Nats-Msg-Id like published events can.
		w.log.Error("marshal oplog event failed — skipping event", "eventId", ev.EventID, "op", ev.Op, "error", err)
		w.metrics.onSkipped(ctx, w.collection)
		return nil
	}

	msg := &nats.Msg{Subject: subj, Data: data, Header: nats.Header{}}
	msg.Header.Set("Nats-Msg-Id", msgID)

	backoff := w.initialBackoff
	for {
		_, err := w.pub.PublishMsg(ctx, msg)
		if err == nil {
			if evt.Degraded {
				w.log.Warn("published degraded event", "eventId", evt.EventID, "reason", evt.DegradedReason)
				w.metrics.onDegraded(ctx, w.collection)
			}
			w.metrics.onPublished(ctx, w.collection, w.now()-ev.ClusterTimeMs)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.metrics.onPublishError(ctx, w.collection)
		w.log.Error("publish failed — retrying", "eventId", msgID, "backoff", backoff.String(), "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, w.maxBackoff)
	}
}

// isHistoryLost reports whether err is a Mongo ChangeStreamHistoryLost (286),
// meaning the resume token is no longer in the oplog and a reseed is required.
func isHistoryLost(err error) bool {
	var se mongo.ServerError
	if errors.As(err, &se) {
		return se.HasErrorCode(286)
	}
	return false
}
