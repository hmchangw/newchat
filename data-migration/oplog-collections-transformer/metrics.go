package main

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// metrics holds the transformer's instruments: processed throughput, nak/term/exhausted
// dispositions, user-seed outcome, and FK resolution misses. Nil-safe (tests run without a meter).
type metrics struct {
	processed   metric.Int64Counter
	naks        metric.Int64Counter
	terms       metric.Int64Counter
	skipped     metric.Int64Counter
	exhausted   metric.Int64Counter
	userSeed    metric.Int64Counter
	resolveMiss metric.Int64Counter
	writes      metric.Int64Counter
}

func newMetrics() (*metrics, error) {
	m := otel.Meter("oplog-collections-transformer")
	processed, err := m.Int64Counter("oplog_collections_transformer_events_processed_total",
		metric.WithDescription("oplog events handled and acked, by op"))
	if err != nil {
		return nil, fmt.Errorf("processed counter: %w", err)
	}
	naks, err := m.Int64Counter("oplog_collections_transformer_naks_total",
		metric.WithDescription("transient failures naked for redelivery, by op"))
	if err != nil {
		return nil, fmt.Errorf("naks counter: %w", err)
	}
	terms, err := m.Int64Counter("oplog_collections_transformer_terms_total",
		metric.WithDescription("poison/undecodable events termed (never redelivered), by op"))
	if err != nil {
		return nil, fmt.Errorf("terms counter: %w", err)
	}
	skipped, err := m.Int64Counter("oplog_collections_transformer_events_skipped_total",
		metric.WithDescription("events deliberately skipped (excluded room type, out-of-scope collection etc.), by reason"))
	if err != nil {
		return nil, fmt.Errorf("skipped counter: %w", err)
	}
	exhausted, err := m.Int64Counter("oplog_collections_transformer_exhausted_total",
		metric.WithDescription("events termed after reaching MaxDeliver (would-be silent JetStream drops), by op"))
	if err != nil {
		return nil, fmt.Errorf("exhausted counter: %w", err)
	}
	userSeed, err := m.Int64Counter("oplog_collections_transformer_user_seed_total",
		metric.WithDescription("user insert-if-absent seeds, by outcome (insert/present)"))
	if err != nil {
		return nil, fmt.Errorf("user seed counter: %w", err)
	}
	resolveMiss, err := m.Int64Counter("oplog_collections_transformer_resolve_miss_total",
		metric.WithDescription("foreign-key resolution misses (thread-sub user/thread_room, room-member user), by kind"))
	if err != nil {
		return nil, fmt.Errorf("resolve miss counter: %w", err)
	}
	writes, err := m.Int64Counter("oplog_collections_transformer_writes_total",
		metric.WithDescription("direct target writes, by collection+action"))
	if err != nil {
		return nil, fmt.Errorf("writes counter: %w", err)
	}
	return &metrics{
		processed: processed, naks: naks, terms: terms, skipped: skipped,
		exhausted: exhausted, userSeed: userSeed, resolveMiss: resolveMiss, writes: writes,
	}, nil
}

// opCollAttr labels a disposition by op (insert/update/delete) and source collection, so ops can
// see which collection is stuck or poisoning — not just the aggregate op breakdown.
func opCollAttr(op, collection string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("op", op), attribute.String("collection", collection))
}

func (m *metrics) onProcessed(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.processed.Add(ctx, 1, opCollAttr(op, collection))
}

func (m *metrics) onNak(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.naks.Add(ctx, 1, opCollAttr(op, collection))
}

func (m *metrics) onTerm(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.terms.Add(ctx, 1, opCollAttr(op, collection))
}

func (m *metrics) onSkipped(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	m.skipped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// onExhausted records an event termed because it reached MaxDeliver — what would otherwise be a
// silent JetStream-side drop after the redelivery cap. Distinct from poison terms; alert on it.
func (m *metrics) onExhausted(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.exhausted.Add(ctx, 1, opCollAttr(op, collection))
}

// onUserSeed records a user insert-if-absent seed, labelled "insert" (a new doc was created) or
// "present" (another sync already owns the account, left untouched).
func (m *metrics) onUserSeed(ctx context.Context, outcome string) {
	if m == nil {
		return
	}
	m.userSeed.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// onResolveMiss records a foreign-key resolution miss for the thread-sub double dependency
// (kind "user" or "thread_room"); used by the later thread-sub mapper to flag Nak retries.
func (m *metrics) onResolveMiss(ctx context.Context, kind string) {
	if m == nil {
		return
	}
	m.resolveMiss.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", kind)))
}

// onWrite records a direct target write (room-member upsert/delete), labelled by collection and
// action ("upsert", "delete", or "delete_noop" for a delete that matched no row).
func (m *metrics) onWrite(ctx context.Context, collection, action string) {
	if m == nil {
		return
	}
	m.writes.Add(ctx, 1, metric.WithAttributes(attribute.String("collection", collection), attribute.String("action", action)))
}
