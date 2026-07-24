package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// runWithWorkerPool runs fn once per item with at most concurrency
// goroutines in flight. Returns the first error from any fn call; the
// group's derived context is canceled on that first error so in-flight and
// not-yet-started work stop promptly. fn is expected to slog.Error its own
// failure before returning it — errgroup keeps only the first error and
// silently drops the rest, so without its own log line a failure past the
// first vanishes with no trace.
func runWithWorkerPool[T any](ctx context.Context, concurrency int, items []T, fn func(ctx context.Context, item T) error) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for _, item := range items {
		g.Go(func() error {
			return fn(gctx, item)
		})
	}
	return g.Wait()
}

// runMessages iterates every room the site's subscriptions reference,
// streams its Cassandra messages in [MigrationStartAt, MigrationEndAt),
// and buffers a versioned index action per message. A room's read error
// aborts that room's worker but not the others; the run's overall error is
// still the first one seen (via runWithWorkerPool). Flush always runs
// after the worker pool, even when it returned an error, via errors.Join —
// any actions already buffered by the rooms that succeeded must reach ES
// rather than being silently discarded on a later room's failure.
//
// f is fed by up to WorkerConcurrency room workers concurrently; mu
// serializes every f.Add call, matching flusher.go's documented contract
// that it is not safe for concurrent use without external synchronization.
//
//nolint:gocritic // hugeParam: cfg is passed by value to match the task API contract (also consumed by main.go, Task 15); struct copy overhead is acceptable, not a hot path
func runMessages(ctx context.Context, subs SubscriptionSource, messages MessageSource, f *flusher, cfg config) error {
	roomIDs, err := subs.RoomIDs(ctx, cfg.SiteID)
	if err != nil {
		return fmt.Errorf("list rooms for site %s: %w", cfg.SiteID, err)
	}

	var mu sync.Mutex
	runErr := runWithWorkerPool(ctx, cfg.WorkerConcurrency, roomIDs, func(ctx context.Context, roomID string) error {
		err := messages.StreamMessages(ctx, cfg.SiteID, roomID, cfg.MigrationStartAt, cfg.MigrationEndAt, func(msg cassandra.Message) error {
			action, err := buildMessageAction(msg, cfg.MsgIndexPrefix)
			if err != nil {
				slog.Error("skip message: build action failed", "roomId", roomID, "error", err)
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			return f.Add(ctx, action)
		})
		if err != nil {
			wrapped := fmt.Errorf("stream messages for room %s: %w", roomID, err)
			slog.Error("room aborted", "roomId", roomID, "error", wrapped)
			return wrapped
		}
		return nil
	})

	return errors.Join(runErr, f.Flush(ctx))
}

// runSpotlight reads every current subscription for the site and buffers
// one versioned spotlight action per row (see Global Constraints: every
// row is an active membership, so this is always the index path — there
// is no delete path to reconstruct from a point-in-time subscriptions
// read). Flush always runs, matching runMessages' errors.Join reasoning.
//
// f is fed by up to WorkerConcurrency subscription workers concurrently;
// mu serializes every f.Add call per flusher.go's documented contract.
//
//nolint:gocritic // hugeParam: cfg is passed by value to match the task API contract (also consumed by main.go, Task 15); struct copy overhead is acceptable, not a hot path
func runSpotlight(ctx context.Context, subs SubscriptionSource, f *flusher, cfg config) error {
	rows, err := subs.Subscriptions(ctx, cfg.SiteID)
	if err != nil {
		return fmt.Errorf("read subscriptions for site %s: %w", cfg.SiteID, err)
	}

	var mu sync.Mutex
	runErr := runWithWorkerPool(ctx, cfg.WorkerConcurrency, rows, func(ctx context.Context, sub model.Subscription) error {
		action, err := buildSpotlightAction(sub, cfg.SpotlightIndex)
		if err != nil {
			slog.Error("skip subscription: build spotlight action failed", "subscriptionId", sub.ID, "error", err)
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		return f.Add(ctx, action)
	})

	return errors.Join(runErr, f.Flush(ctx))
}

// runUserRoom reads every current subscription for the site and buffers
// one scripted user-room update per row (bot subscriptions skipped inside
// buildUserRoomAction). Flush always runs, matching runMessages'
// errors.Join reasoning.
//
// f is fed by up to WorkerConcurrency subscription workers concurrently;
// mu serializes every f.Add call per flusher.go's documented contract.
//
//nolint:gocritic // hugeParam: cfg is passed by value to match the task API contract (also consumed by main.go, Task 15); struct copy overhead is acceptable, not a hot path
func runUserRoom(ctx context.Context, subs SubscriptionSource, f *flusher, cfg config) error {
	rows, err := subs.Subscriptions(ctx, cfg.SiteID)
	if err != nil {
		return fmt.Errorf("read subscriptions for site %s: %w", cfg.SiteID, err)
	}

	var mu sync.Mutex
	runErr := runWithWorkerPool(ctx, cfg.WorkerConcurrency, rows, func(ctx context.Context, sub model.Subscription) error {
		action, err := buildUserRoomAction(sub, cfg.UserRoomIndex)
		if err != nil {
			slog.Error("skip subscription: build user-room action failed", "subscriptionId", sub.ID, "error", err)
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		return f.Add(ctx, action)
	})

	return errors.Join(runErr, f.Flush(ctx))
}
