package cassrepo

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// maxConcurrentIDReads bounds the parallel single-partition point reads issued
// by GetMessagesByIDs, keeping in-flight queries near the per-host connection
// count rather than fanning out a whole page at once.
const maxConcurrentIDReads = 16

const messageByIDQuery = "SELECT " + baseColumns + ", pinned_at, pinned_by FROM messages_by_id"

func (r *Repository) GetMessageByID(ctx context.Context, messageID string) (*models.Message, error) {
	ctx, span := r.startSpan(ctx, "cassrepo.GetMessageByID", "", messageID)
	defer span.End()

	iter := r.session.Query(
		messageByIDQuery+` WHERE message_id = ? LIMIT 1`,
		messageID,
	).WithContext(ctx).Iter()

	var m models.Message
	found, scanErr := structScan(iter, &m)
	if closeErr := iter.Close(); closeErr != nil {
		return nil, fmt.Errorf("querying message by id %s: %w", messageID, closeErr)
	}
	if scanErr != nil {
		return nil, fmt.Errorf("querying message by id %s: %w", messageID, scanErr)
	}
	if !found {
		return nil, nil
	}
	if err := r.decryptIfNeeded(ctx, &m); err != nil {
		return nil, fmt.Errorf("querying message by id %s: %w", messageID, err)
	}
	return &m, nil
}

// Missing IDs are silently omitted. Issues one token-aware single-partition
// point read per ID concurrently instead of a multi-partition IN scatter.
func (r *Repository) GetMessagesByIDs(ctx context.Context, messageIDs []string) ([]models.Message, error) {
	msgs, err := fetchByIDs(ctx, messageIDs, maxConcurrentIDReads, r.GetMessageByID)
	if err != nil {
		return nil, fmt.Errorf("querying messages by IDs: %w", err)
	}
	return msgs, nil
}

// fetchByIDs runs fetch for each id concurrently, bounded by limit, and returns
// the found messages in input order with missing (nil) results omitted. The
// first fetch error cancels the rest and is returned.
func fetchByIDs(
	ctx context.Context,
	ids []string,
	limit int,
	fetch func(context.Context, string) (*models.Message, error),
) ([]models.Message, error) {
	if len(ids) == 0 {
		return []models.Message{}, nil
	}

	results := make([]*models.Message, len(ids))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(limit)
	for i, id := range ids {
		g.Go(func() error {
			msg, err := fetch(gctx, id)
			if err != nil {
				return err
			}
			results[i] = msg // nil when not found → omitted below
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	out := make([]models.Message, 0, len(ids))
	for _, m := range results {
		if m != nil {
			out = append(out, *m)
		}
	}
	return out, nil
}
