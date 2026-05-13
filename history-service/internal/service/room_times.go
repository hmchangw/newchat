package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// resolveRoomTimesOrError invokes resolveRoomTimes and translates the result
// into a natsrouter error suitable for handler return: a wrapped
// mongo.ErrNoDocuments becomes ErrNotFound (the room genuinely does not
// exist), anything else becomes ErrInternal. The raw error is logged
// server-side; only the sanitized RouteError is returned to clients.
func (s *HistoryService) resolveRoomTimesOrError(
	ctx context.Context,
	roomID string,
	meta *models.RoomMeta,
	now time.Time,
) (lastMsgAt, createdAt time.Time, err error) {
	lastMsgAt, createdAt, err = s.resolveRoomTimes(ctx, roomID, meta, now)
	if err == nil {
		return lastMsgAt, createdAt, nil
	}
	slog.Error("resolve room times", "error", err, "roomID", roomID)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, time.Time{}, natsrouter.ErrNotFound("room not found")
	}
	return time.Time{}, time.Time{}, natsrouter.ErrInternal("failed to resolve room metadata")
}

// clockSkewTolerance allows clients with mildly out-of-sync clocks to still
// have their LastMsgAt hint accepted. Anything further out is treated as
// suspicious and triggers a Mongo fallback.
const clockSkewTolerance = time.Hour

// minPlausibleEpoch rejects clearly-bogus millis (e.g. *ms == 0 → 1970-01-01)
// without imposing tight bounds on real-world clock skew. time.Time{}.IsZero()
// does NOT match time.UnixMilli(0) — the latter is unix epoch, a real time.
var minPlausibleEpoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// walkBounds derives the (ceiling, floor) bucket bounds used by ASC and
// surrounding-walk handlers from the resolved lastMsgAt/createdAt. Falls back
// to now+clockSkewTolerance for the ceiling when lastMsgAt is zero. The floor
// is clamped to be no older than now-historyFloor: a client-supplied or
// MongoDB-stored createdAt that predates the history-floor cap would otherwise
// allow a single read to walk further back than configured, amplifying empty-
// partition queries on year-old rooms.
func (s *HistoryService) walkBounds(lastMsgAt, createdAt, now time.Time) (ceiling, floor time.Time) {
	ceiling = lastMsgAt
	if ceiling.IsZero() {
		ceiling = now.Add(clockSkewTolerance)
	}
	historyFloor := now.Add(-s.historyFloor)
	floor = createdAt
	if floor.IsZero() || floor.Before(historyFloor) {
		floor = historyFloor
	}
	// Guard against inverted ranges: a room whose lastMsgAt predates the
	// configured historyFloor cap would otherwise emit ceiling < floor, which
	// the walker handles silently (empty result, no cursor). Clamp ceiling up
	// to floor so the range collapses to a single bucket instead of inverted.
	if ceiling.Before(floor) {
		ceiling = floor
	}
	return ceiling, floor
}

// resolveRoomTimes returns lastMsgAt and createdAt for roomID. Client-supplied
// meta are trusted after sanity checks; missing or invalid meta fall back to
// Mongo via the RoomTimeResolver. now is injected for deterministic testing.
func (s *HistoryService) resolveRoomTimes(
	ctx context.Context,
	roomID string,
	meta *models.RoomMeta,
	now time.Time,
) (lastMsgAt, createdAt time.Time, err error) {
	var last, created *time.Time
	var metaLast, metaCreated bool
	if meta != nil {
		if v := sanitizeLastMsgAt(meta.LastMsgAt, now); v != nil {
			last = v
			metaLast = true
		}
		if v := sanitizeCreatedAt(meta.CreatedAt, now); v != nil {
			created = v
			metaCreated = true
		}
	}

	if last == nil || created == nil {
		l, c, gerr := s.rooms.GetRoomTimes(ctx, roomID)
		if gerr != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("resolve room times for %s: %w", roomID, gerr)
		}
		if last == nil {
			last = &l
		}
		if created == nil {
			created = &c
		}
	}

	// Mixed sources can produce inconsistent values (e.g. a stale hint LastMsgAt
	// older than a Mongo-fetched CreatedAt). When the merged pair is internally
	// inconsistent — created > last — refetch from Mongo IF at least one value
	// came from a hint, so we get a coherent snapshot. When both already came
	// from Mongo (no meta in play) the snapshot is by definition coherent
	// already; an inverted result there means the room is genuinely empty
	// (lastMsgAt unset), so we just normalise last = created.
	if created.After(*last) {
		if metaLast || metaCreated {
			l, c, gerr := s.rooms.GetRoomTimes(ctx, roomID)
			if gerr != nil {
				return time.Time{}, time.Time{}, fmt.Errorf("resolve room times for %s (consistency refetch): %w", roomID, gerr)
			}
			last = &l
			created = &c
		}
		// Empty room or hint-refetch still inconsistent — collapse the range.
		if created.After(*last) {
			last = created
		}
	}

	return *last, *created, nil
}

// sanitizeLastMsgAt allows up to now+clockSkewTolerance because clients with
// slightly fast clocks may legitimately have a more recent lastMsgAt than the
// server's "now" — the actual message could already exist on disk.
func sanitizeLastMsgAt(ms *int64, now time.Time) *time.Time {
	if ms == nil {
		return nil
	}
	t := time.UnixMilli(*ms).UTC()
	if t.Before(minPlausibleEpoch) {
		return nil
	}
	if t.After(now.Add(clockSkewTolerance)) {
		return nil
	}
	return &t
}

// sanitizeCreatedAt rejects any future value (no skew tolerance): a room
// cannot legitimately be created in the future, even with clock drift.
func sanitizeCreatedAt(ms *int64, now time.Time) *time.Time {
	if ms == nil {
		return nil
	}
	t := time.UnixMilli(*ms).UTC()
	if t.Before(minPlausibleEpoch) {
		return nil
	}
	if t.After(now) {
		return nil
	}
	return &t
}
