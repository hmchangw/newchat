package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
)

// resolveRoomTimesOrError calls resolveRoomTimes and translates the result for
// handler return: mongo.ErrNoDocuments → errcode.NotFound; any other error
// (e.g. a Mongo outage) fails open to zero times rather than blocking the read.
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
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, time.Time{}, errcode.NotFound("room not found")
	}
	// Fail-open: a transient room-times failure (Mongo outage) must not block a
	// read. Zero times make the walk use now as the ceiling and the configured
	// history floor as the floor — a wider but correct bucket walk.
	slog.WarnContext(ctx, "room-times unavailable, falling back to now/floor (fail-open)",
		"room_id", roomID, "error", err)
	return time.Time{}, time.Time{}, nil
}

// clockSkewTolerance bounds how far in the future a client LastMsgAt hint may sit before the
// Mongo fallback kicks in; it also pads the server-clock ceilings (walkBounds, GetThreadMessages).
const clockSkewTolerance = time.Hour

// minPlausibleEpoch rejects bogus millis (*ms == 0 → 1970) — time.UnixMilli(0) is a real time IsZero misses.
var minPlausibleEpoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// walkBounds derives the bucket-walk (ceiling, floor): ceiling = lastMsgAt, or now+skew when
// zero; floor = createdAt clamped to now-historyFloor so a stale createdAt can't widen the walk.
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
	// A lastMsgAt predating the historyFloor cap would emit ceiling < floor, which the walker
	// handles silently (empty result); clamp up so the range collapses to a single bucket.
	if ceiling.Before(floor) {
		ceiling = floor
	}
	return ceiling, floor
}

// resolveRoomTimes returns lastMsgAt/createdAt for roomID: sanitized client hints are trusted,
// missing or invalid ones fall back to Mongo. now is injected for deterministic testing.
//
// A usable lastMsgAt hint is sufficient on its own to skip Mongo entirely — createdAt only
// feeds walkBounds' floor, which is clamped to now-historyFloor, so a zero createdAt (the
// case when no hint supplied one) simply collapses the floor to that clamp. A missing or
// invalid lastMsgAt forces the per-room Mongo read (which then also fills createdAt when
// the hint didn't supply a usable one). One further case forces a read: if the hint supplies
// BOTH times but they are mutually inconsistent (createdAt later than lastMsgAt), the pair is
// re-fetched from Mongo to resolve the inconsistency (see the consistency block below).
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

	if last == nil {
		l, c, gerr := s.rooms.GetRoomTimes(ctx, roomID)
		if gerr != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("resolve room times for %s: %w", roomID, gerr)
		}
		last = &l
		if created == nil {
			created = &c
		}
	}
	if created == nil {
		created = &time.Time{}
	}

	// A merged hint+Mongo pair can be internally inconsistent (created > last): refetch from
	// Mongo when a hint was involved; if still inverted the room is genuinely empty — normalise.
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

// sanitizeLastMsgAt allows up to now+clockSkewTolerance — a fast client clock may know a newer lastMsgAt.
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
