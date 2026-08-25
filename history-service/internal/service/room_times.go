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

// walkTimes is what a bucket walk needs in order to bound itself, plus how the
// value was obtained.
//
// There is deliberately no lastMsgAt here. It bounds neither end of a Cassandra
// walk: as a ceiling it hides rows the lagging pointer has not caught up to
// (see walkBounds), and as a floor it says nothing about older messages. The
// walk's ceiling comes from the clock and its floor from createdAt.
type walkTimes struct {
	// createdAt bounds the walk's floor. A room's creation time is immutable,
	// so a cached value is as good as a fresh one at any age.
	createdAt time.Time
	// degraded reports that the fail-open path was taken.
	degraded bool
}

// resolveRoomTimesOrError calls resolveRoomTimes and translates the result for
// handler return: mongo.ErrNoDocuments → errcode.NotFound; any other error
// (e.g. a Mongo outage) fails open rather than blocking the read.
//
// On the fail-open path it consults the room-times L2 tier, which narrows the
// widest legal walk without narrowing what the caller can see: the cached
// createdAt raises the floor, and nothing touches the ceiling, which stays
// unknown so walkBounds widens it to now. Anything written during the outage
// sits under that ceiling and is still reached.
//
// degraded is what a caller that would otherwise inherit the widest legal walk
// uses to narrow it instead. Only the batched preview path
// (roomLastPreviewMessage) acts on it; the explicit per-room readers ignore it
// and keep the full configured walk, because truncating history a caller asked
// for by name is worse than a slow read.
func (s *HistoryService) resolveRoomTimesOrError(
	ctx context.Context,
	roomID string,
	meta *models.RoomMeta,
	now time.Time,
) (walkTimes, error) {
	_, createdAt, err := s.resolveRoomTimes(ctx, roomID, meta, now)
	if err == nil {
		return walkTimes{createdAt: createdAt}, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return walkTimes{}, errcode.NotFound("room not found")
	}
	// A cancelled or timed-out CALLER is not what fail-open is for. Failing open
	// here would send the request on to walk a year of Cassandra buckets to
	// build a response nobody is waiting for — the widened walk is precisely the
	// expensive path. Surface the cancellation instead, before spending a Valkey
	// round trip on a floor nobody will use. Checked on ctx rather than err so a
	// store that swallows the cause is still caught.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return walkTimes{}, fmt.Errorf("resolve room times for %s: %w", roomID, ctxErr)
	}
	// Fail-open: a transient room-times failure (Mongo outage) must not block a
	// read. Without a cached entry, zero times make the walk use now as the
	// ceiling and the configured history floor as the floor — wider, but correct.
	cachedCreated, found := s.roomTimes.Fallback(ctx, roomID)
	slog.WarnContext(ctx, "room-times unavailable, falling back (fail-open)",
		"room_id", roomID, "error", err, "l2_floor", found)
	return walkTimes{createdAt: cachedCreated, degraded: true}, nil
}

// clockSkewTolerance bounds how far in the future a client LastMsgAt hint may sit before the
// Mongo fallback kicks in; it also pads the server-clock ceilings (walkBounds, GetThreadMessages).
const clockSkewTolerance = time.Hour

// minPlausibleEpoch rejects bogus millis (*ms == 0 → 1970) — time.UnixMilli(0) is a real time IsZero misses.
var minPlausibleEpoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// walkBounds derives the bucket-walk (ceiling, floor).
//
// The ceiling is the server clock plus skew tolerance, never rooms.lastMsgAt.
// That pointer is written by unread-worker on a coalescing flush, on a consumer
// separate from the message-worker that writes the Cassandra row, so it trails
// the data by at least one flush interval — and without bound whenever that
// worker is backlogged or down. A ceiling taken from it silently omits rows
// that already exist: post a message, watch it broadcast, reload history, and
// it is missing. threads.go bounds itself by the clock alone for the same
// reason. Nothing can be created after now+skew, so this ceiling cannot hide a
// row, and it costs only the empty buckets between it and the newest one, which
// the walk's adaptive fan-out crosses in a few concurrent waves.
//
// floor = createdAt clamped to now-historyFloor so a stale createdAt can't widen the walk.
func (s *HistoryService) walkBounds(createdAt, now time.Time) (ceiling, floor time.Time) {
	ceiling = now.Add(clockSkewTolerance)
	historyFloor := now.Add(-s.historyFloor)
	floor = createdAt
	if floor.IsZero() || floor.Before(historyFloor) {
		floor = historyFloor
	}
	return ceiling, floor
}

// clampToCeiling bounds a client-supplied DESC upper bound to the server clock.
//
// A DESC walk starts at this time's bucket and descends, so an unclamped future
// value starts it far above any row and spends the whole MESSAGE_READ_MAX_BUCKETS
// budget crossing empties before reaching real data — client-controlled waste.
// Nothing can be created after now+skew, so clamping there cannot hide a row.
//
// Until 59cb66f the lastMsgAt cap masked this by clamping every bound down to
// the last known message; removing that unsound ceiling left the bogus-input
// case uncovered, which is what this restores.
func clampToCeiling(t, now time.Time) time.Time {
	if ceiling := now.Add(clockSkewTolerance); t.After(ceiling) {
		return ceiling
	}
	return t
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
		// Seeded here, at the only place an authoritative answer arrives. A
		// client hint short-circuits this read, and must never reach the shared
		// tier: it would become another reader's degraded walk floor.
		s.roomTimes.Store(ctx, roomID, c)
		last = &l
		if created == nil {
			created = &c
		}
	}
	if created == nil {
		created = &time.Time{}
	}

	// A merged hint+Mongo pair can be internally inconsistent (created > last): refetch from
	// Mongo when a hint was involved; if still inverted with a real lastMsgAt, normalise.
	if created.After(*last) {
		if metaLast || metaCreated {
			l, c, gerr := s.rooms.GetRoomTimes(ctx, roomID)
			if gerr != nil {
				return time.Time{}, time.Time{}, fmt.Errorf("resolve room times for %s (consistency refetch): %w", roomID, gerr)
			}
			s.roomTimes.Store(ctx, roomID, c)
			last = &l
			created = &c
		}
		// Still inverted with a real lastMsgAt — corrupt pair; collapse the
		// range. A zero lastMsgAt stays zero: "not recorded" means UNKNOWN,
		// not "empty room" — the room may hold messages (legacy docs, failed
		// lastMsgAt update). Nothing bounds a walk with it either way — the
		// ceiling is the clock — so zero is simply carried through.
		if !last.IsZero() && created.After(*last) {
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
