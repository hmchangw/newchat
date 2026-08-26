package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/history-service/internal/config"
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
// The walk is never narrowed on this path, only widened: truncating it would
// make an idle room indistinguishable from an empty one, and both the history
// readers and previewAfterMutation act on that difference (see walkForPreview).
func (s *HistoryService) resolveRoomTimesOrError(
	ctx context.Context,
	roomID string,
	meta *models.RoomMeta,
	now time.Time,
) (walkTimes, error) {
	createdAt, err := s.resolveRoomTimes(ctx, roomID, meta, now)
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
	return walkTimes{createdAt: cachedCreated}, nil
}

// clockSkewTolerance bounds how far in the future a client LastMsgAt hint may sit before the
// Mongo fallback kicks in; it also pads the server-clock ceilings (walkBounds, GetThreadMessages).
const clockSkewTolerance = config.WalkCeilingSkewHours * time.Hour

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

// resolveRoomTimes returns createdAt for roomID: a sanitized client hint is trusted,
// a missing or invalid one falls back to Mongo. now is injected for deterministic testing.
//
// createdAt is the only value resolved. It feeds walkBounds' floor, which is clamped to
// now-historyFloor, so a zero value (the case when no hint supplied one and Mongo was not
// consulted) simply collapses the floor to that clamp.
//
// lastMsgAt is read but never returned — it bounds a Cassandra walk at neither end (see
// walkTimes). It survives here as the gate on the Mongo read and as the one signal that a
// hinted createdAt is incoherent: a room cannot be created after its own last message.
// A usable lastMsgAt hint is therefore sufficient to skip Mongo entirely; a missing or
// invalid one forces the read, and an inconsistent pair (createdAt later than lastMsgAt)
// forces it too, to settle the inconsistency authoritatively.
func (s *HistoryService) resolveRoomTimes(
	ctx context.Context,
	roomID string,
	meta *models.RoomMeta,
	now time.Time,
) (createdAt time.Time, err error) {
	var last, created *time.Time
	if meta != nil {
		last = sanitizeLastMsgAt(meta.LastMsgAt, now)
		created = sanitizeCreatedAt(meta.CreatedAt, now)
	}

	// fromMongo records that this call already holds an authoritative pair, so the
	// consistency check below cannot re-read the document it just read.
	fromMongo := false
	if last == nil {
		l, c, gerr := s.rooms.GetRoomTimes(ctx, roomID)
		if gerr != nil {
			return time.Time{}, fmt.Errorf("resolve room times for %s: %w", roomID, gerr)
		}
		// Seeding is the repository decorator's job, beneath the process-local room
		// cache (history-service/cmd/roomtimesSeeder). Storing from here would write
		// Valkey on every request that reaches this branch, including the hits that
		// cache exists to serve.
		//
		// Mongo's createdAt wins over the hint's rather than only filling a gap: the
		// value is immutable, so a hint that disagrees is stale, and taking c also
		// settles the inconsistency below without a second read.
		last, created = &l, &c
		fromMongo = true
	}
	if created == nil {
		created = &time.Time{}
	}

	// A hinted pair can be internally inconsistent (created > last, impossible for a
	// real room): settle it against Mongo. Skipped when the pair already came from
	// Mongo — a never-messaged room has createdAt > a zero lastMsgAt legitimately,
	// and re-reading the same document would only return the same two values.
	if !fromMongo && created.After(*last) {
		_, c, gerr := s.rooms.GetRoomTimes(ctx, roomID)
		if gerr != nil {
			return time.Time{}, fmt.Errorf("resolve room times for %s (consistency refetch): %w", roomID, gerr)
		}
		created = &c
	}

	return *created, nil
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
