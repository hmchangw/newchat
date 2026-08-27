package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// roomActivityRefresh is one room's coalesced position, handed to the publisher
// so destination sites can order a room they hold no rooms doc for.
type roomActivityRefresh struct {
	roomID string
	at     time.Time
}

// roomActivityRefresher announces a room's position cross-site at most once per
// room per interval.
//
// It hangs off the fan-out path rather than a Mongo write batch: broadcast-worker
// no longer writes rooms.lastMsgAt (roomlist-worker owns that now), so there is no
// batch left to ride. Nothing is lost by the move — the announce still happens
// exactly once per created message, the handler has already resolved the room's
// meta so the cross-site test costs no read, and the throttle rather than the
// write cadence was always what governed the announce rate.
//
// A nil refresher is inert, which is how a single-site deployment disables it.
type roomActivityRefresher struct {
	publish  func(context.Context, roomActivityRefresh) error
	interval time.Duration
	now      func() time.Time

	mu            sync.Mutex
	lastRefreshed map[string]time.Time
	lastPruned    time.Time
}

func newRoomActivityRefresher(publish func(context.Context, roomActivityRefresh) error, interval time.Duration) *roomActivityRefresher {
	return &roomActivityRefresher{
		publish:       publish,
		interval:      interval,
		lastRefreshed: make(map[string]time.Time),
		now:           time.Now,
	}
}

// refresh announces roomID's position, subject to the cross-site test and the
// per-room throttle. crossSite is the room meta's tri-state: an unclassified
// room (nil) counts as cross-site, since the cost of guessing wrong is one
// wasted publish and the destination ignores rooms it holds no subscription for.
//
// Cost scales with distinct active cross-site rooms per interval, not with
// message rate. The interval can be generous because the consumer cannot see the
// difference: the subscription list serves ordering from a cache whose own TTL is
// an order of magnitude longer, so a position a few seconds behind is
// indistinguishable from a fresh one. The cost of the throttle is that a room
// going quiet right after a burst keeps the position from its last announce —
// stale by at most one interval, and repaired by its next message.
//
// Failures are logged, never returned: the position is decorative, the next
// message re-establishes it, and returning would NAK a message already
// broadcast to every client in the room.
func (r *roomActivityRefresher) refresh(ctx context.Context, roomID string, crossSite *bool, at time.Time) {
	if r == nil || r.publish == nil {
		return
	}
	if crossSite != nil && !*crossSite {
		return
	}
	now := r.now()
	if r.throttled(roomID, now) {
		return
	}
	if err := r.publish(ctx, roomActivityRefresh{roomID: roomID, at: at}); err != nil {
		slog.WarnContext(ctx, "publish room activity refresh failed",
			"room_id", roomID, "error", err,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
}

// throttled reports whether roomID was announced within interval, and records
// the announce when it was not. Pruning is amortized to once per interval
// rather than run on every message: the map then stays bounded by rooms active
// within roughly two intervals, without an O(rooms) sweep on the hot path.
func (r *roomActivityRefresher) throttled(roomID string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.interval <= 0 {
		return false
	}
	if last, ok := r.lastRefreshed[roomID]; ok && now.Sub(last) < r.interval {
		return true
	}
	r.lastRefreshed[roomID] = now
	if now.Sub(r.lastPruned) >= r.interval {
		// Advance on a complete pass, and equally on one that reclaimed nothing:
		// such a pass has no remainder to resume, so leaving the watermark back
		// would re-scan the budget on every later message. See pruneLocked.
		if complete, reclaimed := r.pruneLocked(now); complete || reclaimed == 0 {
			r.lastPruned = now
		}
	}
	return false
}

// pruneScanBudget caps how many watermarks one prune pass looks at. Pruning runs
// under the mutex every handler needs, so an unbounded pass blocks concurrent
// messages for as long as the room count is large. A fixed budget keeps the lock
// hold constant. Whatever is left is picked up next interval.
const pruneScanBudget = 256

// pruneLocked drops rooms that have gone quiet for longer than the interval;
// their next message re-announces immediately, so keeping them would only leak
// memory. Callers must hold r.mu.
//
// It examines at most pruneScanBudget entries per pass and reports whether it
// reached the end of the map, plus how many watermarks it reclaimed. A partial
// pass that reclaimed something deliberately does NOT advance lastPruned, so the
// next message resumes the sweep rather than waiting another interval — Go
// randomizes map iteration order, so successive partial passes cover the map
// without needing a cursor. The bound trades a slower sweep for a lock nobody
// waits on; the map's size is what the sweep is protecting anyway, and it stays
// bounded by rooms active within one interval.
//
// The reclaimed count is what stops that resume rule degenerating. With more
// rooms active than the budget and none of them stale, a pass can never complete
// — it scans the budget, deletes nothing, reports incomplete, and the caller
// would re-run it on every single message, under the mutex the whole fan-out
// shares. That is the opposite of amortizing, and it bites at ordinary traffic:
// 256 rooms with a message inside one interval is not a stress case. A pass with
// nothing to reclaim has no remainder to come back for, so the caller treats it
// as done for the interval.
func (r *roomActivityRefresher) pruneLocked(now time.Time) (complete bool, reclaimed int) {
	scanned := 0
	for roomID, last := range r.lastRefreshed {
		if scanned == pruneScanBudget {
			return false, reclaimed
		}
		scanned++
		if now.Sub(last) >= r.interval {
			delete(r.lastRefreshed, roomID)
			reclaimed++
		}
	}
	return true, reclaimed
}

// remotePeers returns the sites to refresh: everything in allSiteIDs except this
// one, blanks dropped and duplicates collapsed. Empty means single-site, which
// disables the refresh.
func remotePeers(siteID string, allSiteIDs []string) []string {
	seen := make(map[string]struct{}, len(allSiteIDs))
	var peers []string
	for _, s := range allSiteIDs {
		if s == "" || s == siteID {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		peers = append(peers, s)
	}
	return peers
}

// roomActivityPublisher emits one refresh per peer site on the core-NATS
// activity lane. One room per message keeps the payload bounded by
// construction — a batched form would grow with the number of active rooms and
// eventually exceed max_payload, silently losing a whole window.
func roomActivityPublisher(p Publisher, siteID string, peers []string) func(context.Context, roomActivityRefresh) error {
	return func(ctx context.Context, r roomActivityRefresh) error {
		evt := model.RoomActivityEvent{
			RoomID:    r.roomID,
			SiteID:    siteID,
			LastMsgAt: r.at.UTC().UnixMilli(),
			Timestamp: time.Now().UTC().UnixMilli(),
		}
		data, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("marshal room activity event: %w", err)
		}
		// Every peer is attempted: one unreachable site must not deprive the
		// others of the refresh, since a skipped peer keeps a stale position for
		// the whole throttle interval.
		var errs []error
		for _, peer := range peers {
			if err := p.Publish(ctx, subject.RoomActivity(peer), data); err != nil {
				errs = append(errs, fmt.Errorf("publish room activity to %s: %w", peer, err))
			}
		}
		return errors.Join(errs...)
	}
}
