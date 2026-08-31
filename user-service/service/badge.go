package service

import (
	"log/slog"
	"sync"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// BadgeCountBatch returns each account's badge unread-room count (capped at
// BADGE_COUNT_CAP) for a notification in req.RoomID. Cache hit: one pipelined SADD+SCARD
// batch for all accounts. Miss: seed from unreadRooms (Mongo + cross-site
// room RPCs), MAX_BADGE_SEED_FANOUT accounts at a time, ∪ the trigger room.
// Per-account failures degrade to absence — the push must never block.
// NATS: chat.server.request.user.{siteID}.badge.count.batch
func (s *UserService) BadgeCountBatch(c *natsrouter.Context, req model.BadgeCountBatchRequest) (*model.BadgeCountBatchResponse, error) {
	if len(req.Accounts) == 0 || req.RoomID == "" {
		return nil, errcode.BadRequest("roomId and accounts are required")
	}
	resp := &model.BadgeCountBatchResponse{Counts: make(map[string]int, len(req.Accounts))}
	hits := s.badge.BumpBatch(c, req.Accounts, req.RoomID)
	misses := make([]string, 0, len(req.Accounts))
	for _, account := range req.Accounts {
		// A hit is already in memory from BumpBatch — serve it whatever happens to
		// the recompute path below, including a budget spent part-way through.
		if n, ok := hits[account]; ok {
			resp.Counts[account] = n
			continue
		}
		misses = append(misses, account)
	}

	// Each miss costs a rooms aggregate plus that account's own cross-site RPCs.
	// Run them concurrently — serialized, a batch's latency is the SUM of every
	// miss against one shared handler deadline. The bound matters as much as the
	// concurrency: each seed holds a Mongo connection and opens its own per-site
	// fan-out, so the two bounds multiply.
	// counts[i]/answered[i] are each written by exactly one goroutine, so the
	// merge below needs no lock. WaitGroup, not errgroup: one account's failure
	// must not cancel its siblings.
	counts := make([]int, len(misses))
	answered := make([]bool, len(misses))
	var wg sync.WaitGroup
	sem := make(chan struct{}, s.badgeSeedFanout())
	for i, account := range misses {
		// Once the shared request budget is spent, stop STARTING recomputes: run
		// against a dead context they would only hit the heavy aggregate, fail at
		// connection checkout with a misleading pool error, and degrade to absence
		// anyway. Seeds already in flight are left to finish, and how far along
		// they are decides the outcome: one past the aggregate still answers (a
		// dead context only marks its cross-site half degraded), while one still
		// inside the aggregate fails on it and degrades to absence — logging the
		// same misleading pool error this guard avoids for the unstarted.
		if c.Err() != nil {
			break
		}
		wg.Add(1)
		// Acquire before spawning so the live goroutine COUNT, not just the
		// concurrency, stays within the bound (as fanOutChunks does).
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			// Re-check after parking on the semaphore: the budget may have been
			// spent while this account waited its turn.
			if c.Err() != nil {
				return
			}
			ids, degraded, err := s.unreadRooms(c, account)
			if err != nil {
				slog.WarnContext(c, "badge seed degraded", "account", account, "room_id", req.RoomID, "request_id", natsutil.RequestIDFromContext(c), "error", err)
				return
			}
			// A partial result must not be cached (it would stamp the freshness
			// marker); answer from it directly instead.
			if degraded {
				counts[i], answered[i] = cappedUnion(ids, req.RoomID, s.badgeCap), true
				return
			}
			if n, ok := s.badge.Seed(c, account, ids, req.RoomID); ok {
				counts[i], answered[i] = n, true
				return
			}
			// Cache down entirely: compute without it (ids ∪ trigger, capped).
			counts[i], answered[i] = cappedUnion(ids, req.RoomID, s.badgeCap), true
		}()
	}
	wg.Wait()
	for i, account := range misses {
		if answered[i] {
			resp.Counts[account] = counts[i]
		}
	}
	return resp, nil
}

// cappedUnion returns the size of ids ∪ {trigger} (deduplicated — trigger is
// skipped if blank or already a member of ids), capped at cap (BADGE_COUNT_CAP,
// mirrored into pkg/badgecache so the cache-backed paths agree). This is the
// cache-down fallback BadgeCountBatch uses when neither BumpBatch nor Seed
// could reach Valkey.
func cappedUnion(ids []string, trigger string, cap int) int {
	seen := make(map[string]struct{}, len(ids)+1)
	for _, id := range ids {
		seen[id] = struct{}{}
	}
	if trigger != "" {
		seen[trigger] = struct{}{}
	}
	if len(seen) > cap {
		return cap
	}
	return len(seen)
}
