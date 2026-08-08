package service

import (
	"log/slog"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// badgeCountCap mirrors pkg/badgecache's own 10-cap (10 renders as "9+" in the
// UI) so the cache-down fallback path (cappedUnion) agrees with the
// cache-backed paths on what the badge count tops out at.
const badgeCountCap = 10

// BadgeCountBatch returns each account's badge unread-room count (capped at
// 10) for a notification in req.RoomID. Cache hit: one pipelined SADD+SCARD
// batch for all accounts. Miss: seed from unreadRooms (Mongo + cross-site
// room RPCs) ∪ the trigger room. Per-account failures degrade to absence —
// the push must never block.
// NATS: chat.server.request.user.{siteID}.badge.count.batch
func (s *UserService) BadgeCountBatch(c *natsrouter.Context, req model.BadgeCountBatchRequest) (*model.BadgeCountBatchResponse, error) {
	if len(req.Accounts) == 0 || req.RoomID == "" {
		return nil, errcode.BadRequest("roomId and accounts are required")
	}
	resp := &model.BadgeCountBatchResponse{Counts: make(map[string]int, len(req.Accounts))}
	hits := s.badge.BumpBatch(c, req.Accounts, req.RoomID)
	for _, account := range req.Accounts {
		if n, ok := hits[account]; ok {
			resp.Counts[account] = n
			continue
		}
		ids, err := s.unreadRooms(c, account)
		if err != nil {
			slog.WarnContext(c, "badge seed degraded", "account", account, "room_id", req.RoomID, "request_id", natsutil.RequestIDFromContext(c), "error", err)
			continue
		}
		if n, ok := s.badge.Seed(c, account, ids, req.RoomID); ok {
			resp.Counts[account] = n
			continue
		}
		// Cache down entirely: compute without it (ids ∪ trigger, capped).
		resp.Counts[account] = cappedUnion(ids, req.RoomID)
	}
	return resp, nil
}

// cappedUnion returns the size of ids ∪ {trigger} (deduplicated — trigger is
// skipped if blank or already a member of ids), capped at badgeCountCap. This
// is the cache-down fallback BadgeCountBatch uses when neither Bump nor Seed
// could reach Valkey.
func cappedUnion(ids []string, trigger string) int {
	seen := make(map[string]struct{}, len(ids)+1)
	for _, id := range ids {
		seen[id] = struct{}{}
	}
	if trigger != "" {
		seen[trigger] = struct{}{}
	}
	if len(seen) > badgeCountCap {
		return badgeCountCap
	}
	return len(seen)
}
