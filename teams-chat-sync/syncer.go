package main

import (
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// cachedUser is the in-memory projection of one teams_user, used for member
// account resolution and the siteID vote.
type cachedUser struct {
	siteID  string
	account string
}

// syncConfig carries the orchestration knobs. Now is injectable for tests.
type syncConfig struct {
	MaxWorkers  int
	DefaultFrom time.Time
	Now         func() time.Time
}

// syncer runs one full teams-chat sync. processed is the cross-worker claimed
// set: the first worker to claim a chat id processes it, later workers skip —
// which doubles as the write-reduction cache since many users share chats.
type syncer struct {
	users TeamsUserStore
	chats TeamsChatStore
	graph chatsFetcher
	cfg   syncConfig

	mu        sync.Mutex
	processed map[string]struct{}
}

func newSyncer(users TeamsUserStore, chats TeamsChatStore, graph chatsFetcher, cfg syncConfig) *syncer {
	return &syncer{users: users, chats: chats, graph: graph, cfg: cfg, processed: make(map[string]struct{})}
}

// startOfDayUTC truncates t to 00:00:00 UTC of the same UTC day.
func startOfDayUTC(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// claim atomically claims a chat id, returning true only for the first caller.
func (s *syncer) claim(chatID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, done := s.processed[chatID]; done {
		return false
	}
	s.processed[chatID] = struct{}{}
	return true
}

// voteSiteID returns the majority siteID among the members present in the
// cache; ties break to the lexicographically smallest siteID so the result is
// deterministic across runs and map iteration orders. Returns "" when no
// member is known (not expected in practice: the fetching user is always a
// member and always cached).
func voteSiteID(members []msgraph.ChatMember, cache map[string]cachedUser) string {
	counts := make(map[string]int)
	for _, m := range members {
		if cu, ok := cache[m.UserID]; ok {
			counts[cu.siteID]++
		}
	}
	best, bestN := "", 0
	for site, n := range counts {
		if n > bestN || (n == bestN && n > 0 && site < best) {
			best, bestN = site, n
		}
	}
	return best
}

// buildChat maps a Graph chat to the teams_chat model, resolving member
// accounts and the owning site from the user cache. Unknown members are kept
// with an empty account. UpdatedAt is intentionally left zero — the store
// stamps it at write time.
func buildChat(gc msgraph.Chat, cache map[string]cachedUser) model.TeamsChat {
	members := make([]model.TeamsChatMember, 0, len(gc.Members))
	for _, m := range gc.Members {
		members = append(members, model.TeamsChatMember{
			ID:                          m.UserID,
			Account:                     cache[m.UserID].account,
			VisibleHistoryStartDateTime: m.VisibleHistoryStartDateTime,
		})
	}
	return model.TeamsChat{
		ID:                  gc.ID,
		Name:                gc.Topic,
		ChatType:            gc.ChatType,
		CreatedDateTime:     gc.CreatedDateTime,
		LastUpdatedDateTime: gc.LastUpdatedDateTime,
		Members:             members,
		SiteID:              voteSiteID(gc.Members, cache),
		NeedUserSync:        gc.ChatType != model.TeamsChatTypeOneOnOne,
	}
}
