package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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
	// DefaultSiteID is the fallback siteID for a chat whose vote is empty.
	// Required,notEmpty in production, so syncUser's empty-vote skip is only
	// reachable via direct construction with an empty default.
	DefaultSiteID string
}

// syncer runs one full teams-chat sync. Each worker persists every chat it
// lists, including chats shared with other users: a shared chat is upserted
// once per member rather than claimed by a single worker. Redundant writes are
// safe because the immutable fields (siteId, createdDateTime) are set once at
// the DB layer via $setOnInsert and never overwritten — a repeated upsert only
// refreshes mutable metadata (for a complete-roster chat that includes the
// members and an idempotent needCreateRoom=true). This keeps each user's durability
// self-contained — a user advances its watermark only after its own chats
// persist — so one member's failure can never strand a chat that a surviving
// member would otherwise have written.
type syncer struct {
	users TeamsUserStore
	chats TeamsChatStore
	graph chatsFetcher
	cfg   syncConfig
}

func newSyncer(users TeamsUserStore, chats TeamsChatStore, graph chatsFetcher, cfg syncConfig) *syncer {
	return &syncer{users: users, chats: chats, graph: graph, cfg: cfg}
}

// startOfDayUTC truncates t to 00:00:00 UTC of the same UTC day.
func startOfDayUTC(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// voteSiteID returns the plurality siteID among the members present in the
// cache with a non-empty siteID; ties break to the lexicographically smallest
// siteID so the result is deterministic across runs and map iteration orders.
// Members with an empty siteID (no HR assignment) do not vote, so the empty
// string never wins. When no member casts a real vote it falls back to
// defaultSiteID, which is required,notEmpty in production — so the returned
// siteID is always non-empty (an empty result is only possible under direct
// construction with an empty default).
func voteSiteID(members []msgraph.ChatMember, cache map[string]cachedUser, defaultSiteID string) string {
	counts := make(map[string]int)
	for _, m := range members {
		// Skip members with an empty siteID (no HR assignment): not counting them
		// is what keeps an empty siteID out of counts, so it can never win.
		if cu, ok := cache[m.UserID]; ok && cu.siteID != "" {
			counts[cu.siteID]++
		}
	}
	best, bestN := "", 0
	for site, n := range counts {
		if n > bestN || (n == bestN && site < best) {
			best, bestN = site, n
		}
	}
	if best == "" {
		return defaultSiteID
	}
	return best
}

// expandedMembersTrustLimit is the roster-completeness threshold for the
// $expand=members payload on Graph's list-chats endpoint. Graph truncates the
// expanded members of a large chat, so an expanded roster of this size or
// larger may be partial and the chat is routed through teams-chat-member-sync
// for an authoritative member list. Below the limit (and non-empty) the
// expanded roster IS the full membership and the member-sync stage is skipped.
const expandedMembersTrustLimit = 25

// buildChat maps a Graph chat to the teams_chat model, resolving member
// accounts and the owning site from the user cache. Unknown members are kept
// with an empty account. UpdatedAt is stamped with now; the store writes it
// verbatim on every upsert. NeedMemberSync is set only for non-oneOnOne chats
// whose expanded roster may be truncated (empty or at/over
// expandedMembersTrustLimit); a complete roster skips member-sync entirely.
//
//nolint:gocritic // hugeParam: gc is consumed once per chat on a batch path; passing by value keeps the mapper pure.
func buildChat(gc msgraph.Chat, cache map[string]cachedUser, now time.Time, defaultSiteID string) model.TeamsChat {
	members := make([]model.TeamsChatMember, 0, len(gc.Members))
	for _, m := range gc.Members {
		members = append(members, model.TeamsChatMember{
			ID:                          m.UserID,
			Account:                     cache[m.UserID].account,
			VisibleHistoryStartDateTime: m.VisibleHistoryStartDateTime,
		})
	}
	rosterComplete := len(gc.Members) > 0 && len(gc.Members) < expandedMembersTrustLimit
	return model.TeamsChat{
		ID:                  gc.ID,
		Name:                gc.Topic,
		ChatType:            gc.ChatType,
		CreatedDateTime:     gc.CreatedDateTime,
		LastUpdatedDateTime: gc.LastUpdatedDateTime,
		Members:             members,
		SiteID:              voteSiteID(gc.Members, cache, defaultSiteID),
		UpdatedAt:           now,
		NeedMemberSync:      gc.ChatType != model.TeamsChatTypeOneOnOne && !rosterComplete,
	}
}

// summary is the per-run outcome reported in the final log line. Total and
// Skipped are written only by the dispatching goroutine; the atomics are
// updated by workers.
type summary struct {
	Total, Skipped    int
	Succeeded, Failed atomic.Int64
	Upserted          atomic.Int64
}

// run executes one full sync: load the user cache, fan eligible users out to
// MaxWorkers workers, wait, and report. It returns an error when any user
// failed so main exits non-zero and the CronJob records the failure.
func (s *syncer) run(ctx context.Context) error {
	users, err := s.users.ListUsers(ctx)
	if err != nil {
		return fmt.Errorf("load teams users: %w", err)
	}
	cache := make(map[string]cachedUser, len(users))
	for _, u := range users {
		cache[u.ID] = cachedUser{siteID: u.SiteID, account: u.Account}
	}

	to := startOfDayUTC(s.cfg.Now())
	var sum summary
	sum.Total = len(users)

	jobs := make(chan model.TeamsUser)
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range jobs {
				if err := s.syncUser(ctx, u, to, cache, &sum); err != nil {
					sum.Failed.Add(1)
					slog.Error("teams chat sync: user failed", "userID", u.ID, "error", err)
					continue
				}
				sum.Succeeded.Add(1)
			}
		}()
	}
	for _, u := range users {
		if !s.effectiveFrom(u).Before(to) {
			sum.Skipped++
			continue
		}
		jobs <- u
	}
	close(jobs)
	wg.Wait()

	slog.Info("teams chat sync: run complete",
		"usersTotal", sum.Total, "usersSucceeded", sum.Succeeded.Load(),
		"usersFailed", sum.Failed.Load(), "usersSkipped", sum.Skipped,
		"chatsUpserted", sum.Upserted.Load())

	if failed := sum.Failed.Load(); failed > 0 {
		return fmt.Errorf("%d of %d users failed", failed, sum.Total)
	}
	return nil
}

// effectiveFrom is the user's watermark, falling back to the configured
// default for users that have never synced.
func (s *syncer) effectiveFrom(u model.TeamsUser) time.Time {
	if u.From != nil {
		return *u.From
	}
	return s.cfg.DefaultFrom
}

// syncUser fetches one user's chat window, upserts every chat it lists, and
// advances the user's watermark only after everything succeeded — a failed user
// keeps its old watermark and is retried next run.
func (s *syncer) syncUser(ctx context.Context, u model.TeamsUser, to time.Time, cache map[string]cachedUser, sum *summary) error {
	graphChats, err := s.graph.ListUserChats(ctx, u.ID, s.effectiveFrom(u), to)
	if err != nil {
		return fmt.Errorf("list user chats: %w", err)
	}
	batch := make([]model.TeamsChat, 0, len(graphChats))
	now := s.cfg.Now()
	for _, gc := range graphChats {
		built := buildChat(gc, cache, now, s.cfg.DefaultSiteID)
		// Defensive: DefaultSiteID is required,notEmpty in production, so this
		// only fires if the syncer is built with an empty default (tests).
		if built.SiteID == "" {
			slog.Warn("teams chat sync: siteID vote empty, skipping chat", "chatID", gc.ID, "userID", u.ID)
			continue
		}
		batch = append(batch, built)
	}
	if len(batch) > 0 {
		if err := s.chats.UpsertChats(ctx, batch); err != nil {
			return fmt.Errorf("upsert chats: %w", err)
		}
		sum.Upserted.Add(int64(len(batch)))
	}
	if err := s.users.SetFrom(ctx, u.ID, to); err != nil {
		return fmt.Errorf("advance watermark: %w", err)
	}
	return nil
}
