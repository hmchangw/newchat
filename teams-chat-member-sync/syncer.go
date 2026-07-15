package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

// syncConfig carries the orchestration knobs. Now is injectable for tests.
type syncConfig struct {
	MaxWorkers int
	Now        func() time.Time
}

// syncer runs one full member sync.
type syncer struct {
	chats TeamsChatStore
	users TeamsUserStore
	graph membersFetcher
	cfg   syncConfig
	cache *accountCache
}

func newSyncer(chats TeamsChatStore, users TeamsUserStore, graph membersFetcher, cfg syncConfig) *syncer {
	return &syncer{chats: chats, users: users, graph: graph, cfg: cfg, cache: newAccountCache(users)}
}

// accountFromUPN returns the lowercased local part of a UPN (text before '@'),
// or "" when the UPN is empty or has no local part. Matches teams-user-sync's
// account derivation.
func accountFromUPN(upn string) string {
	at := strings.Index(upn, "@")
	if at <= 0 {
		return ""
	}
	return strings.ToLower(upn[:at])
}

// accountCache is a process-wide userId->account cache shared by all workers.
// It batches uncached ids into a single AccountsByIDs query and caches misses (as "").
// Under concurrency, two goroutines racing on the same uncached id may each issue
// a lookup — harmless and self-healing (the map write is mutex-guarded, resolved value is identical).
type accountCache struct {
	users TeamsUserStore
	mu    sync.Mutex
	cache map[string]string
}

func newAccountCache(users TeamsUserStore) *accountCache {
	return &accountCache{users: users, cache: make(map[string]string)}
}

// resolve returns account for each requested id, querying teams_user only for
// ids not already cached and caching every result including misses.
func (c *accountCache) resolve(ctx context.Context, ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	var missing []string
	seen := make(map[string]struct{}) // dedup within this call

	c.mu.Lock()
	for _, id := range ids {
		if acct, ok := c.cache[id]; ok {
			out[id] = acct
		} else if _, alreadySeen := seen[id]; !alreadySeen {
			missing = append(missing, id)
			seen[id] = struct{}{}
		}
	}
	c.mu.Unlock()

	if len(missing) == 0 {
		return out, nil
	}
	found, err := c.users.AccountsByIDs(ctx, missing)
	if err != nil {
		return nil, fmt.Errorf("resolve accounts: %w", err)
	}

	c.mu.Lock()
	for _, id := range missing {
		acct := found[id] // "" when absent — a cached miss
		c.cache[id] = acct
		out[id] = acct
	}
	c.mu.Unlock()
	return out, nil
}

// buildMembers maps Graph members to stored members. UPN-carrying members
// resolve locally; the rest are resolved in one batched, cached teams_user
// lookup. Unknown members keep account "".
func (s *syncer) buildMembers(ctx context.Context, raw []msgraph.ChatMemberDetail) ([]model.TeamsChatMember, error) {
	accounts := make(map[string]string, len(raw)) // userID -> account, for UPN-less members
	var needLookup []string
	for _, m := range raw {
		if acct := accountFromUPN(m.UserPrincipalName); acct != "" {
			accounts[m.UserID] = acct
			continue
		}
		needLookup = append(needLookup, m.UserID)
	}
	if len(needLookup) > 0 {
		resolved, err := s.cache.resolve(ctx, needLookup)
		if err != nil {
			return nil, err
		}
		for id, acct := range resolved {
			accounts[id] = acct
		}
	}

	members := make([]model.TeamsChatMember, 0, len(raw))
	for _, m := range raw {
		members = append(members, model.TeamsChatMember{
			ID:                          m.UserID,
			Account:                     accounts[m.UserID],
			VisibleHistoryStartDateTime: m.VisibleHistoryStartDateTime,
		})
	}
	return members, nil
}

// summary is the per-run outcome. Total is written only by the dispatching
// goroutine; the atomics are updated by workers.
type summary struct {
	Total             int
	Succeeded, Failed atomic.Int64
	MembersWritten    atomic.Int64
}

// run executes one full member sync: load the flagged chats, fan them out to
// MaxWorkers workers, wait, and report. It returns an error when any chat
// failed so main exits non-zero and the CronJob records the failure.
func (s *syncer) run(ctx context.Context) error {
	ids, err := s.chats.ListChatsToSync(ctx)
	if err != nil {
		return fmt.Errorf("load chats needing member sync: %w", err)
	}

	var sum summary
	sum.Total = len(ids)

	jobs := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chatID := range jobs {
				if err := s.syncChat(ctx, chatID, &sum); err != nil {
					sum.Failed.Add(1)
					slog.Error("teams chat member sync: chat failed", "chatID", chatID, "error", err)
					continue
				}
				sum.Succeeded.Add(1)
			}
		}()
	}
	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	wg.Wait()

	slog.Info("teams chat member sync: run complete",
		"chatsTotal", sum.Total, "chatsSucceeded", sum.Succeeded.Load(),
		"chatsFailed", sum.Failed.Load(), "membersWritten", sum.MembersWritten.Load())

	if failed := sum.Failed.Load(); failed > 0 {
		return fmt.Errorf("%d of %d chats failed", failed, sum.Total)
	}
	return nil
}

// syncChat fetches one chat's members, resolves accounts, and writes the list
// back. On any error the chat's needMemberSync is left true (no SetMembersSynced)
// so it is retried next run.
func (s *syncer) syncChat(ctx context.Context, chatID string, sum *summary) error {
	raw, err := s.graph.ListChatMembers(ctx, chatID)
	if err != nil {
		return fmt.Errorf("list chat members: %w", err)
	}
	members, err := s.buildMembers(ctx, raw)
	if err != nil {
		return fmt.Errorf("build members: %w", err)
	}
	if err := s.chats.SetMembersSynced(ctx, chatID, members, s.cfg.Now()); err != nil {
		return fmt.Errorf("set members synced: %w", err)
	}
	sum.MembersWritten.Add(int64(len(members)))
	return nil
}
