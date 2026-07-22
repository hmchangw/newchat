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

// syncConfig carries the orchestration knobs. Now is injectable for tests.
type syncConfig struct {
	MaxWorkers int
	BatchSize  int
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

// buildMembers maps Graph members to stored members, resolving every member's
// account from teams_user by userId through the batched, cached lookup.
// Members absent from teams_user keep account "".
func (s *syncer) buildMembers(ctx context.Context, raw []msgraph.ChatMemberDetail) ([]model.TeamsChatMember, error) {
	ids := make([]string, 0, len(raw))
	for _, m := range raw {
		ids = append(ids, m.UserID)
	}
	accounts, err := s.cache.resolve(ctx, ids)
	if err != nil {
		return nil, err
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
// goroutine; the atomics are updated by workers (Failed) and the batch
// collector (all counters). MembersWritten counts the members submitted in
// successful batch writes; a bulk write cannot attribute per-chat outcomes,
// so members of superseded chats within a successful batch are included.
type summary struct {
	Total                         int
	Succeeded, Failed, Superseded atomic.Int64
	MembersWritten                atomic.Int64
}

// run executes one full member sync: load the flagged chats, fan them out to
// MaxWorkers workers that resolve each chat's members, collect the resolved
// lists, and write them back in bulk batches of BatchSize. It returns an
// error when any chat failed so main exits non-zero and the CronJob records
// the failure. Superseded chats (concurrent teams-chat-sync rewrite) are
// benign — they keep needMemberSync=true and retry next run — and do not
// fail the run.
func (s *syncer) run(ctx context.Context) error {
	chats, err := s.chats.ListChatsToSync(ctx)
	if err != nil {
		return fmt.Errorf("load chats needing member sync: %w", err)
	}

	var sum summary
	sum.Total = len(chats)

	jobs := make(chan ChatToSync)
	results := make(chan ChatMembersUpdate)
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chat := range jobs {
				update, err := s.resolveChat(ctx, chat)
				if err != nil {
					sum.Failed.Add(1)
					slog.Error("teams chat member sync: chat failed", "chatID", chat.ID, "error", err)
					continue
				}
				results <- update
			}
		}()
	}

	// A single collector serializes the bulk writes: it buffers resolved chats
	// and flushes each full batch while workers keep resolving the next ones.
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		batch := make([]ChatMembersUpdate, 0, s.cfg.BatchSize)
		for update := range results {
			batch = append(batch, update)
			if len(batch) == s.cfg.BatchSize {
				s.flush(ctx, batch, &sum)
				batch = batch[:0]
			}
		}
		s.flush(ctx, batch, &sum)
	}()

	for _, chat := range chats {
		jobs <- chat
	}
	close(jobs)
	wg.Wait()
	close(results)
	<-collectorDone

	slog.Info("teams chat member sync: run complete",
		"chatsTotal", sum.Total, "chatsSucceeded", sum.Succeeded.Load(),
		"chatsFailed", sum.Failed.Load(), "chatsSuperseded", sum.Superseded.Load(),
		"membersWritten", sum.MembersWritten.Load())

	if failed := sum.Failed.Load(); failed > 0 {
		return fmt.Errorf("%d of %d chats failed", failed, sum.Total)
	}
	return nil
}

// resolveChat fetches one chat's members and resolves their accounts. On any
// error the chat never reaches a batch, so its needMemberSync is left true
// and it is retried next run.
func (s *syncer) resolveChat(ctx context.Context, chat ChatToSync) (ChatMembersUpdate, error) {
	raw, err := s.graph.ListChatMembers(ctx, chat.ID)
	if err != nil {
		return ChatMembersUpdate{}, fmt.Errorf("list chat members: %w", err)
	}
	members, err := s.buildMembers(ctx, raw)
	if err != nil {
		return ChatMembersUpdate{}, fmt.Errorf("build members: %w", err)
	}
	return ChatMembersUpdate{ChatID: chat.ID, SeenUpdatedAt: chat.UpdatedAt, Members: members}, nil
}

// flush writes one batch back in a single bulk write and folds the outcome
// into sum. Unmatched chats were superseded by a concurrent rewrite — benign,
// they keep needMemberSync=true for retry. On a (possibly partial) bulk-write
// error the matched chats still count as succeeded and only the remainder as
// failed; failed chats likewise keep needMemberSync=true.
func (s *syncer) flush(ctx context.Context, batch []ChatMembersUpdate, sum *summary) {
	if len(batch) == 0 {
		return
	}
	var members int64
	for i := range batch {
		members += int64(len(batch[i].Members))
	}
	matched, err := s.chats.SetMembersSyncedBatch(ctx, batch, s.cfg.Now())
	sum.Succeeded.Add(matched)
	if err != nil {
		sum.Failed.Add(int64(len(batch)) - matched)
		slog.Error("teams chat member sync: batch write failed", "chats", len(batch), "chatsMatched", matched, "error", err)
		return
	}
	superseded := int64(len(batch)) - matched
	sum.Superseded.Add(superseded)
	sum.MembersWritten.Add(members)
	if superseded > 0 {
		slog.Warn("teams chat member sync: batch had chats superseded by concurrent updates, will retry next run", "chatsSuperseded", superseded)
	}
	slog.Info("teams chat member sync: batch written",
		"chats", len(batch), "chatsMatched", matched, "chatsSuperseded", superseded, "members", members)
}
