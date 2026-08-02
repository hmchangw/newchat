package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/hmchangw/chat/pkg/model"
)

// runConfig holds the runner's pure knobs, so the pass is testable without any
// real dependency.
type runConfig struct {
	BatchSize  int
	MaxWorkers int
	// SiteURLs maps siteId to that site's inspector base URL.
	SiteURLs map[string]string
}

// runner performs one verification pass: list flagged chats, group by site,
// chunk into batches, ask each site's inspector what it holds, and clear the
// flag for the chats that converged.
type runner struct {
	store  TeamsChatStore
	verify verifyFunc
	cfg    runConfig

	mu    sync.Mutex
	stats map[string]*siteStats
}

// siteStats accumulates one site's outcome across its batches, so the summary is
// per site rather than per batch. The three outcome counters partition the
// answered chats, so the checked total is their sum rather than a fourth counter
// that could drift out of step with them.
type siteStats struct {
	roomsMissing int
	subsMismatch int
	ok           int
	unanswered   int
}

// checked is the number of chats the inspector answered about.
func (s *siteStats) checked() int { return s.roomsMissing + s.subsMismatch + s.ok }

func newRunner(store TeamsChatStore, verify verifyFunc, cfg runConfig) *runner {
	return &runner{store: store, verify: verify, cfg: cfg, stats: make(map[string]*siteStats)}
}

// batch is one site's worth of up to BatchSize chats.
type batch struct {
	siteID string
	chats  []model.TeamsChat
}

// run executes one pass. It returns an error only when the initial list fails;
// per-site and per-batch failures are logged and leave those chats flagged for
// the next CronJob run.
func (r *runner) run(ctx context.Context) error {
	chats, err := r.store.ListChatsNeedingVerify(ctx)
	if err != nil {
		return fmt.Errorf("list chats needing verify: %w", err)
	}
	if len(chats) == 0 {
		slog.InfoContext(ctx, "no chats need verification")
		return nil
	}
	batches := planBatches(chats, r.cfg.BatchSize)
	slog.InfoContext(ctx, "verifying room creation", "chats", len(chats), "batches", len(batches))

	sem := make(chan struct{}, r.cfg.MaxWorkers)
	var wg sync.WaitGroup
	for _, b := range batches {
		wg.Add(1)
		sem <- struct{}{}
		go func(b batch) {
			defer wg.Done()
			defer func() { <-sem }()
			r.verifyBatch(ctx, b)
		}(b)
	}
	wg.Wait()
	r.logSummary(ctx)
	return nil
}

// verifyBatch asks one site about one batch and clears the flag for the chats
// that converged. Every failure path leaves the batch's chats flagged.
func (r *runner) verifyBatch(ctx context.Context, b batch) {
	baseURL, ok := r.cfg.SiteURLs[b.siteID]
	if !ok {
		slog.WarnContext(ctx, "no inspector URL for site; skipping batch",
			"site_id", b.siteID, "chats", len(b.chats))
		return
	}

	ids := make([]string, 0, len(b.chats))
	for i := range b.chats {
		ids = append(ids, b.chats[i].ID)
	}
	resp, err := r.verify(ctx, baseURL, ids)
	if err != nil {
		slog.WarnContext(ctx, "inspector call failed; chats stay flagged for the next run",
			"site_id", b.siteID, "chats", len(ids), "error", err)
		return
	}

	byChat := make(map[string]model.TeamsRoomVerifyResult, len(resp.Chats))
	for _, res := range resp.Chats {
		byChat[res.ChatID] = res
	}

	st := &siteStats{}
	refs := make([]VerifiedRef, 0, len(b.chats))
	for i := range b.chats {
		c := &b.chats[i]
		res, answered := byChat[c.ID]
		if !answered {
			st.unanswered++
			slog.WarnContext(ctx, "inspector omitted a requested chat; leaving it flagged",
				"chat_id", c.ID, "site_id", b.siteID)
			continue
		}
		expected := len(c.Members)
		switch {
		case !res.RoomExists:
			st.roomsMissing++
			r.logMismatch(ctx, c, &res, b.siteID, expected, "missing_room")
		case res.SubscriptionCount != expected:
			st.subsMismatch++
			r.logMismatch(ctx, c, &res, b.siteID, expected, "subscription_mismatch")
		default:
			st.ok++
			refs = append(refs, VerifiedRef{ID: c.ID, UpdatedAt: c.UpdatedAt})
		}
	}
	r.addStats(b.siteID, st)

	if err := r.store.MarkVerified(ctx, refs); err != nil {
		slog.WarnContext(ctx, "mark verified failed; chats re-verify next run",
			"site_id", b.siteID, "chats", len(refs), "error", err)
	}
}

// logMismatch reports one chat that did not converge. accounts_present is the
// diagnostic that separates a genuine gap from a member room-worker legitimately
// skipped (a guest with no account).
func (r *runner) logMismatch(ctx context.Context, c *model.TeamsChat, res *model.TeamsRoomVerifyResult, siteID string, expected int, reason string) {
	slog.WarnContext(ctx, "teams room verification mismatch",
		"chat_id", c.ID,
		"site_id", siteID,
		"room_id", res.RoomID,
		"expected_members", expected,
		"accounts_present", accountsPresent(c.Members),
		"actual_subscriptions", res.SubscriptionCount,
		"room_user_count", res.RoomUserCount,
		"reason", reason)
}

// addStats folds one batch's counters into its site's totals.
func (r *runner) addStats(siteID string, st *siteStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	agg, ok := r.stats[siteID]
	if !ok {
		agg = &siteStats{}
		r.stats[siteID] = agg
	}
	agg.roomsMissing += st.roomsMissing
	agg.subsMismatch += st.subsMismatch
	agg.ok += st.ok
	agg.unanswered += st.unanswered
}

// logSummary emits one line per site that answered.
func (r *runner) logSummary(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for siteID, st := range r.stats {
		slog.InfoContext(ctx, "teams room verification summary",
			"site_id", siteID,
			"chats_checked", st.checked(),
			"rooms_missing", st.roomsMissing,
			"subs_mismatched", st.subsMismatch,
			"chats_ok", st.ok,
			"chats_unanswered", st.unanswered)
	}
}

// accountsPresent counts members room-worker would actually subscribe: distinct
// non-empty accounts.
func accountsPresent(members []model.TeamsChatMember) int {
	seen := make(map[string]struct{}, len(members))
	for _, m := range members {
		if m.Account == "" {
			continue
		}
		seen[m.Account] = struct{}{}
	}
	return len(seen)
}

// planBatches groups chats by siteID (deterministic: sites and chats keep input
// order) and chunks each group into batches of at most size.
func planBatches(chats []model.TeamsChat, size int) []batch {
	order := make([]string, 0)
	bySite := make(map[string][]model.TeamsChat)
	//nolint:gocritic // rangeValCopy: c is heavy but using index-range would be less idiomatic
	for _, c := range chats {
		if _, ok := bySite[c.SiteID]; !ok {
			order = append(order, c.SiteID)
		}
		bySite[c.SiteID] = append(bySite[c.SiteID], c)
	}
	var out []batch
	for _, site := range order {
		cs := bySite[site]
		for i := 0; i < len(cs); i += size {
			end := i + size
			if end > len(cs) {
				end = len(cs)
			}
			out = append(out, batch{siteID: site, chats: cs[i:end]})
		}
	}
	return out
}
