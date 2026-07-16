package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// runConfig holds the runner's pure knobs. Now is injected so tests control the
// event Timestamp.
type runConfig struct {
	BatchSize  int
	MaxWorkers int
	Now        func() time.Time
}

// runner performs one room-creation pass: list flagged chats, group by site,
// chunk into batches, publish each batch, and clear the flag for batches that
// were acknowledged.
type runner struct {
	store   TeamsChatStore
	publish publishFunc
	cfg     runConfig
}

func newRunner(store TeamsChatStore, publish publishFunc, cfg runConfig) *runner {
	return &runner{store: store, publish: publish, cfg: cfg}
}

// batch is one site's worth of up to BatchSize chats.
type batch struct {
	siteID string
	chats  []model.TeamsChat
}

// run executes one pass. It returns an error only when the initial list fails;
// per-batch publish failures are logged and leave those chats flagged for the
// next CronJob run.
func (r *runner) run(ctx context.Context) error {
	chats, err := r.store.ListChatsNeedingRoom(ctx)
	if err != nil {
		return fmt.Errorf("list chats needing room: %w", err)
	}
	if len(chats) == 0 {
		slog.InfoContext(ctx, "no chats need room creation")
		return nil
	}
	batches := planBatches(chats, r.cfg.BatchSize)
	slog.InfoContext(ctx, "publishing room-creation batches",
		"chats", len(chats), "batches", len(batches))

	sem := make(chan struct{}, r.cfg.MaxWorkers)
	var wg sync.WaitGroup
	for _, b := range batches {
		wg.Add(1)
		sem <- struct{}{}
		go func(b batch) {
			defer wg.Done()
			defer func() { <-sem }()
			r.publishBatch(ctx, b)
		}(b)
	}
	wg.Wait()
	return nil
}

// publishBatch marshals and publishes one batch, then clears the flag for its
// chats iff the publish was acknowledged.
func (r *runner) publishBatch(ctx context.Context, b batch) {
	evt := buildEvent(b.chats, r.cfg.Now())
	data, err := json.Marshal(evt)
	if err != nil {
		slog.ErrorContext(ctx, "marshal room-creation event", "site_id", b.siteID, "error", err)
		return
	}
	ids := chatIDs(b.chats)
	subj := subject.RoomCanonicalTeamsCreate(b.siteID)
	if err := r.publish(ctx, subj, data, dedupID(b.siteID, ids)); err != nil {
		slog.WarnContext(ctx, "publish room-creation batch failed; will retry next run",
			"site_id", b.siteID, "chats", len(ids), "error", err)
		return
	}
	if err := r.store.MarkRoomsCreated(ctx, roomCreatedRefs(b.chats)); err != nil {
		slog.WarnContext(ctx, "mark rooms created failed; batch republishes next run (dedup absorbs it)",
			"site_id", b.siteID, "chats", len(ids), "error", err)
	}
}

// planBatches groups chats by siteID (deterministic: sites and chats keep
// input order) and chunks each group into batches of at most size.
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

// buildEvent maps a batch of teams_chat docs into the wire event, dropping each
// member's Graph id and stamping the publish timestamp. The site is not carried
// in the payload — it is on the publish subject.
func buildEvent(chats []model.TeamsChat, now time.Time) model.TeamsRoomCreateEvent {
	out := make([]model.TeamsRoomCreateChat, 0, len(chats))
	//nolint:gocritic // rangeValCopy: c is heavy but using index-range would be less idiomatic
	for _, c := range chats {
		members := make([]model.TeamsRoomCreateMember, 0, len(c.Members))
		for _, m := range c.Members {
			members = append(members, model.TeamsRoomCreateMember{
				Account:                     m.Account,
				VisibleHistoryStartDateTime: m.VisibleHistoryStartDateTime,
			})
		}
		out = append(out, model.TeamsRoomCreateChat{
			ID:              c.ID,
			Name:            c.Name,
			Members:         members,
			CreatedDateTime: c.CreatedDateTime,
		})
	}
	return model.TeamsRoomCreateEvent{
		Chats:     out,
		Timestamp: now.UTC().UnixMilli(),
	}
}

// chatIDs extracts the chat ids of a batch, preserving order.
func chatIDs(chats []model.TeamsChat) []string {
	ids := make([]string, 0, len(chats))
	//nolint:gocritic // rangeValCopy: c is heavy but using index-range would be less idiomatic
	for _, c := range chats {
		ids = append(ids, c.ID)
	}
	return ids
}

// roomCreatedRefs pairs each chat id with the updatedAt it was read at, for the
// compare-and-set clear in MarkRoomsCreated.
func roomCreatedRefs(chats []model.TeamsChat) []RoomCreatedRef {
	refs := make([]RoomCreatedRef, 0, len(chats))
	//nolint:gocritic // rangeValCopy: c is heavy but using index-range would be less idiomatic
	for _, c := range chats {
		refs = append(refs, RoomCreatedRef{ID: c.ID, UpdatedAt: c.UpdatedAt})
	}
	return refs
}
