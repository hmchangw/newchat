package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/subject"
)

// openSub is one open room subscription plus the namespace it was opened
// on. Storing global here (instead of re-deriving it from the subject
// string) keeps the resync snapshot trivially correct.
type openSub struct {
	sub    simSub
	global bool
}

// subscribeLanes opens the user event lane and the live update lane on the
// given connection — the update lane BEFORE the bootstrap walk so a
// membership change landing mid-bootstrap is not lost (spec §5.2).
func (s *simClient) subscribeLanes(conn simConn) error {
	if _, err := conn.SubscribeCB(subject.UserRoomEvent(s.account), func(msg *nats.Msg) {
		handleDelivery(s.m, "user", msg.Data, time.Now())
	}); err != nil {
		return fmt.Errorf("subscribe user lane for %s: %w", s.account, err)
	}

	if _, err := conn.SubscribeCB(subject.SubscriptionUpdate(s.account), func(msg *nats.Msg) {
		s.mu.Lock()
		defer s.mu.Unlock()
		view := s.planViewLocked()
		changes, err := applySubscriptionUpdate(view, msg.Data)
		if err != nil {
			s.m.DecodeFailures.Inc()
			return
		}
		for _, ch := range changes {
			s.gen++
			s.touched[ch.RoomID] = s.gen
		}
		if s.applyChangesLocked(changes) > 0 {
			s.markNotReady()
		}
	}); err != nil {
		return fmt.Errorf("subscribe update lane for %s: %w", s.account, err)
	}
	return nil
}

// planViewLocked derives the roomID -> global dedupe view from the open
// subscriptions. Caller holds s.mu.
func (s *simClient) planViewLocked() map[string]bool {
	view := make(map[string]bool, len(s.roomSubs))
	for roomID, open := range s.roomSubs {
		view[roomID] = open.global
	}
	return view
}

// applyChangesLocked opens/closes room subscriptions and returns the number
// of opens that failed. Caller holds s.mu; holding the lock across the whole
// batch is what makes decide+apply atomic with respect to the resync walk
// (the review's lost-update race).
func (s *simClient) applyChangesLocked(changes []subChange) int {
	conn := s.conn
	if conn == nil {
		return 0
	}
	failed := 0
	for _, ch := range changes {
		switch ch.Op {
		case subClose:
			if open, ok := s.roomSubs[ch.RoomID]; ok {
				_ = open.sub.Unsubscribe() // conn may already be closing; nothing to recover
				delete(s.roomSubs, ch.RoomID)
			}
		case subOpen:
			if _, ok := s.roomSubs[ch.RoomID]; ok {
				continue
			}
			sub, err := conn.SubscribeChan(subject.RoomEvent(ch.RoomID, ch.Global), s.roomCh)
			if err != nil {
				// Not recorded in roomSubs, so the next add/resync retries.
				// Until then this client is missing the room's traffic, which
				// is why the caller drops it out of the ready set.
				failed++
				s.m.Errors.WithLabelValues("room_subscribe").Inc()
				slog.Warn("open room subscription", "account", s.account, "roomId", ch.RoomID, "error", err)
				continue
			}
			s.roomSubs[ch.RoomID] = openSub{sub: sub, global: ch.Global}
		}
	}
	return failed
}

// pump drains the shared room-subscription channel — one goroutine per
// client regardless of room count.
func (s *simClient) pump(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-s.roomCh:
			handleDelivery(s.m, "channel", msg.Data, time.Now())
		}
	}
}

// bootstrapWalk fetches the subscription plan via the real client RPC and
// reconciles the open subscriptions with it. Rooms touched by live updates
// while the RPC was in flight are skipped — the update is newer than the
// server snapshot the walk is holding.
func (s *simClient) bootstrapWalk(ctx context.Context) error {
	conn := s.connSnapshot()
	if conn == nil {
		return fmt.Errorf("bootstrap walk for %s: connection closed", s.account)
	}
	s.mu.Lock()
	startGen := s.gen
	s.mu.Unlock()

	lister := &natsLister{
		conn:    conn,
		subject: subject.UserSubscriptionList(s.account, s.cfg.SiteID),
		timeout: 5 * time.Second,
	}
	plan, err := fetchSubscriptionPlan(ctx, lister)
	if err != nil {
		s.m.Errors.WithLabelValues("walk").Inc()
		return fmt.Errorf("bootstrap walk for %s: %w", s.account, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	changes := diffPlans(s.planViewLocked(), plan)
	kept := changes[:0]
	for _, ch := range changes {
		if s.touched[ch.RoomID] > startGen {
			continue // a live update during the RPC outranks the walk's snapshot
		}
		kept = append(kept, ch)
	}
	if s.applyChangesLocked(kept) > 0 {
		s.markNotReady()
		return nil
	}
	s.markReady()
	return nil
}

// resync re-runs the bootstrap walk after a reconnect. Coalescing, not
// single-flight-with-drop: a resync that arrives while one is running marks
// a follow-up instead of vanishing, because the in-flight walk fetched its
// snapshot before the change that triggered the second reconnect and nothing
// else would ever repair the difference. A small jitter keeps 10k clients
// from stampeding subscription.list after a broker restart.
func (s *simClient) resync(ctx context.Context) {
	s.resyncMu.Lock()
	if s.resyncActive {
		s.resyncPending = true
		s.resyncMu.Unlock()
		return
	}
	s.resyncActive = true
	s.resyncMu.Unlock()
	defer func() {
		s.resyncMu.Lock()
		s.resyncActive = false
		s.resyncMu.Unlock()
	}()

	for {
		jitter := time.NewTimer(s.resyncJitter())
		select {
		case <-ctx.Done():
			jitter.Stop()
			return
		case <-jitter.C:
		}
		if err := s.bootstrapWalk(ctx); err != nil {
			s.m.Errors.WithLabelValues("resync").Inc()
			slog.Warn("post-reconnect resync", "account", s.account, "error", err)
		}
		s.resyncMu.Lock()
		again := s.resyncPending
		s.resyncPending = false
		s.resyncMu.Unlock()
		if !again {
			return
		}
	}
}
