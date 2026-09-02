package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/subject"
)

// errPlanEpochChanged marks a walk whose connection went away mid-RPC. It is
// an expected race, not a failure: the reconnect handler has already scheduled
// a resync, so run() must leave the client alive rather than spend one of its
// five restart attempts on something that self-heals.
var errPlanEpochChanged = errors.New("connection changed during the walk")

// openSub is one subscribed room: the two lanes a real client opens on it,
// plus the namespace they were opened on. Storing global here (instead of
// re-deriving it from the subject string) keeps the resync snapshot
// trivially correct.
type openSub struct {
	msg    simSub // chat.room.{id}.event        — messages and mutations
	member simSub // chat.room.{id}.event.member — roster add/remove
	global bool
}

// roomLane names the delivery counter a room subject feeds. The two lanes
// share one channel (and therefore one pump goroutine), so the subject is
// the only thing that tells them apart.
func roomLane(subj string) string {
	if strings.HasSuffix(subj, memberSubjectSuffix) {
		return "member"
	}
	return "channel"
}

// memberSubjectSuffix is what subject.RoomMemberEvent appends to the room
// base; a literal subscription on the message subject does not match it,
// which is why the member lane needs its own subscription.
const memberSubjectSuffix = ".event.member"

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
		if len(changes) > 0 {
			s.applyChangesLocked(changes)
			s.updateReadyLocked()
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

// applyChangesLocked opens/closes room subscriptions. Caller holds s.mu;
// holding the lock across the whole batch is what makes decide+apply atomic
// with respect to the resync walk (the review's lost-update race).
func (s *simClient) applyChangesLocked(changes []subChange) {
	conn := s.conn
	if conn == nil {
		return
	}
	for _, ch := range changes {
		switch ch.Op {
		case subClose:
			delete(s.missingRooms, ch.RoomID)
			if open, ok := s.roomSubs[ch.RoomID]; ok {
				// conn may already be closing; nothing to recover
				_ = open.msg.Unsubscribe()
				_ = open.member.Unsubscribe()
				delete(s.roomSubs, ch.RoomID)
			}
		case subOpen:
			if _, ok := s.roomSubs[ch.RoomID]; ok {
				delete(s.missingRooms, ch.RoomID)
				continue
			}
			open, err := s.openRoomLanes(conn, ch.RoomID, ch.Global)
			if err != nil {
				// Not recorded in roomSubs, so the next add/resync retries.
				// Until then this client is missing the room's traffic, which
				// is why the caller drops it out of the ready set.
				s.missingRooms[ch.RoomID] = struct{}{}
				s.m.Errors.WithLabelValues("room_subscribe").Inc()
				slog.Warn("open room subscription", "account", s.account, "roomId", ch.RoomID, "error", err)
				continue
			}
			delete(s.missingRooms, ch.RoomID)
			s.roomSubs[ch.RoomID] = open
		}
	}
}

// openRoomLanes subscribes both room lanes into the shared channel. A room
// counts as subscribed only when both are open: a half-open room would miss
// every event on the failed lane while looking complete, so the opened lane
// is rolled back and the caller records the room as missing.
func (s *simClient) openRoomLanes(conn simConn, roomID string, global bool) (openSub, error) {
	msg, err := conn.SubscribeChan(subject.RoomEvent(roomID, global), s.roomCh)
	if err != nil {
		return openSub{}, fmt.Errorf("subscribe room message lane: %w", err)
	}
	member, err := conn.SubscribeChan(subject.RoomMemberEvent(roomID, global), s.roomCh)
	if err != nil {
		_ = msg.Unsubscribe() // roll back, so a retry is not a duplicate
		return openSub{}, fmt.Errorf("subscribe room member lane: %w", err)
	}
	return openSub{msg: msg, member: member, global: global}, nil
}

// updateReadyLocked promotes only when a walk has verified the plan AND no
// asynchronous fault is outstanding AND every desired room subscription is
// open. Caller holds s.mu, which makes the checks atomic with the batch that
// just repaired or removed entries.
func (s *simClient) updateReadyLocked() {
	if !s.planVerified || s.asyncFault || len(s.missingRooms) > 0 {
		s.markNotReady()
		return
	}
	s.markReady()
}

// pump drains the shared room-subscription channel — one goroutine per
// client regardless of room count.
func (s *simClient) pump(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-s.roomCh:
			handleDelivery(s.m, roomLane(msg.Subject), msg.Data, time.Now())
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
	startEpoch := s.planEpoch
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
	if s.planEpoch != startEpoch {
		// The connection this plan was fetched over is gone. Applying it would
		// reconcile against a dead snapshot, and marking it verified would
		// vouch for a connection that no longer exists; the resync driving the
		// new connection redoes the work.
		return fmt.Errorf("bootstrap walk for %s: %w", s.account, errPlanEpochChanged)
	}
	changes := diffPlans(s.planViewLocked(), plan)
	kept := changes[:0]
	for _, ch := range changes {
		if s.touched[ch.RoomID] > startGen {
			continue // a live update during the RPC outranks the walk's snapshot
		}
		kept = append(kept, ch)
	}
	s.planVerified = true
	s.applyChangesLocked(kept)
	s.updateReadyLocked()
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

	attempt := 0
	for {
		delay := s.resyncJitter()
		if attempt > 0 {
			delay = s.resyncRetry(attempt)
		}
		jitter := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			jitter.Stop()
			s.finishResync()
			return
		case <-jitter.C:
		}
		if err := s.bootstrapWalk(ctx); err != nil {
			s.m.Errors.WithLabelValues("resync").Inc()
			attempt++
			slog.Warn("post-reconnect resync; retrying", "account", s.account, "attempt", attempt, "error", err)
			continue
		}
		attempt = 0
		s.resyncMu.Lock()
		if s.resyncPending {
			s.resyncPending = false
			s.resyncMu.Unlock()
			continue
		}
		// Set inactive while holding the same lock resync() uses to enqueue
		// follow-ups. This closes the old defer-based lost-wakeup window.
		s.resyncActive = false
		s.resyncMu.Unlock()
		return
	}
}

func (s *simClient) finishResync() {
	s.resyncMu.Lock()
	s.resyncActive = false
	s.resyncPending = false
	s.resyncMu.Unlock()
}

func defaultResyncRetryDelay(attempt int) time.Duration {
	const (
		minDelay = 250 * time.Millisecond
		maxDelay = 5 * time.Second
	)
	delay := minDelay
	for i := 1; i < attempt && delay < maxDelay/2; i++ {
		delay *= 2
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	// Add up to 50% jitter so a recovered broker is not hit by every client
	// on the same backoff boundary.
	if extraMax := int(delay / 2); extraMax > 0 {
		delay += time.Duration(secureIntN(extraMax))
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}
