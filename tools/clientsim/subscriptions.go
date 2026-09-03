package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
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
func (s *simClient) subscribeLanes(ctx context.Context, conn simConn) error {
	if _, err := conn.SubscribeCB(subject.UserRoomEvent(s.account), func(msg *nats.Msg) {
		handleDelivery(s.m, "user", msg.Data, time.Now())
	}); err != nil {
		return fmt.Errorf("subscribe user lane for %s: %w", s.account, err)
	}

	if _, err := conn.SubscribeCB(subject.SubscriptionUpdate(s.account), func(msg *nats.Msg) {
		s.mu.Lock()
		defer s.mu.Unlock()
		view := s.planViewLocked()
		changes, asserted, err := applySubscriptionUpdate(view, msg.Data)
		if err != nil {
			// A control message we cannot parse is the same hazard as one we
			// never received: it may have carried a membership change, so the
			// plan this client holds is no longer proven. Counting it and
			// returning left the client vouching for a plan it could not.
			s.m.DecodeFailures.Inc()
			s.invalidateForControlFaultLocked()
			go s.resync(ctx)
			return
		}
		// Stamped on the asserted room even when the event produced no change:
		// the generation is what makes a live update outrank an in-flight
		// walk's older snapshot, and an idempotent confirmation is still
		// newer than that snapshot.
		if asserted != "" {
			s.gen++
			s.touched[asserted] = s.gen
		}
		if len(changes) > 0 {
			// The RESULT, not the intent: an open the cap refused or whose
			// lanes failed queues nothing, so there is nothing for the broker
			// to acknowledge. Flushing for it would spawn a goroutine and a
			// PING per event — at the cap, once for every further room a
			// faulty publisher names, which is the unbounded growth the cap
			// exists to stop wearing a different hat.
			if s.applyChangesLocked(changes) {
				// A new SUB is only queued locally. Core NATS does not replay,
				// so a client that keeps vouching here misses anything
				// published before the broker installs it — permanently, with
				// the gauge never dipping. The walk already flushes before it
				// promotes; this is the same rule on the live path.
				s.markNotReady()
				s.liveGen++
				go s.flushThenPromote(ctx, s.liveGen)
				return
			}
			// A close-only change cannot make the client miss traffic, so it
			// needs no acknowledgement round-trip.
			s.updateReadyLocked()
		}
	}); err != nil {
		return fmt.Errorf("subscribe update lane for %s: %w", s.account, err)
	}
	return nil
}

// invalidateForControlFaultLocked marks the plan unproven because a
// subscription.update was lost or unusable, and advances the fence a walk in
// flight checks before it promotes. Caller holds s.mu.
func (s *simClient) invalidateForControlFaultLocked() {
	s.planVerified = false
	s.controlGen++
	s.updateReadyLocked() // one place decides readiness; lock order s.mu -> stateMu
}

// flushThenPromote waits for the broker to acknowledge the SUBs a live update
// just queued, then re-evaluates readiness. A failed flush leaves the client
// not-ready and schedules the resync that re-derives and re-flushes the plan —
// the client cannot prove the broker has its subscriptions until one succeeds.
func (s *simClient) flushThenPromote(ctx context.Context, gen uint64) {
	conn := s.connSnapshot()
	if conn == nil {
		return
	}
	flushCtx, cancel := context.WithTimeout(ctx, subFlushTimeout)
	defer cancel()
	if err := conn.FlushWithContext(flushCtx); err != nil {
		s.m.Errors.WithLabelValues("flush").Inc()
		go s.resync(ctx)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.liveGen != gen {
		// A later add already demoted and is waiting on its own flush. This
		// one's acknowledgement says nothing about that newer SUB, so leave
		// the promotion to the flush that covers it.
		return
	}
	s.updateReadyLocked()
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
func (s *simClient) applyChangesLocked(changes []subChange) bool {
	conn := s.conn
	if conn == nil {
		return false
	}
	queued := false
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
			if len(s.roomSubs) >= maxRoomsPerClient {
				s.m.Errors.WithLabelValues("room_cap").Inc()
				// Bounded by the same number: at the cap the client is
				// already unready, so recording every further room would
				// just move the unbounded growth into this map.
				if len(s.missingRooms) < maxRoomsPerClient {
					s.missingRooms[ch.RoomID] = struct{}{}
				}
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
			queued = true
		}
	}
	return queued
}

// maxRoomsPerClient bounds the room subscriptions one simulated client will
// hold. Each open room costs two NATS subscriptions retained until a matching
// removal or teardown, so a membership publisher stuck emitting unique room
// IDs during a soak grows that without bound in every client at once — and
// the process it kills is the one holding the run's measurement.
//
// Past the cap the room is recorded missing instead of opened, which drops
// the client out of the ready set and fails the exit gate: loud, and with the
// numbers still readable. 5000 is far above any real sidebar (a heavy account
// runs to hundreds), so tripping it is a finding about the control plane, not
// a limit to raise — clientsim_errors_total{stage="room_cap"} names it.
const maxRoomsPerClient = 5000

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
			// Sampled before handling, so it reports the backlog this message
			// was taken from rather than the one left after it.
			s.m.RoomQueueDepth.Observe(float64(len(s.roomCh)))
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
	startControl := s.controlGen
	s.mu.Unlock()

	lister := &natsLister{
		conn:    conn,
		subject: subject.UserSubscriptionList(s.account, s.cfg.SiteID),
		timeout: 5 * time.Second,
	}
	plan, err := fetchSubscriptionPlan(ctx, lister)
	// Counted on success and on failure alike: the exposure is the boundary
	// crossing itself, not what the walk went on to do with the rows.
	if lister.calls > 1 {
		s.m.PaginatedWalks.Inc()
	}
	if err != nil {
		// Check the epoch BEFORE blaming the RPC. A connection that dies
		// mid-walk usually makes the request fail rather than return a stale
		// plan, so this is the likelier half of the race — and counting it as
		// a walk failure would swamp the error rate during a broker bounce.
		if s.planEpochChanged(startEpoch) {
			return fmt.Errorf("bootstrap walk for %s: %w", s.account, errPlanEpochChanged)
		}
		s.m.Errors.WithLabelValues("walk").Inc()
		return fmt.Errorf("bootstrap walk for %s: %w", s.account, err)
	}

	if err := s.applyPlan(plan, startGen, startEpoch, startControl); err != nil {
		return err
	}

	// The SUBs just opened are still in nats.go's write buffer. Promoting now
	// would count this client as ready over subscriptions the broker does not
	// hold yet, and at ramp time thousands of clients sit in that window at
	// once — the readiness gauge would lead reality by exactly the interval an
	// operator uses to decide the fleet is up. Flushing outside s.mu keeps the
	// round-trip off the live-update lane.
	flushCtx, cancel := context.WithTimeout(ctx, subFlushTimeout)
	defer cancel()
	if err := conn.FlushWithContext(flushCtx); err != nil {
		s.m.Errors.WithLabelValues("flush").Inc()
		return fmt.Errorf("bootstrap walk for %s: flush subscriptions: %w", s.account, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.planEpoch != startEpoch {
		return fmt.Errorf("bootstrap walk for %s: %w", s.account, errPlanEpochChanged)
	}
	if s.controlGen != startControl {
		// A control message was lost while this walk was in flight, so the
		// plan it holds predates the change that message carried. Promoting
		// would erase the fault with a snapshot that is older than it. The
		// fault already scheduled the resync that re-derives the plan, so this
		// is the expected race rather than a failure.
		return fmt.Errorf("bootstrap walk for %s: %w", s.account, errPlanEpochChanged)
	}
	// Verified only here, after the broker has acknowledged the flush: this is
	// the single point that can promote a client on a walk.
	s.planVerified = true
	s.updateReadyLocked()
	return nil
}

// subFlushTimeout bounds the pre-promotion round-trip. Long enough that a
// loaded broker is not mistaken for a dead one, short enough that a client
// cannot sit unpromoted behind it for a whole ramp.
const subFlushTimeout = 5 * time.Second

// applyPlan reconciles the open subscriptions with a fetched plan under s.mu.
// It deliberately does NOT promote: readiness waits for the flush.
func (s *simClient) applyPlan(plan map[string]bool, startGen, startEpoch, startControl uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.controlGen != startControl {
		// Checked BEFORE applying, not only before promoting. A control-plane
		// fault schedules its own repair walk; if that walk finishes first and
		// opens a room, this older walk would still CLOSE it on the way to
		// failing its promotion check — leaving the client ready (from the
		// repair) but missing the room, with nothing left to notice.
		return fmt.Errorf("bootstrap walk for %s: %w", s.account, errPlanEpochChanged)
	}
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
	// The walk flushes unconditionally before it promotes, so whether this
	// batch queued anything does not change what happens next.
	_ = s.applyChangesLocked(kept)
	s.reconcileBookkeepingLocked(plan, startGen)
	return nil
}

// reconcileBookkeepingLocked settles the two maps diffPlans cannot see.
// Caller holds s.mu.
//
// missingRooms is not derived from roomSubs, so a room whose subscribe failed
// is invisible to the diff: once the server stops listing it, nothing clears
// the entry and the client stays out of the ready set for the rest of the
// soak over a room that no longer exists. touched is keyed by room and written
// on every live update, so without pruning it retains every room the account
// ever saw — in every one of tens of thousands of clients, for hours.
//
// Both skip anything a live update stamped after this walk's snapshot: that
// update is the fresher fact, exactly as in the kept-filter above.
func (s *simClient) reconcileBookkeepingLocked(plan map[string]bool, startGen uint64) {
	for roomID := range s.missingRooms {
		if _, want := plan[roomID]; want {
			continue // still desired and still broken; the repair path owns it
		}
		if s.touched[roomID] > startGen {
			continue
		}
		delete(s.missingRooms, roomID)
	}
	for roomID, g := range s.touched {
		if g > startGen {
			continue
		}
		if _, open := s.roomSubs[roomID]; open {
			continue
		}
		if _, missing := s.missingRooms[roomID]; missing {
			continue
		}
		delete(s.touched, roomID)
	}
}

// planEpochChanged reports whether the connection was replaced since the
// caller sampled startEpoch.
func (s *simClient) planEpochChanged(startEpoch uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.planEpoch != startEpoch
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
			// A rejection the server will repeat identically is not worth
			// retrying: tens of thousands of clients re-asking a
			// permanently-rejecting responder every few seconds is a load
			// test of the wrong thing, and it never converges. Giving up
			// leaves planVerified false, so the client stays out of the ready
			// set and the exit gate fails the run — which is the loud
			// outcome. Infra failures (timeouts, no responders, a broker
			// bounce) are NOT terminal and keep retrying forever, because a
			// soak whose broker recovers must recover with it.
			if terminal, why := terminalWalkError(err); terminal {
				s.m.Errors.WithLabelValues("resync_terminal").Inc()
				slog.Error("post-reconnect resync abandoned", "account", s.account,
					"reason", why, "error", err)
				s.finishResync()
				return
			}
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

// terminalWalkError reports whether a failed walk will fail identically on
// every retry: a client-fault errcode from the responder (bad_request,
// forbidden, not_found — errcode.Terminal's own definition), or a reply whose
// shape the walk cannot use. Everything else, errcode or not, is infra.
func terminalWalkError(err error) (bool, string) {
	if ee, terminal := errcode.Terminal(err); terminal {
		return true, string(ee.Code)
	}
	if errors.Is(err, errWalkProtocol) {
		return true, "protocol"
	}
	return false, ""
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
	// The loop stops the first time delay reaches maxDelay/2, so it can never
	// leave delay above maxDelay — only the jitter below can, and only the
	// clamp after it is reachable.
	delay := minDelay
	for i := 1; i < attempt && delay < maxDelay/2; i++ {
		delay *= 2
	}
	// Add up to 50% jitter so a recovered broker is not hit by every client
	// on the same backoff boundary. delay is always >= minDelay, and
	// secureIntN treats a non-positive bound as zero regardless.
	delay += time.Duration(secureIntN(int(delay / 2)))
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}
