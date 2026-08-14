package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// directPool owns one nats.Conn per simulated user plus one subscription per
// user-room pair. Each subscription callback records broadcast-arrival time
// against the shared Collector for latency correlation.
type directPool struct {
	url       string
	credsFile string
	collector *Collector

	// closing suppresses drop accounting once Close has begun — draining every
	// connection at end of run is not a lost recipient.
	closing atomic.Bool
	// dropped counts unexpected disconnects on recipient connections. A
	// recipient that went away mid-run cannot have its non-delivery attributed
	// to the system under test (verify spec §10).
	dropped atomic.Int64

	// sink is read on every received broadcast, so it is deliberately NOT
	// behind p.mu: in `daily` every direct user's every arrival would then
	// serialise on one pool-global lock. Collector is sharded 64 ways for
	// exactly that reason (collector.go) — an atomic load keeps the nil-sink
	// path (all of `daily`) lock-free.
	sink atomic.Pointer[deliverySink]

	mu    sync.Mutex
	users map[string]*directUser
	// lanes selects which room-broadcast lanes each room is subscribed on:
	// true = global (chat.room.…), false = local (chat.local.room.…). Both by
	// default, which is daily's behaviour; `verify --lane` narrows it.
	lanes []bool
}

// deliverySink receives per-recipient delivery attribution. Implemented by
// ProbeTracker. nil in daily runs, which keeps that path byte-for-byte
// identical to its previous behaviour.
type deliverySink interface {
	RecordDelivery(userID, msgID, roomID string, ln lane, at time.Time)
}

// laneForSubject classifies which subscription carried a message. The
// per-user lane is the only one where delivery is addressed rather than
// broadcast to a topic (spec §7.3).
func laneForSubject(subj string) lane {
	if strings.HasPrefix(subj, "chat.user.") {
		return laneUser
	}
	if strings.Contains(subj, ".local.") {
		return laneLocal
	}
	return laneGlobal
}

type directUser struct {
	id   string
	nc   *nats.Conn
	subs []*nats.Subscription
	// rooms is the set of room IDs this user already holds subscriptions for,
	// guarded by directPool.mu. It exists so SubscribeRoom is idempotent — see
	// the comment there.
	rooms map[string]struct{}
	// registered flips once the user is in p.users. Until then its connection
	// is not a tracked recipient, so losing it is not a dropped recipient —
	// see recordDisconnectFor.
	registered atomic.Bool
}

func newDirectPool(natsURL, credsFile string, c *Collector) *directPool {
	return &directPool{
		url: natsURL, credsFile: credsFile, collector: c,
		users: make(map[string]*directUser),
		lanes: []bool{true, false},
	}
}

// setLanes restricts which room-broadcast lanes are subscribed. Must be called
// before Add; an empty selection is ignored so the pool can never end up
// subscribed to nothing. Only `verify --lane` calls this — daily keeps the
// dual-lane default that makes it ROOM_SUBJECT_MODE-agnostic.
func (p *directPool) setLanes(lanes []bool) {
	if len(lanes) == 0 {
		return
	}
	p.mu.Lock()
	p.lanes = append([]bool(nil), lanes...)
	p.mu.Unlock()
}

func (p *directPool) laneSelection() []bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lanes
}

// DroppedRecipients reports how many recipient connections were lost mid-run.
func (p *directPool) DroppedRecipients() int64 { return p.dropped.Load() }

// recordDisconnect is the connection-lost hook. The closing gate matters: Close
// drains every connection at end of run, and counting those would make every
// run report a dropped recipient and land on INCONCLUSIVE.
func (p *directPool) recordDisconnect() {
	if p.closing.Load() {
		return
	}
	p.dropped.Add(1)
}

// recordDisconnectFor is the per-connection disconnect hook armed by Add.
//
// The registered gate is the point: Add arms the handler before it subscribes,
// and every one of its error paths Drains the connection it just opened.
// Draining closes it, the handler fires, and without this gate a subscribe or
// flush hiccup during activation would count a connection that never entered
// p.users and was never a tracked recipient. That reaches
// VerifyInputs.DroppedRecipients and turns an otherwise clean run
// INCONCLUSIVE for a background user's activation failure.
func (p *directPool) recordDisconnectFor(du *directUser) {
	if !du.registered.Load() {
		return
	}
	p.recordDisconnect()
}

// MissingUsers returns the subset of ids that are not in the pool. Counting
// pool size is not enough: activateUsers skips a user whose Add failed and the
// freed slot is backfilled by the next user, so Size() can reach the expected
// number with a designated user absent (verify spec §16.5).
func (p *directPool) MissingUsers(ids []string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, id := range ids {
		if _, ok := p.users[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// Add opens a connection for u and subscribes to every room in u.Rooms,
// plus the user-scoped subject for DM broadcasts. Safe to call concurrently
// for different users.
//
// Channel-room broadcasts arrive on subject.RoomEvent(roomID); DM and BotDM
// broadcasts arrive on subject.UserRoomEvent(account) — both are needed for
// realistic IM coverage since daily presets are DM-heavy.
//
// Each room is subscribed on both the global and local lanes by default so a
// run stays mode-agnostic: under ROOM_SUBJECT_MODE=local, hard-coding the global
// lane would silently receive nothing and report degraded latency instead of
// failing loudly. An operator who knows their stack's mode can halve the
// subscription count via setLanes (`verify --lane`).
func (p *directPool) Add(u *userState) error {
	du := &directUser{id: u.ID, rooms: make(map[string]struct{})}
	nc, err := connectWithCreds(p.url, "loadgen-daily-"+u.ID, p.credsFile,
		nats.DisconnectErrHandler(func(_ *nats.Conn, _ error) { p.recordDisconnectFor(du) }))
	if err != nil {
		return fmt.Errorf("connect for %s: %w", u.ID, err)
	}
	du.nc = nc
	lanes := p.laneSelection()
	for _, roomID := range u.Rooms {
		for _, global := range lanes {
			sub, err := nc.Subscribe(subject.RoomEvent(roomID, global), func(m *nats.Msg) {
				p.onBroadcast(m)
				p.deliver(u.ID, m.Data, laneForSubject(m.Subject))
			})
			if err != nil {
				_ = nc.Drain()
				return fmt.Errorf("subscribe room %s/%s: %w", u.ID, roomID, err)
			}
			du.subs = append(du.subs, sub)
		}
		du.rooms[roomID] = struct{}{}
	}
	// User-scoped subscription for DM broadcasts.
	userSub, err := nc.Subscribe(subject.UserRoomEvent(u.Account), func(m *nats.Msg) {
		p.onBroadcast(m)
		p.deliver(u.ID, m.Data, laneForSubject(m.Subject))
	})
	if err != nil {
		_ = nc.Drain()
		return fmt.Errorf("subscribe user %s: %w", u.ID, err)
	}
	du.subs = append(du.subs, userSub)
	// Flush so SUB commands reach the server before Add returns; otherwise
	// a publish immediately after Add can be dropped because the broker
	// hasn't registered interest yet. Same rationale as multiplexPool.Add.
	if err := nc.Flush(); err != nil {
		_ = nc.Drain()
		return fmt.Errorf("flush subs for %s: %w", u.ID, err)
	}
	// Arm drop accounting only now: from here on this connection is a tracked
	// recipient, so losing it really does invalidate the run's delivery claims.
	du.registered.Store(true)
	p.mu.Lock()
	p.users[u.ID] = du
	p.mu.Unlock()
	return nil
}

// Size reports the number of users currently in the pool.
func (p *directPool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.users)
}

func (p *directPool) onBroadcast(m *nats.Msg) {
	var evt model.RoomEvent
	if err := json.Unmarshal(m.Data, &evt); err != nil {
		return // ignore malformed
	}
	if evt.LastMsgID == "" {
		return
	}
	p.collector.RecordBroadcast(evt.LastMsgID, time.Now())
}

// attachSink wires a delivery sink. Must be called before Add.
func (p *directPool) attachSink(s deliverySink) {
	p.sink.Store(&s)
}

// deliver decodes a broadcast and attributes it to userID. Split out from the
// subscription callback so it is unit-testable without a NATS connection.
//
// The sink load is atomic, not mutex-guarded: this runs on every broadcast
// every direct user receives, and `daily` (which defaults to 20000 direct
// users and exists to produce latency numbers) never attaches a sink at all.
// Taking a single pool-global lock here would put daily's whole receive path
// behind one mutex — the same ceiling that made Collector shard 64 ways.
func (p *directPool) deliver(userID string, data []byte, ln lane) {
	sp := p.sink.Load()
	if sp == nil || *sp == nil {
		return
	}
	sink := *sp
	var evt model.RoomEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return // ignore malformed
	}
	if evt.LastMsgID == "" {
		return // membership/rename events carry no message
	}
	sink.RecordDelivery(userID, evt.LastMsgID, evt.RoomID, ln, time.Now())
}

// SubscribeRoom adds a room subscription to an already-connected user. Used
// when a reserve floater is added to a probe room mid-run — the same thing a
// real client does on joining.
//
// Idempotent: a room the user already holds returns nil without opening a
// second subscription. `verify`'s churn re-adds users it removed earlier —
// removeMember deliberately leaves the room subscription in place, while
// applyChange rebuilds its candidate set from the membership model, so the same
// floater becomes a legitimate join target again. Two live subscriptions on the
// identical subject deliver every later probe twice on the same lane, which the
// tracker reports as duplicate_delivery against a system that delivered exactly
// once.
func (p *directPool) SubscribeRoom(userID, roomID string) error {
	p.mu.Lock()
	du, ok := p.users[userID]
	if ok {
		if _, held := du.rooms[roomID]; held {
			p.mu.Unlock()
			return nil
		}
		// Claim before releasing the lock so two concurrent calls for the same
		// user-room pair cannot both get past the check and double-subscribe.
		if du.rooms == nil {
			du.rooms = make(map[string]struct{})
		}
		du.rooms[roomID] = struct{}{}
	}
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("subscribe room %s: user %s not in direct pool", roomID, userID)
	}
	// Release the claim on failure: the user does not hold a complete
	// subscription set for this room, and recording otherwise would be a lie in
	// pool state. A retry is not a duplicate risk in practice — addMember treats
	// a SubscribeRoom error as fatal and aborts churn rather than retrying.
	release := func() {
		p.mu.Lock()
		delete(du.rooms, roomID)
		p.mu.Unlock()
	}
	for _, global := range p.laneSelection() {
		subj := subject.RoomEvent(roomID, global)
		sub, err := du.nc.Subscribe(subj, func(m *nats.Msg) {
			p.deliver(userID, m.Data, laneForSubject(m.Subject))
		})
		if err != nil {
			release()
			return fmt.Errorf("subscribe room %s for %s: %w", roomID, userID, err)
		}
		p.mu.Lock()
		du.subs = append(du.subs, sub)
		p.mu.Unlock()
	}
	if err := du.nc.Flush(); err != nil {
		release()
		return fmt.Errorf("flush room subscription %s for %s: %w", roomID, userID, err)
	}
	return nil
}

// Close drains all connections.
func (p *directPool) Close() {
	// Set before draining so the disconnect handler doesn't count an orderly
	// shutdown as a lost recipient.
	p.closing.Store(true)
	p.mu.Lock()
	users := p.users
	p.users = nil
	p.mu.Unlock()
	for _, du := range users {
		_ = du.nc.Drain()
	}
}

// multiplexPool fans M shared NATS connections across N users. Each shared
// connection subscribes (with reference counting) to the union of room
// broadcast subjects for its assigned users. Incoming messages are routed
// to per-user inbox channels via the dispatch map.
type multiplexPool struct {
	url       string
	collector *Collector
	conns     []*nats.Conn

	mu        sync.Mutex
	roomRefs  map[string]int              // roomID -> ref count on the shared conns
	dispatch  map[string][]chan *nats.Msg // roomID -> per-user inboxes
	userInbox map[string]chan *nats.Msg   // userID -> that user's inbox channel
	nextConn  int                         // round-robin assignment
}

func newMultiplexPool(natsURL, credsFile string, c *Collector, size int) (*multiplexPool, error) {
	p := &multiplexPool{
		url: natsURL, collector: c,
		roomRefs:  make(map[string]int),
		dispatch:  make(map[string][]chan *nats.Msg),
		userInbox: make(map[string]chan *nats.Msg),
	}
	for i := 0; i < size; i++ {
		nc, err := connectWithCreds(natsURL, fmt.Sprintf("loadgen-daily-mux-%d", i), credsFile)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("multiplex conn %d: %w", i, err)
		}
		p.conns = append(p.conns, nc)
	}
	return p, nil
}

// connectWithCreds is the single dial helper for daily-IM pools and the
// publisher conn. When credsFile is non-empty, the connection is opened
// with nats.UserCredentials so it authenticates against operator-mode
// NATS servers; otherwise it falls back to anonymous dial (only valid
// against servers that allow anonymous, e.g. a minimal test setup).
// Without this, the daily-IM pools were silently dialing anonymous and
// getting "permissions violation" on subscribe.
// extra options are appended last, so a caller can attach connection-event
// handlers without every other caller changing.
func connectWithCreds(url, name, credsFile string, extra ...nats.Option) (*nats.Conn, error) {
	opts := []nats.Option{nats.Name(name)}
	if credsFile != "" {
		opts = append(opts, nats.UserCredentials(credsFile))
	}
	return nats.Connect(url, append(opts, extra...)...)
}

// Add registers a user with the multiplex pool. Subscribes the shared
// connection BEFORE mutating dispatch/refcount maps so a failed subscribe
// leaves the pool consistent (no orphaned inbox in dispatch).
func (p *multiplexPool) Add(u *userState) error {
	inbox := make(chan *nats.Msg, 128)
	p.mu.Lock()
	defer p.mu.Unlock()

	// First pass: subscribe to any new room subjects via round-robin conn.
	// Track which rooms we subscribed *in this Add* so partial failures can
	// be undone. (roomRefs already > 0 means an earlier user already
	// subscribed — no new sub needed.)
	for _, roomID := range u.Rooms {
		if p.roomRefs[roomID] > 0 || len(p.conns) == 0 {
			continue
		}
		nc := p.conns[p.nextConn%len(p.conns)]
		p.nextConn++
		// Subscribe both lanes (see directPool.Add) — mode-agnostic against ROOM_SUBJECT_MODE.
		for _, global := range []bool{true, false} {
			if _, err := nc.Subscribe(subject.RoomEvent(roomID, global), p.route); err != nil {
				return fmt.Errorf("multiplex subscribe %s: %w", roomID, err)
			}
		}
		// Mark provisionally with refcount 0 — the second pass below will
		// increment it. We don't increment here so a subsequent Subscribe
		// failure doesn't leave a dangling subscription.
	}

	// User-scoped subject for DM broadcasts. Subscribed per-user (no
	// refcount needed since UserRoomEvent is scoped to the account).
	if len(p.conns) > 0 {
		nc := p.conns[p.nextConn%len(p.conns)]
		p.nextConn++
		if _, err := nc.Subscribe(subject.UserRoomEvent(u.Account), p.route); err != nil {
			return fmt.Errorf("multiplex subscribe user %s: %w", u.ID, err)
		}
	}

	// Second pass: mutate state only after every Subscribe succeeded.
	p.userInbox[u.ID] = inbox
	for _, roomID := range u.Rooms {
		p.dispatch[roomID] = append(p.dispatch[roomID], inbox)
		p.roomRefs[roomID]++
	}

	// Flush every shared conn so the SUB commands reach the server before
	// Add returns. Without this, a caller (or test) that publishes
	// immediately after Add() may see the broadcast dropped because the
	// server hasn't registered the subscription interest yet. Production
	// emitters tick on a 1s schedule so they don't hit this race, but
	// tests and synchronous callers do. Flush per conn is one round-trip;
	// dominated by the Subscribe overhead already incurred.
	for _, nc := range p.conns {
		if err := nc.Flush(); err != nil {
			return fmt.Errorf("multiplex flush %s: %w", u.ID, err)
		}
	}
	return nil
}

// route is called by every shared conn's subscription callback. It looks up
// the destination inboxes by RoomID and does a non-blocking send.
// All inbox sends happen under p.mu so Close can safely set userInbox=nil
// without racing against an in-flight send-on-closed-channel.
func (p *multiplexPool) route(m *nats.Msg) {
	var evt model.RoomEvent
	if err := json.Unmarshal(m.Data, &evt); err != nil {
		return
	}
	roomID := evt.RoomID
	if roomID == "" {
		roomID = parseRoomFromSubject(m.Subject)
	}
	p.mu.Lock()
	inboxes := p.dispatch[roomID]
	if evt.LastMsgID != "" && p.collector != nil {
		p.collector.RecordBroadcast(evt.LastMsgID, time.Now())
	}
	dropCount := 0
	for _, ch := range inboxes {
		select {
		case ch <- m:
		default:
			dropCount++
		}
	}
	p.mu.Unlock()
	if dropCount > 0 && p.collector != nil {
		for i := 0; i < dropCount; i++ {
			p.collector.RecordMultiplexDrop()
		}
	}
}

// parseRoomFromSubject extracts the room ID from a "chat.room.<id>.event" subject.
func parseRoomFromSubject(subj string) string {
	parts := strings.Split(subj, ".")
	if len(parts) >= 3 && parts[0] == "chat" && parts[1] == "room" {
		return parts[2]
	}
	return ""
}

// Close drains shared conns. Inbox channels are NOT closed — letting GC
// reclaim them avoids a race between Close and an in-flight route() that
// holds a pre-lock-release inbox snapshot (would panic on send-on-closed).
// Once Drain returns, no further callbacks fire, so the channels are no
// longer referenced and become garbage.
func (p *multiplexPool) Close() {
	p.mu.Lock()
	p.userInbox = nil
	p.dispatch = nil
	p.roomRefs = nil
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, nc := range conns {
		_ = nc.Drain()
	}
}
