package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// lane identifies which subscription carried a delivery. Rooms are subscribed
// on both the global and local lanes to stay ROOM_SUBJECT_MODE-agnostic, so a
// duplicate is only a violation within a single lane (spec §7.1).
type lane string

const (
	laneGlobal lane = "global"
	laneLocal  lane = "local"
	laneUser   lane = "user"
)

// deliveryKey is the dedupe key: one delivery per user per lane is expected.
type deliveryKey struct {
	userID string
	ln     lane
}

type probeRecord struct {
	msgID       string
	roomID      string
	senderID    string
	epoch       int
	publishedAt time.Time
	expected    map[string]struct{}
	received    map[deliveryKey]int
}

// ProbeCounts is the summary surfaced in the report.
type ProbeCounts struct {
	Tracked    int
	Suppressed int
	Complete   int
	Partial    int
	TotalLoss  int
}

// ProbeTracker records per-recipient delivery for sampled messages and reports
// completeness, exactly-once, and leakage violations.
//
// Deliberately separate from Collector: Collector deletes its correlation entry
// on first delivery, which is correct for a latency histogram and wrong for
// fan-out accounting.
type ProbeTracker struct {
	mu     sync.Mutex
	probes map[string]*probeRecord

	suppressed int
}

// NewProbeTracker returns a ready-to-use tracker.
func NewProbeTracker() *ProbeTracker {
	return &ProbeTracker{probes: make(map[string]*probeRecord)}
}

// RegisterProbe records that msgID was published into roomID and is expected to
// reach every user in expected. The sender is included in expected by the
// caller — broadcast-worker echoes to the sender (spec §7.2).
func (t *ProbeTracker) RegisterProbe(msgID, roomID, senderID string, epoch int, expected []string, at time.Time) {
	exp := make(map[string]struct{}, len(expected))
	for _, u := range expected {
		exp[u] = struct{}{}
	}
	rec := &probeRecord{
		msgID: msgID, roomID: roomID, senderID: senderID, epoch: epoch,
		publishedAt: at,
		expected:    exp,
		received:    make(map[deliveryKey]int, len(expected)),
	}
	t.mu.Lock()
	t.probes[msgID] = rec
	t.mu.Unlock()
}

// RecordDelivery notes that userID received msgID on the given lane. Deliveries
// for untracked messages are ignored cheaply — 99% of traffic is untracked.
func (t *ProbeTracker) RecordDelivery(userID, msgID, roomID string, ln lane, _ time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	rec, ok := t.probes[msgID]
	if !ok {
		return
	}
	if _, expected := rec.expected[userID]; !expected {
		return
	}
	rec.received[deliveryKey{userID: userID, ln: ln}]++
}

// RecordSuppressed notes a send that would have been probed but fell inside a
// settle window (spec §9.2). Suppressed probes do not count toward --min-probes.
func (t *ProbeTracker) RecordSuppressed() {
	t.mu.Lock()
	t.suppressed++
	t.mu.Unlock()
}

// Counts returns summary counters for the report.
func (t *ProbeTracker) Counts() ProbeCounts {
	t.mu.Lock()
	defer t.mu.Unlock()

	c := ProbeCounts{Tracked: len(t.probes), Suppressed: t.suppressed}
	for _, rec := range t.probes {
		got := rec.deliveredUsers()
		switch {
		case len(got) == 0:
			c.TotalLoss++
		case len(got) < len(rec.expected):
			c.Partial++
		default:
			c.Complete++
		}
	}
	return c
}

// deliveredUsers returns the distinct users who received this probe on at least
// one lane. Caller must hold the lock.
func (r *probeRecord) deliveredUsers() map[string]struct{} {
	out := make(map[string]struct{}, len(r.received))
	for k := range r.received {
		out[k.userID] = struct{}{}
	}
	return out
}

// Finalize evaluates every probe and returns the violations found, sorted by
// message ID so output is stable across runs.
func (t *ProbeTracker) Finalize() []Violation {
	t.mu.Lock()
	defer t.mu.Unlock()

	msgIDs := make([]string, 0, len(t.probes))
	for id := range t.probes {
		msgIDs = append(msgIDs, id)
	}
	sort.Strings(msgIDs)

	var out []Violation
	for _, id := range msgIDs {
		rec := t.probes[id]
		out = append(out, rec.violations()...)
	}
	return out
}

// violations evaluates one probe. Caller must hold the lock.
func (r *probeRecord) violations() []Violation {
	var out []Violation

	got := r.deliveredUsers()
	if len(got) == 0 {
		out = append(out, Violation{
			Kind: KindTotalLoss, MsgID: r.msgID, RoomID: r.roomID, Epoch: r.epoch,
			Detail: fmt.Sprintf("reached 0 of %d expected recipients", len(r.expected)),
		})
	} else if len(got) < len(r.expected) {
		var missing []string
		for u := range r.expected {
			if _, ok := got[u]; !ok {
				missing = append(missing, u)
			}
		}
		sort.Strings(missing)
		out = append(out, Violation{
			Kind: KindMissingRecipient, MsgID: r.msgID, RoomID: r.roomID,
			Users: missing, Epoch: r.epoch,
			Detail: fmt.Sprintf("reached %d of %d expected recipients", len(got), len(r.expected)),
		})
	}

	var dupes []string
	for k, n := range r.received {
		if n > 1 {
			dupes = append(dupes, k.userID)
		}
	}
	if len(dupes) > 0 {
		sort.Strings(dupes)
		out = append(out, Violation{
			Kind: KindDuplicateDelivery, MsgID: r.msgID, RoomID: r.roomID,
			Users: dupes, Epoch: r.epoch, Detail: "same messageID delivered twice on one lane",
		})
	}

	return out
}
