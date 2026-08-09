package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// changeKind distinguishes an add from a remove.
type changeKind int

const (
	changeAdd changeKind = iota
	changeRemove
)

// membershipChange is one applied add/remove plus the observations made about
// it after its settle window.
type membershipChange struct {
	kind   changeKind
	roomID string
	userID string
	epoch  int

	oracleSeen    bool // subscription.list was queried for this epoch
	oracleHasUser bool // and reported the user as a member

	sendSeen     bool // a send was attempted by the target after settle
	sendAccepted bool
}

// roomState tracks one probe room's membership history. history retains every
// past epoch's membership (not just the current set) so a probe published at
// epoch N can still be judged against epoch N's membership even if it is
// delivered after epoch N+1 has begun (spec §9.2). Collapsing this to a single
// current set would silently break that late-delivery judging.
type roomState struct {
	epoch     int
	members   map[string]struct{}
	history   [][]string // index = epoch, value = sorted members at that epoch
	settleEnd time.Time
}

// MembershipModel is loadgen's own model of probe-room membership — the oracle
// for what membership *should* be, built from the fixtures plus the changes
// loadgen itself issued. It is compared against subscription.list (what the
// system thinks membership is) via RecordOracle: if a membership write were
// lost, the system's actual state and its self-report would agree with each
// other, so a check that trusted subscription.list alone would see nothing
// wrong. Delivery is therefore judged against this model, never against the
// system's self-report (spec §9.3).
//
// Every exported method is safe for concurrent use: the publish hot path reads
// InSettle/Epoch/MembersAtEpoch while a churn goroutine writes via
// ApplyAdd/ApplyRemove.
type MembershipModel struct {
	mu      sync.Mutex
	rooms   map[string]*roomState
	changes []*membershipChange
	settle  time.Duration
}

// NewMembershipModel seeds the model from the probe-room set at epoch 0.
func NewMembershipModel(prs ProbeRoomSet) *MembershipModel {
	m := &MembershipModel{rooms: make(map[string]*roomState), settle: 5 * time.Second}
	for roomID, members := range prs.byRoom {
		set := make(map[string]struct{}, len(members))
		for _, u := range members {
			set[u] = struct{}{}
		}
		initial := append([]string(nil), members...)
		sort.Strings(initial)
		m.rooms[roomID] = &roomState{members: set, history: [][]string{initial}}
	}
	return m
}

// SetSettle configures the post-change quiet window.
func (m *MembershipModel) SetSettle(d time.Duration) {
	m.mu.Lock()
	m.settle = d
	m.mu.Unlock()
}

// Epoch returns the room's current membership epoch.
func (m *MembershipModel) Epoch(roomID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rs, ok := m.rooms[roomID]; ok {
		return rs.epoch
	}
	return 0
}

// Members returns the room's current membership, sorted. The returned slice is
// a copy: callers range over the result while ApplyAdd/ApplyRemove may be
// mutating the room concurrently, so returning the internal slice by reference
// would race.
func (m *MembershipModel) Members(roomID string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	rs, ok := m.rooms[roomID]
	if !ok {
		return nil
	}
	return append([]string(nil), rs.history[rs.epoch]...)
}

// MembersAtEpoch returns the membership in force at a past epoch, so a probe
// delivered after a change is still judged against the set that applied when
// it was published (spec §9.2). The returned slice is a copy — see Members.
func (m *MembershipModel) MembersAtEpoch(roomID string, epoch int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	rs, ok := m.rooms[roomID]
	if !ok || epoch < 0 || epoch >= len(rs.history) {
		return nil
	}
	return append([]string(nil), rs.history[epoch]...)
}

// InSettle reports whether the room is inside its post-change quiet window.
// While true, callers must not send probes into the room — the race between a
// membership write landing and broadcast-worker reading the member list makes
// either delivery outcome legitimate during this window, so no verdict can be
// drawn from a probe sent inside it.
func (m *MembershipModel) InSettle(roomID string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	rs, ok := m.rooms[roomID]
	if !ok {
		return false
	}
	return now.Before(rs.settleEnd)
}

// ApplyAdd records that loadgen added userID to roomID.
func (m *MembershipModel) ApplyAdd(roomID, userID string, now time.Time) {
	m.apply(changeAdd, roomID, userID, now)
}

// ApplyRemove records that loadgen removed userID from roomID.
func (m *MembershipModel) ApplyRemove(roomID, userID string, now time.Time) {
	m.apply(changeRemove, roomID, userID, now)
}

func (m *MembershipModel) apply(kind changeKind, roomID, userID string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rs, ok := m.rooms[roomID]
	if !ok {
		return
	}
	if kind == changeAdd {
		rs.members[userID] = struct{}{}
	} else {
		delete(rs.members, userID)
	}

	next := make([]string, 0, len(rs.members))
	for u := range rs.members {
		next = append(next, u)
	}
	sort.Strings(next)

	rs.epoch++
	rs.history = append(rs.history, next)
	rs.settleEnd = now.Add(m.settle)

	m.changes = append(m.changes, &membershipChange{
		kind: kind, roomID: roomID, userID: userID, epoch: rs.epoch,
	})
}

// RecordOracle records what subscription.list reported for a room at an epoch,
// so Finalize can compare loadgen's model against the system's self-report.
func (m *MembershipModel) RecordOracle(roomID string, observed []string, epoch int) {
	seen := make(map[string]struct{}, len(observed))
	for _, u := range observed {
		seen[u] = struct{}{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.changes {
		if c.roomID != roomID || c.epoch != epoch {
			continue
		}
		c.oracleSeen = true
		_, c.oracleHasUser = seen[c.userID]
	}
}

// RecordSendResult records whether the changed user's post-settle send into the
// room was accepted by the gatekeeper.
func (m *MembershipModel) RecordSendResult(roomID, userID string, accepted bool, epoch int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.changes {
		if c.roomID == roomID && c.userID == userID && c.epoch == epoch {
			c.sendSeen = true
			c.sendAccepted = accepted
		}
	}
}

// ChangeCounts summarises churn for the report.
type ChangeCounts struct {
	Total     int `json:"total"`
	Adds      int `json:"adds"`
	Removes   int `json:"removes"`
	Applied   int `json:"applied"`
	Effective int `json:"effective"`
}

// Counts returns churn summary counters.
func (m *MembershipModel) Counts() ChangeCounts {
	m.mu.Lock()
	defer m.mu.Unlock()
	var c ChangeCounts
	for _, ch := range m.changes {
		c.Total++
		if ch.kind == changeAdd {
			c.Adds++
		} else {
			c.Removes++
		}
		if ch.oracleSeen && ch.oracleHasUser == (ch.kind == changeAdd) {
			c.Applied++
		}
		if ch.sendSeen && ch.sendAccepted == (ch.kind == changeAdd) {
			c.Effective++
		}
	}
	return c
}

// Finalize evaluates every recorded change and returns its violations.
func (m *MembershipModel) Finalize() []Violation {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []Violation
	for _, c := range m.changes {
		wantMember := c.kind == changeAdd

		if c.oracleSeen && c.oracleHasUser != wantMember {
			out = append(out, Violation{
				Kind: KindMembershipNotApplied, RoomID: c.roomID,
				Users: []string{c.userID}, Epoch: c.epoch,
				Detail: fmt.Sprintf("subscription.list membership=%t after %s",
					c.oracleHasUser, changeName(c.kind)),
			})
		}

		if c.sendSeen && c.sendAccepted != wantMember {
			kind := KindMembershipRemoveIneffective
			detail := "send still accepted after remove"
			if wantMember {
				kind = KindMembershipAddIneffective
				detail = "send still rejected after add"
			}
			out = append(out, Violation{
				Kind: kind, RoomID: c.roomID, Users: []string{c.userID},
				Epoch: c.epoch, Detail: detail,
			})
		}
	}
	return out
}

func changeName(k changeKind) string {
	if k == changeAdd {
		return "add"
	}
	return "remove"
}
