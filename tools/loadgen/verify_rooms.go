package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
)

// ViolationKind enumerates the correctness failures verify can report.
// Values are stable strings — they appear in JSON output and operator runbooks.
type ViolationKind string

const (
	KindMissingRecipient            ViolationKind = "missing_recipient"
	KindTotalLoss                   ViolationKind = "total_loss"
	KindDuplicateDelivery           ViolationKind = "duplicate_delivery"
	KindUnexpectedRecipient         ViolationKind = "unexpected_recipient"
	KindPersistenceMiss             ViolationKind = "persistence_miss"
	KindPersistenceMismatch         ViolationKind = "persistence_mismatch"
	KindMembershipNotApplied        ViolationKind = "membership_not_applied"
	KindMembershipAddIneffective    ViolationKind = "membership_add_ineffective"
	KindMembershipRemoveIneffective ViolationKind = "membership_remove_ineffective"
)

// Violation is one concrete correctness failure. Users carries the specific
// user IDs implicated so an operator can grep service logs directly.
// Detail never contains message content.
type Violation struct {
	Kind   ViolationKind `json:"kind"`
	MsgID  string        `json:"msgId,omitempty"`
	RoomID string        `json:"roomId,omitempty"`
	Users  []string      `json:"users,omitempty"`
	Epoch  int           `json:"epoch"`
	Detail string        `json:"detail,omitempty"`
}

// band classifies a fixture room by its ID prefix. BuildFixtures names rooms
// room-dm-NNNNNN / room-small-… / room-medium-… / room-large-….
type band int

const (
	bandUnknown band = iota
	bandDM
	bandSmall
	bandMedium
	bandLarge
)

func bandOf(roomID string) band {
	switch {
	case strings.HasPrefix(roomID, "room-dm-"):
		return bandDM
	case strings.HasPrefix(roomID, "room-small-"):
		return bandSmall
	case strings.HasPrefix(roomID, "room-medium-"):
		return bandMedium
	case strings.HasPrefix(roomID, "room-large-"):
		return bandLarge
	default:
		return bandUnknown
	}
}

// usesUserLane reports whether broadcasts for this room are addressed
// per-recipient (subject.UserRoomEvent) rather than published once to the
// room topic. Only DM rooms use the per-user lane, and only there is the
// leakage check meaningful — see spec §7.3.
func usesUserLane(roomID string) bool { return bandOf(roomID) == bandDM } //nolint:unused // reserved for the leakage check (spec §7.3), added in a later task

// ProbeRoomSet is the outcome of probe-room selection: the chosen rooms, the
// complete union of their members, and a per-room member index.
type ProbeRoomSet struct {
	Rooms   []model.Room
	Members []string // sorted union, deterministic
	byRoom  map[string][]string
}

// RoomIDs returns the selected room IDs in selection order.
func (p ProbeRoomSet) RoomIDs() []string {
	out := make([]string, len(p.Rooms))
	for i := range p.Rooms {
		out[i] = p.Rooms[i].ID
	}
	return out
}

// MembersOf returns the member user IDs of one probe room.
func (p ProbeRoomSet) MembersOf(roomID string) []string { return p.byRoom[roomID] }

// Has reports whether roomID is a probe room.
func (p ProbeRoomSet) Has(roomID string) bool {
	_, ok := p.byRoom[roomID]
	return ok
}

// probeBands is the fixed band mix. DM is mandatory: it is the only band on
// the per-user lane, so a DM-free probe set would leave the leakage check
// permanently unexercised (spec §6.0 step 1).
var probeBands = []band{bandDM, bandSmall, bandMedium}

// selectProbeRooms picks n rooms deterministically from seed, taking an equal
// share from each of probeBands and excluding any room at or above
// largeThreshold. Returns an error rather than silently under-filling, since a
// thin probe set produces a weak verdict.
func selectProbeRooms(fx Fixtures, n, largeThreshold int, seed int64) (ProbeRoomSet, error) { //nolint:gocritic // hugeParam: fx passed by value to match the caller-facing selection interface used by later verify tasks
	if n <= 0 {
		return ProbeRoomSet{}, fmt.Errorf("probe room count must be positive, got %d", n)
	}

	eligible := map[band][]model.Room{}
	for i := range fx.Rooms {
		room := &fx.Rooms[i]
		b := bandOf(room.ID)
		if b == bandLarge || b == bandUnknown {
			continue
		}
		if room.UserCount >= largeThreshold {
			continue // gatekeeper would reject sends here — spec §6.1
		}
		eligible[b] = append(eligible[b], *room)
	}

	// Sort each band by ID so selection is independent of fixture iteration order.
	for b := range eligible {
		sort.Slice(eligible[b], func(i, j int) bool { return eligible[b][i].ID < eligible[b][j].ID })
	}

	r := rand.New(rand.NewSource(seed))
	perBand := n / len(probeBands)
	remainder := n % len(probeBands)

	var chosen []model.Room
	for i, b := range probeBands {
		want := perBand
		if i < remainder {
			want++
		}
		pool := eligible[b]
		if len(pool) < want {
			return ProbeRoomSet{}, fmt.Errorf(
				"not enough eligible rooms in band %d: want %d, have %d", b, want, len(pool))
		}
		perm := r.Perm(len(pool))[:want]
		sort.Ints(perm)
		for _, idx := range perm {
			chosen = append(chosen, pool[idx])
		}
	}

	byRoom := make(map[string][]string, len(chosen))
	for i := range chosen {
		byRoom[chosen[i].ID] = nil
	}
	memberSet := map[string]struct{}{}
	for i := range fx.Subscriptions {
		s := &fx.Subscriptions[i]
		if _, ok := byRoom[s.RoomID]; !ok {
			continue
		}
		byRoom[s.RoomID] = append(byRoom[s.RoomID], s.User.ID)
		memberSet[s.User.ID] = struct{}{}
	}
	for id := range byRoom {
		sort.Strings(byRoom[id])
	}

	members := make([]string, 0, len(memberSet))
	for id := range memberSet {
		members = append(members, id)
	}
	sort.Strings(members)

	return ProbeRoomSet{Rooms: chosen, Members: members, byRoom: byRoom}, nil
}

// selectReserve picks n users who are not already probe-room members. They are
// direct-connected as floaters so a membership change mid-run has an
// observable target (spec §6.0 step 3).
func selectReserve(fx Fixtures, prs ProbeRoomSet, n int, seed int64) []string { //nolint:gocritic // hugeParam: fx passed by value to match the caller-facing selection interface used by later verify tasks
	inProbe := make(map[string]struct{}, len(prs.Members))
	for _, id := range prs.Members {
		inProbe[id] = struct{}{}
	}

	candidates := make([]string, 0, len(fx.Users))
	for i := range fx.Users {
		if _, ok := inProbe[fx.Users[i].ID]; !ok {
			candidates = append(candidates, fx.Users[i].ID)
		}
	}
	sort.Strings(candidates)

	// Offset the seed so the reserve permutation is independent of the
	// room permutation drawn from the same seed.
	r := rand.New(rand.NewSource(seed ^ 0x5EED0BE5))
	if n > len(candidates) {
		n = len(candidates)
	}
	perm := r.Perm(len(candidates))[:n]
	sort.Ints(perm)

	out := make([]string, 0, n)
	for _, idx := range perm {
		out = append(out, candidates[idx])
	}
	return out
}
