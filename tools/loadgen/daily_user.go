package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// actionKind enumerates the user-day operations the simulator can perform.
type actionKind int

const (
	actionSend actionKind = iota
	actionMarkRead
	actionScrollHistory
	actionRefreshRoomList
	actionMemberAdd
	actionRoomCreate
	actionMuteToggle
)

// String gives a stable lowercase name for use in reports, CSV headers,
// and log fields. Keep in sync with the const block above — the report
// code keys per-action stats by this name.
func (k actionKind) String() string {
	switch k {
	case actionSend:
		return "send"
	case actionMarkRead:
		return "mark_read"
	case actionScrollHistory:
		return "scroll_history"
	case actionRefreshRoomList:
		return "refresh_room_list"
	case actionMemberAdd:
		return "member_add"
	case actionRoomCreate:
		return "room_create"
	case actionMuteToggle:
		return "mute_toggle"
	default:
		return fmt.Sprintf("action_%d", k)
	}
}

// allActionKinds is the canonical ordered list, used by report code so
// the CSV column order is stable across runs.
var allActionKinds = []actionKind{
	actionSend, actionMarkRead, actionScrollHistory, actionRefreshRoomList,
	actionMemberAdd, actionRoomCreate, actionMuteToggle,
}

// actionWeights is the per-user-per-day count for each action kind.
// Source of truth: spec section 4 "daily-heavy" budget.
type actionWeights struct {
	Send            float64
	MarkRead        float64
	ScrollHistory   float64
	RefreshRoomList float64
	MemberAdd       float64
	RoomCreate      float64
	MuteToggle      float64
}

func defaultActionWeights() actionWeights {
	return actionWeights{
		Send: 60, MarkRead: 25, ScrollHistory: 3,
		RefreshRoomList: 5, MemberAdd: 0.5, RoomCreate: 0.2, MuteToggle: 0.2,
	}
}

func (w actionWeights) totalPerDay() float64 {
	return w.Send + w.MarkRead + w.ScrollHistory + w.RefreshRoomList +
		w.MemberAdd + w.RoomCreate + w.MuteToggle
}

// actionRatePerSecond converts a per-day count to a Poisson rate
// (actions per second), scaled to the active fraction of a workday.
func actionRatePerSecond(perDay float64, workday time.Duration) float64 {
	return perDay / workday.Seconds()
}

// pickAction returns one actionKind chosen with probability proportional
// to w. r is the source of randomness.
func pickAction(r *rand.Rand, w actionWeights) actionKind {
	total := w.totalPerDay()
	x := r.Float64() * total
	cumulative := []struct {
		k actionKind
		w float64
	}{
		{actionSend, w.Send},
		{actionMarkRead, w.MarkRead},
		{actionScrollHistory, w.ScrollHistory},
		{actionRefreshRoomList, w.RefreshRoomList},
		{actionMemberAdd, w.MemberAdd},
		{actionRoomCreate, w.RoomCreate},
		{actionMuteToggle, w.MuteToggle},
	}
	var acc float64
	for _, c := range cumulative {
		acc += c.w
		if x < acc {
			return c.k
		}
	}
	return actionSend
}

// userState is the per-user runtime state for a daily-IM simulated user.
type userState struct {
	ID      string
	Account string
	Rooms   []string
	// ChannelRooms is the subset of Rooms that are NOT DMs — pre-filtered
	// at activation so the memberAdd action (which room-service rejects
	// on DMs with "cannot add members to a non-channel room") doesn't
	// have to scan + filter every tick. DMs are detected by the fixture
	// builder's ID convention: BuildFixtures names DM rooms
	// "room-dm-NNNNNN" and the other bands "room-small-…"/"medium"/"large".
	ChannelRooms []string
	// Neighbor is an account guaranteed to exist in Mongo, != Account.
	// Used as a valid target for memberAdd and as the initial-user list
	// for roomCreate. Without it, those actions hit errUserNotFound
	// (memberAdd) or errEmptyCreateRequest (roomCreate, because a channel
	// needs at least one invitee besides the creator).
	Neighbor string
	active   bool
	// activeProb / idleProb: stay-in-state probabilities for the
	// idle/active Markov chain. Tuned in newUserState.
	activeProb float64
	idleProb   float64
	// presence is non-nil only when daily runs with --presence. It carries the
	// per-user presence state machine (hello/ping/activity).
	presence *presenceUser
}

func newUserState(id, account string, rooms []string, _seed int64) *userState {
	channels := make([]string, 0, len(rooms))
	for _, r := range rooms {
		if !strings.HasPrefix(r, "room-dm-") {
			channels = append(channels, r)
		}
	}
	return &userState{
		ID: id, Account: account, Rooms: rooms, ChannelRooms: channels,
		Neighbor: neighborOf(account),
		active:   false,
		// Tuned so stationary active fraction ≈ 25%: P(idle->active)=0.05, P(active->idle)=0.15.
		activeProb: 0.85, idleProb: 0.95,
	}
}

// neighborOf returns an account known to exist in Mongo that is != account.
// Account format is "user-N" per preset.go's BuildFixtures; we shift N by 1
// (wrapping at zero to N+1, so "user-0" → "user-1"). Falls back to "user-0"
// if the account doesn't match the expected format. For any preset with
// N ≥ 2 (which is all daily presets) this always produces a valid target.
func neighborOf(account string) string {
	var n int
	if _, err := fmt.Sscanf(account, "user-%d", &n); err != nil {
		return "user-0"
	}
	if n == 0 {
		return "user-1"
	}
	return fmt.Sprintf("user-%d", n-1)
}

// step advances the Markov chain by one tick. Call at the per-user tick
// interval (e.g. every 1s of simulated time).
func (u *userState) step(r *rand.Rand) {
	x := r.Float64()
	if u.active {
		if x > u.activeProb {
			u.active = false
		}
	} else {
		if x > u.idleProb {
			u.active = true
		}
	}
}
