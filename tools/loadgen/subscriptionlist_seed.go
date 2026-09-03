package main

import (
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// subListActivitySpread bounds how far back a room's last-message time may sit.
// The list sorts on this key, so a spread is what makes the comparator do real
// work; an all-equal key would collapse every comparison onto the name tiebreak.
const subListActivitySpread = 30 * 24 * time.Hour

// subListLastSeenSpread bounds how far behind a room's activity a member's
// LastSeenAt may sit. hasUnread is computed per row by comparing the two, so a
// spread means a page carries a realistic mix of read and unread rows rather
// than resolving the same way for every one.
const subListLastSeenSpread = 14 * 24 * time.Hour

// BuildSubscriptionListFixtures reuses the messages preset fixtures and stamps
// the three fields subscription.list's match filter reads and BuildFixtures
// leaves at their zero value:
//
//   - Open, because it has no bson omitempty — a false persists as open:false
//     and match["open"]={$ne:false} drops the row.
//   - RoomType, because the "current" match needs dm/channel (or a subscribed
//     botDM) and "" matches no branch.
//   - Name, which subLite projects for the self-DM pin and the name tiebreak.
//
// A miss on any of them returns an empty page rather than an error, so a ramp
// would measure the cost of finding nothing. Room activity keys and member
// LastSeenAt are spread so the sort and the hasUnread computation both do real
// work. Deterministic for a given seed.
func BuildSubscriptionListFixtures(p *Preset, seed int64, siteID string, now time.Time) Fixtures {
	f := BuildFixtures(p, seed, siteID)
	r := rand.New(rand.NewSource(seed))
	now = now.UTC()

	roomByID := make(map[string]*model.Room, len(f.Rooms))
	for i := range f.Rooms {
		room := &f.Rooms[i]
		roomByID[room.ID] = room
		at := now.Add(-time.Duration(r.Int63n(int64(subListActivitySpread)))).UTC()
		lastMsgAt := at
		room.LastMsgAt = &lastMsgAt
		// The unread reference is LastUserMsgAt ?? LastMsgAt; the fixtures carry
		// no system messages, so the two coincide.
		lastUserMsgAt := at
		room.LastUserMsgAt = &lastUserMsgAt
	}

	// A DM row's name is the counterpart's account, so collect each room's
	// members before naming — a channel needs only the room itself.
	membersByRoom := make(map[string][]string, len(f.Rooms))
	for i := range f.Subscriptions {
		s := &f.Subscriptions[i]
		membersByRoom[s.RoomID] = append(membersByRoom[s.RoomID], s.User.Account)
	}

	for i := range f.Subscriptions {
		s := &f.Subscriptions[i]
		room, ok := roomByID[s.RoomID]
		if !ok {
			continue
		}
		s.Open = true
		s.RoomType = room.Type
		s.Name = subListRowName(room, membersByRoom[s.RoomID], s.User.Account)

		behind := time.Duration(r.Int63n(int64(subListLastSeenSpread)))
		lastSeenAt := room.LastMsgAt.Add(-behind).UTC()
		s.LastSeenAt = &lastSeenAt
	}
	return f
}

// subListRowName mirrors how the service names a row: a channel carries the
// room's name, a DM the counterpart's account. A DM whose counterpart is
// missing (or a self-DM) falls back to the room name rather than empty, which
// the name tiebreak would sort inconsistently.
func subListRowName(room *model.Room, members []string, account string) string {
	if room.Type != model.RoomTypeDM {
		return room.Name
	}
	for _, m := range members {
		if m != account {
			return m
		}
	}
	return room.Name
}

// minSidebarSubsPerAccount is the mean below which a preset stops modelling a
// sidebar. The uniform presets (small/medium/large) put every account in exactly
// one room, so their pages carry a single row: a ramp over them reports the
// latency of a one-row reply, which is fast and says nothing about the endpoint
// under real sidebars. Only `realistic` has the mixed room sizes that give an
// account several rooms.
const minSidebarSubsPerAccount = 2.0

// subscriptionsPerAccount returns the mean subscriptions per distinct account,
// or 0 when there are none.
func subscriptionsPerAccount(f *Fixtures) float64 {
	accounts := make(map[string]struct{}, len(f.Subscriptions))
	for i := range f.Subscriptions {
		accounts[f.Subscriptions[i].User.Account] = struct{}{}
	}
	if len(accounts) == 0 {
		return 0
	}
	return float64(len(f.Subscriptions)) / float64(len(accounts))
}

// degeneratePageFixtures reports whether these fixtures would make the ramp
// measure a page too small to mean anything. Warned about rather than rejected:
// measuring the floor is a legitimate thing to want, as long as it is on purpose.
func degeneratePageFixtures(f *Fixtures) bool {
	return subscriptionsPerAccount(f) < minSidebarSubsPerAccount
}
