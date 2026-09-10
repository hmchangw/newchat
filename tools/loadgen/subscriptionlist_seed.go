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

// subListLastSeenSpread bounds how far a member's LastSeenAt sits from the
// room's activity, in either direction. hasUnread is computed per row by
// comparing the two, so the sign is what decides the branch.
const subListLastSeenSpread = 14 * 24 * time.Hour

// subListCaughtUpShare is the fraction of subscriptions seeded at or after their
// room's activity, i.e. read. Drawing LastSeenAt only from behind made every row
// unread, so the caught-up branch never ran and every row resolved the same way.
const subListCaughtUpShare = 0.4

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

		s.LastSeenAt = seededLastSeenAt(r, *room.LastMsgAt)
	}
	return f
}

// seededLastSeenAt places a member either at/after the room's activity (read) or
// behind it (unread), so a page carries both. Ahead is capped well short of the
// activity spread so a caught-up member cannot be dragged past `now`.
func seededLastSeenAt(r *rand.Rand, lastMsgAt time.Time) *time.Time {
	offset := time.Duration(r.Int63n(int64(subListLastSeenSpread)))
	at := lastMsgAt.Add(-offset).UTC()
	if r.Float64() < subListCaughtUpShare {
		// Equal counts as read, so a zero offset is a legitimate draw here.
		at = lastMsgAt.Add(offset % time.Hour).UTC()
	}
	return &at
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
