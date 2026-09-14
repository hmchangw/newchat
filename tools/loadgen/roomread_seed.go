package main

import (
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"time"
)

// roomReadFutureOffset places every seeded room's LastMsgAt comfortably past
// the longest ramp window. Because a read stamps LastSeenAt = time.Now() (the
// present), a reader's updated LastSeenAt stays before this future LastMsgAt, so
// handleMessageRead never short-circuits on "already caught up" and the
// MinSubscriptionLastSeenByRoomID floor scan fires on every request.
const roomReadFutureOffset = 24 * time.Hour

// roomReadLastSeenSpreadMin bounds how far behind LastMsgAt each subscription's
// seeded LastSeenAt may sit (in minutes). A spread means different members pin
// different floors, so the floor WRITE fires at a rate set by the read
// distribution rather than on every request.
const roomReadLastSeenSpreadMin = 7 * 24 * 60 // one week

// BuildRoomReadFixtures reuses the messages preset fixtures and stamps the
// read-state the mark-as-read workload needs: every room's LastMsgAt is set
// ahead of `now`, every subscription's LastSeenAt is spread behind it, and each
// room's MinUserLastSeenAt is set to the per-room minimum LastSeenAt (the floor
// the room document carries before the run). Deterministic for a given seed.
func BuildRoomReadFixtures(p *Preset, seed int64, siteID string, now time.Time) Fixtures {
	f := BuildFixtures(p, seed, siteID)
	r := rand.New(rand.NewSource(seed))
	lastMsgAt := now.UTC().Add(roomReadFutureOffset)

	for i := range f.Rooms {
		t := lastMsgAt
		f.Rooms[i].LastMsgAt = &t
	}

	minByRoom := make(map[string]time.Time, len(f.Rooms))
	for i := range f.Subscriptions {
		behind := time.Duration(1+r.Intn(roomReadLastSeenSpreadMin)) * time.Minute
		ls := lastMsgAt.Add(-behind).UTC()
		f.Subscriptions[i].LastSeenAt = &ls
		roomID := f.Subscriptions[i].RoomID
		if cur, ok := minByRoom[roomID]; !ok || ls.Before(cur) {
			minByRoom[roomID] = ls
		}
	}

	for i := range f.Rooms {
		if m, ok := minByRoom[f.Rooms[i].ID]; ok {
			mt := m.UTC()
			f.Rooms[i].MinUserLastSeenAt = &mt
		}
	}
	return f
}
