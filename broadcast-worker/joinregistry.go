package main

import (
	"sync"
	"time"
)

// joinRegistry remembers who joined which channel recently, so the channel
// fan-out can find them without a roster query.
//
// A channel message is one publish to the room subject by design, and that
// must stay true: the steady-state cost here is a single map lookup that
// misses. Entries arrive as join notices from room-worker, expire with the
// grace window, and are swept whenever a notice arrives - joins are rare
// relative to messages, so the sweep never lands on the message path.
type joinRegistry struct {
	mu    sync.RWMutex
	ttl   time.Duration
	now   func() time.Time
	rooms map[string]map[string]time.Time
}

func newJoinRegistry(ttl time.Duration, now func() time.Time) *joinRegistry {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &joinRegistry{ttl: ttl, now: now, rooms: map[string]map[string]time.Time{}}
}

// Record notes that accounts joined roomID at joinedAt, and sweeps rooms whose
// entries have all expired.
func (r *joinRegistry) Record(roomID string, accounts []string, joinedAt time.Time) {
	if r.ttl <= 0 || roomID == "" || len(accounts) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	room := r.rooms[roomID]
	if room == nil {
		room = make(map[string]time.Time, len(accounts))
		r.rooms[roomID] = room
	}
	for _, account := range accounts {
		if account != "" {
			room[account] = joinedAt
		}
	}
	r.sweepLocked()
}

// Fresh returns the accounts still inside the grace window for roomID. The
// miss path - every message in every room with no recent join - takes a read
// lock and one map lookup.
func (r *joinRegistry) Fresh(roomID string) []string {
	if r.ttl <= 0 {
		return nil
	}
	r.mu.RLock()
	room, ok := r.rooms[roomID]
	if !ok {
		r.mu.RUnlock()
		return nil
	}
	cutoff := r.now().Add(-r.ttl)
	out := make([]string, 0, len(room))
	expired := false
	for account, at := range room {
		if at.Before(cutoff) {
			expired = true
			continue
		}
		out = append(out, account)
	}
	r.mu.RUnlock()

	if expired {
		r.mu.Lock()
		r.sweepLocked()
		r.mu.Unlock()
	}
	return out
}

// Len reports how many rooms are being tracked.
func (r *joinRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.rooms)
}

func (r *joinRegistry) sweepLocked() {
	cutoff := r.now().Add(-r.ttl)
	for roomID, room := range r.rooms {
		for account, at := range room {
			if at.Before(cutoff) {
				delete(room, account)
			}
		}
		if len(room) == 0 {
			delete(r.rooms, roomID)
		}
	}
}
