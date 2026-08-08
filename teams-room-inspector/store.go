package main

import "context"

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// RoomState is one room's materialisation state at this site. Exists reports
// whether the room document itself is present; SubscriptionCount is the live
// count of subscription documents pointing at it, and UserCount the room's
// denormalized counter (reported so drift between the two is visible).
type RoomState struct {
	Exists            bool
	UserCount         int
	SubscriptionCount int
}

// RoomStore reads room and subscription state from this site's Mongo. Read-only:
// the inspector never writes.
type RoomStore interface {
	// RoomStates returns one entry per room id that has a room document, a
	// subscription, or both. Ids with neither are absent from the map — the
	// caller reports those as a missing room.
	RoomStates(ctx context.Context, roomIDs []string) (map[string]RoomState, error)
}
