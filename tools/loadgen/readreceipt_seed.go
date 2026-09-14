package main

import (
	"context"
	"fmt"
	"math"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
)

// selectReaders returns ceil(readRatio * len(in)) subscriptions chosen
// uniformly at random via rng. Deterministic for a fixed rng seed. A readRatio
// of 1.0 selects all; an empty input returns nil.
func selectReaders(in []model.Subscription, readRatio float64, rng *rand.Rand) []model.Subscription {
	if len(in) == 0 {
		return nil
	}
	k := int(math.Ceil(readRatio * float64(len(in))))
	if k > len(in) {
		k = len(in)
	}
	if k <= 0 {
		return nil
	}
	perm := rng.Perm(len(in))[:k]
	out := make([]model.Subscription, k)
	for i, idx := range perm {
		out[i] = in[idx]
	}
	return out
}

// latestTopLevelByRoom returns the newest top-level message CreatedAt per room.
// Thread replies (ThreadParentID != "") are ignored — read receipts target
// top-level messages, so the read floor only needs to cover those.
func latestTopLevelByRoom(plan *MessagePlan) map[string]time.Time {
	out := map[string]time.Time{}
	for i := range plan.Messages {
		m := &plan.Messages[i]
		if m.ThreadParentID != "" {
			continue
		}
		if t, ok := out[m.RoomID]; !ok || m.CreatedAt.After(t) {
			out[m.RoomID] = m.CreatedAt
		}
	}
	return out
}

// SeedReadReceiptState stamps lastSeenAt on a readRatio fraction of each room's
// subscribers so ListReadReceipts ($match lastSeenAt >= message.createdAt)
// matches real documents and exercises the $lookup/$unwind path. lastSeenAt is
// set to the room's latest top-level message CreatedAt + 1ms so it covers every
// targetable message in the room. Selection is deterministic on `seed`.
func SeedReadReceiptState(ctx context.Context, db *mongo.Database, subs []model.Subscription, plan *MessagePlan, readRatio float64, seed int64) error {
	latest := latestTopLevelByRoom(plan)

	// Group subscriptions by room.
	byRoom := map[string][]model.Subscription{}
	for i := range subs {
		byRoom[subs[i].RoomID] = append(byRoom[subs[i].RoomID], subs[i])
	}

	rng := rand.New(rand.NewSource(seed))
	coll := db.Collection("subscriptions")
	for roomID, roomSubs := range byRoom {
		floor, ok := latest[roomID]
		if !ok {
			continue // room has no top-level messages — nothing to read
		}
		lastSeen := floor.Add(time.Millisecond).UTC()
		chosen := selectReaders(roomSubs, readRatio, rng)
		if len(chosen) == 0 {
			continue
		}
		ids := make([]string, len(chosen))
		for i := range chosen {
			ids[i] = chosen[i].ID
		}
		if _, err := coll.UpdateMany(ctx,
			bson.M{"_id": bson.M{"$in": ids}},
			bson.M{"$set": bson.M{"lastSeenAt": lastSeen}},
		); err != nil {
			return fmt.Errorf("stamp lastSeenAt for room %q: %w", roomID, err)
		}
	}
	return nil
}
