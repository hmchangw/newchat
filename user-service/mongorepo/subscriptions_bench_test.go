//go:build integration

package mongorepo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/testutil"
)

// seedSubscriptionsForBench inserts n rooms + n matching subscriptions for account,
// spreading lastMsgAt across time (newest first) so pagination/sort is exercised.
// The room's lastMsgAt and the subscription's own (denormalized) lastMsgAt are set to
// the SAME value, and the subscription's "name" mirrors the room's "name" — required
// for AggregateSubscriptions and AggregateSubscriptionsFast to sort/return identical
// rows (the whole benchmark is apples-to-apples only under this invariant).
func seedSubscriptionsForBench(b *testing.B, db *mongo.Database, account string, n int) {
	b.Helper()
	ctx := context.Background()
	base := time.Now().UTC()

	rooms := make([]any, 0, n)
	subs := make([]any, 0, n)
	for i := 0; i < n; i++ {
		roomID := fmt.Sprintf("bench-room-%s-%d", account, i)
		name := fmt.Sprintf("Room-%s-%05d", account, i)
		// Spread activity so every row gets a distinct sort key; none are ^Del-.
		lastMsgAt := base.Add(-time.Duration(i) * time.Minute)

		rooms = append(rooms, bson.M{
			"_id": roomID, "name": name, "siteId": "site-a",
			"userCount": 2, "appCount": 0,
			"lastMsgId": fmt.Sprintf("m-%d", i), "lastMsgAt": lastMsgAt,
			"createdAt": lastMsgAt,
		})

		roomType := "channel"
		if i%2 == 1 {
			roomType = "dm"
		}
		subs = append(subs, bson.M{
			"_id": fmt.Sprintf("bench-sub-%s-%d", account, i),
			"u":   bson.M{"_id": "u-" + account, "account": account},
			"roomId": roomID, "name": name, "roomType": roomType, "siteId": "site-a",
			"open": true, "createdAt": lastMsgAt,
			// Denormalized copy the FAST path sorts/paginates on — kept equal to the
			// room's own lastMsgAt per the seed invariant above.
			"lastMsgAt": lastMsgAt,
		})
	}
	if _, err := db.Collection("rooms").InsertMany(ctx, rooms); err != nil {
		b.Fatalf("seed rooms: %v", err)
	}
	if _, err := db.Collection("subscriptions").InsertMany(ctx, subs); err != nil {
		b.Fatalf("seed subscriptions: %v", err)
	}
}

// BenchmarkAggregateSubscriptions compares the CURRENT (room-joined sort) vs FAST
// (subscription's own denormalized lastMsgAt sort) read paths at increasing
// subs-per-account counts. Each N seeds its own account (avoids cross-N pollution)
// once, outside the timed loop.
func BenchmarkAggregateSubscriptions(b *testing.B) {
	db := testutil.MongoDB(b, "user-service-bench")
	r := NewSubscriptionRepo(db, "site-a")
	if err := r.EnsureIndexes(context.Background()); err != nil {
		b.Fatalf("ensure indexes: %v", err)
	}
	ctx := context.Background()
	page := mongoutil.OffsetPageRequest{Offset: 0, Limit: 20}

	for _, n := range []int{10, 100, 1000, 5000} {
		account := fmt.Sprintf("acct-%d", n)
		seedSubscriptionsForBench(b, db, account, n)

		b.Run(fmt.Sprintf("current/%d", n), func(b *testing.B) {
			var rowCount int
			for i := 0; i < b.N; i++ {
				out, err := r.AggregateSubscriptions(ctx, account, "rooms", false, nil, page)
				if err != nil {
					b.Fatalf("AggregateSubscriptions: %v", err)
				}
				rowCount = len(out.Data)
			}
			if rowCount == 0 {
				b.Fatalf("current/%d returned 0 rows — seed or query broken", n)
			}
		})

		var fastRows, currentRows int
		b.Run(fmt.Sprintf("fast/%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				out, err := r.AggregateSubscriptionsFast(ctx, account, "rooms", false, nil, page)
				if err != nil {
					b.Fatalf("AggregateSubscriptionsFast: %v", err)
				}
				fastRows = len(out.Data)
			}
		})

		// Apples-to-apples guard: both variants must return the same page shape for
		// the same seed. (Row IDENTITY/order is asserted once, outside the timing
		// loop, below — this just fails loudly if row COUNT ever drifts.)
		currentOut, err := r.AggregateSubscriptions(ctx, account, "rooms", false, nil, page)
		if err != nil {
			b.Fatalf("AggregateSubscriptions recheck: %v", err)
		}
		currentRows = len(currentOut.Data)
		fastOut, err := r.AggregateSubscriptionsFast(ctx, account, "rooms", false, nil, page)
		if err != nil {
			b.Fatalf("AggregateSubscriptionsFast recheck: %v", err)
		}
		if currentRows != len(fastOut.Data) {
			b.Fatalf("N=%d: row count mismatch current=%d fast=%d", n, currentRows, len(fastOut.Data))
		}
		if fastRows != currentRows {
			b.Fatalf("N=%d: timed-loop row count drifted from recheck current=%d fast=%d", n, currentRows, fastRows)
		}
		for i := range currentOut.Data {
			if currentOut.Data[i].ID != fastOut.Data[i].ID {
				b.Fatalf("N=%d: row order/identity mismatch at index %d: current=%s fast=%s", n, i, currentOut.Data[i].ID, fastOut.Data[i].ID)
			}
		}
	}
}
