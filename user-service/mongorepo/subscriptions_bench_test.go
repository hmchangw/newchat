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

// Benchmarks for the subscription.list read path, against a real Mongo started
// by pkg/testutil (Docker required, same as the integration tests).
//
//	go test -tags integration ./user-service/mongorepo/ \
//	    -run '^$' -bench BenchmarkSubscriptionList -benchtime 50x
//
// To compare against the old $lookup pipeline, copy this file onto main and
// change the one line marked BRANCH-SPECIFIC below, then benchstat the two
// runs. BenchmarkSubscriptionList carries the same name and sub-benchmark
// names on both branches so benchstat can pair them.
const benchAccount = "bench-user"

// newBenchRepo builds the repo under test over an isolated database.
func newBenchRepo(b *testing.B) (*SubscriptionRepo, *mongo.Database) {
	b.Helper()
	db := testutil.MongoDB(b, "user-service-bench")
	// BRANCH-SPECIFIC. On main this line reads:
	//     r := NewSubscriptionRepo(db, "site-a")
	r := NewSubscriptionRepo(db, 100_000, 15*time.Second)
	if err := r.EnsureIndexes(context.Background()); err != nil {
		b.Fatalf("ensure indexes: %v", err)
	}
	return r, db
}

// benchSeed inserts n rooms and n subscriptions for benchAccount. Idempotent:
// the testing package re-invokes a b.Run body while it grows b.N, so seeding
// has to be safe to call repeatedly against the same database.
func benchSeed(b *testing.B, db *mongo.Database, n int) {
	b.Helper()
	ctx := context.Background()
	have, err := db.Collection("subscriptions").CountDocuments(ctx, bson.M{"u.account": benchAccount})
	if err != nil {
		b.Fatalf("count seeded subscriptions: %v", err)
	}
	if have == int64(n) {
		return
	}
	t0 := time.Now().UTC()
	rooms := make([]any, 0, n)
	subs := make([]any, 0, n)
	for i := range n {
		roomID := fmt.Sprintf("bench-room-%06d", i)
		// Spread lastMsgAt so the sort has real work to do and ordering is stable.
		rooms = append(rooms, bson.M{
			"_id": roomID, "name": fmt.Sprintf("room-%06d", i), "siteId": "site-a",
			"userCount": 12, "appCount": 1,
			"lastMsgAt": t0.Add(-time.Duration(i) * time.Minute),
			"createdAt": t0.Add(-time.Duration(i) * time.Hour),
			"lastMsgId": "bench-msg-0000000001",
		})
		subs = append(subs, bson.M{
			"_id": fmt.Sprintf("bench-sub-%06d", i),
			"u":   bson.M{"_id": "bench-user-id", "account": benchAccount},
			"roomId": roomID, "siteId": "site-a", "roomType": "channel",
			"name": fmt.Sprintf("room-%06d", i), "open": true,
			"joinedAt": t0, "roles": bson.A{"member"},
		})
	}
	if _, err := db.Collection("rooms").InsertMany(ctx, rooms); err != nil {
		b.Fatalf("seed rooms: %v", err)
	}
	if _, err := db.Collection("subscriptions").InsertMany(ctx, subs); err != nil {
		b.Fatalf("seed subscriptions: %v", err)
	}
}

// runListPage times b.N first-page reads. The sort-key cache (when enabled)
// warms on the first iteration, so this measures the steady state; with a low
// -benchtime the cold first read is a visible share of the average.
func runListPage(b *testing.B, r *SubscriptionRepo) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		page, err := r.AggregateSubscriptions(ctx, benchAccount, "rooms", false, nil,
			mongoutil.OffsetPageRequest{Offset: 0, Limit: 20})
		if err != nil {
			b.Fatalf("aggregate subscriptions: %v", err)
		}
		if len(page.Data) == 0 {
			b.Fatal("empty page — seeding did not take")
		}
	}
}

var benchSizes = []int{10, 50, 100, 500, 1000}

// BenchmarkSubscriptionList measures one first-page read as the account's
// subscription count grows. Runnable on main unchanged (see newBenchRepo), so
// the two branches can be compared directly.
func BenchmarkSubscriptionList(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("subs=%d", n), func(b *testing.B) {
			r, db := newBenchRepo(b)
			benchSeed(b, db, n)
			runListPage(b, r)
		})
	}
}

// BenchmarkSubscriptionListNoCache is the same read with the sort-key cache
// disabled, so every request resolves sort keys from a fresh batched $in.
// The gap against BenchmarkSubscriptionList is the cache's contribution; the
// gap against main is the phased read path's. This branch only — main has no
// cache to disable.
func BenchmarkSubscriptionListNoCache(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("subs=%d", n), func(b *testing.B) {
			db := testutil.MongoDB(b, "user-service-bench")
			r := NewSubscriptionRepo(db, 0, 0)
			if err := r.EnsureIndexes(context.Background()); err != nil {
				b.Fatalf("ensure indexes: %v", err)
			}
			benchSeed(b, db, n)
			runListPage(b, r)
		})
	}
}
