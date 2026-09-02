// Command e2epreviewrace reproduces the room-creation preview race end to end
// against the REAL broadcast-worker and message-worker binaries.
//
// Timeline modelled (the scenario as described):
//
//	t=0            A's message reaches MESSAGES-CANONICAL (gatekeeper has accepted it)
//	t=t_pub        broadcast-worker publishes new_message on chat.room.{id}.event
//	t=t_sub        B processes subscription.update and its SUB is registered  [swept]
//	t=t_sub+rpc    B's catch-up read executes                                 [swept]
//	t=t_write      message-worker's Cassandra INSERT becomes readable
//
// B loses the live event when t_sub > t_pub. The catch-up heals it only when
// t_sub+rpc > t_write. The band where BOTH fail is the real bug.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mopts "go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/subject"
)

const (
	siteID   = "site-local"
	natsURL  = "nats://127.0.0.1:4222"
	mongoURI = "mongodb://127.0.0.1:27017"
	cassHost = "127.0.0.1:9042"
)

// catchupRPC models the extra hops B's catch-up pays beyond the Cassandra query
// this harness issues directly: client -> NATS -> user-service -> NATS ->
// history-service. A LARGER value is SAFER for B, so the small end is the
// adversarial case.
var reps = flag.Int("reps", 1, "repetitions per sweep point")
var catchupRPC = flag.Duration("catchup-rpc", 5*time.Millisecond, "simulated extra RPC latency before B's catch-up read")

func main() {
	flag.Parse()
	ctx := context.Background()

	mc, err := mongo.Connect(mopts.Client().ApplyURI(mongoURI))
	must(err, "mongo connect")
	db := mc.Database("chat")

	cluster := gocql.NewCluster(cassHost)
	cluster.Keyspace, cluster.Consistency, cluster.Timeout = "chat", gocql.LocalQuorum, 10*time.Second
	cass, err := cluster.CreateSession()
	must(err, "cassandra connect")
	defer cass.Close()

	nc, err := nats.Connect(natsURL)
	must(err, "nats connect")
	// Drain error is irrelevant at exit: the measurement is already printed.
	defer func() { _ = nc.Drain() }()
	js, err := nc.JetStream()
	must(err, "jetstream")

	pollSess, err := cluster.CreateSession()
	must(err, "cassandra poller session")
	defer pollSess.Close()

	bucketer := msgbucket.New(360 * time.Hour)

	fmt.Printf("B's catch-up adds %v of simulated RPC latency before hitting Cassandra\n\n", *catchupRPC)
	fmt.Println("t_sub | t_pub  | t_write | catch-up read window | live? | found? | OUTCOME")
	fmt.Println("------+--------+---------+----------------------+-------+--------+--------------------------")

	var lost int
	delays := []time.Duration{0, 1 * time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond,
		4 * time.Millisecond, 5 * time.Millisecond, 6 * time.Millisecond, 8 * time.Millisecond,
		10 * time.Millisecond, 15 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond,
		50 * time.Millisecond, 100 * time.Millisecond}
	for _, subDelay := range delays {
		for rep := 0; rep < *reps; rep++ {
			roomID := idgen.GenerateID()
			seedRoom(ctx, db, roomID)

			msgID := idgen.GenerateMessageID()
			createdAt := time.Now().UTC()
			payload, err := json.Marshal(model.MessageEvent{
				Event: model.EventCreated,
				Message: model.Message{
					ID: msgID, RoomID: roomID,
					UserID: "u-alice-0000000000000000000", UserAccount: "alice",
					UserDisplayName: "Alice", Content: "hello from A", CreatedAt: createdAt,
				},
				SiteID: siteID, Timestamp: createdAt.UnixMilli(),
			})
			must(err, "marshal")

			// Independent watcher, subscribed up front, purely to timestamp t_pub.
			pubCh := make(chan time.Time, 4)
			w1, _ := nc.Subscribe(subject.RoomEvent(roomID, true), func(*nats.Msg) { pubCh <- time.Now() })
			w2, _ := nc.Subscribe(subject.RoomEvent(roomID, false), func(*nats.Msg) { pubCh <- time.Now() })
			must(nc.Flush(), "flush watcher")

			t0 := time.Now()
			_, err = js.Publish(subject.MsgCanonicalCreated(siteID), payload)
			must(err, "publish canonical")

			// Concurrent ground-truth poller: when does the row actually become readable?
			writeCh := make(chan time.Time, 1)
			go func() {
				writeCh <- pollUntilVisible(pollSess, roomID, bucketer.Of(createdAt), createdAt, msgID)
			}()

			// B subscribes subDelay after A's message entered the pipeline.
			bCh := make(chan time.Time, 4)
			time.Sleep(subDelay - min(subDelay, time.Since(t0)))
			bs1, _ := nc.Subscribe(subject.RoomEvent(roomID, true), func(*nats.Msg) { bCh <- time.Now() })
			bs2, _ := nc.Subscribe(subject.RoomEvent(roomID, false), func(*nats.Msg) { bCh <- time.Now() })
			must(nc.Flush(), "flush B") // the barrier: SUB is now registered server-side

			// B's catch-up read, issued right after the subscribe barrier.
			time.Sleep(*catchupRPC)
			tCatchup := time.Now()
			catchupHit := rowExists(cass, roomID, bucketer.Of(createdAt), createdAt, msgID)
			tCatchupDone := time.Now()

			// Ground truth: when did each thing actually happen?
			tPub := waitFor(pubCh, "t_pub")
			liveDelivered := false
			select {
			case <-bCh:
				liveDelivered = true
			case <-time.After(300 * time.Millisecond):
			}
			tWrite := <-writeCh

			// Per-trial teardown; an already-closed subscription is not a failure here.
			for _, sub := range []*nats.Subscription{w1, w2, bs1, bs2} {
				_ = sub.Unsubscribe()
			}

			outcome := "ok - delivered live"
			switch {
			case !liveDelivered && catchupHit:
				outcome = "ok - healed by catch-up"
			case !liveDelivered && !catchupHit:
				outcome = "*** LOST - blank preview ***"
				lost++
			case liveDelivered && !catchupHit:
				outcome = "ok - delivered live"
			}
			fmt.Printf("%5s | %6s | %7s | %8s -> %-9s | %-5s | %-6s | %s\n",
				subDelay, ms(tPub.Sub(t0)), ms(tWrite.Sub(t0)),
				ms(tCatchup.Sub(t0)), ms(tCatchupDone.Sub(t0)),
				yn(liveDelivered), yn(catchupHit), outcome)
		}
	}
	fmt.Printf("\n%d/%d trials ended with NO preview for B.\n", lost, len(delays)*(*reps))
}

func ms(d time.Duration) string { return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000) }
func yn(b bool) string {
	if b {
		return "yes"
	}
	return "NO"
}

func waitFor(ch chan time.Time, what string) time.Time {
	select {
	case t := <-ch:
		return t
	case <-time.After(20 * time.Second):
		log.Fatalf("timed out waiting for %s", what)
	}
	return time.Time{}
}

func pollUntilVisible(s *gocql.Session, roomID string, bucket int64, createdAt time.Time, msgID string) time.Time {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if rowExists(s, roomID, bucket, createdAt, msgID) {
			return time.Now()
		}
		time.Sleep(500 * time.Microsecond)
	}
	log.Fatal("message never became visible in Cassandra")
	return time.Time{}
}

func rowExists(s *gocql.Session, roomID string, bucket int64, createdAt time.Time, msgID string) bool {
	var got string
	err := s.Query(
		`SELECT message_id FROM messages_by_room WHERE room_id=? AND bucket=? AND created_at=? AND message_id=?`,
		roomID, bucket, createdAt, msgID).Consistency(gocql.LocalQuorum).Scan(&got)
	return err == nil && got == msgID
}

func seedRoom(ctx context.Context, db *mongo.Database, roomID string) {
	now := time.Now().UTC()
	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{
		"_id": roomID, "type": "channel", "name": "e2e-race-room", "siteId": siteID,
		"userCount": 3, "appCount": 0, "crossSite": false, "createdAt": now, "updatedAt": now,
	})
	must(err, "seed room")
	for _, acc := range []string{"alice", "bob", "carol"} {
		_, _ = db.Collection("users").InsertOne(ctx, bson.M{
			"_id": "u-" + acc + "-0000000000000000000", "account": acc,
			"engName": acc, "chineseName": acc, "siteId": siteID,
		})
		_, _ = db.Collection("subscriptions").InsertOne(ctx, bson.M{
			"_id": idgen.GenerateUUIDv7(), "roomId": roomID, "siteId": siteID,
			"roomType": "channel", "joinedAt": now,
			"u": bson.M{"id": "u-" + acc + "-0000000000000000000", "account": acc, "isBot": false},
		})
	}
}

func must(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
