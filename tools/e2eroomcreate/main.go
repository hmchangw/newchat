// Command e2eroomcreate replays the room-creation preview race end to end and
// prints a per-action timeline.
//
// Scenario (as specified):
//  1. Alice calls the create-room RPC naming Bob.
//  2. Alice's client waits for subscription.update.
//  3. Alice and Bob both receive subscription.update and subscribe to the room subject.
//  4. Alice sends "new message" right after her subscribe succeeds.
//  5. broadcast-worker fans out new_message on the room subject.
//  6. Bob receives it only if his SUB was registered before the publish.
//  7. Alice always receives it — she sends only after her own subscribe succeeded.
//
// --bob-delay models how long Bob's client spends between handling
// subscription.update and having its SUB registered (render, state update, etc).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mopts "go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

const (
	siteID   = "site-local"
	natsURL  = "nats://127.0.0.1:4222"
	mongoURI = "mongodb://127.0.0.1:27017"
)

var bobDelay = flag.Duration("bob-delay", 0, "Bob's client processing time between subscription.update and SUB registered")

// got is one room event a client actually received.
type got struct {
	typ, msgID, content string
	at                  time.Time
}

// ev is one row of the timeline.
type ev struct {
	at    time.Time
	actor string
	what  string
}

type timeline struct {
	mu   sync.Mutex
	t0   time.Time
	rows []ev
}

func (t *timeline) add(actor, what string) { t.addAt(time.Now(), actor, what) }

func (t *timeline) addAt(at time.Time, actor, what string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rows = append(t.rows, ev{at, actor, what})
}

func (t *timeline) print() {
	t.mu.Lock()
	defer t.mu.Unlock()
	sort.SliceStable(t.rows, func(i, j int) bool { return t.rows[i].at.Before(t.rows[j].at) })
	fmt.Printf("\n%-10s | %-16s | %s\n", "T+", "ACTOR", "ACTION")
	fmt.Printf("%s\n", "-----------+------------------+--------------------------------------------------------")
	for _, r := range t.rows {
		fmt.Printf("%9.2fms | %-16s | %s\n", float64(r.at.Sub(t.t0).Microseconds())/1000, r.actor, r.what)
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()
	tl := &timeline{}

	mc, err := mongo.Connect(mopts.Client().ApplyURI(mongoURI))
	must(err, "mongo connect")
	seedUsers(ctx, mc.Database("chat"))

	// Two independent NATS connections: one per client, as in production.
	alice, err := nats.Connect(natsURL, nats.Name("alice-client"))
	must(err, "alice connect")
	defer func() { _ = alice.Drain() }()
	bob, err := nats.Connect(natsURL, nats.Name("bob-client"))
	must(err, "bob connect")
	defer func() { _ = bob.Drain() }()

	// Both clients hold their per-user subjects for the whole session (as after login).
	aliceSubUpd := make(chan []byte, 8)
	bobSubUpd := make(chan []byte, 8)
	s1, err := alice.Subscribe(subject.SubscriptionUpdate("alice"), func(m *nats.Msg) { aliceSubUpd <- m.Data })
	must(err, "alice sub.update")
	s2, err := bob.Subscribe(subject.SubscriptionUpdate("bob"), func(m *nats.Msg) { bobSubUpd <- m.Data })
	must(err, "bob sub.update")
	defer func() { _ = s1.Unsubscribe(); _ = s2.Unsubscribe() }()
	must(alice.Flush(), "flush alice")
	must(bob.Flush(), "flush bob")

	// Room events each client actually receives, recorded with arrival time.
	var mu sync.Mutex
	received := map[string][]got{}
	seen := map[string]bool{}
	watch := func(nc *nats.Conn, who, roomID string) []*nats.Subscription {
		var subs []*nats.Subscription
		for _, subj := range []string{subject.RoomEvent(roomID, true), subject.RoomEvent(roomID, false)} {
			s, err := nc.Subscribe(subj, func(m *nats.Msg) {
				var e model.RoomEvent
				if json.Unmarshal(m.Data, &e) != nil {
					return
				}
				content, msgID := "", ""
				if e.Message != nil {
					content, msgID = e.Message.Content, e.Message.ID
					if e.Message.Type != "" {
						content = "<sys:" + e.Message.Type + ">"
					}
				}
				mu.Lock()
				if !seen[who+"|"+msgID] { // ROOM_SUBJECT_MODE=dual delivers on both namespaces
					seen[who+"|"+msgID] = true
					received[who] = append(received[who], got{string(e.Type), msgID, content, time.Now()})
				}
				mu.Unlock()
			})
			must(err, who+" room sub")
			subs = append(subs, s)
		}
		return subs
	}

	// ---- Step 1: Alice calls create room ----
	tl.t0 = time.Now()
	reqID := uuid.NewString()
	body, _ := json.Marshal(map[string]any{"name": "alice-bob-room", "users": []string{"bob"}})
	req := nats.NewMsg(fmt.Sprintf("chat.user.alice.request.room.%s.create", siteID))
	req.Data = body
	req.Header = nats.Header{"X-Request-ID": []string{reqID}}
	tl.add("alice-client", "publish room.create RPC (users=[bob])")
	reply, err := alice.RequestMsg(req, 15*time.Second)
	must(err, "create room rpc")
	var createResp struct {
		Status, RoomID, RoomType string
	}
	must(json.Unmarshal(reply.Data, &createResp), "decode create reply")
	if createResp.RoomID == "" {
		// Not os.Exit: the deferred Drains must run so the clients unwind cleanly.
		panic(fmt.Sprintf("create room rejected: %s", string(reply.Data)))
	}
	roomID := createResp.RoomID
	tl.add("room-service", fmt.Sprintf("sync reply status=%s roomId=%s type=%s",
		createResp.Status, roomID, createResp.RoomType))

	// ---- Steps 2-3: both clients await subscription.update, then subscribe ----
	var wg sync.WaitGroup
	wg.Add(1)
	// Bob runs concurrently, exactly as a real second client would.
	go func() {
		defer wg.Done()
		select {
		case <-bobSubUpd:
			tl.add("bob-client", "RECEIVED subscription.update (added)")
		case <-time.After(20 * time.Second):
			tl.add("bob-client", "TIMEOUT waiting for subscription.update")
			return
		}
		if *bobDelay > 0 {
			time.Sleep(*bobDelay) // client-side processing before subscribing
			tl.add("bob-client", fmt.Sprintf("client processing done (%s)", *bobDelay))
		}
		watch(bob, "bob", roomID)
		must(bob.Flush(), "bob flush room sub") // barrier: SUB now registered
		tl.add("bob-client", "SUBSCRIBED to room subject (flush confirmed)")
	}()

	select {
	case <-aliceSubUpd:
		tl.add("alice-client", "RECEIVED subscription.update (added)")
	case <-time.After(20 * time.Second):
		must(fmt.Errorf("timeout"), "alice subscription.update")
	}
	watch(alice, "alice", roomID)
	must(alice.Flush(), "alice flush room sub")
	tl.add("alice-client", "SUBSCRIBED to room subject (flush confirmed)")

	// ---- Step 4: Alice sends, immediately after her own subscribe succeeded ----
	msgID := idgen.GenerateMessageID()
	sendReq, _ := json.Marshal(model.SendMessageRequest{
		ID: msgID, Content: "new message", RequestID: uuid.NewString(),
	})
	must(alice.Publish(subject.MsgSend("alice", roomID, siteID), sendReq), "msg.send")
	must(alice.Flush(), "flush send")
	tl.add("alice-client", fmt.Sprintf("PUBLISHED msg.send %q id=%s", "new message", msgID))

	wg.Wait()

	// Let the pipeline settle so late arrivals are counted honestly.
	time.Sleep(3 * time.Second)

	mu.Lock()
	for _, who := range []string{"alice", "bob"} {
		for _, g := range received[who] {
			label := g.content
			if label == "" {
				label = "(no body)"
			}
			tl.addAt(g.at, who+"-client", fmt.Sprintf("<- room event %s %s", g.typ, label))
		}
	}
	aliceGot, bobGot := hasMsg(received["alice"], msgID), hasMsg(received["bob"], msgID)
	mu.Unlock()

	tl.print()

	fmt.Printf("\nRESULT (bob-delay=%s)\n", *bobDelay)
	fmt.Printf("  Alice received her own \"new message\": %s\n", yn(aliceGot))
	fmt.Printf("  Bob   received \"new message\":         %s\n", yn(bobGot))
	if !bobGot {
		fmt.Println("  => Bob's sidebar shows the room with NO preview message.")
	}
}

func hasMsg(gs []got, id string) bool {
	for _, g := range gs {
		if g.msgID == id {
			return true
		}
	}
	return false
}

func yn(b bool) string {
	if b {
		return "YES"
	}
	return "NO  <-- LOST"
}

func seedUsers(ctx context.Context, db *mongo.Database) {
	for _, acc := range []string{"alice", "bob"} {
		id := idgen.GenerateUUIDv7()
		_, _ = db.Collection("users").UpdateOne(ctx,
			bson.M{"account": acc},
			bson.M{"$setOnInsert": bson.M{
				"_id": id, "account": acc, "engName": acc, "chineseName": acc,
				"siteId": siteID, "isBot": false,
			}},
			mopts.UpdateOne().SetUpsert(true))
	}
}

func must(err error, what string) {
	if err != nil {
		fmt.Printf("FATAL %s: %v\n", what, err)
		os.Exit(1)
	}
}
