// Command e2e drives the reported channel scenario against a real stack
// (room-service, room-worker, message-gatekeeper, broadcast-worker,
// message-worker, history-service) and reports, with timestamps, what each
// client would have on screen.
//
//	A creates a channel with B  ->  A waits for the async job result
//	->  A sends the first message  ->  A "jumps into" the room
//	->  what do A and B actually see, live and from history?
//
// Two client behaviours are run for A: subscribing to the room subject when it
// opens the room (today), and subscribing as soon as its own subscription.update
// arrives (the fix). B always subscribes on subscription.update, as the frontend does.
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

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

var (
	natsURL   = flag.String("nats", "nats://localhost:4222", "NATS URL")
	credsFile = flag.String("creds", "docker-local/backend.creds", "NATS creds file")
	siteID    = flag.String("site", "site-local", "site id")
	userA     = flag.String("a", "alice", "creator account")
	userB     = flag.String("b", "bob", "invited account")
	earlySub  = flag.Bool("early-sub", false, "A subscribes to the room subject on its own subscription.update instead of on room open")
	backfill  = flag.Bool("retry-history", false, "retry the history load until it contains what the room's lastMsgId promises")
	settle    = flag.Duration("settle", 3*time.Second, "how long to keep listening after the send")
	hint      = flag.Bool("hint", false, "pass meta.lastMsgAt=now on msg.history so the scan ceiling is not the stale room doc")
	probeRoom = flag.String("probe-room", "", "just load history for this room id and exit")
)

// ev is one observed event, with the offset from t0 at which it landed.
type ev struct {
	at      time.Duration
	who     string
	source  string
	kind    string
	msgID   string
	content string
}

type recorder struct {
	mu  sync.Mutex
	t0  time.Time
	evs []ev
}

func (r *recorder) add(who, source, kind, msgID, content string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, ev{at: time.Since(r.t0), who: who, source: source, kind: kind, msgID: msgID, content: content})
}

func (r *recorder) snapshot() []ev {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]ev(nil), r.evs...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].at < out[j].at })
	return out
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: %v\n", err)
		os.Exit(1)
	}
}

func connect(name string) (*nats.Conn, error) {
	nc, err := nats.Connect(*natsURL, nats.Name(name), nats.UserCredentials(*credsFile))
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", name, err)
	}
	return nc, nil
}

func run() error {
	rec := &recorder{t0: time.Now()}

	if *probeRoom != "" {
		nc, err := connect("e2e-probe")
		if err != nil {
			return err
		}
		defer nc.Close()
		c := newClient(*userA, nc, rec)
		msgs, _ := c.loadHistory(*probeRoom, false, "")
		fmt.Printf("probe room %s: msg.history returned %d %v\n", *probeRoom, len(msgs), summarize(msgs))
		return nil
	}

	aConn, err := connect("e2e-" + *userA)
	if err != nil {
		return err
	}
	defer aConn.Close()
	bConn, err := connect("e2e-" + *userB)
	if err != nil {
		return err
	}
	defer bConn.Close()

	a := newClient(*userA, aConn, rec)
	b := newClient(*userB, bConn, rec)
	if err := a.login(); err != nil {
		return err
	}
	if err := b.login(); err != nil {
		return err
	}

	if *earlySub {
		a.subscribeOnUpdate = true
	}
	b.subscribeOnUpdate = true

	roomName := fmt.Sprintf("e2e-%d", time.Now().UnixMilli())
	roomID, err := a.createChannel(roomName, *userB)
	if err != nil {
		return fmt.Errorf("create channel: %w", err)
	}
	rec.add(*userA, "rpc", "create-room async result ok", "", roomID)

	msgID := idgen.GenerateMessageID()
	if err := a.send(roomID, msgID, "first message"); err != nil {
		return fmt.Errorf("send first message: %w", err)
	}
	rec.add(*userA, "rpc", "msg.send reply ok", msgID, "")

	// "A's UI jumps into the room": subscribe (if it hasn't already) and load history.
	a.openRoom(roomID)
	b.waitRoomOpen(roomID, 2*time.Second)

	aHist, aTries := a.loadHistory(roomID, *backfill, msgID)
	rec.add(*userA, "history", fmt.Sprintf("msg.history returned %d (tries %d)", len(aHist), aTries), "", "")
	bHist, bTries := b.loadHistory(roomID, *backfill, msgID)
	rec.add(*userB, "history", fmt.Sprintf("msg.history returned %d (tries %d)", len(bHist), bTries), "", "")

	time.Sleep(*settle)

	finalA, _ := a.loadHistory(roomID, false, "")
	finalB, _ := b.loadHistory(roomID, false, "")

	printTimeline(rec.snapshot())
	printScreen(*userA, a, aHist, finalA)
	printScreen(*userB, b, bHist, finalB)
	return nil
}

func printTimeline(evs []ev) {
	fmt.Printf("\n=== timeline ===\n")
	for _, e := range evs {
		detail := e.content
		if e.msgID != "" {
			detail = fmt.Sprintf("%s %s", e.msgID, e.content)
		}
		fmt.Printf("  %7.0fms  %-6s %-14s %s\n", float64(e.at.Microseconds())/1000, e.who, e.source, trimTo(e.kind+" "+detail, 90))
	}
}

func printScreen(who string, c *client, hist []histMsg, final []histMsg) {
	c.mu.Lock()
	live := append([]string(nil), c.liveMessages...)
	c.mu.Unlock()

	fmt.Printf("\n=== what %s has on screen ===\n", who)
	fmt.Printf("  live room events received : %d %v\n", len(live), live)
	fmt.Printf("  history at room-open      : %d %v\n", len(hist), summarize(hist))
	fmt.Printf("  history after settle      : %d %v\n", len(final), summarize(final))
	visible := map[string]bool{}
	for _, m := range live {
		visible[m] = true
	}
	for _, m := range hist {
		visible[m.MessageID] = true
	}
	fmt.Printf("  VISIBLE when the room opens: %d message(s)\n", len(visible))
}

func summarize(ms []histMsg) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		label := m.Msg
		if m.Type != "" {
			label = "[" + m.Type + "]"
		}
		out = append(out, label)
	}
	return out
}

func trimTo(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "~"
}

var _ = context.Background
var _ = model.CreateRoomRequest{}
var _ = subject.RoomEvent
var _ = json.Marshal
