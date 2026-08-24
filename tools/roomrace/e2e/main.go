// Command e2e drives the reported channel scenario against a real stack
// (room-service, room-worker, message-gatekeeper, broadcast-worker,
// message-worker, history-service) and reports what each client would have
// on screen.
//
//	alice creates a channel with bob  ->  alice waits for the async job result
//	->  alice sends the first message  ->  alice "jumps into" the room
//
// room-worker also emits two system messages (room_created, members_added), so
// a correct client shows three messages.
//
// The point of -client-delay is the residual race: a client cannot subscribe
// instantly, so with any delay at all the live path is lossy. What matters then
// is whether the read path recovers every message, or whether a message can fall
// between the two - delivered before the subscription was live, and not yet
// readable when history was read.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/idgen"
)

var (
	natsURL     = flag.String("nats", "nats://localhost:4222", "NATS URL")
	credsFile   = flag.String("creds", "docker-local/backend.creds", "NATS creds file")
	siteID      = flag.String("site", "site-local", "site id")
	userA       = flag.String("a", "alice", "creator account")
	userB       = flag.String("b", "bob", "invited account")
	earlySub    = flag.Bool("early-sub", false, "clients subscribe to the room subject on subscription.update rather than on room open")
	clientDelay = flag.Duration("client-delay", 0, "how long a client spends rendering before it subscribes")
	backfill    = flag.Bool("retry-history", false, "retry the history load until it contains the sent message")
	hint        = flag.Bool("hint", false, "pass meta.lastMsgAt=now on msg.history so the scan ceiling is not the stale room doc")
	settle      = flag.Duration("settle", time.Second, "how long to keep listening after the send")
	iterations  = flag.Int("iterations", 1, "how many rooms to run")
	probeRoom   = flag.String("probe-room", "", "just load history for this room id and exit")
	writeLag    = flag.Bool("write-lag", false, "after the send, poll hinted history every 5ms to measure when the message becomes readable")
	verbose     = flag.Bool("v", false, "print the per-iteration timeline")
	historyAt   = flag.String("history-at", "join", "when the client reads history: join (on subscription.update) or open (after the send is accepted, i.e. when the UI enters the room)")
)

type ev struct {
	at     time.Duration
	who    string
	source string
	kind   string
	msgID  string
	detail string
}

type recorder struct {
	mu  sync.Mutex
	t0  time.Time
	evs []ev
}

func (r *recorder) add(who, source, kind, msgID, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, ev{at: time.Since(r.t0), who: who, source: source, kind: kind, msgID: msgID, detail: detail})
}

func (r *recorder) snapshot() []ev {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]ev(nil), r.evs...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].at < out[j].at })
	return out
}

// seen is one user's outcome for one room.
type seen struct {
	live      map[string]bool
	history   map[string]bool
	writeLag  time.Duration
	lagProbes int
}

func (s seen) union() map[string]bool {
	out := map[string]bool{}
	for k := range s.live {
		out[k] = true
	}
	for k := range s.history {
		out[k] = true
	}
	return out
}

type tally struct {
	rooms        int
	liveMissing  int
	histMissing  int
	doubleMissed int
	expected     int
	lagSum       time.Duration
	lagMax       time.Duration
	lagSamples   int
}

func main() {
	flag.Parse()
	if err := runAll(); err != nil {
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

func runAll() error {
	if *probeRoom != "" {
		return probe()
	}

	aTally, bTally := &tally{}, &tally{}
	for i := range *iterations {
		if err := runOne(i, aTally, bTally); err != nil {
			return err
		}
	}
	report(*userA, aTally)
	report(*userB, bTally)
	return nil
}

func probe() error {
	nc, err := connect("e2e-probe")
	if err != nil {
		return err
	}
	defer nc.Close()
	c := newClient(*userA, nc, &recorder{t0: time.Now()})
	msgs, _ := c.loadHistory(*probeRoom, false, "")
	fmt.Printf("probe room %s: msg.history returned %d %v\n", *probeRoom, len(msgs), summarize(msgs))
	return nil
}

func runOne(i int, aTally, bTally *tally) error {
	rec := &recorder{t0: time.Now()}

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
	a.renderDelay = *clientDelay
	b.renderDelay = *clientDelay
	a.subscribeOnUpdate = *earlySub
	b.subscribeOnUpdate = true
	a.readHistoryOnJoin = *historyAt == "join"
	b.readHistoryOnJoin = *historyAt == "join"
	if err := a.login(); err != nil {
		return err
	}
	if err := b.login(); err != nil {
		return err
	}

	roomID, err := a.createChannel(fmt.Sprintf("e2e-%d-%d", time.Now().UnixMilli(), i), *userB)
	if err != nil {
		return fmt.Errorf("create channel: %w", err)
	}

	rec.add(*userA, "rpc", "create-room async job result ok", "", roomID)

	msgID := idgen.GenerateMessageID()
	if err := a.send(roomID, msgID, "first message"); err != nil {
		return fmt.Errorf("send first message: %w", err)
	}
	rec.add(*userA, "rpc", "msg.send accepted by gatekeeper", msgID, "")

	var lag time.Duration
	var probes int
	if *writeLag {
		lag, probes = a.measureWriteLag(roomID, msgID)
	}

	a.openRoom(roomID)
	b.waitRoomOpen(roomID, 3*time.Second)

	aHist, _ := a.loadHistory(roomID, *backfill, msgID)
	rec.add(*userA, "history", fmt.Sprintf("room-open msg.history returned %d", len(aHist)), "", "")
	bHist, _ := b.loadHistory(roomID, *backfill, msgID)
	rec.add(*userB, "history", fmt.Sprintf("room-open msg.history returned %d", len(bHist)), "", "")
	// When the client joined via subscription.update it already read history
	// there; that earlier read is the one whose freshness is under test.
	if jh, ok := a.joinHistory(roomID); ok {
		aHist = jh
	}
	if jh, ok := b.joinHistory(roomID); ok {
		bHist = jh
	}

	time.Sleep(*settle)

	// Authority for "what should be on screen": every message the room actually holds.
	final, _ := a.loadHistory(roomID, false, "")
	expected := map[string]bool{}
	for _, m := range final {
		expected[m.MessageID] = true
	}
	expected[msgID] = true

	aSeen := seen{live: a.liveSet(), history: idSet(aHist), writeLag: lag, lagProbes: probes}
	bSeen := seen{live: b.liveSet(), history: idSet(bHist)}
	accumulate(aTally, expected, aSeen)
	accumulate(bTally, expected, bSeen)

	if *verbose {
		printTimeline(rec.snapshot())
	}
	return nil
}

func accumulate(t *tally, expected map[string]bool, s seen) {
	t.rooms++
	t.expected += len(expected)
	union := s.union()
	for id := range expected {
		if !s.live[id] {
			t.liveMissing++
		}
		if !s.history[id] {
			t.histMissing++
		}
		if !union[id] {
			t.doubleMissed++
		}
	}
	if s.lagProbes > 0 {
		t.lagSamples++
		t.lagSum += s.writeLag
		if s.writeLag > t.lagMax {
			t.lagMax = s.writeLag
		}
	}
}

func report(who string, t *tally) {
	fmt.Printf("\n=== %s over %d room(s), %d expected message(s) ===\n", who, t.rooms, t.expected)
	fmt.Printf("  missing from the live path      : %d (%.0f%%)\n", t.liveMissing, pct(t.liveMissing, t.expected))
	fmt.Printf("  missing from the history read   : %d (%.0f%%)\n", t.histMissing, pct(t.histMissing, t.expected))
	fmt.Printf("  MISSING FROM BOTH (lost)        : %d (%.0f%%)\n", t.doubleMissed, pct(t.doubleMissed, t.expected))
	if t.lagSamples > 0 {
		fmt.Printf("  send -> readable in history     : avg %v, max %v over %d sample(s)\n",
			(t.lagSum / time.Duration(t.lagSamples)).Round(time.Millisecond), t.lagMax.Round(time.Millisecond), t.lagSamples)
	}
}

func pct(n, d int) float64 {
	if d == 0 {
		return 0
	}
	return 100 * float64(n) / float64(d)
}

func idSet(ms []histMsg) map[string]bool {
	out := map[string]bool{}
	for _, m := range ms {
		out[m.MessageID] = true
	}
	return out
}

func printTimeline(evs []ev) {
	fmt.Printf("\n=== timeline ===\n")
	for _, e := range evs {
		detail := e.detail
		if e.msgID != "" {
			detail = e.msgID + " " + e.detail
		}
		fmt.Printf("  %7.0fms  %-6s %-14s %s\n", float64(e.at.Microseconds())/1000, e.who, e.source, trimTo(e.kind+" "+detail, 90))
	}
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

var _ = strings.TrimSpace
