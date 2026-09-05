package presence

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/read"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

const (
	SignalHello        = "hello"
	SignalPing         = "ping"
	SignalActivityAway = "activity_away"
	SignalActivityBack = "activity_back"
	SignalBye          = "bye"
)

const (
	CheckMatch    = "match"
	CheckMismatch = "mismatch"
	CheckSkipped  = "skipped"
	CheckUnknown  = "unknown"
)

// Publisher sends a presence signal. The signals are fire-and-forget
// core NATS publishes, so this returns an error only for local failures.
type Publisher interface {
	Publish(subject string, data []byte) error
}

// Observer keeps the lane independent from the root Prometheus collector.
type Observer interface {
	CountSignal(signal string)
	CountCheck(result string, count int)
	SetConnections(status model.PresenceStatus, count int)
}

type Config struct {
	SiteID string
	// Connections bounds how many virtual clients the lane maintains.
	Connections int
	// QueryShare is the fraction of lane slots spent verifying instead of
	// signalling.
	QueryShare float64
	// Settle is how long after a signal the reported state is allowed to
	// disagree before a mismatch is counted.
	Settle time.Duration
	// TTL mirrors the presence service's connection TTL. Past it the server is
	// entitled to drop a connection it stopped hearing from, so a stale
	// expectation must not be scored as a mismatch.
	TTL            time.Duration
	QueryBatchSize int
	RequestTimeout time.Duration
}

type connection struct {
	account  string
	connID   string
	expected model.PresenceStatus
	lastSent time.Time
	lastPing time.Time
	inFlight bool
}

// Lane drives presence traffic and samples whether the reported
// state agrees with what it last sent.
//
// It deliberately keeps no evidence ledger. Presence signals are unacknowledged
// publishes — during a NATS outage the client library buffers them and returns
// success — so a successful send proves nothing, and the state is TTL-bound and
// allowed to expire on its own. The only honest evidence is a later query, and
// only for a connection that was refreshed recently enough that the server
// still owes an answer.
type Lane struct {
	cfg      Config
	pool     Publisher
	rpc      *rpc.Client
	observer Observer
	recorder read.SampleRecorder
	now      func() time.Time

	mu          sync.Mutex
	rng         *rand.Rand
	connections []*connection
	cursor      int
	queryCredit float64
}

func New(
	cfg Config,
	topology *topology.Topology,
	pool Publisher,
	rpc *rpc.Client,
	observer Observer,
	recorder read.SampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) (*Lane, error) {
	if topology == nil {
		return nil, fmt.Errorf("soak presence lane requires a topology")
	}
	if rng == nil {
		return nil, fmt.Errorf("soak presence lane requires a random source")
	}
	if now == nil {
		now = time.Now
	}
	if cfg.Connections <= 0 {
		cfg.Connections = 1000
	}
	if cfg.QueryBatchSize <= 0 {
		cfg.QueryBatchSize = 50
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.Settle <= 0 {
		cfg.Settle = 5 * time.Second
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 5 * time.Minute
	}
	cfg.QueryShare = min(max(cfg.QueryShare, 0), 1)

	lane := &Lane{
		cfg: cfg, pool: pool, rpc: rpc, observer: observer, recorder: recorder,
		now: now, rng: rng,
	}
	seen := make(map[string]struct{}, cfg.Connections)
	for i := range topology.ActiveUsers {
		if len(lane.connections) >= cfg.Connections {
			break
		}
		account := topology.ActiveUsers[i].Account
		if account == "" {
			continue
		}
		if _, duplicate := seen[account]; duplicate {
			continue
		}
		seen[account] = struct{}{}
		lane.connections = append(lane.connections, &connection{
			account: account,
			connID:  fmt.Sprintf("soak-%s-%04d", topology.ActiveUsers[i].ID, i),
			// Every connection starts offline: the run has sent nothing yet.
			expected: model.StatusOffline,
		})
	}
	if len(lane.connections) == 0 {
		return nil, fmt.Errorf("soak presence lane requires at least one active account")
	}
	lane.refreshGauges()
	return lane, nil
}

// Signal advances one virtual client through its lifecycle, or spends the slot
// verifying instead when the query share is due.
func (l *Lane) Signal(ctx context.Context) error {
	if l.claimQuerySlot() {
		return l.Verify(ctx)
	}
	connection, signal, payload, ok := l.nextSignal()
	if !ok {
		return nil
	}
	err := l.publish(connection, signal, payload)
	l.settle(connection, signal, err == nil)
	if err != nil {
		return fmt.Errorf("publish presence %s: %w", signal, err)
	}
	return nil
}

// Verify queries a batch of connections and compares the reported status with
// what the lane last sent.
func (l *Lane) Verify(ctx context.Context) error {
	// The constructor does not require an RPC client and publish already guards
	// its pool, so guard here too rather than panicking on the query path.
	if l.rpc == nil {
		return fmt.Errorf("soak presence lane requires an RPC client")
	}
	accounts, expectations := l.verifiableBatch()
	if len(accounts) == 0 {
		l.countCheck(CheckSkipped, 1)
		return nil
	}
	var response model.PresenceQueryResponse
	startedAt := l.now()
	result, err := l.rpc.Call(ctx, rpc.Request{
		Action:  rpc.ActionPresenceQuery,
		Subject: subject.PresenceQueryBatch(l.cfg.SiteID),
		Body:    model.PresenceQuery{Accounts: accounts},
		Timeout: l.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
	}, &response)
	sample := read.Sample{
		Action: rpc.ActionPresenceQuery, Latency: l.now().Sub(startedAt),
		ReplyBytes: result.ReplyBytes, Retries: result.Retries,
	}
	if err != nil {
		sample.ErrorClass = result.ErrorClass
		sample.ErrorReason = result.ErrorReason
		l.record(&sample)
		l.countCheck(CheckUnknown, len(accounts))
		return fmt.Errorf("query presence batch: %w", err)
	}
	sample.CountRows(len(response.States))
	l.record(&sample)

	reported := make(map[string]model.PresenceStatus, len(response.States))
	for i := range response.States {
		reported[response.States[i].Account] = response.States[i].Status
	}
	matched, mismatched, unknown := 0, 0, 0
	for account, expected := range expectations {
		status, ok := reported[account]
		switch {
		case !ok:
			unknown++
		case status == expected:
			matched++
		default:
			mismatched++
		}
	}
	l.countCheck(CheckMatch, matched)
	l.countCheck(CheckMismatch, mismatched)
	l.countCheck(CheckUnknown, unknown)
	return nil
}

func (l *Lane) nextSignal() (*connection, string, []byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for offset := range l.connections {
		connection := l.connections[(l.cursor+offset)%len(l.connections)]
		if connection.inFlight {
			continue
		}
		connection.inFlight = true
		l.cursor = (l.cursor + offset + 1) % len(l.connections)
		signal := l.nextSignalKindLocked(connection)
		return connection, signal, payloadFor(signal, connection.connID, l.now()), true
	}
	return nil, "", nil, false
}

// nextSignalKindLocked walks a client through hello, refreshes and idle edges,
// then a disconnect, which is the shape a real client produces.
func (l *Lane) nextSignalKindLocked(connection *connection) string {
	if connection.expected == model.StatusOffline {
		return SignalHello
	}
	switch roll := l.rng.Float64(); {
	case roll < 0.60:
		return SignalPing
	case roll < 0.75 && connection.expected == model.StatusOnline:
		return SignalActivityAway
	case roll < 0.90 && connection.expected == model.StatusAway:
		return SignalActivityBack
	case roll < 0.95:
		return SignalBye
	default:
		return SignalPing
	}
}

func (l *Lane) publish(
	connection *connection,
	signal string,
	payload []byte,
) error {
	if l.pool == nil {
		return fmt.Errorf("soak presence lane requires a publisher")
	}
	target := subjectFor(signal, connection.account, l.cfg.SiteID)
	if target == "" {
		return fmt.Errorf("unsupported presence signal %q", signal)
	}
	return l.pool.Publish(target, payload)
}

func (l *Lane) settle(
	connection *connection,
	signal string,
	sent bool,
) {
	l.mu.Lock()
	defer l.mu.Unlock()
	connection.inFlight = false
	if !sent {
		return
	}
	at := l.now().UTC()
	connection.lastSent = at
	// The publish is unacknowledged, so this records what the lane asked for,
	// never a confirmed server state. The query is what turns it into evidence.
	switch signal {
	case SignalHello, SignalActivityBack:
		connection.expected = model.StatusOnline
		connection.lastPing = at
	case SignalPing:
		connection.lastPing = at
	case SignalActivityAway:
		connection.expected = model.StatusAway
		connection.lastPing = at
	case SignalBye:
		connection.expected = model.StatusOffline
	}
	l.countSignal(signal)
	l.refreshGaugesLocked()
}

// verifiableBatch selects connections whose expectation the server still owes.
// A connection is skipped while its last signal is settling, and an online
// expectation is skipped once the TTL could legitimately have expired it.
func (l *Lane) verifiableBatch() ([]string, map[string]model.PresenceStatus) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now().UTC()
	accounts := make([]string, 0, l.cfg.QueryBatchSize)
	expectations := make(map[string]model.PresenceStatus, l.cfg.QueryBatchSize)
	for offset := range l.connections {
		if len(accounts) >= l.cfg.QueryBatchSize {
			break
		}
		connection := l.connections[(l.cursor+offset)%len(l.connections)]
		if connection.inFlight || connection.lastSent.IsZero() {
			continue
		}
		if now.Sub(connection.lastSent) < l.cfg.Settle {
			continue
		}
		if connection.expected != model.StatusOffline &&
			now.Sub(connection.lastPing) >= l.cfg.TTL {
			continue
		}
		accounts = append(accounts, connection.account)
		expectations[connection.account] = connection.expected
	}
	return accounts, expectations
}

func (l *Lane) claimQuerySlot() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.queryCredit += l.cfg.QueryShare
	if l.queryCredit < 1 {
		return false
	}
	l.queryCredit--
	return true
}

func (l *Lane) countSignal(signal string) {
	if l.observer == nil {
		return
	}
	l.observer.CountSignal(signal)
}

func (l *Lane) countCheck(result string, count int) {
	if l.observer == nil || count <= 0 {
		return
	}
	l.observer.CountCheck(result, count)
}

func (l *Lane) record(sample *read.Sample) {
	if l.recorder != nil {
		l.recorder.Record(sample)
	}
}

func (l *Lane) refreshGauges() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refreshGaugesLocked()
}

func (l *Lane) refreshGaugesLocked() {
	if l.observer == nil {
		return
	}
	counts := map[model.PresenceStatus]int{
		model.StatusOnline: 0, model.StatusAway: 0, model.StatusOffline: 0,
	}
	for _, connection := range l.connections {
		counts[connection.expected]++
	}
	for status, count := range counts {
		l.observer.SetConnections(status, count)
	}
}

func concreteSubject(pattern, account string) string {
	return strings.Replace(pattern, "{account}", account, 1)
}

func helloSubject(account, siteID string) string {
	return concreteSubject(subject.PresenceHelloPattern(siteID), account)
}

func pingSubject(account, siteID string) string {
	return concreteSubject(subject.PresencePingPattern(siteID), account)
}

func activitySubject(account, siteID string) string {
	return concreteSubject(subject.PresenceActivityPattern(siteID), account)
}

func byeSubject(account, siteID string) string {
	return concreteSubject(subject.PresenceByePattern(siteID), account)
}

func subjectFor(signal, account, siteID string) string {
	switch signal {
	case SignalHello:
		return helloSubject(account, siteID)
	case SignalPing:
		return pingSubject(account, siteID)
	case SignalActivityAway, SignalActivityBack:
		return activitySubject(account, siteID)
	case SignalBye:
		return byeSubject(account, siteID)
	default:
		return ""
	}
}

func payloadFor(signal, connID string, at time.Time) []byte {
	timestamp := at.UTC().UnixMilli()
	var payload any
	switch signal {
	case SignalHello:
		payload = model.Hello{ConnID: connID, Timestamp: timestamp}
	case SignalPing:
		payload = model.Ping{ConnID: connID, Timestamp: timestamp}
	case SignalActivityAway:
		payload = model.Activity{ConnID: connID, Away: true, Timestamp: timestamp}
	case SignalActivityBack:
		payload = model.Activity{ConnID: connID, Away: false, Timestamp: timestamp}
	case SignalBye:
		payload = model.ByeRequest{ConnID: connID, Timestamp: timestamp}
	default:
		return nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return encoded
}

// NATSPublisher sends presence signals on the soak's existing
// connection. They are plain core NATS publishes with no reply, matching how a
// real client emits them.
type NATSPublisher struct {
	conn *nats.Conn
}

func NewNATSPublisher(conn *nats.Conn) *NATSPublisher {
	return &NATSPublisher{conn: conn}
}

func (p *NATSPublisher) Publish(target string, data []byte) error {
	if p == nil || p.conn == nil {
		return fmt.Errorf("soak presence publisher is not connected")
	}
	if err := p.conn.PublishMsg(&nats.Msg{
		Subject: target,
		Data:    data,
		Header:  nats.Header{natsutil.RequestIDHeader: []string{idgen.GenerateRequestID()}},
	}); err != nil {
		return fmt.Errorf("publish presence signal: %w", err)
	}
	return nil
}
