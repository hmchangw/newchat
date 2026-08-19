package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

const (
	soakPresenceSignalHello        = "hello"
	soakPresenceSignalPing         = "ping"
	soakPresenceSignalActivityAway = "activity_away"
	soakPresenceSignalActivityBack = "activity_back"
	soakPresenceSignalBye          = "bye"
)

const (
	soakPresenceCheckMatch    = "match"
	soakPresenceCheckMismatch = "mismatch"
	soakPresenceCheckSkipped  = "skipped"
	soakPresenceCheckUnknown  = "unknown"
)

// soakPresencePublisher sends a presence signal. The signals are fire-and-forget
// core NATS publishes, so this returns an error only for local failures.
type soakPresencePublisher interface {
	Publish(subject string, data []byte) error
}

type soakPresenceConfig struct {
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

type soakPresenceConnection struct {
	account  string
	connID   string
	expected model.PresenceStatus
	lastSent time.Time
	lastPing time.Time
	inFlight bool
}

// soakPresenceLane drives presence traffic and samples whether the reported
// state agrees with what it last sent.
//
// It deliberately keeps no evidence ledger. Presence signals are unacknowledged
// publishes — during a NATS outage the client library buffers them and returns
// success — so a successful send proves nothing, and the state is TTL-bound and
// allowed to expire on its own. The only honest evidence is a later query, and
// only for a connection that was refreshed recently enough that the server
// still owes an answer.
type soakPresenceLane struct {
	cfg      soakPresenceConfig
	pool     soakPresencePublisher
	rpc      *soakRPCClient
	metrics  *Metrics
	recorder soakReadSampleRecorder
	now      func() time.Time

	mu          sync.Mutex
	rng         *rand.Rand
	connections []*soakPresenceConnection
	cursor      int
	queryCredit float64
}

func newSoakPresenceLane(
	cfg soakPresenceConfig,
	topology *soakTopology,
	pool soakPresencePublisher,
	rpc *soakRPCClient,
	metrics *Metrics,
	recorder soakReadSampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) (*soakPresenceLane, error) {
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
		cfg.RequestTimeout = soakRequestTimeout
	}
	if cfg.Settle <= 0 {
		cfg.Settle = 5 * time.Second
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 5 * time.Minute
	}
	cfg.QueryShare = min(max(cfg.QueryShare, 0), 1)

	lane := &soakPresenceLane{
		cfg: cfg, pool: pool, rpc: rpc, metrics: metrics, recorder: recorder,
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
		lane.connections = append(lane.connections, &soakPresenceConnection{
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
func (l *soakPresenceLane) Signal(ctx context.Context) error {
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
func (l *soakPresenceLane) Verify(ctx context.Context) error {
	// The constructor does not require an RPC client and publish already guards
	// its pool, so guard here too rather than panicking on the query path.
	if l.rpc == nil {
		return fmt.Errorf("soak presence lane requires an RPC client")
	}
	accounts, expectations := l.verifiableBatch()
	if len(accounts) == 0 {
		l.countCheck(soakPresenceCheckSkipped, 1)
		return nil
	}
	var response model.PresenceQueryResponse
	startedAt := l.now()
	result, err := l.rpc.Call(ctx, soakRPCRequest{
		Action:  soakRPCPresenceQuery,
		Subject: subject.PresenceQueryBatch(l.cfg.SiteID),
		Body:    model.PresenceQuery{Accounts: accounts},
		Timeout: l.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response)
	sample := soakReadSample{
		Action: soakRPCPresenceQuery, Latency: l.now().Sub(startedAt), Retries: result.Retries,
	}
	if err != nil {
		sample.ErrorClass = result.ErrorClass
		l.record(&sample)
		l.countCheck(soakPresenceCheckUnknown, len(accounts))
		return fmt.Errorf("query presence batch: %w", err)
	}
	sample.Messages = len(response.States)
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
	l.countCheck(soakPresenceCheckMatch, matched)
	l.countCheck(soakPresenceCheckMismatch, mismatched)
	l.countCheck(soakPresenceCheckUnknown, unknown)
	return nil
}

func (l *soakPresenceLane) nextSignal() (*soakPresenceConnection, string, []byte, bool) {
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
		return connection, signal, soakPresencePayload(signal, connection.connID, l.now()), true
	}
	return nil, "", nil, false
}

// nextSignalKindLocked walks a client through hello, refreshes and idle edges,
// then a disconnect, which is the shape a real client produces.
func (l *soakPresenceLane) nextSignalKindLocked(connection *soakPresenceConnection) string {
	if connection.expected == model.StatusOffline {
		return soakPresenceSignalHello
	}
	switch roll := l.rng.Float64(); {
	case roll < 0.60:
		return soakPresenceSignalPing
	case roll < 0.75 && connection.expected == model.StatusOnline:
		return soakPresenceSignalActivityAway
	case roll < 0.90 && connection.expected == model.StatusAway:
		return soakPresenceSignalActivityBack
	case roll < 0.95:
		return soakPresenceSignalBye
	default:
		return soakPresenceSignalPing
	}
}

func (l *soakPresenceLane) publish(
	connection *soakPresenceConnection,
	signal string,
	payload []byte,
) error {
	if l.pool == nil {
		return fmt.Errorf("soak presence lane requires a publisher")
	}
	target := soakPresenceSubject(signal, connection.account, l.cfg.SiteID)
	if target == "" {
		return fmt.Errorf("unsupported presence signal %q", signal)
	}
	return l.pool.Publish(target, payload)
}

func (l *soakPresenceLane) settle(
	connection *soakPresenceConnection,
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
	case soakPresenceSignalHello, soakPresenceSignalActivityBack:
		connection.expected = model.StatusOnline
		connection.lastPing = at
	case soakPresenceSignalPing:
		connection.lastPing = at
	case soakPresenceSignalActivityAway:
		connection.expected = model.StatusAway
		connection.lastPing = at
	case soakPresenceSignalBye:
		connection.expected = model.StatusOffline
	}
	l.countSignal(signal)
	l.refreshGaugesLocked()
}

// verifiableBatch selects connections whose expectation the server still owes.
// A connection is skipped while its last signal is settling, and an online
// expectation is skipped once the TTL could legitimately have expired it.
func (l *soakPresenceLane) verifiableBatch() ([]string, map[string]model.PresenceStatus) {
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

func (l *soakPresenceLane) claimQuerySlot() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.queryCredit += l.cfg.QueryShare
	if l.queryCredit < 1 {
		return false
	}
	l.queryCredit--
	return true
}

func (l *soakPresenceLane) countSignal(signal string) {
	if l.metrics == nil {
		return
	}
	l.metrics.SoakPresenceSignals.WithLabelValues(signal).Inc()
}

func (l *soakPresenceLane) countCheck(result string, count int) {
	if l.metrics == nil || count <= 0 {
		return
	}
	l.metrics.SoakPresenceChecks.WithLabelValues(result).Add(float64(count))
}

func (l *soakPresenceLane) record(sample *soakReadSample) {
	if l.recorder != nil {
		l.recorder.Record(sample)
	}
}

func (l *soakPresenceLane) refreshGauges() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refreshGaugesLocked()
}

func (l *soakPresenceLane) refreshGaugesLocked() {
	if l.metrics == nil {
		return
	}
	counts := map[model.PresenceStatus]int{
		model.StatusOnline: 0, model.StatusAway: 0, model.StatusOffline: 0,
	}
	for _, connection := range l.connections {
		counts[connection.expected]++
	}
	for status, count := range counts {
		l.metrics.SoakPresenceConnections.WithLabelValues(string(status)).Set(float64(count))
	}
}

func soakPresenceSubject(signal, account, siteID string) string {
	switch signal {
	case soakPresenceSignalHello:
		return presenceHelloSubject(account, siteID)
	case soakPresenceSignalPing:
		return presencePingSubject(account, siteID)
	case soakPresenceSignalActivityAway, soakPresenceSignalActivityBack:
		return presenceActivitySubject(account, siteID)
	case soakPresenceSignalBye:
		return presenceByeSubject(account, siteID)
	default:
		return ""
	}
}

func soakPresencePayload(signal, connID string, at time.Time) []byte {
	timestamp := at.UTC().UnixMilli()
	var payload any
	switch signal {
	case soakPresenceSignalHello:
		payload = model.Hello{ConnID: connID, Timestamp: timestamp}
	case soakPresenceSignalPing:
		payload = model.Ping{ConnID: connID, Timestamp: timestamp}
	case soakPresenceSignalActivityAway:
		payload = model.Activity{ConnID: connID, Away: true, Timestamp: timestamp}
	case soakPresenceSignalActivityBack:
		payload = model.Activity{ConnID: connID, Away: false, Timestamp: timestamp}
	case soakPresenceSignalBye:
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

// natsSoakPresencePublisher sends presence signals on the soak's existing
// connection. They are plain core NATS publishes with no reply, matching how a
// real client emits them.
type natsSoakPresencePublisher struct {
	conn *nats.Conn
}

func newNATSSoakPresencePublisher(conn *nats.Conn) *natsSoakPresencePublisher {
	return &natsSoakPresencePublisher{conn: conn}
}

func (p *natsSoakPresencePublisher) Publish(target string, data []byte) error {
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
