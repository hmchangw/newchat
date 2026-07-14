package main

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// roundRobin is a lock-free monotonic counter used to spread publishes across
// the publisher connection pool.
type roundRobin struct{ n atomic.Uint64 }

func (r *roundRobin) next(mod int) int {
	if mod <= 0 {
		return 0
	}
	return int((r.n.Add(1) - 1) % uint64(mod))
}

// decodePresenceState extracts the account and status from a PresenceState
// publish. ok is false on malformed input.
func decodePresenceState(data []byte) (string, model.PresenceStatus, bool) {
	var st model.PresenceState
	if err := json.Unmarshal(data, &st); err != nil || st.Account == "" {
		return "", "", false
	}
	return st.Account, st.Status, true
}

// presencePool owns publisher conns (round-robin) and observer conns
// (subscribed to the per-user state wildcard). Each observed publish is
// timestamped and fed to the collector.
type presencePool struct {
	pubConns  []*nats.Conn
	obsConns  []*nats.Conn
	rr        roundRobin
	collector *presenceCollector
}

// newPresencePool dials pubN publisher conns and obsN observer conns, and
// subscribes every observer conn to chat.user.presence.state.*.
func newPresencePool(url, credsFile string, pubN, obsN int, c *presenceCollector) (*presencePool, error) {
	p := &presencePool{collector: c}
	for i := 0; i < pubN; i++ {
		nc, err := connectWithCreds(url, fmt.Sprintf("loadgen-presence-pub-%d", i), credsFile)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("presence publisher conn %d: %w", i, err)
		}
		p.pubConns = append(p.pubConns, nc)
	}
	wildcard := subject.PresenceState("*")
	for i := 0; i < obsN; i++ {
		nc, err := connectWithCreds(url, fmt.Sprintf("loadgen-presence-obs-%d", i), credsFile)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("presence observer conn %d: %w", i, err)
		}
		if _, err := nc.Subscribe(wildcard, p.onState); err != nil {
			_ = nc.Drain()
			p.Close()
			return nil, fmt.Errorf("presence observer subscribe: %w", err)
		}
		if err := nc.Flush(); err != nil {
			p.Close()
			return nil, fmt.Errorf("presence observer flush: %w", err)
		}
		p.obsConns = append(p.obsConns, nc)
	}
	return p, nil
}

func (p *presencePool) onState(m *nats.Msg) {
	acc, status, ok := decodePresenceState(m.Data)
	if !ok {
		return
	}
	p.collector.Observe(acc, status, time.Now())
}

// Publish sends one transition on a round-robin publisher conn with a fresh
// X-Request-ID header (matches the daily emitter convention).
func (p *presencePool) Publish(subj string, data []byte) error {
	if len(p.pubConns) == 0 {
		return fmt.Errorf("no publisher conn")
	}
	nc := p.pubConns[p.rr.next(len(p.pubConns))]
	return nc.PublishMsg(&nats.Msg{
		Subject: subj,
		Data:    data,
		Header:  nats.Header{natsutil.RequestIDHeader: []string{idgen.GenerateRequestID()}},
	})
}

// Close drains all connections. The slice fields are deliberately left intact
// (not set to nil): emitter/warm goroutines may still call Publish concurrently
// during shutdown, and mutating the slice header here would be a data race.
// PublishMsg on a drained conn returns an error, which callers already handle.
func (p *presencePool) Close() {
	for _, nc := range p.pubConns {
		_ = nc.Drain()
	}
	for _, nc := range p.obsConns {
		_ = nc.Drain()
	}
}
