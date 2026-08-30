//go:build integration

package main

import (
	"context"
	"encoding/json"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// soakPresenceSpy stands in for presence-service: it records the signals that
// actually arrived on the wire and answers batch queries from that record, so
// the test exercises the real publish path rather than a fake publisher.
type soakPresenceSpy struct {
	mu       sync.Mutex
	statuses map[string]model.PresenceStatus
	requests []string
	connIDs  []string
}

func newSoakPresenceSpy(t *testing.T, natsURL, siteID string) *soakPresenceSpy {
	t.Helper()
	spy := &soakPresenceSpy{statuses: map[string]model.PresenceStatus{}}

	conn, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	for pattern, status := range map[string]model.PresenceStatus{
		subject.PresenceHelloPattern(siteID): model.StatusOnline,
		subject.PresencePingPattern(siteID):  "",
		subject.PresenceByePattern(siteID):   model.StatusOffline,
	} {
		wildcard := concretePresenceSubject(pattern, "*")
		subscription, subscribeErr := conn.Subscribe(wildcard, spy.handler(status))
		require.NoError(t, subscribeErr)
		t.Cleanup(func() { _ = subscription.Unsubscribe() })
	}
	activity, err := conn.Subscribe(
		concretePresenceSubject(subject.PresenceActivityPattern(siteID), "*"),
		func(msg *nats.Msg) {
			var body model.Activity
			if json.Unmarshal(msg.Data, &body) != nil {
				return
			}
			status := model.StatusOnline
			if body.Away {
				status = model.StatusAway
			}
			spy.handler(status)(msg)
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = activity.Unsubscribe() })

	query, err := conn.Subscribe(
		subject.PresenceQueryBatch(siteID),
		func(msg *nats.Msg) {
			var request model.PresenceQuery
			if json.Unmarshal(msg.Data, &request) != nil {
				return
			}
			response := model.PresenceQueryResponse{}
			spy.mu.Lock()
			for _, account := range request.Accounts {
				status, ok := spy.statuses[account]
				if !ok {
					continue
				}
				response.States = append(response.States, model.PresenceState{
					Account: account, Status: status,
				})
			}
			spy.mu.Unlock()
			encoded, err := json.Marshal(response)
			if err != nil {
				return
			}
			_ = msg.Respond(encoded)
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = query.Unsubscribe() })
	require.NoError(t, conn.Flush())
	return spy
}

// handler records the account's new status. A ping carries no status change, so
// it only proves the signal arrived.
func (s *soakPresenceSpy) handler(status model.PresenceStatus) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		account := presenceAccountFromSubject(msg.Subject)
		var envelope struct {
			ConnID string `json:"connId"`
		}
		_ = json.Unmarshal(msg.Data, &envelope)

		s.mu.Lock()
		defer s.mu.Unlock()
		s.requests = append(s.requests, msg.Header.Get(natsutil.RequestIDHeader))
		s.connIDs = append(s.connIDs, envelope.ConnID)
		if status != "" {
			s.statuses[account] = status
		}
	}
}

func (s *soakPresenceSpy) snapshot() ([]string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...), append([]string(nil), s.connIDs...)
}

// presenceAccountFromSubject pulls the account out of
// chat.user.{account}.event.presence.{siteID}.{signal}.
func presenceAccountFromSubject(target string) string {
	const prefix = "chat.user."
	rest := target[len(prefix):]
	for i := range rest {
		if rest[i] == '.' {
			return rest[:i]
		}
	}
	return rest
}

func newSoakPresenceIntegrationLane(
	t *testing.T,
	natsURL, siteID string,
	now func() time.Time,
) (*soakPresenceLane, *Metrics) {
	t.Helper()
	conn, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	metrics := NewMetrics()
	t.Cleanup(metrics.stopNATSHealth)
	lane, err := newSoakPresenceLane(
		soakPresenceConfig{
			SiteID: siteID, Connections: 2, QueryShare: 0,
			Settle: time.Millisecond, TTL: time.Minute,
			QueryBatchSize: 8, RequestTimeout: 5 * time.Second,
		},
		soakRoomStateTestTopology(3),
		newNATSSoakPresencePublisher(conn),
		newSoakRPCClient(
			newNATSHistoryRequester(conn), soakRetryConfig{MaxAttempts: 1}, nil, nil,
		),
		metrics, &soakRoomReadRecorder{},
		rand.New(rand.NewSource(1)), now,
	)
	require.NoError(t, err)
	return lane, metrics
}

func TestSoakPresence_SignalsReachPresenceSubjectsWithARequestID(t *testing.T) {
	const siteID = "site-a"
	natsURL := testutil.NATS(t)
	spy := newSoakPresenceSpy(t, natsURL, siteID)

	now := time.Now().UTC()
	lane, metrics := newSoakPresenceIntegrationLane(
		t, natsURL, siteID, func() time.Time { return now },
	)
	ctx := context.Background()

	// Both connections start offline, so the first pass is two hellos.
	require.NoError(t, lane.Signal(ctx))
	require.NoError(t, lane.Signal(ctx))
	require.Eventually(t, func() bool {
		requests, _ := spy.snapshot()
		return len(requests) == 2
	}, 5*time.Second, 10*time.Millisecond)

	requests, connIDs := spy.snapshot()
	for i := range requests {
		assert.True(t, idgen.IsValidUUID(requests[i]),
			"every presence publish must carry a correlatable request ID")
		assert.NotEmpty(t, connIDs[i])
	}
	assert.Equal(t, float64(2), promtestutil.ToFloat64(
		metrics.SoakPresenceSignals.WithLabelValues(soakPresenceSignalHello)))
}

func TestSoakPresence_QueryMatchesTheAnnouncedState(t *testing.T) {
	const siteID = "site-a"
	natsURL := testutil.NATS(t)
	spy := newSoakPresenceSpy(t, natsURL, siteID)

	now := time.Now().UTC()
	lane, metrics := newSoakPresenceIntegrationLane(
		t, natsURL, siteID, func() time.Time { return now },
	)
	ctx := context.Background()

	require.NoError(t, lane.Signal(ctx))
	require.NoError(t, lane.Signal(ctx))
	require.Eventually(t, func() bool {
		requests, _ := spy.snapshot()
		return len(requests) == 2
	}, 5*time.Second, 10*time.Millisecond)

	// Past the settle window the announced state is fair to compare, and still
	// inside the TTL so the server is not entitled to have expired it.
	now = now.Add(time.Second)

	accounts, expectations := lane.verifiableBatch()
	require.Len(t, accounts, 2)
	for _, expected := range expectations {
		assert.Equal(t, model.StatusOnline, expected)
	}

	require.NoError(t, lane.Verify(ctx))

	assert.Equal(t, float64(2), promtestutil.ToFloat64(
		metrics.SoakPresenceChecks.WithLabelValues(soakPresenceCheckMatch)))
	assert.Equal(t, float64(0), promtestutil.ToFloat64(
		metrics.SoakPresenceChecks.WithLabelValues(soakPresenceCheckMismatch)))
}
