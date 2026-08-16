package main

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

type soakPresenceRecordingPublisher struct {
	mu       sync.Mutex
	subjects []string
	payloads [][]byte
	err      error
}

func (p *soakPresenceRecordingPublisher) Publish(target string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.subjects = append(p.subjects, target)
	p.payloads = append(p.payloads, append([]byte(nil), data...))
	return nil
}

func (p *soakPresenceRecordingPublisher) sent() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.subjects...)
}

type soakPresenceFixture struct {
	lane      *soakPresenceLane
	publisher *soakPresenceRecordingPublisher
	transport *soakRoomOpsTransport
	metrics   *Metrics
	now       time.Time
}

func (f *soakPresenceFixture) advance(d time.Duration) { f.now = f.now.Add(d) }

func newSoakPresenceFixture(t *testing.T, queryShare float64, reply []byte) *soakPresenceFixture {
	t.Helper()
	fixture := &soakPresenceFixture{
		publisher: &soakPresenceRecordingPublisher{},
		transport: &soakRoomOpsTransport{reply: reply},
		metrics:   NewMetrics(),
		now:       time.Date(2026, 8, 16, 9, 0, 0, 0, time.UTC),
	}
	topology := soakRoomStateTestTopology(3)
	lane, err := newSoakPresenceLane(
		soakPresenceConfig{
			SiteID: "site-a", Connections: 2, QueryShare: queryShare,
			Settle: time.Second, TTL: 5 * time.Minute, QueryBatchSize: 10,
			RequestTimeout: time.Second,
		},
		topology,
		fixture.publisher,
		newSoakRPCClient(fixture.transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		fixture.metrics,
		&soakRoomReadRecorder{},
		rand.New(rand.NewSource(1)),
		func() time.Time { return fixture.now },
	)
	require.NoError(t, err)
	fixture.lane = lane
	return fixture
}

func TestSoakPresenceLane_StartsEveryConnectionWithHello(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, nil)

	require.NoError(t, fixture.lane.Signal(context.Background()))

	sent := fixture.publisher.sent()
	require.Len(t, sent, 1)
	assert.True(t, strings.HasSuffix(sent[0], ".presence.site-a.hello"),
		"a connection the run has not opened yet must start with hello, got %q", sent[0])
	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.SoakPresenceSignals.WithLabelValues(soakPresenceSignalHello)))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.SoakPresenceConnections.WithLabelValues(string(model.StatusOnline))))
}

func TestSoakPresenceLane_PayloadsCarryAStableConnectionID(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, nil)

	for range 6 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
	}

	connIDs := make(map[string]map[string]struct{})
	for i, payload := range fixture.publisher.payloads {
		var decoded struct {
			ConnID    string `json:"connId"`
			Timestamp int64  `json:"timestamp"`
		}
		require.NoError(t, json.Unmarshal(payload, &decoded))
		assert.NotEmpty(t, decoded.ConnID)
		assert.Positive(t, decoded.Timestamp)
		account := fixture.publisher.subjects[i]
		if connIDs[account] == nil {
			connIDs[account] = make(map[string]struct{})
		}
		connIDs[account][decoded.ConnID] = struct{}{}
	}
	for account, ids := range connIDs {
		assert.Len(t, ids, 1, "account %s must reuse one connection ID", account)
	}
}

func TestSoakPresenceLane_AdvancesThroughItsLifecycle(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, nil)

	for range 30 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
		fixture.advance(time.Second)
	}

	kinds := make(map[string]bool)
	for _, sent := range fixture.publisher.sent() {
		switch {
		case strings.HasSuffix(sent, ".hello"):
			kinds["hello"] = true
		case strings.HasSuffix(sent, ".ping"):
			kinds["ping"] = true
		case strings.HasSuffix(sent, ".activity"):
			kinds["activity"] = true
		case strings.HasSuffix(sent, ".bye"):
			kinds["bye"] = true
		}
	}
	assert.True(t, kinds["hello"])
	assert.True(t, kinds["ping"], "a live connection must be refreshed")
	assert.GreaterOrEqual(t, len(kinds), 3, "the lane must produce more than one signal shape")
}

func TestSoakPresenceLane_FailedPublishDoesNotMoveTheExpectation(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, nil)
	fixture.publisher.err = errors.New("connection closed")

	err := fixture.lane.Signal(context.Background())

	require.Error(t, err)
	assert.Equal(t, float64(2), testutil.ToFloat64(
		fixture.metrics.SoakPresenceConnections.WithLabelValues(string(model.StatusOffline))),
		"a signal that never left the process cannot change what the server is expected to report")
}

func TestSoakPresenceLane_VerifyComparesAgainstTheLastSignal(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, []byte(
		`{"states":[{"account":"user-a0","status":"online"},{"account":"user-b0","status":"online"}],"timestamp":1}`,
	))
	for range 2 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
	}
	fixture.advance(2 * time.Second)

	require.NoError(t, fixture.lane.Verify(context.Background()))

	assert.Equal(t, subject.PresenceQueryBatch("site-a"), fixture.transport.subjects[0])
	assert.Equal(t, float64(2), testutil.ToFloat64(
		fixture.metrics.SoakPresenceChecks.WithLabelValues(soakPresenceCheckMatch)))
}

func TestSoakPresenceLane_VerifyCountsADisagreement(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, []byte(
		`{"states":[{"account":"user-a0","status":"offline"},{"account":"user-b0","status":"offline"}],"timestamp":1}`,
	))
	for range 2 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
	}
	fixture.advance(2 * time.Second)

	require.NoError(t, fixture.lane.Verify(context.Background()))

	assert.Equal(t, float64(2), testutil.ToFloat64(
		fixture.metrics.SoakPresenceChecks.WithLabelValues(soakPresenceCheckMismatch)))
}

func TestSoakPresenceLane_VerifySkipsSignalsThatHaveNotSettled(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, []byte(`{"states":[],"timestamp":1}`))
	require.NoError(t, fixture.lane.Signal(context.Background()))

	require.NoError(t, fixture.lane.Verify(context.Background()))

	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.SoakPresenceChecks.WithLabelValues(soakPresenceCheckSkipped)))
	assert.Empty(t, fixture.transport.subjects, "nothing settled yet, so nothing is asked")
}

func TestSoakPresenceLane_VerifySkipsExpectationsThePresenceTTLCouldHaveDropped(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, []byte(`{"states":[],"timestamp":1}`))
	for range 2 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
	}
	// Past the connection TTL the server is entitled to drop the connection, so
	// "we think it is online" stops being a claim it owes an answer to.
	fixture.advance(10 * time.Minute)

	require.NoError(t, fixture.lane.Verify(context.Background()))

	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.SoakPresenceChecks.WithLabelValues(soakPresenceCheckSkipped)))
	assert.Equal(t, float64(0), testutil.ToFloat64(
		fixture.metrics.SoakPresenceChecks.WithLabelValues(soakPresenceCheckMismatch)))
}

func TestSoakPresenceLane_VerifyCountsAnUnansweredQuery(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, nil)
	fixture.transport.err = nats.ErrNoResponders
	for range 2 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
	}
	fixture.advance(2 * time.Second)

	err := fixture.lane.Verify(context.Background())

	require.Error(t, err)
	assert.Equal(t, float64(2), testutil.ToFloat64(
		fixture.metrics.SoakPresenceChecks.WithLabelValues(soakPresenceCheckUnknown)),
		"an unreachable presence service is never a mismatch")
}

func TestSoakPresenceLane_MissingAccountInTheReplyIsUnknown(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, []byte(`{"states":[],"timestamp":1}`))
	for range 2 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
	}
	fixture.advance(2 * time.Second)

	require.NoError(t, fixture.lane.Verify(context.Background()))

	assert.Equal(t, float64(2), testutil.ToFloat64(
		fixture.metrics.SoakPresenceChecks.WithLabelValues(soakPresenceCheckUnknown)))
}

func TestSoakPresenceLane_QueryShareSpendsSlotsOnVerification(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 1.0, []byte(`{"states":[],"timestamp":1}`))

	require.NoError(t, fixture.lane.Signal(context.Background()))

	assert.Empty(t, fixture.publisher.sent(),
		"a full query share turns every slot into a verification")
	assert.Positive(t, testutil.ToFloat64(
		fixture.metrics.SoakPresenceChecks.WithLabelValues(soakPresenceCheckSkipped)))
}

func TestSoakPresenceLane_RejectsInvalidConstruction(t *testing.T) {
	_, err := newSoakPresenceLane(
		soakPresenceConfig{SiteID: "site-a"}, nil, nil, nil, nil, nil,
		rand.New(rand.NewSource(1)), nil,
	)
	require.Error(t, err)

	_, err = newSoakPresenceLane(
		soakPresenceConfig{SiteID: "site-a"}, soakRoomStateTestTopology(1), nil, nil, nil, nil,
		nil, nil,
	)
	require.Error(t, err)

	_, err = newSoakPresenceLane(
		soakPresenceConfig{SiteID: "site-a"}, &soakTopology{}, nil, nil, nil, nil,
		rand.New(rand.NewSource(1)), nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "active account")
}

func TestSoakPresenceLane_RequiresAPublisher(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, nil)
	fixture.lane.pool = nil

	err := fixture.lane.Signal(context.Background())

	require.Error(t, err)
}

func TestSoakPresencePayload_CoversEverySignal(t *testing.T) {
	at := time.Date(2026, 8, 16, 9, 0, 0, 0, time.UTC)
	for _, signal := range []string{
		soakPresenceSignalHello, soakPresenceSignalPing,
		soakPresenceSignalActivityAway, soakPresenceSignalActivityBack,
		soakPresenceSignalBye,
	} {
		t.Run(signal, func(t *testing.T) {
			payload := soakPresencePayload(signal, "conn-1", at)

			require.NotNil(t, payload)
			assert.Contains(t, string(payload), `"connId":"conn-1"`)
			assert.NotEmpty(t, soakPresenceSubject(signal, "user-a0", "site-a"))
		})
	}
	assert.Nil(t, soakPresencePayload("unknown", "conn-1", at))
	assert.Empty(t, soakPresenceSubject("unknown", "user-a0", "site-a"))
}

func TestSoakPresenceLane_AwayEdgeIsReportedAsAway(t *testing.T) {
	fixture := newSoakPresenceFixture(t, 0, nil)
	require.NoError(t, fixture.lane.Signal(context.Background()))

	// Drive until the lane emits an away edge for a live connection.
	for range 40 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
		if testutil.ToFloat64(
			fixture.metrics.SoakPresenceSignals.WithLabelValues(soakPresenceSignalActivityAway),
		) > 0 {
			break
		}
	}

	assert.Positive(t, testutil.ToFloat64(
		fixture.metrics.SoakPresenceSignals.WithLabelValues(soakPresenceSignalActivityAway)))
	assert.Positive(t, testutil.ToFloat64(
		fixture.metrics.SoakPresenceConnections.WithLabelValues(string(model.StatusAway))))
}
