package presence

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

type recordingPublisher struct {
	mu       sync.Mutex
	subjects []string
	payloads [][]byte
	err      error
}

func (p *recordingPublisher) Publish(target string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.subjects = append(p.subjects, target)
	p.payloads = append(p.payloads, append([]byte(nil), data...))
	return nil
}

func (p *recordingPublisher) sent() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.subjects...)
}

type presenceFixture struct {
	lane      *Lane
	publisher *recordingPublisher
	transport *testTransport
	observer  *testObserver
	now       time.Time
}

func (f *presenceFixture) advance(d time.Duration) { f.now = f.now.Add(d) }

func newPresenceFixture(t *testing.T, queryShare float64, reply []byte) *presenceFixture {
	t.Helper()
	fixture := &presenceFixture{
		publisher: &recordingPublisher{},
		transport: &testTransport{reply: reply},
		observer:  newTestObserver(),
		now:       time.Date(2026, 8, 16, 9, 0, 0, 0, time.UTC),
	}
	topology := testTopology()
	lane, err := New(
		Config{
			SiteID: "site-a", Connections: 2, QueryShare: queryShare,
			Settle: time.Second, TTL: 5 * time.Minute, QueryBatchSize: 10,
			RequestTimeout: time.Second,
		},
		topology,
		fixture.publisher,
		rpc.NewClient(fixture.transport, rpc.RetryConfig{MaxAttempts: 1}, &testSleeper{}, nil),
		fixture.observer,
		nil,
		rand.New(rand.NewSource(1)),
		func() time.Time { return fixture.now },
	)
	require.NoError(t, err)
	fixture.lane = lane
	return fixture
}

func TestSoakPresenceLane_StartsEveryConnectionWithHello(t *testing.T) {
	fixture := newPresenceFixture(t, 0, nil)

	require.NoError(t, fixture.lane.Signal(context.Background()))

	sent := fixture.publisher.sent()
	require.Len(t, sent, 1)
	assert.True(t, strings.HasSuffix(sent[0], ".presence.site-a.hello"),
		"a connection the run has not opened yet must start with hello, got %q", sent[0])
	assert.Equal(t, 1, fixture.observer.signal(SignalHello))
	assert.Equal(t, 1, fixture.observer.connection(model.StatusOnline))
}

func TestSoakPresenceLane_PayloadsCarryAStableConnectionID(t *testing.T) {
	fixture := newPresenceFixture(t, 0, nil)

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
	fixture := newPresenceFixture(t, 0, nil)

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
	fixture := newPresenceFixture(t, 0, nil)
	fixture.publisher.err = errors.New("connection closed")

	err := fixture.lane.Signal(context.Background())

	require.Error(t, err)
	assert.Equal(t, 2, fixture.observer.connection(model.StatusOffline),
		"a signal that never left the process cannot change what the server is expected to report")
}

func TestSoakPresenceLane_VerifyComparesAgainstTheLastSignal(t *testing.T) {
	fixture := newPresenceFixture(t, 0, []byte(
		`{"states":[{"account":"user-a0","status":"online"},{"account":"user-b0","status":"online"}],"timestamp":1}`,
	))
	for range 2 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
	}
	fixture.advance(2 * time.Second)

	require.NoError(t, fixture.lane.Verify(context.Background()))

	assert.Equal(t, subject.PresenceQueryBatch("site-a"), fixture.transport.subjects[0])
	assert.Equal(t, 2, fixture.observer.check(CheckMatch))
}

func TestSoakPresenceLane_VerifyCountsADisagreement(t *testing.T) {
	fixture := newPresenceFixture(t, 0, []byte(
		`{"states":[{"account":"user-a0","status":"offline"},{"account":"user-b0","status":"offline"}],"timestamp":1}`,
	))
	for range 2 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
	}
	fixture.advance(2 * time.Second)

	require.NoError(t, fixture.lane.Verify(context.Background()))

	assert.Equal(t, 2, fixture.observer.check(CheckMismatch))
}

func TestSoakPresenceLane_VerifySkipsSignalsThatHaveNotSettled(t *testing.T) {
	fixture := newPresenceFixture(t, 0, []byte(`{"states":[],"timestamp":1}`))
	require.NoError(t, fixture.lane.Signal(context.Background()))

	require.NoError(t, fixture.lane.Verify(context.Background()))

	assert.Equal(t, 1, fixture.observer.check(CheckSkipped))
	assert.Empty(t, fixture.transport.subjects, "nothing settled yet, so nothing is asked")
}

func TestSoakPresenceLane_VerifySkipsExpectationsThePresenceTTLCouldHaveDropped(t *testing.T) {
	fixture := newPresenceFixture(t, 0, []byte(`{"states":[],"timestamp":1}`))
	for range 2 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
	}
	// Past the connection TTL the server is entitled to drop the connection, so
	// "we think it is online" stops being a claim it owes an answer to.
	fixture.advance(10 * time.Minute)

	require.NoError(t, fixture.lane.Verify(context.Background()))

	assert.Equal(t, 1, fixture.observer.check(CheckSkipped))
	assert.Zero(t, fixture.observer.check(CheckMismatch))
}

func TestSoakPresenceLane_VerifyCountsAnUnansweredQuery(t *testing.T) {
	fixture := newPresenceFixture(t, 0, nil)
	fixture.transport.err = nats.ErrNoResponders
	for range 2 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
	}
	fixture.advance(2 * time.Second)

	err := fixture.lane.Verify(context.Background())

	require.Error(t, err)
	assert.Equal(t, 2, fixture.observer.check(CheckUnknown),
		"an unreachable presence service is never a mismatch")
}

func TestSoakPresenceLane_MissingAccountInTheReplyIsUnknown(t *testing.T) {
	fixture := newPresenceFixture(t, 0, []byte(`{"states":[],"timestamp":1}`))
	for range 2 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
	}
	fixture.advance(2 * time.Second)

	require.NoError(t, fixture.lane.Verify(context.Background()))

	assert.Equal(t, 2, fixture.observer.check(CheckUnknown))
}

func TestSoakPresenceLane_QueryShareSpendsSlotsOnVerification(t *testing.T) {
	fixture := newPresenceFixture(t, 1.0, []byte(`{"states":[],"timestamp":1}`))

	require.NoError(t, fixture.lane.Signal(context.Background()))

	assert.Empty(t, fixture.publisher.sent(),
		"a full query share turns every slot into a verification")
	assert.Positive(t, fixture.observer.check(CheckSkipped))
}

func TestSoakPresenceLane_RejectsInvalidConstruction(t *testing.T) {
	_, err := New(
		Config{SiteID: "site-a"}, nil, nil, nil, nil, nil,
		rand.New(rand.NewSource(1)), nil,
	)
	require.Error(t, err)

	_, err = New(
		Config{SiteID: "site-a"}, testTopology(), nil, nil, nil, nil,
		nil, nil,
	)
	require.Error(t, err)

	_, err = New(
		Config{SiteID: "site-a"}, &topology.Topology{}, nil, nil, nil, nil,
		rand.New(rand.NewSource(1)), nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "active account")
}

func TestNewSoakPresenceLane_FiltersAndBoundsConnections(t *testing.T) {
	lane, err := New(
		Config{SiteID: "site-a", Connections: 2},
		&topology.Topology{ActiveUsers: []model.User{
			{ID: "empty"},
			{ID: "u-1", Account: "alice"},
			{ID: "u-duplicate", Account: "alice"},
			{ID: "u-2", Account: "bob"},
			{ID: "u-3", Account: "carol"},
		}},
		nil,
		nil,
		newTestObserver(),
		nil,
		rand.New(rand.NewSource(1)),
		nil,
	)

	require.NoError(t, err)
	require.Len(t, lane.connections, 2)
	assert.Equal(t, "alice", lane.connections[0].account)
	assert.Equal(t, "bob", lane.connections[1].account)
}

func TestSoakPresenceLane_RequiresAPublisher(t *testing.T) {
	fixture := newPresenceFixture(t, 0, nil)
	fixture.lane.pool = nil

	err := fixture.lane.Signal(context.Background())

	require.Error(t, err)
}

func TestSoakPresencePayload_CoversEverySignal(t *testing.T) {
	at := time.Date(2026, 8, 16, 9, 0, 0, 0, time.UTC)
	for _, signal := range []string{
		SignalHello, SignalPing,
		SignalActivityAway, SignalActivityBack,
		SignalBye,
	} {
		t.Run(signal, func(t *testing.T) {
			payload := payloadFor(signal, "conn-1", at)

			require.NotNil(t, payload)
			assert.Contains(t, string(payload), `"connId":"conn-1"`)
			assert.NotEmpty(t, subjectFor(signal, "user-a0", "site-a"))
		})
	}
	assert.Nil(t, payloadFor("unknown", "conn-1", at))
	assert.Empty(t, subjectFor("unknown", "user-a0", "site-a"))
}

func TestSoakPresenceLane_AwayEdgeIsReportedAsAway(t *testing.T) {
	fixture := newPresenceFixture(t, 0, nil)
	require.NoError(t, fixture.lane.Signal(context.Background()))

	// Drive until the lane emits an away edge for a live connection.
	for range 40 {
		require.NoError(t, fixture.lane.Signal(context.Background()))
		if fixture.observer.signal(SignalActivityAway) > 0 {
			break
		}
	}

	assert.Positive(t, fixture.observer.signal(SignalActivityAway))
	assert.Positive(t, fixture.observer.connection(model.StatusAway))
}
