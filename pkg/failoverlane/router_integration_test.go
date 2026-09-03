//go:build integration

package failoverlane

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) {
	testutil.RunTestsWithPrewarm(m, testutil.EnsureNATS, testutil.EnsureNATSBuddy)
}

type echoReq struct {
	Ping string `json:"ping"`
}

type echoResp struct {
	Lane string `json:"lane"`
}

// dial opens a connection the test owns, drained on cleanup.
func dial(t *testing.T, url string) *o11ynats.Conn {
	t.Helper()
	conn, err := natsutil.Connect(context.Background(), url, "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NoError(t, err)
	t.Cleanup(func() { conn.NatsConn().Close() })
	return conn
}

// The point of the second router is that a client displaced onto the buddy
// cluster still reaches THIS site's service. The subjects are site-scoped, so
// nothing else can answer them — without a router listening on the buddy, a
// displaced client's request gets no responder at all.
func TestBindRouters_BuddyRouterAnswersOnTheBuddyCluster(t *testing.T) {
	homeURL, buddyURL := testutil.NATSPair(t)
	homeConn := dial(t, homeURL)
	ctx := context.Background()

	const pattern = "chat.user.*.request.failoverlane.echo"
	var order []subject.Lane
	routers, err := BindRouters(ctx, homeConn, nil,
		&natsutil.BuddyDialer{Config: natsutil.BuddyConfig{SiteID: "site-b", NatsURL: buddyURL},
			TracerProvider: noop.NewTracerProvider(), Propagator: propagation.TraceContext{}},
		func(_ context.Context, conn *o11ynats.Conn, _ o11ynats.JetStream, lane subject.Lane) (*natsrouter.Router, error) {
			order = append(order, lane)
			r := natsrouter.Default(conn, "failoverlane-test")
			natsrouter.Register(r, pattern, func(_ *natsrouter.Context, _ echoReq) (*echoResp, error) {
				return &echoResp{Lane: laneName(lane)}, nil
			})
			return r, nil
		})
	require.NoError(t, err)
	// Builders capture home-lane state for the failover build (room-service hands
	// the home handler the standby publisher), so the order is part of the contract.
	require.Equal(t, []subject.Lane{subject.LaneHome, subject.LaneFailover}, order)
	require.NotNil(t, routers.Buddy, "the buddy lane must bind against a live buddy server")
	t.Cleanup(func() {
		for _, hook := range routers.ShutdownHooks() {
			_ = hook(context.Background())
		}
	})

	const subj = "chat.user.alice.request.failoverlane.echo"
	assert.Equal(t, "home", requestLane(t, homeConn, subj), "the home lane answers on its own cluster")

	// The displaced client: same subject, buddy cluster.
	buddyClient := dial(t, buddyURL)
	assert.Equal(t, "failover", requestLane(t, buddyClient, subj),
		"a request arriving on the buddy must be answered by the buddy router")
}

// With no buddy configured — the single-site deployment — the home lane must be
// unaffected, and nothing must be listening anywhere else.
func TestBindRouters_NoBuddyStillServesHome(t *testing.T) {
	homeURL, buddyURL := testutil.NATSPair(t)
	homeConn := dial(t, homeURL)

	const pattern = "chat.user.*.request.failoverlane.homeonly"
	routers, err := BindRouters(context.Background(), homeConn, nil,
		&natsutil.BuddyDialer{TracerProvider: noop.NewTracerProvider(), Propagator: propagation.TraceContext{}},
		func(_ context.Context, conn *o11ynats.Conn, _ o11ynats.JetStream, lane subject.Lane) (*natsrouter.Router, error) {
			r := natsrouter.Default(conn, "failoverlane-test-homeonly")
			natsrouter.Register(r, pattern, func(_ *natsrouter.Context, _ echoReq) (*echoResp, error) {
				return &echoResp{Lane: laneName(lane)}, nil
			})
			return r, nil
		})
	require.NoError(t, err)
	require.Nil(t, routers.Buddy)
	t.Cleanup(func() {
		for _, hook := range routers.ShutdownHooks() {
			_ = hook(context.Background())
		}
	})

	const subj = "chat.user.alice.request.failoverlane.homeonly"
	assert.Equal(t, "home", requestLane(t, homeConn, subj))

	// No responder on the buddy: the request is refused immediately rather than
	// answered by the buddy site's own service.
	_, err = dial(t, buddyURL).NatsConn().Request(subj, []byte(`{"ping":"x"}`), 2*time.Second)
	assert.Error(t, err, "nothing may answer this site's subjects on the buddy cluster")
}

func laneName(l subject.Lane) string {
	if l == subject.LaneFailover {
		return "failover"
	}
	return "home"
}

// requestLane issues the RPC and returns which lane answered it.
func requestLane(t *testing.T, conn *o11ynats.Conn, subj string) string {
	t.Helper()
	msg, err := conn.NatsConn().Request(subj, []byte(`{"ping":"x"}`), 5*time.Second)
	require.NoError(t, err)

	var resp struct {
		Lane string `json:"lane"`
	}
	require.NoError(t, json.Unmarshal(msg.Data, &resp))
	return resp.Lane
}
