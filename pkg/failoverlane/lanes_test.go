package failoverlane

import (
	"context"
	"sync"
	"testing"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// embeddedJetStream starts an in-process JetStream server; a fixed port when
// given, so a test can bring "home" up on a URL it handed out earlier.
func embeddedJetStream(t *testing.T, port int) *natsserver.Server {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: port, JetStream: true, StoreDir: t.TempDir(),
	})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)
	return ns
}

// recorder is a lane handler that remembers which lane it was built for and
// what it received.
type recorder struct {
	mu   sync.Mutex
	got  []string
	lane []subject.Lane
}

func (r *recorder) build(_ context.Context, _ *o11ynats.Conn, _ o11ynats.JetStream, lane subject.Lane) (func(context.Context, jetstream.Msg), error) {
	r.mu.Lock()
	r.lane = append(r.lane, lane)
	r.mu.Unlock()
	return func(_ context.Context, msg jetstream.Msg) {
		r.mu.Lock()
		r.got = append(r.got, laneTag(lane)+":"+msg.Subject())
		r.mu.Unlock()
		_ = msg.Ack()
	}, nil
}

func laneTag(l subject.Lane) string {
	if l == subject.LaneFailover {
		return "failover"
	}
	return "home"
}

func (r *recorder) received() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.got...)
}

func (r *recorder) built() []subject.Lane {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]subject.Lane(nil), r.lane...)
}

var (
	homeStream  = stream.Config{Name: "LANES-T", Subjects: []string{"lanes.t.>"}}
	buddyStream = stream.Config{Name: "LANES-T-FAILOVER", Subjects: []string{"lanes.f.>"}}
)

func homeSpec() HomeSpec {
	return HomeSpec{
		Stream:   homeStream,
		Consumer: jetstream.ConsumerConfig{Durable: "lanes-t", FilterSubject: "lanes.t.>", AckPolicy: jetstream.AckExplicitPolicy},
		Bootstrap: func(ctx context.Context, js o11ynats.JetStream) error {
			_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: homeStream.Name, Subjects: homeStream.Subjects})
			return err
		},
	}
}

func buddySpec() *LaneSpec {
	return &LaneSpec{
		Stream:   buddyStream,
		Consumer: jetstream.ConsumerConfig{Durable: "lanes-t-failover", FilterSubject: "lanes.f.>", AckPolicy: jetstream.AckExplicitPolicy},
	}
}

func lanesDialer(cfg natsutil.BuddyConfig) *natsutil.BuddyDialer {
	return &natsutil.BuddyDialer{Config: cfg, TracerProvider: noop.NewTracerProvider(), Propagator: propagation.TraceContext{}}
}

// The ordinary deployment: home up, no buddy. The home lane binds synchronously
// and consumes; readiness is up; the hooks run clean.
func TestBindLanes_HomeUpNoBuddy(t *testing.T) {
	ctx := context.Background()
	home := embeddedJetStream(t, -1)
	d := lanesDialer(natsutil.BuddyConfig{})
	nc, js, err := d.ConnectHomeJS(ctx, home.ClientURL(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { nc.NatsConn().Close() })

	rec := &recorder{}
	lanes, err := BindLanes(ctx, nc, js, d, &LanesSpec{SiteID: "site-a", Home: homeSpec(), MaxWorkers: 4}, rec.build)
	require.NoError(t, err)
	assert.Equal(t, []subject.Lane{subject.LaneHome}, rec.built())
	assert.NoError(t, lanes.Check().Probe(ctx))

	_, err = js.Publish(ctx, "lanes.t.1", []byte("x"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(rec.received()) == 1 }, 10*time.Second, 20*time.Millisecond)
	assert.Equal(t, []string{"home:lanes.t.1"}, rec.received())

	for _, h := range append(lanes.StopHooks(), lanes.DrainHooks()...) {
		require.NoError(t, h(ctx))
	}
}

// The case the whole thing exists for: home down at boot, buddy up. The buddy
// lane binds and serves at once, readiness is up on its account, and the home
// lane joins on its own when home returns.
func TestBindLanes_HomeDownBuddyUp(t *testing.T) {
	ctx := context.Background()
	homeURL, startHome := reserveJetStreamPort(t)
	buddy := embeddedJetStream(t, -1)
	d := lanesDialer(natsutil.BuddyConfig{SiteID: "site-b", NatsURL: buddy.ClientURL()})
	nc, js, err := d.ConnectHomeJS(ctx, homeURL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { nc.NatsConn().Close() })

	rec := &recorder{}
	lanes, err := BindLanes(ctx, nc, js, d,
		&LanesSpec{SiteID: "site-a", Home: homeSpec(), Buddy: buddySpec(), Bootstrap: true, MaxWorkers: 4}, rec.build)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, h := range append(lanes.StopHooks(), lanes.DrainHooks()...) {
			_ = h(ctx)
		}
	})

	assert.Equal(t, []subject.Lane{subject.LaneHome, subject.LaneFailover}, rec.built(), "both handlers are built up front, home first")
	assert.NoError(t, lanes.Check().Probe(ctx), "ready on the buddy lane alone")
	assert.False(t, lanes.HomeReady())

	bconn := natsutil.ConnectBuddy(ctx, buddy.ClientURL(), "", d.TracerProvider, d.Propagator, false)
	require.NotNil(t, bconn)
	t.Cleanup(func() { bconn.NatsConn().Close() })
	bjs, err := bconn.JetStream()
	require.NoError(t, err)
	_, err = bjs.Publish(ctx, "lanes.f.1", []byte("x"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(rec.received()) == 1 }, 10*time.Second, 20*time.Millisecond)
	assert.Equal(t, []string{"failover:lanes.f.1"}, rec.received())

	startHome()
	require.Eventually(t, lanes.HomeReady, 20*time.Second, 50*time.Millisecond, "the home lane must bind when home returns")
	_, err = js.Publish(ctx, "lanes.t.1", []byte("x"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(rec.received()) == 2 }, 10*time.Second, 20*time.Millisecond)
	assert.Contains(t, rec.received(), "home:lanes.t.1")
}

// A home handler that cannot be built is a startup fault, exactly as before.
func TestBindLanes_HomeBuildErrorIsFatal(t *testing.T) {
	ctx := context.Background()
	home := embeddedJetStream(t, -1)
	d := lanesDialer(natsutil.BuddyConfig{})
	nc, js, err := d.ConnectHomeJS(ctx, home.ClientURL(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { nc.NatsConn().Close() })

	_, err = BindLanes(ctx, nc, js, d, &LanesSpec{SiteID: "site-a", Home: homeSpec(), MaxWorkers: 4},
		func(_ context.Context, _ *o11ynats.Conn, _ o11ynats.JetStream, _ subject.Lane) (func(context.Context, jetstream.Msg), error) {
			return nil, assert.AnError
		})
	require.Error(t, err)
}

// Nothing bound at all — home down, no buddy — must read not-ready, and the
// hooks must still run: shutdown lists them unconditionally.
func TestBindLanes_NothingBoundIsNotReadyAndStopsClean(t *testing.T) {
	ctx := context.Background()
	homeURL, _ := reserveJetStreamPort(t)
	d := lanesDialer(natsutil.BuddyConfig{SiteID: "site-b", NatsURL: "nats://127.0.0.1:1"})
	nc, js, err := d.ConnectHomeJS(ctx, homeURL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { nc.NatsConn().Close() })

	lanes, err := BindLanes(ctx, nc, js, d,
		&LanesSpec{SiteID: "site-a", Home: homeSpec(), Buddy: buddySpec(), Bootstrap: true, MaxWorkers: 4}, (&recorder{}).build)
	require.NoError(t, err)

	assert.Error(t, lanes.Check().Probe(ctx))
	for _, h := range append(lanes.StopHooks(), lanes.DrainHooks()...) {
		require.NoError(t, h(ctx))
	}
}
