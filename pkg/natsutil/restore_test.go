package natsutil_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsutil"
)

func TestRestoreTracker_ZeroUntilMarked(t *testing.T) {
	var tr natsutil.RestoreTracker
	assert.True(t, tr.RestoredAt().IsZero(),
		"never-lost must read as zero so the grace window is never entered")
}

func TestRestoreTracker_MarkAndRead(t *testing.T) {
	var tr natsutil.RestoreTracker
	at := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	tr.MarkRestored(at)
	assert.Equal(t, at, tr.RestoredAt())
}

func TestRestoreTracker_LatestWins(t *testing.T) {
	var tr natsutil.RestoreTracker
	first := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)
	tr.MarkRestored(first)
	tr.MarkRestored(second)
	assert.Equal(t, second, tr.RestoredAt(),
		"a second outage must restart the window, not keep the first stamp")
}

// An out-of-order stamp must not shorten a window already open — the grace runs
// from the LATEST restoration.
func TestRestoreTracker_EarlierStampIsIgnored(t *testing.T) {
	var tr natsutil.RestoreTracker
	later := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	tr.MarkRestored(later)
	tr.MarkRestored(later.Add(-time.Hour))
	assert.Equal(t, later, tr.RestoredAt())
}

func TestRestoreTracker_ConcurrentAccess(t *testing.T) {
	var tr natsutil.RestoreTracker
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			tr.MarkRestored(time.Now())
		}
	}()
	for i := 0; i < 1000; i++ {
		_ = tr.RestoredAt()
	}
	wg.Wait()
}

func TestTrackRestores_NilConnYieldsAnInertTracker(t *testing.T) {
	tr := natsutil.TrackRestores(context.Background(), nil)
	require.NotNil(t, tr)
	assert.True(t, tr.RestoredAt().IsZero())
}

// The watcher goroutine must exit when its context is cancelled, or every
// service that opens a tracked connection leaks one goroutine per restart.
func TestTrackRestores_StopsOnContextCancel(t *testing.T) {
	url := startTestNATSURL(t)
	conn := natsutil.ConnectBuddy(context.Background(), url, "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NotNil(t, conn)
	t.Cleanup(func() { conn.NatsConn().Close() })

	ctx, cancel := context.WithCancel(context.Background())
	tr := natsutil.TrackRestores(ctx, conn)
	require.NotNil(t, tr)
	assert.True(t, tr.RestoredAt().IsZero(), "a connection that never dropped is not restored")

	cancel()
	// -race plus the goroutine's deferred listener removal catch a watcher that
	// outlives its context; there is nothing further to assert on.
}

// The end-to-end property: bounce the server under a live connection and the
// tracker must stamp the reconnect, because that stamp is what opens the
// dual-publish window during recovery.
func TestTrackRestores_StampsAReconnect(t *testing.T) {
	ns, url := startRestartableNATS(t)
	conn := natsutil.ConnectBuddy(context.Background(), url, "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NotNil(t, conn)
	t.Cleanup(func() { conn.NatsConn().Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tr := natsutil.TrackRestores(ctx, conn)
	require.True(t, tr.RestoredAt().IsZero())

	ns.restart(t)

	require.Eventually(t, func() bool { return !tr.RestoredAt().IsZero() },
		20*time.Second, 50*time.Millisecond,
		"a reconnect must open the room-route grace window")
}

// restartableNATS is an embedded server on a fixed port, so a client dialled at
// its URL reconnects to the replacement rather than finding nothing there. The
// shared startTestNATSURL helper uses Port: -1, which picks a fresh port each
// time and cannot be restarted in place.
type restartableNATS struct {
	port int
	srv  *natsserver.Server
}

func startRestartableNATS(t *testing.T) (*restartableNATS, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	n := &restartableNATS{port: port}
	n.start(t)
	t.Cleanup(func() { n.srv.Shutdown() })
	return n, n.srv.ClientURL()
}

func (n *restartableNATS) start(t *testing.T) {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: n.port})
	require.NoError(t, err)
	srv.Start()
	require.True(t, srv.ReadyForConnections(5*time.Second))
	n.srv = srv
}

func (n *restartableNATS) restart(t *testing.T) {
	t.Helper()
	n.srv.Shutdown()
	n.srv.WaitForShutdown()
	n.start(t)
}
