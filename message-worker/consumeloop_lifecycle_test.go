package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// These tests characterise what main.go's consume loop actually does when
// iter.Next() returns an error, and what a SIGTERM rolling update does to
// messages the client has already pre-fetched. They run against an embedded
// JetStream server (no Docker) so they execute under `make test`.
//
// The production loop takes the o11y wrapper's three-value Next (ctx, msg,
// err); the raw nats.go iterator used here returns two. The wrapper is a
// pass-through on the error path (o11y/nats/jetstream.go returns the
// underlying error verbatim), so the control flow under test is identical.

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

type lifecycleRig struct {
	server  *natsserver.Server
	nc      *nats.Conn
	js      jetstream.JetStream
	stream  string
	durable string
	subject string
	proxy   *freezeProxy
}

// startLifecycleRig brings up an embedded JetStream server holding
// MESSAGES-CANONICAL and, when withProxy is set, routes the client through a
// freezable TCP proxy so a test can starve the client of server traffic
// without closing the connection.
func startLifecycleRig(t *testing.T, siteID string, withProxy bool) *lifecycleRig {
	t.Helper()

	opts := &natsserver.Options{Port: -1, JetStream: true, StoreDir: t.TempDir()}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	url := ns.ClientURL()
	rig := &lifecycleRig{server: ns}
	if withProxy {
		rig.proxy = startFreezeProxy(t, ns.Addr().String())
		url = rig.proxy.URL()
	}

	nc, err := nats.Connect(url, nats.ReconnectWait(50*time.Millisecond), nats.MaxReconnects(-1))
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	sc := stream.MessagesCanonical(siteID)
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{Name: sc.Name, Subjects: sc.Subjects})
	require.NoError(t, err)

	rig.nc = nc
	rig.js = js
	rig.stream = sc.Name
	rig.durable = "message-worker"
	rig.subject = subject.MsgCanonicalCreated(siteID)
	return rig
}

// consumer creates (or updates) the service's durable with the given ack
// settings. BackOffSteps is left at zero so redelivery is a flat AckWait,
// which keeps the shutdown test's redelivery assertion fast.
func (r *lifecycleRig) consumer(t *testing.T, ackWait time.Duration) jetstream.Consumer {
	t.Helper()
	cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait: ackWait, MaxDeliver: -1, MaxWaiting: 512, MaxAckPending: 1000,
	})
	cc.Durable = r.durable
	cc.FilterSubjects = []string{r.subject}
	cons, err := r.js.CreateOrUpdateConsumer(context.Background(), r.stream, cc)
	require.NoError(t, err)
	return cons
}

func (r *lifecycleRig) publish(t *testing.T, n int) {
	t.Helper()
	for i := range n {
		_, err := r.js.Publish(context.Background(), r.subject, fmt.Appendf(nil, `{"seq":%d}`, i))
		require.NoError(t, err, "publish %d", i)
	}
}

func (r *lifecycleRig) numAckPending(t *testing.T, cons jetstream.Consumer) int {
	t.Helper()
	info, err := cons.Info(context.Background())
	require.NoError(t, err)
	return info.NumAckPending
}

// ---------------------------------------------------------------------------
// the loop shapes under test
// ---------------------------------------------------------------------------

// currentLoop mirrors message-worker/main.go's consume loop: `return` on any
// error from Next, with the loop goroutine itself counted in wg so shutdown's
// wg.Wait cannot pass a message already handed off. The returned channel
// carries the error that stopped it.
func currentLoop(iter jetstream.MessagesContext, maxWorkers int, wg *sync.WaitGroup, process func(jetstream.Msg)) <-chan error {
	stopped := make(chan error, 1)
	sem := make(chan struct{}, maxWorkers)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			msg, err := iter.Next()
			if err != nil {
				stopped <- err
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(msg jetstream.Msg) {
				defer func() { <-sem; wg.Done() }()
				process(msg)
			}(msg)
		}
	}()
	return stopped
}

// resilientLoop is the control: it returns only on errors that genuinely close
// the iterator and keeps calling Next on everything else. Its sole purpose is
// to show that the message loss in the current shape comes from the `return`,
// not from the iterator being unusable.
func resilientLoop(iter jetstream.MessagesContext, maxWorkers int, wg *sync.WaitGroup, process func(jetstream.Msg)) <-chan error {
	stopped := make(chan error, 1)
	sem := make(chan struct{}, maxWorkers)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			msg, err := iter.Next()
			if err != nil {
				if isIteratorTerminal(err) {
					stopped <- err
					return
				}
				continue
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(msg jetstream.Msg) {
				defer func() { <-sem; wg.Done() }()
				process(msg)
			}(msg)
		}
	}()
	return stopped
}

// isIteratorTerminal reports whether Next's error leaves the iterator closed.
// Everything else — a missed heartbeat above all — leaves it usable.
func isIteratorTerminal(err error) bool {
	return errors.Is(err, jetstream.ErrMsgIteratorClosed) ||
		errors.Is(err, jetstream.ErrConsumerDeleted) ||
		errors.Is(err, jetstream.ErrConsumerNotFound) ||
		errors.Is(err, nats.ErrConnectionClosed)
}

func waitStopped(t *testing.T, stopped <-chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-stopped:
		return err
	case <-time.After(within):
		t.Fatalf("consume loop still running after %s", within)
		return nil
	}
}

func waitCount(t *testing.T, c *atomic.Int64, want int64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if c.Load() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("processed %d messages, want %d within %s", c.Load(), want, within)
}

func waitWG(t *testing.T, wg *sync.WaitGroup, within time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(within):
		t.Fatalf("worker drain did not finish within %s", within)
	}
}

// ---------------------------------------------------------------------------
// scenario 1: Next returns an error
// ---------------------------------------------------------------------------

// TestNextError_LoopReturnsAndNeverPullsAgain answers the question directly:
// the `return` leaves the for loop AND the goroutine, so the pod stops pulling
// for the rest of its life. Nothing restarts it, and the NATS readiness probe
// still reports healthy — so Kubernetes keeps the deaf pod in service.
func TestNextError_LoopReturnsAndNeverPullsAgain(t *testing.T) {
	rig := startLifecycleRig(t, "site1", false)
	cons := rig.consumer(t, 30*time.Second)

	iter, err := cons.Messages(jetstream.PullMaxMessages(10))
	require.NoError(t, err)
	t.Cleanup(iter.Stop)

	var processed atomic.Int64
	var wg sync.WaitGroup
	stopped := currentLoop(iter, 4, &wg, func(msg jetstream.Msg) {
		processed.Add(1)
		require.NoError(t, msg.Ack())
	})

	rig.publish(t, 1)
	waitCount(t, &processed, 1, 5*time.Second)

	// Deleting the consumer is the cleanest way to make the server hand the
	// blocked Next a terminal status. Any error takes the same code path.
	require.NoError(t, rig.js.DeleteConsumer(context.Background(), rig.stream, rig.durable))

	loopErr := waitStopped(t, stopped, 5*time.Second)
	require.Error(t, loopErr)
	t.Logf("loop exited with: %v", loopErr)

	// The pod is now deaf. Recreate the consumer — as an operator would — and
	// publish more work: this process consumes none of it.
	cons = rig.consumer(t, 30*time.Second)
	rig.publish(t, 5)
	time.Sleep(1500 * time.Millisecond)

	assert.Equal(t, int64(1), processed.Load(),
		"loop must not have consumed anything after Next returned an error")
	info, err := cons.Info(context.Background())
	require.NoError(t, err)
	// 6, not 5: the recreated durable starts from DeliverAll, so it also
	// replays the message this pod acked before the loop died. The point is
	// that the backlog only grows — this process consumes none of it.
	assert.Equal(t, uint64(6), info.NumPending, "the backlog is stranded in the stream")

	// And nothing in the pod's own health surface reflects this.
	assert.Equal(t, nats.CONNECTED, rig.nc.Status(),
		"natsutil.HealthCheck still reports the pod ready while the loop is dead")

	waitWG(t, &wg, 5*time.Second)
}

// TestNextError_MissedHeartbeatIsRecoverableButLoopQuitsAnyway is the case that
// matters in production: ErrNoHeartbeat is a transient stall, the iterator
// stays fully usable, and yet the current loop shape treats it as fatal.
func TestNextError_MissedHeartbeatIsRecoverableButLoopQuitsAnyway(t *testing.T) {
	rig := startLifecycleRig(t, "site2", true)
	cons := rig.consumer(t, 30*time.Second)

	// Short expiry/heartbeat so a stall is detected in ~1s instead of ~60s.
	// Production uses the defaults: 30s expiry, 15s heartbeat, so the same
	// error fires after 30s of server silence.
	iter, err := cons.Messages(jetstream.PullMaxMessages(10),
		jetstream.PullExpiry(2*time.Second), jetstream.PullHeartbeat(500*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(iter.Stop)

	// Part A: a frozen link makes Next report a missed heartbeat...
	rig.proxy.Freeze()
	_, err = iter.Next()
	rig.proxy.Thaw()
	require.ErrorIs(t, err, jetstream.ErrNoHeartbeat)

	// ...and the very same iterator keeps working once traffic resumes. Next
	// did not close it: the library considers this recoverable.
	rig.publish(t, 1)
	msg, err := iter.Next()
	require.NoError(t, err, "iterator must still be usable after ErrNoHeartbeat")
	require.NoError(t, msg.Ack())

	// Part B: the production loop shape over the same stall. One 1s hiccup and
	// the pod never consumes again, even though the link is healthy after.
	iter2, err := cons.Messages(jetstream.PullMaxMessages(10),
		jetstream.PullExpiry(2*time.Second), jetstream.PullHeartbeat(500*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(iter2.Stop)

	var processed atomic.Int64
	var wg sync.WaitGroup
	stopped := currentLoop(iter2, 4, &wg, func(msg jetstream.Msg) {
		processed.Add(1)
		require.NoError(t, msg.Ack())
	})

	rig.proxy.Freeze()
	loopErr := waitStopped(t, stopped, 10*time.Second)
	rig.proxy.Thaw()
	require.ErrorIs(t, loopErr, jetstream.ErrNoHeartbeat)

	rig.publish(t, 3)
	time.Sleep(2 * time.Second)
	assert.Equal(t, int64(0), processed.Load(),
		"a transient stall permanently stopped a loop whose iterator was still good")
	waitWG(t, &wg, 5*time.Second)

	// The control — the same stall against a loop that continues on
	// non-terminal errors — is TestNextError_ResilientLoopSurvivesTheSameStall.
	// It needs its own consumer: iter2 above still holds an outstanding
	// server-side pull request, so messages published now are routed to its
	// abandoned inbox and stay invisible until AckWait lapses. That is itself
	// part of the cost of a dead loop that nobody tears down.
}

// TestNextError_ResilientLoopSurvivesTheSameStall is the control for the test
// above: identical stall, identical iterator settings, but the loop treats a
// non-terminal Next error as a retry rather than an exit. It resumes on its
// own with no restart and no message loss.
func TestNextError_ResilientLoopSurvivesTheSameStall(t *testing.T) {
	rig := startLifecycleRig(t, "site5", true)
	cons := rig.consumer(t, 30*time.Second)

	iter, err := cons.Messages(jetstream.PullMaxMessages(10),
		jetstream.PullExpiry(2*time.Second), jetstream.PullHeartbeat(500*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(iter.Stop)

	var processed atomic.Int64
	var wg sync.WaitGroup
	resilientLoop(iter, 4, &wg, func(msg jetstream.Msg) {
		processed.Add(1)
		require.NoError(t, msg.Ack())
	})

	rig.publish(t, 1)
	waitCount(t, &processed, 1, 5*time.Second)

	// Stall long enough for at least one ErrNoHeartbeat, then restore the link.
	rig.proxy.Freeze()
	time.Sleep(1500 * time.Millisecond)
	rig.proxy.Thaw()

	rig.publish(t, 3)
	waitCount(t, &processed, 4, 20*time.Second)

	iter.Stop()
	waitWG(t, &wg, 5*time.Second)
}

// ---------------------------------------------------------------------------
// scenario 2: SIGTERM during a Kubernetes rolling update
// ---------------------------------------------------------------------------

// TestShutdown_StopEndsLoopDrainsInFlightAndDiscardsPrefetched walks the exact
// shutdown.Wait order from main.go: iter.Stop() -> wg.Wait() -> drain. Three
// things fall out: the loop ends on ErrMsgIteratorClosed, the in-flight worker
// finishes before the drain returns, and every message the client had already
// pre-fetched is dropped unacked and comes back on redelivery.
func TestShutdown_StopEndsLoopDrainsInFlightAndDiscardsPrefetched(t *testing.T) {
	const ackWait = 2 * time.Second
	rig := startLifecycleRig(t, "site3", false)
	cons := rig.consumer(t, ackWait)

	rig.publish(t, 10)

	iter, err := cons.Messages(jetstream.PullMaxMessages(10))
	require.NoError(t, err)

	var processed atomic.Int64
	var inFlight atomic.Int64
	var lastAck atomic.Int64
	release := make(chan struct{})
	var wg sync.WaitGroup

	// One worker slot: one message is in the handler, one more is held by the
	// loop on the semaphore send, and the remaining eight sit in the client
	// buffer that Stop is about to throw away.
	stopped := currentLoop(iter, 1, &wg, func(msg jetstream.Msg) {
		inFlight.Add(1)
		<-release
		require.NoError(t, msg.Ack())
		lastAck.Store(time.Now().UnixNano())
		processed.Add(1)
	})

	waitCount(t, &inFlight, 1, 5*time.Second)
	// Wait until the server has handed all ten to this client, so what follows
	// is genuinely about pre-fetched messages being discarded.
	deadline := time.Now().Add(10 * time.Second)
	for rig.numAckPending(t, cons) < 10 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, 10, rig.numAckPending(t, cons), "server should have delivered all 10 to the client")

	// --- SIGTERM arrives; shutdown.Wait runs its funcs in order ---
	iter.Stop()

	// Stop alone does not end the loop here: it is parked on the semaphore
	// send, not inside Next. The loop only unwinds once a worker frees a slot,
	// which is why the ordering below (Stop, then wait for workers) matters.
	select {
	case err := <-stopped:
		t.Fatalf("loop exited before its worker was released: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release) // handlers finish on their own, mid-drain
	loopErr := waitStopped(t, stopped, 10*time.Second)
	require.ErrorIs(t, loopErr, jetstream.ErrMsgIteratorClosed,
		"Stop is what ends the loop on a graceful shutdown")

	waitWG(t, &wg, 10*time.Second)
	drainedAt := time.Now().UnixNano()

	handled := processed.Load()
	assert.Positive(t, lastAck.Load(), "the in-flight message must be acked before the drain completes")
	assert.Less(t, lastAck.Load(), drainedAt, "wg.Wait must not return before its workers acked")
	assert.LessOrEqual(t, handled, int64(2),
		"only the messages already past Next are handled; the pre-fetched rest are discarded")

	// Everything not handled was never acked, so JetStream redelivers it once
	// AckWait lapses. Nothing is lost; the cost is latency plus a second
	// delivery the replacement pod must absorb idempotently.
	want := 10 - int(handled)
	next, err := cons.Messages(jetstream.PullMaxMessages(10))
	require.NoError(t, err)
	t.Cleanup(next.Stop)

	redelivered := 0
	for seen := 0; seen < want; seen++ {
		msg, err := next.Next()
		require.NoError(t, err)
		meta, err := msg.Metadata()
		require.NoError(t, err)
		if meta.NumDelivered > 1 {
			redelivered++
		}
		require.NoError(t, msg.Ack())
	}
	assert.Equal(t, want, redelivered,
		"every discarded pre-fetch came back as a redelivery on the replacement consumer")
}

// TestShutdown_DrainWouldDeliverBufferedMessagesInstead contrasts Stop with
// Drain on the identical setup: Drain hands the already-buffered messages to
// Next before reporting closed, so the pod finishes what it already holds
// instead of leaving it for a redelivery.
func TestShutdown_DrainWouldDeliverBufferedMessagesInstead(t *testing.T) {
	rig := startLifecycleRig(t, "site4", false)
	cons := rig.consumer(t, 30*time.Second)

	rig.publish(t, 10)

	iter, err := cons.Messages(jetstream.PullMaxMessages(10))
	require.NoError(t, err)
	t.Cleanup(iter.Stop)

	// The client issues its first pull request from inside Next, so prime it
	// before measuring what the server has handed over.
	first, err := iter.Next()
	require.NoError(t, err)
	require.NoError(t, first.Ack())

	deadline := time.Now().Add(10 * time.Second)
	for rig.numAckPending(t, cons) < 9 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.GreaterOrEqual(t, rig.numAckPending(t, cons), 9, "server should have delivered the batch to the client")

	iter.Drain()

	handled := 0
	for {
		msg, err := iter.Next()
		if err != nil {
			require.ErrorIs(t, err, jetstream.ErrMsgIteratorClosed)
			break
		}
		require.NoError(t, msg.Ack())
		handled++
	}
	assert.Equal(t, 9, handled, "Drain delivers the client-side buffer; Stop discards it")
}

// TestReconnect_LoopSurvivesADroppedConnection is the contrast that makes the
// heartbeat case worth caring about. A real disconnect — a NATS pod restart, a
// severed TCP link — is handled inside Next: the client reconnects, Next never
// returns an error, and the loop keeps consuming. It is only the silent stall
// that the client cannot distinguish from a healthy idle link that surfaces as
// an error, and that is the one the current shape treats as fatal.
func TestReconnect_LoopSurvivesADroppedConnection(t *testing.T) {
	rig := startLifecycleRig(t, "site6", true)
	cons := rig.consumer(t, 30*time.Second)

	iter, err := cons.Messages(jetstream.PullMaxMessages(10))
	require.NoError(t, err)
	t.Cleanup(iter.Stop)

	var processed atomic.Int64
	var wg sync.WaitGroup
	stopped := currentLoop(iter, 4, &wg, func(msg jetstream.Msg) {
		processed.Add(1)
		require.NoError(t, msg.Ack())
	})

	rig.publish(t, 1)
	waitCount(t, &processed, 1, 5*time.Second)

	rig.proxy.Break()
	deadline := time.Now().Add(10 * time.Second)
	for rig.nc.Status() != nats.CONNECTED && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, nats.CONNECTED, rig.nc.Status(), "client should have reconnected through the proxy")

	select {
	case err := <-stopped:
		t.Fatalf("loop exited on a reconnect it should have absorbed: %v", err)
	default:
	}

	rig.publish(t, 3)
	waitCount(t, &processed, 4, 20*time.Second)

	iter.Stop()
	require.ErrorIs(t, waitStopped(t, stopped, 5*time.Second), jetstream.ErrMsgIteratorClosed)
	waitWG(t, &wg, 5*time.Second)
}

// ---------------------------------------------------------------------------
// freezable TCP proxy
// ---------------------------------------------------------------------------

// freezeProxy sits between the NATS client and server and can stall the
// server→client direction on demand. That models the failure ErrNoHeartbeat
// exists to catch: the socket stays open and the client never sees a
// disconnect, but no server traffic arrives.
type freezeProxy struct {
	ln     net.Listener
	gate   sync.RWMutex
	wg     sync.WaitGroup
	fmu    sync.Mutex
	frozen bool

	cmu   sync.Mutex
	conns []net.Conn
}

func startFreezeProxy(t *testing.T, target string) *freezeProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &freezeProxy{ln: ln}

	t.Cleanup(func() {
		p.Thaw() // a frozen gate would deadlock the pipes below
		_ = ln.Close()
		p.Break()
		p.wg.Wait()
	})

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			cli, err := ln.Accept()
			if err != nil {
				return
			}
			srv, err := net.Dial("tcp", target)
			if err != nil {
				_ = cli.Close()
				return
			}
			p.cmu.Lock()
			p.conns = append(p.conns, cli, srv)
			p.cmu.Unlock()

			p.wg.Add(2)
			go func() { defer p.wg.Done(); p.pipe(srv, cli, false); _ = srv.Close(); _ = cli.Close() }()
			go func() { defer p.wg.Done(); p.pipe(cli, srv, true); _ = srv.Close(); _ = cli.Close() }()
		}
	}()
	return p
}

func (p *freezeProxy) URL() string { return "nats://" + p.ln.Addr().String() }

func (p *freezeProxy) pipe(dst, src net.Conn, gated bool) {
	buf := make([]byte, 32*1024)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			if gated {
				p.gate.RLock()
			}
			_, werr := dst.Write(buf[:n])
			if gated {
				p.gate.RUnlock()
			}
			if werr != nil {
				return
			}
		}
		if rerr != nil {
			return
		}
	}
}

// Break severs every live connection, which the NATS client sees as a dropped
// TCP link. New connections are still accepted, so it reconnects.
func (p *freezeProxy) Break() {
	p.cmu.Lock()
	conns := p.conns
	p.conns = nil
	p.cmu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// Freeze stops server→client bytes without touching the connection.
func (p *freezeProxy) Freeze() {
	p.fmu.Lock()
	defer p.fmu.Unlock()
	if !p.frozen {
		p.gate.Lock()
		p.frozen = true
	}
}

// Thaw resumes forwarding. Idempotent, so cleanup can call it unconditionally.
func (p *freezeProxy) Thaw() {
	p.fmu.Lock()
	defer p.fmu.Unlock()
	if p.frozen {
		p.gate.Unlock()
		p.frozen = false
	}
}
