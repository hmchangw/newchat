//go:build integration

package natsrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"

	"github.com/hmchangw/chat/pkg/testutil"
)

// setupNATS returns an otelnats client connected to the process-shared
// NATS. Required to surface timing races that in-process NATS cannot
// reproduce (real TCP, real server dispatch goroutines, real latency).
func setupNATS(t *testing.T) *otelnats.Conn {
	t.Helper()
	nc, err := otelnats.Connect(testutil.NATS(t))
	require.NoError(t, err, "connect to NATS")
	t.Cleanup(nc.Close)
	return nc
}

type echoReq struct {
	Name string `json:"name"`
	Seq  int    `json:"seq"`
}

type echoResp struct {
	Greeting string `json:"greeting"`
	Seq      int    `json:"seq"`
	ReqID    string `json:"reqId"`
}

// TestIntegration_ConcurrentRequestsWithCopy exercises the full hot path
// against a real NATS server under heavy concurrency: context pool reuse,
// middleware keys, and Copy() handed to an async goroutine that outlives
// the handler. With -race, this must stay clean.
// The unbounded default is sufficient for this test — no WithMaxConcurrency
// override is needed.
func TestIntegration_ConcurrentRequestsWithCopy(t *testing.T) {
	nc := setupNATS(t)
	r := New(nc, "integration-concurrent")
	r.Use(RequestID())
	r.Use(Recovery())
	r.Use(Logging())

	// Async goroutines use Copy() — we count them to prove they all ran.
	var asyncCompleted atomic.Int64
	var asyncStarted sync.WaitGroup

	Register(r, "chat.user.{account}.echo.{room}",
		func(c *Context, req echoReq) (*echoResp, error) {
			c.Set("account", c.Param("account"))
			c.Set("room", c.Param("room"))

			reqID := c.MustGet("requestID").(string)

			// Hand *Context directly to a goroutine that outlives the handler.
			// With the split-struct design (ctx/Msg/Params/keys fresh per
			// request, only the middleware chain-state pooled), this is
			// race-free without any Copy().
			asyncStarted.Add(1)
			go func() {
				defer asyncStarted.Done()
				time.Sleep(5 * time.Millisecond)
				if c.Param("account") == "" || c.Param("room") == "" {
					t.Errorf("context lost params after handler return")
				}
				if got := c.MustGet("account"); got != c.Param("account") {
					t.Errorf("context keys mismatch: %v", got)
				}
				c.Set("async-done", true)
				_, _ = c.Get("async-done")
				asyncCompleted.Add(1)
			}()

			return &echoResp{
				Greeting: "hi " + c.Param("account"),
				Seq:      req.Seq,
				ReqID:    reqID,
			}, nil
		})

	const n = 300
	var clients sync.WaitGroup
	clients.Add(n)
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer clients.Done()
			data, _ := json.Marshal(echoReq{Name: "load", Seq: i})
			subj := fmt.Sprintf("chat.user.u%d.echo.r%d", i%10, i%5)
			resp, err := nc.Request(context.Background(), subj, data, 5*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("seq %d: %w", i, err)
				return
			}
			var r echoResp
			if err := json.Unmarshal(resp.Data, &r); err != nil {
				errCh <- fmt.Errorf("seq %d unmarshal: %w", i, err)
				return
			}
			if r.Seq != i {
				errCh <- fmt.Errorf("seq %d got seq %d", i, r.Seq)
			}
			if r.ReqID == "" {
				errCh <- fmt.Errorf("seq %d got empty reqID", i)
			}
		}(i)
	}
	clients.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	asyncStarted.Wait()
	assert.Equal(t, int64(n), asyncCompleted.Load(), "every async goroutine must complete")
}

// TestIntegration_ShutdownUnderLoad regression-guards the "Add at zero after
// Wait" race we fixed in Shutdown. Fires requests continuously, calls
// Shutdown mid-flight, and re-runs the cycle many times to catch any
// timing-sensitive leak. Must stay clean under -race.
func TestIntegration_ShutdownUnderLoad(t *testing.T) {
	const cycles = 5
	for cycle := 0; cycle < cycles; cycle++ {
		t.Run(fmt.Sprintf("cycle-%d", cycle), func(t *testing.T) {
			nc := setupNATS(t)
			r := New(nc, "integration-shutdown")

			var completed atomic.Int64
			started := make(chan struct{})
			var startOnce sync.Once
			Register(r, "load.{id}",
				func(c *Context, req echoReq) (*echoResp, error) {
					startOnce.Do(func() { close(started) })
					time.Sleep(time.Duration(1+req.Seq%7) * time.Millisecond)
					completed.Add(1)
					return &echoResp{Seq: req.Seq}, nil
				})

			const inflight = 150
			var clientsWG sync.WaitGroup
			clientsWG.Add(inflight)
			for i := 0; i < inflight; i++ {
				go func(i int) {
					defer clientsWG.Done()
					data, _ := json.Marshal(echoReq{Seq: i})
					// Intentionally ignore: Shutdown will time some of these out.
					_, _ = nc.Request(context.Background(), fmt.Sprintf("load.%d", i), data, 5*time.Second)
				}(i)
			}

			// Deterministic gate: wait until the first handler is actually
			// running before calling Shutdown, so the regression check
			// ("Shutdown blocks while handlers are in-flight") is exercised
			// without depending on a fragile sleep on slow CI runners.
			<-started

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, r.Shutdown(ctx))

			clientsWG.Wait()
			t.Logf("cycle %d completed %d/%d handlers", cycle, completed.Load(), inflight)
			assert.Greater(t, completed.Load(), int64(0), "at least some handlers must run")
		})
	}
}

// TestIntegration_BusyReplyOnSaturation verifies that requests arriving
// while the per-pod concurrency cap is exhausted receive an ErrUnavailable
// reply rather than blocking.
func TestIntegration_BusyReplyOnSaturation(t *testing.T) {
	nc := setupNATS(t)
	r := New(nc, "integration-busy", WithMaxConcurrency(1))

	gate := make(chan struct{})
	// Safety net: if any assertion below fails before we close the gate,
	// the spawned client and handler goroutines would block on `<-gate`
	// forever (bounded only by nc.Request's 5s timeout). This idempotent
	// closer guarantees release on every test exit path.
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()

	// Synchronize on real handler entry instead of polling: the handler
	// signals `entered` before blocking on `gate`, so the busy-reply poll
	// only starts once the slot is genuinely held.
	entered := make(chan struct{}, 1)
	Register(r, "busy.{id}",
		func(c *Context, req echoReq) (*echoResp, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-gate
			return &echoResp{Seq: req.Seq}, nil
		})

	// First request occupies the only slot.
	first := make(chan struct {
		resp []byte
		err  error
	}, 1)
	go func() {
		data, _ := json.Marshal(echoReq{Seq: 1})
		resp, err := nc.Request(context.Background(), "busy.1", data, 5*time.Second)
		var b []byte
		if resp != nil {
			b = resp.Data
		}
		first <- struct {
			resp []byte
			err  error
		}{b, err}
	}()

	// Wait for handler to actually be in the gate before polling for busy.
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first handler never entered the chain")
	}

	// A second request must now get busy because the slot is held.
	data, _ := json.Marshal(echoReq{Seq: 2})
	resp, err := nc.Request(context.Background(), "busy.2", data, 2*time.Second)
	require.NoError(t, err)
	var re RouteError
	require.NoError(t, json.Unmarshal(resp.Data, &re))
	assert.Equal(t, CodeUnavailable, re.Code, "expected busy reply once slot is held")

	// Release the gate; first request must complete normally.
	close(gate)
	got := <-first
	require.NoError(t, got.err)
	var ok echoResp
	require.NoError(t, json.Unmarshal(got.resp, &ok))
	assert.Equal(t, 1, ok.Seq)
}

// TestIntegration_SpawnSitePanicBackstop verifies that a handler panic
// without Recovery middleware is caught by the spawn-site backstop:
// the process survives, the caller receives an "internal error" reply,
// and subsequent requests still work (semaphore slot released, WG
// decremented).
func TestIntegration_SpawnSitePanicBackstop(t *testing.T) {
	nc := setupNATS(t)
	// Note: NO Recovery middleware installed. We're testing the spawn-site
	// backstop, not the middleware path.
	//
	// MaxConcurrency=1 is load-bearing: with cap=1, a leaked semaphore slot
	// would block every subsequent request. cap=2 (or higher) would let
	// the follow-up "ok" request acquire a slot even if cleanup were
	// broken, masking the regression. cap=1 forces the test to actually
	// observe slot release.
	r := New(nc, "integration-panic-backstop", WithMaxConcurrency(1))

	Register(r, "boom.{id}",
		func(c *Context, req echoReq) (*echoResp, error) {
			panic("intentional handler panic")
		})

	// Panicking request must receive a reply (not time out) and the
	// reply must indicate an error.
	data, _ := json.Marshal(echoReq{Seq: 1})
	resp, err := nc.Request(context.Background(), "boom.1", data, 5*time.Second)
	require.NoError(t, err, "panicking handler should still produce a reply via backstop")

	var payload struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(resp.Data, &payload))
	assert.Equal(t, "internal error", payload.Error, "expected internal error reply from backstop")

	// Process survived: a follow-up normal request must succeed.
	Register(r, "ok.{id}",
		func(c *Context, req echoReq) (*echoResp, error) {
			return &echoResp{Seq: req.Seq}, nil
		})
	data, _ = json.Marshal(echoReq{Seq: 2})
	resp, err = nc.Request(context.Background(), "ok.42", data, 5*time.Second)
	require.NoError(t, err)
	var ok echoResp
	require.NoError(t, json.Unmarshal(resp.Data, &ok))
	assert.Equal(t, 2, ok.Seq)
}

// TestIntegration_ShutdownWaitsForSpawnedHandlers verifies that Shutdown
// blocks until handler goroutines (spawned by the semaphore admission
// model) have returned, not merely until the dispatcher has stopped.
func TestIntegration_ShutdownWaitsForSpawnedHandlers(t *testing.T) {
	nc := setupNATS(t)
	r := New(nc, "integration-shutdown-wg", WithMaxConcurrency(8))

	gate := make(chan struct{})
	// Safety net: any test failure before close(gate) below would pin
	// the spawned client goroutines and the gated handler goroutines
	// for up to nc.Request's 5s timeout. This idempotent closer
	// guarantees release on every exit path (success, t.Fatal, or
	// require failure).
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	var entered atomic.Int64
	var completed atomic.Int64
	Register(r, "wg.{id}",
		func(c *Context, req echoReq) (*echoResp, error) {
			entered.Add(1)
			<-gate
			completed.Add(1)
			return &echoResp{Seq: req.Seq}, nil
		})

	const inflight = 4
	var clientsWG sync.WaitGroup
	reqErrCh := make(chan error, inflight)
	for i := 0; i < inflight; i++ {
		clientsWG.Add(1)
		go func(i int) {
			defer clientsWG.Done()
			data, _ := json.Marshal(echoReq{Seq: i})
			if _, err := nc.Request(context.Background(), fmt.Sprintf("wg.%d", i), data, 5*time.Second); err != nil {
				reqErrCh <- fmt.Errorf("wg.%d request failed: %w", i, err)
			}
		}(i)
	}

	// Synchronise on a real signal: every handler increments `entered` on
	// arrival and then blocks on `gate`. Once entered==inflight, all four
	// goroutines are inside the chain and Shutdown will have to wait on
	// the WaitGroup for them.
	require.Eventually(t, func() bool {
		return entered.Load() == int64(inflight)
	}, 5*time.Second, 20*time.Millisecond, "all %d handlers must enter before Shutdown is called", inflight)

	// Shutdown in a goroutine; it must NOT return before we close gate.
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownDone <- r.Shutdown(ctx)
	}()

	// Give Shutdown 200ms to (incorrectly) return early.
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before handlers completed: err=%v", err)
	case <-time.After(200 * time.Millisecond):
		// expected — Shutdown is still blocked on the WaitGroup.
	}

	close(gate)

	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return after handlers completed")
	}
	assert.Equal(t, int64(inflight), completed.Load(), "every gated handler must complete")

	// Join client goroutines and surface any nc.Request errors.
	clientsWG.Wait()
	close(reqErrCh)
	for err := range reqErrCh {
		require.NoError(t, err)
	}
}

// TestIntegration_MultipleRouterInstances simulates multiple service pods
// sharing a queue group. Ensures:
//   - requests load-balance across instances (queue group semantics),
//   - shutting down one instance leaves the others serving,
//   - Shutdown on one instance does not disturb the others.
func TestIntegration_MultipleRouterInstances(t *testing.T) {
	nc := setupNATS(t)

	const queue = "integration-queue-group"
	const instances = 3

	routers := make([]*Router, instances)
	hits := make([]atomic.Int64, instances)
	for idx := 0; idx < instances; idx++ {
		idx := idx
		r := New(nc, queue)
		Register(r, "qg.work.{id}",
			func(c *Context, req echoReq) (*echoResp, error) {
				hits[idx].Add(1)
				return &echoResp{Seq: req.Seq}, nil
			})
		routers[idx] = r
	}

	// Warm up: fire enough requests that each instance should get some work.
	const warmup = 300
	for i := 0; i < warmup; i++ {
		data, _ := json.Marshal(echoReq{Seq: i})
		_, err := nc.Request(context.Background(), fmt.Sprintf("qg.work.%d", i), data, 5*time.Second)
		require.NoError(t, err)
	}
	for idx := 0; idx < instances; idx++ {
		assert.Greater(t, hits[idx].Load(), int64(0),
			"queue group must distribute work to instance %d", idx)
	}
	totalAfterWarmup := int64(0)
	for idx := 0; idx < instances; idx++ {
		totalAfterWarmup += hits[idx].Load()
	}
	require.Equal(t, int64(warmup), totalAfterWarmup, "every request must be answered exactly once")

	// Each Shutdown call gets its own deadline. Reusing one ticking context
	// would mean the cleanup loop could see an already-expired ctx after
	// the warmup-shutdown + 100 sequential RPCs above.
	shutdown := func(r *Router) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require.NoError(t, r.Shutdown(ctx))
	}

	// Shutdown the first instance; the others must continue serving cleanly.
	shutdown(routers[0])
	// Shutdown guarantees routers[0]'s dispatch goroutine has exited, so
	// hits[0] cannot grow from here on. Sample it before any more traffic.
	hitsAt0AfterShutdown := hits[0].Load()

	const postShutdown = 100
	for i := 0; i < postShutdown; i++ {
		data, _ := json.Marshal(echoReq{Seq: i})
		_, err := nc.Request(context.Background(), fmt.Sprintf("qg.work.%d", warmup+i), data, 5*time.Second)
		require.NoError(t, err, "remaining instances must keep serving after one shuts down")
	}

	assert.Equal(t, hitsAt0AfterShutdown, hits[0].Load(),
		"shutdown instance must not receive any more traffic")

	// Clean up the rest.
	for idx := 1; idx < instances; idx++ {
		shutdown(routers[idx])
	}
}
