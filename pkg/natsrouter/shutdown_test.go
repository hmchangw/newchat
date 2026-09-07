package natsrouter

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)

// TestShutdown_StopsAcceptingNewRequests verifies that after Shutdown returns,
// requests to routes registered through the router time out — the subscriptions
// have been drained so the NATS server no longer routes to this instance.
func TestRouter_Shutdown_StopsAcceptingNewRequests(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-shutdown")

	Register(r, "test.shutdown.{id}", natsmetrics.MethodGetSettings,
		func(c *Context, req testReq) (*testResp, error) {
			return &testResp{Greeting: "hi"}, nil
		})

	// Baseline: request succeeds.
	data, _ := json.Marshal(testReq{Name: "first"})
	_, err := nc.Request(context.Background(), "test.shutdown.1", data, 2*time.Second)
	require.NoError(t, err)

	// Shutdown the router (context with generous deadline — no in-flight work).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, r.Shutdown(shutdownCtx))

	// Post-shutdown: request must NOT be answered by this router.
	_, err = nc.Request(context.Background(), "test.shutdown.2", data, 200*time.Millisecond)
	require.Error(t, err, "router must stop answering after Shutdown")
}

// TestShutdown_WaitsForInflightHandlers proves Shutdown blocks until a
// handler currently running returns, rather than returning immediately.
func TestRouter_Shutdown_WaitsForInflightHandlers(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-shutdown-inflight")

	handlerReached := make(chan struct{})
	release := make(chan struct{})
	handlerFinished := make(chan struct{})

	Register(r, "test.slow.{id}", natsmetrics.MethodSetSettings,
		func(c *Context, req testReq) (*testResp, error) {
			close(handlerReached)
			<-release
			close(handlerFinished)
			return &testResp{Greeting: "done"}, nil
		})

	// Kick off a request; do NOT wait for it here — it will be blocked in the handler.
	go func() {
		data, _ := json.Marshal(testReq{Name: "x"})
		_, _ = nc.Request(context.Background(), "test.slow.1", data, 5*time.Second)
	}()

	// Wait until the handler is actually running.
	select {
	case <-handlerReached:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never entered")
	}

	// Shutdown must wait for the in-flight handler.
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		shutdownDone <- r.Shutdown(ctx)
	}()

	// Shutdown should still be running since the handler is blocked.
	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned before in-flight handler finished")
	case <-time.After(100 * time.Millisecond):
	}

	// Release the handler.
	close(release)

	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not return after handler finished")
	}

	// Handler actually ran to completion.
	select {
	case <-handlerFinished:
	default:
		t.Fatal("handler did not finish")
	}
}

// TestShutdown_RespectsContextDeadline proves Shutdown returns a context
// error when a handler is still running past ctx's deadline.
func TestRouter_Shutdown_RespectsContextDeadline(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-shutdown-deadline")

	handlerReached := make(chan struct{})
	done := make(chan struct{})
	defer close(done) // unblock the handler when test ends

	Register(r, "test.stuck.{id}", natsmetrics.MethodListPriorityContacts,
		func(c *Context, req testReq) (*testResp, error) {
			close(handlerReached)
			<-done
			return &testResp{}, nil
		})

	go func() {
		data, _ := json.Marshal(testReq{})
		_, _ = nc.Request(context.Background(), "test.stuck.1", data, 5*time.Second)
	}()

	<-handlerReached

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := r.Shutdown(ctx)
	require.Error(t, err, "Shutdown must return ctx error when handlers outlast deadline")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestShutdown_Idempotent verifies calling Shutdown twice is safe.
func TestRouter_Shutdown_Idempotent(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-shutdown-idempotent")
	Register(r, "test.idem.{id}", natsmetrics.MethodAddPriorityContact,
		func(c *Context, req testReq) (*testResp, error) {
			return &testResp{}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, r.Shutdown(ctx))
	require.NoError(t, r.Shutdown(ctx))
}

// testMethods supplies one distinct declared method per concurrently registered
// route below. The router does not reject a reused constant — it logs and
// registers both — but reusing one here would collapse the routes into a
// single Routes() line and make the test's own bookkeeping ambiguous.
var testMethods = []natsmetrics.RPCMethod{
	natsmetrics.MethodSearchMessages, natsmetrics.MethodSearchRooms,
	natsmetrics.MethodSearchApps, natsmetrics.MethodSearchUsers,
	natsmetrics.MethodSearchOrgs, natsmetrics.MethodListEmojis,
	natsmetrics.MethodDeleteEmoji, natsmetrics.MethodTranslateText,
	natsmetrics.MethodCreateDMRoom, natsmetrics.MethodSetManualPresence,
	natsmetrics.MethodBatchGetPresence, natsmetrics.MethodBatchGetPeerPresence,
	natsmetrics.MethodCreateBotRoom, natsmetrics.MethodAddBotRoomMembers,
	natsmetrics.MethodRemoveBotRoomMembers, natsmetrics.MethodGetBotRoom,
	natsmetrics.MethodEnsureBotDMRoom, natsmetrics.MethodSendRoomMessage,
	natsmetrics.MethodSendDM, natsmetrics.MethodGetPresenceSnapshot,
}

// TestShutdown_ConcurrentRegistrationSafe verifies addRoute is safe to run
// alongside other addRoute calls (belt-and-suspenders for the subs slice
// even though registration is normally startup-only).
func TestRouter_Shutdown_ConcurrentRegistrationSafe(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-shutdown-concurrent")

	var wg sync.WaitGroup
	for i := range testMethods {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			RegisterNoBody(r, subjectf(i), testMethods[i],
				func(c *Context) (*testResp, error) { return &testResp{}, nil })
		}(i)
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, r.Shutdown(ctx))
}

// TestRouter_Shutdown_StressUnderLoad pummels the router with 200 concurrent
// requests, calls Shutdown once the first handler is confirmed running, and
// asserts the race detector finds nothing.
func TestRouter_Shutdown_StressUnderLoad(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "stress-shutdown")

	firstEntered := make(chan struct{})
	var once sync.Once
	Register(r, "stress.{id}", natsmetrics.MethodRemovePriorityContact,
		func(c *Context, req testReq) (*testResp, error) {
			once.Do(func() { close(firstEntered) })
			time.Sleep(time.Duration(1+req.Name[0]%5) * time.Millisecond)
			return &testResp{}, nil
		})

	const inflight = 200
	var clientWG sync.WaitGroup
	clientWG.Add(inflight)
	for i := 0; i < inflight; i++ {
		go func(i int) {
			defer clientWG.Done()
			data, _ := json.Marshal(testReq{Name: string(rune('a' + i%26))})
			// Intentionally ignore: Shutdown will time some of these out.
			_, _ = nc.Request(context.Background(), "stress.x", data, 5*time.Second)
		}(i)
	}

	// Deterministic gate — wait until at least one handler is running
	// before calling Shutdown so the race window is exercised regardless
	// of CI load.
	<-firstEntered

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, r.Shutdown(ctx))
	clientWG.Wait()
}

func subjectf(i int) string {
	// #nosec G115 -- inputs bounded by %26 and /26 within a fixed test loop; cannot overflow rune
	return "test.concurrent.reg." + string(rune('a'+i%26)) + "." + string(rune('0'+i/26))
}
