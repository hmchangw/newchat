package shutdown_test

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/shutdown"
)

func TestWaitCallsCleanup(t *testing.T) {
	skipOnWindows(t)
	called := false
	cleanup := func(ctx context.Context) error {
		called = true
		return nil
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		signalSelf(t)
	}()

	shutdown.Wait(context.Background(), 5*time.Second, cleanup)

	if !called {
		t.Error("cleanup function was not called")
	}
}

func TestWaitTimesOutWhenCleanupHangs(t *testing.T) {
	skipOnWindows(t)
	hangingCleanup := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		signalSelf(t)
	}()

	start := time.Now()
	shutdown.Wait(context.Background(), 500*time.Millisecond, hangingCleanup)
	elapsed := time.Since(start)

	if elapsed > 1*time.Second {
		t.Errorf("expected shutdown to complete within ~500ms timeout, took %v", elapsed)
	}
}

func TestWaitCompletesBeforeTimeout(t *testing.T) {
	skipOnWindows(t)
	var order []string
	first := func(ctx context.Context) error {
		order = append(order, "first")
		return nil
	}
	second := func(ctx context.Context) error {
		order = append(order, "second")
		return nil
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		signalSelf(t)
	}()

	shutdown.Wait(context.Background(), 5*time.Second, first, second)

	assert.Equal(t, []string{"first", "second"}, order)
}

// A service that can raise its OWN shutdown signal — a worker whose consume
// loop died, say — races Wait: the signal can be raised before Wait reaches
// signal.Notify, and until that call runs SIGTERM still has its default
// disposition and kills the process outright. WaitOn takes an already-armed
// channel so the caller can register the handler before starting anything able
// to raise it; a signal that lands early then sits in the buffer instead of
// being lost or fatal.
func TestWaitOnConsumesASignalRaisedBeforeItIsCalled(t *testing.T) {
	skipOnWindows(t)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sig)

	// Raised well before WaitOn — exactly the ordering that kills the process
	// when the handler is registered inside the wait.
	signalSelf(t)
	assert.Eventually(t, func() bool { return len(sig) == 1 }, time.Second, 5*time.Millisecond,
		"the pre-armed handler must have latched the signal")

	called := false
	done := make(chan struct{})
	go func() {
		defer close(done)
		shutdown.WaitOn(context.Background(), sig, 5*time.Second,
			func(context.Context) error { called = true; return nil })
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WaitOn ignored the signal that was already buffered")
	}
	assert.True(t, called, "cleanup must run for a signal raised before the wait began")
}

// Wait must keep behaving exactly as before — it is what every other service
// calls, and it arms its own handler.
func TestWaitOnStillBlocksUntilSignalled(t *testing.T) {
	skipOnWindows(t)
	sig := make(chan os.Signal, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		shutdown.WaitOn(context.Background(), sig, 5*time.Second,
			func(context.Context) error { return nil })
	}()

	select {
	case <-done:
		t.Fatal("WaitOn returned without a signal")
	case <-time.After(100 * time.Millisecond):
	}

	sig <- syscall.SIGTERM
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WaitOn did not return after the signal")
	}
}

func skipOnWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("signal delivery test uses syscall.Kill, which is Unix-only")
	}
}

func signalSelf(t *testing.T) {
	t.Helper()
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Errorf("find self: %v", err)
		return
	}
	if err := p.Signal(os.Interrupt); err != nil {
		t.Errorf("signal self: %v", err)
	}
}

// Signals must return a channel that is already armed and buffered, so a
// worker can arm it BEFORE starting a loop able to raise SIGTERM on itself: an
// early signal is latched in the buffer instead of killing the process.
func TestSignals_LatchesASignalRaisedBeforeWaitOn(t *testing.T) {
	sig := shutdown.Signals()
	defer signal.Stop(sig)

	assert.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))

	select {
	case got := <-sig:
		assert.Equal(t, syscall.SIGTERM, got)
	case <-time.After(2 * time.Second):
		t.Fatal("Signals must deliver a SIGTERM raised after it was armed")
	}
}
