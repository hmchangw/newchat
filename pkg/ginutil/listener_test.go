package ginutil

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listenLocal returns a listener on a free loopback port, closed on cleanup.
func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

func TestLimitListener_AcceptsUpToTheLimit(t *testing.T) {
	ln := LimitListener(listenLocal(t), 2)

	var accepted []net.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 2 {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted = append(accepted, c)
		}
	}()

	var dialed []net.Conn
	for range 2 {
		c, err := net.Dial("tcp", ln.Addr().String())
		require.NoError(t, err)
		dialed = append(dialed, c)
	}
	wg.Wait()

	assert.Len(t, accepted, 2)
	for _, c := range append(dialed, accepted...) {
		_ = c.Close()
	}
}

// The reason this listener exists rather than a bare semaphore: Shutdown closes
// the listener while Accept is parked at capacity, and that must not deadlock
// the drain.
func TestLimitListener_CloseUnblocksAcceptAtCapacity(t *testing.T) {
	ln := LimitListener(listenLocal(t), 1)

	held, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer func() { _ = held.Close() }()
	first, err := ln.Accept()
	require.NoError(t, err)
	defer func() { _ = first.Close() }()

	// The slot is taken, so this Accept parks acquiring the semaphore.
	returned := make(chan error, 1)
	go func() {
		_, err := ln.Accept()
		returned <- err
	}()

	require.NoError(t, ln.Close())

	select {
	case err := <-returned:
		assert.Error(t, err, "a parked Accept must return once the listener closes")
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock an Accept parked at capacity")
	}
}

func TestLimitListener_ClosingAConnFreesItsSlot(t *testing.T) {
	ln := LimitListener(listenLocal(t), 1)

	d1, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	c1, err := ln.Accept()
	require.NoError(t, err)
	require.NoError(t, c1.Close())
	_ = d1.Close()

	d2, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer func() { _ = d2.Close() }()

	accepted := make(chan struct{})
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_ = c.Close()
			close(accepted)
		}
	}()

	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("the slot was not released when the first connection closed")
	}
}

func TestLimitListener_DoubleCloseIsSafe(t *testing.T) {
	ln := LimitListener(listenLocal(t), 1)

	require.NoError(t, ln.Close())
	assert.NotPanics(t, func() { _ = ln.Close() })
}
