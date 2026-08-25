package ginutil

import (
	"net"
	"sync"
	"sync/atomic"
)

// LimitListener wraps l to accept at most n simultaneous connections. The
// handler limiter only counts requests that reached Gin, so without this a
// client trickling headers, or holding idle keep-alives, grows goroutines and
// buffers independently of it.
// n <= 0 returns l unwrapped, as MaxConcurrency and Timeout do, so callers need
// no guard of their own.
func LimitListener(l net.Listener, n int) net.Listener {
	if n <= 0 {
		return l
	}
	return &limitListener{Listener: l, sem: make(chan struct{}, n), done: make(chan struct{})}
}

type limitListener struct {
	net.Listener
	sem       chan struct{}
	closeOnce sync.Once
	done      chan struct{} // closed by Close; never sent on
}

// acquire takes a slot, or reports false once Close signals done. Without that
// select a parked Accept outlives Close and deadlocks the drain.
func (l *limitListener) acquire() bool {
	select {
	case <-l.done:
		return false
	case l.sem <- struct{}{}:
		return true
	}
}

func (l *limitListener) release() { <-l.sem }

func (l *limitListener) Accept() (net.Conn, error) {
	if !l.acquire() {
		// Close shuts the underlying listener before it signals done, so there is
		// nothing left to accept by the time acquire can fail.
		return nil, net.ErrClosed
	}
	c, err := l.Listener.Accept()
	if err != nil {
		l.release()
		return nil, err
	}
	return &limitListenerConn{Conn: c, l: l}, nil
}

func (l *limitListener) Close() error {
	err := l.Listener.Close()
	l.closeOnce.Do(func() { close(l.done) })
	return err
}

// limitListenerConn returns its slot once, however many times it is closed.
// Holds the listener rather than a release closure: a method value would be a
// second allocation on every accepted connection.
type limitListenerConn struct {
	net.Conn
	l        *limitListener
	released atomic.Bool
}

func (c *limitListenerConn) Close() error {
	err := c.Conn.Close()
	if c.released.CompareAndSwap(false, true) {
		c.l.release()
	}
	return err
}
