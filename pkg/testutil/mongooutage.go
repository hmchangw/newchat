//go:build integration

package testutil

import (
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MongoOutage fakes a MongoDB that is down and later comes back, so a test can
// check what a service does during an outage.
//
// It hands out an address where nothing is listening, so connections are
// refused — the same thing a service sees when MongoDB is really down. Calling
// Restore then starts listening on that address and passes traffic through to
// the real MongoDB, which looks to the client like the database coming back.
//
// Why fake it instead of stopping MongoDB: every test in the package shares one
// MongoDB container, so stopping it would break all of them. This gives one
// test its own outage without touching anything the others use.
//
// Why an address with nothing listening, rather than something that accepts the
// connection and hangs up: only a dead address gets connections refused.
// Anything that accepts looks like a working server to the driver, which is a
// different failure from an outage.
type MongoOutage struct {
	addr   string
	target string

	mu       sync.Mutex
	listener net.Listener
	wg       sync.WaitGroup
	conns    []net.Conn
	closed   bool
}

// NewMongoOutage reserves an address that refuses connections and, once
// Restore runs, forwards it to the MongoDB behind targetURI.
func NewMongoOutage(t *testing.T, targetURI string) *MongoOutage {
	t.Helper()
	u, err := url.Parse(targetURI)
	require.NoError(t, err, "parse target URI %q", targetURI)
	require.NotEmpty(t, u.Host, "parse host from %q", targetURI)

	p := &MongoOutage{addr: ReserveAddr(t), target: u.Host}
	t.Cleanup(p.Close)
	return p
}

// URI is the address to point a MongoDB client at. Connections to it are
// refused until Restore runs.
//
// Both settings in the URI matter, despite looking like boilerplate.
// directConnection=true stops the driver asking MongoDB what address it should
// really use — without it the driver gets the real container's address and goes
// straight there, skipping the fake one, and the test proves nothing.
// serverSelectionTimeoutMS shortens how long the driver waits before giving up,
// so a test takes 2 seconds instead of the default 30, while still leaving a
// slow machine enough time to notice the database is back after Restore.
func (p *MongoOutage) URI() string {
	return fmt.Sprintf("mongodb://%s/?directConnection=true&serverSelectionTimeoutMS=2000", p.addr)
}

// track records a socket so Close can shut it down later, or closes it straight
// away and returns false if Close has already run. Without that check, a
// connection arriving during shutdown would never be closed, and Close would
// wait for it forever.
func (p *MongoOutage) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = c.Close()
		return false
	}
	p.conns = append(p.conns, c)
	return true
}

// Restore starts passing traffic through to the real MongoDB — the outage ends.
// The client is never told; it has to notice by itself, which is the whole
// point of the test.
//
// The retry loop is needed rather than defensive: we deliberately gave the port
// up so connections would be refused, so the operating system may not hand it
// straight back.
func (p *MongoOutage) Restore(t *testing.T) {
	t.Helper()
	var l net.Listener
	var err error
	for range 50 {
		l, err = net.Listen("tcp", p.addr)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, err, "bind outage proxy at %s", p.addr)

	p.mu.Lock()
	p.listener = l
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return // listener closed
			}
			if !p.track(conn) {
				return // teardown started
			}
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				p.forward(conn)
			}()
		}
	}()
}

// forward passes one client connection through to MongoDB and back. Both
// directions have to finish before either side is closed: closing early cuts
// the other direction off mid-message, and the client would see a broken
// connection instead of a working database — looking like the outage never
// ended.
func (p *MongoOutage) forward(client net.Conn) {
	upstream, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	if !p.track(upstream) {
		_ = client.Close()
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = io.Copy(upstream, client) }()
	go func() { defer wg.Done(); _, _ = io.Copy(client, upstream) }()
	wg.Wait()
	_ = client.Close()
	_ = upstream.Close()
}

// Close tears the proxy down. Registered by NewMongoOutage; safe to call twice.
func (p *MongoOutage) Close() {
	p.mu.Lock()
	p.closed = true
	if p.listener != nil {
		_ = p.listener.Close()
		p.listener = nil
	}
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
	p.mu.Unlock()
	p.wg.Wait()
}
