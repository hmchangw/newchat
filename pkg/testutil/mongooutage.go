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

// MongoOutage models a MongoDB endpoint that is down and later comes back. The
// address is reserved up front but nothing listens on it until Restore is
// called, so the driver sees connection-refused — what a pod restarting into a
// live outage actually sees — and then a healthy endpoint at the same address.
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

// URI is the mongodb:// URI to point a client at. It refuses connections until
// Restore runs. The shortened serverSelectionTimeoutMS keeps the down paths
// failing in test time rather than at the driver's 30s default, while leaving
// enough budget for a loaded runner to rediscover the server after Restore.
func (p *MongoOutage) URI() string {
	return fmt.Sprintf("mongodb://%s/?directConnection=true&serverSelectionTimeoutMS=2000", p.addr)
}

// track registers a socket for teardown, or closes it immediately and reports
// false if Close already ran — otherwise a connection accepted during teardown
// is never closed and its io.Copy pins Close's wg.Wait forever.
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

// Restore starts forwarding the reserved address to the real MongoDB, i.e. the
// outage ends. Binding the just-released port can race with the OS, so retry
// briefly rather than flake.
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
