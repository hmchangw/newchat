//go:build integration

package mongoutil

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

// outageProxy models a MongoDB endpoint that is down and later comes back.
// The address is reserved up front but nothing listens on it until Restore is
// called, so the driver sees connection-refused — what a pod restarting into a
// live outage actually sees — and then a healthy endpoint at the same address.
type outageProxy struct {
	addr   string
	target string

	mu       sync.Mutex
	listener net.Listener
	wg       sync.WaitGroup
	conns    []net.Conn
}

// newOutageProxy reserves a local address without serving on it.
func newOutageProxy(t *testing.T, target string) *outageProxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	p := &outageProxy{addr: addr, target: target}
	t.Cleanup(p.Close)
	return p
}

// Restore starts forwarding the reserved address to the real MongoDB, i.e. the
// outage ends. Binding the just-released port can race with the OS, so retry
// briefly rather than flake.
func (p *outageProxy) Restore(t *testing.T) {
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
			p.mu.Lock()
			p.conns = append(p.conns, conn)
			p.mu.Unlock()
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				p.forward(conn)
			}()
		}
	}()
}

func (p *outageProxy) forward(client net.Conn) {
	upstream, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, upstream)
	p.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = io.Copy(upstream, client) }()
	go func() { defer wg.Done(); _, _ = io.Copy(client, upstream) }()
	wg.Wait()
	_ = client.Close()
	_ = upstream.Close()
}

func (p *outageProxy) Close() {
	p.mu.Lock()
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

// hostPort extracts host:port from a mongodb:// URI so the proxy can forward
// to the shared test container.
func hostPort(t *testing.T, uri string) string {
	t.Helper()
	u, err := url.Parse(uri)
	require.NoError(t, err)
	require.NotEmpty(t, u.Host, "parse host from %q", uri)
	return u.Host
}

// TestConnect_LazyBootsDuringOutageThenRecovers is the scenario this option
// exists for: a process starts while MongoDB is unreachable, serves (failing)
// requests, and then works normally once MongoDB returns — all without a
// restart. The default ping path cannot start at all here.
func TestConnect_LazyBootsDuringOutageThenRecovers(t *testing.T) {
	proxy := newOutageProxy(t, hostPort(t, testutil.MongoURI(t)))
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true", proxy.addr)

	// Boot during the outage: this is the step that currently kills every service.
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer bootCancel()
	client, err := Connect(bootCtx, uri, "", "", WithLazyConnect())
	require.NoError(t, err, "lazy Connect must boot while MongoDB is unreachable")
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	coll := client.Database("mongoutil_lazy_recovery_test").Collection("docs")
	t.Cleanup(func() {
		_ = client.Database("mongoutil_lazy_recovery_test").Drop(context.Background())
	})

	// While down, operations fail — bounded, not hanging.
	downCtx, downCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer downCancel()
	start := time.Now()
	_, err = coll.InsertOne(downCtx, bson.M{"_id": "during-outage"})
	require.Error(t, err, "operations must fail while MongoDB is unreachable")
	assert.Less(t, time.Since(start), 5*time.Second, "must fail bounded, not hang")

	// MongoDB comes back. The same client must recover with no restart.
	proxy.Restore(t)

	upCtx, upCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer upCancel()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := coll.InsertOne(upCtx, bson.M{"_id": "after-recovery"})
		assert.NoError(c, err)
	}, 25*time.Second, 250*time.Millisecond, "lazy client must recover once MongoDB returns")

	n, err := coll.CountDocuments(upCtx, bson.M{"_id": "after-recovery"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}

// TestConnect_LazyAgainstHealthyMongoWorksImmediately confirms skipping the
// ping costs nothing in the normal case — the usual startup, just unproven.
func TestConnect_LazyAgainstHealthyMongoWorksImmediately(t *testing.T) {
	ctx := context.Background()
	client, err := Connect(ctx, testutil.MongoURI(t), "", "", WithLazyConnect())
	require.NoError(t, err)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	db := client.Database("mongoutil_lazy_healthy_test")
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	opCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err = db.Collection("docs").InsertOne(opCtx, bson.M{"_id": "x"})
	require.NoError(t, err)
	n, err := db.Collection("docs").CountDocuments(opCtx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}

// TestConnect_DefaultStillFailsDuringOutage pins the default: batch and CLI
// jobs must keep dying loudly at startup when MongoDB is missing.
func TestConnect_DefaultStillFailsDuringOutage(t *testing.T) {
	proxy := newOutageProxy(t, hostPort(t, testutil.MongoURI(t)))
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true", proxy.addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Connect(ctx, uri, "", "")
	require.Error(t, err, "default Connect must still fail when MongoDB is unreachable")
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "mongo ping")
}
