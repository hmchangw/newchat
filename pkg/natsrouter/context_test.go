package natsrouter

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/errcode"
)

// TestContext_WithLogValues_NoCycleAndEnriches verifies the seam derives from
// the inner ctx (no Value-delegation cycle) and that the attached attrs reach
// the centralized Classify log line.
func TestContext_WithLogValues_NoCycleAndEnriches(t *testing.T) {
	var buf bytes.Buffer
	c := NewContext(map[string]string{})
	c.SetContext(errcode.WithLogger(c.ctx, slog.New(slog.NewJSONHandler(&buf, nil))))

	c.WithLogValues("account", "alice") // must not hang (no ctx cycle)
	_ = c.Value("anything")             // a lookup must terminate (would loop on a cycle)

	errcode.Classify(c, errors.New("boom"))
	if !strings.Contains(buf.String(), "alice") {
		t.Fatalf("log values not applied: %s", buf.String())
	}
}

// TestContext_ConcurrentKeysAccess_NoRace proves that Set and Get are safe to
// call concurrently. Without a mutex, Go's map detector panics on concurrent
// writes and the race detector flags concurrent read/write.
func TestContext_ConcurrentKeysAccess_NoRace(t *testing.T) {
	c := NewContext(nil)
	c.Set("initial", 0)

	const n = 500
	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			c.Set("a"+strconv.Itoa(i), i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			c.Set("b"+strconv.Itoa(i), i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			_, _ = c.Get("a" + strconv.Itoa(i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			_, _ = c.Get("b" + strconv.Itoa(i))
		}
	}()
	wg.Wait()
}

// TestContext_StableCtxAcrossPoolReuse is the critical race guard.
//
// Scenario: a background goroutine — modelling a net/http.Transport
// keep-alive cancellation watcher — holds a *Context from request #1 and
// reads its context.Context methods long after the handler returns and
// subsequent requests have cycled through the pool.
//
// With the split-struct design (Context header is fresh per request, only
// the scratchpad is pooled), the ctx field on the first *Context is set
// once and never mutated, so the goroutine's reads never race with pool
// reuse. Run with -race.
func TestContext_StableCtxAcrossPoolReuse(t *testing.T) {
	reqCtx := context.Background()
	c1 := acquireContext(reqCtx, nil, NewParams(map[string]string{"req": "1"}), nil, nil)
	c1.Set("id", "alice")

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = c1.Done()
			_ = c1.Err()
			_, _ = c1.Deadline()
			_ = c1.Value("anything")
		}
	}()

	// Release c1 and churn the pool. Under the old pooled-Context design
	// this would race c1's ctx reads; under the split-struct design each
	// acquire allocates a fresh Context header.
	releaseContext(c1)
	for i := 0; i < 200; i++ {
		c2 := acquireContext(
			context.Background(),
			nil,
			NewParams(map[string]string{"req": strconv.Itoa(i + 2)}),
			nil,
			nil,
		)
		c2.Set("id", "bob")
		releaseContext(c2)
	}
	close(stop)
	<-done
}

// TestContext_KeysIndependentPerRequest is a regression guard: today only
// chainState is pooled (acquireContext returns a fresh *Context per call),
// so keys set on c1 cannot leak into c2 through any path. This test fails
// fast if someone ever reintroduces pooling of the Context header itself —
// the likely accident given how much of the rest of the struct looks poolable.
func TestContext_KeysIndependentPerRequest(t *testing.T) {
	c1 := acquireContext(context.Background(), nil, Params{}, nil, nil)
	c1.Set("leak", "bad")
	releaseContext(c1)

	c2 := acquireContext(context.Background(), nil, Params{}, nil, nil)
	_, ok := c2.Get("leak")
	assert.False(t, ok, "keys set on a released context must not be visible on the next acquire")
	releaseContext(c2)
}

// TestContext_GetHeader covers all four reachable branches of GetHeader:
// nil Msg (test-context path), nil Header map, key present, key absent.
// NATS headers are case-sensitive, so a key set with one casing is NOT
// reachable via a different casing — covered by an explicit assertion.
func TestContext_GetHeader(t *testing.T) {
	t.Run("nil Msg returns empty string", func(t *testing.T) {
		c := NewContext(nil)
		assert.Equal(t, "", c.GetHeader("X-Request-ID"))
	})

	t.Run("nil Header map returns empty string", func(t *testing.T) {
		c := NewContext(nil)
		c.Msg = &nats.Msg{}
		assert.Equal(t, "", c.GetHeader("X-Request-ID"))
	})

	t.Run("present key returns value", func(t *testing.T) {
		c := NewContext(nil)
		c.Msg = &nats.Msg{Header: nats.Header{}}
		c.Msg.Header.Set("X-Foo", "bar")
		assert.Equal(t, "bar", c.GetHeader("X-Foo"))
	})

	t.Run("absent key returns empty string", func(t *testing.T) {
		c := NewContext(nil)
		c.Msg = &nats.Msg{Header: nats.Header{}}
		c.Msg.Header.Set("X-Foo", "bar")
		assert.Equal(t, "", c.GetHeader("X-Missing"))
	})

	t.Run("case-sensitive lookup miss", func(t *testing.T) {
		// NATS headers are case-sensitive (unlike net/http). A key set
		// with lowercase is NOT reachable via canonical case. This test
		// pins the documented behavior so an accidental future switch
		// to case-insensitive (e.g. via textproto.CanonicalMIMEHeaderKey)
		// would fail loudly here.
		c := NewContext(nil)
		c.Msg = &nats.Msg{Header: nats.Header{}}
		c.Msg.Header.Set("authorization", "token")
		assert.Equal(t, "", c.GetHeader("Authorization"),
			"NATS headers are case-sensitive; lowercase set should not match canonical-case lookup")
		assert.Equal(t, "token", c.GetHeader("authorization"),
			"exact case match must succeed")
	})
}

func TestContext_ReplyJSONUsesResponder(t *testing.T) {
	reply := &capturingResponder{}
	msg := &nats.Msg{Subject: "chat.test", Reply: "_INBOX.reply"}
	c := acquireContext(context.Background(), msg, Params{}, nil, reply)
	defer releaseContext(c)

	c.ReplyJSON(map[string]string{"ok": "true"})

	assert.True(t, reply.called)
	assert.Same(t, msg, reply.msg)
	assert.JSONEq(t, `{"ok":"true"}`, string(reply.data))
}

// Use-after-release safety: chainState is pooled, so a post-handler goroutine
// calling Next/Abort/IsAborted on a released *Context would silently read the
// next request's chain state. The nil-out + nil-check converts the silent
// corruption into a loud panic.
func TestContext_ChainMethodsPanicAfterRelease(t *testing.T) {
	c := acquireContext(context.Background(), nil, Params{}, []HandlerFunc{func(*Context) {}}, nil)
	releaseContext(c)

	assert.PanicsWithValue(t, chainAfterReleasePanic, func() { c.Next() })
	assert.PanicsWithValue(t, chainAfterReleasePanic, func() { c.Abort() })
	assert.PanicsWithValue(t, chainAfterReleasePanic, func() { c.IsAborted() })
}

type capturingResponder struct {
	called bool
	msg    *nats.Msg
	data   []byte
}

func (r *capturingResponder) Respond(_ context.Context, msg *nats.Msg, data []byte) error {
	r.called = true
	r.msg = msg
	r.data = append([]byte(nil), data...)
	return nil
}
