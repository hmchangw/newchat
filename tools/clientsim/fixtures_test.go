package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
)

// forceExpireForTest backdates the cached expiry. Lives in the test build
// only (CLAUDE.md §4: test helpers never ship in production code).
func (c *jwtCache) forceExpireForTest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expiresAt = time.Now().Add(-time.Minute)
}

type countingMinter struct {
	calls atomic.Int64
	jwt   func() string
	err   error
}

func (c *countingMinter) Mint(context.Context, string, string) (string, error) {
	c.calls.Add(1)
	if c.err != nil {
		return "", c.err
	}
	return c.jwt(), nil
}

// mintTestJWT produces a decodable NATS user JWT with the given expiry,
// signed by a throwaway account key (same pattern as tools/nats-debug).
// mintTestJWT reports failures with Errorf rather than require, because the
// minter closures that call it run on the client's own goroutines, and
// FailNow off the test goroutine is undefined behaviour.
func mintTestJWT(t *testing.T, expires time.Time) string {
	t.Helper()
	accountKey, err := nkeys.CreateAccount()
	if err != nil {
		t.Errorf("create account nkey: %v", err)
		return ""
	}
	userKey, err := nkeys.CreateUser()
	if err != nil {
		t.Errorf("create user nkey: %v", err)
		return ""
	}
	pub, err := userKey.PublicKey()
	if err != nil {
		t.Errorf("user nkey public key: %v", err)
		return ""
	}
	claims := jwt.NewUserClaims(pub)
	claims.Expires = expires.Unix()
	token, err := claims.Encode(accountKey)
	if err != nil {
		t.Errorf("encode user claims: %v", err)
		return ""
	}
	return token
}

func newTestSimClient(t *testing.T, account, mode string, mint minter) *simClient {
	t.Helper()
	cfg := &config{
		NATSWSURL: "ws://127.0.0.1:1", SiteID: "site-a", JWTMode: mode,
		SubPendingMsgs: 512, SubPendingBytes: 1 << 20,
		ReconnectBufBytes: 1 << 16, PingInterval: 2 * time.Minute,
	}
	sc, err := newSimClient(account, cfg, mint, newMetrics())
	require.NoError(t, err)
	return sc
}

type fakeSub struct{ unsubs atomic.Int64 }

func (f *fakeSub) Unsubscribe() error { f.unsubs.Add(1); return nil }

// fakeConn is an in-memory simConn: callback subs are invocable, chan subs
// record their target channel, Request serves canned subscription.list
// pages (optionally gated to simulate a slow RPC).
type fakeConn struct {
	mu       sync.Mutex
	cbSubs   map[string]nats.MsgHandler
	chanSubs map[string]chan *nats.Msg
	subs     map[string]*fakeSub

	pages     []subListPage
	reqCount  atomic.Int64
	reqGate   chan struct{} // when non-nil, Request blocks until it closes
	reqErrors []error       // returned before pages, one error per Request
	// reqEntered receives once per Request that reaches the gate, so a test
	// can prove a walk is in flight (and therefore holding resyncMu) rather
	// than racing the scheduler to guess.
	reqEntered chan struct{}

	subChanErr      error // next SubscribeChan fails with this, then clears
	forceReconnects atomic.Int64
	closes          atomic.Int64
}

func newFakeConn(pages ...subListPage) *fakeConn {
	return &fakeConn{
		cbSubs:   map[string]nats.MsgHandler{},
		chanSubs: map[string]chan *nats.Msg{},
		subs:     map[string]*fakeSub{},
		pages:    pages,
	}
}

func (f *fakeConn) SubscribeCB(subj string, cb nats.MsgHandler) (simSub, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cbSubs[subj] = cb
	sub := &fakeSub{}
	f.subs[subj] = sub
	return sub, nil
}

func (f *fakeConn) SubscribeChan(subj string, ch chan *nats.Msg) (simSub, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subChanErr != nil {
		err := f.subChanErr
		f.subChanErr = nil
		return nil, err
	}
	f.chanSubs[subj] = ch
	sub := &fakeSub{}
	f.subs[subj] = sub
	return sub, nil
}

func (f *fakeConn) Request(ctx context.Context, _ string, _ []byte) (*nats.Msg, error) {
	if f.reqEntered != nil {
		select {
		case f.reqEntered <- struct{}{}:
		default:
		}
	}
	if f.reqGate != nil {
		select {
		case <-f.reqGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	i := int(f.reqCount.Add(1)) - 1
	f.mu.Lock()
	if len(f.reqErrors) > 0 {
		err := f.reqErrors[0]
		f.reqErrors = f.reqErrors[1:]
		f.mu.Unlock()
		return nil, err
	}
	pages := append([]subListPage(nil), f.pages...)
	f.mu.Unlock()
	if len(pages) == 0 {
		return nil, errors.New("fakeConn: no pages configured")
	}
	if i >= len(pages) {
		i = len(pages) - 1
	}
	data, err := json.Marshal(pages[i])
	if err != nil {
		return nil, err
	}
	return &nats.Msg{Data: data}, nil
}

func (f *fakeConn) failNextRequests(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqErrors = append(f.reqErrors, errs...)
}

func (f *fakeConn) ForceReconnect() error { f.forceReconnects.Add(1); return nil }

func (f *fakeConn) Close() { f.closes.Add(1) }

// deliverCB invokes a callback subscription as the broker would.
func (f *fakeConn) deliverCB(t *testing.T, subj string, data []byte) {
	t.Helper()
	f.mu.Lock()
	cb := f.cbSubs[subj]
	f.mu.Unlock()
	require.NotNil(t, cb, "no callback subscription on %s", subj)
	cb(&nats.Msg{Subject: subj, Data: data})
}

func newLifecycleClient(t *testing.T, fc *fakeConn, mode string) (*simClient, *countingMinter) {
	t.Helper()
	mint := &countingMinter{jwt: func() string { return mintTestJWT(t, time.Now().Add(2*time.Hour)) }}
	s := newTestSimClient(t, "user-lc", mode, mint)
	s.dial = func(context.Context) (simConn, error) { return fc, nil }
	s.resyncJitter = func() time.Duration { return 0 }
	s.resyncRetry = func(int) time.Duration { return 0 }
	return s, mint
}

func startClient(t *testing.T, s *simClient) (context.CancelFunc, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("simClient.run did not exit")
		}
	})
	return cancel, done
}
