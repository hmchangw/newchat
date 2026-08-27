// Package valkeyfake is an in-memory valkeyutil.Client for unit tests.
//
// Twelve test packages had each hand-rolled one. They agreed on the parts that
// matter — an absent key is ErrCacheMiss, EXPIRE never creates a key — and
// differed everywhere else: some tracked TTLs and some discarded them, some
// recorded Del batches and some only counted them, MGet returned (nil, nil) in
// three of them regardless of what was stored. A tier's bulk path could pass
// its package's tests against a fake that never returned anything.
//
// pkg/valkeyutil's own in-package tests cannot import this one back — it imports
// valkeyutil for ErrCacheMiss — so they embed the Client interface and override
// the one or two methods under test instead of standing up a whole fake.
package valkeyfake

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Calls counts invocations per method — calls, not keys, so a test can assert
// how many round trips a bulk operation cost.
type Calls struct {
	Get    int
	MGet   int
	Set    int
	SetNX  int
	IncrEx int
	Del    int
	Expire int
}

type entry struct {
	value string
	ttl   time.Duration
	// deadline is set only when a clock was injected; the zero value means the
	// entry never expires on its own.
	deadline time.Time
}

// Client is an in-memory valkeyutil.Client. The zero value is not usable — call
// New. Every method is safe for concurrent use, since the packages using it run
// under -race and several drive it from several goroutines.
type Client struct {
	mu    sync.Mutex
	store map[string]entry
	now   func() time.Time

	calls       Calls
	delBatches  [][]string
	msetBatches [][]valkeyutil.KV
	mgetBatches [][]string
	expired     []string
	setKeys     []string

	getErr, mgetErr, setErr, delErr, expireErr error

	onDel    func(context.Context)
	afterGet func(key string)
}

// New returns an empty Client. Nothing expires until SetClock installs a clock:
// most tiers derive staleness from a stamp inside the cached value and inject
// their own clock there, and an entry vanishing underneath them would only add
// noise.
func New() *Client { return &Client{store: make(map[string]entry)} }

// SetClock makes stored entries expire against now, so a test can drive an entry
// past its deadline. It applies to later writes; entries already stored keep
// whatever deadline they were written with.
func (c *Client) SetClock(now func() time.Time) { c.set(func() { c.now = now }) }

// --- valkeyutil.Client ---

func (c *Client) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	c.calls.Get++
	if err := c.getErr; err != nil {
		c.mu.Unlock()
		return "", err
	}
	e, ok := c.live(key)
	after := c.afterGet
	c.mu.Unlock()

	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	// Outside the lock: the hook exists to interleave another call on this same
	// client, which would deadlock against a held mutex.
	if after != nil {
		after(key)
	}
	return e.value, nil
}

func (c *Client) MGet(_ context.Context, keys []string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls.MGet++
	c.mgetBatches = append(c.mgetBatches, slices.Clone(keys))
	if c.mgetErr != nil {
		return nil, c.mgetErr
	}
	if len(keys) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		if e, ok := c.live(k); ok {
			out[k] = e.value
		}
	}
	return out, nil
}

func (c *Client) Set(_ context.Context, key, value string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls.Set++
	// Recorded before the injected failure: a test asserting nothing was cached
	// needs to see that the attempt was made.
	c.setKeys = append(c.setKeys, key)
	if c.setErr != nil {
		return c.setErr
	}
	c.put(key, value, ttl)
	return nil
}

// MSet stores every entry under one TTL in a single call, satisfying
// valkeyutil's optional multiSetter capability so a caller's bulk write-back
// takes the same one-round-trip path here that it takes in production. Without
// it SetMany silently falls back to a per-key loop, and a test asserting "one
// bulk write, no per-key Sets" would be asserting the fallback.
func (c *Client) MSet(_ context.Context, entries []valkeyutil.KV, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msetBatches = append(c.msetBatches, slices.Clone(entries))
	if c.setErr != nil {
		return c.setErr
	}
	for _, e := range entries {
		c.put(e.Key, e.Value, ttl)
	}
	return nil
}

func (c *Client) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls.SetNX++
	if c.setErr != nil {
		return false, c.setErr
	}
	if _, held := c.live(key); held {
		return false, nil
	}
	c.put(key, value, ttl)
	return true, nil
}

func (c *Client) IncrEx(_ context.Context, key string, ttl time.Duration) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls.IncrEx++
	if c.setErr != nil {
		return 0, c.setErr
	}
	var n int64
	if e, ok := c.live(key); ok {
		n, _ = strconv.ParseInt(e.value, 10, 64)
	}
	n++
	// The TTL applies only on the 0->1 transition, the fixed-window rate-limit
	// recipe: a later increment inside the window must not re-arm it.
	if n == 1 {
		c.put(key, strconv.FormatInt(n, 10), ttl)
	} else {
		e := c.store[key]
		e.value = strconv.FormatInt(n, 10)
		c.store[key] = e
	}
	return n, nil
}

func (c *Client) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	c.mu.Lock()
	c.calls.Del++
	c.delBatches = append(c.delBatches, slices.Clone(keys))
	hook, err := c.onDel, c.delErr
	if err == nil {
		for _, k := range keys {
			delete(c.store, k)
		}
	}
	c.mu.Unlock()

	if hook != nil {
		hook(ctx)
	}
	return err
}

// Expire mirrors Valkey: it re-arms an existing key's deadline and reports
// whether the key was there, but never creates one.
func (c *Client) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls.Expire++
	c.expired = append(c.expired, key)
	if c.expireErr != nil {
		return false, c.expireErr
	}
	e, ok := c.live(key)
	if !ok {
		return false, nil
	}
	c.put(key, e.value, ttl)
	return true, nil
}

// Close satisfies valkeyutil.Client. Nothing to release in memory, and no test
// has needed to assert on it — a counter and an error injector lived here
// unused, so they are gone rather than shipped untested.
func (c *Client) Close() error { return nil }

// --- seeding and inspection ---

// Seed stores a key directly, bypassing the call counters and any injected Set
// failure, so a test can arrange state without it looking like traffic.
func (c *Client) Seed(key, value string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.put(key, value, ttl)
}

// Value returns a stored value without counting a Get, or "" when the key is
// absent. Use Has to distinguish absent from empty.
func (c *Client) Value(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, _ := c.live(key)
	return e.value
}

// TTL returns the TTL a key was last written with.
func (c *Client) TTL(key string) (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.live(key)
	return e.ttl, ok
}

// Deadline returns a key's absolute expiry, present only when a clock is
// installed. A slide must move this, not merely the recorded TTL.
func (c *Client) Deadline(key string) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.live(key)
	if !ok || e.deadline.IsZero() {
		return time.Time{}, false
	}
	return e.deadline, true
}

// Has reports whether a key is present.
func (c *Client) Has(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.live(key)
	return ok
}

// Keys returns every live key, sorted so assertions are deterministic.
func (c *Client) Keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.store))
	for k := range c.store {
		if _, ok := c.live(k); ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}

// Calls returns a snapshot of the per-method call counts.
func (c *Client) Calls() Calls {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// DelBatches returns the key sets passed to each Del call, in order. One entry
// per call, so a test can assert how many round trips an invalidation cost.
func (c *Client) DelBatches() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]string, len(c.delBatches))
	for i, b := range c.delBatches {
		out[i] = slices.Clone(b)
	}
	return out
}

// mgetBatchesSnapshot returns the key sets passed to each MGet call, in order.
func (c *Client) mgetBatchesSnapshot() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]string, len(c.mgetBatches))
	for i, b := range c.mgetBatches {
		out[i] = slices.Clone(b)
	}
	return out
}

// MGetKeys is every key passed to MGet, flattened in call order.
func (c *Client) MGetKeys() []string { return slices.Concat(c.mgetBatchesSnapshot()...) }

// DeletedKeys is every key passed to Del, flattened in call order.
func (c *Client) DeletedKeys() []string {
	return slices.Concat(c.DelBatches()...)
}

// MSetBatches returns the entry sets passed to each MSet call, in order — one
// entry per call, so a test can tell one bulk write from a per-key loop.
func (c *Client) MSetBatches() [][]valkeyutil.KV {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]valkeyutil.KV, len(c.msetBatches))
	for i, b := range c.msetBatches {
		out[i] = slices.Clone(b)
	}
	return out
}

// SetKeys is every key passed to Set, in call order, recorded even when Set was
// made to fail.
func (c *Client) SetKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.setKeys)
}

// ExpiredKeys is every key passed to Expire, in call order.
func (c *Client) ExpiredKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.expired)
}

// --- failure injection ---

// FailGet makes every later Get return err. A nil clears it.
func (c *Client) FailGet(err error) { c.set(func() { c.getErr = err }) }

// FailMGet makes every later MGet return err.
func (c *Client) FailMGet(err error) { c.set(func() { c.mgetErr = err }) }

// FailSet makes every later Set, SetNX and IncrEx return err without writing.
func (c *Client) FailSet(err error) { c.set(func() { c.setErr = err }) }

// FailDel makes every later Del return err without deleting. The call is still
// recorded, so a test can assert the attempt was made.
func (c *Client) FailDel(err error) { c.set(func() { c.delErr = err }) }

// FailExpire makes every later Expire return err.
func (c *Client) FailExpire(err error) { c.set(func() { c.expireErr = err }) }

// OnDel runs fn with the context Del was called with, before Del returns. For
// asserting what a bust's context carries — cancellation stripped, deadline kept.
func (c *Client) OnDel(fn func(context.Context)) { c.set(func() { c.onDel = fn }) }

// AfterGet runs fn after a hit reads its value but before Get returns it,
// letting a test interleave a write into that window. It runs outside the
// client's lock, so fn may call back into this client.
func (c *Client) AfterGet(fn func(key string)) { c.set(func() { c.afterGet = fn }) }

// --- internals ---

func (c *Client) set(mutate func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	mutate()
}

// live returns a key's entry if it is present and not past its deadline,
// dropping it if it is. Callers must hold the lock.
func (c *Client) live(key string) (entry, bool) {
	e, ok := c.store[key]
	if !ok {
		return entry{}, false
	}
	if !e.deadline.IsZero() && !c.now().Before(e.deadline) {
		delete(c.store, key)
		return entry{}, false
	}
	return e, true
}

// put writes a key. Callers must hold the lock.
func (c *Client) put(key, value string, ttl time.Duration) {
	e := entry{value: value, ttl: ttl}
	if c.now != nil && ttl > 0 {
		e.deadline = c.now().Add(ttl)
	}
	c.store[key] = e
}
