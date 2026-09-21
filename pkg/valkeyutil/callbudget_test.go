package valkeyutil

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blackholeAddr starts a listener that accepts TCP and never answers, then
// returns its address. This is what a paused or packet-dropping Valkey looks
// like to the client — the degraded mode the profiles exist for, and the one a
// refused connection does NOT reproduce.
func blackholeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		var held []net.Conn
		for {
			c, err := ln.Accept()
			if err != nil {
				for _, h := range held {
					_ = h.Close()
				}
				return
			}
			held = append(held, c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

// blackholeClient builds a cluster client pinned to addr with slots supplied
// directly, so no live CLUSTER SLOTS is needed. That mirrors the real outage:
// a service that was healthy when Valkey stalled still holds cached topology,
// so its commands route to the stalled node instead of failing fast on a
// topology reload.
func blackholeClient(t *testing.T, addr string, p Profile) *clusterClient {
	t.Helper()
	opts := ClusterOptionsFor([]string{addr}, "", p)
	opts.ClusterSlots = func(context.Context) ([]redis.ClusterSlot, error) {
		return []redis.ClusterSlot{{
			Start: 0, End: 16383,
			Nodes: []redis.ClusterNode{{Addr: addr}},
		}}, nil
	}
	c := newProfiledClusterClient(opts, p)
	t.Cleanup(func() { _ = c.Close() })
	return &clusterClient{c: c}
}

// A profile's ReadTimeout bounds one socket read, not one operation: go-redis
// retries a timed-out read at two nesting levels (MaxRedirects on the cluster
// loop, MaxRetries on the node client), so the wall cost of a single Get is
// their product — measured at ~2.1s for CacheProfile's 150ms, and ~6.3s for
// StoreProfile's 500ms.
//
// That defeats the whole design. Every consumer here is fail-open with a source
// of truth behind it, but the fallback only runs if there is request budget left
// when the cache read gives up. At 2s a call and several calls per request,
// history-service's 10s guard expires before Cassandra is ever asked, so a
// degraded Valkey produces a timeout instead of a slower correct answer.
//
// CallBudget caps one operation end to end, across every retry layer.
func TestCallBudget_BoundsOneOperationAgainstABlackhole(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile Profile
	}{
		{"cache", CacheProfile},
		{"store", StoreProfile},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Positive(t, tc.profile.CallBudget, "every profile must cap a call")
			client := blackholeClient(t, blackholeAddr(t), tc.profile)

			start := time.Now()
			_, err := client.Get(context.Background(), "some:key")
			elapsed := time.Since(start)

			require.Error(t, err, "a blackholed read must fail rather than hang")
			// Slack for scheduling and the final in-flight read; the point is the
			// retry product no longer applies, not the exact figure.
			assert.Less(t, elapsed, tc.profile.CallBudget+500*time.Millisecond,
				"one operation must not cost the retry product (was ~2.1s cache / ~6.3s store)")
		})
	}
}

// The budget is a ceiling, never an extension: a caller with less time left than
// the budget keeps its own deadline, so a request already near its guard cannot
// be pushed past it by a cache read.
func TestCallBudget_NeverExtendsACallersDeadline(t *testing.T) {
	client := blackholeClient(t, blackholeAddr(t), CacheProfile)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := client.Get(ctx, "some:key")
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, CacheProfile.CallBudget,
		"the caller's shorter deadline must win over the budget")
}

// Pipelines route through a different go-redis entry point than single
// commands, and IncrEx (the bot rate limiter) is a pipeline — so a budget that
// only covered single commands would leave the rate-limit path unbounded.
func TestCallBudget_CoversPipelines(t *testing.T) {
	client := blackholeClient(t, blackholeAddr(t), CacheProfile)

	start := time.Now()
	_, err := client.IncrEx(context.Background(), "botrl:caller:x", time.Minute)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, CacheProfile.CallBudget+500*time.Millisecond,
		"a pipelined operation must be bounded too")
}
