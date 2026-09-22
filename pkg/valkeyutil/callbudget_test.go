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

// blackholeAddr accepts TCP and never answers: what a paused Valkey looks like,
// and the mode a refused connection does NOT reproduce.
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

// blackholeClient supplies slots directly, mirroring a service that was healthy
// when Valkey stalled and still holds topology, so commands reach the dead node.
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

// ReadTimeout bounds one socket read; the retry layers multiply it to ~2.1s
// (cache) / ~6.3s (store), leaving no request budget for the fallback to run in.
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

// A ceiling, never an extension: a request near its own guard must not be
// pushed past it by a cache read.
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

// Pipelines take a different go-redis entry point, and IncrEx — the bot rate
// limiter — is one, so a commands-only budget would leave it unbounded.
func TestCallBudget_CoversPipelines(t *testing.T) {
	client := blackholeClient(t, blackholeAddr(t), CacheProfile)

	start := time.Now()
	_, err := client.IncrEx(context.Background(), "botrl:caller:x", time.Minute)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, CacheProfile.CallBudget+500*time.Millisecond,
		"a pipelined operation must be bounded too")
}
