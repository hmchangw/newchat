package cachescan

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// NewCluster adapts a go-redis cluster client to Cluster. Scanning is a
// keyspace walk, which has no cluster-wide command: it must run per primary
// and be summed, which is what ForEachPrimary does.
func NewCluster(c *redis.ClusterClient) Cluster { return clusterAdapter{c: c} }

type clusterAdapter struct{ c *redis.ClusterClient }

// ForEachPrimary visits every primary. go-redis runs the callback concurrently
// across shards, so Scan's accumulator is mutex-guarded.
func (a clusterAdapter) ForEachPrimary(ctx context.Context, fn func(context.Context, Node) error) error {
	err := a.c.ForEachMaster(ctx, func(ctx context.Context, shard *redis.Client) error {
		return fn(ctx, nodeAdapter{c: shard})
	})
	if err != nil {
		return fmt.Errorf("for each valkey primary: %w", err)
	}
	return nil
}

type nodeAdapter struct{ c *redis.Client }

// Scan pages the node's keyspace unfiltered. Classification happens in Go
// rather than via SCAN MATCH: MATCH filters after iteration, so a pattern
// saves no server work, and one unfiltered walk replaces one walk per cache.
func (n nodeAdapter) Scan(ctx context.Context, cursor uint64, count int64) ([]string, uint64, error) {
	keys, next, err := n.c.Scan(ctx, cursor, "", count).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("valkey scan: %w", err)
	}
	return keys, next, nil
}

// MemoryUsage reports a key's footprint including its own overhead. The
// SAMPLES argument is deliberately left at the server default: for the
// collection-valued caches (presence sets) it approximates from a few elements
// instead of walking every one, and the result is extrapolated anyway.
func (n nodeAdapter) MemoryUsage(ctx context.Context, key string) (int64, bool, error) {
	v, err := n.c.MemoryUsage(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("valkey memory usage: %w", err)
	}
	return v, true, nil
}
