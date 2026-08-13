//go:build integration

package testutil

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// valkeyBinaryNames lists the server binaries that can back the test cluster,
// in preference order. Valkey is a Redis fork, so a redis-server on PATH
// speaks the same cluster protocol and serves every command these tests use.
var valkeyBinaryNames = []string{"valkey-server", "redis-server"}

// startValkeyBinary spawns a cluster-enabled server on a free local port and
// assigns it all 16384 slots, matching the single-node cluster the
// testcontainers path builds. Returns the address, a stop func, or an error;
// when no binary is on PATH the caller falls back to the container.
func startValkeyBinary() (addr string, stop func(), err error) {
	binPath, err := lookupValkeyBinary()
	if err != nil {
		return "", nil, err
	}
	port, err := freePort()
	if err != nil {
		return "", nil, fmt.Errorf("alloc port: %w", err)
	}
	// cluster-config-file is resolved against the working directory, so the
	// server runs in a temp dir rather than dropping nodes.conf into whichever
	// package is under test.
	dir, err := os.MkdirTemp("", "valkey-cluster-")
	if err != nil {
		return "", nil, fmt.Errorf("mkdtemp: %w", err)
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	// #nosec G204 -- binPath via exec.LookPath over a fixed name list; argv fixed (free port, temp dir).
	cmd := exec.CommandContext(bgCtx, binPath, // nosemgrep
		"--cluster-enabled", "yes",
		"--cluster-config-file", "nodes.conf",
		"--cluster-node-timeout", "5000",
		"--bind", "127.0.0.1",
		"--port", strconv.Itoa(port),
		"--save", "",
		"--appendonly", "no",
	)
	cmd.Dir = dir
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Start(); err != nil {
		cancel()
		_ = os.RemoveAll(dir)
		return "", nil, fmt.Errorf("start %s: %w", binPath, err)
	}
	stop = func() {
		cancel()
		_ = cmd.Wait()
		_ = os.RemoveAll(dir)
	}

	addr = fmt.Sprintf("127.0.0.1:%d", port)
	if err := formValkeyCluster(addr); err != nil {
		stop()
		return "", nil, err
	}
	return addr, stop, nil
}

func lookupValkeyBinary() (string, error) {
	for _, name := range valkeyBinaryNames {
		if path, err := exec.LookPath(name); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("no valkey server binary on PATH (tried %s)", strings.Join(valkeyBinaryNames, ", "))
}

// formValkeyCluster waits for the node to accept connections, claims every
// slot for it, and waits for the cluster to report itself healthy. A
// single-slot-owner cluster is what the ClusterClient's slot override assumes.
func formValkeyCluster(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	node := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = node.Close() }()

	if err := waitForValkeyPing(ctx, node); err != nil {
		return err
	}
	if err := node.Do(ctx, "cluster", "addslotsrange", 0, 16383).Err(); err != nil {
		return fmt.Errorf("cluster addslotsrange: %w", err)
	}
	return waitForValkeyClusterOK(ctx, node)
}

func waitForValkeyPing(ctx context.Context, node *redis.Client) error {
	for {
		if err := node.Ping(ctx).Err(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("valkey never accepted connections: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitForValkeyClusterOK(ctx context.Context, node *redis.Client) error {
	for {
		info, err := node.ClusterInfo(ctx).Result()
		if err == nil && strings.Contains(info, "cluster_state:ok") {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("valkey cluster never reached ok state: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
