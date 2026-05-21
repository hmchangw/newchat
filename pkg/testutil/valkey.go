//go:build integration

package testutil

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

// StartValkeyCluster boots a per-test cluster-mode Valkey. Use when a
// test asserts on cluster-routing state; otherwise prefer SharedValkeyCluster.
func StartValkeyCluster(t *testing.T) *redis.ClusterClient {
	t.Helper()
	ctx := context.Background()
	container, addr := startValkeyClusterContainer(ctx, t)
	t.Cleanup(func() { _ = container.Terminate(ctx) })
	c := newValkeyClusterClient(addr)
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, pingCluster(ctx, c), "ping valkey cluster")
	return c
}

// SharedValkeyCluster returns a *redis.ClusterClient against a
// process-shared cluster-mode Valkey (started via sync.Once, reaped via
// TerminateAll). Callers must register
// `t.Cleanup(func() { testutil.FlushValkey(t) })` for keyspace isolation.
func SharedValkeyCluster(t *testing.T) *redis.ClusterClient {
	t.Helper()
	ensureSharedValkeyCluster()
	if sharedValkeyErr != nil {
		t.Fatalf("testutil.SharedValkeyCluster: %v", sharedValkeyErr)
	}
	return sharedValkeyClient
}

// EnsureValkey is the no-t variant for TestMain pre-warming.
func EnsureValkey() error { ensureSharedValkeyCluster(); return sharedValkeyErr }

// FlushValkey runs FLUSHALL on every master in the shared cluster.
// Test-fatal on error — leftover state would silently break the next test.
func FlushValkey(t *testing.T) {
	t.Helper()
	if sharedValkeyClient == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := sharedValkeyClient.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
		return m.FlushAll(ctx).Err()
	})
	if err != nil {
		t.Errorf("flush shared valkey cluster: %v", err)
	}
}

// TerminateValkey closes the shared client/container. Idempotent.
func TerminateValkey() {
	if sharedValkeyClient != nil {
		_ = sharedValkeyClient.Close()
		sharedValkeyClient = nil
	}
	if sharedValkeyContainer == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sharedValkeyContainer.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "terminate shared valkey: %v\n", err)
	}
	sharedValkeyContainer = nil
}

var (
	sharedValkeyOnce      sync.Once
	sharedValkeyContainer testcontainers.Container
	sharedValkeyClient    *redis.ClusterClient
	sharedValkeyErr       error
)

func ensureSharedValkeyCluster() {
	sharedValkeyOnce.Do(func() {
		ctx := context.Background()
		container, addr, err := startValkeyClusterContainerNoT(ctx)
		if err != nil {
			sharedValkeyErr = fmt.Errorf("start shared valkey cluster: %w", err)
			return
		}
		c := newValkeyClusterClient(addr)
		if err := pingCluster(ctx, c); err != nil {
			_ = c.Close()
			_ = container.Terminate(ctx)
			sharedValkeyErr = fmt.Errorf("ping shared valkey cluster: %w", err)
			return
		}
		sharedValkeyContainer = container
		sharedValkeyClient = c
	})
}

func startValkeyClusterContainer(ctx context.Context, t *testing.T) (testcontainers.Container, string) {
	t.Helper()
	container, addr, err := startValkeyClusterContainerNoT(ctx)
	require.NoError(t, err, "start valkey cluster container")
	return container, addr
}

func startValkeyClusterContainerNoT(ctx context.Context) (testcontainers.Container, string, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testimages.Valkey,
			ExposedPorts: []string{"6379/tcp"},
			Cmd: []string{
				"valkey-server",
				"--cluster-enabled", "yes",
				"--cluster-config-file", "nodes.conf",
				"--cluster-node-timeout", "5000",
				"--save", "",
			},
			WaitingFor: wait.ForLog("Ready to accept connections"),
		},
		Started: true,
	})
	if err != nil {
		return nil, "", err
	}
	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, "", fmt.Errorf("get valkey host: %w", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, "", fmt.Errorf("get valkey port: %w", err)
	}
	addr := fmt.Sprintf("%s:%s", host, port.Port())

	exitCode, _, err := container.Exec(ctx, []string{"valkey-cli", "CLUSTER", "ADDSLOTSRANGE", "0", "16383"})
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, "", fmt.Errorf("exec cluster addslotsrange: %w", err)
	}
	if exitCode != 0 {
		_ = container.Terminate(ctx)
		return nil, "", fmt.Errorf("cluster addslotsrange exited %d", exitCode)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, out, execErr := container.Exec(ctx, []string{"valkey-cli", "CLUSTER", "INFO"})
		if execErr == nil {
			buf, _ := io.ReadAll(out)
			if strings.Contains(string(buf), "cluster_state:ok") {
				return container, addr, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = container.Terminate(ctx)
	return nil, "", fmt.Errorf("cluster never reached ok state within 10s")
}

// newValkeyClusterClient builds a ClusterClient that routes all 16384
// slots to the externally-mapped addr. The ClusterSlots override is
// required because the node announces 127.0.0.1:6379 to peers (the
// container-internal address), which the host can't reach.
func newValkeyClusterClient(addr string) *redis.ClusterClient {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{addr},
		ClusterSlots: func(_ context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{
				{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: addr}}},
			}, nil
		},
	})
}

func pingCluster(ctx context.Context, c *redis.ClusterClient) error {
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.Ping(pingCtx).Err()
}
