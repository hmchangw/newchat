//go:build integration

package testutil

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

// NATSWebSocketInfo describes the process-shared WebSocket-enabled NATS
// instance: host-reachable URLs for Go clients, plus the shared docker
// network and in-network URL for sibling containers (e.g. a Node client).
//
// Backing instance is a testcontainers NATS container when Docker is
// available; otherwise a `nats-server` subprocess (binary on PATH). In
// subprocess mode there is no docker network, so Network and AliasWSURL
// are empty — tests that attach sibling containers must skip on that.
type NATSWebSocketInfo struct {
	WSURL      string // ws://<host>:<ws port>
	TCPURL     string // nats://<host>:<tcp port>
	Network    string // shared docker network name ("" in subprocess mode)
	AliasWSURL string // ws://nats-ws:8080 from that network ("" in subprocess mode)
}

const natsWSAlias = "nats-ws"

var (
	natsWSOnce      sync.Once
	natsWSContainer testcontainers.Container
	natsWSNetwork   *testcontainers.DockerNetwork
	natsWSStopProc  func()
	natsWSInfo      NATSWebSocketInfo
	natsWSInitErr   error
)

// startNATSWSBinary runs a websocket-enabled nats-server subprocess for
// hosts without Docker, mirroring nats_binary.go's fallback for the plain
// shared instance (container-first here, because the docker network only
// exists in container mode).
func startNATSWSBinary() (NATSWebSocketInfo, func(), error) {
	binPath, err := exec.LookPath("nats-server")
	if err != nil {
		return NATSWebSocketInfo{}, nil, fmt.Errorf("nats-server binary not on PATH: %w", err)
	}
	tcpPort, err := freePort()
	if err != nil {
		return NATSWebSocketInfo{}, nil, fmt.Errorf("alloc tcp port: %w", err)
	}
	wsPort, err := freePort()
	if err != nil {
		return NATSWebSocketInfo{}, nil, fmt.Errorf("alloc ws port: %w", err)
	}
	confDir, err := os.MkdirTemp("", "nats-ws-")
	if err != nil {
		return NATSWebSocketInfo{}, nil, fmt.Errorf("mkdtemp: %w", err)
	}
	conf := fmt.Sprintf(`
listen: 127.0.0.1:%d
websocket {
  listen: "127.0.0.1:%d"
  no_tls: true
}
`, tcpPort, wsPort)
	confPath := filepath.Join(confDir, "nats.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		_ = os.RemoveAll(confDir)
		return NATSWebSocketInfo{}, nil, fmt.Errorf("write ws-nats conf: %w", err)
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	// #nosec G204 -- binPath via exec.LookPath("nats-server"); argv is a fixed temp config path.
	cmd := exec.CommandContext(bgCtx, binPath, "--config", confPath) // nosemgrep
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Start(); err != nil {
		cancel()
		_ = os.RemoveAll(confDir)
		return NATSWebSocketInfo{}, nil, fmt.Errorf("start nats-server: %w", err)
	}
	stop := func() {
		cancel()
		_ = cmd.Wait()
		_ = os.RemoveAll(confDir)
	}
	tcpURL := "nats://127.0.0.1:" + strconv.Itoa(tcpPort)
	if err := waitForNATSReady(tcpURL, 30*time.Second); err != nil {
		stop()
		return NATSWebSocketInfo{}, nil, fmt.Errorf("ws nats-server never became ready: %w", err)
	}
	if err := waitForTCPPort(wsPort, 10*time.Second); err != nil {
		stop()
		return NATSWebSocketInfo{}, nil, fmt.Errorf("ws listener never became ready: %w", err)
	}
	return NATSWebSocketInfo{
		WSURL:  "ws://127.0.0.1:" + strconv.Itoa(wsPort),
		TCPURL: tcpURL,
	}, stop, nil
}

// waitForTCPPort probes the advertised websocket listener — TCP readiness
// on 4222 does not imply the ws listener is accepting yet.
func waitForTCPPort(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("port %d not accepting within %s", port, timeout)
}

func startNATSWSContainer() (NATSWebSocketInfo, error) {
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		return NATSWebSocketInfo{}, fmt.Errorf("create ws-nats network: %w", err)
	}
	// Plain broker for client-transport tests: WebSocket without TLS,
	// no auth, no JetStream.
	natsConf := `
listen: 0.0.0.0:4222
websocket {
  listen: "0.0.0.0:8080"
  no_tls: true
}
`
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testimages.NATS,
			ExposedPorts: []string{"4222/tcp", "8080/tcp"},
			Cmd:          []string{"--config", "/nats.conf"},
			Files: []testcontainers.ContainerFile{{
				Reader:            strings.NewReader(natsConf),
				ContainerFilePath: "/nats.conf",
				FileMode:          0o644,
			}},
			Networks:       []string{nw.Name},
			NetworkAliases: map[string][]string{nw.Name: {natsWSAlias}},
			WaitingFor:     wait.ForLog("Server is ready").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		_ = nw.Remove(ctx)
		return NATSWebSocketInfo{}, fmt.Errorf("start ws-nats: %w", err)
	}
	cleanupOnErr := func() {
		_ = c.Terminate(ctx)
		_ = nw.Remove(ctx)
	}
	host, err := c.Host(ctx)
	if err != nil {
		cleanupOnErr()
		return NATSWebSocketInfo{}, fmt.Errorf("resolve ws-nats host: %w", err)
	}
	tcpPort, err := c.MappedPort(ctx, "4222")
	if err != nil {
		cleanupOnErr()
		return NATSWebSocketInfo{}, fmt.Errorf("resolve ws-nats tcp port: %w", err)
	}
	wsPort, err := c.MappedPort(ctx, "8080")
	if err != nil {
		cleanupOnErr()
		return NATSWebSocketInfo{}, fmt.Errorf("resolve ws-nats ws port: %w", err)
	}
	natsWSContainer = c
	natsWSNetwork = nw
	return NATSWebSocketInfo{
		WSURL:      fmt.Sprintf("ws://%s:%s", host, wsPort.Port()),
		TCPURL:     fmt.Sprintf("nats://%s:%s", host, tcpPort.Port()),
		Network:    nw.Name,
		AliasWSURL: fmt.Sprintf("ws://%s:8080", natsWSAlias),
	}, nil
}

// ensureNATSWebSocket is container-first (the docker network only exists in
// container mode, and the Node-container tests need it), with a
// nats-server-subprocess fallback for hosts where Docker cannot run or
// cannot pull images.
func ensureNATSWebSocket() (NATSWebSocketInfo, error) {
	natsWSOnce.Do(func() {
		info, containerErr := startNATSWSContainer()
		if containerErr == nil {
			natsWSInfo = info
			return
		}
		info, stop, binErr := startNATSWSBinary()
		if binErr != nil {
			natsWSInitErr = errors.Join(
				fmt.Errorf("ws-nats container: %w", containerErr),
				fmt.Errorf("binary fallback: %w", binErr))
			return
		}
		natsWSStopProc = stop
		natsWSInfo = info
	})
	return natsWSInfo, natsWSInitErr
}

// NATSWebSocket returns the shared WebSocket-enabled NATS instance (no auth,
// no JetStream — a plain broker for client-transport tests).
func NATSWebSocket(t *testing.T) NATSWebSocketInfo {
	t.Helper()
	info, err := ensureNATSWebSocket()
	if err != nil {
		t.Fatalf("testutil.NATSWebSocket: %v", err)
	}
	return info
}

// EnsureNATSWebSocket starts the shared ws-NATS instance if not already
// started. No-t variant intended for TestMain pre-warming.
func EnsureNATSWebSocket() error { _, err := ensureNATSWebSocket(); return err }

// TerminateNATSWebSocket stops the shared ws-NATS instance and its network.
// Best-effort, idempotent.
func TerminateNATSWebSocket() {
	if natsWSStopProc != nil {
		natsWSStopProc()
		natsWSStopProc = nil
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if natsWSContainer != nil {
		if err := natsWSContainer.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "terminate ws-nats: %v\n", err)
		}
		natsWSContainer = nil
	}
	if natsWSNetwork != nil {
		_ = natsWSNetwork.Remove(ctx)
		natsWSNetwork = nil
	}
}
