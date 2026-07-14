//go:build integration

package testutil

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
)

// startNATSBinary spawns `nats-server -js` on a free local port with a
// per-process JetStream store directory. Returns the connect URL, a
// stop func, or an error. The binary is located via
// exec.LookPath("nats-server"); when absent the caller falls back to
// the testcontainers NATS module.
func startNATSBinary() (url string, stop func(), err error) {
	binPath, err := exec.LookPath("nats-server")
	if err != nil {
		return "", nil, fmt.Errorf("nats-server binary not on PATH: %w", err)
	}
	port, err := freePort()
	if err != nil {
		return "", nil, fmt.Errorf("alloc port: %w", err)
	}
	storeDir, err := os.MkdirTemp("", "nats-jetstream-")
	if err != nil {
		return "", nil, fmt.Errorf("mkdtemp: %w", err)
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	// #nosec G204 -- binPath via exec.LookPath("nats-server"); argv fixed (free port, temp dir).
	cmd := exec.CommandContext(bgCtx, binPath, // nosemgrep
		"-js",
		"-a", "127.0.0.1",
		"-p", strconv.Itoa(port),
		"-sd", storeDir,
	)
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Start(); err != nil {
		cancel()
		_ = os.RemoveAll(storeDir)
		return "", nil, fmt.Errorf("start nats-server: %w", err)
	}
	stop = func() {
		cancel()
		_ = cmd.Wait()
		_ = os.RemoveAll(storeDir)
	}

	url = fmt.Sprintf("nats://127.0.0.1:%d", port)
	if err := waitForNATSReady(url, 30*time.Second); err != nil {
		stop()
		return "", nil, fmt.Errorf("nats-server never became ready: %w", err)
	}
	return url, stop, nil
}

func waitForNATSReady(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nc, err := nats.Connect(url, nats.Timeout(500*time.Millisecond))
		if err == nil {
			nc.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("nats not ready within %s", timeout)
}
