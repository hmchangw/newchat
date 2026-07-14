//go:build integration

package testutil

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"time"

	vaultapi "github.com/hashicorp/vault/api"
)

// startVaultDevBinary spawns `vault server -dev` as a subprocess listening
// on a free local port and waits until the HTTP API is initialized and
// unsealed. Returns the HTTP address, a stop func, or an error.
//
// The transit secrets engine is NOT mounted here — the caller does that
// once against the returned address, identical to the container path.
//
// The binary is located via exec.LookPath("vault"); when it's absent the
// caller falls back to the testcontainers Vault module. The process is
// started with a detached context so it outlives the first test that
// triggered the lazy init.
func startVaultDevBinary() (addr string, stop func(), err error) {
	binPath, err := exec.LookPath("vault")
	if err != nil {
		return "", nil, fmt.Errorf("vault binary not on PATH: %w", err)
	}
	port, err := freePort()
	if err != nil {
		return "", nil, fmt.Errorf("alloc port: %w", err)
	}
	listenAddr := "127.0.0.1:" + strconv.Itoa(port)
	addr = "http://" + listenAddr

	bgCtx, cancel := context.WithCancel(context.Background())
	// #nosec G204 -- binPath via exec.LookPath("vault"); argv fixed (const token, loopback addr).
	cmd := exec.CommandContext(bgCtx, binPath, "server", "-dev", // nosemgrep
		"-dev-root-token-id="+vaultRootToken,
		"-dev-listen-address="+listenAddr,
	)
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Start(); err != nil {
		cancel()
		return "", nil, fmt.Errorf("start vault binary: %w", err)
	}
	stop = func() {
		cancel()
		_ = cmd.Wait()
	}

	if err := waitForVaultReady(addr, 30*time.Second); err != nil {
		stop()
		return "", nil, fmt.Errorf("vault dev never became ready: %w", err)
	}
	return addr, stop, nil
}

func waitForVaultReady(addr string, timeout time.Duration) error {
	cfg := vaultapi.DefaultConfig()
	cfg.Address = addr
	client, err := vaultapi.NewClient(cfg)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		h, err := client.Sys().Health()
		if err == nil && h != nil && h.Initialized && !h.Sealed {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("vault did not become ready within %s", timeout)
}

// freePort returns an unused local TCP port. There is a small TOCTOU
// window between Close() here and the child process binding the port,
// but this is acceptable for test fixtures.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
