//go:build integration

package roomkeysender

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/pkg/stdcopy"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomcrypto"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

// testIdentity derives a per-test account and room ID so tests sharing the
// process-wide broker can never cross-talk on identical subjects
// (CLAUDE.md §4: per-test isolation is the caller's responsibility).
func testIdentity(t *testing.T) (account, roomID string) {
	t.Helper()
	base := strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()))
	if len(base) > 24 {
		base = base[:24]
	}
	return "acct-" + base, "room-" + base
}

// setupNATS connects to the process-shared WebSocket-enabled NATS instance
// (testutil.NATSWebSocket). Returns a per-test Go NATS client (TCP), the
// WebSocket URL reachable from sibling containers on the shared network,
// and that network's name for setupNode.
func setupNATS(t *testing.T) (*nats.Conn, string, string) {
	t.Helper()
	info := testutil.NATSWebSocket(t)
	if info.Network == "" {
		t.Skip("shared ws-NATS is running in subprocess mode (no Docker): the TypeScript client needs a Node container on a shared docker network")
	}
	nc, err := nats.Connect(info.TCPURL)
	require.NoError(t, err, "connect to NATS")
	t.Cleanup(func() {
		nc.Close()
	})
	return nc, info.AliasWSURL, info.Network
}

// setupNode starts a Node container on the shared network, installs tsx + nats npm
// packages, and copies the client.ts script. Returns the container for exec calls.
func setupNode(t *testing.T, networkName string) testcontainers.Container {
	t.Helper()
	ctx := context.Background()

	scriptPath := filepath.Join("testdata", "client.ts")

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      testimages.Node,
			Cmd:        []string{"sh", "-c", "sleep 600"},
			Networks:   []string{networkName},
			WaitingFor: wait.ForExec([]string{"node", "--version"}).WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err, "start Node container")
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	// Copy client script into the container.
	err = container.CopyFileToContainer(ctx, scriptPath, "/client.ts", 0o644)
	require.NoError(t, err, "copy client.ts into container")

	// Copy WebSocket polyfill needed for nats.ws in Node.js environments without native WebSocket.
	err = container.CopyFileToContainer(
		ctx,
		filepath.Join("testdata", "ws-polyfill.cjs"),
		"/ws-polyfill.cjs",
		0o644,
	)
	require.NoError(t, err, "copy ws-polyfill.cjs into container")

	// Install tsx, nats.ws (WebSocket NATS client), and websocket (W3C WebSocket
	// polyfill referenced by ws-polyfill.cjs — Node 20 ships no native WebSocket).
	exitCode, reader, err := container.Exec(ctx, []string{
		"sh", "-c",
		"command -v tsx >/dev/null 2>&1 || (npm install -g tsx --quiet 2>&1 && npm install -g nats.ws --quiet 2>&1 && npm install -g websocket --quiet 2>&1)",
	})
	require.NoError(t, err, "exec npm install")
	out := readCombined(reader)
	require.Equal(t, 0, exitCode, "npm install failed:\n%s", out)

	return container
}

// readCombined reads a Docker multiplexed output stream and concatenates stdout and stderr.
func readCombined(r io.Reader) string {
	if r == nil {
		return ""
	}
	var stdout, stderr bytes.Buffer
	_, _ = stdcopy.StdCopy(&stdout, &stderr, r)
	return stdout.String() + stderr.String()
}

// splitOutput reads a Docker multiplexed stream and returns stdout and combined output separately.
func splitOutput(r io.Reader) (stdout, combined string) {
	if r == nil {
		return "", ""
	}
	var outBuf, errBuf bytes.Buffer
	_, _ = stdcopy.StdCopy(&outBuf, &errBuf, r)
	return outBuf.String(), outBuf.String() + errBuf.String()
}

// skipOnVFS skips the calling test when Docker is explicitly configured with
// the VFS storage driver. VFS lacks copy-on-write, so pulling node:20-alpine
// and running npm install inside a container takes several minutes — exceeding
// the default 10-minute test timeout. The unset case is NOT treated as VFS so
// CI/dev shells that don't export DOCKER_STORAGE_DRIVER still run these tests
// on whatever driver Docker actually uses (typically overlay2). Follow-up:
// migrate the npm installs to a pre-built image so the test runs in reasonable
// time on any driver.
func skipOnVFS(t *testing.T) {
	t.Helper()
	if os.Getenv("DOCKER_STORAGE_DRIVER") == "vfs" {
		t.Skip("skipping TypeScript client test: VFS storage driver is too slow (unset DOCKER_STORAGE_DRIVER or set to overlay2/btrfs to enable)")
	}
}

func TestRoomKeySender_TypeScriptClient_Unencrypted(t *testing.T) {
	skipOnVFS(t)
	ctx := context.Background()

	// 1. Start infrastructure.
	nc, wsURL, networkName := setupNATS(t)
	nodeContainer := setupNode(t, networkName)

	// 2. Test parameters — per-test identifiers on a process-shared broker.
	account, roomID := testIdentity(t)
	plaintext := "hello unencrypted"

	// 3. Start the TypeScript client in background.
	clientDone := make(chan struct {
		exitCode int
		stdout   string
		combined string
		err      error
	}, 1)

	go func() {
		exitCode, reader, err := nodeContainer.Exec(ctx, []string{
			"env", "NODE_PATH=/usr/local/lib/node_modules",
			"tsx", "--require", "/ws-polyfill.cjs", "/client.ts", wsURL, account, roomID,
		})
		stdout, combined := splitOutput(reader)
		clientDone <- struct {
			exitCode int
			stdout   string
			combined string
			err      error
		}{exitCode, stdout, combined, err}
	}()

	// 4. Brief delay for TypeScript subscriptions to establish.
	time.Sleep(3 * time.Second)

	// 5. Publish plain message WITHOUT X-Room-Key-Version header.
	msgSubject := fmt.Sprintf("test.room.%s.msg", roomID)
	err := nc.Publish(msgSubject, []byte(plaintext))
	require.NoError(t, err, "publish unencrypted message")

	// 6. Wait for TypeScript client to finish.
	select {
	case result := <-clientDone:
		require.NoError(t, result.err, "exec client.ts")
		require.Equal(t, 0, result.exitCode, "client.ts exited non-zero:\n%s", result.combined)
		assert.Equal(t, plaintext, strings.TrimRight(result.stdout, "\n"))
	case <-time.After(30 * time.Second):
		t.Fatal("TypeScript client timed out after 30s")
	}
}

func TestRoomKeySender_TypeScriptClient(t *testing.T) {
	skipOnVFS(t)
	ctx := context.Background()

	// 1. Start infrastructure.
	nc, wsURL, networkName := setupNATS(t)
	nodeContainer := setupNode(t, networkName)

	// 2. Generate a fresh 32-byte room secret.
	privKeyBytes := make([]byte, 32)
	_, err := rand.Read(privKeyBytes)
	require.NoError(t, err)

	// 3. Test parameters — per-test identifiers on a process-shared broker.
	account, roomID := testIdentity(t)
	version := 0
	plaintext := "hello from Go integration test"

	// 4. Start the TypeScript client (blocks until it prints output or times out).
	// Run in background via exec; we read output after publishing.
	clientDone := make(chan struct {
		exitCode int
		stdout   string
		combined string
		err      error
	}, 1)

	go func() {
		exitCode, reader, err := nodeContainer.Exec(ctx, []string{
			"env", "NODE_PATH=/usr/local/lib/node_modules",
			"tsx", "--require", "/ws-polyfill.cjs", "/client.ts", wsURL, account, roomID,
		})
		stdout, combined := splitOutput(reader)
		clientDone <- struct {
			exitCode int
			stdout   string
			combined string
			err      error
		}{exitCode, stdout, combined, err}
	}()

	// 5. Brief delay for TypeScript subscriptions to establish.
	time.Sleep(3 * time.Second)

	// 6. Publish room key via roomkeysender.
	sender := NewSender(nc)
	evt := &model.RoomKeyEvent{
		RoomID:     roomID,
		Version:    version,
		PrivateKey: privKeyBytes,
	}
	err = sender.Send(account, *evt)
	require.NoError(t, err, "send room key event")

	// 7. Small delay to ensure key is received before the encrypted message.
	time.Sleep(500 * time.Millisecond)

	// 8. Encrypt a message using the Encoder (room private key used directly
	// as AES-256-GCM key — no key derivation step).
	encoder := roomcrypto.NewEncoder()
	encrypted, err := encoder.Encode(roomID, plaintext, privKeyBytes, version)
	require.NoError(t, err, "encrypt message")
	encryptedJSON, err := json.Marshal(encrypted)
	require.NoError(t, err, "marshal encrypted message")

	// 9. Publish encrypted message with X-Room-Key-Version header.
	msgSubject := fmt.Sprintf("test.room.%s.msg", roomID)
	natsMsg := &nats.Msg{
		Subject: msgSubject,
		Data:    encryptedJSON,
		Header:  nats.Header{"X-Room-Key-Version": []string{strconv.Itoa(version)}},
	}
	err = nc.PublishMsg(natsMsg)
	require.NoError(t, err, "publish encrypted message")

	// 10. Wait for TypeScript client to finish.
	select {
	case result := <-clientDone:
		require.NoError(t, result.err, "exec client.ts")
		require.Equal(t, 0, result.exitCode, "client.ts exited non-zero:\n%s", result.combined)
		assert.Equal(t, plaintext, strings.TrimRight(result.stdout, "\n"))
	case <-time.After(30 * time.Second):
		t.Fatal("TypeScript client timed out after 30s")
	}
}
