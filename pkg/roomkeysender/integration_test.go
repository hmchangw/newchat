//go:build integration

package roomkeysender

import (
	"bytes"
	"context"
	"crypto/ecdh"
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
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomcrypto"
	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

const natsAlias = "nats-server"

// setupNetwork creates a shared Docker network for the NATS and Node containers.
func setupNetwork(t *testing.T) *testcontainers.DockerNetwork {
	t.Helper()
	ctx := context.Background()
	nw, err := network.New(ctx)
	require.NoError(t, err, "create docker network")
	t.Cleanup(func() {
		_ = nw.Remove(ctx)
	})
	return nw
}

// setupNATS starts a NATS container with TCP (4222) and WebSocket (8080) enabled.
// Returns a connected Go NATS client (TCP) and the WebSocket URL reachable from
// other containers on the shared network.
func setupNATS(t *testing.T, nw *testcontainers.DockerNetwork) (*nats.Conn, string) {
	t.Helper()
	ctx := context.Background()

	// NATS config enabling WebSocket without TLS.
	natsConf := `
listen: 0.0.0.0:4222
websocket {
  listen: "0.0.0.0:8080"
  no_tls: true
}
`

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testimages.NATS,
			ExposedPorts: []string{"4222/tcp", "8080/tcp"},
			Cmd:          []string{"--config", "/nats.conf"},
			Files: []testcontainers.ContainerFile{
				{
					Reader:            strings.NewReader(natsConf),
					ContainerFilePath: "/nats.conf",
					FileMode:          0o644,
				},
			},
			Networks:       []string{nw.Name},
			NetworkAliases: map[string][]string{nw.Name: {natsAlias}},
			WaitingFor:     wait.ForLog("Server is ready").WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err, "start NATS container")
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	// Build TCP URL for Go client (mapped host port).
	host, err := container.Host(ctx)
	require.NoError(t, err)
	tcpPort, err := container.MappedPort(ctx, "4222")
	require.NoError(t, err)
	tcpURL := fmt.Sprintf("nats://%s:%s", host, tcpPort.Port())

	nc, err := nats.Connect(tcpURL)
	require.NoError(t, err, "connect to NATS")
	t.Cleanup(func() {
		nc.Close()
	})

	// Build WS URL for TypeScript client (container-to-container via network alias).
	wsURL := fmt.Sprintf("ws://%s:8080", natsAlias)

	return nc, wsURL
}

// setupNode starts a Node container on the shared network, installs tsx + nats npm
// packages, and copies the client.ts script. Returns the container for exec calls.
func setupNode(t *testing.T, nw *testcontainers.DockerNetwork) testcontainers.Container {
	t.Helper()
	ctx := context.Background()

	scriptPath := filepath.Join("testdata", "client.ts")

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      testimages.Node,
			Cmd:        []string{"sh", "-c", "sleep 600"},
			Networks:   []string{nw.Name},
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
	nw := setupNetwork(t)
	nc, wsURL := setupNATS(t, nw)
	nodeContainer := setupNode(t, nw)

	// 2. Test parameters.
	account := "alice"
	roomID := "room-1"
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
	nw := setupNetwork(t)
	nc, wsURL := setupNATS(t, nw)
	nodeContainer := setupNode(t, nw)

	// 2. Generate a fresh P-256 key pair.
	privKey, err := ecdh.P256().GenerateKey(rand.Reader)
	require.NoError(t, err)
	pubKeyBytes := privKey.PublicKey().Bytes()
	privKeyBytes := privKey.Bytes()

	// 3. Test parameters.
	account := "alice"
	roomID := "room-1"
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
		PublicKey:  pubKeyBytes,
		PrivateKey: privKeyBytes,
	}
	err = sender.Send(account, *evt)
	require.NoError(t, err, "send room key event")

	// 7. Small delay to ensure key is received before the encrypted message.
	time.Sleep(500 * time.Millisecond)

	// 8. Encrypt a message with the room public key.
	encrypted, err := roomcrypto.Encode(plaintext, pubKeyBytes, version)
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
