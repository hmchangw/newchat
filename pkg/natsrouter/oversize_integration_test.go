//go:build integration

package natsrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

// cappedMaxPayload is small enough that an ordinary reply overflows it without
// allocating megabytes, and above the 1024-byte floor NATS enforces.
const cappedMaxPayload = 4096

// startCappedNATS runs a dedicated NATS container whose max_payload is capped.
// The process-shared testutil.NATS cannot be used here: its cap is the 1 MiB
// default and lowering it would apply to every other test in the run.
func startCappedNATS(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	// max_payload is config-file-only; nats-server exposes no CLI flag for it.
	conf := fmt.Sprintf("listen: 0.0.0.0:4222\nmax_payload: %d\n", cappedMaxPayload)

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        testimages.NATS,
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          []string{"--config", "/nats.conf"},
			Files: []testcontainers.ContainerFile{{
				Reader:            strings.NewReader(conf),
				ContainerFilePath: "/nats.conf",
				FileMode:          0o644,
			}},
			WaitingFor: wait.ForLog("Server is ready").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err, "start capped NATS container")
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "4222")
	require.NoError(t, err)
	return fmt.Sprintf("nats://%s:%s", host, port.Port())
}

func connectCapped(t *testing.T, url string) *o11ynats.Conn {
	t.Helper()
	nc, err := o11ynats.Connect(context.Background(), url, noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err, "connect to capped NATS")
	t.Cleanup(nc.Close)
	return nc
}

type bulkReq struct {
	Limit int `json:"limit"`
}

type bulkResp struct {
	Items []string `json:"items"`
}

// End-to-end proof of the oversize contract over a real broker: a real
// natsrouter route, a real o11y connection, a real request from a separate
// client. The handler succeeds and returns a response that simply does not fit,
// which before the fallback left the caller with nothing to read but a timeout
// — the one failure it cannot tell apart from a dead service.
func TestIntegration_OversizeReplyReturnsErrorEnvelope(t *testing.T) {
	url := startCappedNATS(t)
	r := Default(connectCapped(t, url), "integration-oversize")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Shutdown(ctx)
	})

	const subject = "chat.user.alice.request.history.load"
	Register(r, "chat.user.{account}.request.history.load", natsmetrics.MethodGetChannelHistory,
		func(c *Context, req bulkReq) (*bulkResp, error) {
			items := make([]string, req.Limit)
			for i := range items {
				items[i] = strings.Repeat("m", 64)
			}
			return &bulkResp{Items: items}, nil
		})

	client, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	t.Run("oversize reply returns response_too_large instead of timing out", func(t *testing.T) {
		reply, err := client.Request(subject, []byte(`{"limit":500}`), 5*time.Second)
		require.NoError(t, err, "the caller must get an answer, not a timeout")

		var got errcode.Error
		require.NoError(t, json.Unmarshal(reply.Data, &got))
		assert.Equal(t, errcode.CodeInternal, got.Code)
		assert.Equal(t, errcode.ResponseTooLarge, got.Reason)
		assert.Less(t, len(reply.Data), cappedMaxPayload, "the fallback envelope must itself fit")
	})

	// The fallback must not fire on replies that fit — otherwise it would mask
	// successful responses rather than rescue failed ones.
	t.Run("reply that fits is unaffected", func(t *testing.T) {
		reply, err := client.Request(subject, []byte(`{"limit":2}`), 5*time.Second)
		require.NoError(t, err)

		var got bulkResp
		require.NoError(t, json.Unmarshal(reply.Data, &got))
		assert.Len(t, got.Items, 2)
	})
}

// The same guarantee on the error lane: an errcode reply too large to send
// still has to reach the caller as an error envelope.
func TestIntegration_OversizeErrorReplyReturnsErrorEnvelope(t *testing.T) {
	url := startCappedNATS(t)
	r := Default(connectCapped(t, url), "integration-oversize-err")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Shutdown(ctx)
	})

	const subject = "chat.user.bob.request.rooms.info"
	Register(r, "chat.user.{account}.request.rooms.info", natsmetrics.MethodBatchGetRoomsInfo,
		func(c *Context, _ bulkReq) (*bulkResp, error) {
			return nil, errcode.BadRequest(strings.Repeat("y", cappedMaxPayload*2))
		})

	client, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	reply, err := client.Request(subject, []byte(`{}`), 5*time.Second)
	require.NoError(t, err, "the caller must get an answer, not a timeout")

	var got errcode.Error
	require.NoError(t, json.Unmarshal(reply.Data, &got))
	assert.Equal(t, errcode.CodeInternal, got.Code)
	assert.Equal(t, errcode.ResponseTooLarge, got.Reason)
}
