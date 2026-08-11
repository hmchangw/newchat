package natsutil

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// This file is in package natsutil (not natsutil_test) because it exercises
// unexported helpers. The rest of the package's tests stay external.

func TestSlowConsumerFields_RealSubscription(t *testing.T) {
	nc := newLocalConn(t)
	sub, err := nc.QueueSubscribe("slow.subject", "slow-queue", func(*nats.Msg) {})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	got := fieldMap(t, slowConsumerFields(sub))

	require.Equal(t, "slow.subject", got["subject"])
	require.Equal(t, "slow-queue", got["queue"])
	require.Contains(t, got, "dropped")
	require.Contains(t, got, "pending_msgs")
	require.Contains(t, got, "pending_bytes")
	require.Contains(t, got, "limit_msgs")
	require.Contains(t, got, "limit_bytes")
}

func TestSlowConsumerFields_NilSubscription(t *testing.T) {
	require.NotPanics(t, func() {
		got := fieldMap(t, slowConsumerFields(nil))
		require.Equal(t, "unknown", got["subject"])
	})
}

func TestSlowConsumerFields_ClosedSubscription(t *testing.T) {
	nc := newLocalConn(t)
	sub, err := nc.Subscribe("closed.subject", func(*nats.Msg) {})
	require.NoError(t, err)
	require.NoError(t, sub.Unsubscribe())

	require.NotPanics(t, func() {
		got := fieldMap(t, slowConsumerFields(sub))
		require.Equal(t, "closed.subject", got["subject"])
	})
}

func TestLogSlowConsumer_LevelSelection(t *testing.T) {
	tests := []struct {
		name      string
		dropped   int
		wantLevel string
	}{
		{name: "no drops yet warns", dropped: 0, wantLevel: "WARN"},
		{name: "drops are an error", dropped: 3, wantLevel: "ERROR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			logSlowConsumerAt(log, tt.dropped, []any{"subject", "x"})

			var rec map[string]any
			require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
			require.Equal(t, tt.wantLevel, rec["level"])
		})
	}
}

func fieldMap(t *testing.T, fields []any) map[string]any {
	t.Helper()
	require.Zero(t, len(fields)%2, "slog fields must be key-value pairs")
	out := make(map[string]any, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		require.True(t, ok, "field key at %d is not a string", i)
		out[key] = fields[i+1]
	}
	return out
}

func newLocalConn(t *testing.T) *nats.Conn {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}
