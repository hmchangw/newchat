package natsutil

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file is in package natsutil (not natsutil_test) because it exercises
// unexported helpers. The rest of the package's tests stay external.

func TestSlowConsumerFields_RealSubscription(t *testing.T) {
	nc := newLocalConn(t)
	sub, err := nc.QueueSubscribe("slow.subject", "slow-queue", func(*nats.Msg) {})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	dropped, ok := subDropped(sub)
	require.True(t, ok, "a live subscription must report its drop count")

	got := fieldMap(t, slowConsumerFields(sub, dropped, ok))

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
		dropped, ok := subDropped(nil)
		require.False(t, ok)
		got := fieldMap(t, slowConsumerFields(nil, dropped, ok))
		require.Equal(t, "unknown", got["subject"])
	})
}

// A closed subscription must omit the counters it cannot read rather than
// reporting a bogus zero — an ERROR line saying "messages dropped" with a
// dropped:0 field would be worse than no field at all.
func TestSlowConsumerFields_ClosedSubscription(t *testing.T) {
	nc := newLocalConn(t)
	sub, err := nc.Subscribe("closed.subject", func(*nats.Msg) {})
	require.NoError(t, err)
	require.NoError(t, sub.Unsubscribe())

	require.NotPanics(t, func() {
		dropped, ok := subDropped(sub)
		require.False(t, ok, "a closed subscription cannot report a drop count")

		got := fieldMap(t, slowConsumerFields(sub, dropped, ok))
		require.Equal(t, "closed.subject", got["subject"])
		require.NotContains(t, got, "dropped")
		require.NotContains(t, got, "pending_msgs")
		require.NotContains(t, got, "limit_msgs")
	})
}

// logSlowConsumer is the only function connect.go's ErrorHandler calls, so the
// level rule and the field wiring must be verified on that path and not just on
// the helpers beneath it.
func TestLogSlowConsumer_LiveSubscriptionWarnsWithNoDrops(t *testing.T) {
	nc := newLocalConn(t)
	sub, err := nc.QueueSubscribe("live.subject", "live-queue", func(*nats.Msg) {})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NotPanics(t, func() { logSlowConsumer(log, sub) })

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	require.Equal(t, "WARN", rec["level"], "a fresh subscription has dropped nothing yet")
	require.Equal(t, "live.subject", rec["subject"])
	require.Equal(t, "live-queue", rec["queue"])
	require.Equal(t, float64(0), rec["dropped"])
}

func TestLogSlowConsumer_NilSubscriptionDoesNotPanic(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NotPanics(t, func() { logSlowConsumer(log, nil) })

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	require.Equal(t, "WARN", rec["level"])
	require.Equal(t, "unknown", rec["subject"])
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

// A response-inbox subscription must not put its subject on the metric.
//
// nats.go opens one shared response subscription per connection, on
// `_INBOX.<per-connection random token>.*`. At any instant that is a single
// series, which is why this label looked bounded and why the semgrep rule
// flagging it was suppressed. But the token is regenerated on every connection,
// so every process restart and every reconnect mints a label value that never
// appears again — unbounded growth spread over time rather than all at once.
// The contract's own forbidden-label list already names "concrete request/reply
// inboxes".
func TestSubjectLabel_CollapsesResponseInboxes(t *testing.T) {
	tests := []struct {
		name    string
		subject string
		want    string
	}{
		{name: "response inbox wildcard", subject: "_INBOX.hK4bQ2ZmTpVn1aXc.*", want: "inbox"},
		{name: "concrete response inbox", subject: "_INBOX.hK4bQ2ZmTpVn1aXc.7", want: "inbox"},
		{name: "custom inbox prefix is still an inbox", subject: "_INBOX.chat.abc123", want: "inbox"},
		{name: "registered room subject is kept", subject: "chat.room.canonical.site-a.create", want: "chat.room.canonical.site-a.create"},
		{name: "wildcarded router pattern is kept", subject: "chat.user.*.request.room.*.site-a.member.list", want: "chat.user.*.request.room.*.site-a.member.list"},
		{name: "empty", subject: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, subjectLabel(tt.subject))
		})
	}
}
