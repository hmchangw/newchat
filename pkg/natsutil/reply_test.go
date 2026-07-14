package natsutil_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

func TestMarshalResponse(t *testing.T) {
	room := model.Room{ID: "1", Name: "general"}
	data, err := natsutil.MarshalResponse(room)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got model.Room
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != "1" || got.Name != "general" {
		t.Errorf("got %+v", got)
	}
}

func startTestNATS(t *testing.T) *nats.Conn {
	t.Helper()
	return startTestNATSWithMaxPayload(t, 0)
}

// startTestNATSWithMaxPayload starts an embedded server; maxPayload > 0 caps the
// negotiated max_payload so an oversize reply can be exercised deterministically
// without marshalling megabytes.
func startTestNATSWithMaxPayload(t *testing.T, maxPayload int32) *nats.Conn {
	t.Helper()
	opts := &natsserver.Options{Port: -1}
	if maxPayload > 0 {
		opts.MaxPayload = maxPayload
	}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

// A reply that would exceed the negotiated max_payload must come back as the
// "response too large" error envelope, not a silent timeout (issue #404).
func TestReplyJSON_OversizeReply(t *testing.T) {
	nc := startTestNATSWithMaxPayload(t, 4096)
	const subj = "test.replyjson.oversize"
	sub, err := nc.Subscribe(subj, func(m *nats.Msg) {
		natsutil.ReplyJSON(m, model.Room{ID: "r1", Name: strings.Repeat("x", 8192)})
	})
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	reply, err := nc.Request(subj, []byte(`{}`), 2*time.Second)
	require.NoError(t, err) // the bug: this used to time out (no reply at all)
	body := string(reply.Data)
	if !strings.Contains(body, `"code":"internal"`) ||
		!strings.Contains(body, `"reason":"response_too_large"`) ||
		!strings.Contains(body, `"error":"response payload exceeds maximum size"`) {
		t.Fatalf("expected oversize error envelope, got: %s", body)
	}
}

// ReplyJSON's marshal-failure branch writes a fixed internal-error envelope.
// Pass an unmarshalable value (channels can't be JSON-encoded) and assert the
// fallback envelope reaches the requester rather than leaving them hanging.
func TestReplyJSON_MarshalFailure(t *testing.T) {
	nc := startTestNATS(t)
	const subj = "test.replyjson.failure"
	sub, err := nc.Subscribe(subj, func(m *nats.Msg) {
		natsutil.ReplyJSON(m, make(chan int))
	})
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	reply, err := nc.Request(subj, []byte(`{}`), 2*time.Second)
	require.NoError(t, err)
	body := string(reply.Data)
	if !strings.Contains(body, `"code":"internal"`) || !strings.Contains(body, `"error":"internal error"`) {
		t.Fatalf("expected fallback internal-error envelope, got: %s", body)
	}
}

func TestReplyJSON_HappyPath(t *testing.T) {
	nc := startTestNATS(t)
	const subj = "test.replyjson.happy"
	sub, err := nc.Subscribe(subj, func(m *nats.Msg) {
		natsutil.ReplyJSON(m, model.Room{ID: "r1", Name: "general"})
	})
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	reply, err := nc.Request(subj, []byte(`{}`), 2*time.Second)
	require.NoError(t, err)
	var got model.Room
	require.NoError(t, json.Unmarshal(reply.Data, &got))
	if got.ID != "r1" {
		t.Fatalf("got %+v", got)
	}
}

// TestMarshalError / TestTryParseError / TestMarshalErrorWithCode were removed
// when the legacy ErrorResponse helpers were deleted. Client-facing error
// envelope marshalling is covered by pkg/errcode/errnats/reply_test.go;
// envelope parsing is covered by pkg/errcode/parse_test.go.
