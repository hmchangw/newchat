package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	o11ynats "github.com/flywindy/o11y/nats"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// benchEnvelope is the reply a peer sends for the commonest error outcome on
// this path: the thread parent is gone.
var benchEnvelope = []byte(`{"code":"not_found","reason":"thread_parent_not_found","error":"message not found"}`)

func benchNATS(b *testing.B, payload []byte) *o11ynats.Conn {
	b.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	if err != nil {
		b.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		b.Fatal("nats not ready")
	}
	b.Cleanup(ns.Shutdown)
	nc, err := o11ynats.Connect(context.Background(), ns.ClientURL(), noop.NewTracerProvider(), propagation.TraceContext{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(nc.Close)
	if _, err := nc.Subscribe(context.Background(), subject.MsgGet("alice", "room-1", "site-a"),
		func(_ context.Context, m *nats.Msg) { _ = m.Respond(payload) }); err != nil {
		b.Fatal(err)
	}
	return nc
}

// fetchParentViaFromReply mirrors FetchParent exactly, swapping only the
// envelope check, so the benchmark isolates that one decision.
func fetchParentViaFromReply(ctx context.Context, nc *o11ynats.Conn, account, roomID, siteID, messageID string) (*ParentMessageInfo, error) {
	reqBytes, err := sonic.Marshal(getMessageByIDRequest{MessageID: messageID})
	if err != nil {
		return nil, errcode.MarshalFailed("GetMessageByID request", err)
	}
	msg, err := nc.Request(ctx, subject.MsgGet(account, roomID, siteID), reqBytes, parentFetchTimeout)
	if err != nil {
		return nil, natsutil.RequestFailure(fmt.Sprintf("history request for parent %s", messageID), err)
	}
	if remoteErr := errcode.FromReply(msg.Data); remoteErr != nil {
		return nil, remoteErr
	}
	var parent parentMessageProjection
	if err := sonic.Unmarshal(msg.Data, &parent); err != nil {
		return nil, fmt.Errorf("unmarshal parent message %s: %w", messageID, err)
	}
	return &ParentMessageInfo{SenderAccount: parent.Sender.Account, CreatedAt: parent.CreatedAt}, nil
}

// BenchmarkRoundTripOnly is the floor: the NATS request/reply the envelope
// check is a rounding error on top of.
func BenchmarkRoundTripOnly(b *testing.B) {
	nc := benchNATS(b, benchEnvelope)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := nc.Request(ctx, subject.MsgGet("alice", "room-1", "site-a"), []byte(`{}`), parentFetchTimeout); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFetchParentError_Parse is the shipped code on the error path.
func BenchmarkFetchParentError_Parse(b *testing.B) {
	nc := benchNATS(b, benchEnvelope)
	f := newHistoryParentFetcher(nc, natsmetrics.Publisher{})
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := f.FetchParent(ctx, "alice", "room-1", "site-a", "parent-msg"); err == nil {
			b.Fatal("expected error")
		}
	}
}

// BenchmarkFetchParentError_FromReply is the same path under PR #477.
func BenchmarkFetchParentError_FromReply(b *testing.B) {
	nc := benchNATS(b, benchEnvelope)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := fetchParentViaFromReply(ctx, nc, "alice", "room-1", "site-a", "parent-msg"); err == nil {
			b.Fatal("expected error")
		}
	}
}
