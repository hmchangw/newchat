package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hmchangw/chat/pkg/natsmetrics"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
)

func startTestNATS(t *testing.T) *o11ynats.Conn {
	t.Helper()
	opts := &natsserver.Options{Port: -1}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := o11ynats.Connect(context.Background(), ns.ClientURL(), noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

func TestHistoryParentFetcher_FetchQuotedParent(t *testing.T) {
	const (
		account   = "alice"
		roomID    = "room-1"
		siteID    = "site-a"
		messageID = "parent-msg-uuid"
		baseURL   = "http://localhost:3000"
	)
	parentCreatedAt := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	threadParentCreatedAt := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)

	t.Run("happy path — returns projected snapshot with thread context and messageLink", func(t *testing.T) {
		nc := startTestNATS(t)

		parent := cassandra.Message{
			MessageID:             messageID,
			RoomID:                roomID,
			Sender:                cassandra.Participant{ID: "u-bob", Account: "bob", EngName: "Bob Chen"},
			CreatedAt:             parentCreatedAt,
			Msg:                   "a reply inside thread T",
			Mentions:              []cassandra.Participant{{ID: "u-carol", Account: "carol", EngName: "Carol Lee"}},
			ThreadParentID:        "thread-parent-uuid",
			ThreadParentCreatedAt: &threadParentCreatedAt,
		}

		// Stand up a stub responder on the exact subject the fetcher should publish on.
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(parent)
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		pub, requests := requestMetricFor(t)
		fetcher := newHistoryParentFetcher(nc, baseURL, pub)
		got, err := fetcher.FetchQuotedParent(context.Background(), account, roomID, siteID, messageID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, int64(1), requests("success"), "a served history request must be counted as success")
		assert.Equal(t, messageID, got.MessageID)
		assert.Equal(t, roomID, got.RoomID)
		assert.Equal(t, "a reply inside thread T", got.Msg)
		assert.Equal(t, "bob", got.Sender.Account)
		assert.Equal(t, parentCreatedAt, got.CreatedAt.UTC())
		require.Len(t, got.Mentions, 1)
		assert.Equal(t, "carol", got.Mentions[0].Account)
		assert.Equal(t, baseURL+"/"+roomID+"/"+messageID, got.MessageLink)
		assert.Equal(t, "thread-parent-uuid", got.ThreadParentID)
		require.NotNil(t, got.ThreadParentCreatedAt)
		assert.Equal(t, threadParentCreatedAt, got.ThreadParentCreatedAt.UTC())
	})

	t.Run("captures the parent's decoded attachments into the snapshot", func(t *testing.T) {
		nc := startTestNATS(t)
		parent := cassandra.Message{
			MessageID:          messageID,
			RoomID:             roomID,
			Sender:             cassandra.Participant{ID: "u-bob", Account: "bob"},
			CreatedAt:          parentCreatedAt,
			Msg:                "has an attachment",
			DecodedAttachments: []cassandra.Attachment{{ID: "f1", Title: "a.png", Type: "file"}},
		}
		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(parent)
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		fetcher := newHistoryParentFetcher(nc, baseURL, natsmetrics.Publisher{})
		got, err := fetcher.FetchQuotedParent(context.Background(), account, roomID, siteID, messageID)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Len(t, got.DecodedAttachments, 1)
		assert.Equal(t, "f1", got.DecodedAttachments[0].ID)
	})

	t.Run("history returns errcode error envelope — returns error", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.MsgGet(account, roomID, siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.NotFound("message not found"))
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		pub, requests := requestMetricFor(t)
		fetcher := newHistoryParentFetcher(nc, baseURL, pub)
		got, err := fetcher.FetchQuotedParent(context.Background(), account, roomID, siteID, messageID)
		require.Error(t, err)
		assert.Nil(t, got)
		assert.Equal(t, int64(1), requests("success"),
			"a replied-to request is a transport success even when the payload is an error envelope")
		var ec *errcode.Error
		require.ErrorAs(t, err, &ec, "the history error envelope must survive as a typed errcode")
		assert.Equal(t, errcode.CodeNotFound, ec.Code)
	})

	t.Run("no responder — returns error", func(t *testing.T) {
		nc := startTestNATS(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".

		pub, requests := requestMetricFor(t)
		fetcher := newHistoryParentFetcher(nc, baseURL, pub)
		got, err := fetcher.FetchQuotedParent(context.Background(), account, roomID, siteID, messageID)
		require.Error(t, err)
		assert.Nil(t, got)
		assert.Equal(t, int64(1), requests("no_responders"), "an unanswered history request must be counted as no_responders")
	})
}

// requestMetricFor builds a Publisher backed by a manual reader so a test can
// assert the history request outcome the fetcher records. Injecting a zero
// natsmetrics.Publisher makes Request a no-op, which proves nothing.
func requestMetricFor(t *testing.T) (natsmetrics.Publisher, func(outcome string) int64) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	pub := natsmetrics.NewFromProvider(mp).Publisher("message-gatekeeper", "s1")
	return pub, func(outcome string) int64 {
		t.Helper()
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		// operationTotal guards the assertion itself: summing only the requested
		// outcome would still pass if one request were recorded under two.
		var total, operationTotal int64
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != "chat.nats.requests" {
					continue
				}
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok, "chat.nats.requests must be a counter")
				for _, dp := range sum.DataPoints {
					got := map[string]string{}
					for _, kv := range dp.Attributes.ToSlice() {
						got[string(kv.Key)] = kv.Value.AsString()
					}
					if got["operation"] != string(natsmetrics.OperationHistoryGetMessage) {
						continue
					}
					operationTotal += dp.Value
					if got["outcome"] == outcome {
						total += dp.Value
					}
				}
			}
		}
		require.Equal(t, int64(1), operationTotal, "one history request must record exactly one outcome")
		return total
	}
}
