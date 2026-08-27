package main

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A page that creeps toward the broker's payload ceiling is invisible today:
// the run only reports latency and an outcome, so the first symptom is the
// reply failing outright. Sizing every good reply is what turns that into a
// trend somebody can watch.
func TestSoakRPCClient_RecordsTheReplySize(t *testing.T) {
	reply := []byte(`{"messageId":"m1"}`)
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{{data: reply}}}
	client := newCarrierTestClient(transport, 1)

	var response soakUnpinMessageResponse
	result, err := client.Call(context.Background(), soakRPCRequest{
		Action: soakRPCGetMessage, Subject: "chat.test",
		Timeout: time.Second, RetryMode: soakRetrySafe,
	}, &response)

	require.NoError(t, err)
	assert.Equal(t, len(reply), result.ReplyBytes)
}

// A transport failure has no reply to size, and an oversize failure carries
// only the compact envelope — recording either would drag the distribution
// down with values that are not page sizes.
func TestSoakRPCClient_LeavesReplySizeZeroWhenNothingCameBack(t *testing.T) {
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{
		{err: context.DeadlineExceeded},
	}}
	client := newCarrierTestClient(transport, 1)

	result, err := client.Call(context.Background(), soakRPCRequest{
		Action: soakRPCGetMessage, Subject: "chat.test",
		Timeout: time.Second, RetryMode: soakRetryNever,
	}, nil)

	require.Error(t, err)
	assert.Zero(t, result.ReplyBytes)
}

func TestSoakRPCClient_OversizeEnvelopeIsNotSizedAsAPage(t *testing.T) {
	envelope := `{"code":"internal","error":"response payload exceeds maximum size","reason":"response_too_large"}`
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{{data: []byte(envelope)}}}
	client := newCarrierTestClient(transport, 1)

	result, err := client.Call(context.Background(), soakRPCRequest{
		Action: soakRPCSubscriptionList, Subject: "chat.test",
		Timeout: time.Second, RetryMode: soakRetryNever,
	}, nil)

	require.Error(t, err)
	assert.Zero(t, result.ReplyBytes)
}

func newMetricsTestCollector(t *testing.T, start time.Time) (*SoakCollector, *Metrics) {
	t.Helper()
	metrics := NewMetrics()
	return NewSoakCollector(metrics, start, 0, time.Hour), metrics
}

func TestSoakCollector_ObservesPageSizeAndRowCount(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	collector, metrics := newMetricsTestCollector(t, start)

	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCSubscriptionList, Outcome: soakOutcomeSucceeded,
		At: start.Add(time.Second), Latency: time.Millisecond,
		HasPage: true, ReplyBytes: 98_304, Rows: 40,
	}))

	assert.Equal(t, 1, testutil.CollectAndCount(metrics.SoakReplyBytes))
	assert.Equal(t, 1, testutil.CollectAndCount(metrics.SoakRows))
}

// Warmup traffic runs against cold caches and an unfilled catalog; folding it
// into the same distribution as the measured phase is what the collector
// already refuses to do for latency.
func TestSoakCollector_SkipsWarmupPages(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	metrics := NewMetrics()
	collector := NewSoakCollector(metrics, start, time.Minute, time.Hour)

	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCSubscriptionList, Outcome: soakOutcomeSucceeded,
		At: start.Add(time.Second), Latency: time.Millisecond,
		HasPage: true, ReplyBytes: 98_304, Rows: 40,
	}))

	assert.Zero(t, testutil.CollectAndCount(metrics.SoakReplyBytes))
	assert.Zero(t, testutil.CollectAndCount(metrics.SoakRows))
}

// A failed call produced no page. Recording a zero would report a healthy
// small reply where the run in fact got nothing.
func TestSoakCollector_SkipsPagesFromFailedCalls(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	collector, metrics := newMetricsTestCollector(t, start)

	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCSubscriptionList, Outcome: soakOutcomeFailed,
		At: start.Add(time.Second), Latency: time.Millisecond,
		ErrorClass: soakErrorResponseTooLarge,
	}))

	assert.Zero(t, testutil.CollectAndCount(metrics.SoakReplyBytes))
	assert.Zero(t, testutil.CollectAndCount(metrics.SoakRows))
}

// An empty page is a real answer — a user with no rooms — so it must be
// counted rather than dropped alongside the failures.
func TestSoakCollector_CountsAnEmptyPage(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	collector, metrics := newMetricsTestCollector(t, start)

	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCSubscriptionList, Outcome: soakOutcomeSucceeded,
		At: start.Add(time.Second), Latency: time.Millisecond,
		HasPage: true, ReplyBytes: 21, Rows: 0,
	}))

	assert.Equal(t, 1, testutil.CollectAndCount(metrics.SoakRows))
}

// Only paged reads populate Rows. A mutation has no concept of one, so
// observing its zero would stand a solid bar at rows=0 next to the reads and
// read as "this action returns nothing" rather than "this action has no page".
func TestSoakCollector_SkipsActionsThatHaveNoPage(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	collector, metrics := newMetricsTestCollector(t, start)

	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCReact, Outcome: soakOutcomeSucceeded,
		At: start.Add(time.Second), Latency: time.Millisecond,
	}))

	assert.Zero(t, testutil.CollectAndCount(metrics.SoakRows))
	assert.Zero(t, testutil.CollectAndCount(metrics.SoakReplyBytes))
}
