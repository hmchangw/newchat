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
		RowsCounted: true, ReplyBytes: 98_304, Rows: 40,
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
		RowsCounted: true, ReplyBytes: 98_304, Rows: 40,
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
		RowsCounted: true, ReplyBytes: 21, Rows: 0,
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

// A read whose Messages is a constant (get_message_by_id always returns one)
// or a server-side total (subscription.count returns the user's whole count,
// not rows in the reply) is not a row count. Marking every read sample as one
// would put those in the same distribution as a real page.
func TestSoakReadSample_CountRowsMarksOnlyRealCounts(t *testing.T) {
	counted := soakReadSample{Action: soakRPCMemberList}
	counted.countRows(7)
	assert.Equal(t, 7, counted.Messages)
	assert.True(t, counted.RowsCounted)

	constant := soakReadSample{Action: soakRPCGetMessage, Messages: 1}
	assert.False(t, constant.RowsCounted, "a hardcoded 1 is not a row count")
}

func TestSoakReadCollectorRecorder_ObservesOnlyCountedRows(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	for name, tc := range map[string]struct {
		sample soakReadSample
		want   int
	}{
		"a counted page is observed": {
			sample: soakReadSample{
				Action: soakRPCSubscriptionList, Latency: time.Millisecond,
				Messages: 40, RowsCounted: true, ReplyBytes: 98_304,
			},
			want: 1,
		},
		"an uncounted read is not": {
			sample: soakReadSample{
				Action: soakRPCGetMessage, Latency: time.Millisecond,
				Messages: 1, ReplyBytes: 512,
			},
			want: 0,
		},
	} {
		t.Run(name, func(t *testing.T) {
			metrics := NewMetrics()
			recorder := &soakReadCollectorRecorder{
				collector: NewSoakCollector(metrics, start, 0, time.Hour),
				now:       func() time.Time { return start.Add(time.Second) },
			}

			sample := tc.sample
			recorder.Record(&sample)

			assert.Equal(t, tc.want, testutil.CollectAndCount(metrics.SoakRows))
		})
	}
}

// room_state_read is written by two lanes: RoomState returns a member list
// whose length is the number the histogram exists to show, and RoomInfoFor asks
// for exactly one room. Counting the second stands a constant 1 in the same
// distribution — the defect get_message_by_id is already excluded for.
func TestSoakRoomReader_PointLookupsAreNotCountedAsPages(t *testing.T) {
	for name, tc := range map[string]struct {
		reply string
		call  func(*soakRoomReader, context.Context) error
	}{
		"room info answers for one room": {
			reply: `{"rooms":[{"roomId":"room-1","found":true}]}`,
			call: func(r *soakRoomReader, ctx context.Context) error {
				_, err := r.RoomInfoFor(ctx, "room-1")
				return err
			},
		},
		"subscription-for answers zero or one": {
			reply: `{"subscriptions":[{"roomId":"room-1"}],"total":1}`,
			call: func(r *soakRoomReader, ctx context.Context) error {
				_, err := r.SubscriptionFor(ctx, "user-a0", "room-1")
				return err
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			transport := &soakRoomOpsTransport{reply: []byte(tc.reply)}
			reader, _, recorder := newSoakRoomReadFixture(t, transport, 1)

			require.NoError(t, tc.call(reader, context.Background()))

			require.Len(t, recorder.samples, 1)
			assert.False(t, recorder.samples[0].RowsCounted)
		})
	}
}

// The member list shares that action label and is a genuine page, so excluding
// the point lookups must not empty the distribution they were polluting.
func TestSoakRoomReader_RoomStateRemainsCounted(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"members":[{"id":"m1"},{"id":"m2"}]}`),
	}
	reader, _, recorder := newSoakRoomReadFixture(t, transport, 1)

	_, err := reader.RoomState(context.Background(), "room-1", "user-a0")

	require.NoError(t, err)
	require.Len(t, recorder.samples, 1)
	assert.True(t, recorder.samples[0].RowsCounted)
	assert.Equal(t, 2, recorder.samples[0].Messages)
}

func TestSoakUserReader_SubscriptionByRoomIsNotCountedAsAPage(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"subscriptions":[{"roomId":"room-1"}],"total":1}`),
	}
	reader, recorder := newSoakUserReadFixture(t, transport, 1)

	require.NoError(t, reader.SubscriptionByRoom(context.Background()))

	require.Len(t, recorder.samples, 1)
	assert.False(t, recorder.samples[0].RowsCounted)
}
