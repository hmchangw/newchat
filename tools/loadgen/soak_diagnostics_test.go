package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A mismatch is only actionable once you know which field disagreed: a
// message_id/room_id/author mismatch is a service-side correctness failure,
// while content/deleted is almost always the harness reading a mutation the
// write path has not persisted yet. The class alone cannot tell them apart.
func TestSoakCollector_VerificationMismatchCarriesTheDisagreeingField(t *testing.T) {
	metrics := NewMetrics()
	collector := NewSoakCollector(metrics, time.Unix(100, 0), 0, time.Minute)

	require.NoError(t, collector.RecordVerification(&soakVerifyResult{
		Class: soakVerifyMismatch, Action: soakRPCGetMessage, Field: "content",
	}))
	require.NoError(t, collector.RecordVerification(&soakVerifyResult{
		Class: soakVerifyMismatch, Action: soakRPCGetMessage, Field: "author",
	}))

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakVerifications.WithLabelValues(
			string(soakRPCGetMessage), string(soakVerifyMismatch), "content"),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakVerifications.WithLabelValues(
			string(soakRPCGetMessage), string(soakVerifyMismatch), "author"),
	))
}

func TestSoakCollector_VerificationFieldIsEmptyWhenNothingDisagreed(t *testing.T) {
	metrics := NewMetrics()
	collector := NewSoakCollector(metrics, time.Unix(100, 0), 0, time.Minute)

	require.NoError(t, collector.RecordVerification(&soakVerifyResult{
		Class: soakVerifyOK, Action: soakRPCGetMessage,
	}))

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakVerifications.WithLabelValues(
			string(soakRPCGetMessage), string(soakVerifyOK), ""),
	))
}

// The field is a label, so it has to come from a closed set for the same reason
// the action and class do — one unbounded value would multiply the series.
func TestSoakCollector_RejectsAnUnboundedVerificationField(t *testing.T) {
	collector := NewSoakCollector(NewMetrics(), time.Unix(100, 0), 0, time.Minute)

	err := collector.RecordVerification(&soakVerifyResult{
		Class: soakVerifyMismatch, Action: soakRPCGetMessage,
		Field: "content of message msg-0001",
	})

	require.Error(t, err)
	assert.Empty(t, collector.Snapshot(time.Unix(101, 0)).Verifications)
}

// Every field the verifiers can assign must be accepted, or a real mismatch is
// dropped instead of reported.
func TestSoakCollector_AcceptsEveryFieldTheVerifiersAssign(t *testing.T) {
	for _, field := range []string{
		"", "message_id", "room_id", "author", "deleted", "content",
		"edited_at", "pagination",
	} {
		t.Run("field="+field, func(t *testing.T) {
			collector := NewSoakCollector(NewMetrics(), time.Unix(100, 0), 0, time.Minute)
			require.NoError(t, collector.RecordVerification(&soakVerifyResult{
				Class: soakVerifyMismatch, Action: soakRPCGetMessage, Field: field,
			}))
		})
	}
}

// A process that keeps restarting spends most of its life in warm-up, and the
// error counter used to be written only in the measured phase — so the run that
// most needs diagnosing is the one that reports almost nothing. The phase label
// keeps the two separable without hiding either.
func TestSoakCollector_CountsErrorsAndRetriesInBothPhases(t *testing.T) {
	start := time.Unix(100, 0)
	metrics := NewMetrics()
	collector := NewSoakCollector(metrics, start, time.Minute, time.Hour)

	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCSubscriptionList, Outcome: soakOutcomeFailed,
		ErrorClass: soakErrorTimeout, Retries: 3, At: start.Add(time.Second),
	}))
	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCSubscriptionList, Outcome: soakOutcomeFailed,
		ErrorClass: soakErrorTimeout, Retries: 2, At: start.Add(2 * time.Minute),
	}))

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakErrors.WithLabelValues(
			string(soakRPCSubscriptionList), string(soakErrorTimeout), "warmup"),
	), "a warm-up failure must still reach the error counter")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakErrors.WithLabelValues(
			string(soakRPCSubscriptionList), string(soakErrorTimeout), "measured"),
	))
	assert.Equal(t, float64(3), testutil.ToFloat64(
		metrics.SoakRetries.WithLabelValues(string(soakRPCSubscriptionList), "warmup"),
	))
	assert.Equal(t, float64(2), testutil.ToFloat64(
		metrics.SoakRetries.WithLabelValues(string(soakRPCSubscriptionList), "measured"),
	))
}

// The reported percentiles compare like with like, so latency stays
// measured-only: a warm-up sample is taken against cold caches and an unsettled
// topology, and folding it in would bias every drift number the run reports.
func TestSoakCollector_LeavesWarmupLatencyOutOfTheHistogram(t *testing.T) {
	start := time.Unix(100, 0)
	metrics := NewMetrics()
	collector := NewSoakCollector(metrics, start, time.Minute, time.Hour)

	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCSubscriptionList, Outcome: soakOutcomeSucceeded,
		At: start.Add(time.Second), Latency: 5 * time.Second,
	}))

	assert.Equal(t, 0, testutil.CollectAndCount(metrics.SoakRPCLatency),
		"a warm-up sample must not open a latency series")
}

// forbidden collapses two service answers that need opposite responses:
// "not_subscribed" means the harness verified with an account the membership
// lane had already removed, while "outside_access_window" means the account
// rejoined and its own older message now predates its history window. The
// class alone cannot separate them.
func TestClassifySoakRPCReason_SeparatesTheTwoForbiddenAnswers(t *testing.T) {
	notSubscribed := errcode.Forbidden("not subscribed to room",
		errcode.WithReason(errcode.MessageNotSubscribed))
	outsideWindow := errcode.Forbidden("message is outside access window",
		errcode.WithReason(errcode.MessageOutsideAccessWindow))

	assert.Equal(t, soakErrorReason("not_subscribed"), classifySoakRPCReason(notSubscribed))
	assert.Equal(t, soakErrorReason("outside_access_window"), classifySoakRPCReason(outsideWindow))
}

func TestClassifySoakRPCReason_IsEmptyWhenTheServiceSentNone(t *testing.T) {
	assert.Equal(t, soakErrorReason(""), classifySoakRPCReason(nil))
	assert.Equal(t, soakErrorReason(""), classifySoakRPCReason(errcode.NotFound("message not found")))
	assert.Equal(t, soakErrorReason(""), classifySoakRPCReason(context.DeadlineExceeded))
}

// The reason is a label, and errcode's registry lives in a _test.go file that
// cannot be imported, so the harness keeps its own allow-list. An unrecognised
// reason must fold into a single bucket rather than open a new series — and the
// bucket counting up is the signal to add the reason here.
func TestClassifySoakRPCReason_FoldsAnUnknownReasonIntoOneBucket(t *testing.T) {
	unknown := errcode.Forbidden("nope", errcode.WithReason(errcode.Reason("brand_new_reason")))

	assert.Equal(t, soakErrorReasonUnknown, classifySoakRPCReason(unknown))
}

func TestSoakCollector_CountsErrorReasonsSeparatelyFromClasses(t *testing.T) {
	start := time.Unix(100, 0)
	metrics := NewMetrics()
	collector := NewSoakCollector(metrics, start, 0, time.Minute)

	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCGetMessage, Outcome: soakOutcomeFailed,
		ErrorClass: soakErrorForbidden, ErrorReason: "not_subscribed",
		At: start.Add(time.Second),
	}))

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakErrorReasons.WithLabelValues(
			string(soakRPCGetMessage), string(soakErrorForbidden), "not_subscribed", "measured"),
	))
	// The existing error counter keeps its shape so dashboards built on it survive.
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakErrors.WithLabelValues(
			string(soakRPCGetMessage), string(soakErrorForbidden), "measured"),
	))
}

func TestSoakCollector_RejectsAnUnboundedErrorReason(t *testing.T) {
	collector := NewSoakCollector(NewMetrics(), time.Unix(100, 0), 0, time.Minute)

	err := collector.Record(&soakOperationSample{
		Action: soakRPCGetMessage, Outcome: soakOutcomeFailed,
		ErrorClass: soakErrorForbidden, ErrorReason: "account alice is not in room-42",
		At: time.Unix(101, 0),
	})

	require.Error(t, err)
}

// Classifying the reason is only useful if the call path carries it out to the
// sample the collector sees.
func TestSoakRPCClient_CarriesTheServiceReasonOutOfTheCall(t *testing.T) {
	envelope := []byte(`{"code":"forbidden","reason":"not_subscribed","error":"not subscribed to room"}`)
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{{data: envelope}}}
	client := newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, nil, nil)

	result, err := client.Call(context.Background(), soakRPCRequest{
		Action: soakRPCGetMessage, Subject: "chat.test", Timeout: time.Second,
	}, nil)

	require.Error(t, err)
	assert.Equal(t, soakErrorForbidden, result.ErrorClass)
	assert.Equal(t, soakErrorReason("not_subscribed"), result.ErrorReason)
}

func TestSoakRPCClient_LeavesTheReasonEmptyOnATransportTimeout(t *testing.T) {
	transport := &soakRPCFakeTransport{
		replies: []soakRPCFakeReply{{err: context.DeadlineExceeded}},
	}
	client := newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, nil, nil)

	result, err := client.Call(context.Background(), soakRPCRequest{
		Action: soakRPCGetMessage, Subject: "chat.test", Timeout: time.Second,
	}, nil)

	require.Error(t, err)
	assert.Equal(t, soakErrorTimeout, result.ErrorClass)
	assert.Equal(t, soakErrorReason(""), result.ErrorReason)
}

// The reconcile verifier is the path that produces the forbidden answers on
// get_message_by_id, so it is the one place the reason must not be dropped.
func TestSoakFailureRPCVerifier_ReportsTheServiceReasonOnAForbiddenReadBack(t *testing.T) {
	envelope := []byte(`{"code":"forbidden","reason":"not_subscribed","error":"not subscribed to room"}`)
	transport := &soakRPCFakeTransport{replies: []soakRPCFakeReply{{data: envelope}}}
	recorder := &soakVerifyResultCapture{}
	verifier := newSoakFailureRPCVerifier(
		"site-a",
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, nil, nil),
		nil,
		recorder,
		func() time.Time { return time.Unix(100, 0) },
	)

	_, err := verifier.Verify(context.Background(), &failureOperation{
		ID: "msg-1",
		Attributes: map[string]string{
			soakFailureAttributeRoomID:        "room-1",
			soakFailureAttributeAccount:       "user-1",
			soakFailureAttributeContentSHA256: "abc",
		},
	})

	require.Error(t, err)
	require.Len(t, recorder.results, 1)
	assert.Equal(t, soakErrorForbidden, recorder.results[0].RPCErrorClass)
	assert.Equal(t, soakErrorReason("not_subscribed"), recorder.results[0].RPCErrorReason)
}

type soakVerifyResultCapture struct {
	results []soakVerifyResult
}

func (c *soakVerifyResultCapture) Record(result *soakVerifyResult) {
	c.results = append(c.results, *result)
}

// Every lane that records an error class must record the reason beside it, or
// the reason counter is silently empty for that lane and the label looks like
// the service never sends one.
func TestSoakSamples_EveryLaneThatCarriesAnErrorClassAlsoCarriesItsReason(t *testing.T) {
	roomOutcome := soakRoomMutationOutcome{
		ErrorClass: soakErrorForbidden, ErrorReason: "not_room_member",
	}
	roomRecorder := &soakMutationSampleCapture{}
	lanes := &soakRoomLanes{recorder: roomRecorder}
	lanes.record(soakRPCMemberAdd, &roomOutcome)

	require.Len(t, roomRecorder.samples, 1)
	assert.Equal(t, soakErrorReason("not_room_member"), roomRecorder.samples[0].ErrorReason,
		"the room and member mutation lanes must report the service reason")
}

type soakMutationSampleCapture struct {
	samples []soakMutationSample
}

func (c *soakMutationSampleCapture) Record(sample soakMutationSample) {
	c.samples = append(c.samples, sample)
}

// The send lane carries the most traffic and is where the gatekeeper's own
// refusals arrive, so a reason dropped here is the one most worth having.
func TestSoakSendReply_CarriesTheGatekeeperReason(t *testing.T) {
	envelope := []byte(`{"code":"forbidden","reason":"not_subscribed","error":"not subscribed to room"}`)

	class := classifySoakRPCError(parseSoakErrorEnvelope(envelope))
	reason := classifySoakRPCReason(parseSoakErrorEnvelope(envelope))

	require.Equal(t, soakErrorForbidden, class)
	require.Equal(t, soakErrorReason("not_subscribed"), reason)
	assert.Contains(t, soakSendReplyResultFields(), "ErrorReason",
		"soakSendReplyResult must have somewhere to put the reason")
}

func soakSendReplyResultFields() []string {
	value := reflect.TypeOf(soakSendReplyResult{})
	fields := make([]string, 0, value.NumField())
	for i := range value.NumField() {
		fields = append(fields, value.Field(i).Name)
	}
	return fields
}

// The reason counter has to line up with the error counter it sits beside, or
// warm-up and measured traffic merge and the two cannot be reconciled.
func TestSoakCollector_ErrorReasonsCarryThePhaseErrorsDo(t *testing.T) {
	start := time.Unix(100, 0)
	metrics := NewMetrics()
	collector := NewSoakCollector(metrics, start, time.Minute, time.Hour)

	require.NoError(t, collector.Record(&soakOperationSample{
		Action: soakRPCGetMessage, Outcome: soakOutcomeFailed,
		ErrorClass: soakErrorForbidden, ErrorReason: "not_subscribed",
		At: start.Add(time.Second),
	}))

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakErrorReasons.WithLabelValues(
			string(soakRPCGetMessage), string(soakErrorForbidden), "not_subscribed", "warmup"),
	))
}
