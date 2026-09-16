package retrylane_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/retrylane"
)

func TestDedupID(t *testing.T) {
	assert.Equal(t, "MESSAGES-CANONICAL-site1:42:message-worker",
		retrylane.DedupID("MESSAGES-CANONICAL-site1", 42, "message-worker"))
}

func TestDedupIDIsDeterministic(t *testing.T) {
	a := retrylane.DedupID("S", 7, "c")
	b := retrylane.DedupID("S", 7, "c")
	assert.Equal(t, a, b, "escalation is publish-then-ack; a crash-retried escalation must dedup")
}

func TestReasonFor(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"not found", errcode.NotFound("room not found"), "not_found"},
		{"wrapped errcode", fmt.Errorf("outer: %w", errcode.Conflict("dup")), "conflict"},
		{"plain infra error", errors.New("dial tcp: connection refused"), "internal"},
		{"nil", nil, "internal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, retrylane.ReasonFor(tt.err))
		})
	}
}

func TestReasonForNeverLeaksTheErrorText(t *testing.T) {
	secret := "user-said-something-private"
	got := retrylane.ReasonFor(errors.New(secret))
	assert.NotContains(t, got, secret, "the reason is a bounded category, never the error string")
}

func meta(stream string, seq uint64, delivered uint64) *jetstream.MsgMetadata {
	return &jetstream.MsgMetadata{
		Stream:       stream,
		NumDelivered: delivered,
		Sequence:     jetstream.SequencePair{Stream: seq},
	}
}

func TestBuildHeadersPopulatesProvenance(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000).UTC()
	got := retrylane.BuildHeaders(nil, meta("MESSAGES-CANONICAL-site1", 42, 4),
		"chat.msg.canonical.site1.created", "message-worker", "internal", now)

	assert.Equal(t, "MESSAGES-CANONICAL-site1", got.Get(retrylane.HeaderOriginStream))
	assert.Equal(t, "42", got.Get(retrylane.HeaderOriginSeq))
	assert.Equal(t, "chat.msg.canonical.site1.created", got.Get(retrylane.HeaderOriginSubject))
	assert.Equal(t, "message-worker", got.Get(retrylane.HeaderConsumer))
	assert.Equal(t, "internal", got.Get(retrylane.HeaderReason))
	assert.Equal(t, "4", got.Get(retrylane.HeaderAttempt))
	assert.Equal(t, "1700000000000", got.Get(retrylane.HeaderFirstFailedAt))
}

func TestBuildHeadersCarriesCorrelationThrough(t *testing.T) {
	in := nats.Header{}
	in.Set(natsutil.RequestIDHeader, "01970a4f-8c2d-7c9a-abcd-e0123456789f")
	in.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

	got := retrylane.BuildHeaders(in, meta("S", 1, 4), "subj", "c", "internal", time.Now())

	assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f", got.Get(natsutil.RequestIDHeader),
		"a retry twelve minutes later must land in the same trace lineage")
	assert.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", got.Get("traceparent"))
}

func TestBuildHeadersAccumulatesAttemptsAcrossLanes(t *testing.T) {
	in := nats.Header{}
	in.Set(retrylane.HeaderAttempt, "4")

	got := retrylane.BuildHeaders(in, meta("S", 1, 2), "subj", "c", "internal", time.Now())

	assert.Equal(t, "6", got.Get(retrylane.HeaderAttempt),
		"attempts are cumulative: 4 on the hot lane plus 2 on the retry lane")
}

func TestBuildHeadersPreservesFirstFailedAt(t *testing.T) {
	in := nats.Header{}
	in.Set(retrylane.HeaderFirstFailedAt, "1600000000000")

	got := retrylane.BuildHeaders(in, meta("S", 1, 2), "subj", "c", "internal", time.Now())

	assert.Equal(t, "1600000000000", got.Get(retrylane.HeaderFirstFailedAt),
		"first failure time must survive every hop or time-to-dead-letter is wrong")
}

func TestBuildHeadersDoesNotMutateInput(t *testing.T) {
	in := nats.Header{}
	in.Set(retrylane.HeaderAttempt, "4")

	_ = retrylane.BuildHeaders(in, meta("S", 1, 2), "subj", "c", "internal", time.Now())

	assert.Equal(t, "4", in.Get(retrylane.HeaderAttempt), "input headers belong to the live message")
}

func TestBuildHeadersHandlesNilMetadata(t *testing.T) {
	got := retrylane.BuildHeaders(nil, nil, "subj", "c", "internal", time.Now())
	require.NotNil(t, got)
	assert.Equal(t, "c", got.Get(retrylane.HeaderConsumer))
	assert.Equal(t, "", got.Get(retrylane.HeaderOriginStream))
}
