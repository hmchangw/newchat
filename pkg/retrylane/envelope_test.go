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

func TestBuildHeadersForwardsEveryInboundHeader(t *testing.T) {
	in := nats.Header{}
	in.Set(natsutil.HeaderMigration, natsutil.MigrationLive)
	in.Set(natsutil.DebugHeader, "trace")
	in.Set(natsutil.DebugPayloadHeader, "1")
	in.Set("Nats-Encoding", "zstd")

	got := retrylane.BuildHeaders(in, meta("S", 1, 4), "subj", "c", "internal", time.Now())

	assert.Equal(t, natsutil.MigrationLive, got.Get(natsutil.HeaderMigration),
		"a migrated event that escalates must still suppress re-notification on the retry lane")
	assert.Equal(t, "trace", got.Get(natsutil.DebugHeader),
		"logctx.Admit on the retry loop is dead code unless the debug rung survives escalation")
	assert.Equal(t, "1", got.Get(natsutil.DebugPayloadHeader))
	assert.Equal(t, "zstd", got.Get("Nats-Encoding"),
		"Data() is forwarded byte-identically, so a content-encoding header must ride with it")
}

func TestBuildHeadersDropsJetStreamPublishHeaders(t *testing.T) {
	in := nats.Header{}
	in.Set(jetstream.MsgIDHeader, "origin-dedup-id")
	in.Set(jetstream.ExpectedStreamHeader, "MESSAGES-CANONICAL-site1")
	in.Set(jetstream.ExpectedLastSeqHeader, "41")
	in.Set(jetstream.ExpectedLastSubjSeqHeader, "7")
	in.Set(jetstream.ExpectedLastSubjSeqSubjHeader, "chat.msg.canonical.site1.created")
	in.Set(jetstream.ExpectedLastMsgIDHeader, "prev-id")

	got := retrylane.BuildHeaders(in, meta("S", 1, 4), "subj", "c", "internal", time.Now())

	for _, h := range []string{
		jetstream.MsgIDHeader, jetstream.ExpectedStreamHeader, jetstream.ExpectedLastSeqHeader,
		jetstream.ExpectedLastSubjSeqHeader, jetstream.ExpectedLastSubjSeqSubjHeader,
		jetstream.ExpectedLastMsgIDHeader,
	} {
		assert.Empty(t, got.Get(h),
			"%s addresses the origin stream; forwarding it would fight the escalation's own dedup id", h)
	}
}

func TestBuildHeadersOverridesForwardedRetryHeaders(t *testing.T) {
	in := nats.Header{}
	in.Set(retrylane.HeaderOriginStream, "STALE-STREAM")
	in.Set(retrylane.HeaderOriginSeq, "999")
	in.Set(retrylane.HeaderOriginSubject, "stale.subject")
	in.Set(retrylane.HeaderConsumer, "stale-consumer")
	in.Set(retrylane.HeaderReason, "stale")

	got := retrylane.BuildHeaders(in, meta("S", 1, 4), "subj", "c", "internal", time.Now())

	assert.Equal(t, "S", got.Get(retrylane.HeaderOriginStream), "this hop's provenance wins")
	assert.Equal(t, "1", got.Get(retrylane.HeaderOriginSeq))
	assert.Equal(t, "subj", got.Get(retrylane.HeaderOriginSubject))
	assert.Equal(t, "c", got.Get(retrylane.HeaderConsumer))
	assert.Equal(t, "internal", got.Get(retrylane.HeaderReason))
}

func TestBuildHeadersDoesNotMutateForwardedMultiValueInput(t *testing.T) {
	in := nats.Header{}
	in.Add("X-Multi", "a")
	in.Add("X-Multi", "b")

	got := retrylane.BuildHeaders(in, meta("S", 1, 4), "subj", "c", "internal", time.Now())
	got.Add("X-Multi", "c")

	assert.Equal(t, []string{"a", "b"}, in["X-Multi"],
		"the copy must be deep enough that appending to it cannot reach the live message")
	assert.Equal(t, []string{"a", "b", "c"}, got["X-Multi"])
}
