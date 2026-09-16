// Package retrylane moves the long tail of JetStream retries off a hot
// consumer lane and onto the RETRY stream.
//
// A message Nak'd with a delay keeps its ack-pending slot for the whole
// backoff, so jsretry.DefaultBackoff spends 756s of a shared 1000-slot budget
// per failing message. At a sustained 5 failures/s the budget is exhausted in
// ~200s and the consumer stops delivering anything, healthy messages included
// — the stall arrives long before the MaxDeliver drop does.
//
// This package keeps the fast rungs in place (~36s) and republishes the
// message onto RETRY-{siteID} when they are spent, where a second per-service
// consumer runs the same handler on the slow rungs. The total retry budget is
// unchanged; only the ack-pending occupancy moves.
package retrylane

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// Escalation headers. Metadata rides in headers so the body stays
// byte-identical: message-worker writes those bytes to Cassandra, and the
// hot-path workers marshal via sonic whose output is not byte-identical to
// stdlib, so a re-marshal could change bytes that dedup keys pin.
const (
	HeaderOriginStream  = "X-Retry-Origin-Stream"
	HeaderOriginSeq     = "X-Retry-Origin-Seq"
	HeaderOriginSubject = "X-Retry-Origin-Subject"
	HeaderConsumer      = "X-Retry-Consumer"
	HeaderAttempt       = "X-Retry-Attempt"
	HeaderFirstFailedAt = "X-Retry-First-Failed-At"
	HeaderReason        = "X-Retry-Reason"
)

// traceparentHeader is the W3C trace context header carried through every hop
// so a retry lands in the same trace lineage as the original send.
const traceparentHeader = "traceparent"

// DedupID is the escalation's Nats-Msg-Id. Escalation is publish-then-Ack and
// therefore at-least-once: a crash between the two re-runs the handler and
// re-escalates, and this deterministic id makes the second publish a dedup
// no-op — while the stream's duplicate window holds, which is ops-owned.
func DedupID(originStream string, originSeq uint64, consumer string) string {
	return fmt.Sprintf("%s:%d:%s", originStream, originSeq, consumer)
}

// ReasonFor maps an error to a bounded category label. It returns the errcode
// Code when one is in the chain and "internal" otherwise. It never returns the
// error text: the reason reaches a header and a metric label, and a raw cause
// can carry a message body or token.
func ReasonFor(err error) string {
	var ec *errcode.Error
	if errors.As(err, &ec) && ec.Code.Valid() {
		return string(ec.Code)
	}
	return string(errcode.CodeInternal)
}

// BuildHeaders returns the headers for an escalated message. in is the live
// message's headers and is never mutated. Attempts accumulate across lanes and
// the first-failure timestamp survives every hop, so time-to-dead-letter stays
// truthful however many times a message is re-escalated.
func BuildHeaders(in nats.Header, meta *jetstream.MsgMetadata,
	originSubject, consumer, reason string, now time.Time,
) nats.Header {
	out := nats.Header{}

	// Correlation first: a retry must stay in the original trace lineage.
	if v := in.Get(natsutil.RequestIDHeader); v != "" {
		out.Set(natsutil.RequestIDHeader, v)
	}
	if v := in.Get(traceparentHeader); v != "" {
		out.Set(traceparentHeader, v)
	}

	out.Set(HeaderOriginSubject, originSubject)
	out.Set(HeaderConsumer, consumer)
	out.Set(HeaderReason, reason)

	var delivered uint64
	if meta != nil {
		out.Set(HeaderOriginStream, meta.Stream)
		out.Set(HeaderOriginSeq, strconv.FormatUint(meta.Sequence.Stream, 10))
		delivered = meta.NumDelivered
	}

	prior, _ := strconv.ParseUint(in.Get(HeaderAttempt), 10, 64)
	out.Set(HeaderAttempt, strconv.FormatUint(prior+delivered, 10))

	firstFailed := in.Get(HeaderFirstFailedAt)
	if firstFailed == "" {
		firstFailed = strconv.FormatInt(now.UTC().UnixMilli(), 10)
	}
	out.Set(HeaderFirstFailedAt, firstFailed)

	return out
}
