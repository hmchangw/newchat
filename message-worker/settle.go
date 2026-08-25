package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// settle resolves a processed message. The give-up decision is made by asking
// Cassandra why the write failed, not by inspecting the site's health:
//
//   - success              → Ack (and let the tracker consider clearing the marker)
//   - permanent (decode)   → Ack-drop, unchanged and still first among error cases
//   - non-history failure  → NAK indefinitely; a Mongo/user-lookup/mention failure
//     leaves the marker untouched, so dropping one would open a hole while
//     history-service keeps telling clients their history is complete
//   - infra-class history failure → NAK indefinitely; the cluster cannot serve the
//     write, and every message is in the same position
//   - request-class history failure → NAK until the accumulated retry time reaches
//     the window, then drop — subject to the kill switch and the per-pod drop rate cap
//
// The consumer runs MaxDeliver=-1 (enforced in buildConsumerConfig and re-checked at
// startup), so JetStream never terminates a message behind this function's back;
// every give-up is deliberate.
//
// The degraded marker is deliberately NOT read here. Reading it was the wedge that
// forced the earlier revert: a history failure re-degrades the site, so a message's
// own failure destroyed the evidence needed to condemn it. cassutil.ClassifyCQL answers
// the question the marker was standing in for — is this failure specific to this
// message? — directly, per failure, with no site-wide state in the way.
func (h *Handler) settle(ctx context.Context, msg jetstream.Msg, err error) {
	if err == nil {
		h.degrade.OnWriteSuccess(ctx)
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, nil)
		return
	}
	if _, isPermanent := errcode.IsPermanent(err); isPermanent {
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, err)
		return
	}
	if !isHistoryWriteError(err) {
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, err)
		return
	}

	class := cassutil.ClassifyCQL(err)
	h.histMetrics.onHistoryWriteFailure(class.String())
	h.degrade.OnWriteFailure(ctx)

	// The infra-class early return comes before retriedFor on purpose: no infra-class
	// failure can ever lead to a drop, and during an actual Cassandra outage that is
	// essentially every failure — so the metadata decode retriedFor needs (which walks
	// the reply subject and allocates) is skipped on the dominant path.
	if class == cassutil.CQLInfra {
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, err)
		return
	}

	retried, measurable := retriedFor(msg)
	if !measurable || retried < h.drop.RetryWindow {
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, err)
		return
	}

	// Two guards stand between a request-class verdict and destruction. Both answer
	// the same way — NAK, so the message comes back — and both are counted, because a
	// brake nobody can see is a brake nobody trusts.
	code := cassutil.CQLCode(err)
	switch {
	case !h.drop.Enabled:
		// The operator's brake for a migration that turns every write request-class:
		// without it, stopping the bleeding would need a deploy.
		h.suppressDrop(ctx, msg, err, code, retried, dropSuppressedDisabled)
		return
	case !h.drop.limiter.Allow():
		// The unattended brake: `Invalid` is mostly a site-wide fault in practice
		// (unconfigured table, undefined column, failed re-prepare), so the cap is
		// what bounds loss when nobody is watching the class-labelled metric.
		h.suppressDrop(ctx, msg, err, code, retried, dropSuppressedRateLimited)
		return
	}

	// Data destruction: this message is gone from history after the Ack, while
	// clients have already seen it delivered and search has already indexed it. The
	// log line is the only remaining record, so it carries the identifiers and the
	// CQL code — and deliberately not the error text or the payload, either of which
	// can echo message content into the server log.
	//
	// It is emitted BEFORE the Ack on purpose, unlike the counter: a record for a
	// message that survives a failed Ack is a false alarm an operator can reconcile
	// against the failure logged below, whereas a destroyed message with no record is
	// unrecoverable and invisible.
	id, roomID := droppedIdentity(msg.Data())
	slog.ErrorContext(ctx, "dropping message: Cassandra rejects it as a request error and the retry window has elapsed",
		"message_id", id, "room_id", roomID, "site", h.siteID,
		"cql_code", code, "retried_for", retried.String(),
		"retry_window", h.drop.RetryWindow.String(),
		"request_id", natsutil.RequestIDFromContext(ctx))
	if ackErr := msg.Ack(); ackErr != nil {
		// Nothing was destroyed: JetStream redelivers and the drop is re-decided.
		slog.ErrorContext(ctx, "failed to ack dropped message — it will be redelivered", "error", ackErr,
			"message_id", id, "request_id", natsutil.RequestIDFromContext(ctx))
		return
	}
	h.histMetrics.onDropped(code)
}

// suppressDrop withholds a drop one of the guards refused and retries the message
// instead.
//
// The raw error is deliberately NOT logged, matching the drop-ERROR log above: a CQL
// "Invalid" message echoes the offending value (`Invalid string constant (…) for "col"`),
// so the error text is untrusted message content. cql_code carries the whole diagnostic
// without it. This matters more here than anywhere else on the path — suppression runs
// on every re-evaluation of the message, and a schema-drift wave re-evaluates
// constantly, so an error field would pour that text into the log at volume. Truncating
// or sanitizing it is not the fix; the point is that the text cannot be trusted at all.
// SettleQuiet keeps jsretry from logging it either.
func (h *Handler) suppressDrop(ctx context.Context, msg jetstream.Msg, err error, code string, retried time.Duration, reason string) {
	h.histMetrics.onDropSuppressed(reason)
	slog.WarnContext(ctx, "history drop suppressed — retrying instead",
		"reason", reason, "cql_code", code, "retried_for", retried.String(),
		"request_id", natsutil.RequestIDFromContext(ctx))
	jsretry.SettleQuiet(ctx, msg, jsretry.DefaultBackoff, err)
}

// droppedIdentity extracts just enough of the event to name what was destroyed.
// A narrow projection on purpose: the drop log must identify the message without
// re-marshalling content into it. Empty strings when the payload cannot be decoded —
// a malformed event never reaches here (it is Ack-dropped as permanent above), and
// an unnamed drop is still logged.
func droppedIdentity(data []byte) (messageID, roomID string) {
	var evt struct {
		Message struct {
			ID     string `json:"id"`
			RoomID string `json:"roomId"`
		} `json:"message"`
	}
	if err := sonic.Unmarshal(data, &evt); err != nil {
		return "", ""
	}
	return evt.Message.ID, evt.Message.RoomID
}

// retriedFor reports how long this message has already spent waiting on NAK
// backoff, and whether that is knowable at all. It is accumulated backoff, not the
// message's age on the stream: a replayed backlog is already hours old before its
// first retry, so an age-since-publish measure would drop a request-class failure on
// its very first delivery — exactly backwards during the outage this service is
// sized for.
//
// A message whose metadata cannot be read has no delivery count, so the answer is
// false and the caller retries: an unmeasurable message is never destroyed.
func retriedFor(msg jetstream.Msg) (time.Duration, bool) {
	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return 0, false
	}
	return jsretry.ElapsedFor(jsretry.DefaultBackoff, meta.NumDelivered), true
}

// maxReportedDeliveries caps deliveriesToDrop's search. A window this far out is a
// misconfiguration, and the startup log only needs an order of magnitude.
const maxReportedDeliveries uint64 = 1_000_000

// deliveriesToDrop reports the first delivery count whose accumulated backoff
// reaches window — the redelivery number a request-class failure is dropped on.
// Logged at startup so an operator can see what the configured duration means in
// deliveries (1h is 11 against the current DefaultBackoff) instead of having to
// derive it from the backoff schedule. The count moves whenever that schedule is
// retuned, which is why it is computed here rather than written down.
func deliveriesToDrop(window time.Duration) uint64 {
	for n := uint64(1); n < maxReportedDeliveries; n++ {
		if jsretry.ElapsedFor(jsretry.DefaultBackoff, n) >= window {
			return n
		}
	}
	return maxReportedDeliveries
}
