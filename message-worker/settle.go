package main

import (
	"context"
	"errors"
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
//   - orphaned parent      → NAK while the site is degraded (the parent is still
//     replaying), otherwise NAK until the retry window elapses and then drop; see
//     settleOrphanedParent
//   - history *read* failure → NAK indefinitely, counted, marker untouched: a failed
//     lookup means nothing is missing, so it is not evidence history is behind
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
	if parentID, orphaned := orphanedParent(err); orphaned {
		h.settleOrphanedParent(ctx, msg, err, parentID)
		return
	}

	// A read failure retries like an infra-class write and is counted like one, but it
	// never raises the marker: the marker means history is behind, and only a failed
	// write can make that true. Checked before the write branch because the tag is what
	// separates them — see historyReadError for what tagging a read as a write cost.
	if isHistoryReadError(err) {
		h.histMetrics.onHistoryReadFailure(cassutil.ClassifyCQL(err).String())
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, err)
		return
	}
	if !isHistoryWriteError(err) {
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, err)
		return
	}

	class := cassutil.ClassifyCQL(err)
	h.histMetrics.onHistoryWriteFailure(class.String())

	// The infra-class early return comes before retriedFor on purpose: no infra-class
	// failure can ever lead to a drop, and during an actual Cassandra outage that is
	// essentially every failure — so the metadata decode retriedFor needs (which walks
	// the reply subject and allocates) is skipped on the dominant path.
	//
	// The marker is set only here, on the infra path. It is site-wide state — it makes
	// every room report incompleteSince and suppresses every thread badge — so only a
	// failure that says something about the site may set it. A request-class verdict is
	// the classifier saying "this one row is unwritable", which is per-message by
	// construction; marking on it turned one bad row into a site-wide "history is
	// incomplete" for the whole drain grace.
	//
	// The gap this leaves is a site-wide fault that presents as request class (a rolling
	// migration returning Invalid for every write): no marker is set, so clients are not
	// told. That is the population-signal follow-up in the PR description — the drop-rate
	// metric, its alert and HISTORY_DROP_ENABLED are what cover it today. Marking
	// per-message was never a correct stand-in for it: it fired on one poison row too.
	if class == cassutil.CQLInfra {
		h.degrade.OnWriteFailure(ctx)
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
		"drop_code", code, "retried_for", retried.String(),
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

// orphanedParentError marks a thread reply whose parent is absent from
// messages_by_id. It is deliberately a type rather than an error string: settle has
// to tell it apart from every other plain failure, and with MaxDeliver=-1 the cost
// of not telling them apart is unbounded (see settleOrphanedParent).
//
// Not a historyWriteError: a clean miss is an ordering race between concurrent
// workers, not evidence that Cassandra is unwell, so it must never flip the
// site-wide marker. Only the *read error* on that lookup carries that signal.
type orphanedParentError struct{ parentID string }

func (e orphanedParentError) Error() string {
	return "thread parent " + e.parentID + " not yet persisted in messages_by_id"
}

// orphanedParent reports whether err is a missing-thread-parent failure, and names
// the parent so a drop can be logged against it.
func orphanedParent(err error) (string, bool) {
	var o orphanedParentError
	if errors.As(err, &o) {
		return o.parentID, true
	}
	return "", false
}

// settleOrphanedParent bounds the one non-Cassandra failure that can be permanent.
//
// Every other plain error on this path is a dependency being unavailable — Mongo,
// the user lookup, a downstream publish — and those resolve for every message at
// once when the dependency comes back, so retrying them forever is right. A parent
// that is absent from messages_by_id does not resolve that way: the reply overtook
// it (transient, resolves in seconds) or the parent is never coming (permanent, and
// this service manufactures that case itself — dropping a parent above orphans every
// reply to it). Under MaxDeliver=-1 the permanent case NAKs for the life of the pod,
// holding one of MaxAckPending slots that is never released; enough of them and the
// consumer stops delivering anything at all, which is a total history outage rather
// than the bounded per-message loss this PR set out to remove.
//
// The two cases are separated by site health rather than by inspecting the parent:
//
//   - Degraded: the parent is very likely still in the replay backlog, so retry
//     regardless of how long this has gone on. A drain must never be read as a
//     permanently missing parent.
//   - Healthy: history has caught up, so a parent still absent after the full retry
//     window is absent for good. Drop it — through the same kill switch, the same
//     per-pod rate cap and the same counter as the CQL path, because those brakes
//     bound destruction, not a particular cause of it.
func (h *Handler) settleOrphanedParent(ctx context.Context, msg jetstream.Msg, err error, parentID string) {
	if h.degrade.Degraded() {
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, err)
		return
	}
	retried, measurable := retriedFor(msg)
	if !measurable || retried < h.drop.RetryWindow {
		jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, err)
		return
	}

	switch {
	case !h.drop.Enabled:
		h.suppressDrop(ctx, msg, err, dropCodeOrphanedParent, retried, dropSuppressedDisabled)
		return
	case !h.drop.limiter.Allow():
		h.suppressDrop(ctx, msg, err, dropCodeOrphanedParent, retried, dropSuppressedRateLimited)
		return
	}

	// Same ordering as the CQL drop: log before the Ack, so a message that survives a
	// failed Ack leaves a reconcilable false alarm rather than a silent destruction.
	// parent_id is safe to log (an application-generated ID); the reply's own content
	// is not, and is not logged.
	id, roomID := droppedIdentity(msg.Data())
	slog.ErrorContext(ctx, "dropping thread reply: its parent is absent from history and the retry window has elapsed",
		"message_id", id, "room_id", roomID, "site", h.siteID,
		"parent_id", parentID, "drop_code", dropCodeOrphanedParent,
		"retried_for", retried.String(), "retry_window", h.drop.RetryWindow.String(),
		"request_id", natsutil.RequestIDFromContext(ctx))
	if ackErr := msg.Ack(); ackErr != nil {
		slog.ErrorContext(ctx, "failed to ack dropped thread reply — it will be redelivered", "error", ackErr,
			"message_id", id, "request_id", natsutil.RequestIDFromContext(ctx))
		return
	}
	h.histMetrics.onDropped(dropCodeOrphanedParent)
}

// suppressDrop withholds a drop one of the guards refused and retries the message
// instead.
//
// The raw error is deliberately NOT logged, matching the drop-ERROR log above: a CQL
// "Invalid" message echoes the offending value (`Invalid string constant (…) for "col"`),
// so the error text is untrusted message content. drop_code carries the whole diagnostic
// without it. This matters more here than anywhere else on the path — suppression runs
// on every re-evaluation of the message, and a schema-drift wave re-evaluates
// constantly, so an error field would pour that text into the log at volume. Truncating
// or sanitizing it is not the fix; the point is that the text cannot be trusted at all.
// SettleQuiet keeps jsretry from logging it either.
func (h *Handler) suppressDrop(ctx context.Context, msg jetstream.Msg, err error, code string, retried time.Duration, reason string) {
	h.histMetrics.onDropSuppressed(reason)
	slog.WarnContext(ctx, "history drop suppressed — retrying instead",
		"reason", reason, "drop_code", code, "retried_for", retried.String(),
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
