package natsmetrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// These lists used to live in metrics.go, where they existed only to drive the
// startup loops that precomputed every attribute set. Now that sets are built
// on first use they belong here, doing the job the loops never did: proving the
// normalizers accept every value the package declares, and pinning the worst
// case label space so widening an enum is a visible decision rather than a
// silent multiplication of series.
var (
	allOutcomes = []Outcome{OutcomeAck, OutcomeNak, OutcomeTerm, OutcomeLeftPending, OutcomeHandlerCancelled}

	allEventTypes = []EventType{
		EventCreated, EventUpdated, EventDeleted, EventPinned, EventUnpinned,
		EventReacted, EventThreadReplyAdded, EventSend, EventTeamsBatch, EventUnknown,
		EventRoomCreate, EventMemberAdd, EventMemberRemove, EventRoomRename, EventMemberMuted,
	}

	allPublishOutcomes = []PublishOutcome{
		PublishTimeout, PublishNoResponders, PublishDisconnected,
		PublishBufferFull, PublishPermission, PublishPayloadTooLarge, PublishOtherError,
	}

	allRequestOutcomes = append([]PublishOutcome{RequestSucceeded}, allPublishOutcomes...)

	allTerminalReasons = []TerminalReason{
		TerminalMaxDeliver, TerminalPermanent, TerminalPublishExhausted, TerminalConsumerDeleted,
		TerminalStreamUnavailable, TerminalInvalidPayload, TerminalInternal,
	}

	allRequestResults = []RequestResult{
		RequestSuccess, RequestBadRequest, RequestUnauthenticated, RequestForbidden,
		RequestNotFound, RequestConflict, RequestTooManyRequests, RequestUnavailable, RequestInternal,
	}

	allDestinations = []DestinationKind{
		DestinationCanonical, DestinationRecipientEvent, DestinationNotification, DestinationPush,
		DestinationOutbox, DestinationInbox, DestinationRoomCanonical, DestinationRoomEvent,
		DestinationMemberEvent, DestinationClientResponse, DestinationUserSync, DestinationUnknown,
	}

	allOperations = []Operation{
		OperationCanonicalPublish, OperationClientResponse, OperationRecipientPublish,
		OperationNotificationPublish, OperationPushPublish, OperationHistoryGetMessage,
		OperationPresenceLookup, OperationThreadTCount, OperationTeamsUserUpsert,
		OperationHistoryRead, OperationHistoryMutation, OperationRoomRead, OperationRoomMutation,
		OperationMemberRead, OperationMemberMutation, OperationTeamsRoom, OperationRoomPublish,
		OperationMemberPublish, OperationOutboxPublish, OperationUnknown,
	}
)

// A declared enum value that its own normalizer rejects would be recorded as
// "unknown"/"internal" forever, and nothing else would catch it: the recorder
// path takes whatever the normalizer returns.
func TestNormalizersAcceptEveryDeclaredValue(t *testing.T) {
	for _, event := range allEventTypes {
		assert.Equal(t, event, NormalizeEventType(string(event)))
	}
	for _, destination := range allDestinations {
		assert.Equal(t, destination, normalizeDestination(destination))
	}
	for _, operation := range allOperations {
		assert.Equal(t, operation, normalizeOperation(operation))
	}
	for _, result := range allRequestResults {
		assert.Equal(t, result, normalizeRequestResult(result))
	}
	for _, reason := range allTerminalReasons {
		assert.Equal(t, reason, normalizeTerminalReason(reason))
	}
}

// The worst case is per site, per service, and for the consumer families also
// per stream/consumer pair, so these numbers multiply out in the backend. They
// are asserted rather than described so that adding one operation — which adds
// 7 publish-failure series and 17 request series at a stroke — has to be
// acknowledged here.
func TestLabelSpaceStaysWithinBudget(t *testing.T) {
	assert.Equal(t, 75, len(allEventTypes)*len(allOutcomes),
		"chat.nats.consumer.messages / .processing.duration: event_type x outcome")
	assert.Equal(t, 105, len(allEventTypes)*len(allTerminalReasons),
		"chat.nats.terminal.failures: event_type x reason")
	assert.Equal(t, 1680, len(allDestinations)*len(allOperations)*len(allPublishOutcomes),
		"chat.nats.publish.failures: destination_kind x operation x outcome")
	assert.Equal(t, 160, len(allOperations)*len(allRequestOutcomes),
		"chat.nats.requests / .request.duration: operation x outcome")
	assert.Equal(t, 180, len(allOperations)*len(allRequestResults),
		"chat.nats.request.handled / .handler.duration: operation x result")
}

// The publish family's theoretical 1,680 is the clearest argument for building
// sets on demand: destination and operation are chosen together at each call
// site, never crossed, so the pairs that can actually occur are the ones
// PublishLabelsFromSubject returns plus the fixed literal pairs. Roughly
// seventeen of 240, and only the ones that fail ever get recorded.
func TestPublishLabelPairsAreFarNarrowerThanTheCrossProduct(t *testing.T) {
	reachable := map[publishPair]bool{}
	for _, subj := range []string{
		"chat.msg.canonical.site-a.created",
		"chat.room.canonical.site-a.create",
		"chat.room.canonical.site-a.member.add",
		"chat.outbox.site-a.site-b.room_renamed",
		"chat.inbox.site-a.internal.member_added",
		"chat.inbox.site-a.internal.room_renamed",
		"chat.room.room-a.event",
		"chat.room.room-a.event.member",
		"chat.user.alice.event.room",
		"chat.user.alice.event.subscription.update",
		"chat.user.alice.response.request-a",
		"chat.hr.site-a.users.upsert",
		"chat.dynamic.nothing.matches.this",
	} {
		destination, operation := PublishLabelsFromSubject(subj)
		reachable[publishPair{destination, operation}] = true
	}
	// The literal pairs passed at fixed call sites, which no subject produces.
	for _, pair := range []publishPair{
		{DestinationRecipientEvent, OperationRecipientPublish},
		{DestinationRecipientEvent, OperationThreadTCount},
		{DestinationOutbox, OperationRecipientPublish},
		{DestinationPush, OperationPushPublish},
	} {
		reachable[pair] = true
	}

	assert.Less(t, len(reachable), 20,
		"if this grows toward %d, revisit whether destination_kind still adds information over operation",
		len(allDestinations)*len(allOperations))
}

type publishPair struct {
	destination DestinationKind
	operation   Operation
}
