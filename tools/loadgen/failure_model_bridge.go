package main

import (
	"time"

	failuremodel "github.com/hmchangw/chat/tools/loadgen/internal/failure"
)

type failureObserver = failuremodel.Observer

const (
	failureObserverAdmission   = failuremodel.ObserverAdmission
	failureObserverHistory     = failuremodel.ObserverHistory
	failureObserverRecipient   = failuremodel.ObserverRecipient
	failureObserverRoomState   = failuremodel.ObserverRoomState
	failureObserverSearchIndex = failuremodel.ObserverSearchIndex
)

const failureObserverContractSchemaVersion = failuremodel.ObserverContractSchemaVersion

type failureObserverContract = failuremodel.ObserverContract

func newFailureObserverContract(recipientEnabled, searchEnabled bool) failureObserverContract {
	return failuremodel.NewObserverContract(recipientEnabled, searchEnabled)
}

type failureOperationType = failuremodel.OperationType

const (
	failureOperationMessageCreate = failuremodel.OperationMessageCreate
	failureOperationMemberAdd     = failuremodel.OperationMemberAdd
	failureOperationMemberRemove  = failuremodel.OperationMemberRemove
	failureOperationRoomRename    = failuremodel.OperationRoomRename
	failureOperationMuteToggle    = failuremodel.OperationMuteToggle
	failureOperationRoomCreate    = failuremodel.OperationRoomCreate
	failureOperationMessageRead   = failuremodel.OperationMessageRead
)

type failureOperationLifecycle = failuremodel.OperationLifecycle

const (
	failureOperationJournaled = failuremodel.OperationJournaled
	failureOperationActive    = failuremodel.OperationActive
)

const (
	failureEffectAdmission        = failuremodel.EffectAdmission
	failureEffectMessagePersisted = failuremodel.EffectMessagePersisted
	failureEffectRecipientEvent   = failuremodel.EffectRecipientEvent
	failureEffectMemberState      = failuremodel.EffectMemberState
	failureEffectRoomName         = failuremodel.EffectRoomName
	failureEffectSubscriptionMute = failuremodel.EffectSubscriptionMute
	failureEffectRoomCreated      = failuremodel.EffectRoomCreated
	failureEffectSubscriptionRead = failuremodel.EffectSubscriptionRead
	failureEffectMessageIndexed   = failuremodel.EffectMessageIndexed
)

type failureExpectedEffect = failuremodel.ExpectedEffect

func messageCreateExpectedEffectsForObservers(
	recipientEnabled bool,
	searchEnabled bool,
	recipientCount int,
	recipientHash string,
) []failureExpectedEffect {
	return failuremodel.MessageCreateExpectedEffects(
		recipientEnabled, searchEnabled, recipientCount, recipientHash,
	)
}

func memberMutationExpectedEffects() []failureExpectedEffect {
	return failuremodel.MemberMutationExpectedEffects()
}

func roomMutationExpectedEffects(operationType failureOperationType) []failureExpectedEffect {
	return failuremodel.RoomMutationExpectedEffects(operationType)
}

func readReceiptExpectedEffects() []failureExpectedEffect {
	return failuremodel.ReadReceiptExpectedEffects()
}

func roomCreateExpectedEffects() []failureExpectedEffect {
	return failuremodel.RoomCreateExpectedEffects()
}

type failureObservation = failuremodel.Observation

const (
	failureObservationGood                 = failuremodel.ObservationGood
	failureObservationBad                  = failuremodel.ObservationBad
	failureObservationUnverified           = failuremodel.ObservationUnverified
	failureObservationMissingAfterDeadline = failuremodel.ObservationMissingAfterDeadline
)

type failureReason = failuremodel.Reason

const (
	failureReasonNone                      = failuremodel.ReasonNone
	failureReasonAdmissionRejected         = failuremodel.ReasonAdmissionRejected
	failureReasonHistoryContentMismatch    = failuremodel.ReasonHistoryContentMismatch
	failureReasonHistoryMissing            = failuremodel.ReasonHistoryMissing
	failureReasonRecipientDuplicate        = failuremodel.ReasonRecipientDuplicate
	failureReasonRecipientUnexpected       = failuremodel.ReasonRecipientUnexpected
	failureReasonRecipientIdentityMismatch = failuremodel.ReasonRecipientIdentityMismatch
	failureReasonRecipientMissing          = failuremodel.ReasonRecipientMissing
	failureReasonPublishLocalError         = failuremodel.ReasonPublishLocalError
	failureReasonMemberStateMismatch       = failuremodel.ReasonMemberStateMismatch
	failureReasonRoomNameMismatch          = failuremodel.ReasonRoomNameMismatch
	failureReasonMuteStateMismatch         = failuremodel.ReasonMuteStateMismatch
	failureReasonRoomStateMissing          = failuremodel.ReasonRoomStateMissing
	failureReasonReadStateRegressed        = failuremodel.ReasonReadStateRegressed
)

var errFailureObserverContractMismatch = failuremodel.ErrObserverContractMismatch

type failureResult = failuremodel.Result

const (
	failureResultGood                 = failuremodel.ResultGood
	failureResultBad                  = failuremodel.ResultBad
	failureResultUnverified           = failuremodel.ResultUnverified
	failureResultNotSent              = failuremodel.ResultNotSent
	failureResultMissingAfterDeadline = failuremodel.ResultMissingAfterDeadline
)

const (
	failureObserverEvent = failuremodel.ObserverEvent
	failureObserverQuery = failuremodel.ObserverQuery
	failureObserverBoth  = failuremodel.ObserverBoth
)

type failureObserverDefinition = failuremodel.ObserverDefinition

func failureObserverDefinitionFor(observer failureObserver) (failureObserverDefinition, bool) {
	return failuremodel.ObserverDefinitionFor(observer)
}

type failureOperation = failuremodel.Operation

type failureLedgerEvent = failuremodel.Event

const (
	failureLedgerEventStarted     = failuremodel.EventStarted
	failureLedgerEventActivated   = failuremodel.EventActivated
	failureLedgerEventObserved    = failuremodel.EventObserved
	failureLedgerEventFinalized   = failuremodel.EventFinalized
	failureLedgerEventCheckpoint  = failuremodel.EventCheckpoint
	failureLedgerEventInvariant   = failuremodel.EventInvariant
	failureLedgerEventInvalidated = failuremodel.EventInvalidated
)

type failureJournal = failuremodel.Journal
type streamingFailureJournal = failuremodel.StreamingJournal
type bufferedFailureJournal = failuremodel.BufferedJournal
type fileFailureWAL = failuremodel.WAL

const failureWALSchemaVersion = failuremodel.WALSchemaVersion

func openFailureWAL(path string) (*fileFailureWAL, error) {
	return failuremodel.OpenWAL(path)
}

func syncFailureWALDirectory(directory string) error {
	return failuremodel.SyncWALDirectory(directory)
}

func validateFailureOperation(operation *failureOperation) error {
	return failuremodel.ValidateOperation(operation)
}

func validFailureObservation(observation failureObservation) bool {
	return failuremodel.ValidObservation(observation)
}

func validFailureReason(reason failureReason) bool {
	return failuremodel.ValidReason(reason)
}

func validFailureResult(result failureResult) bool {
	return failuremodel.ValidResult(result)
}

func failureOperationResult(operation *failureOperation) failureResult {
	return failuremodel.OperationResult(operation)
}

func failureOperationFinalReason(operation *failureOperation, result failureResult) failureReason {
	return failuremodel.OperationFinalReason(operation, result)
}

func cloneFailureOperation(operation *failureOperation) *failureOperation {
	return failuremodel.CloneOperation(operation)
}

func defaultFailureReason(observer failureObserver, observation failureObservation) failureReason {
	return failuremodel.DefaultReason(observer, observation)
}

func validateFailureObserverContract(contract failureObserverContract) error {
	return failuremodel.ValidateObserverContract(contract)
}

func failureOperationMatchesObserverContract(
	operation *failureOperation,
	contract failureObserverContract,
) bool {
	return failuremodel.OperationMatchesObserverContract(operation, contract)
}

type failureObserverHealth = failuremodel.ObserverHealth

func newFailureObserverHealth(observer failureObserver, startedAt time.Time) *failureObserverHealth {
	return failuremodel.NewObserverHealth(observer, startedAt)
}

const (
	soakFailureScenario           = failuremodel.ScenarioMessageSoak
	soakFailureLaneMessageSend    = failuremodel.LaneMessageSend
	soakFailureLaneMemberMutation = failuremodel.LaneMemberMutation
	soakFailureLaneRoomMutation   = failuremodel.LaneRoomMutation
	soakFailureLaneRoomCreate     = failuremodel.LaneRoomCreate
	soakFailureLaneReadReceipt    = failuremodel.LaneReadReceipt
)
