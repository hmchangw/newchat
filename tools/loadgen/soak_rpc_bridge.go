package main

import soakrpc "github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"

// These aliases keep the existing lanes stable while the RPC transport moves
// behind an explicit package boundary. They are removed as lanes move to their
// owning packages.
type soakRPCAction = soakrpc.Action

const (
	soakRPCSend             = soakrpc.ActionSend
	soakRPCThreadReply      = soakrpc.ActionThreadReply
	soakRPCLoadHistory      = soakrpc.ActionLoadHistory
	soakRPCLoadNext         = soakrpc.ActionLoadNext
	soakRPCGetThread        = soakrpc.ActionGetThread
	soakRPCGetMessage       = soakrpc.ActionGetMessage
	soakRPCReact            = soakrpc.ActionReact
	soakRPCEdit             = soakrpc.ActionEdit
	soakRPCDelete           = soakrpc.ActionDelete
	soakRPCPin              = soakrpc.ActionPin
	soakRPCUnpin            = soakrpc.ActionUnpin
	soakRPCPinnedList       = soakrpc.ActionPinnedList
	soakRPCReadBack         = soakrpc.ActionReadBack
	soakRPCMarkRead         = soakrpc.ActionMarkRead
	soakRPCScroll           = soakrpc.ActionScroll
	soakRPCMemberAdd        = soakrpc.ActionMemberAdd
	soakRPCMemberRemove     = soakrpc.ActionMemberRemove
	soakRPCRoomRename       = soakrpc.ActionRoomRename
	soakRPCMuteToggle       = soakrpc.ActionMuteToggle
	soakRPCRoomCreate       = soakrpc.ActionRoomCreate
	soakRPCMemberList       = soakrpc.ActionMemberList
	soakRPCRoomsInfo        = soakrpc.ActionRoomsInfo
	soakRPCSubscriptionList = soakrpc.ActionSubscriptionList
	soakRPCRoomStateRead    = soakrpc.ActionRoomStateRead
	soakRPCMessageRead      = soakrpc.ActionMessageRead
	soakRPCReadReceiptList  = soakrpc.ActionReadReceiptList
	soakRPCPresenceQuery    = soakrpc.ActionPresenceQuery
	soakRPCSearchMessages   = soakrpc.ActionSearchMessages
	soakRPCSearchRooms      = soakrpc.ActionSearchRooms
	soakRPCSearchIndexProbe = soakrpc.ActionSearchIndexProbe
)

func validSoakRPCAction(action soakRPCAction) bool {
	return soakrpc.ValidAction(action)
}

type soakErrorClass = soakrpc.ErrorClass

const (
	soakErrorTimeout               = soakrpc.ErrorTimeout
	soakErrorNoResponder           = soakrpc.ErrorNoResponder
	soakErrorDisconnected          = soakrpc.ErrorDisconnected
	soakErrorUnavailable           = soakrpc.ErrorUnavailable
	soakErrorInternal              = soakrpc.ErrorInternal
	soakErrorNotFound              = soakrpc.ErrorNotFound
	soakErrorForbidden             = soakrpc.ErrorForbidden
	soakErrorBadRequest            = soakrpc.ErrorBadRequest
	soakErrorConflict              = soakrpc.ErrorConflict
	soakErrorRequestEncode         = soakrpc.ErrorRequestEncode
	soakErrorResponseDecode        = soakrpc.ErrorResponseDecode
	soakErrorAssertion             = soakrpc.ErrorAssertion
	soakErrorAmbiguous             = soakrpc.ErrorAmbiguous
	soakErrorMutationTargetMissing = soakrpc.ErrorMutationTargetMissing
	soakErrorResponseTooLarge      = soakrpc.ErrorResponseTooLarge
	soakErrorCanceled              = soakrpc.ErrorCanceled
)

func validSoakErrorClass(class soakErrorClass) bool {
	return soakrpc.ValidErrorClass(class)
}

type soakErrorReason = soakrpc.ErrorReason

const (
	soakErrorReasonUnknown     = soakrpc.ErrorReasonUnknown
	soakReasonResponseTooLarge = soakrpc.ReasonResponseTooLarge
)

func validSoakErrorReason(reason soakErrorReason) bool {
	return soakrpc.ValidErrorReason(reason)
}

func newSoakAssertionError(message string) error {
	return soakrpc.NewAssertionError(message)
}

func parseSoakErrorEnvelope(data []byte) error {
	return soakrpc.ParseErrorEnvelope(data)
}

func classifySoakRPCReason(err error) soakErrorReason {
	return soakrpc.ClassifyReason(err)
}

func classifySoakRPCError(err error) soakErrorClass {
	return soakrpc.ClassifyError(err)
}

const (
	soakRetryNever     = soakrpc.RetryNever
	soakRetrySafe      = soakrpc.RetrySafe
	soakRetryAmbiguous = soakrpc.RetryAmbiguous
)

type soakRetryConfig = soakrpc.RetryConfig
type soakRPCTransport = soakrpc.Transport
type soakSleeper = soakrpc.Sleeper
type soakTimerSleeper = soakrpc.TimerSleeper
type soakRPCRequest = soakrpc.Request
type soakRPCResult = soakrpc.Result
type soakRPCClient = soakrpc.Client
type soakRequestError = soakrpc.RequestError

func newSoakRPCClient(
	transport soakRPCTransport,
	retry soakRetryConfig,
	sleeper soakSleeper,
	random func() float64,
) *soakRPCClient {
	return soakrpc.NewClient(transport, retry, sleeper, random)
}

func transientSoakError(class soakErrorClass) bool {
	return soakrpc.IsTransientError(class)
}

func soakErrorAttrs(err error) []any {
	return soakrpc.ErrorAttrs(err)
}
