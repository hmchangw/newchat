package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
)

type soakRPCAction string

const (
	soakRPCSend        soakRPCAction = "send"
	soakRPCThreadReply soakRPCAction = "thread_reply"
	soakRPCLoadHistory soakRPCAction = "load_history"
	soakRPCLoadNext    soakRPCAction = "load_next"
	soakRPCGetThread   soakRPCAction = "get_thread_messages"
	soakRPCGetMessage  soakRPCAction = "get_message_by_id"
	soakRPCReact       soakRPCAction = "reaction"
	soakRPCEdit        soakRPCAction = "edit"
	soakRPCDelete      soakRPCAction = "delete"
	soakRPCPin         soakRPCAction = "pin"
	soakRPCUnpin       soakRPCAction = "unpin"
	soakRPCPinnedList  soakRPCAction = "pinned_list"
	soakRPCReadBack    soakRPCAction = "read_back"
	soakRPCMarkRead    soakRPCAction = "mark_read"
	soakRPCScroll      soakRPCAction = "scroll_history"

	soakRPCMemberAdd        soakRPCAction = "member_add"
	soakRPCMemberRemove     soakRPCAction = "member_remove"
	soakRPCRoomRename       soakRPCAction = "room_rename"
	soakRPCMuteToggle       soakRPCAction = "mute_toggle"
	soakRPCRoomCreate       soakRPCAction = "room_create"
	soakRPCMemberList       soakRPCAction = "member_list"
	soakRPCRoomsInfo        soakRPCAction = "rooms_info"
	soakRPCSubscriptionList soakRPCAction = "subscription_list"
	soakRPCRoomStateRead    soakRPCAction = "room_state_read"
	soakRPCMessageRead      soakRPCAction = "message_read"
	soakRPCReadReceiptList  soakRPCAction = "read_receipt_list"
	soakRPCPresenceQuery    soakRPCAction = "presence_query"

	soakRPCSearchMessages soakRPCAction = "search_messages"
	soakRPCSearchRooms    soakRPCAction = "search_rooms"
	// soakRPCSearchIndexProbe is the evidence query, kept on its own label so
	// its latency and error rate never blend into the read lane's.
	soakRPCSearchIndexProbe soakRPCAction = "search_index_probe"

	// The user-service read lane. Every one of these is read-only, so they
	// carry latency and outcome only and never enter the evidence ledger.
	soakRPCUserMe                  soakRPCAction = "user_me"
	soakRPCUserProfileGet          soakRPCAction = "user_profile_get"
	soakRPCUserStatusGet           soakRPCAction = "user_status_get"
	soakRPCUserSettingsGet         soakRPCAction = "user_settings_get"
	soakRPCUserChatlistGet         soakRPCAction = "user_chatlist_get"
	soakRPCUserPriorityContacts    soakRPCAction = "user_priority_contacts"
	soakRPCUserAppsList            soakRPCAction = "user_apps_list"
	soakRPCUserAppsCategories      soakRPCAction = "user_apps_categories"
	soakRPCUserSubscriptionCount   soakRPCAction = "user_subscription_count"
	soakRPCUserSubscriptionByRoom  soakRPCAction = "user_subscription_by_room"
	soakRPCUserSubscriptionChannel soakRPCAction = "user_subscription_channels"
	soakRPCUserSubscriptionDM      soakRPCAction = "user_subscription_dm"
	soakRPCUserThreadList          soakRPCAction = "user_thread_list"
	soakRPCUserThreadUnread        soakRPCAction = "user_thread_unread"
)

// soakUserReadActions is every action the user-service read lane dispatches. It
// is the single source for both the allowlist and the lane's own dispatch
// table, so an action can never be allowlisted without being sent.
var soakUserReadActions = []soakRPCAction{
	soakRPCUserMe, soakRPCUserProfileGet, soakRPCUserStatusGet,
	soakRPCUserSettingsGet, soakRPCUserChatlistGet, soakRPCUserPriorityContacts,
	soakRPCUserAppsList, soakRPCUserAppsCategories,
	soakRPCUserSubscriptionCount, soakRPCUserSubscriptionByRoom,
	soakRPCUserSubscriptionChannel, soakRPCUserSubscriptionDM,
	soakRPCUserThreadList, soakRPCUserThreadUnread,
}

func validSoakRPCAction(action soakRPCAction) bool {
	switch action {
	case soakRPCSend, soakRPCThreadReply, soakRPCLoadHistory, soakRPCLoadNext,
		soakRPCGetThread, soakRPCGetMessage, soakRPCReact, soakRPCEdit,
		soakRPCDelete, soakRPCPin, soakRPCUnpin, soakRPCPinnedList,
		soakRPCReadBack, soakRPCMarkRead, soakRPCScroll,
		soakRPCMemberAdd, soakRPCMemberRemove, soakRPCRoomRename,
		soakRPCMuteToggle, soakRPCRoomCreate, soakRPCMemberList,
		soakRPCRoomsInfo, soakRPCSubscriptionList, soakRPCRoomStateRead,
		soakRPCMessageRead, soakRPCReadReceiptList, soakRPCPresenceQuery,
		soakRPCSearchMessages, soakRPCSearchRooms, soakRPCSearchIndexProbe:
		return true
	default:
		return slices.Contains(soakUserReadActions, action)
	}
}

type soakErrorClass string

const (
	soakErrorTimeout      soakErrorClass = "timeout"
	soakErrorNoResponder  soakErrorClass = "no_responder"
	soakErrorDisconnected soakErrorClass = "disconnected"
	soakErrorUnavailable  soakErrorClass = "unavailable"
	soakErrorInternal     soakErrorClass = "internal"
	soakErrorNotFound     soakErrorClass = "not_found"
	soakErrorForbidden    soakErrorClass = "forbidden"
	soakErrorBadRequest   soakErrorClass = "bad_request"
	soakErrorConflict     soakErrorClass = "conflict"
	// soakErrorRequestEncode is a body that never reached the wire; it is the
	// only decode-shaped failure a mutation may treat as proven not-sent.
	soakErrorRequestEncode soakErrorClass = "request_encode"
	// soakErrorResponseDecode means the server replied and the reply could not
	// be parsed. The request was delivered, so any effect it had is real.
	soakErrorResponseDecode        soakErrorClass = "response_decode"
	soakErrorAssertion             soakErrorClass = "assertion"
	soakErrorAmbiguous             soakErrorClass = "ambiguous"
	soakErrorMutationTargetMissing soakErrorClass = "mutation_target_missing"
	soakErrorResponseTooLarge      soakErrorClass = "response_too_large"
	// soakErrorCanceled is the run itself going away, not the site failing.
	// Folding it into internal would spike a server-fault class at every
	// shutdown; leaving it empty made the recorder count the operation as a
	// success, because the outcome is derived from the class alone.
	soakErrorCanceled soakErrorClass = "canceled"
)

func validSoakErrorClass(class soakErrorClass) bool {
	switch class {
	case soakErrorTimeout, soakErrorNoResponder, soakErrorDisconnected,
		soakErrorUnavailable, soakErrorInternal, soakErrorNotFound,
		soakErrorForbidden, soakErrorBadRequest, soakErrorConflict,
		soakErrorRequestEncode, soakErrorResponseDecode,
		soakErrorAssertion, soakErrorAmbiguous,
		soakErrorMutationTargetMissing, soakErrorResponseTooLarge,
		soakErrorCanceled:
		return true
	default:
		return false
	}
}

type soakAssertionError struct {
	message string
}

func (e *soakAssertionError) Error() string { return e.message }

func newSoakAssertionError(message string) error {
	return &soakAssertionError{message: message}
}

func parseSoakErrorEnvelope(data []byte) error {
	parsed, ok := errcode.Parse(data)
	if !ok {
		return nil
	}
	return parsed
}

// soakReasonResponseTooLarge aliases the platform reason carried by the
// oversize reply envelope, so the harness cannot drift from the wire contract.
const soakReasonResponseTooLarge = errcode.ResponseTooLarge

// soakErrorReason is the service-supplied errcode reason, kept beside the
// collapsed class because the two forbidden answers a soak read can get need
// opposite responses: "not_subscribed" means the harness verified with an
// account the membership lane had already removed, "outside_access_window"
// means that account rejoined and its own older message now predates its
// history window.
type soakErrorReason string

// soakErrorReasonUnknown absorbs any reason not listed below. errcode's own
// registry lives in a _test.go file and cannot be imported, so this list is
// maintained here; the unknown bucket counting up is the signal to extend it.
const soakErrorReasonUnknown soakErrorReason = "unknown"

var soakKnownErrorReasons = map[errcode.Reason]soakErrorReason{
	errcode.MessageNotSubscribed:           soakErrorReason(errcode.MessageNotSubscribed),
	errcode.MessageOutsideAccessWindow:     soakErrorReason(errcode.MessageOutsideAccessWindow),
	errcode.MessageLargeRoomPostRestricted: soakErrorReason(errcode.MessageLargeRoomPostRestricted),
	errcode.PinDisabled:                    soakErrorReason(errcode.PinDisabled),
	errcode.PinLimitReached:                soakErrorReason(errcode.PinLimitReached),
	errcode.PinRoomTooLarge:                soakErrorReason(errcode.PinRoomTooLarge),
	errcode.RoomMaxSizeReached:             soakErrorReason(errcode.RoomMaxSizeReached),
	errcode.RoomNotMember:                  soakErrorReason(errcode.RoomNotMember),
	errcode.RoomNotOwner:                   soakErrorReason(errcode.RoomNotOwner),
	errcode.RoomLastOwnerCannotLeave:       soakErrorReason(errcode.RoomLastOwnerCannotLeave),
	errcode.RoomLastMemberCannotRemove:     soakErrorReason(errcode.RoomLastMemberCannotRemove),
	errcode.RoomTargetNotMember:            soakErrorReason(errcode.RoomTargetNotMember),
	errcode.RoomNonChannelOperation:        soakErrorReason(errcode.RoomNonChannelOperation),
	errcode.RoomReadReceiptsUnavailable:    soakErrorReason(errcode.RoomReadReceiptsUnavailable),
	errcode.RoomUserNotFound:               soakErrorReason(errcode.RoomUserNotFound),
	errcode.RoomSelfDM:                     soakErrorReason(errcode.RoomSelfDM),
	errcode.UserSubscriptionNotFound:       soakErrorReason(errcode.UserSubscriptionNotFound),
	soakReasonResponseTooLarge:             soakErrorReason(soakReasonResponseTooLarge),
}

func validSoakErrorReason(reason soakErrorReason) bool {
	if reason == "" || reason == soakErrorReasonUnknown {
		return true
	}
	for _, known := range soakKnownErrorReasons {
		if reason == known {
			return true
		}
	}
	return false
}

// classifySoakRPCReason returns the reason the service tagged the failure with,
// or "" when the error carries no errcode envelope (a transport timeout has no
// reason to report).
func classifySoakRPCReason(err error) soakErrorReason {
	if err == nil {
		return ""
	}
	var envelope *errcode.Error
	if !errors.As(err, &envelope) || envelope.Reason == "" {
		return ""
	}
	if known, ok := soakKnownErrorReasons[envelope.Reason]; ok {
		return known
	}
	return soakErrorReasonUnknown
}

func classifySoakRPCError(err error) soakErrorClass {
	if err == nil {
		return ""
	}
	var assertion *soakAssertionError
	if errors.As(err, &assertion) {
		return soakErrorAssertion
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
		return soakErrorTimeout
	}
	if errors.Is(err, context.Canceled) {
		return soakErrorCanceled
	}
	if errors.Is(err, nats.ErrNoResponders) {
		return soakErrorNoResponder
	}
	if errors.Is(err, nats.ErrDisconnected) || errors.Is(err, nats.ErrConnectionClosed) {
		return soakErrorDisconnected
	}

	var envelope *errcode.Error
	if errors.As(err, &envelope) {
		switch envelope.Code {
		case errcode.CodeBadRequest:
			return soakErrorBadRequest
		case errcode.CodeUnauthenticated, errcode.CodeForbidden:
			return soakErrorForbidden
		case errcode.CodeNotFound:
			return soakErrorNotFound
		case errcode.CodeConflict:
			return soakErrorConflict
		case errcode.CodeTooManyRequests, errcode.CodeUnavailable:
			return soakErrorUnavailable
		case errcode.CodeInternal:
			// pkg/natsutil replies with a compact oversize envelope when a
			// response would exceed the broker's max_payload. It is code
			// `internal`, so without this branch an over-large page reads as a
			// server fault and the operator cannot tell "lower --page-limit"
			// from "the service is broken".
			if envelope.Reason == soakReasonResponseTooLarge {
				return soakErrorResponseTooLarge
			}
			return soakErrorInternal
		default:
			return soakErrorInternal
		}
	}
	return soakErrorInternal
}

type soakRetryMode uint8

const (
	soakRetryNever soakRetryMode = iota
	soakRetrySafe
	soakRetryAmbiguous
)

type soakRetryConfig struct {
	MaxAttempts int
	MinBackoff  time.Duration
	MaxBackoff  time.Duration
	Jitter      float64
}

type soakRPCTransport interface {
	Request(
		ctx context.Context,
		subject string,
		data []byte,
		timeout time.Duration,
	) ([]byte, error)
}

type soakSleeper interface {
	Sleep(context.Context, time.Duration) error
}

type soakTimerSleeper struct{}

func (soakTimerSleeper) Sleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type soakRPCRequest struct {
	Action  soakRPCAction
	Subject string
	// Account and RoomID identify the request in the failure log. A metric
	// label cannot hold them (unbounded), so this is the only place a reader
	// learns which account to grep the server's logs for.
	Account          string
	RoomID           string
	Body             any
	Timeout          time.Duration
	RetryMode        soakRetryMode
	ResolveAmbiguity func(context.Context) (retryNeeded bool, err error)
}

type soakRPCResult struct {
	Attempts int
	Retries  int
	// ReplyBytes is the wire size of a SUCCESSFUL reply. A transport failure
	// has nothing to size and an oversize failure carries only the compact
	// envelope, so both leave it zero rather than reporting a tiny page.
	ReplyBytes        int
	ErrorClass        soakErrorClass
	ErrorReason       soakErrorReason
	AmbiguityResolved bool
}

type soakRPCClient struct {
	transport soakRPCTransport
	retry     soakRetryConfig
	sleeper   soakSleeper
	random    func() float64
}

var errSoakRetryExhausted = errors.New("soak RPC retry attempts exhausted")

func newSoakRPCClient(
	transport soakRPCTransport,
	retry soakRetryConfig,
	sleeper soakSleeper,
	random func() float64,
) *soakRPCClient {
	if retry.MaxAttempts <= 0 {
		retry.MaxAttempts = 1
	}
	if retry.MinBackoff <= 0 {
		retry.MinBackoff = 100 * time.Millisecond
	}
	if retry.MaxBackoff < retry.MinBackoff {
		retry.MaxBackoff = retry.MinBackoff
	}
	retry.Jitter = min(max(retry.Jitter, 0), 1)
	if sleeper == nil {
		sleeper = soakTimerSleeper{}
	}
	if random == nil {
		random = func() float64 { return 0.5 }
	}
	return &soakRPCClient{
		transport: transport,
		retry:     retry,
		sleeper:   sleeper,
		random:    random,
	}
}

//nolint:gocritic // hugeParam: the request carries the failure identity; the copy is nothing beside the round trip.
func (c *soakRPCClient) Call(
	ctx context.Context,
	request soakRPCRequest,
	response any,
) (soakRPCResult, error) {
	var result soakRPCResult
	// carry stamps the request identity onto every failure leaving this
	// function, so the lane logger above does not have to thread it through.
	// Declared before the first guard: a context that died before the wire is
	// still a failure the operator has to be able to place.
	carry := func(err error) error {
		if err == nil {
			return nil
		}
		return &soakRequestError{
			Action: request.Action, Subject: request.Subject,
			Account: request.Account, RoomID: request.RoomID,
			Class: result.ErrorClass, Reason: result.ErrorReason,
			Attempts: result.Attempts, Retries: result.Retries,
			err: err,
		}
	}
	if err := ctx.Err(); err != nil {
		result.ErrorClass = classifySoakRPCError(err)
		return result, carry(err)
	}
	if !validSoakRPCAction(request.Action) {
		result.ErrorClass = soakErrorInternal
		return result, carry(fmt.Errorf("invalid soak RPC action %q", request.Action))
	}
	body, err := json.Marshal(request.Body)
	if err != nil {
		result.ErrorClass = soakErrorRequestEncode
		return result, carry(fmt.Errorf("marshal %s request: %w", request.Action, err))
	}

	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			// Only when no attempt reached the wire. Once one has failed, that
			// failure is why the operation failed and the cancellation merely
			// ended the retrying — overwriting it would erase a timeout whose
			// effect on the server is unknown.
			if result.ErrorClass == "" {
				result.ErrorClass = classifySoakRPCError(err)
			}
			return result, carry(err)
		}
		result.Attempts++
		reply, requestErr := c.transport.Request(
			ctx,
			request.Subject,
			body,
			request.Timeout,
		)
		if requestErr == nil {
			requestErr = parseSoakErrorEnvelope(reply)
		}
		if requestErr == nil {
			if response != nil {
				if err := json.Unmarshal(reply, response); err != nil {
					result.ErrorClass = soakErrorResponseDecode
					return result, carry(fmt.Errorf("decode %s response: %w", request.Action, err))
				}
			}
			result.ErrorClass = ""
			result.ErrorReason = ""
			result.ReplyBytes = len(reply)
			return result, nil
		}

		class := classifySoakRPCError(requestErr)
		result.ErrorClass = class
		result.ErrorReason = classifySoakRPCReason(requestErr)
		retry, resolved, resolveErr := c.shouldRetryAmbiguous(ctx, request, class)
		if resolveErr != nil {
			result.ErrorClass = classifySoakRPCError(resolveErr)
			result.ErrorReason = classifySoakRPCReason(resolveErr)
			return result, carry(fmt.Errorf("resolve %s ambiguity: %w", request.Action, resolveErr))
		}
		if resolved {
			result.AmbiguityResolved = true
			result.ErrorClass = ""
			result.ErrorReason = ""
			return result, nil
		}
		if request.RetryMode != soakRetryAmbiguous {
			retry = request.RetryMode == soakRetrySafe && transientSoakError(class)
		}
		if !retry {
			if request.RetryMode == soakRetryAmbiguous && transientSoakError(class) {
				result.ErrorClass = soakErrorAmbiguous
				return result, carry(fmt.Errorf("%s result is ambiguous: %w", request.Action, requestErr))
			}
			return result, carry(fmt.Errorf("%s request failed: %w", request.Action, requestErr))
		}
		if attempt == c.retry.MaxAttempts {
			return result, carry(fmt.Errorf(
				"%w: %s: %w",
				errSoakRetryExhausted,
				request.Action,
				requestErr,
			))
		}

		delay := c.backoff(result.Retries)
		if err := c.sleeper.Sleep(ctx, delay); err != nil {
			return result, carry(fmt.Errorf("wait to retry %s: %w", request.Action, err))
		}
		result.Retries++
	}

	return result, carry(fmt.Errorf(
		"RPC retry loop exited unexpectedly: %w",
		errSoakRetryExhausted,
	))
}

//nolint:gocritic // hugeParam: the request carries the failure identity; the copy is nothing beside the round trip.
func (c *soakRPCClient) shouldRetryAmbiguous(
	ctx context.Context,
	request soakRPCRequest,
	class soakErrorClass,
) (retry bool, resolved bool, err error) {
	if request.RetryMode != soakRetryAmbiguous || !transientSoakError(class) {
		return false, false, nil
	}
	if request.ResolveAmbiguity == nil {
		return false, false, nil
	}
	retryNeeded, err := request.ResolveAmbiguity(ctx)
	if err != nil {
		return false, false, err
	}
	return retryNeeded, !retryNeeded, nil
}

func transientSoakError(class soakErrorClass) bool {
	switch class {
	case soakErrorTimeout, soakErrorNoResponder, soakErrorDisconnected,
		soakErrorUnavailable, soakErrorInternal:
		return true
	default:
		return false
	}
}

func (c *soakRPCClient) backoff(retry int) time.Duration {
	base := float64(c.retry.MinBackoff) * math.Pow(2, float64(retry))
	base = min(base, float64(c.retry.MaxBackoff))
	factor := 1 + c.retry.Jitter*(2*min(max(c.random(), 0), 1)-1)
	return time.Duration(base * factor)
}
