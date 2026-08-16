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
		soakRPCMessageRead, soakRPCReadReceiptList, soakRPCPresenceQuery:
		return true
	default:
		return slices.Contains(soakUserReadActions, action)
	}
}

type soakErrorClass string

const (
	soakErrorTimeout               soakErrorClass = "timeout"
	soakErrorNoResponder           soakErrorClass = "no_responder"
	soakErrorDisconnected          soakErrorClass = "disconnected"
	soakErrorUnavailable           soakErrorClass = "unavailable"
	soakErrorInternal              soakErrorClass = "internal"
	soakErrorNotFound              soakErrorClass = "not_found"
	soakErrorForbidden             soakErrorClass = "forbidden"
	soakErrorBadRequest            soakErrorClass = "bad_request"
	soakErrorConflict              soakErrorClass = "conflict"
	soakErrorDecode                soakErrorClass = "decode"
	soakErrorAssertion             soakErrorClass = "assertion"
	soakErrorAmbiguous             soakErrorClass = "ambiguous"
	soakErrorMutationTargetMissing soakErrorClass = "mutation_target_missing"
	soakErrorResponseTooLarge      soakErrorClass = "response_too_large"
)

func validSoakErrorClass(class soakErrorClass) bool {
	switch class {
	case soakErrorTimeout, soakErrorNoResponder, soakErrorDisconnected,
		soakErrorUnavailable, soakErrorInternal, soakErrorNotFound,
		soakErrorForbidden, soakErrorBadRequest, soakErrorConflict,
		soakErrorDecode, soakErrorAssertion, soakErrorAmbiguous,
		soakErrorMutationTargetMissing, soakErrorResponseTooLarge:
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

// soakReasonResponseTooLarge mirrors the reason in pkg/natsutil's oversize
// reply envelope. Declared here rather than in pkg/errcode so this change stays
// inside tools/loadgen; if a named constant is ever added upstream, this should
// become an alias for it.
const soakReasonResponseTooLarge errcode.Reason = "response_too_large"

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
	Action           soakRPCAction
	Subject          string
	Body             any
	Timeout          time.Duration
	RetryMode        soakRetryMode
	ResolveAmbiguity func(context.Context) (retryNeeded bool, err error)
}

type soakRPCResult struct {
	Attempts          int
	Retries           int
	ErrorClass        soakErrorClass
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

func (c *soakRPCClient) Call(
	ctx context.Context,
	request soakRPCRequest,
	response any,
) (soakRPCResult, error) {
	var result soakRPCResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !validSoakRPCAction(request.Action) {
		result.ErrorClass = soakErrorInternal
		return result, fmt.Errorf("invalid soak RPC action %q", request.Action)
	}
	body, err := json.Marshal(request.Body)
	if err != nil {
		result.ErrorClass = soakErrorDecode
		return result, fmt.Errorf("marshal %s request: %w", request.Action, err)
	}

	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return result, err
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
					result.ErrorClass = soakErrorDecode
					return result, fmt.Errorf("decode %s response: %w", request.Action, err)
				}
			}
			result.ErrorClass = ""
			return result, nil
		}

		class := classifySoakRPCError(requestErr)
		result.ErrorClass = class
		retry, resolved, resolveErr := c.shouldRetryAmbiguous(ctx, request, class)
		if resolveErr != nil {
			result.ErrorClass = classifySoakRPCError(resolveErr)
			return result, fmt.Errorf("resolve %s ambiguity: %w", request.Action, resolveErr)
		}
		if resolved {
			result.AmbiguityResolved = true
			result.ErrorClass = ""
			return result, nil
		}
		if request.RetryMode != soakRetryAmbiguous {
			retry = request.RetryMode == soakRetrySafe && transientSoakError(class)
		}
		if !retry {
			if request.RetryMode == soakRetryAmbiguous && transientSoakError(class) {
				result.ErrorClass = soakErrorAmbiguous
				return result, fmt.Errorf("%s result is ambiguous: %w", request.Action, requestErr)
			}
			return result, fmt.Errorf("%s request failed: %w", request.Action, requestErr)
		}
		if attempt == c.retry.MaxAttempts {
			return result, fmt.Errorf(
				"%w: %s: %w",
				errSoakRetryExhausted,
				request.Action,
				requestErr,
			)
		}

		delay := c.backoff(result.Retries)
		if err := c.sleeper.Sleep(ctx, delay); err != nil {
			return result, fmt.Errorf("wait to retry %s: %w", request.Action, err)
		}
		result.Retries++
	}

	return result, fmt.Errorf(
		"RPC retry loop exited unexpectedly: %w",
		errSoakRetryExhausted,
	)
}

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
