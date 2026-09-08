package rpc

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

type Action string

const (
	ActionSend        Action = "send"
	ActionThreadReply Action = "thread_reply"
	ActionLoadHistory Action = "load_history"
	ActionLoadNext    Action = "load_next"
	ActionGetThread   Action = "get_thread_messages"
	ActionGetMessage  Action = "get_message_by_id"
	ActionReact       Action = "reaction"
	ActionEdit        Action = "edit"
	ActionDelete      Action = "delete"
	ActionPin         Action = "pin"
	ActionUnpin       Action = "unpin"
	ActionPinnedList  Action = "pinned_list"
	ActionReadBack    Action = "read_back"
	ActionMarkRead    Action = "mark_read"
	ActionScroll      Action = "scroll_history"

	ActionMemberAdd        Action = "member_add"
	ActionMemberRemove     Action = "member_remove"
	ActionRoomRename       Action = "room_rename"
	ActionMuteToggle       Action = "mute_toggle"
	ActionRoomCreate       Action = "room_create"
	ActionMemberList       Action = "member_list"
	ActionRoomsInfo        Action = "rooms_info"
	ActionSubscriptionList Action = "subscription_list"
	ActionRoomStateRead    Action = "room_state_read"
	ActionMessageRead      Action = "message_read"
	ActionReadReceiptList  Action = "read_receipt_list"
	ActionPresenceQuery    Action = "presence_query"

	ActionSearchMessages Action = "search_messages"
	ActionSearchRooms    Action = "search_rooms"
	// ActionSearchIndexProbe is the evidence query, kept on its own label so
	// its latency and error rate never blend into the read lane's.
	ActionSearchIndexProbe Action = "search_index_probe"

	// The user-service read lane. Every one of these is read-only, so they
	// carry latency and outcome only and never enter the evidence ledger.
	ActionUserMe                  Action = "user_me"
	ActionUserProfileGet          Action = "user_profile_get"
	ActionUserStatusGet           Action = "user_status_get"
	ActionUserSettingsGet         Action = "user_settings_get"
	ActionUserChatlistGet         Action = "user_chatlist_get"
	ActionUserPriorityContacts    Action = "user_priority_contacts"
	ActionUserAppsList            Action = "user_apps_list"
	ActionUserAppsCategories      Action = "user_apps_categories"
	ActionUserSubscriptionCount   Action = "user_subscription_count"
	ActionUserSubscriptionByRoom  Action = "user_subscription_by_room"
	ActionUserSubscriptionChannel Action = "user_subscription_channels"
	ActionUserSubscriptionDM      Action = "user_subscription_dm"
	ActionUserThreadList          Action = "user_thread_list"
	ActionUserThreadUnread        Action = "user_thread_unread"
)

// userReadActions is every action the user-service read lane dispatches. It
// is the single source for both the allowlist and the lane's own dispatch
// table, so an action can never be allowlisted without being sent.
var userReadActions = []Action{
	ActionUserMe, ActionUserProfileGet, ActionUserStatusGet,
	ActionUserSettingsGet, ActionUserChatlistGet, ActionUserPriorityContacts,
	ActionUserAppsList, ActionUserAppsCategories,
	ActionUserSubscriptionCount, ActionUserSubscriptionByRoom,
	ActionUserSubscriptionChannel, ActionUserSubscriptionDM,
	ActionUserThreadList, ActionUserThreadUnread,
}

func ValidAction(action Action) bool {
	switch action {
	case ActionSend, ActionThreadReply, ActionLoadHistory, ActionLoadNext,
		ActionGetThread, ActionGetMessage, ActionReact, ActionEdit,
		ActionDelete, ActionPin, ActionUnpin, ActionPinnedList,
		ActionReadBack, ActionMarkRead, ActionScroll,
		ActionMemberAdd, ActionMemberRemove, ActionRoomRename,
		ActionMuteToggle, ActionRoomCreate, ActionMemberList,
		ActionRoomsInfo, ActionSubscriptionList, ActionRoomStateRead,
		ActionMessageRead, ActionReadReceiptList, ActionPresenceQuery,
		ActionSearchMessages, ActionSearchRooms, ActionSearchIndexProbe:
		return true
	default:
		return slices.Contains(userReadActions, action)
	}
}

type ErrorClass string

const (
	ErrorTimeout      ErrorClass = "timeout"
	ErrorNoResponder  ErrorClass = "no_responder"
	ErrorDisconnected ErrorClass = "disconnected"
	ErrorUnavailable  ErrorClass = "unavailable"
	ErrorInternal     ErrorClass = "internal"
	ErrorNotFound     ErrorClass = "not_found"
	ErrorForbidden    ErrorClass = "forbidden"
	ErrorBadRequest   ErrorClass = "bad_request"
	ErrorConflict     ErrorClass = "conflict"
	// ErrorRequestEncode is a body that never reached the wire; it is the
	// only decode-shaped failure a mutation may treat as proven not-sent.
	ErrorRequestEncode ErrorClass = "request_encode"
	// ErrorResponseDecode means the server replied and the reply could not
	// be parsed. The request was delivered, so any effect it had is real.
	ErrorResponseDecode        ErrorClass = "response_decode"
	ErrorAssertion             ErrorClass = "assertion"
	ErrorAmbiguous             ErrorClass = "ambiguous"
	ErrorMutationTargetMissing ErrorClass = "mutation_target_missing"
	ErrorResponseTooLarge      ErrorClass = "response_too_large"
	// ErrorCanceled is the run itself going away, not the site failing.
	// Folding it into internal would spike a server-fault class at every
	// shutdown; leaving it empty made the recorder count the operation as a
	// success, because the outcome is derived from the class alone.
	ErrorCanceled ErrorClass = "canceled"
)

func ValidErrorClass(class ErrorClass) bool {
	switch class {
	case ErrorTimeout, ErrorNoResponder, ErrorDisconnected,
		ErrorUnavailable, ErrorInternal, ErrorNotFound,
		ErrorForbidden, ErrorBadRequest, ErrorConflict,
		ErrorRequestEncode, ErrorResponseDecode,
		ErrorAssertion, ErrorAmbiguous,
		ErrorMutationTargetMissing, ErrorResponseTooLarge,
		ErrorCanceled:
		return true
	default:
		return false
	}
}

type assertionError struct {
	message string
}

func (e *assertionError) Error() string { return e.message }

func NewAssertionError(message string) error {
	return &assertionError{message: message}
}

func ParseErrorEnvelope(data []byte) error {
	// FromReply, not Parse: the harness measures rejections, and Parse answers
	// whether the payload fits this build's Error struct rather than whether the
	// peer refused. One retyped field made a refusal read as a successful
	// response — a silent hole in the numbers a soak run exists to produce.
	// ClassifyError already falls through to ErrorInternal for the untyped error
	// FromReply returns for a code or shape it cannot model.
	return errcode.FromReply(data)
}

// ReasonResponseTooLarge aliases the platform reason carried by the
// oversize reply envelope, so the harness cannot drift from the wire contract.
const ReasonResponseTooLarge = errcode.ResponseTooLarge

// ErrorReason is the service-supplied errcode reason, kept beside the
// collapsed class because the two forbidden answers a soak read can get need
// opposite responses: "not_subscribed" means the harness verified with an
// account the membership lane had already removed, "outside_access_window"
// means that account rejoined and its own older message now predates its
// history window.
type ErrorReason string

// ReasonUnknown absorbs any reason not listed below. errcode's own
// registry lives in a _test.go file and cannot be imported, so this list is
// maintained here; the unknown bucket counting up is the signal to extend it.
const ReasonUnknown ErrorReason = "unknown"

var knownErrorReasons = map[errcode.Reason]ErrorReason{
	errcode.MessageNotSubscribed:           ErrorReason(errcode.MessageNotSubscribed),
	errcode.MessageOutsideAccessWindow:     ErrorReason(errcode.MessageOutsideAccessWindow),
	errcode.MessageLargeRoomPostRestricted: ErrorReason(errcode.MessageLargeRoomPostRestricted),
	errcode.PinDisabled:                    ErrorReason(errcode.PinDisabled),
	errcode.PinLimitReached:                ErrorReason(errcode.PinLimitReached),
	errcode.PinRoomTooLarge:                ErrorReason(errcode.PinRoomTooLarge),
	errcode.RoomMaxSizeReached:             ErrorReason(errcode.RoomMaxSizeReached),
	errcode.RoomNotMember:                  ErrorReason(errcode.RoomNotMember),
	errcode.RoomNotOwner:                   ErrorReason(errcode.RoomNotOwner),
	errcode.RoomLastOwnerCannotLeave:       ErrorReason(errcode.RoomLastOwnerCannotLeave),
	errcode.RoomLastMemberCannotRemove:     ErrorReason(errcode.RoomLastMemberCannotRemove),
	errcode.RoomTargetNotMember:            ErrorReason(errcode.RoomTargetNotMember),
	errcode.RoomNonChannelOperation:        ErrorReason(errcode.RoomNonChannelOperation),
	errcode.RoomReadReceiptsUnavailable:    ErrorReason(errcode.RoomReadReceiptsUnavailable),
	errcode.RoomUserNotFound:               ErrorReason(errcode.RoomUserNotFound),
	errcode.RoomSelfDM:                     ErrorReason(errcode.RoomSelfDM),
	errcode.UserSubscriptionNotFound:       ErrorReason(errcode.UserSubscriptionNotFound),
	ReasonResponseTooLarge:                 ErrorReason(ReasonResponseTooLarge),
}

func ValidErrorReason(reason ErrorReason) bool {
	if reason == "" || reason == ReasonUnknown {
		return true
	}
	for _, known := range knownErrorReasons {
		if reason == known {
			return true
		}
	}
	return false
}

// ClassifyReason returns the reason the service tagged the failure with,
// or "" when the error carries no errcode envelope (a transport timeout has no
// reason to report).
func ClassifyReason(err error) ErrorReason {
	if err == nil {
		return ""
	}
	var envelope *errcode.Error
	if !errors.As(err, &envelope) || envelope.Reason == "" {
		return ""
	}
	if known, ok := knownErrorReasons[envelope.Reason]; ok {
		return known
	}
	return ReasonUnknown
}

func ClassifyError(err error) ErrorClass {
	if err == nil {
		return ""
	}
	var assertion *assertionError
	if errors.As(err, &assertion) {
		return ErrorAssertion
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
		return ErrorTimeout
	}
	if errors.Is(err, context.Canceled) {
		return ErrorCanceled
	}
	if errors.Is(err, nats.ErrNoResponders) {
		return ErrorNoResponder
	}
	if errors.Is(err, nats.ErrDisconnected) || errors.Is(err, nats.ErrConnectionClosed) {
		return ErrorDisconnected
	}

	var envelope *errcode.Error
	if errors.As(err, &envelope) {
		switch envelope.Code {
		case errcode.CodeBadRequest:
			return ErrorBadRequest
		case errcode.CodeUnauthenticated, errcode.CodeForbidden:
			return ErrorForbidden
		case errcode.CodeNotFound:
			return ErrorNotFound
		case errcode.CodeConflict:
			return ErrorConflict
		case errcode.CodeTooManyRequests, errcode.CodeUnavailable:
			return ErrorUnavailable
		case errcode.CodeInternal:
			// pkg/natsutil replies with a compact oversize envelope when a
			// response would exceed the broker's max_payload. It is code
			// `internal`, so without this branch an over-large page reads as a
			// server fault and the operator cannot tell "lower --page-limit"
			// from "the service is broken".
			if envelope.Reason == ReasonResponseTooLarge {
				return ErrorResponseTooLarge
			}
			return ErrorInternal
		default:
			return ErrorInternal
		}
	}
	return ErrorInternal
}

type RetryMode uint8

const (
	RetryNever RetryMode = iota
	RetrySafe
	RetryAmbiguous
)

type RetryConfig struct {
	MaxAttempts int
	MinBackoff  time.Duration
	MaxBackoff  time.Duration
	Jitter      float64
}

type Transport interface {
	Request(
		ctx context.Context,
		subject string,
		data []byte,
		timeout time.Duration,
	) ([]byte, error)
}

type Sleeper interface {
	Sleep(context.Context, time.Duration) error
}

type TimerSleeper struct{}

func (TimerSleeper) Sleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type Request struct {
	Action  Action
	Subject string
	// Account and RoomID identify the request in the failure log. A metric
	// label cannot hold them (unbounded), so this is the only place a reader
	// learns which account to grep the server's logs for.
	Account          string
	RoomID           string
	Body             any
	Timeout          time.Duration
	RetryMode        RetryMode
	ResolveAmbiguity func(context.Context) (retryNeeded bool, err error)
}

type Result struct {
	Attempts int
	Retries  int
	// ReplyBytes is the wire size of a SUCCESSFUL reply. A transport failure
	// has nothing to size and an oversize failure carries only the compact
	// envelope, so both leave it zero rather than reporting a tiny page.
	ReplyBytes        int
	ErrorClass        ErrorClass
	ErrorReason       ErrorReason
	AmbiguityResolved bool
}

type Client struct {
	transport Transport
	retry     RetryConfig
	sleeper   Sleeper
	random    func() float64
}

var ErrRetryExhausted = errors.New("soak RPC retry attempts exhausted")

func NewClient(
	transport Transport,
	retry RetryConfig,
	sleeper Sleeper,
	random func() float64,
) *Client {
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
		sleeper = TimerSleeper{}
	}
	if random == nil {
		random = func() float64 { return 0.5 }
	}
	return &Client{
		transport: transport,
		retry:     retry,
		sleeper:   sleeper,
		random:    random,
	}
}

//nolint:gocritic // hugeParam: the request carries the failure identity; the copy is nothing beside the round trip.
func (c *Client) Call(
	ctx context.Context,
	request Request,
	response any,
) (Result, error) {
	var result Result
	// carry stamps the request identity onto every failure leaving this
	// function, so the lane logger above does not have to thread it through.
	// Declared before the first guard: a context that died before the wire is
	// still a failure the operator has to be able to place.
	carry := func(err error) error {
		if err == nil {
			return nil
		}
		return &RequestError{
			Action: request.Action, Subject: request.Subject,
			Account: request.Account, RoomID: request.RoomID,
			Class: result.ErrorClass, Reason: result.ErrorReason,
			Attempts: result.Attempts, Retries: result.Retries,
			Cause: err,
		}
	}
	if err := ctx.Err(); err != nil {
		result.ErrorClass = ClassifyError(err)
		return result, carry(interruptedError(request.Action, err, nil))
	}
	if !ValidAction(request.Action) {
		result.ErrorClass = ErrorInternal
		return result, carry(fmt.Errorf("invalid soak RPC action %q", request.Action))
	}
	body, err := json.Marshal(request.Body)
	if err != nil {
		result.ErrorClass = ErrorRequestEncode
		return result, carry(fmt.Errorf("marshal %s request: %w", request.Action, err))
	}

	// The failure that sent the last attempt back for a retry. A cancellation
	// stops the retrying without explaining anything, so both exits below name
	// this too — otherwise the message contradicts the class kept with it.
	var lastRequestErr error
	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			// Only when no attempt reached the wire. Once one has failed, that
			// failure is why the operation failed and the cancellation merely
			// ended the retrying — overwriting it would erase a timeout whose
			// effect on the server is unknown.
			if result.ErrorClass == "" {
				result.ErrorClass = ClassifyError(err)
			}
			return result, carry(
				interruptedError(request.Action, err, lastRequestErr),
			)
		}
		if attempt > 1 {
			// Counted here rather than after the backoff: a run torn down
			// between the two would otherwise report a retry that never
			// reached the transport. Retries stays Attempts-1.
			result.Retries++
		}
		result.Attempts++
		reply, requestErr := c.transport.Request(
			ctx,
			request.Subject,
			body,
			request.Timeout,
		)
		if requestErr == nil {
			requestErr = ParseErrorEnvelope(reply)
		}
		if requestErr == nil {
			if response != nil {
				if err := json.Unmarshal(reply, response); err != nil {
					result.ErrorClass = ErrorResponseDecode
					return result, carry(fmt.Errorf("decode %s response: %w", request.Action, err))
				}
			}
			result.ErrorClass = ""
			result.ErrorReason = ""
			result.ReplyBytes = len(reply)
			return result, nil
		}

		class := ClassifyError(requestErr)
		result.ErrorClass = class
		result.ErrorReason = ClassifyReason(requestErr)
		retry, resolved, resolveErr := c.shouldRetryAmbiguous(ctx, request, class)
		if resolveErr != nil {
			result.ErrorClass = ClassifyError(resolveErr)
			result.ErrorReason = ClassifyReason(resolveErr)
			return result, carry(fmt.Errorf("resolve %s ambiguity: %w", request.Action, resolveErr))
		}
		if resolved {
			result.AmbiguityResolved = true
			result.ErrorClass = ""
			result.ErrorReason = ""
			return result, nil
		}
		if request.RetryMode != RetryAmbiguous {
			retry = request.RetryMode == RetrySafe && IsTransientError(class)
		}
		if !retry {
			if request.RetryMode == RetryAmbiguous && IsTransientError(class) {
				result.ErrorClass = ErrorAmbiguous
				return result, carry(fmt.Errorf("%s result is ambiguous: %w", request.Action, requestErr))
			}
			return result, carry(fmt.Errorf("%s request failed: %w", request.Action, requestErr))
		}
		if attempt == c.retry.MaxAttempts {
			// wraps plain sentinels in loadgen internals, not *errcode.Error; the one-per-chain invariant does not apply
			// nosemgrep: errcode-no-multi-wrap-errcode
			return result, carry(fmt.Errorf(
				"%w: %s: %w",
				ErrRetryExhausted,
				request.Action,
				requestErr,
			))
		}

		lastRequestErr = requestErr
		delay := c.backoff(result.Retries)
		if err := c.sleeper.Sleep(ctx, delay); err != nil {
			return result, carry(
				interruptedError(request.Action, err, lastRequestErr),
			)
		}
	}

	return result, carry(fmt.Errorf(
		"RPC retry loop exited unexpectedly: %w",
		ErrRetryExhausted,
	))
}

//nolint:gocritic // hugeParam: the request carries the failure identity; the copy is nothing beside the round trip.
func (c *Client) shouldRetryAmbiguous(
	ctx context.Context,
	request Request,
	class ErrorClass,
) (retry bool, resolved bool, err error) {
	if request.RetryMode != RetryAmbiguous || !IsTransientError(class) {
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

// interruptedError names both halves of an interrupted request: why the
// operation failed, and why it stopped being retried. Reporting only the
// cancellation would leave the message disagreeing with the error class, which
// is kept from the attempt that actually reached the wire.
func interruptedError(action Action, cancelErr, lastErr error) error {
	if lastErr == nil {
		return fmt.Errorf("%s interrupted: %w", action, cancelErr)
	}
	// wraps plain sentinels in loadgen internals, not *errcode.Error; the one-per-chain invariant does not apply
	// nosemgrep: errcode-no-multi-wrap-errcode
	return fmt.Errorf("%s retry interrupted: %w: last attempt: %w",
		action, cancelErr, lastErr)
}

func IsTransientError(class ErrorClass) bool {
	switch class {
	case ErrorTimeout, ErrorNoResponder, ErrorDisconnected,
		ErrorUnavailable, ErrorInternal:
		return true
	default:
		return false
	}
}

func (c *Client) backoff(retry int) time.Duration {
	base := float64(c.retry.MinBackoff) * math.Pow(2, float64(retry))
	base = min(base, float64(c.retry.MaxBackoff))
	factor := 1 + c.retry.Jitter*(2*min(max(c.random(), 0), 1)-1)
	return time.Duration(base * factor)
}

func UserReadActions() []Action {
	return slices.Clone(userReadActions)
}
