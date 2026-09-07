package read

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/wire"
)

type VerifyClass string

const (
	VerifyOK        VerifyClass = "ok"
	VerifySkipped   VerifyClass = "skipped"
	VerifyMissing   VerifyClass = "missing"
	VerifyMismatch  VerifyClass = "mismatch"
	VerifyMalformed VerifyClass = "malformed"
	VerifyRetryable VerifyClass = "retryable_rpc"
	VerifyRPCError  VerifyClass = "rpc_error"
)

// VerifyField names the read-back field that disagreed. It is a metric
// label, so the set is closed: a message_id/room_id/author mismatch is a
// service-side correctness failure, while content/deleted usually means the
// harness read a mutation the write path had not persisted yet, and the two
// need different responses.
type VerifyField string

const (
	VerifyFieldNone       VerifyField = ""
	VerifyFieldMessageID  VerifyField = "message_id"
	VerifyFieldRoomID     VerifyField = "room_id"
	VerifyFieldAuthor     VerifyField = "author"
	VerifyFieldDeleted    VerifyField = "deleted"
	VerifyFieldContent    VerifyField = "content"
	VerifyFieldEditedAt   VerifyField = "edited_at"
	VerifyFieldPagination VerifyField = "pagination"
)

var allVerifyFields = [...]VerifyField{
	VerifyFieldNone, VerifyFieldMessageID, VerifyFieldRoomID,
	VerifyFieldAuthor, VerifyFieldDeleted, VerifyFieldContent,
	VerifyFieldEditedAt, VerifyFieldPagination,
}

func ValidVerifyField(field VerifyField) bool {
	for _, known := range allVerifyFields {
		if field == known {
			return true
		}
	}
	return false
}

type VerifyConfig struct {
	SiteID         string
	PageLimit      int
	MaxPages       int
	RequestTimeout time.Duration
}

type VerifyMessage struct {
	RoomID    string                `json:"roomId"`
	MessageID string                `json:"messageId"`
	Sender    cassandra.Participant `json:"sender"`
	Msg       string                `json:"msg"`
	Deleted   bool                  `json:"deleted,omitempty"`
	EditedAt  *time.Time            `json:"editedAt,omitempty"`
	CreatedAt time.Time             `json:"createdAt"`
}

type verifyHistoryResponse struct {
	Messages []VerifyMessage `json:"messages"`
}

type VerifyResult struct {
	Class          VerifyClass
	Action         rpc.Action
	ExpectedAction rpc.Action
	RoomID         string
	MessageID      string
	Field          VerifyField
	RPCErrorClass  rpc.ErrorClass
	RPCErrorReason rpc.ErrorReason
	Retries        int
	Pages          int
	Latency        time.Duration
}

func (r VerifyResult) String() string {
	fields := []string{
		"class=" + string(r.Class),
		"action=" + string(r.Action),
		"room_id=" + r.RoomID,
		"message_id=" + r.MessageID,
	}
	if r.Field != "" {
		fields = append(fields, "field="+string(r.Field))
	}
	return strings.Join(fields, " ")
}

type VerifyResultRecorder interface {
	Record(*VerifyResult)
}

type Verifier struct {
	cfg      VerifyConfig
	catalog  *catalog.Catalog
	rpc      *rpc.Client
	recorder VerifyResultRecorder
	now      func() time.Time

	sampleMu       sync.Mutex
	sampleSequence int
}

func NewVerifier(
	cfg *VerifyConfig,
	messageCatalog *catalog.Catalog,
	rpcClient *rpc.Client,
	recorder VerifyResultRecorder,
	now func() time.Time,
) *Verifier {
	if cfg == nil {
		cfg = &VerifyConfig{}
	}
	config := *cfg
	if config.PageLimit <= 0 {
		config.PageLimit = 50
	}
	if config.MaxPages <= 0 {
		config.MaxPages = 20
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 5 * time.Second
	}
	if now == nil {
		now = time.Now
	}
	return &Verifier{
		cfg: config, catalog: messageCatalog, rpc: rpcClient,
		recorder: recorder, now: now,
	}
}

func (v *Verifier) Sample(
	ctx context.Context,
	roomID string,
) VerifyResult {
	v.sampleMu.Lock()
	v.sampleSequence++
	preferMutated := v.sampleSequence%10 == 0
	v.sampleMu.Unlock()

	message, ok := v.catalog.PickVerificationCandidate(
		roomID,
		preferMutated,
	)
	if !ok {
		result := VerifyResult{
			Class: VerifySkipped, Action: rpc.ActionReadBack, RoomID: roomID,
		}
		v.record(&result)
		return result
	}
	return v.VerifyByID(ctx, roomID, message.ID)
}

func (v *Verifier) VerifyByID(
	ctx context.Context,
	roomID string,
	messageID string,
) VerifyResult {
	expected, ok := v.catalog.GetVerificationCandidate(roomID, messageID)
	if !ok {
		result := VerifyResult{
			Class: VerifySkipped, Action: rpc.ActionGetMessage,
			RoomID: roomID, MessageID: messageID,
		}
		v.record(&result)
		return result
	}
	result := newVerifyResult(
		rpc.ActionGetMessage,
		roomID,
		messageID,
		&expected,
	)
	var response VerifyMessage
	startedAt := v.now()
	rpcResult, err := v.rpc.Call(ctx, rpc.Request{
		Action: rpc.ActionGetMessage,
		Subject: subject.MsgGet(
			expected.Author,
			roomID,
			v.cfg.SiteID,
		),
		Account:   expected.Author,
		RoomID:    roomID,
		Body:      wire.GetMessageByIDRequest{MessageID: messageID},
		Timeout:   v.cfg.RequestTimeout,
		RetryMode: rpc.RetrySafe,
	}, &response)
	result.Latency = v.now().Sub(startedAt)
	result.Retries = rpcResult.Retries
	if err != nil {
		ClassifyRPCError(&result, rpcResult.ErrorClass, rpcResult.ErrorReason)
		v.record(&result)
		return result
	}
	CompareVerifiedMessage(&result, &expected, &response)
	v.record(&result)
	return result
}

func (v *Verifier) VerifyHistory(
	ctx context.Context,
	roomID string,
	messageID string,
) VerifyResult {
	expected, ok := v.catalog.GetVerificationCandidate(roomID, messageID)
	if !ok {
		result := VerifyResult{
			Class: VerifySkipped, Action: rpc.ActionLoadHistory,
			RoomID: roomID, MessageID: messageID,
		}
		v.record(&result)
		return result
	}
	result := newVerifyResult(
		rpc.ActionLoadHistory,
		roomID,
		messageID,
		&expected,
	)
	beforeMillis := expected.CreatedAt.UTC().UnixMilli() + 1
	before := &beforeMillis
	lastMsgAt := v.now().UTC().UnixMilli()
	meta := &wire.RoomMeta{LastMsgAt: &lastMsgAt}
	var totalLatency time.Duration
	for range v.cfg.MaxPages {
		var response verifyHistoryResponse
		startedAt := v.now()
		rpcResult, err := v.rpc.Call(ctx, rpc.Request{
			Action: rpc.ActionLoadHistory,
			Subject: subject.MsgHistory(
				expected.Author,
				roomID,
				v.cfg.SiteID,
			),
			Account: expected.Author, RoomID: roomID,
			Body: wire.LoadHistoryRequest{
				Before: before,
				Limit:  v.cfg.PageLimit,
				Meta:   meta,
			},
			Timeout: v.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
		}, &response)
		totalLatency += v.now().Sub(startedAt)
		result.Retries += rpcResult.Retries
		if err != nil {
			result.Latency = totalLatency
			ClassifyRPCError(&result, rpcResult.ErrorClass, rpcResult.ErrorReason)
			v.record(&result)
			return result
		}
		result.Pages++
		for i := range response.Messages {
			if response.Messages[i].MessageID != messageID {
				continue
			}
			result.Latency = totalLatency
			CompareVerifiedMessage(
				&result,
				&expected,
				&response.Messages[i],
			)
			v.record(&result)
			return result
		}
		if len(response.Messages) == 0 {
			break
		}
		oldest := oldestVerifyMillis(response.Messages)
		if before != nil && oldest >= *before {
			result.Class = VerifyMismatch
			result.Field = VerifyFieldPagination
			result.Latency = totalLatency
			v.record(&result)
			return result
		}
		nextBefore := oldest - 1
		before = &nextBefore
	}
	result.Class = VerifyMissing
	result.Latency = totalLatency
	v.record(&result)
	return result
}

func newVerifyResult(
	action rpc.Action,
	roomID string,
	messageID string,
	expected *catalog.Message,
) VerifyResult {
	expectedAction := rpc.ActionReadBack
	switch {
	case expected.Deleted:
		expectedAction = rpc.ActionDelete
	case expected.Edited:
		expectedAction = rpc.ActionEdit
	}
	return VerifyResult{
		Class: VerifyOK, Action: action, ExpectedAction: expectedAction,
		RoomID: roomID, MessageID: messageID,
	}
}

func ClassifyRPCError(
	result *VerifyResult,
	class rpc.ErrorClass,
	reason rpc.ErrorReason,
) {
	result.RPCErrorClass = class
	result.RPCErrorReason = reason
	switch {
	case class == rpc.ErrorNotFound:
		result.Class = VerifyMissing
	case class == rpc.ErrorRequestEncode, class == rpc.ErrorResponseDecode:
		result.Class = VerifyMalformed
	case rpc.IsTransientError(class):
		result.Class = VerifyRetryable
	default:
		result.Class = VerifyRPCError
	}
}

func CompareVerifiedMessage(
	result *VerifyResult,
	expected *catalog.Message,
	actual *VerifyMessage,
) {
	switch {
	case actual.MessageID != expected.ID:
		result.Class = VerifyMismatch
		result.Field = VerifyFieldMessageID
	case actual.RoomID != expected.RoomID:
		result.Class = VerifyMismatch
		result.Field = VerifyFieldRoomID
	case actual.Sender.Account != expected.Author:
		result.Class = VerifyMismatch
		result.Field = VerifyFieldAuthor
	case actual.Deleted != expected.Deleted:
		result.Class = VerifyMismatch
		result.Field = VerifyFieldDeleted
	case !expected.Deleted && catalog.ContentDigest(actual.Msg) != expected.ContentSHA256:
		result.Class = VerifyMismatch
		result.Field = VerifyFieldContent
	case expected.Edited && actual.EditedAt == nil:
		result.Class = VerifyMismatch
		result.Field = VerifyFieldEditedAt
	default:
		result.Class = VerifyOK
		result.Field = VerifyFieldNone
	}
}

func oldestVerifyMillis(messages []VerifyMessage) int64 {
	oldest := messages[0].CreatedAt.UTC().UnixMilli()
	for i := 1; i < len(messages); i++ {
		createdAt := messages[i].CreatedAt.UTC().UnixMilli()
		if createdAt < oldest {
			oldest = createdAt
		}
	}
	return oldest
}

func (v *Verifier) record(result *VerifyResult) {
	if v.recorder != nil {
		v.recorder.Record(result)
	}
}
