package main

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
)

type soakVerifyClass string

const (
	soakVerifyOK        soakVerifyClass = "ok"
	soakVerifySkipped   soakVerifyClass = "skipped"
	soakVerifyMissing   soakVerifyClass = "missing"
	soakVerifyMismatch  soakVerifyClass = "mismatch"
	soakVerifyMalformed soakVerifyClass = "malformed"
	soakVerifyRetryable soakVerifyClass = "retryable_rpc"
	soakVerifyRPCError  soakVerifyClass = "rpc_error"
)

type soakVerifyConfig struct {
	SiteID         string
	PageLimit      int
	MaxPages       int
	RequestTimeout time.Duration
}

type soakVerifyMessage struct {
	RoomID    string                `json:"roomId"`
	MessageID string                `json:"messageId"`
	Sender    cassandra.Participant `json:"sender"`
	Msg       string                `json:"msg"`
	Deleted   bool                  `json:"deleted,omitempty"`
	EditedAt  *time.Time            `json:"editedAt,omitempty"`
	CreatedAt time.Time             `json:"createdAt"`
}

type soakVerifyHistoryResponse struct {
	Messages []soakVerifyMessage `json:"messages"`
}

type soakVerifyResult struct {
	Class          soakVerifyClass
	Action         soakRPCAction
	ExpectedAction soakRPCAction
	RoomID         string
	MessageID      string
	Field          string
	RPCErrorClass  soakErrorClass
	Retries        int
	Pages          int
	Latency        time.Duration
}

func (r soakVerifyResult) String() string {
	fields := []string{
		"class=" + string(r.Class),
		"action=" + string(r.Action),
		"room_id=" + r.RoomID,
		"message_id=" + r.MessageID,
	}
	if r.Field != "" {
		fields = append(fields, "field="+r.Field)
	}
	return strings.Join(fields, " ")
}

type soakVerifyResultRecorder interface {
	Record(*soakVerifyResult)
}

type soakVerifier struct {
	cfg      soakVerifyConfig
	catalog  *soakCatalog
	rpc      *soakRPCClient
	recorder soakVerifyResultRecorder
	now      func() time.Time

	sampleMu       sync.Mutex
	sampleSequence int
}

func newSoakVerifier(
	cfg *soakVerifyConfig,
	catalog *soakCatalog,
	rpc *soakRPCClient,
	recorder soakVerifyResultRecorder,
	now func() time.Time,
) *soakVerifier {
	if cfg == nil {
		cfg = &soakVerifyConfig{}
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
	return &soakVerifier{
		cfg: config, catalog: catalog, rpc: rpc, recorder: recorder, now: now,
	}
}

func (v *soakVerifier) Sample(
	ctx context.Context,
	roomID string,
) soakVerifyResult {
	v.sampleMu.Lock()
	v.sampleSequence++
	preferMutated := v.sampleSequence%10 == 0
	v.sampleMu.Unlock()

	message, ok := v.catalog.PickVerificationCandidate(
		roomID,
		preferMutated,
	)
	if !ok {
		result := soakVerifyResult{
			Class: soakVerifySkipped, Action: soakRPCReadBack, RoomID: roomID,
		}
		v.record(&result)
		return result
	}
	return v.VerifyByID(ctx, roomID, message.ID)
}

func (v *soakVerifier) VerifyByID(
	ctx context.Context,
	roomID string,
	messageID string,
) soakVerifyResult {
	expected, ok := v.catalog.GetVerificationCandidate(roomID, messageID)
	if !ok {
		result := soakVerifyResult{
			Class: soakVerifySkipped, Action: soakRPCGetMessage,
			RoomID: roomID, MessageID: messageID,
		}
		v.record(&result)
		return result
	}
	result := newSoakVerifyResult(
		soakRPCGetMessage,
		roomID,
		messageID,
		&expected,
	)
	var response soakVerifyMessage
	startedAt := v.now()
	rpcResult, err := v.rpc.Call(ctx, soakRPCRequest{
		Action: soakRPCGetMessage,
		Subject: subject.MsgGet(
			expected.Author,
			roomID,
			v.cfg.SiteID,
		),
		Body:      soakGetMessageByIDRequest{MessageID: messageID},
		Timeout:   v.cfg.RequestTimeout,
		RetryMode: soakRetrySafe,
	}, &response)
	result.Latency = v.now().Sub(startedAt)
	result.Retries = rpcResult.Retries
	if err != nil {
		classifySoakVerifyRPCError(&result, rpcResult.ErrorClass)
		v.record(&result)
		return result
	}
	compareSoakVerifiedMessage(&result, &expected, &response)
	v.record(&result)
	return result
}

func (v *soakVerifier) VerifyHistory(
	ctx context.Context,
	roomID string,
	messageID string,
) soakVerifyResult {
	expected, ok := v.catalog.GetVerificationCandidate(roomID, messageID)
	if !ok {
		result := soakVerifyResult{
			Class: soakVerifySkipped, Action: soakRPCLoadHistory,
			RoomID: roomID, MessageID: messageID,
		}
		v.record(&result)
		return result
	}
	result := newSoakVerifyResult(
		soakRPCLoadHistory,
		roomID,
		messageID,
		&expected,
	)
	beforeMillis := expected.CreatedAt.UTC().UnixMilli() + 1
	before := &beforeMillis
	lastMsgAt := v.now().UTC().UnixMilli()
	meta := &soakRoomMeta{LastMsgAt: &lastMsgAt}
	var totalLatency time.Duration
	for range v.cfg.MaxPages {
		var response soakVerifyHistoryResponse
		startedAt := v.now()
		rpcResult, err := v.rpc.Call(ctx, soakRPCRequest{
			Action: soakRPCLoadHistory,
			Subject: subject.MsgHistory(
				expected.Author,
				roomID,
				v.cfg.SiteID,
			),
			Body: soakLoadHistoryRequest{
				Before: before,
				Limit:  v.cfg.PageLimit,
				Meta:   meta,
			},
			Timeout: v.cfg.RequestTimeout, RetryMode: soakRetrySafe,
		}, &response)
		totalLatency += v.now().Sub(startedAt)
		result.Retries += rpcResult.Retries
		if err != nil {
			result.Latency = totalLatency
			classifySoakVerifyRPCError(&result, rpcResult.ErrorClass)
			v.record(&result)
			return result
		}
		result.Pages++
		for i := range response.Messages {
			if response.Messages[i].MessageID != messageID {
				continue
			}
			result.Latency = totalLatency
			compareSoakVerifiedMessage(
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
		oldest := oldestSoakVerifyMillis(response.Messages)
		if before != nil && oldest >= *before {
			result.Class = soakVerifyMismatch
			result.Field = "pagination"
			result.Latency = totalLatency
			v.record(&result)
			return result
		}
		nextBefore := oldest - 1
		before = &nextBefore
	}
	result.Class = soakVerifyMissing
	result.Latency = totalLatency
	v.record(&result)
	return result
}

func newSoakVerifyResult(
	action soakRPCAction,
	roomID string,
	messageID string,
	expected *soakCatalogMessage,
) soakVerifyResult {
	expectedAction := soakRPCReadBack
	switch {
	case expected.Deleted:
		expectedAction = soakRPCDelete
	case expected.Edited:
		expectedAction = soakRPCEdit
	}
	return soakVerifyResult{
		Class: soakVerifyOK, Action: action, ExpectedAction: expectedAction,
		RoomID: roomID, MessageID: messageID,
	}
}

func classifySoakVerifyRPCError(
	result *soakVerifyResult,
	class soakErrorClass,
) {
	result.RPCErrorClass = class
	switch {
	case class == soakErrorNotFound:
		result.Class = soakVerifyMissing
	case class == soakErrorRequestEncode, class == soakErrorResponseDecode:
		result.Class = soakVerifyMalformed
	case transientSoakError(class):
		result.Class = soakVerifyRetryable
	default:
		result.Class = soakVerifyRPCError
	}
}

func compareSoakVerifiedMessage(
	result *soakVerifyResult,
	expected *soakCatalogMessage,
	actual *soakVerifyMessage,
) {
	switch {
	case actual.MessageID != expected.ID:
		result.Class = soakVerifyMismatch
		result.Field = "message_id"
	case actual.RoomID != expected.RoomID:
		result.Class = soakVerifyMismatch
		result.Field = "room_id"
	case actual.Sender.Account != expected.Author:
		result.Class = soakVerifyMismatch
		result.Field = "author"
	case actual.Deleted != expected.Deleted:
		result.Class = soakVerifyMismatch
		result.Field = "deleted"
	case !expected.Deleted && actual.Msg != expected.Content:
		result.Class = soakVerifyMismatch
		result.Field = "content"
	case expected.Edited && actual.EditedAt == nil:
		result.Class = soakVerifyMismatch
		result.Field = "edited_at"
	default:
		result.Class = soakVerifyOK
		result.Field = ""
	}
}

func oldestSoakVerifyMillis(messages []soakVerifyMessage) int64 {
	oldest := messages[0].CreatedAt.UTC().UnixMilli()
	for i := 1; i < len(messages); i++ {
		createdAt := messages[i].CreatedAt.UTC().UnixMilli()
		if createdAt < oldest {
			oldest = createdAt
		}
	}
	return oldest
}

func (v *soakVerifier) record(result *soakVerifyResult) {
	if v.recorder != nil {
		v.recorder.Record(result)
	}
}
