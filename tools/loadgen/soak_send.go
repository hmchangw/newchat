package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

type soakSendKind string

const (
	soakSendTopLevel    soakSendKind = "send"
	soakSendThreadReply soakSendKind = "thread_reply"
)

func pickSoakSendKind(rng *rand.Rand, threadShare float64) soakSendKind {
	threadShare = min(max(threadShare, 0), 1)
	if rng.Float64() < threadShare {
		return soakSendThreadReply
	}
	return soakSendTopLevel
}

type soakSendConfig struct {
	SiteID               string
	ThreadShare          float64
	ReplyTimeout         time.Duration
	NextThreadReplyLimit func() int
}

type soakSendIDs struct {
	messageID func() string
	requestID func() string
}

func newProductionSoakSendIDs() *soakSendIDs {
	return &soakSendIDs{
		messageID: idgen.GenerateMessageID,
		requestID: idgen.GenerateRequestID,
	}
}

type soakSendTarget struct {
	UserID  string
	Account string
	RoomID  string
}

type soakPendingSend struct {
	Kind           soakSendKind
	MessageID      string
	RequestID      string
	ThreadParentID string
	Target         soakSendTarget
	Content        string
	Subject        string
	Payload        []byte
	PublishedAt    time.Time
	Deadline       time.Time
}

type soakSendReplyStatus string

const (
	soakSendReplyAccepted  soakSendReplyStatus = "accepted"
	soakSendReplyRejected  soakSendReplyStatus = "rejected"
	soakSendReplyMalformed soakSendReplyStatus = "malformed"
	soakSendReplyUnmatched soakSendReplyStatus = "unmatched"
)

type soakSendReplyResult struct {
	Status     soakSendReplyStatus
	Kind       soakSendKind
	RequestID  string
	MessageID  string
	Latency    time.Duration
	ErrorClass soakErrorClass
}

type soakSender struct {
	cfg       soakSendConfig
	catalog   *soakCatalog
	publisher Publisher
	clock     soakClock
	rng       *rand.Rand
	ids       *soakSendIDs

	rngMu     sync.Mutex
	pendingMu sync.Mutex
	pending   map[string]*soakPendingSend
}

func newSoakSender(
	cfg soakSendConfig,
	catalog *soakCatalog,
	publisher Publisher,
	clock soakClock,
	rng *rand.Rand,
	ids *soakSendIDs,
) *soakSender {
	if clock == nil {
		clock = soakRealClock{}
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if ids == nil {
		ids = newProductionSoakSendIDs()
	}
	if cfg.ReplyTimeout <= 0 {
		cfg.ReplyTimeout = 10 * time.Second
	}
	return &soakSender{
		cfg: cfg, catalog: catalog, publisher: publisher, clock: clock,
		rng: rng, ids: ids, pending: make(map[string]*soakPendingSend),
	}
}

func (s *soakSender) Publish(
	ctx context.Context,
	target soakSendTarget,
	content string,
) (*soakPendingSend, error) {
	if target.UserID == "" || target.Account == "" || target.RoomID == "" {
		return nil, fmt.Errorf("soak send target requires user ID, account, and room ID")
	}
	if content == "" {
		return nil, fmt.Errorf("soak send content is required")
	}

	messageID := s.ids.messageID()
	requestID := s.ids.requestID()
	if messageID == "" || requestID == "" {
		return nil, fmt.Errorf("soak send identity generation returned an empty ID")
	}

	kind := s.pickKind()
	threadParentID := ""
	if kind == soakSendThreadReply {
		parent, ok := s.catalog.PickEligible(
			target.RoomID,
			target.Account,
			soakCatalogThreadParent,
		)
		if !ok || !s.catalog.IncrementThreadReplies(target.RoomID, parent.ID) {
			kind = soakSendTopLevel
		} else {
			threadParentID = parent.ID
		}
	}

	request := model.SendMessageRequest{
		ID: messageID, Content: content, RequestID: requestID,
		ThreadParentMessageID: threadParentID,
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal soak send: %w", err)
	}
	now := s.clock.Now()
	pending := &soakPendingSend{
		Kind: kind, MessageID: messageID, RequestID: requestID,
		ThreadParentID: threadParentID, Target: target, Content: content,
		Subject: subject.MsgSend(target.Account, target.RoomID, s.cfg.SiteID),
		Payload: payload, PublishedAt: now, Deadline: now.Add(s.cfg.ReplyTimeout),
	}
	candidate := soakCatalogCandidate{
		ID: messageID, RoomID: target.RoomID, Author: target.Account,
		Content: content, CreatedAt: now, ThreadParentID: threadParentID,
	}
	if kind == soakSendTopLevel && s.cfg.NextThreadReplyLimit != nil {
		s.rngMu.Lock()
		candidate.ThreadReplyLimit = s.cfg.NextThreadReplyLimit()
		s.rngMu.Unlock()
	}
	if err := s.catalog.TrackPublished(&candidate); err != nil {
		return nil, fmt.Errorf("track soak send before publish: %w", err)
	}
	if err := s.addPending(pending); err != nil {
		s.catalog.Reject(target.RoomID, messageID)
		return nil, err
	}

	if err := s.publisher.Publish(ctx, pending.Subject, pending.Payload); err != nil {
		return cloneSoakPendingSend(pending), fmt.Errorf("publish soak send: %w", err)
	}
	return cloneSoakPendingSend(pending), nil
}

func (s *soakSender) Retry(ctx context.Context, requestID string) error {
	s.pendingMu.Lock()
	pending := s.pending[requestID]
	if pending != nil {
		pending = cloneSoakPendingSend(pending)
	}
	s.pendingMu.Unlock()
	if pending == nil {
		return fmt.Errorf("retry soak send %q: pending request not found", requestID)
	}
	if err := s.publisher.Publish(ctx, pending.Subject, pending.Payload); err != nil {
		return fmt.Errorf("retry soak send publish: %w", err)
	}
	return nil
}

func (s *soakSender) HandleReply(replySubject string, data []byte) soakSendReplyResult {
	requestID := lastToken(replySubject)
	pending := s.takePending(requestID, replySubject)
	if pending == nil {
		return soakSendReplyResult{Status: soakSendReplyUnmatched, RequestID: requestID}
	}
	result := soakSendReplyResult{
		Status: soakSendReplyRejected, Kind: pending.Kind, RequestID: requestID,
		MessageID: pending.MessageID, Latency: s.clock.Now().Sub(pending.PublishedAt),
	}

	if responseErr := parseSoakErrorEnvelope(data); responseErr != nil {
		s.catalog.Reject(pending.Target.RoomID, pending.MessageID)
		result.ErrorClass = classifySoakRPCError(responseErr)
		return result
	}
	var response model.Message
	if err := json.Unmarshal(data, &response); err != nil {
		s.catalog.Reject(pending.Target.RoomID, pending.MessageID)
		result.Status = soakSendReplyMalformed
		result.ErrorClass = soakErrorDecode
		return result
	}
	if !matchingSoakSendReply(pending, &response) {
		s.catalog.Reject(pending.Target.RoomID, pending.MessageID)
		result.ErrorClass = soakErrorAssertion
		return result
	}
	if !s.catalog.Accept(pending.Target.RoomID, pending.MessageID) {
		result.ErrorClass = soakErrorAssertion
		return result
	}
	result.Status = soakSendReplyAccepted
	return result
}

func (s *soakSender) Expire() int {
	now := s.clock.Now()
	expired := make([]*soakPendingSend, 0)
	s.pendingMu.Lock()
	for requestID, pending := range s.pending {
		if now.Before(pending.Deadline) {
			continue
		}
		expired = append(expired, pending)
		delete(s.pending, requestID)
	}
	s.pendingMu.Unlock()
	for _, pending := range expired {
		s.catalog.Reject(pending.Target.RoomID, pending.MessageID)
	}
	return len(expired)
}

func (s *soakSender) Pending() int {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	return len(s.pending)
}

func (s *soakSender) pickKind() soakSendKind {
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	return pickSoakSendKind(s.rng, s.cfg.ThreadShare)
}

func (s *soakSender) addPending(pending *soakPendingSend) error {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if _, exists := s.pending[pending.RequestID]; exists {
		return fmt.Errorf("soak send request %q is already pending", pending.RequestID)
	}
	s.pending[pending.RequestID] = pending
	return nil
}

func (s *soakSender) takePending(
	requestID string,
	replySubject string,
) *soakPendingSend {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	pending := s.pending[requestID]
	if pending == nil ||
		replySubject != subject.UserResponse(pending.Target.Account, requestID) {
		return nil
	}
	delete(s.pending, requestID)
	return pending
}

func matchingSoakSendReply(
	pending *soakPendingSend,
	response *model.Message,
) bool {
	return response.ID == pending.MessageID &&
		response.RoomID == pending.Target.RoomID &&
		response.UserID == pending.Target.UserID &&
		response.UserAccount == pending.Target.Account &&
		response.Content == pending.Content &&
		response.ThreadParentMessageID == pending.ThreadParentID
}

func cloneSoakPendingSend(pending *soakPendingSend) *soakPendingSend {
	cloned := *pending
	cloned.Payload = append([]byte(nil), pending.Payload...)
	return &cloned
}

type soakResponseSubscription interface {
	Unsubscribe() error
}

type soakResponseSource interface {
	Subscribe(
		subject string,
		handler nats.MsgHandler,
	) (soakResponseSubscription, error)
	Flush() error
}

type natsSoakResponseSource struct {
	nc *nats.Conn
}

var _ soakResponseSource = (*natsSoakResponseSource)(nil)

func newNATSSoakResponseSource(nc *nats.Conn) *natsSoakResponseSource {
	return &natsSoakResponseSource{nc: nc}
}

func (s *natsSoakResponseSource) Subscribe(
	subject string,
	handler nats.MsgHandler,
) (soakResponseSubscription, error) {
	subscription, err := s.nc.Subscribe(subject, handler)
	if err != nil {
		return nil, fmt.Errorf("subscribe soak send responses: %w", err)
	}
	return subscription, nil
}

func (s *natsSoakResponseSource) Flush() error {
	if err := s.nc.Flush(); err != nil {
		return fmt.Errorf("flush soak send response subscription: %w", err)
	}
	return nil
}

func startSoakSendResponses(
	source soakResponseSource,
	sender *soakSender,
) (soakResponseSubscription, error) {
	return startSoakSendResponsesWithObserver(source, sender, nil)
}

func startSoakSendResponsesWithObserver(
	source soakResponseSource,
	sender *soakSender,
	observer func(soakSendReplyResult),
) (soakResponseSubscription, error) {
	subscription, err := source.Subscribe(
		subject.UserResponseWildcard(),
		func(message *nats.Msg) {
			result := sender.HandleReply(message.Subject, message.Data)
			if observer != nil && result.Status != soakSendReplyUnmatched {
				observer(result)
			}
		},
	)
	if err != nil {
		return nil, fmt.Errorf("start soak send responses: %w", err)
	}
	if err := source.Flush(); err != nil {
		if subscription != nil {
			_ = subscription.Unsubscribe()
		}
		return nil, fmt.Errorf("start soak send responses: %w", err)
	}
	return subscription, nil
}
