package send

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
)

type Publisher interface {
	Publish(context.Context, string, []byte) error
}

type Kind string

const (
	KindTopLevel    Kind = "send"
	KindThreadReply Kind = "thread_reply"
)

func PickKind(rng *rand.Rand, threadShare float64) Kind {
	threadShare = min(max(threadShare, 0), 1)
	if rng.Float64() < threadShare {
		return KindThreadReply
	}
	return KindTopLevel
}

type Config struct {
	SiteID               string
	ThreadShare          float64
	ReplyTimeout         time.Duration
	NextThreadReplyLimit func() int
}

type IDs struct {
	MessageID func() string
	RequestID func() string
}

func ProductionIDs() *IDs {
	return &IDs{
		MessageID: idgen.GenerateMessageID,
		RequestID: idgen.GenerateRequestID,
	}
}

type Target struct {
	UserID               string
	Account              string
	RoomID               string
	RoomType             model.RoomType
	Recipients           []string
	RecipientSetSource   RecipientSetSource
	RecipientSetComplete bool
	RecipientRoute       RecipientExpectedRoute
}

type RecipientExpectedRoute string

const (
	RecipientRouteAny  RecipientExpectedRoute = "any"
	RecipientRouteRoom RecipientExpectedRoute = "room"
	RecipientRouteUser RecipientExpectedRoute = "user"
)

type RecipientSetSource string

const (
	RecipientSourceLegacy          RecipientSetSource = "legacy"
	RecipientSourceTopology        RecipientSetSource = "topology_subscriptions"
	RecipientSourceThreadFollowers RecipientSetSource = "catalog_thread_followers"
)

type Pending struct {
	Kind           Kind
	MessageID      string
	RequestID      string
	ThreadParentID string
	Target         Target
	Content        string
	Subject        string
	Payload        []byte
	PublishedAt    time.Time
	Deadline       time.Time
	// Tracked reports whether the failure ledger accepted this send's intent.
	Tracked bool
}

type ReplyStatus string

const (
	ReplyAccepted  ReplyStatus = "accepted"
	ReplyRejected  ReplyStatus = "rejected"
	ReplyMalformed ReplyStatus = "malformed"
	ReplyUnmatched ReplyStatus = "unmatched"
)

type ReplyResult struct {
	Status      ReplyStatus
	Kind        Kind
	RequestID   string
	MessageID   string
	Latency     time.Duration
	ErrorClass  rpc.ErrorClass
	ErrorReason rpc.ErrorReason
}

//go:generate mockgen -destination=mock_lifecycle_test.go -package=send . Lifecycle
type Lifecycle interface {
	Start(*Pending) error
	Activate(*Pending) error
	AbandonUnsent(*Pending) error
}

type Option func(*Sender)

// WithLifecycle attaches durable send accounting. onError is invoked
// when the lifecycle refuses an intent; the send still goes out, because losing
// observation is a lesser evil than stalling the traffic under observation.
func WithLifecycle(
	lifecycle Lifecycle,
	onError func(error),
) Option {
	return func(sender *Sender) {
		sender.lifecycle = lifecycle
		sender.lifecycleError = onError
	}
}

type Sender struct {
	cfg       Config
	catalog   *catalog.Catalog
	publisher Publisher
	clock     catalog.TimeProvider
	rng       *rand.Rand
	ids       *IDs
	lifecycle Lifecycle

	lifecycleError func(error)

	rngMu     sync.Mutex
	pendingMu sync.Mutex
	pending   map[string]*Pending
}

func New(
	cfg Config,
	messageCatalog *catalog.Catalog,
	publisher Publisher,
	clock catalog.TimeProvider,
	rng *rand.Rand,
	ids *IDs,
	options ...Option,
) *Sender {
	if clock == nil {
		clock = catalog.RealClock{}
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if ids == nil {
		ids = ProductionIDs()
	}
	if cfg.ReplyTimeout <= 0 {
		cfg.ReplyTimeout = 10 * time.Second
	}
	sender := &Sender{
		cfg: cfg, catalog: messageCatalog, publisher: publisher, clock: clock,
		rng: rng, ids: ids, pending: make(map[string]*Pending),
	}
	for _, option := range options {
		option(sender)
	}
	return sender
}

//nolint:gocritic // Publish snapshots the target by value into the durable pending operation.
func (s *Sender) Publish(
	ctx context.Context,
	target Target,
	content string,
) (*Pending, error) {
	if target.UserID == "" || target.Account == "" || target.RoomID == "" {
		return nil, fmt.Errorf("soak send target requires user ID, account, and room ID")
	}
	if content == "" {
		return nil, fmt.Errorf("soak send content is required")
	}

	messageID := s.ids.MessageID()
	requestID := s.ids.RequestID()
	if messageID == "" || requestID == "" {
		return nil, fmt.Errorf("soak send identity generation returned an empty ID")
	}

	kind := s.pickKind()
	threadParentID := ""
	if kind == KindThreadReply {
		parent, ok := s.catalog.PickEligible(
			target.RoomID,
			target.Account,
			catalog.ActionThreadParent,
		)
		if !ok || !s.catalog.ReserveThreadReply(target.RoomID, parent.ID) {
			kind = KindTopLevel
		} else {
			threadParentID = parent.ID
			if target.RoomType == model.RoomTypeChannel {
				target.Recipients, target.RecipientSetComplete = s.catalog.ThreadRecipientSet(
					target.RoomID,
					threadParentID,
					target.Account,
				)
				target.RecipientSetSource = RecipientSourceThreadFollowers
				target.RecipientRoute = RecipientRouteUser
			}
		}
	}

	request := model.SendMessageRequest{
		ID: messageID, Content: content, RequestID: requestID,
		ThreadParentMessageID: threadParentID,
	}
	payload, err := json.Marshal(request)
	if err != nil {
		s.releaseThreadReservation(target.RoomID, threadParentID)
		return nil, fmt.Errorf("marshal soak send: %w", err)
	}
	now := s.clock.Now()
	pending := &Pending{
		Kind: kind, MessageID: messageID, RequestID: requestID,
		ThreadParentID: threadParentID, Target: target, Content: content,
		Subject: subject.MsgSend(target.Account, target.RoomID, s.cfg.SiteID),
		Payload: payload, PublishedAt: now, Deadline: now.Add(s.cfg.ReplyTimeout),
	}
	candidate := catalog.Candidate{
		ID: messageID, RoomID: target.RoomID, Author: target.Account,
		Content: content, CreatedAt: now, ThreadParentID: threadParentID,
	}
	if kind == KindTopLevel && s.cfg.NextThreadReplyLimit != nil {
		s.rngMu.Lock()
		candidate.ThreadReplyLimit = s.cfg.NextThreadReplyLimit()
		s.rngMu.Unlock()
	}
	if err := s.catalog.TrackPublished(&candidate); err != nil {
		s.releaseThreadReservation(target.RoomID, threadParentID)
		return nil, fmt.Errorf("track soak send before publish: %w", err)
	}
	if err := s.addPending(pending); err != nil {
		s.rejectPending(pending)
		return nil, err
	}
	tracked := false
	if s.lifecycle != nil {
		if err := s.lifecycle.Start(ClonePending(pending)); err != nil {
			s.reportLifecycleError(fmt.Errorf("persist soak send intent: %w", err))
		} else {
			s.markPendingTracked(pending.RequestID)
			tracked = true
		}
	}

	published := s.markPendingDispatched(pending.RequestID, s.clock.Now())
	if published == nil {
		published = ClonePending(pending)
	}
	publishErr := s.publisher.Publish(ctx, pending.Subject, pending.Payload)
	definitelyNotSent := publishErr != nil && PublishDefinitelyNotSent(publishErr)
	if tracked {
		if definitelyNotSent {
			if err := s.lifecycle.AbandonUnsent(ClonePending(published)); err != nil {
				s.reportLifecycleError(fmt.Errorf("persist unsent soak send: %w", err))
			}
		} else if err := s.lifecycle.Activate(ClonePending(published)); err != nil {
			s.reportLifecycleError(fmt.Errorf("persist soak send activation: %w", err))
		}
	}
	if definitelyNotSent {
		s.Discard(pending.RequestID)
	}
	if publishErr != nil {
		return published, fmt.Errorf("publish soak send: %w", publishErr)
	}
	return published, nil
}

func PublishDefinitelyNotSent(err error) bool {
	return errors.Is(err, nats.ErrConnectionClosed) ||
		errors.Is(err, nats.ErrConnectionDraining) ||
		errors.Is(err, nats.ErrBadSubject) ||
		errors.Is(err, nats.ErrMaxPayload) ||
		errors.Is(err, nats.ErrReconnectBufExceeded)
}

func (s *Sender) Retry(ctx context.Context, requestID string) error {
	s.pendingMu.Lock()
	pending := s.pending[requestID]
	if pending != nil {
		pending = ClonePending(pending)
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

func lastToken(subjectName string) string {
	index := strings.LastIndex(subjectName, ".")
	if index < 0 {
		return subjectName
	}
	return subjectName[index+1:]
}

func (s *Sender) HandleReply(replySubject string, data []byte) ReplyResult {
	requestID := lastToken(replySubject)
	pending := s.takePending(requestID, replySubject)
	if pending == nil {
		return ReplyResult{Status: ReplyUnmatched, RequestID: requestID}
	}
	result := ReplyResult{
		Status: ReplyRejected, Kind: pending.Kind, RequestID: requestID,
		MessageID: pending.MessageID, Latency: s.clock.Now().Sub(pending.PublishedAt),
	}

	if responseErr := rpc.ParseErrorEnvelope(data); responseErr != nil {
		s.rejectPending(pending)
		result.ErrorClass = rpc.ClassifyError(responseErr)
		result.ErrorReason = rpc.ClassifyReason(responseErr)
		return result
	}
	var response model.Message
	if err := json.Unmarshal(data, &response); err != nil {
		s.rejectPending(pending)
		result.Status = ReplyMalformed
		result.ErrorClass = rpc.ErrorResponseDecode
		return result
	}
	if !matchingReply(pending, &response) {
		s.rejectPending(pending)
		result.ErrorClass = rpc.ErrorAssertion
		return result
	}
	if !s.catalog.AcceptAt(
		pending.Target.RoomID,
		pending.MessageID,
		response.CreatedAt,
	) {
		s.rejectPending(pending)
		result.ErrorClass = rpc.ErrorAssertion
		return result
	}
	if pending.ThreadParentID != "" {
		// Publishing reserved parent capacity before sending. Only the accepted
		// response converts that reservation into a confirmed reply; reads then
		// wait the catalog's persistence grace before using the thread.
		s.catalog.ConfirmThreadReply(pending.Target.RoomID, pending.ThreadParentID)
	}
	result.Status = ReplyAccepted
	return result
}

func (s *Sender) Expire() int {
	return len(s.ExpireResults())
}

func (s *Sender) ExpireResults() []ReplyResult {
	now := s.clock.Now()
	expired := make([]*Pending, 0)
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
		s.rejectPending(pending)
	}
	results := make([]ReplyResult, 0, len(expired))
	for _, pending := range expired {
		results = append(results, ReplyResult{
			Status: ReplyRejected, Kind: pending.Kind,
			RequestID: pending.RequestID, MessageID: pending.MessageID,
			Latency: now.Sub(pending.PublishedAt), ErrorClass: rpc.ErrorTimeout,
		})
	}
	return results
}

func (s *Sender) rejectPending(pending *Pending) {
	s.catalog.Reject(pending.Target.RoomID, pending.MessageID)
	s.releaseThreadReservation(
		pending.Target.RoomID,
		pending.ThreadParentID,
	)
}

func (s *Sender) releaseThreadReservation(
	roomID string,
	parentID string,
) {
	if parentID != "" {
		s.catalog.ReleaseThreadReplyReservation(roomID, parentID)
	}
}

func (s *Sender) Pending() int {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	return len(s.pending)
}

func (s *Sender) pickKind() Kind {
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	return PickKind(s.rng, s.cfg.ThreadShare)
}

func (s *Sender) addPending(pending *Pending) error {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if _, exists := s.pending[pending.RequestID]; exists {
		return fmt.Errorf("soak send request %q is already pending", pending.RequestID)
	}
	s.pending[pending.RequestID] = pending
	return nil
}

func (s *Sender) takePending(
	requestID string,
	replySubject string,
) *Pending {
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

// Discard drops a pending send that never reached NATS. Without it the send is
// counted once for the publish error and again when its reply deadline passes.
func (s *Sender) Discard(requestID string) {
	s.pendingMu.Lock()
	pending := s.pending[requestID]
	delete(s.pending, requestID)
	s.pendingMu.Unlock()
	if pending == nil {
		return
	}
	s.rejectPending(pending)
}

func (s *Sender) markPendingTracked(requestID string) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if pending := s.pending[requestID]; pending != nil {
		pending.Tracked = true
	}
}

// markPendingDispatched restarts the reply clock immediately before the publish
// leaves the process. Journaling the send intent is durable and waits on a WAL
// group commit, so measuring from the intent timestamp would report the load
// generator's own flush delay as server latency. The ledger operation keeps the
// earlier intent timestamp, which stays conservative for its verify deadline.
func (s *Sender) markPendingDispatched(
	requestID string,
	at time.Time,
) *Pending {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	pending := s.pending[requestID]
	if pending == nil {
		return nil
	}
	pending.PublishedAt = at
	pending.Deadline = at.Add(s.cfg.ReplyTimeout)
	return ClonePending(pending)
}

func (s *Sender) reportLifecycleError(err error) {
	if s.lifecycleError != nil {
		s.lifecycleError(err)
	}
}

func matchingReply(
	pending *Pending,
	response *model.Message,
) bool {
	return response.ID == pending.MessageID &&
		response.RoomID == pending.Target.RoomID &&
		response.UserID == pending.Target.UserID &&
		response.UserAccount == pending.Target.Account &&
		response.Content == pending.Content &&
		response.ThreadParentMessageID == pending.ThreadParentID
}

func ClonePending(pending *Pending) *Pending {
	cloned := *pending
	cloned.Payload = append([]byte(nil), pending.Payload...)
	cloned.Target.Recipients = append([]string(nil), pending.Target.Recipients...)
	return &cloned
}

type ResponseSubscription interface {
	Unsubscribe() error
}

type ResponseSource interface {
	Subscribe(
		subject string,
		handler nats.MsgHandler,
	) (ResponseSubscription, error)
	Flush() error
}

type NATSResponseSource struct {
	nc *nats.Conn
}

var _ ResponseSource = (*NATSResponseSource)(nil)

func NewNATSResponseSource(nc *nats.Conn) *NATSResponseSource {
	return &NATSResponseSource{nc: nc}
}

func (s *NATSResponseSource) Subscribe(
	subject string,
	handler nats.MsgHandler,
) (ResponseSubscription, error) {
	subscription, err := s.nc.Subscribe(subject, handler)
	if err != nil {
		return nil, fmt.Errorf("subscribe soak send responses: %w", err)
	}
	return subscription, nil
}

func (s *NATSResponseSource) Flush() error {
	if err := s.nc.Flush(); err != nil {
		return fmt.Errorf("flush soak send response subscription: %w", err)
	}
	return nil
}

func StartResponses(
	source ResponseSource,
	sender *Sender,
) (ResponseSubscription, error) {
	return StartResponsesWithObserver(source, sender, nil)
}

func StartResponsesWithObserver(
	source ResponseSource,
	sender *Sender,
	observer func(ReplyResult),
) (ResponseSubscription, error) {
	subscription, err := source.Subscribe(
		subject.UserResponseWildcard(),
		func(message *nats.Msg) {
			result := sender.HandleReply(message.Subject, message.Data)
			if observer != nil && result.Status != ReplyUnmatched {
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
