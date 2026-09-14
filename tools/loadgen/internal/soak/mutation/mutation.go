package mutation

import (
	"context"
	"fmt"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/wire"
)

const ReactionShortcode = "thumbsup"

type Kind string

const (
	KindEdit      Kind = "edit"
	KindDelete    Kind = "delete"
	KindPinFamily Kind = "pin_family"
)

type Scheduler struct {
	softDeleteRatio float64
	rng             *rand.Rand
	rngMu           sync.Mutex
	pendingDeletes  atomic.Int64
}

func NewScheduler(
	softDeleteRatio float64,
	rng *rand.Rand,
) *Scheduler {
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Scheduler{
		softDeleteRatio: min(max(softDeleteRatio, 0), 1),
		rng:             rng,
	}
}

func (s *Scheduler) ObserveAcceptedSend() {
	s.rngMu.Lock()
	scheduled := s.rng.Float64() < s.softDeleteRatio
	s.rngMu.Unlock()
	if scheduled {
		s.pendingDeletes.Add(1)
	}
}

func (s *Scheduler) Next() Kind {
	for {
		pending := s.pendingDeletes.Load()
		if pending == 0 {
			break
		}
		if s.pendingDeletes.CompareAndSwap(pending, pending-1) {
			return KindDelete
		}
	}
	s.rngMu.Lock()
	edit := s.rng.Intn(2) == 0
	s.rngMu.Unlock()
	if edit {
		return KindEdit
	}
	return KindPinFamily
}

type Config struct {
	SiteID                 string
	MutationRetries        int
	RetryMinBackoff        time.Duration
	RetryMaxBackoff        time.Duration
	MaxPinnedPerRoom       int
	ReactionsPerHotMessage int
	ReactionRemoveShare    float64
	ReactionMessageScope   string
	RequestTimeout         time.Duration
}

type Sample struct {
	Action        rpc.Action
	Latency       time.Duration
	ErrorClass    rpc.ErrorClass
	ErrorReason   rpc.ErrorReason
	Retries       int
	Skipped       bool
	TargetMissing bool
}

type SampleRecorder interface {
	Record(Sample)
}

type Outcome struct {
	Action            rpc.Action
	MessageID         string
	Retries           int
	Skipped           bool
	TargetMissing     bool
	ReactionAction    model.ReactionAction
	AmbiguityResolved bool
}

type reactionStateMessage struct {
	RoomID    string `json:"roomId"`
	MessageID string `json:"messageId"`
	Reactions map[string][]struct {
		Account string `json:"account"`
	} `json:"reactions"`
}

type Mutator struct {
	cfg      Config
	catalog  *catalog.Catalog
	rpc      *rpc.Client
	recorder SampleRecorder
	rng      *rand.Rand
	clock    catalog.TimeProvider
	sleeper  rpc.Sleeper

	rngMu      sync.Mutex
	members    map[string][]model.SubscriptionUser
	hotMu      sync.Mutex
	hotTargets map[string]string
}

func New(
	cfg *Config,
	roomTopology *topology.Topology,
	messageCatalog *catalog.Catalog,
	rpcClient *rpc.Client,
	recorder SampleRecorder,
	rng *rand.Rand,
	clock catalog.TimeProvider,
	sleeper rpc.Sleeper,
) *Mutator {
	if cfg == nil {
		cfg = &Config{}
	}
	config := *cfg
	if config.MutationRetries < 0 {
		config.MutationRetries = 0
	}
	if config.RetryMinBackoff <= 0 {
		config.RetryMinBackoff = 100 * time.Millisecond
	}
	if config.RetryMaxBackoff < config.RetryMinBackoff {
		config.RetryMaxBackoff = config.RetryMinBackoff
	}
	if config.MaxPinnedPerRoom <= 0 {
		config.MaxPinnedPerRoom = 10
	}
	if config.ReactionsPerHotMessage <= 0 {
		config.ReactionsPerHotMessage = 1
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 5 * time.Second
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if clock == nil {
		clock = catalog.RealClock{}
	}
	if sleeper == nil {
		sleeper = rpc.TimerSleeper{}
	}
	members := make(map[string][]model.SubscriptionUser)
	if roomTopology != nil {
		active := topology.ActiveUserIDs(roomTopology)
		for i := range roomTopology.Subscriptions {
			subscription := &roomTopology.Subscriptions[i]
			if topology.IsActiveSubscription(subscription, active) &&
				subscription.RoomID != "" &&
				subscription.User.Account != "" {
				members[subscription.RoomID] = append(
					members[subscription.RoomID],
					subscription.User,
				)
			}
		}
	}
	return &Mutator{
		cfg: config, catalog: messageCatalog, rpc: rpcClient, recorder: recorder,
		rng: rng, clock: clock, sleeper: sleeper, members: members,
		hotTargets: make(map[string]string),
	}
}

func (m *Mutator) Edit(
	ctx context.Context,
	roomID string,
	content string,
) (Outcome, error) {
	outcome := Outcome{Action: rpc.ActionEdit}
	message, ok := m.catalog.PickAnyEligible(roomID, catalog.ActionEdit)
	if !ok {
		return m.skip(outcome), nil
	}
	outcome.MessageID = message.ID
	var response wire.EditMessageResponse
	result, retries, latency, targetMissing, err := m.callMutation(
		ctx,
		rpc.Request{
			Action: rpc.ActionEdit,
			Subject: subject.MsgEdit(
				message.Author,
				roomID,
				m.cfg.SiteID,
			),
			Account: message.Author, RoomID: roomID,
			Body: wire.EditMessageRequest{
				MessageID: message.ID,
				NewMsg:    content,
			},
			Timeout: m.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
		},
		&response,
	)
	outcome.Retries = retries
	if targetMissing {
		return m.missing(outcome, latency), nil
	}
	if err != nil {
		m.recordResult(outcome, latency, result.ErrorClass, result.ErrorReason, false)
		return outcome, err
	}
	if response.MessageID != message.ID {
		err = rpc.NewAssertionError("edit returned a different message ID")
		m.recordResult(outcome, latency, rpc.ErrorAssertion, "", false)
		return outcome, err
	}
	m.catalog.MarkEdited(roomID, message.ID, content)
	m.recordResult(outcome, latency, "", "", false)
	return outcome, nil
}

func (m *Mutator) Delete(
	ctx context.Context,
	roomID string,
) (Outcome, error) {
	outcome := Outcome{Action: rpc.ActionDelete}
	message, ok := m.catalog.PickAnyEligible(roomID, catalog.ActionDelete)
	if !ok {
		return m.skip(outcome), nil
	}
	outcome.MessageID = message.ID
	var response wire.DeleteMessageResponse
	result, retries, latency, targetMissing, err := m.callMutation(
		ctx,
		rpc.Request{
			Action: rpc.ActionDelete,
			Subject: subject.MsgDelete(
				message.Author,
				roomID,
				m.cfg.SiteID,
			),
			Account: message.Author, RoomID: roomID,
			Body:    wire.DeleteMessageRequest{MessageID: message.ID},
			Timeout: m.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
		},
		&response,
	)
	outcome.Retries = retries
	if targetMissing {
		return m.missing(outcome, latency), nil
	}
	if err != nil {
		m.recordResult(outcome, latency, result.ErrorClass, result.ErrorReason, false)
		return outcome, err
	}
	if response.MessageID != message.ID {
		err = rpc.NewAssertionError("delete returned a different message ID")
		m.recordResult(outcome, latency, rpc.ErrorAssertion, "", false)
		return outcome, err
	}
	m.catalog.MarkDeleted(roomID, message.ID)
	m.recordResult(outcome, latency, "", "", false)
	return outcome, nil
}

func (m *Mutator) PinOrUnpin(
	ctx context.Context,
	roomID string,
) (Outcome, error) {
	pinnedCount := m.catalog.PinnedCount(roomID)
	var (
		message catalog.Message
		ok      bool
		pin     bool
	)
	if pinnedCount >= m.cfg.MaxPinnedPerRoom {
		message, ok = m.catalog.PickPinCandidate(roomID, true)
	} else {
		message, ok = m.catalog.PickPinCandidate(roomID, false)
		pin = ok
		if !ok {
			message, ok = m.catalog.PickPinCandidate(roomID, true)
		}
	}
	action := rpc.ActionUnpin
	if pin {
		action = rpc.ActionPin
	}
	outcome := Outcome{Action: action}
	if !ok {
		return m.skip(outcome), nil
	}
	outcome.MessageID = message.ID

	if pin {
		return m.pin(ctx, roomID, &message, outcome)
	}
	return m.unpin(ctx, roomID, &message, outcome)
}

func (m *Mutator) pin(
	ctx context.Context,
	roomID string,
	message *catalog.Message,
	outcome Outcome,
) (Outcome, error) {
	var response wire.PinMessageResponse
	result, retries, latency, targetMissing, err := m.callMutation(
		ctx,
		rpc.Request{
			Action: rpc.ActionPin,
			Subject: subject.MsgPin(
				message.Author,
				roomID,
				m.cfg.SiteID,
			),
			Account: message.Author, RoomID: roomID,
			Body:    wire.PinMessageRequest{MessageID: message.ID},
			Timeout: m.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
		},
		&response,
	)
	outcome.Retries = retries
	if targetMissing {
		return m.missing(outcome, latency), nil
	}
	if err != nil {
		m.recordResult(outcome, latency, result.ErrorClass, result.ErrorReason, false)
		return outcome, err
	}
	if response.MessageID != message.ID {
		m.recordResult(outcome, latency, rpc.ErrorAssertion, "", false)
		return outcome, rpc.NewAssertionError("pin returned a different message ID")
	}
	m.catalog.SetPinned(roomID, message.ID, true)
	m.recordResult(outcome, latency, "", "", false)
	return outcome, nil
}

func (m *Mutator) unpin(
	ctx context.Context,
	roomID string,
	message *catalog.Message,
	outcome Outcome,
) (Outcome, error) {
	var response wire.UnpinMessageResponse
	result, retries, latency, targetMissing, err := m.callMutation(
		ctx,
		rpc.Request{
			Action: rpc.ActionUnpin,
			Subject: subject.MsgUnpin(
				message.Author,
				roomID,
				m.cfg.SiteID,
			),
			Account: message.Author, RoomID: roomID,
			Body:    wire.UnpinMessageRequest{MessageID: message.ID},
			Timeout: m.cfg.RequestTimeout, RetryMode: rpc.RetrySafe,
		},
		&response,
	)
	outcome.Retries = retries
	if targetMissing {
		return m.missing(outcome, latency), nil
	}
	if err != nil {
		m.recordResult(outcome, latency, result.ErrorClass, result.ErrorReason, false)
		return outcome, err
	}
	if response.MessageID != message.ID {
		m.recordResult(outcome, latency, rpc.ErrorAssertion, "", false)
		return outcome, rpc.NewAssertionError("unpin returned a different message ID")
	}
	m.catalog.SetPinned(roomID, message.ID, false)
	m.recordResult(outcome, latency, "", "", false)
	return outcome, nil
}

func (m *Mutator) React(
	ctx context.Context,
	roomID string,
) (Outcome, error) {
	outcome := Outcome{Action: rpc.ActionReact}
	message, ok := m.reactionTarget(roomID)
	if !ok {
		return m.skip(outcome), nil
	}
	outcome.MessageID = message.ID
	actor, desired, ok := m.reactionActor(roomID, &message)
	if !ok {
		return m.skip(outcome), nil
	}
	outcome.ReactionAction = desired

	var rpcLatency time.Duration
	request := rpc.Request{
		Action:  rpc.ActionReact,
		Subject: subject.MsgReact(actor.Account, roomID, m.cfg.SiteID),
		Account: actor.Account, RoomID: roomID,
		Body: wire.ReactMessageRequest{
			MessageID: message.ID,
			Shortcode: ReactionShortcode,
		},
		Timeout:   m.cfg.RequestTimeout,
		RetryMode: rpc.RetryAmbiguous,
		ResolveAmbiguity: func(resolveCtx context.Context) (bool, error) {
			actual, resolveErr := m.readReactionState(
				resolveCtx,
				actor.Account,
				roomID,
				message.ID,
			)
			if resolveErr != nil {
				return false, resolveErr
			}
			wantPresent := desired == model.ReactionActionAdded
			return actual != wantPresent, nil
		},
	}
	var (
		response      wire.ReactMessageResponse
		result        rpc.Result
		err           error
		targetRetries int
	)
	for {
		response = wire.ReactMessageResponse{}
		attemptStartedAt := m.clock.Now()
		result, err = m.rpc.Call(ctx, request, &response)
		rpcLatency += m.clock.Now().Sub(attemptStartedAt)
		if err == nil || result.ErrorClass != rpc.ErrorNotFound {
			break
		}
		if targetRetries >= m.cfg.MutationRetries {
			break
		}
		if sleepErr := m.sleeper.Sleep(
			ctx,
			m.mutationBackoff(targetRetries),
		); sleepErr != nil {
			err = fmt.Errorf("wait to retry reaction target: %w", sleepErr)
			break
		}
		targetRetries++
	}
	latency := rpcLatency
	outcome.Retries = targetRetries + result.Retries
	outcome.AmbiguityResolved = result.AmbiguityResolved
	if err != nil && result.ErrorClass == rpc.ErrorNotFound &&
		targetRetries >= m.cfg.MutationRetries {
		return m.missing(outcome, latency), nil
	}
	if err != nil {
		m.recordResult(outcome, latency, result.ErrorClass, result.ErrorReason, false)
		return outcome, err
	}
	if !result.AmbiguityResolved &&
		(response.MessageID != message.ID ||
			response.Shortcode != ReactionShortcode ||
			(response.Action != model.ReactionActionAdded &&
				response.Action != model.ReactionActionRemoved)) {
		m.recordResult(outcome, latency, rpc.ErrorAssertion, "", false)
		return outcome, rpc.NewAssertionError(
			"reaction response did not identify a valid state transition",
		)
	}
	actual := desired
	if !result.AmbiguityResolved {
		actual = response.Action
		outcome.ReactionAction = actual
	}
	m.catalog.SetReaction(
		roomID,
		message.ID,
		ReactionShortcode,
		actor.Account,
		actual == model.ReactionActionAdded,
	)
	m.recordResult(outcome, latency, "", "", false)
	return outcome, nil
}

func (m *Mutator) reactionTarget(
	roomID string,
) (catalog.Message, bool) {
	if m.cfg.ReactionMessageScope == "hot_only" {
		m.hotMu.Lock()
		messageID := m.hotTargets[roomID]
		m.hotMu.Unlock()
		if messageID != "" {
			if message, ok := m.catalog.GetEligible(
				roomID,
				messageID,
				catalog.ActionReaction,
			); ok {
				return message, true
			}
		}
	}
	message, ok := m.catalog.PickAnyEligible(roomID, catalog.ActionReaction)
	if ok && m.cfg.ReactionMessageScope == "hot_only" {
		m.hotMu.Lock()
		m.hotTargets[roomID] = message.ID
		m.hotMu.Unlock()
	}
	return message, ok
}

func (m *Mutator) reactionActor(
	roomID string,
	message *catalog.Message,
) (model.SubscriptionUser, model.ReactionAction, bool) {
	members := m.members[roomID]
	if len(members) == 0 {
		return model.SubscriptionUser{}, "", false
	}
	existing := message.Reactions[ReactionShortcode]
	maxWidth := min(m.cfg.ReactionsPerHotMessage, len(members))
	remove := len(existing) >= maxWidth
	if !remove && len(existing) > 0 {
		m.rngMu.Lock()
		remove = m.rng.Float64() < m.cfg.ReactionRemoveShare
		m.rngMu.Unlock()
	}
	if remove && len(existing) > 0 {
		m.rngMu.Lock()
		account := existing[m.rng.Intn(len(existing))]
		m.rngMu.Unlock()
		for _, member := range members {
			if member.Account == account {
				return member, model.ReactionActionRemoved, true
			}
		}
	}

	reacted := make(map[string]struct{}, len(existing))
	for _, account := range existing {
		reacted[account] = struct{}{}
	}
	candidates := make([]model.SubscriptionUser, 0, len(members))
	for _, member := range members {
		if _, exists := reacted[member.Account]; !exists {
			candidates = append(candidates, member)
		}
	}
	if len(candidates) == 0 {
		return model.SubscriptionUser{}, "", false
	}
	m.rngMu.Lock()
	actor := candidates[m.rng.Intn(len(candidates))]
	m.rngMu.Unlock()
	return actor, model.ReactionActionAdded, true
}

func (m *Mutator) readReactionState(
	ctx context.Context,
	account string,
	roomID string,
	messageID string,
) (bool, error) {
	var state reactionStateMessage
	_, err := m.rpc.Call(ctx, rpc.Request{
		Action: rpc.ActionGetMessage,
		Subject: subject.MsgGet(
			account,
			roomID,
			m.cfg.SiteID,
		),
		Account: account, RoomID: roomID,
		Body:      wire.GetMessageByIDRequest{MessageID: messageID},
		Timeout:   m.cfg.RequestTimeout,
		RetryMode: rpc.RetrySafe,
	}, &state)
	if err != nil {
		return false, fmt.Errorf("read reaction state: %w", err)
	}
	if state.MessageID != messageID || state.RoomID != roomID {
		return false, rpc.NewAssertionError(
			"reaction state read returned a different message",
		)
	}
	for _, reactor := range state.Reactions[ReactionShortcode] {
		if reactor.Account == account {
			return true, nil
		}
	}
	return false, nil
}

//nolint:gocritic // hugeParam: the request carries the failure identity; the copy is nothing beside the round trip.
func (m *Mutator) callMutation(
	ctx context.Context,
	request rpc.Request,
	response any,
) (rpc.Result, int, time.Duration, bool, error) {
	var result rpc.Result
	var rpcLatency time.Duration
	for attempt := 0; attempt <= m.cfg.MutationRetries; attempt++ {
		attemptStartedAt := m.clock.Now()
		var err error
		result, err = m.rpc.Call(ctx, request, response)
		rpcLatency += m.clock.Now().Sub(attemptStartedAt)
		if err == nil {
			return result, attempt, rpcLatency, false, nil
		}
		if result.ErrorClass != rpc.ErrorNotFound {
			return result, attempt, rpcLatency, false, err
		}
		if attempt == m.cfg.MutationRetries {
			return result, attempt, rpcLatency, true, nil
		}
		if err := m.sleeper.Sleep(
			ctx,
			m.mutationBackoff(attempt),
		); err != nil {
			return result, attempt, rpcLatency, false,
				fmt.Errorf("wait to retry mutation target: %w", err)
		}
	}
	return result, 0, rpcLatency, false, fmt.Errorf(
		"mutation retry loop exited without a result",
	)
}

func (m *Mutator) mutationBackoff(retry int) time.Duration {
	delay := m.cfg.RetryMinBackoff
	for range retry {
		if delay >= m.cfg.RetryMaxBackoff/2 {
			return m.cfg.RetryMaxBackoff
		}
		delay *= 2
	}
	return min(delay, m.cfg.RetryMaxBackoff)
}

func (m *Mutator) missing(
	outcome Outcome,
	latency time.Duration,
) Outcome {
	outcome.Skipped = true
	outcome.TargetMissing = true
	m.recordResult(
		outcome,
		latency,
		rpc.ErrorMutationTargetMissing,
		"",
		true,
	)
	return outcome
}

func (m *Mutator) skip(outcome Outcome) Outcome {
	outcome.Skipped = true
	m.recordResult(outcome, 0, "", "", false)
	return outcome
}

func (m *Mutator) recordResult(
	outcome Outcome,
	latency time.Duration,
	class rpc.ErrorClass,
	reason rpc.ErrorReason,
	targetMissing bool,
) {
	if m.recorder == nil {
		return
	}
	m.recorder.Record(Sample{
		Action: outcome.Action, Latency: latency,
		ErrorClass: class, ErrorReason: reason,
		Retries: outcome.Retries, Skipped: outcome.Skipped,
		TargetMissing: targetMissing,
	})
}
