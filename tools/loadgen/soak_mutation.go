package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

const soakReactionShortcode = "thumbsup"

type soakMutationKind string

const (
	soakMutationEdit      soakMutationKind = "edit"
	soakMutationDelete    soakMutationKind = "delete"
	soakMutationPinFamily soakMutationKind = "pin_family"
)

type soakMutationScheduler struct {
	softDeleteRatio float64
	rng             *rand.Rand
	rngMu           sync.Mutex
	pendingDeletes  atomic.Int64
}

func newSoakMutationScheduler(
	softDeleteRatio float64,
	rng *rand.Rand,
) *soakMutationScheduler {
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &soakMutationScheduler{
		softDeleteRatio: min(max(softDeleteRatio, 0), 1),
		rng:             rng,
	}
}

func (s *soakMutationScheduler) ObserveAcceptedSend() {
	s.rngMu.Lock()
	scheduled := s.rng.Float64() < s.softDeleteRatio
	s.rngMu.Unlock()
	if scheduled {
		s.pendingDeletes.Add(1)
	}
}

func (s *soakMutationScheduler) Next() soakMutationKind {
	for {
		pending := s.pendingDeletes.Load()
		if pending == 0 {
			break
		}
		if s.pendingDeletes.CompareAndSwap(pending, pending-1) {
			return soakMutationDelete
		}
	}
	s.rngMu.Lock()
	edit := s.rng.Intn(2) == 0
	s.rngMu.Unlock()
	if edit {
		return soakMutationEdit
	}
	return soakMutationPinFamily
}

type soakMutationConfig struct {
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

type soakMutationSample struct {
	Action        soakRPCAction
	Latency       time.Duration
	ErrorClass    soakErrorClass
	ErrorReason   soakErrorReason
	Retries       int
	Skipped       bool
	TargetMissing bool
}

type soakMutationSampleRecorder interface {
	Record(soakMutationSample)
}

type soakMutationOutcome struct {
	Action            soakRPCAction
	MessageID         string
	Retries           int
	Skipped           bool
	TargetMissing     bool
	ReactionAction    model.ReactionAction
	AmbiguityResolved bool
}

type soakReactionStateMessage struct {
	RoomID    string `json:"roomId"`
	MessageID string `json:"messageId"`
	Reactions map[string][]struct {
		Account string `json:"account"`
	} `json:"reactions"`
}

type soakMutator struct {
	cfg      soakMutationConfig
	catalog  *soakCatalog
	rpc      *soakRPCClient
	recorder soakMutationSampleRecorder
	rng      *rand.Rand
	clock    soakClock
	sleeper  soakSleeper

	rngMu      sync.Mutex
	members    map[string][]model.SubscriptionUser
	hotMu      sync.Mutex
	hotTargets map[string]string
}

func newSoakMutator(
	cfg *soakMutationConfig,
	topology *soakTopology,
	catalog *soakCatalog,
	rpc *soakRPCClient,
	recorder soakMutationSampleRecorder,
	rng *rand.Rand,
	clock soakClock,
	sleeper soakSleeper,
) *soakMutator {
	if cfg == nil {
		cfg = &soakMutationConfig{}
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
		clock = soakRealClock{}
	}
	if sleeper == nil {
		sleeper = soakTimerSleeper{}
	}
	members := make(map[string][]model.SubscriptionUser)
	if topology != nil {
		active := activeSoakUserIDs(topology)
		for i := range topology.Subscriptions {
			subscription := &topology.Subscriptions[i]
			if isActiveSoakSubscription(subscription, active) &&
				subscription.RoomID != "" &&
				subscription.User.Account != "" {
				members[subscription.RoomID] = append(
					members[subscription.RoomID],
					subscription.User,
				)
			}
		}
	}
	return &soakMutator{
		cfg: config, catalog: catalog, rpc: rpc, recorder: recorder,
		rng: rng, clock: clock, sleeper: sleeper, members: members,
		hotTargets: make(map[string]string),
	}
}

func (m *soakMutator) Edit(
	ctx context.Context,
	roomID string,
	content string,
) (soakMutationOutcome, error) {
	outcome := soakMutationOutcome{Action: soakRPCEdit}
	message, ok := m.catalog.PickAnyEligible(roomID, soakCatalogEdit)
	if !ok {
		return m.skip(outcome), nil
	}
	outcome.MessageID = message.ID
	var response soakEditMessageResponse
	result, retries, latency, targetMissing, err := m.callMutation(
		ctx,
		soakRPCRequest{
			Action: soakRPCEdit,
			Subject: subject.MsgEdit(
				message.Author,
				roomID,
				m.cfg.SiteID,
			),
			Account: message.Author, RoomID: roomID,
			Body: soakEditMessageRequest{
				MessageID: message.ID,
				NewMsg:    content,
			},
			Timeout: m.cfg.RequestTimeout, RetryMode: soakRetrySafe,
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
		err = newSoakAssertionError("edit returned a different message ID")
		m.recordResult(outcome, latency, soakErrorAssertion, "", false)
		return outcome, err
	}
	m.catalog.MarkEdited(roomID, message.ID, content)
	m.recordResult(outcome, latency, "", "", false)
	return outcome, nil
}

func (m *soakMutator) Delete(
	ctx context.Context,
	roomID string,
) (soakMutationOutcome, error) {
	outcome := soakMutationOutcome{Action: soakRPCDelete}
	message, ok := m.catalog.PickAnyEligible(roomID, soakCatalogDelete)
	if !ok {
		return m.skip(outcome), nil
	}
	outcome.MessageID = message.ID
	var response soakDeleteMessageResponse
	result, retries, latency, targetMissing, err := m.callMutation(
		ctx,
		soakRPCRequest{
			Action: soakRPCDelete,
			Subject: subject.MsgDelete(
				message.Author,
				roomID,
				m.cfg.SiteID,
			),
			Account: message.Author, RoomID: roomID,
			Body:    soakDeleteMessageRequest{MessageID: message.ID},
			Timeout: m.cfg.RequestTimeout, RetryMode: soakRetrySafe,
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
		err = newSoakAssertionError("delete returned a different message ID")
		m.recordResult(outcome, latency, soakErrorAssertion, "", false)
		return outcome, err
	}
	m.catalog.MarkDeleted(roomID, message.ID)
	m.recordResult(outcome, latency, "", "", false)
	return outcome, nil
}

func (m *soakMutator) PinOrUnpin(
	ctx context.Context,
	roomID string,
) (soakMutationOutcome, error) {
	pinnedCount := m.catalog.PinnedCount(roomID)
	var (
		message soakCatalogMessage
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
	action := soakRPCUnpin
	if pin {
		action = soakRPCPin
	}
	outcome := soakMutationOutcome{Action: action}
	if !ok {
		return m.skip(outcome), nil
	}
	outcome.MessageID = message.ID

	if pin {
		return m.pin(ctx, roomID, &message, outcome)
	}
	return m.unpin(ctx, roomID, &message, outcome)
}

func (m *soakMutator) pin(
	ctx context.Context,
	roomID string,
	message *soakCatalogMessage,
	outcome soakMutationOutcome,
) (soakMutationOutcome, error) {
	var response soakPinMessageResponse
	result, retries, latency, targetMissing, err := m.callMutation(
		ctx,
		soakRPCRequest{
			Action: soakRPCPin,
			Subject: subject.MsgPin(
				message.Author,
				roomID,
				m.cfg.SiteID,
			),
			Account: message.Author, RoomID: roomID,
			Body:    soakPinMessageRequest{MessageID: message.ID},
			Timeout: m.cfg.RequestTimeout, RetryMode: soakRetrySafe,
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
		m.recordResult(outcome, latency, soakErrorAssertion, "", false)
		return outcome, newSoakAssertionError("pin returned a different message ID")
	}
	m.catalog.SetPinned(roomID, message.ID, true)
	m.recordResult(outcome, latency, "", "", false)
	return outcome, nil
}

func (m *soakMutator) unpin(
	ctx context.Context,
	roomID string,
	message *soakCatalogMessage,
	outcome soakMutationOutcome,
) (soakMutationOutcome, error) {
	var response soakUnpinMessageResponse
	result, retries, latency, targetMissing, err := m.callMutation(
		ctx,
		soakRPCRequest{
			Action: soakRPCUnpin,
			Subject: subject.MsgUnpin(
				message.Author,
				roomID,
				m.cfg.SiteID,
			),
			Account: message.Author, RoomID: roomID,
			Body:    soakUnpinMessageRequest{MessageID: message.ID},
			Timeout: m.cfg.RequestTimeout, RetryMode: soakRetrySafe,
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
		m.recordResult(outcome, latency, soakErrorAssertion, "", false)
		return outcome, newSoakAssertionError("unpin returned a different message ID")
	}
	m.catalog.SetPinned(roomID, message.ID, false)
	m.recordResult(outcome, latency, "", "", false)
	return outcome, nil
}

func (m *soakMutator) React(
	ctx context.Context,
	roomID string,
) (soakMutationOutcome, error) {
	outcome := soakMutationOutcome{Action: soakRPCReact}
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
	request := soakRPCRequest{
		Action:  soakRPCReact,
		Subject: subject.MsgReact(actor.Account, roomID, m.cfg.SiteID),
		Account: actor.Account, RoomID: roomID,
		Body: soakReactMessageRequest{
			MessageID: message.ID,
			Shortcode: soakReactionShortcode,
		},
		Timeout:   m.cfg.RequestTimeout,
		RetryMode: soakRetryAmbiguous,
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
		response      soakReactMessageResponse
		result        soakRPCResult
		err           error
		targetRetries int
	)
	for {
		response = soakReactMessageResponse{}
		attemptStartedAt := m.clock.Now()
		result, err = m.rpc.Call(ctx, request, &response)
		rpcLatency += m.clock.Now().Sub(attemptStartedAt)
		if err == nil || result.ErrorClass != soakErrorNotFound {
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
	if err != nil && result.ErrorClass == soakErrorNotFound &&
		targetRetries >= m.cfg.MutationRetries {
		return m.missing(outcome, latency), nil
	}
	if err != nil {
		m.recordResult(outcome, latency, result.ErrorClass, result.ErrorReason, false)
		return outcome, err
	}
	if !result.AmbiguityResolved &&
		(response.MessageID != message.ID ||
			response.Shortcode != soakReactionShortcode ||
			(response.Action != model.ReactionActionAdded &&
				response.Action != model.ReactionActionRemoved)) {
		m.recordResult(outcome, latency, soakErrorAssertion, "", false)
		return outcome, newSoakAssertionError(
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
		soakReactionShortcode,
		actor.Account,
		actual == model.ReactionActionAdded,
	)
	m.recordResult(outcome, latency, "", "", false)
	return outcome, nil
}

func (m *soakMutator) reactionTarget(
	roomID string,
) (soakCatalogMessage, bool) {
	if m.cfg.ReactionMessageScope == "hot_only" {
		m.hotMu.Lock()
		messageID := m.hotTargets[roomID]
		m.hotMu.Unlock()
		if messageID != "" {
			if message, ok := m.catalog.GetEligible(
				roomID,
				messageID,
				soakCatalogReaction,
			); ok {
				return message, true
			}
		}
	}
	message, ok := m.catalog.PickAnyEligible(roomID, soakCatalogReaction)
	if ok && m.cfg.ReactionMessageScope == "hot_only" {
		m.hotMu.Lock()
		m.hotTargets[roomID] = message.ID
		m.hotMu.Unlock()
	}
	return message, ok
}

func (m *soakMutator) reactionActor(
	roomID string,
	message *soakCatalogMessage,
) (model.SubscriptionUser, model.ReactionAction, bool) {
	members := m.members[roomID]
	if len(members) == 0 {
		return model.SubscriptionUser{}, "", false
	}
	existing := message.Reactions[soakReactionShortcode]
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

func (m *soakMutator) readReactionState(
	ctx context.Context,
	account string,
	roomID string,
	messageID string,
) (bool, error) {
	var state soakReactionStateMessage
	_, err := m.rpc.Call(ctx, soakRPCRequest{
		Action: soakRPCGetMessage,
		Subject: subject.MsgGet(
			account,
			roomID,
			m.cfg.SiteID,
		),
		Account: account, RoomID: roomID,
		Body:      soakGetMessageByIDRequest{MessageID: messageID},
		Timeout:   m.cfg.RequestTimeout,
		RetryMode: soakRetrySafe,
	}, &state)
	if err != nil {
		return false, fmt.Errorf("read reaction state: %w", err)
	}
	if state.MessageID != messageID || state.RoomID != roomID {
		return false, newSoakAssertionError(
			"reaction state read returned a different message",
		)
	}
	for _, reactor := range state.Reactions[soakReactionShortcode] {
		if reactor.Account == account {
			return true, nil
		}
	}
	return false, nil
}

//nolint:gocritic // hugeParam: the request carries the failure identity; the copy is nothing beside the round trip.
func (m *soakMutator) callMutation(
	ctx context.Context,
	request soakRPCRequest,
	response any,
) (soakRPCResult, int, time.Duration, bool, error) {
	var result soakRPCResult
	var rpcLatency time.Duration
	for attempt := 0; attempt <= m.cfg.MutationRetries; attempt++ {
		attemptStartedAt := m.clock.Now()
		var err error
		result, err = m.rpc.Call(ctx, request, response)
		rpcLatency += m.clock.Now().Sub(attemptStartedAt)
		if err == nil {
			return result, attempt, rpcLatency, false, nil
		}
		if result.ErrorClass != soakErrorNotFound {
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

func (m *soakMutator) mutationBackoff(retry int) time.Duration {
	delay := m.cfg.RetryMinBackoff
	for range retry {
		if delay >= m.cfg.RetryMaxBackoff/2 {
			return m.cfg.RetryMaxBackoff
		}
		delay *= 2
	}
	return min(delay, m.cfg.RetryMaxBackoff)
}

func (m *soakMutator) missing(
	outcome soakMutationOutcome,
	latency time.Duration,
) soakMutationOutcome {
	outcome.Skipped = true
	outcome.TargetMissing = true
	m.recordResult(
		outcome,
		latency,
		soakErrorMutationTargetMissing,
		"",
		true,
	)
	return outcome
}

func (m *soakMutator) skip(outcome soakMutationOutcome) soakMutationOutcome {
	outcome.Skipped = true
	m.recordResult(outcome, 0, "", "", false)
	return outcome
}

func (m *soakMutator) recordResult(
	outcome soakMutationOutcome,
	latency time.Duration,
	class soakErrorClass,
	reason soakErrorReason,
	targetMissing bool,
) {
	if m.recorder == nil {
		return
	}
	m.recorder.Record(soakMutationSample{
		Action: outcome.Action, Latency: latency,
		ErrorClass: class, ErrorReason: reason,
		Retries: outcome.Retries, Skipped: outcome.Skipped,
		TargetMissing: targetMissing,
	})
}
