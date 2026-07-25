package main

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

type soakReadKind string

const (
	soakReadHistory soakReadKind = "load_history"
	soakReadThread  soakReadKind = "get_thread_messages"
	soakReadMessage soakReadKind = "get_message_by_id"
)

func pickSoakReadKind(rng *rand.Rand) soakReadKind {
	roll := rng.Float64()
	switch {
	case roll < 0.75:
		return soakReadHistory
	case roll < 0.90:
		return soakReadThread
	default:
		return soakReadMessage
	}
}

type soakReadConfig struct {
	SiteID         string
	PageLimit      int
	MaxPages       int
	RequestTimeout time.Duration
}

type soakReadSample struct {
	Action     soakRPCAction
	Latency    time.Duration
	Messages   int
	ErrorClass soakErrorClass
	Retries    int
	Skipped    bool
}

type soakReadSampleRecorder interface {
	Record(soakReadSample)
}

type soakReadOutcome struct {
	Action    soakRPCAction
	Pages     int
	Messages  int
	MessageID string
	Skipped   bool
}

type soakReader struct {
	cfg      soakReadConfig
	catalog  *soakCatalog
	rpc      *soakRPCClient
	recorder soakReadSampleRecorder
	rng      *rand.Rand
	now      func() time.Time
	members  map[string][]model.SubscriptionUser
	rngMu    sync.Mutex
}

func newSoakReader(
	cfg soakReadConfig,
	topology *soakTopology,
	catalog *soakCatalog,
	rpc *soakRPCClient,
	recorder soakReadSampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) *soakReader {
	if cfg.PageLimit <= 0 {
		cfg.PageLimit = 50
	}
	if cfg.MaxPages <= 0 {
		cfg.MaxPages = 100
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if now == nil {
		now = time.Now
	}
	members := make(map[string][]model.SubscriptionUser)
	if topology != nil {
		for i := range topology.Subscriptions {
			subscription := &topology.Subscriptions[i]
			if !subscription.IsSubscribed ||
				subscription.RoomID == "" ||
				subscription.User.Account == "" {
				continue
			}
			members[subscription.RoomID] = append(
				members[subscription.RoomID],
				subscription.User,
			)
		}
	}
	return &soakReader{
		cfg: cfg, catalog: catalog, rpc: rpc, recorder: recorder,
		rng: rng, now: now, members: members,
	}
}

func (r *soakReader) ReadMixed(
	ctx context.Context,
	roomID string,
) (soakReadOutcome, error) {
	r.rngMu.Lock()
	kind := pickSoakReadKind(r.rng)
	r.rngMu.Unlock()
	switch kind {
	case soakReadThread:
		return r.GetThreadMessages(ctx, roomID)
	case soakReadMessage:
		return r.GetMessageByID(ctx, roomID)
	default:
		return r.LoadHistory(ctx, roomID)
	}
}

func (r *soakReader) LoadHistory(
	ctx context.Context,
	roomID string,
) (soakReadOutcome, error) {
	outcome := soakReadOutcome{Action: soakRPCLoadHistory}
	account, ok := r.pickAccount(roomID)
	if !ok {
		return r.skip(outcome), nil
	}

	var before *int64
	lastMsgAt := r.now().UTC().UnixMilli()
	meta := &soakRoomMeta{LastMsgAt: &lastMsgAt}
	for range r.cfg.MaxPages {
		request := soakLoadHistoryRequest{
			Before: before,
			Limit:  r.cfg.PageLimit,
			Meta:   meta,
		}
		var response soakLoadHistoryResponse
		result, latency, err := r.call(ctx, soakRPCRequest{
			Action: soakRPCLoadHistory,
			Subject: subject.MsgHistory(
				account,
				roomID,
				r.cfg.SiteID,
			),
			Body: request, Timeout: r.cfg.RequestTimeout,
			RetryMode: soakRetrySafe,
		}, &response)
		if err != nil {
			r.record(soakReadSample{
				Action: soakRPCLoadHistory, Latency: latency,
				ErrorClass: result.ErrorClass, Retries: result.Retries,
			})
			return outcome, err
		}

		outcome.Pages++
		outcome.Messages += len(response.Messages)
		sample := soakReadSample{
			Action: soakRPCLoadHistory, Latency: latency,
			Messages: len(response.Messages), Retries: result.Retries,
		}
		if len(response.Messages) == 0 {
			r.record(sample)
			return outcome, nil
		}

		oldest := oldestSoakMessageMillis(response.Messages)
		if before != nil && oldest >= *before {
			sample.ErrorClass = soakErrorAssertion
			r.record(sample)
			return outcome, newSoakAssertionError(
				"LoadHistory page did not make timestamp progress",
			)
		}
		if oldest == math.MinInt64 {
			sample.ErrorClass = soakErrorAssertion
			r.record(sample)
			return outcome, newSoakAssertionError(
				"LoadHistory oldest timestamp cannot advance",
			)
		}
		nextBefore := oldest - 1
		before = &nextBefore
		r.record(sample)
	}
	return outcome, nil
}

func (r *soakReader) GetThreadMessages(
	ctx context.Context,
	roomID string,
) (soakReadOutcome, error) {
	outcome := soakReadOutcome{Action: soakRPCGetThread}
	account, ok := r.pickAccount(roomID)
	if !ok {
		return r.skip(outcome), nil
	}
	parent, ok := r.catalog.PickEligible(
		roomID,
		account,
		soakCatalogThreadParent,
	)
	if !ok {
		return r.skip(outcome), nil
	}

	cursor := ""
	seen := make(map[string]struct{})
	for range r.cfg.MaxPages {
		var response soakGetThreadMessagesResponse
		result, latency, err := r.call(ctx, soakRPCRequest{
			Action:  soakRPCGetThread,
			Subject: subject.MsgThread(account, roomID, r.cfg.SiteID),
			Body: soakGetThreadMessagesRequest{
				ThreadMessageID: parent.ID,
				Cursor:          cursor,
				Limit:           r.cfg.PageLimit,
			},
			Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
		}, &response)
		if err != nil {
			r.record(soakReadSample{
				Action: soakRPCGetThread, Latency: latency,
				ErrorClass: result.ErrorClass, Retries: result.Retries,
			})
			return outcome, err
		}
		outcome.Pages++
		outcome.Messages += len(response.Messages)
		sample := soakReadSample{
			Action: soakRPCGetThread, Latency: latency,
			Messages: len(response.Messages), Retries: result.Retries,
		}
		if !response.HasNext {
			r.record(sample)
			return outcome, nil
		}
		if !advanceSoakCursor(cursor, response.NextCursor, seen) {
			sample.ErrorClass = soakErrorAssertion
			r.record(sample)
			return outcome, newSoakAssertionError(
				"GetThreadMessages cursor did not make progress",
			)
		}
		seen[response.NextCursor] = struct{}{}
		cursor = response.NextCursor
		r.record(sample)
	}
	return outcome, nil
}

func (r *soakReader) GetMessageByID(
	ctx context.Context,
	roomID string,
) (soakReadOutcome, error) {
	outcome := soakReadOutcome{Action: soakRPCGetMessage}
	account, ok := r.pickAccount(roomID)
	if !ok {
		return r.skip(outcome), nil
	}
	message, ok := r.catalog.PickEligible(
		roomID,
		account,
		soakCatalogReaction,
	)
	if !ok {
		return r.skip(outcome), nil
	}

	var response soakWireMessage
	result, latency, err := r.call(ctx, soakRPCRequest{
		Action:  soakRPCGetMessage,
		Subject: subject.MsgGet(account, roomID, r.cfg.SiteID),
		Body:    soakGetMessageByIDRequest{MessageID: message.ID},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response)
	if err != nil {
		r.record(soakReadSample{
			Action: soakRPCGetMessage, Latency: latency,
			ErrorClass: result.ErrorClass, Retries: result.Retries,
		})
		return outcome, err
	}
	sample := soakReadSample{
		Action: soakRPCGetMessage, Latency: latency,
		Messages: 1, Retries: result.Retries,
	}
	if response.MessageID != message.ID {
		sample.ErrorClass = soakErrorAssertion
		r.record(sample)
		return outcome, newSoakAssertionError(
			"GetMessageByID returned a different message ID",
		)
	}
	outcome.Pages = 1
	outcome.Messages = 1
	outcome.MessageID = response.MessageID
	r.record(sample)
	return outcome, nil
}

func (r *soakReader) ListPinnedMessages(
	ctx context.Context,
	roomID string,
) (soakReadOutcome, error) {
	outcome := soakReadOutcome{Action: soakRPCPinnedList}
	account, ok := r.pickAccount(roomID)
	if !ok {
		return r.skip(outcome), nil
	}

	cursor := ""
	seen := make(map[string]struct{})
	for range r.cfg.MaxPages {
		var response soakListPinnedMessagesResponse
		result, latency, err := r.call(ctx, soakRPCRequest{
			Action:  soakRPCPinnedList,
			Subject: subject.MsgPinnedList(account, roomID, r.cfg.SiteID),
			Body: soakListPinnedMessagesRequest{
				Cursor: cursor,
				Limit:  r.cfg.PageLimit,
			},
			Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
		}, &response)
		if err != nil {
			r.record(soakReadSample{
				Action: soakRPCPinnedList, Latency: latency,
				ErrorClass: result.ErrorClass, Retries: result.Retries,
			})
			return outcome, err
		}
		outcome.Pages++
		outcome.Messages += len(response.Messages)
		if r.catalog != nil {
			for i := range response.Messages {
				r.catalog.ObservePinned(&response.Messages[i])
			}
		}
		sample := soakReadSample{
			Action: soakRPCPinnedList, Latency: latency,
			Messages: len(response.Messages), Retries: result.Retries,
		}
		if !response.HasNext {
			r.record(sample)
			return outcome, nil
		}
		if !advanceSoakCursor(cursor, response.NextCursor, seen) {
			sample.ErrorClass = soakErrorAssertion
			r.record(sample)
			return outcome, newSoakAssertionError(
				"ListPinnedMessages cursor did not make progress",
			)
		}
		seen[response.NextCursor] = struct{}{}
		cursor = response.NextCursor
		r.record(sample)
	}
	return outcome, nil
}

func (r *soakReader) call(
	ctx context.Context,
	request soakRPCRequest,
	response any,
) (soakRPCResult, time.Duration, error) {
	startedAt := r.now()
	result, err := r.rpc.Call(ctx, request, response)
	return result, r.now().Sub(startedAt), err
}

func (r *soakReader) skip(outcome soakReadOutcome) soakReadOutcome {
	outcome.Skipped = true
	r.record(soakReadSample{Action: outcome.Action, Skipped: true})
	return outcome
}

func (r *soakReader) record(sample soakReadSample) {
	if r.recorder != nil {
		r.recorder.Record(sample)
	}
}

func (r *soakReader) pickAccount(roomID string) (string, bool) {
	members := r.members[roomID]
	if len(members) == 0 {
		return "", false
	}
	r.rngMu.Lock()
	member := members[r.rng.Intn(len(members))]
	r.rngMu.Unlock()
	return member.Account, true
}

func oldestSoakMessageMillis(messages []soakWireMessage) int64 {
	oldest := messages[0].CreatedAt.UTC().UnixMilli()
	for i := 1; i < len(messages); i++ {
		createdAt := messages[i].CreatedAt.UTC().UnixMilli()
		if createdAt < oldest {
			oldest = createdAt
		}
	}
	return oldest
}

func advanceSoakCursor(
	current string,
	next string,
	seen map[string]struct{},
) bool {
	if next == "" || next == current {
		return false
	}
	_, duplicate := seen[next]
	return !duplicate
}
