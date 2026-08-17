package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/subject"
)

type soakRoomReadConfig struct {
	SiteID         string
	BatchSize      int
	RequestTimeout time.Duration
}

// soakRoomMessageSource supplies a persisted message the read-receipt read can
// ask about. The message catalog satisfies it; the interface lives here so the
// reader depends on the one method it needs.
type soakRoomMessageSource interface {
	PickAnyEligible(roomID string, action soakCatalogAction) (soakCatalogMessage, bool)
}

// soakRoomReader drives the read-only room lane and serves the point lookups
// the room-state observer needs. Reads carry no correctness ledger: a read has
// no expected side effect to reconcile, only latency and an outcome.
type soakRoomReader struct {
	cfg      soakRoomReadConfig
	pool     *soakRoomStatePool
	rpc      *soakRPCClient
	recorder soakReadSampleRecorder
	now      func() time.Time

	mu       sync.Mutex
	rng      *rand.Rand
	rooms    []string
	accounts map[string]string
	created  []string
	messages soakRoomMessageSource
}

// SetMessageSource wires the message catalog in after construction, because the
// catalog is built alongside the message lanes rather than the room pool.
func (r *soakRoomReader) SetMessageSource(messages soakRoomMessageSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = messages
}

func newSoakRoomReader(
	cfg soakRoomReadConfig,
	pool *soakRoomStatePool,
	rpc *soakRPCClient,
	recorder soakReadSampleRecorder,
	rng *rand.Rand,
	now func() time.Time,
) *soakRoomReader {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 20
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = soakRequestTimeout
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if now == nil {
		now = time.Now
	}
	reader := &soakRoomReader{
		cfg: cfg, pool: pool, rpc: rpc, recorder: recorder, now: now, rng: rng,
		accounts: make(map[string]string),
	}
	if pool != nil {
		reader.rooms = pool.RoomIDs()
		for _, roomID := range reader.rooms {
			if owner, ok := pool.Owner(roomID); ok {
				reader.accounts[roomID] = owner
			}
		}
	}
	return reader
}

// RegisterCreatedRoom brings a room the create lane made into the read mix.
// Created rooms deliberately stay out of the message and history lanes: those
// build their subscriptions, recipient expectations, and room distribution once
// at startup, so adding rooms mid-run would shift the traffic profile a fault
// window is compared against.
func (r *soakRoomReader) RegisterCreatedRoom(roomID, account string) {
	if roomID == "" || account == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.created = append(r.created, roomID)
	r.accounts[roomID] = account
}

func (r *soakRoomReader) ReadMixed(ctx context.Context) error {
	r.mu.Lock()
	roll := r.rng.Float64()
	r.mu.Unlock()
	switch {
	case roll < 0.45:
		roomID, account, ok := r.pickRoom()
		if !ok {
			r.recordSkip(soakRPCMemberList)
			return nil
		}
		_, err := r.ListMembers(ctx, roomID, account)
		return err
	case roll < 0.72:
		return r.RoomsInfo(ctx)
	case roll < 0.9:
		return r.SubscriptionList(ctx)
	default:
		return r.ReadReceipts(ctx)
	}
}

// ReadReceipts asks who has read one persisted message. It needs a real message
// ID, so it is the one room read that depends on the message catalog; without
// one it degrades to a skip rather than measuring a rejected request.
//
// The request is addressed as the message's own author, not as the account the
// reader holds the room under: room-service serves read receipts only to the
// sender, so any other identity is a guaranteed permission error.
func (r *soakRoomReader) ReadReceipts(ctx context.Context) error {
	roomID, _, ok := r.pickRoom()
	if !ok {
		r.recordSkip(soakRPCReadReceiptList)
		return nil
	}
	r.mu.Lock()
	messages := r.messages
	r.mu.Unlock()
	if messages == nil {
		r.recordSkip(soakRPCReadReceiptList)
		return nil
	}
	message, found := messages.PickAnyEligible(roomID, soakCatalogReadReceipt)
	if !found || message.ID == "" || message.Author == "" {
		r.recordSkip(soakRPCReadReceiptList)
		return nil
	}

	var response soakReadReceiptResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCReadReceiptList,
		Subject: subject.MessageReadReceipt(message.Author, roomID, r.cfg.SiteID),
		Body:    soakReadReceiptRequest{MessageID: message.ID},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Readers)
	})
}

func (r *soakRoomReader) ListMembers(
	ctx context.Context,
	roomID, account string,
) (soakListMembersResponse, error) {
	var response soakListMembersResponse
	if roomID == "" || account == "" {
		return response, fmt.Errorf("list members requires a room and account")
	}
	err := r.call(ctx, soakRPCRequest{
		Action:  soakRPCMemberList,
		Subject: subject.MemberList(account, roomID, r.cfg.SiteID),
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Members)
	})
	return response, err
}

func (r *soakRoomReader) RoomsInfo(ctx context.Context) error {
	roomIDs := r.pickRoomBatch()
	if len(roomIDs) == 0 {
		r.recordSkip(soakRPCRoomsInfo)
		return nil
	}
	var response soakRoomsInfoResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCRoomsInfo,
		Subject: subject.RoomsInfoBatchSubscribe(r.cfg.SiteID),
		Body:    soakRoomsInfoRequest{RoomIDs: roomIDs},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Rooms)
	})
}

func (r *soakRoomReader) SubscriptionList(ctx context.Context) error {
	_, account, ok := r.pickRoom()
	if !ok {
		r.recordSkip(soakRPCSubscriptionList)
		return nil
	}
	var response soakSubscriptionListResponse
	return r.call(ctx, soakRPCRequest{
		Action:  soakRPCSubscriptionList,
		Subject: subject.UserSubscriptionList(account, r.cfg.SiteID),
		Body:    soakSubscriptionListRequest{Type: soakSubscriptionListType},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Subscriptions)
	})
}

// RoomState reads one room's members through the production RPC path. The
// room-state observer uses it as its first source before falling back to the
// authoritative store.
func (r *soakRoomReader) RoomState(
	ctx context.Context,
	roomID, account string,
) (soakListMembersResponse, error) {
	var response soakListMembersResponse
	if roomID == "" || account == "" {
		return response, fmt.Errorf("room state read requires a room and account")
	}
	err := r.call(ctx, soakRPCRequest{
		Action:  soakRPCRoomStateRead,
		Subject: subject.MemberList(account, roomID, r.cfg.SiteID),
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Members)
	})
	return response, err
}

// RoomInfoFor reads one room's metadata through the production RPC path.
func (r *soakRoomReader) RoomInfoFor(ctx context.Context, roomID string) (soakRoomInfo, error) {
	if roomID == "" {
		return soakRoomInfo{}, fmt.Errorf("room info read requires a room")
	}
	var response soakRoomsInfoResponse
	err := r.call(ctx, soakRPCRequest{
		Action:  soakRPCRoomStateRead,
		Subject: subject.RoomsInfoBatchSubscribe(r.cfg.SiteID),
		Body:    soakRoomsInfoRequest{RoomIDs: []string{roomID}},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Rooms)
	})
	if err != nil {
		return soakRoomInfo{}, err
	}
	for i := range response.Rooms {
		if response.Rooms[i].RoomID == roomID {
			return response.Rooms[i], nil
		}
	}
	return soakRoomInfo{RoomID: roomID}, nil
}

// SubscriptionFor reads one account's subscription to one room through the
// production RPC path, so mute and read state are checked the way a client sees
// them. Deliberately the by-room lookup rather than subscription.list: that list
// is paged, and the verifiers read "room absent from the response" as a
// mismatch, so a room that fell off the page would be reported as corruption.
// This answers 0-or-1 for the room asked about, and an empty answer means the
// subscription is genuinely gone.
func (r *soakRoomReader) SubscriptionFor(
	ctx context.Context,
	account, roomID string,
) (soakSubscriptionListResponse, error) {
	var response soakSubscriptionListResponse
	if account == "" || roomID == "" {
		return response, fmt.Errorf("subscription read requires an account and room")
	}
	err := r.call(ctx, soakRPCRequest{
		Action:  soakRPCRoomStateRead,
		Subject: subject.UserSubscriptionGetByRoomID(account, r.cfg.SiteID),
		Body:    soakUserRoomRequest{RoomID: roomID},
		Timeout: r.cfg.RequestTimeout, RetryMode: soakRetrySafe,
	}, &response, func(sample *soakReadSample) {
		sample.Messages = len(response.Subscriptions)
	})
	if err != nil {
		// Two verifiers share this call, so the transport error alone does not
		// say which room's state could not be read.
		return response, fmt.Errorf("read subscription for room %q: %w", roomID, err)
	}
	return response, nil
}

// Account returns the account the reader addresses a room as, so the observer
// asks room-service with an identity that is actually a member.
func (r *soakRoomReader) Account(roomID string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	account, ok := r.accounts[roomID]
	return account, ok
}

func (r *soakRoomReader) pickRoom() (string, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := len(r.rooms) + len(r.created)
	if total == 0 {
		return "", "", false
	}
	index := r.rng.Intn(total)
	roomID := ""
	if index < len(r.rooms) {
		roomID = r.rooms[index]
	} else {
		roomID = r.created[index-len(r.rooms)]
	}
	account, ok := r.accounts[roomID]
	return roomID, account, ok
}

func (r *soakRoomReader) pickRoomBatch() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	pool := make([]string, 0, len(r.rooms)+len(r.created))
	pool = append(pool, r.rooms...)
	pool = append(pool, r.created...)
	if len(pool) == 0 {
		return nil
	}
	size := min(r.cfg.BatchSize, len(pool))
	start := r.rng.Intn(len(pool))
	batch := make([]string, 0, size)
	for offset := range size {
		batch = append(batch, pool[(start+offset)%len(pool)])
	}
	return batch
}

func (r *soakRoomReader) call(
	ctx context.Context,
	request soakRPCRequest,
	response any,
	apply func(*soakReadSample),
) error {
	if r.rpc == nil {
		return fmt.Errorf("soak room reader requires an RPC client")
	}
	startedAt := r.now()
	result, err := r.rpc.Call(ctx, request, response)
	sample := soakReadSample{
		Action: request.Action, Latency: r.now().Sub(startedAt), Retries: result.Retries,
	}
	if err != nil {
		sample.ErrorClass = result.ErrorClass
		r.record(sample)
		return fmt.Errorf("issue %s request: %w", request.Action, err)
	}
	apply(&sample)
	r.record(sample)
	return nil
}

func (r *soakRoomReader) recordSkip(action soakRPCAction) {
	r.record(soakReadSample{Action: action, Skipped: true})
}

func (r *soakRoomReader) record(sample soakReadSample) {
	if r.recorder != nil {
		r.recorder.Record(sample)
	}
}
